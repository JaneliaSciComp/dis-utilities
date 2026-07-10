''' pull_internal_acks.py

PURPOSE
-------
Fetches and stores acknowledgement text for Janelia-authored (internal) DOIs that
do not yet have a `jrc_acknowledgements` field in the DIS MongoDB database.
Sources queried, in order:
- eLife       – via the eLife API (doi_common.get_doi_record)
- Elsevier    – via the Elsevier full-text API (doi_common.get_acknowledgements)
- PubMed Central (PMC) – via the PMC OAI-PMH API (doi_common.get_acknowledgements
                         with a PMCID)
- arXiv        – via the arXiv HTML render (then e-print TeX source) for DataCite
                arXiv DOIs (10.48550/arxiv.*), handled inside
                doi_common.get_acknowledgements

INPUTS
------
- NCBI_API_KEY environment variable (required): API key for the NCBI E-utilities API.
- DIS MongoDB database (read/write depending on --write flag):
    - Collection `dois`      : source of DOI records; updated with acknowledgements.
- Command-line flags:
    --doi DOI  Restrict processing to a single DOI (across all sources).
    --source   Restrict processing to a single source (elife, elsevier, pmc, or
               arxiv). Omit to process all sources.
    --write    Actually update the database (default: dry-run).
    --verbose  Increase logging verbosity.
    --debug    Maximum logging verbosity.

EMAIL RECIPIENT
----------------
The summary email always goes to the configured developer address, never the
full receivers list, and is sent any time acknowledgements are found -
--write or not (a dry run's "would update" findings are just as worth seeing
as a real run's).

HIGH-LEVEL FLOW
---------------
1. Initialization
   - Connects to the DIS MongoDB database (read-only by default; read/write with --write).
2. eLife pass (add_elife_internal_acks)
   - Queries `dois` for records whose DOI matches /elife/ and that lack
     `jrc_acknowledgements`.
   - Calls doi_common.get_doi_record(doi, source='elife') and concatenates the
     returned acknowledgement paragraph texts.
3. Elsevier pass (add_elsevier_internal_acks)
   - Queries `dois` for records whose DOI matches /10.1016\// and that lack
     `jrc_acknowledgements`.
   - Calls doi_common.get_acknowledgements with a 0.1 s inter-request sleep to
     stay within the Elsevier rate limit.
4. PMC pass (add_pmc_internal_acks)
   - Queries `dois` for records that have a `jrc_pmc` field but lack
     `jrc_acknowledgements`.
   - Calls doi_common.get_acknowledgements with the PMCID.
5. arXiv pass (add_arxiv_internal_acks)
   - Queries `dois` for records whose DOI matches /10.48550\/arxiv/ and that lack
     `jrc_acknowledgements`.
   - Calls doi_common.get_acknowledgements, which downloads the paper from arXiv
     (HTML render, then e-print TeX source) and extracts the Acknowledgements
     section.
6. Database update (--write mode)
   - For each collected record, performs a MongoDB update_one setting
     `jrc_acknowledgements` on the matching DOI document.
7. Output
   - Prints a per-source summary of counts.
   - Writes internal_acks.json with all collected acknowledgement records.
   - Writes internal_ack_errors.json if any source calls raised exceptions.
   - Sends a summary email whenever records were found (--write or not):
     a header banner (run data, mode, DRY RUN/WRITE badge), KPI stat tiles per
     source plus an error tile, one card per source (eLife/Elsevier/PMC/arXiv)
     listing its DOIs (linked to the DIS UI, with a PMCID column for PMC), and
     an Errors table when any source call raised. Built entirely from inline
     styles/tables (no <style> block) for compatibility with older email clients,
     matching the convention used by sync_citations.py.

DEPENDENCIES
------------
- jrc_common.jrc_common  (JRC): logging, config, database connection, email helpers.
- doi_common.doi_common  (DL): DOI record retrieval and acknowledgement extraction
                               (eLife API, Elsevier API, PMC OAI-PMH, arXiv full text).
- tqdm: progress bars for per-source processing loops.
'''

import argparse
import collections
import html
import json
from operator import attrgetter
import os
import sys
import time
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError
from tqdm import tqdm
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL

__version__ = '1.4.0'

# pylint: disable=broad-exception-caught,logging-fstring-interpolation,no-member

# Database
DB = {}
# Counters
COUNT = collections.defaultdict(lambda: 0, {})
# Global variables
ARG = DIS = LOGGER = None
# Display order for the "source" label stored on each internal-DOI record
# (add_elife_internal_acks etc.), used to group the run-summary email.
SOURCE_LABELS = ('eLife', 'Elsevier', 'PMC', 'arXiv')
# HTML run-summary email palette/layout (generate_email and its html_* helpers).
# Mirrors sync_citations.py's email convention: inline styles only (no <style>
# block/classes), colors paired with an icon/label (not color alone) for
# colorblind accessibility, for reliable rendering across email clients
# including older Outlook.
EMAIL_NAVY = '#1f3a5f'
EMAIL_GREEN = '#1c7c3f'
EMAIL_GREEN_BG = '#eefaf1'
EMAIL_RED = '#c0392b'
EMAIL_RED_BG = '#fdecea'
EMAIL_AMBER = '#d68a1f'
EMAIL_GRAY = '#5b6b7c'
EMAIL_GRAY_BG = '#f2f4f6'
EMAIL_STRIPE_BG = '#f7f9fb'
EMAIL_BORDER = '#eef1f4'
EMAIL_BLUE = '#2f7fd1'

def terminate_program(msg=None):
    ''' Terminate the program gracefully
        Keyword arguments:
          msg: error message or object
        Returns:
          None
    '''
    if msg:
        if not isinstance(msg, str):
            msg = f"An exception of type {type(msg).__name__} occurred. Arguments:\n{msg.args}"
        LOGGER.critical(msg)
    sys.exit(-1 if msg else 0)


def initialize_program():
    ''' Initialize database connection
        Keyword arguments:
          None
        Returns:
          None
    '''
    if "NCBI_API_KEY" not in os.environ:
        terminate_program("Missing NCBI API key - set in NCBI_API_KEY environment variable")
    try:
        dbconfig = JRC.get_config("databases")
    except Exception as err:
        terminate_program(err)
    dbs = ['dis']
    for source in dbs:
        dbo = attrgetter(f"{source}.prod.{'read' if not ARG.WRITE else 'write'}")(dbconfig)
        LOGGER.info(f"Connecting to {dbo.name} prod on {dbo.host} as {dbo.user}")
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)


def restrict_to_doi(payload):
    ''' Restrict a query payload to a single DOI if --doi was supplied
        Keyword arguments:
          payload: MongoDB query payload
        Returns:
          The (possibly restricted) query payload
    '''
    if ARG.DOI:
        return {"$and": [payload, {"doi": ARG.DOI.lower()}]}
    return payload


def add_elife_internal_acks(internal, error):
    ''' Add eLife acknowledgements to the internal DOIs
        Keyword arguments:
          internal: list of internal DOIs
          error: list of error records
        Returns:
          None
    '''
    payload = {"doi": {"$regex": r"10\.7554/elife"}, "jrc_acknowledgements": {"$exists": False}}
    payload = restrict_to_doi(payload)
    try:
        cnt = DB['dis'].dois.count_documents(payload)
        LOGGER.info(f"Found {cnt:,} eLife DOIs without acknowledgements")
        rows = DB['dis'].dois.find(payload)
    except Exception as err:
        terminate_program(err)
    for row in tqdm(rows, total=cnt, desc="Finding eLife acknowledgements"):
        doi = row['doi']
        time.sleep(0.1)
        # Guard the whole per-DOI fetch/parse: a single bad record (failed lookup,
        # malformed acknowledgements, missing 'text') must not abort the pass - and
        # since eLife runs first, an uncaught error here would block every source.
        try:
            edata = DL.get_doi_record(doi, source='elife')
            acklist = [ack['text'] for ack in (edata or {}).get('acknowledgements', [])
                       if ack.get('text')]
        except Exception as err:
            error.append({"doi": doi, "source": "elife", "error": str(err)})
            continue
        if acklist:
            COUNT['elife_add'] += 1
            internal.append({"doi": doi,
                             "ack": ' '.join(acklist),
                             "source": "eLife"})


def add_elsevier_internal_acks(internal, error):
    ''' Add Elsevier acknowledgements to the internal DOIs
        Keyword arguments:
          internal: list of internal DOIs
          error: list of error records
        Returns:
          None
    '''
    payload = {"doi": {"$regex": r"10\.1016/"}, "jrc_acknowledgements": {"$exists": False}}
    payload = restrict_to_doi(payload)
    try:
        cnt = DB['dis'].dois.count_documents(payload)
        LOGGER.info(f"Found {cnt:,} Elsevier DOIs without acknowledgements")
        rows = DB['dis'].dois.find(payload)
    except Exception as err:
        terminate_program(err)
    for row in tqdm(rows, total=cnt, desc="Finding Elsevier acknowledgements"):
        time.sleep(0.1)
        try:
            acktext, _ = DL.get_acknowledgements(row['doi'])
        except Exception as err:
            error.append({"doi": row['doi'], "source": "elsevier", "error": str(err)})
            continue
        if acktext:
            COUNT['elsevier_add'] += 1
            internal.append({"doi": row['doi'],
                             "ack": acktext,
                             "source": "Elsevier"})


def add_pmc_internal_acks(internal, error):
    ''' Add PMC acknowledgements to the internal DOIs
        Keyword arguments:
          internal: list of internal DOIs
          error: list of error records
        Returns:
          None
    '''
    payload = {"jrc_pmc": {"$exists": True}, "jrc_acknowledgements": {"$exists": False}}
    payload = restrict_to_doi(payload)
    try:
        cnt = DB['dis'].dois.count_documents(payload)
        if cnt < 1:
            return
        LOGGER.info(f"Found {cnt:,} PMC DOIs without acknowledgements")
        rows = DB['dis'].dois.find(payload)
    except Exception as err:
        terminate_program(err)
    for row in tqdm(rows, total=cnt, desc="Finding PMC acknowledgements"):
        time.sleep(0.1)
        try:
            ack, _ = DL.get_acknowledgements(row['doi'], pmcid=row['jrc_pmc'])
        except Exception as err:
            error.append({"doi": row['doi'], "pmcid": row['jrc_pmc'],
                          "source": "pmc", "error": str(err)})
            continue
        if ack:
            COUNT['pmc_add'] += 1
            internal.append({"pmcid": row['jrc_pmc'],
                             "doi": row['doi'],
                             "ack": ack,
                             "source": "PMC"})


def add_arxiv_internal_acks(internal, error):
    ''' Add arXiv acknowledgements to the internal DOIs
        Keyword arguments:
          internal: list of internal DOIs
          error: list of error records
        Returns:
          None
    '''
    payload = {"doi": {"$regex": r"10\.48550/arxiv"}, "jrc_acknowledgements": {"$exists": False}}
    payload = restrict_to_doi(payload)
    try:
        cnt = DB['dis'].dois.count_documents(payload)
        if cnt < 1:
            return
        LOGGER.info(f"Found {cnt:,} arXiv DOIs without acknowledgements")
        rows = DB['dis'].dois.find(payload)
    except Exception as err:
        terminate_program(err)
    for row in tqdm(rows, total=cnt, desc="Finding arXiv acknowledgements"):
        time.sleep(0.5)
        try:
            acktext, _ = DL.get_acknowledgements(row['doi'])
        except Exception as err:
            error.append({"doi": row['doi'], "source": "arxiv", "error": str(err)})
            continue
        if acktext:
            COUNT['arxiv_add'] += 1
            internal.append({"doi": row['doi'],
                             "ack": acktext,
                             "source": "arXiv"})


def doiurl(doi):
    ''' Format a DOI as a DIS UI link
        Keyword arguments:
          doi: DOI to format
        Returns:
          HTML anchor
    '''
    return (f"<a href='https://dis.int.janelia.org/doiui/{doi}' "
            f"style='color:{EMAIL_BLUE};text-decoration:none;'>{doi}</a>")


def html_kpi_card(value, label, tone='neutral', width='20%'):
    ''' Build one KPI stat tile for the run-summary email's header row.
        A single <td> carries the box look directly (bgcolor attribute +
        background-color, no nested table) - Outlook's Word rendering engine
        chokes on a percentage-width table nested inside a percentage-width <td>.
        Keyword arguments:
          value: display value (already formatted, e.g. "3")
          label: caption under the value
          tone: 'neutral', 'good', or 'bad' - selects the tile's color scheme
          width: tile width as a percentage string (tune to the tile count)
        Returns:
          HTML for one table cell
    '''
    bg, fg = {'good': (EMAIL_GREEN_BG, EMAIL_GREEN),
              'bad': (EMAIL_RED_BG, EMAIL_RED),
              'neutral': (EMAIL_GRAY_BG, EMAIL_GRAY)}[tone]
    return (f'<td width="{width}" align="center" valign="top" bgcolor="{bg}" '
            f'style="padding:14px 6px;background-color:{bg};border-radius:8px;">'
            f'<div style="font-size:24px;font-weight:700;color:{fg};">{value}</div>'
            f'<div style="font-size:10.5px;color:{EMAIL_GRAY};text-transform:uppercase;'
            f'letter-spacing:.04em;margin-top:2px;">{label}</div>'
            f'</td>')


def html_section_header(title):
    ''' Build a section header bar for the run-summary email
        Keyword arguments:
          title: section title (may include an HTML entity icon prefix)
        Returns:
          HTML div block
    '''
    return (f'<div style="font-size:14px;font-weight:700;color:{EMAIL_NAVY};'
            f'border-bottom:2px solid {EMAIL_BORDER};padding-bottom:7px;'
            f'margin-bottom:10px;">{title}</div>')


def html_pill(bg, fg, text):
    ''' Build a small colored status badge as an auto-width single-cell table
        (bgcolor attribute + background-color CSS), not a <span> - Outlook's
        Word engine does not honor background-color on inline elements. Only
        safe where the badge is the sole content of its table cell.
        Keyword arguments:
          bg: background color
          fg: text color
          text: pill text (may include an HTML entity icon prefix)
        Returns:
          HTML for a single-cell table sized to its content
    '''
    return (f'<table role="presentation" cellpadding="0" cellspacing="0"><tr>'
            f'<td bgcolor="{bg}" style="background-color:{bg};color:{fg};padding:2px 10px;'
            f'border-radius:10px;font-size:11.5px;font-weight:600;">{text}</td></tr></table>')


def html_source_card(label, records):
    ''' Build one "acknowledgements found" card for a single source: a header
        with a count pill, followed by a zebra-striped table of DOIs (each
        linked to its DIS UI page), with a PMCID column when the source's
        records carry one (PMC only).
        Keyword arguments:
          label: display label (e.g. "eLife")
          records: list of internal-DOI records for this source
        Returns:
          HTML card block
    '''
    has_pmcid = any(rec.get('pmcid') for rec in records)
    rows = []
    for i, rec in enumerate(records):
        striped = i % 2 == 0
        bgattr = f' bgcolor="{EMAIL_STRIPE_BG}"' if striped else ''
        bg = f'background-color:{EMAIL_STRIPE_BG};' if striped else ''
        if has_pmcid:
            doi_radius = 'border-radius:6px 0 0 6px;' if bg else ''
            pmcid_radius = 'border-radius:0 6px 6px 0;' if bg else ''
            pmcid_html = (f'<td style="padding:6px 10px;{pmcid_radius}color:{EMAIL_GRAY};" '
                          f'align="right">{rec["pmcid"]}</td>')
        else:
            doi_radius = 'border-radius:6px;' if bg else ''
            pmcid_html = ''
        rows.append(f'<tr{bgattr} style="{bg}">'
                    f'<td style="padding:6px 10px;{doi_radius}">{doiurl(rec["doi"])}</td>'
                    f'{pmcid_html}</tr>')
    header = ('<td style="padding:5px 10px;">DOI</td>'
              + ('<td style="padding:5px 10px;" align="right">PMCID</td>' if has_pmcid else ''))
    table = ('<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
             'style="border-collapse:collapse;font-size:12.5px;">'
             f'<tr style="color:{EMAIL_GRAY};font-size:10.5px;text-transform:uppercase;'
             f'letter-spacing:.03em;">{header}</tr>' + "".join(rows) + '</table>')
    # Header row is two <td>s directly in the outer table (not a nested
    # width="100%" table inside one <td>) - Outlook's Word engine chokes on
    # that nesting. The body row below spans both with colspan="2".
    return (
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
        f'style="border:1px solid {EMAIL_BORDER};border-radius:8px;margin-bottom:14px;'
        'border-collapse:separate;">'
        f'<tr><td bgcolor="{EMAIL_STRIPE_BG}" style="background-color:{EMAIL_STRIPE_BG};'
        f'padding:10px 16px;border-radius:8px 0 0 0;font-weight:700;color:{EMAIL_NAVY};'
        f'font-size:13.5px;">{label}</td>'
        f'<td bgcolor="{EMAIL_STRIPE_BG}" align="right" style="background-color:'
        f'{EMAIL_STRIPE_BG};padding:10px 16px;border-radius:0 8px 0 0;">'
        + html_pill(EMAIL_GREEN_BG, EMAIL_GREEN, f'&#10003; {len(records):,}') + '</td></tr>'
        f'<tr><td colspan="2" style="padding:4px 16px 10px 16px;">{table}</td></tr></table>')


def html_error_table(error):
    ''' Build the Errors table for the run-summary email
        Keyword arguments:
          error: list of error records (doi, source, error)
        Returns:
          HTML table block
    '''
    rows = []
    for i, err in enumerate(error):
        striped = i % 2 == 0
        bgattr = f' bgcolor="{EMAIL_STRIPE_BG}"' if striped else ''
        bg = f'background-color:{EMAIL_STRIPE_BG};' if striped else ''
        r_l = 'border-radius:6px 0 0 6px;' if bg else ''
        r_r = 'border-radius:0 6px 6px 0;' if bg else ''
        rows.append(
            f'<tr{bgattr} style="{bg}"><td style="padding:6px 10px;{r_l}">'
            f'{doiurl(err["doi"])}</td>'
            f'<td style="padding:6px 10px;">{err["source"]}</td>'
            f'<td style="padding:6px 10px;{r_r}color:{EMAIL_RED};">'
            f'{html.escape(str(err["error"]))}</td></tr>')
    return (
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
        'style="border-collapse:collapse;font-size:12.5px;">'
        f'<tr style="color:{EMAIL_GRAY};font-size:10.5px;text-transform:uppercase;'
        'letter-spacing:.03em;"><td style="padding:6px 10px;">DOI</td>'
        '<td style="padding:6px 10px;">Source</td>'
        '<td style="padding:6px 10px;">Error</td></tr>'
        + "".join(rows) + '</table>')


def generate_email(internal, error):
    ''' Generate and send the HTML run-summary email, grouping DOIs by source
        (eLife/Elsevier/PMC/arXiv) into cards rather than one flat list.
        Keyword arguments:
          internal: list of internal DOIs
          error: list of error records
        Returns:
          None
    '''
    run_data = JRC.get_run_data(__file__, __version__).strip()
    mode_badge_bg = EMAIL_GREEN if ARG.WRITE else EMAIL_AMBER
    mode_label = 'WRITE' if ARG.WRITE else 'DRY RUN'

    by_source = collections.defaultdict(list)
    for rec in internal:
        by_source[rec['source']].append(rec)

    kpis = ''.join(html_kpi_card(f"{len(by_source.get(label, [])):,}", f"{label} added",
                                 'good' if by_source.get(label) else 'neutral')
                   for label in SOURCE_LABELS)
    kpis += html_kpi_card(f"{len(error):,}", "Errors", 'bad' if error else 'neutral')

    cards = ''.join(html_source_card(label, by_source[label])
                    for label in SOURCE_LABELS if by_source.get(label))
    found_section = (
        html_section_header(f"&#128209; Acknowledgements Found ({len(internal):,})")
        + (cards if cards else f'<div style="color:{EMAIL_GRAY};font-size:13px;">'
                                'No new acknowledgements were found.</div>'))

    error_row = ''
    if error:
        error_row = (
            f'<tr><td style="padding:16px 28px 6px 28px;">'
            + html_section_header(f"&#9888; Errors ({len(error):,})")
            + html_error_table(error) + '</td></tr>')

    msg = (
        f'<div style="font-family:-apple-system,\'Segoe UI\',Roboto,Helvetica,Arial,sans-serif;'
        f'background-color:#eef1f4;padding:8px 0;">'
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0"><tr>'
        '<td align="center" style="padding:8px 14px 32px 14px;">'
        '<table role="presentation" width="720" cellpadding="0" cellspacing="0" '
        'bgcolor="#ffffff" '
        f'style="max-width:720px;width:100%;background-color:#ffffff;border-radius:10px;'
        f'border:1px solid {EMAIL_BORDER};overflow:hidden;">'
        f'<tr><td bgcolor="{EMAIL_NAVY}" style="background-color:{EMAIL_NAVY};'
        f'padding:22px 28px;">'
        f'<div style="color:#ffffff;font-size:19px;font-weight:600;">'
        f'{os.path.basename(__file__)}&nbsp;'
        f'<span style="font-weight:400;opacity:.7;font-size:14px;">v{__version__}</span></div>'
        f'<div style="color:#c9d6e6;font-size:12.5px;margin-top:6px;">{run_data} &middot; '
        f'<span style="background-color:{mode_badge_bg};color:#fff;border-radius:10px;'
        f'padding:1px 9px;font-size:11px;font-weight:600;letter-spacing:.03em;">'
        f'{mode_label}</span></div></td></tr>'
        f'<tr><td style="padding:22px 22px 6px 22px;">'
        # cellspacing (not CSS margin, which <td> mostly ignores) puts a real gap
        # between the KPI tiles; Outlook's Word engine honors this old-school
        # HTML attribute far more reliably than CSS spacing tricks.
        f'<table role="presentation" width="100%" cellpadding="0" cellspacing="6"><tr>'
        f'{kpis}</tr></table></td></tr>'
        f'<tr><td style="padding:18px 28px 4px 28px;">{found_section}</td></tr>'
        f'{error_row}'
        f'<tr><td bgcolor="{EMAIL_STRIPE_BG}" style="padding:18px 28px;'
        f'background-color:{EMAIL_STRIPE_BG};color:{EMAIL_GRAY};'
        f'font-size:11px;text-align:center;border-top:1px solid {EMAIL_BORDER};">'
        'Generated by pull_internal_acks.py &middot; Data and Information Services &middot; '
        'Janelia Research Campus</td></tr>'
        '</table></td></tr></table></div>')

    email = DIS['developer']
    subject = "Acknowledgements updated for DOIs"
    JRC.send_email(msg, DIS['sender'], email, subject, mime='html')


def processing():
    ''' Find DOIs without acknowledgements.
        Keyword arguments:
          None
        Returns:
          None
    '''
    internal = []
    error = []
    sources = {'elife': lambda: add_elife_internal_acks(internal, error),
               'elsevier': lambda: add_elsevier_internal_acks(internal, error),
               'pmc': lambda: add_pmc_internal_acks(internal, error),
               'arxiv': lambda: add_arxiv_internal_acks(internal, error)}
    for source, handler in sources.items():
        if ARG.SOURCE in (None, source):
            handler()
    operations = []
    for row in tqdm(internal, total=len(internal), desc="Updating internal DOIs"):
        if not isinstance(row['ack'], str):
            LOGGER.warning(f"Weird format for {row['doi']}")
            continue
        operations.append(UpdateOne({"doi": row['doi']},
                                    {"$set": {"jrc_acknowledgements": row['ack']}}))
    COUNT['updated'] = len(operations)
    if ARG.WRITE and operations:
        # Unordered so one failed update doesn't block the rest of the batch.
        try:
            result = DB['dis']['dois'].bulk_write(operations, ordered=False)
            COUNT['updated'] = result.modified_count
        except BulkWriteError as err:
            # Unordered means every non-failing op in the batch already went
            # through - report the real counts and keep going instead of
            # losing this run's JSON/email output over one bad document.
            write_errors = err.details.get('writeErrors', [])
            COUNT['updated'] = err.details.get('nModified', 0)
            COUNT['write_errors'] = len(write_errors)
            LOGGER.error(f"{len(write_errors):,} of {len(operations):,} updates failed: "
                        f"{write_errors}")
        except Exception as err:
            terminate_program(err)
    if ARG.SOURCE in (None, 'elife'):
        print(f"eLife DOIs added:    {COUNT['elife_add']:,}")
    if ARG.SOURCE in (None, 'elsevier'):
        print(f"Elsevier DOIs added: {COUNT['elsevier_add']:,}")
    if ARG.SOURCE in (None, 'pmc'):
        print(f"PMC DOIs added:      {COUNT['pmc_add']:,}")
    if ARG.SOURCE in (None, 'arxiv'):
        print(f"arXiv DOIs added:    {COUNT['arxiv_add']:,}")
    print(f"DOIs updated:        {COUNT['updated']:,}")
    if COUNT['write_errors']:
        print(f"DOIs failed to update: {COUNT['write_errors']:,}")
    if internal:
        with open('internal_acks.json', 'w', encoding='utf-8') as fileout:
            json.dump(internal, fileout, indent=4)
    if error:
        with open('internal_ack_errors.json', 'w', encoding='utf-8') as fileout:
            json.dump(error, fileout, indent=4)
    if internal:
        generate_email(internal, error)

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Add acknowledgements to internal DOIs")
    PARSER.add_argument('--doi', dest='DOI', default=None,
                        help='Restrict processing to a single DOI')
    PARSER.add_argument('--source', dest='SOURCE', action='store',
                        choices=['elife', 'elsevier', 'pmc', 'arxiv'], default=None,
                        help='Restrict processing to a single source [all]')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Flag, Update database')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    DIS = JRC.simplenamespace_to_dict(JRC.get_config("dis"))
    initialize_program()
    processing()
    terminate_program()
