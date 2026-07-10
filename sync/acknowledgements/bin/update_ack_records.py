''' update_ack_records.py

PURPOSE
-------
Backfills metadata fields on DOI records that already have acknowledgement text,
for both the DIS "external_dois" and "dois" collections:
- external_dois: jrc_journal, title, jrc_ack_first_author, jrc_ack_last_author,
                 is_preprint, type
- dois (only records with jrc_acknowledgements): jrc_ack_first_author,
                 jrc_ack_last_author
Missing fields are filled in from a fresh CrossRef/DataCite metadata lookup.
A record whose fetch still can't resolve every field (a genuine preprint has
no journal; some records have no derivable author) is marked
jrc_ack_backfill_checked so future runs don't re-fetch it forever.

INPUTS
------
- DIS MongoDB database (read/write depending on --write flag).
- Command-line flags:
    --manifold  MongoDB manifold (dev or prod; default prod).
    --write     Actually update the database (default: dry-run).
    --verbose   Increase logging verbosity.
    --debug     Maximum logging verbosity.

EMAIL RECIPIENT
----------------
The summary email always goes to the configured developer address, never the
full receivers list (same convention as pull_internal_acks.py/
pull_external_acks.py), and is sent any time a record was updated - --write or
not (a dry run's "would update" findings are just as worth seeing as a real
run's).

HIGH-LEVEL FLOW
---------------
1. Initialization
   - Connects to the DIS MongoDB database.
2. External DOIs (process_external)
   - Scans every external_dois record; records missing any of jrc_journal,
     title, jrc_ack_first_author, jrc_ack_last_author, is_preprint, or type
     are re-fetched from CrossRef (or DataCite, for DataCite DOIs) and updated.
     A record already marked jrc_ack_backfill_checked is skipped outright.
3. Internal DOIs (process_internal)
   - Scans dois records that have jrc_acknowledgements; records missing
     jrc_ack_first_author or jrc_ack_last_author are re-fetched and updated.
     Same jrc_ack_backfill_checked short-circuit as external.
   - A DOI whose fetched record still yields no updatable field (e.g. an eLife
     peer-review sub-document - a decision letter or author response - which
     carries no normal author list, so no first/last author can be derived)
     gets jrc_ack_backfill_checked=True $set instead of the missing field, so
     it is not re-attempted on every future run.
4. Output
   - Prints a per-collection summary of counts.
   - Writes update_ack_records.json with one entry per updated DOI (doi,
     collection, and the fields payload that was $set) - written in both
     --write and dry-run modes, since dry-run's "would update" records are the
     same payloads a real run would set.
   - Sends a summary email whenever at least one record was updated, --write
     or not: a header banner (run data, manifold, DRY RUN/WRITE
     badge), KPI stat tiles (records updated/errors per collection), and a
     funnel card per collection (read -> already complete -> fetch errors ->
     marked unresolvable -> updated). Built entirely from inline styles/tables
     (no <style> block) for compatibility with older email clients, matching
     the convention used by sync_citations.py / pull_internal_acks.py /
     pull_external_acks.py.

DEPENDENCIES
------------
- jrc_common.jrc_common  (JRC): logging, config, database, and email helpers.
- doi_common.doi_common  (DL): author-detail extraction, preprint/journal/title
                               helpers.
- tqdm: progress bars.
'''

__version__ = '1.2.0'

import argparse
import collections
import json
from operator import attrgetter
import os
import sys
import time
from tqdm import tqdm
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Database
DB = {}
# Counters
COUNT = collections.defaultdict(lambda: 0, {})
# Per-DOI update records (doi, collection, fields payload), written to
# update_ack_records.json
RECORDS = []
# Global variables
ARG = DISCONFIG = LOGGER = None
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
    try:
        dbconfig = JRC.get_config("databases")
    except Exception as err:
        terminate_program(err)
    dbs = ['dis']
    for source in dbs:
        dbo = attrgetter(f"{source}.{ARG.MANIFOLD}.write")(dbconfig)
        LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, ARG.MANIFOLD, dbo.host, dbo.user)
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)


def get_first_last_authors(rec, payload):
    ''' Get first and last authors from a record
        Keyword arguments:
          rec: record
          payload: payload to update
        Returns:
          None
    '''
    authors = DL.get_author_details(rec, DB['dis'].orcid)
    first = []
    last = []
    for auth in authors:
        if 'is_first' in auth and auth['is_first']:
            if 'family' in auth and 'given' in auth:
                first.append(', '.join([auth['family'], auth['given']]))
            else:
                first.append(auth['name'])
        if 'is_last' in auth and auth['is_last']:
            if 'family' in auth and 'given' in auth:
                last.append(', '.join([auth['family'], auth['given']]))
            else:
                last.append(auth['name'])
    if first:
        payload['jrc_ack_first_author'] = first
    if last:
        if len(last) > 1:
            # jrc_ack_last_author is a single string (unlike jrc_ack_first_author,
            # which is a list) - co-last-authorship is rare but real. Rather than
            # abort the entire run over one messy record, keep the first one
            # found and log the rest for manual follow-up.
            LOGGER.warning(f"Multiple last authors for {rec['doi']}: {last} - using {last[0]!r}")
        payload['jrc_ack_last_author'] = last[0]


def process_external():
    ''' Process external DOIs
        Keyword arguments:
          None
        Returns:
          None
    '''
    try:
        cnt = DB['dis'].external_dois.count_documents({})
        rows = DB['dis'].external_dois.find({})
    except Exception as err:
        terminate_program(err)
    LOGGER.info(f"Found {cnt:,} records in external_dois")
    COUNT['external_read'] = cnt
    required = ['jrc_journal', 'title', 'jrc_ack_first_author', 'jrc_ack_last_author',
               'is_preprint', 'type']
    for row in tqdm(rows, total=cnt, desc="External"):
        if row.get('jrc_ack_backfill_checked'):
            COUNT['external_ok'] += 1
            continue
        missing = False
        for field in required:
            if field not in row:
                LOGGER.debug(f"Missing {field} for {row['doi']}")
                missing = True
        if not missing:
            COUNT['external_ok'] += 1
            continue
        try:
            if DL.is_datacite(row['doi']):
                rec = JRC.call_datacite(row['doi'])
                if rec:
                    rec = rec['data']['attributes']
            else:
                rec = JRC.call_crossref(row['doi'])
                if rec:
                    rec = rec['message']
            time.sleep(.7)
        except Exception as err:
            LOGGER.warning(err)
            COUNT['external_error'] += 1
            continue
        if not rec:
            LOGGER.warning(f"Could not find record for {row['doi']}")
            COUNT['external_error'] += 1
            continue
        rec['doi'] = row['doi']
        payload = {}
        is_pp = DL.is_preprint(rec)
        payload['is_preprint'] = is_pp
        for transfer in ['type', 'subtype']:
            if rec.get(transfer):
                payload[transfer] = rec[transfer]
        jrn = DL.get_journal(rec, name_only=True)
        if jrn:
            payload['jrc_journal'] = jrn
        ttl = DL.get_title(rec)
        if ttl:
            payload['title'] = ttl
        get_first_last_authors(rec, payload)
        # If this fetch still leaves one of the required fields unresolved (a
        # genuine preprint has no journal; some records have no derivable
        # title/authors), mark the record so future runs don't re-fetch it
        # forever instead of re-attempting an unresolvable lookup every run.
        merged = {**row, **payload}
        if any(field not in merged for field in required):
            payload['jrc_ack_backfill_checked'] = True
            COUNT['external_marked_checked'] += 1
        if ARG.WRITE:
            try:
                result = DB['dis'].external_dois.update_one({"doi": row['doi']}, {"$set": payload})
                if hasattr(result, 'modified_count') and result.modified_count:
                    COUNT['external_dois_written'] += 1
                    RECORDS.append({'doi': row['doi'], 'collection': 'external_dois',
                                    'fields': payload})
            except Exception as err:
                terminate_program(err)
        else:
            COUNT['external_dois_written'] += 1
            RECORDS.append({'doi': row['doi'], 'collection': 'external_dois',
                            'fields': payload})


def process_internal():
    ''' Process internal DOIs
        Keyword arguments:
          None
        Returns:
          None
    '''
    query = {"jrc_acknowledgements": {"$exists": True}}
    try:
        cnt = DB['dis'].dois.count_documents(query)
        rows = DB['dis'].dois.find(query)
    except Exception as err:
        terminate_program(err)
    LOGGER.info(f"Found {cnt:,} records in internal_dois")
    COUNT['internal_read'] = cnt
    required = ['jrc_ack_first_author', 'jrc_ack_last_author']
    for row in tqdm(rows, total=cnt, desc="Internal"):
        if row.get('jrc_ack_backfill_checked'):
            COUNT['internal_ok'] += 1
            continue
        missing = False
        for field in required:
            if field not in row:
                LOGGER.debug(f"Missing {field} for {row['doi']}")
                missing = True
        if not missing:
            COUNT['internal_ok'] += 1
            continue
        try:
            if DL.is_datacite(row['doi']):
                rec = JRC.call_datacite(row['doi'])
                if rec:
                    rec = rec['data']['attributes']
            else:
                rec = JRC.call_crossref(row['doi'])
                if rec:
                    rec = rec['message']
            time.sleep(.7)
        except Exception as err:
            LOGGER.warning(err)
            COUNT['internal_error'] += 1
            continue
        if not rec:
            LOGGER.warning(f"Could not find record for {row['doi']}")
            COUNT['internal_error'] += 1
            continue
        rec['doi'] = row['doi']
        payload = {}
        get_first_last_authors(rec, payload)
        # No author flagged is_first/is_last was found (e.g. an eLife
        # peer-review sub-document, which carries no normal author list) -
        # mark the record so future runs don't re-fetch it forever.
        merged = {**row, **payload}
        if any(field not in merged for field in required):
            payload['jrc_ack_backfill_checked'] = True
            COUNT['internal_marked_checked'] += 1
        if ARG.WRITE:
            try:
                result = DB['dis'].dois.update_one({"doi": row['doi']}, {"$set": payload})
                if hasattr(result, 'modified_count') and result.modified_count:
                    COUNT['internal_dois_written'] += 1
                    RECORDS.append({'doi': row['doi'], 'collection': 'dois', 'fields': payload})
            except Exception as err:
                terminate_program(err)
        else:
            COUNT['internal_dois_written'] += 1
            RECORDS.append({'doi': row['doi'], 'collection': 'dois', 'fields': payload})


def html_kpi_card(value, label, tone='neutral'):
    ''' Build one KPI stat tile for the run-summary email's header row.
        A single <td> carries the box look directly (bgcolor attribute +
        background-color, no nested table) - Outlook's Word rendering engine
        chokes on a percentage-width table nested inside a percentage-width <td>.
        Keyword arguments:
          value: display value (already formatted, e.g. "3")
          label: caption under the value
          tone: 'neutral', 'good', or 'bad' - selects the tile's color scheme
        Returns:
          HTML for one table cell
    '''
    bg, fg = {'good': (EMAIL_GREEN_BG, EMAIL_GREEN),
              'bad': (EMAIL_RED_BG, EMAIL_RED),
              'neutral': (EMAIL_GRAY_BG, EMAIL_GRAY)}[tone]
    return (f'<td width="25%" align="center" valign="top" bgcolor="{bg}" '
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


def html_metric_rows(rows):
    ''' Build a zebra-striped label/value table for the run-summary email
        Keyword arguments:
          rows: list of (label, value_html) pairs
        Returns:
          HTML table
    '''
    trs = []
    for i, (mlabel, value) in enumerate(rows):
        striped = i % 2 == 0
        bgattr = f' bgcolor="{EMAIL_STRIPE_BG}"' if striped else ''
        bg = f'background-color:{EMAIL_STRIPE_BG};' if striped else ''
        r_l = 'border-radius:6px 0 0 6px;' if bg else ''
        r_r = 'border-radius:0 6px 6px 0;' if bg else ''
        trs.append(f'<tr{bgattr} style="{bg}">'
                   f'<td style="padding:8px 10px;{r_l}">{mlabel}</td>'
                   f'<td align="right" style="padding:8px 10px;text-align:right;{r_r}">'
                   f'{value}</td></tr>')
    return ('<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
            'style="border-collapse:collapse;font-size:13px;margin-top:6px;">'
            + "".join(trs) + '</table>')


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


def html_funnel_card(label, prefix):
    ''' Build one collection's Backfill Funnel card: how many records read were
        winnowed down to records actually updated, as a metric-rows table, plus
        an "updated" count pill in the header.
        Keyword arguments:
          label: display label (e.g. "External DOIs")
          prefix: COUNT key prefix for this collection ('external' or 'internal')
        Returns:
          HTML card block
    '''
    rows = [("Read", f"{COUNT[f'{prefix}_read']:,}"),
            ("Already complete", f"{COUNT[f'{prefix}_ok']:,}")]
    if COUNT[f'{prefix}_error']:
        rows.append(("Fetch errors", f"{COUNT[f'{prefix}_error']:,}"))
    if COUNT[f'{prefix}_marked_checked']:
        rows.append(("Marked unresolvable (won't retry)",
                     f"{COUNT[f'{prefix}_marked_checked']:,}"))
    updated = COUNT[f'{prefix}_dois_written']
    pill = html_pill(EMAIL_GREEN_BG, EMAIL_GREEN, f'&#10003; {updated:,} updated')
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
        f'{EMAIL_STRIPE_BG};padding:10px 16px;border-radius:0 8px 0 0;">{pill}</td></tr>'
        f'<tr><td colspan="2" style="padding:4px 16px 10px 16px;">'
        f'{html_metric_rows(rows)}</td></tr></table>')


def generate_email():
    ''' Generate and send the HTML run-summary email. Built directly from the
        module-level COUNT/ARG state (same convention as sync_citations.py),
        grouped into a funnel card per collection (external_dois / dois)
        instead of a flat print of six counters.
        Keyword arguments:
          None
        Returns:
          None
    '''
    run_data = JRC.get_run_data(__file__, __version__).strip()
    mode_badge_bg = EMAIL_GREEN if ARG.WRITE else EMAIL_AMBER
    mode_label = 'WRITE' if ARG.WRITE else 'DRY RUN'

    kpis = ''.join([
        html_kpi_card(f"{COUNT['external_dois_written']:,}", "External updated",
                      'good' if COUNT['external_dois_written'] else 'neutral'),
        html_kpi_card(f"{COUNT['internal_dois_written']:,}", "Internal updated",
                      'good' if COUNT['internal_dois_written'] else 'neutral'),
        html_kpi_card(f"{COUNT['external_error']:,}", "External errors",
                      'bad' if COUNT['external_error'] else 'neutral'),
        html_kpi_card(f"{COUNT['internal_error']:,}", "Internal errors",
                      'bad' if COUNT['internal_error'] else 'neutral'),
    ])

    funnel_section = (
        html_section_header("&#128200; Backfill Funnel")
        + html_funnel_card("External DOIs", 'external')
        + html_funnel_card("Internal DOIs", 'internal'))

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
        f'{mode_label}</span> &middot; manifold: {ARG.MANIFOLD}</div></td></tr>'
        f'<tr><td style="padding:22px 22px 6px 22px;">'
        # cellspacing (not CSS margin, which <td> mostly ignores) puts a real gap
        # between the KPI tiles; Outlook's Word engine honors this old-school
        # HTML attribute far more reliably than CSS spacing tricks.
        f'<table role="presentation" width="100%" cellpadding="0" cellspacing="6"><tr>'
        f'{kpis}</tr></table></td></tr>'
        f'<tr><td style="padding:18px 28px 4px 28px;">{funnel_section}</td></tr>'
        f'<tr><td bgcolor="{EMAIL_STRIPE_BG}" style="padding:18px 28px;'
        f'background-color:{EMAIL_STRIPE_BG};color:{EMAIL_GRAY};'
        f'font-size:11px;text-align:center;border-top:1px solid {EMAIL_BORDER};">'
        'Generated by update_ack_records.py &middot; Data and Information Services &middot; '
        'Janelia Research Campus</td></tr>'
        '</table></td></tr></table></div>')

    email = DISCONFIG['developer']
    LOGGER.info(f"Sending email to {email}")
    JRC.send_email(msg, DISCONFIG['sender'], email,
                   "Acknowledgement record fields updated", mime='html')


def processing():
    ''' Main processing routine
        Keyword arguments:
          None
        Returns:
          None
    '''
    process_external()
    process_internal()
    print(f"External records read:    {COUNT['external_read']:,}")
    print(f"External records ok:      {COUNT['external_ok']:,}")
    if COUNT['external_error']:
        print(f"External records errors: {COUNT['external_error']:,}")
    if COUNT['external_marked_checked']:
        print(f"External records marked unresolvable (won't retry): "
              f"{COUNT['external_marked_checked']:,}")
    print(f"External records updated: {COUNT['external_dois_written']:,}")
    print(f"Internal records read:    {COUNT['internal_read']:,}")
    print(f"Internal records ok:      {COUNT['internal_ok']:,}")
    if COUNT['internal_error']:
        print(f"Internal records errors: {COUNT['internal_error']:,}")
    if COUNT['internal_marked_checked']:
        print(f"Internal records marked unresolvable (won't retry): "
              f"{COUNT['internal_marked_checked']:,}")
    print(f"Internal records updated: {COUNT['internal_dois_written']:,}")
    if RECORDS:
        jfile = 'update_ack_records.json'
        with open(jfile, 'w', encoding='utf-8') as fileout:
            json.dump(RECORDS, fileout, indent=4)
        LOGGER.info(f"Wrote {len(RECORDS):,} records to {jfile}")
    if COUNT['external_dois_written'] or COUNT['internal_dois_written']:
        generate_email()


# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Check external_dois for jrc_journal field")
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, prod)')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Write to database')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    DISCONFIG = JRC.simplenamespace_to_dict(JRC.get_config("dis"))
    initialize_program()
    processing()
    terminate_program()
