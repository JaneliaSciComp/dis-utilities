""" sync_openalex.py
    Sync work data from OpenAlex. DOIs will almost always make it into the local database before
    they're present in OpenAlex.
    Data brought in from OpenAlex:
      open_access.is_oa -> jrc_is_oa
      open_access.oa_status -> jrc_oa_status
      primary_location.license -> jrc_license
    This program will also look for DOIs with a "closed" Open Access status to override. If
    the OA status is "closed" and the DOI has a fulltext URL, the OA status will be set to
    "hybrid" and jrc_is_oa will be set to True. The former status will be saved as
    jrc_former_status.

    An HTML summary email is sent (when --test or --write is supplied and at least one DOI
    was updated): a header banner (run data, DRY RUN/WRITE badge), KPI stat tiles, and two
    Change Breakdown tables - one per pass (OA/license enrichment; OA-status override) -
    with sync_openalex.json attached.
"""

__version__ = '4.2.0'

import argparse
import collections
from datetime import datetime
import html
import json
from operator import attrgetter
import os
import sys
import time
import traceback
from tqdm import tqdm
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL
import dis_license_lib as DISL

# pylint: disable=broad-exception-caught,logging-fstring-interpolation,logging-not-lazy,duplicate-code

ARG = DISCONFIG = LOGGER = None
DB = {}
LICENSE = {}
JOURNAL = {}
OUTPUT = []
COUNT = collections.defaultdict(lambda: 0, {})
# Unique raw license strings that couldn't be mapped ("Unknown ... license"),
# excluding bare URLs (http/https...), for triage - shown in the console and email.
UNKNOWN_LICENSES = set()
# HTML run-summary email palette (generate_email and its html_* helpers). Mirrors
# the sibling sync scripts: inline styles only (no <style> block/classes) for
# reliable rendering across email clients including older Outlook.
EMAIL_NAVY = '#1f3a5f'
EMAIL_GREEN = '#1c7c3f'
EMAIL_GREEN_BG = '#eefaf1'
EMAIL_RED = '#c0392b'
EMAIL_RED_BG = '#fdecea'
EMAIL_AMBER = '#d68a1f'
EMAIL_AMBER_BG = '#fdf3e0'
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
    try:
        rows = DB['dis'].cvterm.find({'cv': 'license_mapping'})
    except Exception as err:
        terminate_program(err)
    for row in rows:
        LICENSE[row['name']] = row['display']
    try:
        rows = DB['dis'].cvterm.find({'cv': 'license'})
    except Exception as err:
        terminate_program(err)
    for row in rows:
        if row['name'] not in LICENSE:
            LICENSE[row['name']] = row['name']
    try:
        rows = DB['dis'].subscription.find({"oa_status": {"$exists": True}})
    except Exception as err:
        terminate_program(err)
    for row in rows:
        JOURNAL[row['title']] = row['oa_status']


def get_dois():
    """ Get a list of DOIs to process
        Keyword arguments:
          None
        Returns:
          List of DOIs
    """
    dois = []
    if ARG.DOI:
        dois.append(ARG.DOI.lower().strip())
    elif ARG.FILE:
        try:
            with open(ARG.FILE, "r", encoding="ascii") as instream:
                for doi in instream.read().splitlines():
                    dois.append(doi.lower().strip())
        except Exception as err:
            LOGGER.error(f"Could not process {ARG.FILE}")
            terminate_program(err)
    return dois


def update_processing(doi, action, notes=None):
    ''' Update the processing status for a DOI
        Keyword arguments:
          doi: DOI to update
          notes: notes to add to the processing record
        Returns:
          None
    '''
    proc = {'action': action,
            'program': os.path.basename(__file__),
            'version': __version__,
            'timestamp': datetime.now().isoformat()}
    if notes is not None:
        proc['notes'] = notes
    if not ARG.WRITE:
        return
    try:
        DB['dis'].processing.update_one({'type': 'doi', 'key': doi},
                                        {'$push': {'processes': proc}}, upsert=True)
    except Exception as err:
        terminate_program(err)


def update_open_access(row):  # pylint: disable=too-many-branches,too-many-statements
    """ Update jrc_is_oa, jrc_oa_status, and jrc_license
        Keyword arguments:
          row: row to update from dois collection
        Returns:
          None
    """
    payload = {}
    if not row.get('jrc_oa_status') and row.get('jrc_journal') and row['jrc_journal'] in JOURNAL:
        row['jrc_is_oa'] = True
        payload['jrc_is_oa'] = True
        row['jrc_oa_status'] = JOURNAL[row['jrc_journal']]
        payload['jrc_oa_status'] = JOURNAL[row['jrc_journal']]
        LOGGER.warning(f"Using journal OA status {row['jrc_oa_status']} for {row['doi']}")
    time.sleep(.05)
    data, openalex_unreachable = DISL.get_openalex_record(row['doi'])
    if openalex_unreachable:
        COUNT['openalex_unreachable'] += 1
    elif not data:
        if not ARG.SILENT:
            LOGGER.warning(f"{row['doi']} was not found in OpenAlex")
        COUNT["notfound"] += 1
    else:
        # Guard the open_access block with .get() so a missing key can't KeyError,
        # and treat any other malformed-record error as a skip+count for this DOI
        # rather than aborting the whole run.
        try:
            oa = data.get('open_access') or {}
            if 'jrc_is_oa' not in row and 'is_oa' in oa:
                payload['jrc_is_oa'] = bool(oa['is_oa'])
            if 'jrc_oa_status' not in row and oa.get('oa_status'):
                payload['jrc_oa_status'] = oa['oa_status']
        except Exception as err:
            LOGGER.error(f"Could not process OpenAlex data for {row['doi']}: {err}")
            COUNT['openalex_error'] += 1
            return
    # License -- tries DataCite rightsList, OpenAlex (reusing the fetch above), PMC, Unpaywall
    if not row.get('jrc_license'):
        result = DISL.resolve_license(row, LICENSE, openalex_data=data)
        if result.mapped:
            payload['jrc_license'] = result.mapped
        if result.pmc_skipped_no_id:
            COUNT['pmc_skipped_no_id'] += 1
        if result.pmc_429_exhausted:
            COUNT['pmc_429_exhausted'] += 1
        if result.pmc_unreachable:
            COUNT['pmc_unreachable'] += 1
        if result.unpaywall_not_indexed:
            COUNT['unpaywall_not_indexed'] += 1
        if result.unpaywall_unreachable:
            COUNT['unpaywall_unreachable'] += 1
        # Collect unmappable license slugs for triage, skipping bare URLs (http/https...)
        for lic in result.unknown_licenses:
            if lic and not lic.startswith('http'):
                UNKNOWN_LICENSES.add(lic)
    if not payload:
        return
    if data and data.get('id'):
        payload['jrc_openalex_id'] = data['id']
    write_record(row, payload)
    notes = "update_openalex"
    if payload.get('jrc_is_oa'):
        notes += f" OA: {payload.get('jrc_is_oa')}"
    if payload.get('jrc_oa_status'):
        notes += f" Status: {payload.get('jrc_oa_status')}"
    if payload.get('jrc_license'):
        notes += f" License: {payload.get('jrc_license')}"
    update_processing(row['doi'], 'sync_openaccess_license', notes)


def write_record(row, payload):
    """ Write record to database
        Keyword arguments:
          row: record to write
          payload: data to add/update
        Returns:
          None
    """
    if ARG.WRITE:
        try:
            DB['dis']['dois'].update_one({"doi": row['doi']}, {"$set": payload})
        except Exception as err:
            terminate_program(err)
    payload['doi'] = row['doi']
    OUTPUT.append(payload)
    COUNT["updated"] += 1


def override_oa_closed(row):
    """ Override OA closed status
        Keyword arguments:
          row: row to update
        Returns:
          None
    """
    payload = {'jrc_former_status': row['jrc_oa_status'],
               'jrc_is_oa': True}
    payload['jrc_oa_status'] = "hybrid"
    if ARG.WRITE:
        try:
            DB['dis']['dois'].update_one({"doi": row['doi']}, {"$set": payload})
        except Exception as err:
            terminate_program(err)
    payload['doi'] = row['doi']
    OUTPUT.append(payload)
    COUNT["updated"] += 1


def show_counts():
    """ Show the counts
        Keyword arguments:
          None
        Returns:
          None
    """
    msg = f"DOIs read:      {COUNT['dois']:,}\n"
    if COUNT['notfound']:
        msg += f"DOIs not found: {COUNT['notfound']:,}\n"
    if COUNT['pmc_skipped_no_id']:
        msg += f"DOIs with no PMC ID:            {COUNT['pmc_skipped_no_id']:,}\n"
    if COUNT['pmc_429_exhausted']:
        msg += f"DOIs with PMC 429 exhausted:    {COUNT['pmc_429_exhausted']:,}\n"
    if COUNT['pmc_unreachable']:
        msg += f"DOIs skipped (PMC down):        {COUNT['pmc_unreachable']:,}\n"
    if COUNT['openalex_unreachable']:
        msg += f"DOIs skipped (OpenAlex down):   {COUNT['openalex_unreachable']:,}\n"
    if COUNT['openalex_error']:
        msg += f"DOIs with OpenAlex parse error: {COUNT['openalex_error']:,}\n"
    if COUNT['unpaywall_not_indexed']:
        msg += f"DOIs not indexed in Unpaywall:  {COUNT['unpaywall_not_indexed']:,}\n"
    if COUNT['unpaywall_unreachable']:
        msg += f"DOIs skipped (Unpaywall down):  {COUNT['unpaywall_unreachable']:,}\n"
    if COUNT['updated']:
        msg += f"DOIs updated:   {COUNT['updated']:,}\n"
    return msg


def html_kpi_card(value, label, tone='neutral'):
    ''' Build one KPI stat tile for the run-summary email's header row. A single
        <td> carries the box look directly (bgcolor + background-color, no nested
        table) - Outlook's Word engine chokes on a percentage-width table nested
        in a percentage-width <td>.
        Keyword arguments:
          value: display value (already formatted)
          label: caption under the value
          tone: 'neutral', 'good', 'amber', or 'bad' - selects the color scheme
        Returns:
          HTML for one table cell
    '''
    bg, fg = {'good': (EMAIL_GREEN_BG, EMAIL_GREEN),
              'bad': (EMAIL_RED_BG, EMAIL_RED),
              'amber': (EMAIL_AMBER_BG, EMAIL_AMBER),
              'neutral': (EMAIL_GRAY_BG, EMAIL_GRAY)}[tone]
    return (f'<td width="25%" align="center" valign="top" bgcolor="{bg}" '
            f'style="padding:14px 6px;background-color:{bg};border-radius:8px;">'
            f'<div style="font-size:24px;font-weight:700;color:{fg};">{value}</div>'
            f'<div style="font-size:10.5px;color:{EMAIL_GRAY};text-transform:uppercase;'
            f'letter-spacing:.04em;margin-top:2px;">{label}</div>'
            f'</td>')


def html_section_header(title):
    ''' Build a section header bar. Table-based (not a bare <div>) so it never
        sits as a naked div immediately before a sibling <table> in the same <td>
        - Outlook's Word engine can misparse that boundary and leak a stray
        closing tag as literal text. The spacer row substitutes for CSS
        margin-bottom, which <td> doesn't honor.
        Keyword arguments:
          title: section title (may include an HTML entity icon prefix)
        Returns:
          HTML table block
    '''
    return ('<table role="presentation" width="100%" cellpadding="0" cellspacing="0">'
            f'<tr><td style="font-size:14px;font-weight:700;color:{EMAIL_NAVY};'
            f'border-bottom:2px solid {EMAIL_BORDER};padding-bottom:7px;">'
            f'{title}</td></tr>'
            '<tr><td style="height:10px;line-height:10px;font-size:1px;">&nbsp;</td></tr>'
            '</table>')


def html_metric_rows(rows):
    ''' Build a zebra-striped label/value table. No per-cell border-radius:
        Outlook's Word engine can leak a cell's opening tag as literal text in
        tables repeating the same complex inline style across many rows, so cells
        use plain background-color striping. A trailing spacer row absorbs the
        last-row-before-</table> boundary (see html_section_header).
        Keyword arguments:
          rows: list of (label, value) pairs
        Returns:
          HTML table
    '''
    trs = []
    for i, (mlabel, value) in enumerate(rows):
        bgattr = f' bgcolor="{EMAIL_STRIPE_BG}"' if i % 2 == 0 else ''
        bg = f'background-color:{EMAIL_STRIPE_BG};' if i % 2 == 0 else ''
        trs.append(f'<tr{bgattr} style="{bg}">'
                   f'<td style="padding:8px 10px;">{mlabel}</td>'
                   f'<td align="right" style="padding:8px 10px;text-align:right;">'
                   f'{value}</td></tr>')
    trs.append('<tr><td colspan="2" style="height:1px;line-height:1px;font-size:1px;">'
               '&nbsp;</td></tr>')
    return ('<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
            'style="border-collapse:collapse;font-size:13px;margin-top:6px;">'
            + "".join(trs) + '</table>')


def html_license_list(licenses):
    ''' Build a single-column table of unmapped license strings (monospace),
        with no row striping. Trailing spacer row per the Outlook-safe convention
        (see html_metric_rows).
        Keyword arguments:
          licenses: sorted list of license strings
        Returns:
          HTML table
    '''
    rows = []
    for lic in licenses:
        rows.append('<tr><td style="padding:6px 10px;'
                    f'font-family:Menlo,Consolas,monospace;">{html.escape(lic)}</td></tr>')
    rows.append('<tr><td style="height:1px;line-height:1px;font-size:1px;">&nbsp;</td></tr>')
    return ('<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
            'style="border-collapse:collapse;font-size:12.5px;">'
            + "".join(rows) + '</table>')


def generate_email(pass1, pass2, unknown_licenses):
    ''' Generate and send the HTML run-summary email: a header banner (run data,
        DRY RUN/WRITE badge), KPI stat tiles, two Change Breakdown tables (one
        for the OA/license pass and one for the OA-status-override pass), and -
        when any were found - an Unmapped Licenses list. Built entirely from
        inline styles/tables (no <style> block) for compatibility with older
        email clients, matching the sibling sync scripts.
        Keyword arguments:
          pass1: counts snapshot from the OA/license pass
          pass2: counts snapshot from the OA-status-override pass
          unknown_licenses: sorted list of unmapped license strings (may be empty)
        Returns:
          None
    '''
    run_data = JRC.get_run_data(__file__, __version__).strip()
    mode_badge_bg = EMAIL_GREEN if ARG.WRITE else EMAIL_AMBER
    mode_label = 'WRITE' if ARG.WRITE else 'DRY RUN'
    if ARG.DOI:
        restrict = f' &middot; doi: {ARG.DOI}'
    elif ARG.FILE:
        restrict = f' &middot; file: {os.path.basename(ARG.FILE)}'
    elif ARG.NEW:
        restrict = ' &middot; --new'
    else:
        restrict = ''
    kpis = ''.join([
        html_kpi_card(f"{pass1.get('dois', 0):,}", "DOIs read"),
        html_kpi_card(f"{pass1.get('updated', 0):,}", "OA/license set",
                      'good' if pass1.get('updated') else 'neutral'),
        html_kpi_card(f"{pass2.get('updated', 0):,}", "OA overrides",
                      'good' if pass2.get('updated') else 'neutral'),
        html_kpi_card(f"{pass1.get('notfound', 0):,}", "Not in OpenAlex",
                      'amber' if pass1.get('notfound') else 'neutral'),
    ])
    oa_rows = [
        ("DOIs read", f"{pass1.get('dois', 0):,}"),
        ("OA / license set", f"{pass1.get('updated', 0):,}"),
        ("Not found in OpenAlex", f"{pass1.get('notfound', 0):,}"),
        ("OpenAlex parse errors", f"{pass1.get('openalex_error', 0):,}"),
        ("OpenAlex unreachable", f"{pass1.get('openalex_unreachable', 0):,}"),
        ("No PMC ID", f"{pass1.get('pmc_skipped_no_id', 0):,}"),
        ("PMC 429 exhausted", f"{pass1.get('pmc_429_exhausted', 0):,}"),
        ("PMC unreachable", f"{pass1.get('pmc_unreachable', 0):,}"),
        ("Unpaywall not indexed", f"{pass1.get('unpaywall_not_indexed', 0):,}"),
        ("Unpaywall unreachable", f"{pass1.get('unpaywall_unreachable', 0):,}"),
    ]
    override_rows = [
        ("DOIs read", f"{pass2.get('dois', 0):,}"),
        ("Closed &rarr; hybrid overrides", f"{pass2.get('updated', 0):,}"),
    ]
    oa_section = (html_section_header("&#128220; Open Access / License")
                  + html_metric_rows(oa_rows))
    override_section = (html_section_header("&#128260; OA Status Override")
                        + html_metric_rows(override_rows))
    unknown_block = ''
    if unknown_licenses:
        unknown_block = (
            '<tr><td style="padding:20px 28px 4px 28px;">'
            + html_section_header(f"&#10067; Unmapped Licenses ({len(unknown_licenses):,})")
            + html_license_list(unknown_licenses)
            + '</td></tr>')
    msg = (
        f'<div style="font-family:-apple-system,\'Segoe UI\',Roboto,Helvetica,Arial,sans-serif;'
        f'background-color:#eef1f4;padding:8px 0;">'
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0"><tr>'
        '<td align="center" style="padding:8px 14px 32px 14px;">'
        '<table role="presentation" width="720" cellpadding="0" cellspacing="0" bgcolor="#ffffff" '
        f'style="max-width:720px;width:100%;background-color:#ffffff;border-radius:10px;'
        f'border:1px solid {EMAIL_BORDER};overflow:hidden;">'
        f'<tr><td bgcolor="{EMAIL_NAVY}" style="background-color:{EMAIL_NAVY};padding:22px 28px;">'
        f'<div style="color:#ffffff;font-size:19px;font-weight:600;">'
        f'{os.path.basename(__file__)}&nbsp;'
        f'<span style="font-weight:400;opacity:.7;font-size:14px;">v{__version__}</span></div>'
        f'<div style="color:#c9d6e6;font-size:12.5px;margin-top:6px;">{run_data} &middot; '
        f'manifold: {ARG.MANIFOLD}{restrict} &middot; '
        f'<span style="background-color:{mode_badge_bg};color:#fff;border-radius:10px;'
        f'padding:1px 9px;font-size:11px;font-weight:600;letter-spacing:.03em;">'
        f'{mode_label}</span></div></td></tr>'
        f'<tr><td style="padding:22px 22px 6px 22px;">'
        f'<table role="presentation" width="100%" cellpadding="0" cellspacing="6"><tr>'
        f'{kpis}</tr></table></td></tr>'
        f'<tr><td style="padding:18px 28px 4px 28px;">{oa_section}</td></tr>'
        f'<tr><td style="padding:20px 28px 4px 28px;">{override_section}</td></tr>'
        f'{unknown_block}'
        f'<tr><td bgcolor="{EMAIL_STRIPE_BG}" style="padding:18px 28px;'
        f'background-color:{EMAIL_STRIPE_BG};color:{EMAIL_GRAY};'
        f'font-size:11px;text-align:center;border-top:1px solid {EMAIL_BORDER};">'
        'Generated by sync_openalex.py &middot; Data and Information Services '
        '&middot; Janelia Research Campus</td></tr>'
        '</table></td></tr></table></div>')
    subject = "OpenAlex OA/license sync"
    email = DISCONFIG['developer'] if ARG.TEST else DISCONFIG['receivers']
    attach = 'sync_openalex.json' if os.path.exists('sync_openalex.json') else None
    try:
        LOGGER.info(f"Sending email to {email}")
        JRC.send_email(msg, DISCONFIG['sender'], email, subject, attachment=attach, mime='html')
    except Exception as err:
        print(str(err))
        traceback.print_exc()
        terminate_program(err)


def process_dois():
    """ Process a list of DOIs
        Keyword arguments:
          None
        Returns:
          None
    """
    cnt = 0
    rows = []
    dois = get_dois()
    if dois:
        for doi in dois:
            data = DL.get_doi_record(doi, coll=DB['dis']['dois'])
            if data is None:
                LOGGER.warning(f"{doi} was not found in the database")
                continue
            rows.append(data)
        cnt = len(rows)
    else:
        payload = {"doi": {"$not": {"$regex": "janelia"}}}
        if ARG.NEW:
            payload["$and"] = [{"jrc_is_oa": {"$exists": False}},
                               {"jrc_license": {"$exists": False}}]
        else:
            payload["$or"] = [{"jrc_is_oa": {"$exists": False}},
                              {"jrc_license": {"$exists": False}}]
        try:
            cnt = DB['dis'].dois.count_documents(payload)
            rows = DB['dis'].dois.find(payload)
        except Exception as err:
            terminate_program(err)
    # Open Access / license data
    LOGGER.info(f"Found {cnt} DOI{'s' if cnt != 1 else ''} to process for OpenAlex")
    for row in tqdm(rows, total=cnt, desc="Add OpenAlex"):
        COUNT['dois'] += 1
        update_open_access(row)
    pass1 = dict(COUNT)
    print(show_counts())
    # Open Access status override. Reset the counters with clear() (rather than a
    # hand-maintained key list that can drift) for a clean second-pass tally.
    COUNT.clear()
    if dois:
        rows = []
        for doi in dois:
            data = DL.get_doi_record(doi, coll=DB['dis']['dois'])
            if data is not None:
                rows.append(data)
        cnt = len(rows)
    else:
        rows = []
        payload = {"jrc_is_oa": {"$exists": True}, "jrc_oa_status": "closed",
                   "jrc_fulltext_url": {"$exists": True}
                  }
        try:
            cnt = DB['dis'].dois.count_documents(payload)
            rows = DB['dis'].dois.find(payload)
        except Exception as err:
            terminate_program(err)
    LOGGER.info(f"Found {cnt} DOI{'s' if cnt != 1 else ''} to process for OA status")
    for row in tqdm(rows, total=cnt, desc="Fix OA status"):
        if 'jrc_oa_status' not in row or not row['jrc_oa_status']:
            continue
        COUNT["dois"] += 1
        if row['jrc_oa_status'] == "closed" and row.get('jrc_fulltext_url'):
            override_oa_closed(row)
    pass2 = dict(COUNT)
    print(show_counts())
    unknown_licenses = sorted(UNKNOWN_LICENSES)
    if unknown_licenses:
        print(f"Unmapped licenses ({len(unknown_licenses):,}):")
        for lic in unknown_licenses:
            print(f"  {lic}")
    if OUTPUT:
        LOGGER.info("Writing output to sync_openalex.json")
        with open('sync_openalex.json', 'w', encoding='utf-8') as fileout:
            json.dump(OUTPUT, fileout, indent=4)
        if ARG.TEST or ARG.WRITE:
            generate_email(pass1, pass2, unknown_licenses)
    if not ARG.WRITE:
        LOGGER.warning("Dry run successful, no updates were made")

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Sync Open Access/license data from OpenAlex")
    GROUP_A = PARSER.add_mutually_exclusive_group(required=False)
    GROUP_A.add_argument('--doi', dest='DOI', action='store',
                         help='Single DOI to process')
    GROUP_A.add_argument('--file', dest='FILE', action='store',
                         help='File of DOIs to process')
    GROUP_A.add_argument('--new', dest='NEW', action='store_true',
                         help='Process DOIs with no OpenAlex data')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, prod)')
    PARSER.add_argument('--test', dest='TEST', action='store_true',
                        default=False, help='Send email to developer')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Write to database')
    PARSER.add_argument('--silent', dest='SILENT', action='store_true',
                        default=False, help="Don't display warnings")
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    try:
        DISCONFIG = JRC.simplenamespace_to_dict(JRC.get_config("dis"))
    except Exception as err:
        terminate_program(err)
    process_dois()
    terminate_program()
