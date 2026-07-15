''' apply_orcids.py
    Apply ORCIDs from the ORCID public API (and the doi collection) to the
    orcid collection.

    An HTML summary email is sent when --test is supplied, or when --write is
    supplied and at least one ORCID was applied: a header banner (run data,
    DRY RUN/WRITE badge), KPI stat tiles (ORCIDs read / considered / added /
    mismatches), a Change Breakdown table mirroring the console summary, and an
    ORCIDs Added table (each author linked to their /userui/ record, each ORCID
    linked to orcid.org).
'''

__version__ = '2.1.0'

import argparse
import collections
import configparser
import html
import json
from operator import attrgetter
import os
import sys
import time
import traceback
from pymongo.collation import Collation
import requests
from tqdm import tqdm
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Database
DB = {}
# Counters
COUNT = collections.defaultdict(lambda: 0, {})
# Global variables
ARG = CONFIG = DIS = LOGGER = None
IGNORE = {}
# ORCID public API: max rows per search response, and the public-API cap on how
# many results can be paged through (see
# https://info.orcid.org/documentation/api-tutorials/api-tutorial-searching-the-orcid-registry/)
ORCID_ROWS = 1000
ORCID_PUBLIC_CAP = 10000
# ORCID rate-limit handling (~24 req/s, bursts -> 503, plus 2025 daily quotas)
ORCID_MAX_RETRIES = 4
ORCID_BACKOFF = 2
TIMEOUT = (requests.exceptions.ConnectTimeout, requests.exceptions.ReadTimeout,
           requests.exceptions.Timeout)
# Added ORCIDs for the summary email: list of {name, userId, orcid} dicts
ADDED = []
OUTPUT = {"name_error": [], "name_multi_records": [], "name_not_found": [], "orcid_exists": [],
          "orcid_mismatch": [], "orcid_added": []}
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
    if "PEOPLE_API_KEY" not in os.environ:
        terminate_program("Missing token - set in PEOPLE_API_KEY environment variable")
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
        rows = DB['dis']['to_ignore'].find()
        for row in rows:
            if row['type'] not in IGNORE:
                IGNORE[row['type']] = {}
            IGNORE[row['type']][row['key']] = True
    except Exception as err:
        terminate_program(err)


def orcid_get(url, params=None):
    ''' GET from the ORCID public API, retrying with backoff on rate-limit
        responses (429/503) and transient timeouts. ORCID enforces ~24 req/s
        (bursts rejected as 503) plus daily quotas on the public API, so a
        single blip should not abort the whole run. Non-200 responses that
        aren't rate limits (301 deprecated, 404 not found, 409 deactivated/
        locked, ...) return None for the caller to skip and count.
        Keyword arguments:
          url: full request URL
          params: optional query-parameter dict (URL-encoded by requests)
        Returns:
          Parsed JSON dict on HTTP 200, else None. Never raises on HTTP status
          or JSON-decode errors.
    '''
    headers = {"Accept": "application/json"}
    for attempt in range(1, ORCID_MAX_RETRIES + 1):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=15)
        except TIMEOUT:
            LOGGER.warning(f"ORCID request timed out ({attempt}/{ORCID_MAX_RETRIES}): {url}")
            if attempt < ORCID_MAX_RETRIES:
                time.sleep(ORCID_BACKOFF * attempt)
            continue
        except requests.exceptions.RequestException as err:
            LOGGER.warning(f"ORCID request failed for {url}: {err}")
            return None
        if resp.status_code == 200:
            try:
                return resp.json()
            except ValueError:
                LOGGER.warning(f"ORCID returned non-JSON for {url}")
                return None
        if resp.status_code in (429, 503):
            retry_after = resp.headers.get('Retry-After', '')
            wait = int(retry_after) if retry_after.isdigit() else ORCID_BACKOFF * attempt
            LOGGER.warning(f"ORCID rate-limited ({resp.status_code}); waiting {wait}s "
                           f"({attempt}/{ORCID_MAX_RETRIES})")
            if attempt < ORCID_MAX_RETRIES:
                time.sleep(wait)
            continue
        # Deprecated (301), not found (404), deactivated/locked (409), etc.
        LOGGER.debug(f"ORCID {resp.status_code} for {url}")
        return None
    LOGGER.warning(f"ORCID request gave up after {ORCID_MAX_RETRIES} attempts: {url}")
    return None


def check_orcid(oid, name, family, given):
    ''' Check an ORCID record
        Keyword arguments:
          oid: ORCID
          name: name record from ORCID
          family: family name
          given: given name
    '''
    LOGGER.debug(f"{oid}: {family}, {given}")
    coll = DB['dis']['orcid']
    try:
        cnt = coll.count_documents({'orcid': oid})
        if cnt:
            OUTPUT['orcid_exists'].append(name)
            return
        payload = {'family': family, 'given': given}
        cnt = coll.count_documents(payload, collation=Collation(locale='en_US', strength=1))
        if not cnt:
            OUTPUT['name_not_found'].append(name)
            return
        if cnt > 1:
            OUTPUT['name_multi_records'].append(name)
            return
        rec = coll.find_one(payload, collation=Collation(locale='en_US', strength=1))
    except Exception as err:
        terminate_program(err)
    if 'orcid' in rec:
        OUTPUT['orcid_mismatch'].append(name)
        return
    OUTPUT['orcid_added'].append(rec)
    email = rec.get('userIdO365')
    if not email:
        LOGGER.warning(f"{oid}: {given} {family} has no email")
    # Record the applied ORCID for the summary email, and persist it regardless
    # of whether the person has a userIdO365 - the email is only used to link
    # the name, not to gate the database write.
    ADDED.append({"name": f"{given} {family}", "userId": email, "orcid": oid})
    if ARG.WRITE:
        try:
            coll.update_one({'_id': rec['_id']}, {'$set': {'orcid': oid}})
        except Exception as err:
            terminate_program(err)


def process_crossref_author(aut):
    ''' Process a Crossref author
        Keyword arguments:
          aut: author record
        Returns:
          None
    '''
    if not any('Janelia' in (arec.get('name') or '')
               for arec in aut.get('affiliation', []) if isinstance(arec, dict)):
        return None
    if 'ORCID' not in aut:
        return None
    oid = aut['ORCID'].split('orcid.org/')[-1]
    return oid


def process_datacite_author(aut):
    ''' Process a DataCite author
        Keyword arguments:
          aut: author record
        Returns:
          None
    '''
    if not any('Janelia' in arec for arec in aut.get('affiliation', [])
               if isinstance(arec, str)):
        return None
    if 'nameIdentifiers' not in aut:
        return None
    oid = None
    for findorcid in aut['nameIdentifiers']:
        if findorcid['nameIdentifierScheme'] == 'ORCID':
            oid = findorcid['nameIdentifier'].split('/')[-1]
            break
    return oid


def get_orcids_from_doi(oids, existing):
    ''' Get ORCIDs from the doi collection
        Keyword arguments:
          oids: list of ORCIDs
          existing: list of existing ORCIDs
          None
        Returns:
          None
    '''
    # Get ORCIDs from the doi collection
    dcoll = DB['dis'].dois
    # Crossref
    payload = {"author.affiliation.name": {"$regex": "Janelia"},
               "author.ORCID": {"$exists": True}}
    project = {"author.given": 1, "author.family": 1,
               "author.ORCID": 1, "author.affiliation": 1, "doi": 1}
    try:
        recs = dcoll.find(payload, project)
    except Exception as err:
        terminate_program(err)
    for rec in tqdm(recs, desc="Adding Crossref ORCIDs from doi collection"):
        if 'author' not in rec or rec['doi'] in IGNORE['doi']:
            continue
        for aut in rec['author']:
            oid = process_crossref_author(aut)
            if not oid or oid in existing or oid in IGNORE['orcid']:
                continue
            if oid not in oids:
                COUNT['read'] += 1
                oids.append(oid)
    # DataCite
    payload = {"creators.affiliation": {"$regex": "Janelia"},
               "creators.nameIdentifiers.nameIdentifierScheme": "ORCID"}
    project = {"creators": 1, "doi": 1}
    try:
        recs = dcoll.find(payload, project)
    except Exception as err:
        terminate_program(err)
    for rec in tqdm(recs, desc="Adding DataCite ORCIDs from doi collection"):
        if rec['doi'] in IGNORE['doi']:
            continue
        for aut in rec['creators']:
            oid = process_datacite_author(aut)
            if not oid or oid in existing or oid in IGNORE['orcid']:
                continue
            if oid not in oids:
                COUNT['read'] += 1
                oids.append(oid)


def process_orcid(oid):
    ''' Process an ORCID ID
        Keyword arguments:
          oid: ORCID
        Returns:
          None
    '''
    orc = orcid_get(f"{CONFIG['orcid']['base']}{oid}")
    if not orc:
        LOGGER.warning(f"Could not read ORCID record {oid} (deactivated, deprecated, "
                       "or unavailable) - skipping")
        COUNT['orcid_unavailable'] += 1
        return
    name = (orc.get('person') or {}).get('name')
    if not name or not name.get('family-name') or not name.get('given-names'):
        LOGGER.warning(f"ORCID {oid} has no name")
        OUTPUT['name_error'].append(name)
        return
    family = (name['family-name'] or {}).get('value')
    given = (name['given-names'] or {}).get('value')
    if not (family and given):
        OUTPUT['name_error'].append(name)
        return
    check_orcid(oid, name, family, given)


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
        sits as a naked div immediately before a sibling <table> in the same
        <td> - Outlook's Word engine can misparse that boundary and leak a stray
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
        tables that repeat the same complex inline style across many rows, so
        cells use only plain background-color striping. A trailing spacer row
        absorbs the last-row-before-</table> boundary (see html_section_header).
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


def html_added_table(added):
    ''' Build the "ORCIDs Added" table: one row per applied ORCID, author linked
        to their /userui/ record (when a userIdO365 is known) and the ORCID
        linked to orcid.org.
        Keyword arguments:
          added: list of {name, userId, orcid} dicts
        Returns:
          HTML table, or a plain message if empty
    '''
    if not added:
        return f'<div style="color:{EMAIL_GRAY};font-size:13px;">No ORCIDs were added.</div>'
    rows = []
    for i, entry in enumerate(added):
        bgattr = f' bgcolor="{EMAIL_STRIPE_BG}"' if i % 2 == 0 else ''
        bg = f'background-color:{EMAIL_STRIPE_BG};' if i % 2 == 0 else ''
        name = html.escape(entry['name'])
        if entry.get('userId'):
            name = (f"<a href='https://dis.int.janelia.org/userui/{entry['userId']}' "
                    f"style='color:{EMAIL_NAVY};text-decoration:none;font-weight:600;'>"
                    f"{name}</a>")
        oid = html.escape(entry['orcid'])
        oid_link = (f"<a href='https://orcid.org/{oid}' "
                    f"style='color:{EMAIL_NAVY};text-decoration:none;'>{oid}</a>")
        rows.append(f'<tr{bgattr} style="{bg}">'
                    f'<td style="padding:8px 10px;white-space:nowrap;">{name}</td>'
                    f'<td style="padding:8px 10px;">{oid_link}</td></tr>')
    rows.append('<tr><td colspan="2" style="height:1px;line-height:1px;font-size:1px;">'
                '&nbsp;</td></tr>')
    return ('<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
            'style="border-collapse:collapse;font-size:12.5px;">'
            f'<tr style="color:{EMAIL_GRAY};font-size:10.5px;text-transform:uppercase;'
            'letter-spacing:.03em;"><td style="padding:6px 10px;">Author</td>'
            '<td style="padding:6px 10px;">ORCID</td></tr>'
            + "".join(rows) + '</table>')


def generate_email(dois, fname):
    ''' Generate and send the HTML run-summary email.
        Keyword arguments:
          dois: list of DOIs needing an update (attached as fname when present)
          fname: path to the DOIs-to-update file
        Returns:
          None
    '''
    run_data = JRC.get_run_data(__file__, __version__).strip()
    mode_badge_bg = EMAIL_GREEN if ARG.WRITE else EMAIL_AMBER
    mode_label = 'WRITE' if ARG.WRITE else 'DRY RUN'
    kpis = ''.join([
        html_kpi_card(f"{COUNT['read']:,}", "ORCIDs read"),
        html_kpi_card(f"{COUNT['considered']:,}", "Considered"),
        html_kpi_card(f"{len(OUTPUT['orcid_added']):,}", "Added",
                      'good' if OUTPUT['orcid_added'] else 'neutral'),
        html_kpi_card(f"{len(OUTPUT['orcid_mismatch']):,}", "Mismatches",
                      'amber' if OUTPUT['orcid_mismatch'] else 'neutral'),
    ])
    breakdown_rows = [
        ("Already have ORCID", f"{len(OUTPUT['orcid_exists']):,}"),
        ("Name not found", f"{len(OUTPUT['name_not_found']):,}"),
        ("Multiple name matches", f"{len(OUTPUT['name_multi_records']):,}"),
        ("ORCID / name mismatch", f"{len(OUTPUT['orcid_mismatch']):,}"),
        ("Name errors (ORCID record)", f"{len(OUTPUT['name_error']):,}"),
        ("Records unavailable", f"{COUNT['orcid_unavailable']:,}"),
        ("Ignored", f"{COUNT['orcid_ignored']:,}"),
        ("ORCIDs added", f"{len(OUTPUT['orcid_added']):,}"),
    ]
    if dois:
        breakdown_rows.append(("DOIs to update", f"{len(dois):,}"))
    breakdown_section = (html_section_header("&#128202; Change Breakdown")
                         + html_metric_rows(breakdown_rows))
    added_section = (html_section_header(f"&#9989; ORCIDs Added ({len(ADDED):,})")
                     + html_added_table(ADDED))
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
        f'institution: {ARG.INSTITUTION} &middot; manifold: {ARG.MANIFOLD} &middot; '
        f'<span style="background-color:{mode_badge_bg};color:#fff;border-radius:10px;'
        f'padding:1px 9px;font-size:11px;font-weight:600;letter-spacing:.03em;">'
        f'{mode_label}</span></div></td></tr>'
        f'<tr><td style="padding:22px 22px 6px 22px;">'
        f'<table role="presentation" width="100%" cellpadding="0" cellspacing="6"><tr>'
        f'{kpis}</tr></table></td></tr>'
        f'<tr><td style="padding:18px 28px 4px 28px;">{breakdown_section}</td></tr>'
        f'<tr><td style="padding:20px 28px 4px 28px;">{added_section}</td></tr>'
        f'<tr><td bgcolor="{EMAIL_STRIPE_BG}" style="padding:18px 28px;'
        f'background-color:{EMAIL_STRIPE_BG};color:{EMAIL_GRAY};'
        f'font-size:11px;text-align:center;border-top:1px solid {EMAIL_BORDER};">'
        'Generated by apply_orcids.py &middot; Data and Information Services '
        '&middot; Janelia Research Campus</td></tr>'
        '</table></td></tr></table></div>')
    subject = "ORCIDs added to orcid collection"
    email = DIS['developer'] if ARG.TEST else DIS['receivers']
    attach = fname if (dois and os.path.exists(fname)) else None
    try:
        LOGGER.info(f"Sending email to {email}")
        JRC.send_email(msg, DIS['sender'], email, subject, attachment=attach, mime='html')
    except Exception as err:
        print(str(err))
        traceback.print_exc()
        terminate_program(err)


def postprocessing():
    ''' Postprocessing
        Keyword arguments:
          None
        Returns:
          None
    '''
    for key, value in OUTPUT.items():
        fname = f"orcid_{key}.json"
        if value:
            with open(fname, "w", encoding="utf-8") as outstream:
                outstream.write(json.dumps(value, indent=2, default=str))
        elif os.path.exists(fname):
            os.remove(fname)
    dois = []
    if OUTPUT['orcid_added']:
        for rec in OUTPUT['orcid_added']:
            try:
                adois = DL.get_dois_by_author(rec, coll=DB['dis'].dois)
            except Exception as err:
                terminate_program(err)
            dois.extend(adois)
    print(f"ORCIDs read:                  {COUNT['read']:,}")
    print(f"ORCIDs considered:            {COUNT['considered']:,}")
    print(f"ORCIDs ignored:               {COUNT['orcid_ignored']:,}")
    print(f"ORCIDs unavailable:           {COUNT['orcid_unavailable']:,}")
    print(f"ORCIDs with name error:       {len(OUTPUT['name_error']):,}")
    print(f"ORCIDs existing:              {len(OUTPUT['orcid_exists']):,}")
    print(f"ORCIDs with name not found:   {len(OUTPUT['name_not_found']):,}")
    print(f"ORCIDs with multiple records: {len(OUTPUT['name_multi_records']):,}")
    print(f"ORCIDs with mismatch:         {len(OUTPUT['orcid_mismatch']):,}")
    print(f"ORCIDs added:                 {len(OUTPUT['orcid_added']):,}")
    if dois:
        print(f"DOIs to update:               {len(dois):,}")
    fname = "dois_to_update.txt"
    if os.path.exists(fname):
        os.remove(fname)
    if dois:
        with open(fname, "w", encoding="ascii") as outstream:
            for doi in dois:
                outstream.write(f"{doi}\n")
    if ARG.TEST or (ARG.WRITE and OUTPUT['orcid_added']):
        generate_email(dois, fname)


def apply_orcids():
    ''' Find ORCID IDs using the ORCID API
        Keyword arguments:
          None
        Returns:
          None
    '''
    # Get existing DOIs from the orcid collection
    existing = []
    try:
        existing = list(DB['dis']['orcid'].find({"orcid": {"$exists": True}}))
    except Exception as err:
        terminate_program(err)
    existing = [rec['orcid'] for rec in existing]
    # Get ORCIDs from the doi collection
    oids = []
    get_orcids_from_doi(oids, existing)
    if oids and ARG.VERBOSE:
        LOGGER.info(f"ORCIDs from doi collection: {len(oids)}")
    # Get ORCIDs from the ORCID API. Each query is paginated (start/rows): ORCID
    # caps a search response at ORCID_ROWS results, so without paging we would
    # silently see only the first page.
    base = f"{CONFIG['orcid']['base']}search"
    search = {'hhmi': [f'ror-org-id:"{CONFIG["ror"]["hhmi"]}"',
                       'affiliation-org-name:"Howard Hughes Medical Institute"'],
              'janelia': [f'ror-org-id:"{CONFIG["ror"]["janelia"]}"',
                          'affiliation-org-name:"Janelia Research Campus"',
                          'affiliation-org-name:"Janelia Farm Research Campus"']
             }
    for query in search[ARG.INSTITUTION]:
        start = 0
        while True:
            data = orcid_get(f"{base}/", params={'q': query, 'start': start,
                                                 'rows': ORCID_ROWS})
            if not data:
                break
            results = data.get('result') or []
            for orcid in results:
                oid = (orcid.get('orcid-identifier') or {}).get('path')
                if not oid:
                    continue
                COUNT['read'] += 1
                if oid in existing:
                    COUNT['orcid_exists'] += 1
                    continue
                if oid not in oids:
                    oids.append(oid)
            num_found = data.get('num-found') or 0
            start += ORCID_ROWS
            if not results or start >= num_found:
                break
            if start >= ORCID_PUBLIC_CAP:
                LOGGER.warning(f'ORCID search "{query}": {num_found:,} results but the public '
                               f'API caps retrieval at {ORCID_PUBLIC_CAP:,}; remainder skipped')
                break
    for oid in tqdm(sorted(oids), desc="Processing ORCIDs"):
        COUNT['considered'] += 1
        if oid in IGNORE['orcid']:
            COUNT['orcid_ignored'] += 1
            continue
        process_orcid(oid)
    postprocessing()


# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Apply ORCIDS to orcid collection")
    PARSER.add_argument('--institution', dest='INSTITUTION', action='store',
                        default='janelia', choices=['hhmi', 'janelia'],
                        help='Institution (hhmi, [janelia])')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, prod)')
    PARSER.add_argument('--test', dest='TEST', action='store_true',
                        default=False, help='Send email to developer')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Write to database/config system')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    try:
        DIS = JRC.simplenamespace_to_dict(JRC.get_config("dis"))
    except Exception as err:
        terminate_program(err)
    initialize_program()
    CONFIG = configparser.ConfigParser()
    # config.ini lives beside this script; read by absolute path so the run
    # doesn't depend on the current working directory, and fail loudly if the
    # file is missing (ConfigParser.read() silently no-ops otherwise).
    CFGPATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'config.ini')
    if not CONFIG.read(CFGPATH):
        terminate_program(f"Could not read config file {CFGPATH}")
    apply_orcids()
    terminate_program()
