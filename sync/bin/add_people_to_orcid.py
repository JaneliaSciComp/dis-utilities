''' add_people_to_orcid.py
    Add new employees to the orcid collection from the People system.

    An HTML summary email is sent when --test is supplied, or when --write is
    supplied and at least one record was actually added/changed: a header
    banner (run data, DRY RUN/WRITE badge), KPI stat tiles (People records
    read, new employees, boomerangs, alumni set), a Change Breakdown table
    mirroring the console summary, New Employees/Boomerangs/Alumni Set/
    Skipped tables listing exactly who was affected (linked to their
    /userui/ record where applicable), and an Ignored Organizations table
    (org -> count) for the (often large) set of people skipped because
    their organization is on the ignore list.
'''

__version__ = '7.3.1'

import argparse
import collections
from datetime import datetime
import html
import json
from operator import attrgetter
import os
import sys
import traceback
import requests
from tqdm import tqdm
import jrc_common.jrc_common as JRC

# pylint: disable=broad-exception-caught,logging-fstring-interpolation,logging-not-lazy

# Database
DB = {}
# Counters
COUNT = collections.defaultdict(lambda: 0, {})
# Global variables
ARG = DIS = LOGGER = REST = None
IGNORE = {}
# People API timeouts that @retry (in jrc_common) exhausts and re-raises
TIMEOUT = (requests.exceptions.ConnectTimeout, requests.exceptions.ReadTimeout,
           requests.exceptions.Timeout)
# HTML run-summary email palette (generate_email and its html_* helpers). Mirrors
# update_janelians_from_people.py/sync_citations.py's email convention: inline
# styles only (no <style> block/classes) for reliable rendering across email
# clients including older Outlook.
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
        rows = DB['dis']['to_ignore'].find({"type": "group"})
        for row in rows:
            IGNORE[row['key']] = True
    except Exception as err:
        terminate_program(err)


def call_responder(server, endpoint):
    """ Call a responder
        Keyword arguments:
          server: config key for the REST service (e.g. "people")
          endpoint: REST endpoint appended to the service's base URL
        Returns:
          Response JSON
    """
    # REST (from the rest_services config) is set before this is ever called;
    # fall back to CONFIG_SERVER_URL only if the module is imported without it.
    base = getattr(REST, server).url if REST else os.environ.get('CONFIG_SERVER_URL', '')
    url = base + endpoint
    try:
        headers = {'APIKey': os.environ['PEOPLE_API_KEY'],
                   'Content-Type': 'application/json'}
        req = requests.get(url, headers=headers, timeout=10)
    except requests.exceptions.RequestException as err:
        terminate_program(f"Could not fetch from {url}\n{str(err)}")
    if req.status_code != 200:
        terminate_program(f"Status: {str(req.status_code)} ({url})")
    return req.json()


def add_middle_name(rec, given):
    ''' Add middle name to given name
        Keyword arguments:
          rec: person record from People
          given: list of given names
        Returns:
          None
    '''
    if rec["nameMiddlePreferred"]:
        temp = given.copy()
        for first in temp:
            given.append(' '.join([first, rec["nameMiddlePreferred"]]))
            if len(rec["nameMiddlePreferred"]) > 1:
                given.append(' '.join([first, rec["nameMiddlePreferred"][0]]))
                given.append(' '.join([first, rec["nameMiddlePreferred"][0]+'.']))
    if rec["nameMiddle"]:
        temp = given.copy()
        for first in temp:
            if " " in first:
                continue
            mid = ' '.join([first, rec["nameMiddle"]])
            if mid not in temp:
                given.append(mid)
            if len(rec["nameMiddle"]) > 1:
                mid = ' '.join([first, rec["nameMiddle"][0]])
                if mid not in temp:
                    given.append(mid)
                mid = ' '.join([first, rec["nameMiddle"][0]+'.'])
                if mid not in temp:
                    given.append(mid)


def add_new_record(person, output, email_entries):
    ''' Add a new record to the orcid collection
        Keyword arguments:
          person: person record from People
          output: output dictionary
          email_entries: {new, boomerang, alumni, skipped} -> list of
                         {name, userId, detail} dicts, and
                         skipped_orgs -> {org name: count} dict, for the
                         summary email. A person skipped for having no
                         People record or no organization is listed by name
                         in `skipped`; a person skipped because their
                         organization is on the ignore list is tallied by
                         org in `skipped_orgs` instead (that bucket can run
                         to hundreds of people, too many to list by name
                         usefully)
        Returns:
          None
    '''
    try:
        rec = JRC.call_people_by_id(person['employeeId'])
    except TIMEOUT as err:
        terminate_program(f"Request failed after multiple retries: {err}")
    except Exception as err:
        terminate_program(f"Error calling People by id: {err}")
    if not rec:
        LOGGER.warning(f"No record found in People for {person['nameFirstPreferred']} " \
                       + f"{person['nameLastPreferred']}")
        output['skipped'].append(f"{person['nameFirstPreferred']} {person['nameLastPreferred']} " \
                                 + "(no People record)")
        email_entries['skipped'].append({"name": f"{person['nameFirstPreferred']} "
                                         f"{person['nameLastPreferred']}", "userId": None,
                                         "detail": "No People record"})
        COUNT['skipped_no_record'] += 1
        return
    if not rec['supOrgName']:
        output['skipped'].append(f"{rec['nameFirstPreferred']} {rec['nameLastPreferred']} " \
                                 + "(no organization)")
        email_entries['skipped'].append({"name": f"{rec['nameFirstPreferred']} "
                                         f"{rec['nameLastPreferred']}", "userId": None,
                                         "detail": "(no organization)"})
        COUNT['skipped_no_org'] += 1
        return
    if rec['supOrgName'] in IGNORE:
        output['skipped'].append(f"{rec['nameFirstPreferred']} {rec['nameLastPreferred']} " \
                                 + f"(ignored organization: {rec['supOrgName']})")
        email_entries['skipped_orgs'][rec['supOrgName']] = \
            email_entries['skipped_orgs'].get(rec['supOrgName'], 0) + 1
        COUNT['skipped_ignored_org'] += 1
        return
    COUNT['new'] += 1
    payload = {"userIdO365": rec["userIdO365"],
               "employeeId": rec["employeeId"]
              }
    # Family name
    family = [rec["nameLastPreferred"]]
    if rec["nameLast"] not in family:
        family.append(rec["nameLast"])
    payload['family'] = family
    # Given name
    given = [rec["nameFirstPreferred"]]
    if rec["nameFirst"] not in given:
        given.append(rec["nameFirst"])
    add_middle_name(rec, given)
    payload['given'] = given
    # Iterate a snapshot (list(...)): we append to the same list inside the loop.
    for variant in list(payload['given']):
        stripped = JRC.convert_diacritics(variant)
        if stripped is not None and stripped not in payload['given']:
            payload['given'].append(stripped)
    for variant in list(payload['family']):
        stripped = JRC.convert_diacritics(variant)
        if stripped is not None and stripped not in payload['family']:
            payload['family'].append(stripped)
    if rec.get('hireDate'):
        hdate = rec['hireDate'].split(' ')[0]
        date_object = datetime.strptime(hdate, "%m/%d/%Y")
        payload['hireDate'] = date_object.strftime('%Y-%m-%d')
    output['new'].append(json.dumps(payload, indent=2))
    email_entries['new'].append({"name": f"{payload['given'][0]} {payload['family'][0]}",
                                 "userId": payload['userIdO365'], "detail": rec['supOrgName']})
    if not ARG.WRITE:
        print(json.dumps(payload, indent=2))
        return
    try:
        result = DB['dis']['orcid'].insert_one(payload)
        if hasattr(result, 'inserted_id') and result.inserted_id:
            COUNT['insert'] += 1
    except Exception as err:
        terminate_program(err)


def unset_alumni(person, output, email_entries, orcid_uid):
    ''' Unset the alumni flag in orcid
        Keyword arguments:
          person: person record from People
          output: output dictionary
          email_entries: {new, boomerang, alumni, skipped} -> list of
                         {name, userId, detail} dicts, and skipped_orgs ->
                         {org name: count} dict, for the summary email
          orcid_uid: employeeId -> userIdO365 map (built in update_orcid);
                     the record is known to exist since the caller only
                     invokes this when the employeeId is in the orcid map
        Returns:
          None
    '''
    name = f"{person['nameFirstPreferred']} {person['nameLastPreferred']}"
    COUNT['boomerang'] += 1
    LOGGER.warning(f"Unsetting alumni flag for {name}")
    output['boomerang'].append(json.dumps(person, indent=2))
    email_entries['boomerang'].append({"name": name,
                                       "userId": orcid_uid.get(person['employeeId']),
                                       "detail": ''})
    if not ARG.WRITE:
        return
    try:
        result = DB['dis']['orcid'].update_one({'employeeId': person['employeeId']},
                                               {'$unset': {'alumni': None}})
        if hasattr(result, 'modified_count') and result.modified_count:
            COUNT['update'] += 1
    except Exception as err:
        terminate_program(err)


def set_alumni(person, orcid, email_entries, orcid_uid):
    ''' Set the alumni flag in orcid
        Keyword arguments:
          person: person record from People
          orcid: orcid dictionary
          email_entries: {new, boomerang, alumni, skipped} -> list of
                         {name, userId, detail} dicts, and skipped_orgs ->
                         {org name: count} dict, for the summary email
          orcid_uid: employeeId -> userIdO365 map (built in update_orcid);
                     the record is known to exist since the caller only
                     invokes this when the employeeId is in the orcid map
        Returns:
          None
    '''
    if person['employeeId'] in orcid and not orcid[person['employeeId']]:
        COUNT['people_alumni'] += 1
        return
    name = f"{person['nameFirstPreferred']} {person['nameLastPreferred']}"
    COUNT['set_alumni'] += 1
    LOGGER.warning(f"Setting alumni flag for {name}")
    email_entries['alumni'].append({"name": name,
                                    "userId": orcid_uid.get(person['employeeId']),
                                    "detail": ''})
    if not ARG.WRITE:
        return
    now = datetime.now()
    try:
        result = DB['dis']['orcid'].update_one({'employeeId': person['employeeId']},
                                      {'$set': {'alumni': True,
                                                'alumni_date': now.strftime("%Y-%m-%d")}})
        if hasattr(result, 'modified_count') and result.modified_count:
            COUNT['update'] += 1
    except Exception as err:
        terminate_program(err)


def html_kpi_card(value, label, tone='neutral'):
    ''' Build one KPI stat tile for the run-summary email's header row.
        A single <td> carries the box look directly (bgcolor attribute +
        background-color, no nested table) - Outlook's Word rendering engine
        chokes on a percentage-width table nested inside a percentage-width <td>.
        Keyword arguments:
          value: display value (already formatted, e.g. "3")
          label: caption under the value
          tone: 'neutral', 'good', 'amber', or 'bad' - selects the tile's
                color scheme
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
    ''' Build a section header bar for the run-summary email. Table-based
        (not a bare <div>) so it never sits as a naked div immediately before
        a sibling <table> inside the same <td> - Outlook's Word rendering
        engine can misparse that div-then-table boundary and leak a stray
        closing tag as literal visible text. The spacer row substitutes for
        CSS margin-bottom, which <td> doesn't honor.
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
    ''' Build a zebra-striped label/value table for the run-summary email.
        Deliberately no per-cell border-radius: Outlook's Word rendering
        engine can leak a cell's opening tag as literal visible text in
        documents with many rows repeating the same complex inline style
        (confirmed via a real Outlook test - a trailing spacer row alone did
        not fix it), so cells here use only plain background-color striping,
        which Word handles reliably. Also no margin-top on the table itself
        (html_section_header's trailing spacer row already provides that
        gap) - confirmed via a real Outlook test that a <table> carrying its
        own margin-top, sitting immediately after a sibling table's
        </table>, leaks a stray closing tag as literal visible text. A
        trailing throwaway spacer row is kept anyway as cheap insurance
        against the last-row-before-</table> boundary (see
        html_section_header).
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
        trs.append(f'<tr{bgattr} style="{bg}">'
                   f'<td style="padding:8px 10px;">{mlabel}</td>'
                   f'<td align="right" style="padding:8px 10px;text-align:right;">'
                   f'{value}</td></tr>')
    trs.append('<tr><td colspan="2" style="height:1px;line-height:1px;font-size:1px;">'
               '&nbsp;</td></tr>')
    return ('<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
            'style="border-collapse:collapse;font-size:13px;">'
            + "".join(trs) + '</table>')


def html_people_table(entries, empty_message):
    ''' Build a zebra-striped Name/Detail table for a list of {name, userId,
        detail} dicts (new employees, boomerangs, or skipped people). Name is
        linked to the person's /userui/ record when userId is available.
        Deliberately no per-cell border-radius (see html_metric_rows) -
        confirmed via a real Outlook test that it can leak a cell's opening
        tag as literal visible text in a large, highly-repetitive table like
        this one (Skipped can run to hundreds of rows). A trailing throwaway
        spacer row is kept as cheap insurance against the last-row-before-
        </table> boundary (see html_section_header).
        Keyword arguments:
          entries: list of {name, userId, detail} dicts
          empty_message: message to show when entries is empty
        Returns:
          HTML table, or a plain empty_message div if entries is empty
    '''
    if not entries:
        return f'<div style="color:{EMAIL_GRAY};font-size:13px;">{empty_message}</div>'
    rows = []
    for i, entry in enumerate(entries):
        striped = i % 2 == 0
        bgattr = f' bgcolor="{EMAIL_STRIPE_BG}"' if striped else ''
        bg = f'background-color:{EMAIL_STRIPE_BG};' if striped else ''
        name = html.escape(entry['name'])
        if entry.get('userId'):
            name = (f"<a href='https://dis.int.janelia.org/userui/{entry['userId']}' "
                    f"style='color:{EMAIL_NAVY};text-decoration:none;font-weight:600;'>"
                    f"{name}</a>")
        detail = html.escape(entry.get('detail') or '')
        rows.append(
            f'<tr{bgattr} style="{bg}">'
            f'<td style="padding:8px 10px;white-space:nowrap;">{name}</td>'
            f'<td style="padding:8px 10px;color:{EMAIL_GRAY};">{detail}</td></tr>')
    rows.append('<tr><td colspan="2" style="height:1px;line-height:1px;font-size:1px;">'
                '&nbsp;</td></tr>')
    return ('<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
            'style="border-collapse:collapse;font-size:12.5px;">'
            + "".join(rows) + '</table>')


def generate_email(email_entries, new_fname=None):
    ''' Generate and send the HTML run-summary email: a header banner (run
        data, DRY RUN/WRITE badge), KPI stat tiles, a change-type breakdown,
        New Employees/Boomerangs/Alumni Set/Skipped tables, and an Ignored
        Organizations table (org name -> count, sorted by count descending)
        for people skipped due to an ignore-listed organization - listed by
        org rather than by name since that bucket can run to hundreds of
        people. Built entirely from inline styles/tables (no <style> block)
        for compatibility with older email clients, matching the convention
        used by update_janelians_from_people.py and sync_citations.py.
        Keyword arguments:
          email_entries: {new, boomerang, alumni, skipped} -> list of
                         {name, userId, detail} dicts, and skipped_orgs ->
                         {org name: count} dict
          new_fname: path to the new-employees JSON output file, attached
                     when present (same file the console/output section
                     writes) - None if no new employees were added this run
        Returns:
          None
    '''
    run_data = JRC.get_run_data(__file__, __version__).strip()
    mode_badge_bg = EMAIL_GREEN if ARG.WRITE else EMAIL_AMBER
    mode_label = 'WRITE' if ARG.WRITE else 'DRY RUN'

    kpis = ''.join([
        html_kpi_card(f"{COUNT['people']:,}", "Records read"),
        html_kpi_card(f"{COUNT['new']:,}", "New employees",
                      'good' if COUNT['new'] else 'neutral'),
        html_kpi_card(f"{COUNT['boomerang']:,}", "Boomerangs",
                      'amber' if COUNT['boomerang'] else 'neutral'),
        html_kpi_card(f"{COUNT['set_alumni']:,}", "Alumni set",
                      'amber' if COUNT['set_alumni'] else 'neutral'),
    ])

    breakdown_rows = [
        ("Already active", f"{COUNT['already_active']:,}"),
        ("Skipped (no People record)", f"{COUNT['skipped_no_record']:,}"),
        ("Skipped (no organization)", f"{COUNT['skipped_no_org']:,}"),
        ("Skipped (organization ignored)", f"{COUNT['skipped_ignored_org']:,}"),
        ("Not at Janelia", f"{COUNT['not_janelia']:,}"),
        ("No employee ID", f"{COUNT['no_empid']:,}"),
        ("Already alumni (no change)", f"{COUNT['people_alumni']:,}"),
        ("JRC Alumni set", f"{COUNT['set_alumni']:,}"),
        ("Boomerangs", f"{COUNT['boomerang']:,}"),
        ("New employees", f"{COUNT['new']:,}"),
        ("Records inserted", f"{COUNT['insert']:,}"),
        ("Records updated", f"{COUNT['update']:,}"),
    ]
    breakdown_section = (html_section_header("&#128202; Change Breakdown")
                         + html_metric_rows(breakdown_rows))

    new_section = (html_section_header(f"&#128100; New Employees "
                                       f"({len(email_entries['new']):,})")
                  + html_people_table(email_entries['new'], "No new employees were added."))
    boomerang_section = (html_section_header(f"&#128257; Boomerangs "
                                             f"({len(email_entries['boomerang']):,})")
                        + html_people_table(email_entries['boomerang'], "No boomerangs."))
    alumni_section = (html_section_header(f"&#127891; Alumni Set "
                                          f"({len(email_entries['alumni']):,})")
                      + html_people_table(email_entries['alumni'], "No one was set to alumni."))
    skipped_section = (html_section_header(f"&#9940; Skipped "
                                           f"({len(email_entries['skipped']):,})")
                      + html_people_table(email_entries['skipped'], "Nobody was skipped."))
    ignored_orgs = sorted(email_entries['skipped_orgs'].items(), key=lambda kv: -kv[1])
    ignored_org_section = (
        html_section_header(f"&#127970; Ignored Organizations ({len(ignored_orgs):,})")
        + (html_metric_rows([(html.escape(org), f"{cnt:,}") for org, cnt in ignored_orgs])
           if ignored_orgs
           else f'<div style="color:{EMAIL_GRAY};font-size:13px;">'
                'No organizations were ignored.</div>'))

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
        f'manifold: {ARG.MANIFOLD} &middot; '
        f'<span style="background-color:{mode_badge_bg};color:#fff;border-radius:10px;'
        f'padding:1px 9px;font-size:11px;font-weight:600;letter-spacing:.03em;">'
        f'{mode_label}</span></div></td></tr>'
        f'<tr><td style="padding:22px 22px 6px 22px;">'
        # cellspacing (not CSS margin, which <td> mostly ignores) puts a real gap
        # between the KPI tiles; Outlook's Word engine honors this old-school
        # HTML attribute far more reliably than CSS spacing tricks.
        f'<table role="presentation" width="100%" cellpadding="0" cellspacing="6"><tr>'
        f'{kpis}</tr></table></td></tr>'
        f'<tr><td style="padding:18px 28px 4px 28px;">{breakdown_section}</td></tr>'
        f'<tr><td style="padding:20px 28px 4px 28px;">{new_section}</td></tr>'
        f'<tr><td style="padding:20px 28px 4px 28px;">{boomerang_section}</td></tr>'
        f'<tr><td style="padding:20px 28px 4px 28px;">{alumni_section}</td></tr>'
        f'<tr><td style="padding:20px 28px 4px 28px;">{skipped_section}</td></tr>'
        f'<tr><td style="padding:20px 28px 4px 28px;">{ignored_org_section}</td></tr>'
        f'<tr><td bgcolor="{EMAIL_STRIPE_BG}" style="padding:18px 28px;'
        f'background-color:{EMAIL_STRIPE_BG};color:{EMAIL_GRAY};'
        f'font-size:11px;text-align:center;border-top:1px solid {EMAIL_BORDER};">'
        'Generated by add_people_to_orcid.py &middot; Data and Information Services '
        '&middot; Janelia Research Campus</td></tr>'
        '</table></td></tr></table></div>')

    subject = "Janelians added to orcid collection from People system"
    email = DIS['developer'] if ARG.TEST else DIS['receivers']
    try:
        LOGGER.info(f"Sending email to {email}")
        JRC.send_email(msg, DIS['sender'], email, subject, mime='html',
                       attachment=new_fname)
    except Exception as err:
        print(str(err))
        traceback.print_exc()
        terminate_program(err)


def update_orcid():
    ''' Add people to the orcid collection
        Keyword arguments:
          None
        Returns:
          None
    '''
    try:
        rows = DB['dis']['orcid'].find({'employeeId': {"$exists": True}},
                                       {'employeeId': 1, 'alumni': 1, 'userIdO365': 1})
    except Exception as err:
        terminate_program(err)
    # orcid: employeeId -> is-active (False if the record carries an alumni flag).
    # orcid_uid: employeeId -> userIdO365, so set_alumni/unset_alumni can link the
    # person in the email without a second per-record find_one round-trip.
    orcid = {}
    orcid_uid = {}
    for row in rows:
        eid = row['employeeId']
        orcid[eid] = 'alumni' not in row
        orcid_uid[eid] = row.get('userIdO365')
    if ARG.NAME:
        resp = call_responder("people", f"People/Search/ByName/{ARG.NAME}")
    else:
        #resp = call_responder("people", "People/Search/ByOther/Janelia Research Campus")
        resp = call_responder("people", "People/GetForExternal/JANELIA_SITE/7")
    COUNT['people'] = len(resp)
    output = {'boomerang': [], 'new': [], 'skipped': []}
    email_entries = {'new': [], 'boomerang': [], 'alumni': [], 'skipped': [], 'skipped_orgs': {}}
    for person in tqdm(resp):
        # People records are trusted to have these fields, but one malformed
        # record shouldn't abort the whole run: an unknown location is treated
        # as not-Janelia (skip), a missing businessTitle as not-alumni, and a
        # record with no employeeId (unmatchable) is logged and skipped.
        if person.get('locationName') != 'Janelia Research Campus':
            COUNT['not_janelia'] += 1
            continue
        eid = person.get('employeeId')
        if not eid:
            LOGGER.warning(f"No employeeId for {person.get('nameFirstPreferred', '?')} "
                           f"{person.get('nameLastPreferred', '?')} - skipping")
            COUNT['no_empid'] += 1
            continue
        if eid in orcid and ('enabled' in person and not person['enabled']):
            # People says this record isn't active - update the flag in orcid if necessary
            set_alumni(person, orcid, email_entries, orcid_uid)
        elif eid in orcid:
            if orcid[eid]:
                # Person is active in orcid
                if person.get('businessTitle') == 'JRC Alumni':
                    set_alumni(person, orcid, email_entries, orcid_uid)
                else:
                    COUNT['already_active'] += 1
            elif person.get('businessTitle') != 'JRC Alumni':
                # People says active, orcid says alumni - boomerang!
                unset_alumni(person, output, email_entries, orcid_uid)
            else:
                # People says alumni - update the flag in orcid if necessary
                set_alumni(person, orcid, email_entries, orcid_uid)
        else:
            # Person is in People but not orcid - insert record
            add_new_record(person, output, email_entries)
    # Write output files (microsecond-resolution timestamp so back-to-back runs
    # don't overwrite each other's output files)
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S%f")
    new_fname = None
    for key, val in output.items():
        if val:
            fname = f"{timestamp}_{key}.json"
            with open(fname, "w", encoding="utf-8") as outfile:
                outfile.write("[" + ",\n".join(val) + "]")
            if key == 'new':
                new_fname = fname
    if ARG.TEST or (ARG.WRITE and (COUNT['new'] or COUNT['boomerang'] or COUNT['set_alumni'])):
        generate_email(email_entries, new_fname)
    print(f"Records from People:            {COUNT['people']:,}")
    print(f"Already active:                 {COUNT['already_active']:,}")
    print(f"Skipped (no People record):     {COUNT['skipped_no_record']:,}")
    print(f"Skipped (no organization):      {COUNT['skipped_no_org']:,}")
    print(f"Skipped (organization ignored): {COUNT['skipped_ignored_org']:,}")
    print(f"Not at Janelia:                 {COUNT['not_janelia']:,}")
    print(f"No employee ID:                 {COUNT['no_empid']:,}")
    print(f"Already alumni (no change):     {COUNT['people_alumni']:,}")
    print(f"JRC Alumni set:                 {COUNT['set_alumni']:,}")
    print(f"Boomerangs:                     {COUNT['boomerang']:,}")
    print(f"New employees:                  {COUNT['new']:,}")
    print(f"Records inserted:               {COUNT['insert']:,}")
    print(f"Records updated:                {COUNT['update']:,}")
# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Sync People to MongoDB:orcid")
    PARSER.add_argument('--name', dest='NAME', action='store',
                        default=None, help='Name to search for')
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
        REST = JRC.get_config("rest_services")
    except Exception as err:
        terminate_program(err)
    initialize_program()
    update_orcid()
    terminate_program()
