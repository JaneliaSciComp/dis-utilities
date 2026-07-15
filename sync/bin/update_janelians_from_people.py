"""
Sync active Janelian records in the MongoDB orcid collection with current
data from the People system.

Usage:
    python update_janelians_from_people.py [--orcid ORCID] [--manifold dev|prod]
                                           [--alumni] [--reset] [--test] [--write]
                                           [--verbose] [--debug]

Requires a valid API key in the PEOPLE_API_KEY environment variable.

For each active Janelian in the orcid collection (or a single record when
--orcid is given), the script fetches the corresponding People record and
updates the following fields if they have changed:

    - Preferred given/family name ordering
    - Affiliations (from supOrgName, ccDescr, and managedTeams)
    - workerType
    - Managed teams and lab group
    - hireDate (set once, never overwritten)

If --alumni is set, records with no matching People entry are marked as
former employees (alumni=True) rather than causing an error.

If --reset is set, affiliations, group, and group_code are cleared before
updates are applied; useful for rebuilding stale affiliation data.

Changes are written to MongoDB only when --write is supplied. An audit file
(people_orcid_updates.json) is written for any run that produces updates.

An HTML summary email is sent when --test or --write is supplied: a header
banner (run data, DRY RUN/WRITE badge), KPI stat tiles (authors read/updated/
written, former employees), a change-type breakdown table, and an Authors
With Changes table listing every updated author (linked to their /userui/
record) with a plain-English diff of exactly what changed on their record.
"""

__version__ = '6.2.0'

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
ARG = DIS = LOGGER = None
IGNORE = {}
TIMEOUT = (requests.exceptions.ConnectTimeout, requests.exceptions.ReadTimeout,
           requests.exceptions.Timeout)
# HTML run-summary email palette (generate_email and its html_* helpers). Mirrors
# sync_citations.py/tag_janelia_acks.py's email convention: inline styles only (no
# <style> block/classes) for reliable rendering across email clients including
# older Outlook.
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
        LOGGER.info(f"Connecting to {dbo.name} {ARG.MANIFOLD} on {dbo.host} as {dbo.user}")
        try:
            DB['dis'] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)
    try:
        rows = DB['dis']['to_ignore'].find({"type": "suporg"})
        for row in rows:
            IGNORE[row['key']] = True
    except Exception as err:
        terminate_program(err)


def update_preferred_name(idresp, row):
    ''' Update preferred name
        Keyword arguments:
            idresp: response from People
            row: record to update
        Returns:
            dirty: indicates if record is dirty
    '''
    dirty = False
    old_given = row['given'].copy()
    old_family = row['family'].copy()
    name = {'given': 'nameFirstPreferred',
            'family': 'nameLastPreferred'}
    for key,val in name.items():
        try:
            if val in idresp and idresp[val] and idresp[val] != row[key][0]:
                if idresp[val] in row[key]:
                    row[key].remove(idresp[val])
                row[key].insert(0, idresp[val])
                dirty = True
        except Exception as err:
            print(f"Key: {key}   Value: {val}\nidresp:")
            print(json.dumps(idresp, indent=2))
            print("row:")
            print(json.dumps(row, indent=2))
            terminate_program(err)
    if not dirty:
        return dirty
    if sorted(old_given) == sorted(row['given']) and sorted(old_family) == sorted(row['family']):
        dirty = False
    if dirty:
        COUNT['name'] += 1
        if sorted(old_given) != sorted(row['given']):
            LOGGER.warning(f"Given name changed: {old_given} -> {row['given']}")
        if sorted(old_family) != sorted(row['family']):
            LOGGER.warning(f"Family name changed: {old_family} -> {row['family']}")
    return dirty


def reset_record(row):
    ''' Reset affiliations and group (managed is reset in update_managed_teams)
        Keyword arguments:
            row: record to reset
        Returns:
            None
    '''
    for key in ['affiliations', 'group', 'group_code']:
        if key in row:
            del row[key]


def set_row(row, field):
    ''' Set field in row if not present
        Keyword arguments:
          row: record to update
          field: field to set
        Returns:
          None
    '''
    if field not in row:
        row[field] = []


def snapshot(row):
    ''' Capture the fields record_updates() may touch, for a before/after diff
        (see describe_changes). Taken before any People-driven mutation so it
        reflects the record's true prior state - lists are copied so later
        in-place appends elsewhere don't retroactively change this snapshot.
        Keyword arguments:
          row: record to snapshot
        Returns:
          dict of field name -> current value (or [] / None / False if absent)
    '''
    return {'given': row.get('given', []).copy(),
            'family': row.get('family', []).copy(),
            'affiliations': row.get('affiliations', []).copy(),
            'managed': row.get('managed', []).copy(),
            'group': row.get('group'),
            'workerType': row.get('workerType'),
            'hireDate': row.get('hireDate'),
            'alumni': row.get('alumni', False)}


def describe_changes(before, row):
    ''' Build a human-readable list of what changed on a record, by diffing a
        snapshot() taken before record_updates() against the record's final
        state. Diffing the actual before/after values (rather than threading
        change descriptions out of each update_* function) keeps this
        independent of those functions' internal dirty-tracking quirks.
        Keyword arguments:
          before: snapshot() dict taken before record_updates()
          row: record after record_updates() has run
        Returns:
          list of description strings (may be empty if nothing recognizable
          changed, e.g. a record only touched by --reset)
    '''
    changes = []
    if before['given'] != row.get('given', []) or before['family'] != row.get('family', []):
        old_name = f"{before['given'][0]} {before['family'][0]}" \
                   if before['given'] and before['family'] else '—'
        new_name = f"{row['given'][0]} {row['family'][0]}" \
                   if row.get('given') and row.get('family') else '—'
        changes.append(f"Name: {old_name} &rarr; {new_name}")
    if before['affiliations'] != row.get('affiliations', []):
        old_aff = ', '.join(before['affiliations']) or '—'
        new_aff = ', '.join(row.get('affiliations', [])) or '—'
        changes.append(f"Affiliations: {old_aff} &rarr; {new_aff}")
    if before['group'] != row.get('group'):
        changes.append(f"Group: {before['group'] or '—'} &rarr; {row.get('group') or '—'}")
    if before['managed'] != row.get('managed', []):
        old_mgd = ', '.join(before['managed']) or '—'
        new_mgd = ', '.join(row.get('managed', [])) or '—'
        changes.append(f"Managed teams: {old_mgd} &rarr; {new_mgd}")
    if before['workerType'] != row.get('workerType'):
        changes.append(f"Worker type: {before['workerType'] or '—'} &rarr; {row.get('workerType')}")
    if before['hireDate'] != row.get('hireDate'):
        changes.append(f"Hire date set: {row.get('hireDate')}")
    if not before['alumni'] and row.get('alumni'):
        changes.append("Marked as former employee")
    return changes


def update_affiliations(idresp, row):
    ''' Update affiliations
        Keyword arguments:
            idresp: response from People
            row: record to update
        Returns:
            dirty: indicates if record is dirty
    '''
    dirty = False
    bumped = False
    old_affiliations = row['affiliations'].copy() if 'affiliations' in row else []
    # Add affiliations from People
    if 'affiliations' in idresp and idresp['affiliations']:
        for aff in idresp['affiliations']:
            set_row(row, 'affiliations')
            if aff['supOrgName'] not in row['affiliations']:
                row['affiliations'].append(aff['supOrgName'])
                dirty = True
        if dirty:
            bumped = True
            COUNT['affiliations'] += 1
            LOGGER.warning(f"{row['given'][0]} {row['family'][0]}: {old_affiliations} -> " \
                           + f"{row['affiliations']}")
    # Add ccDescr if this person doesn't already have a group
    if 'group' not in row and 'ccDescr' in idresp and idresp['ccDescr']:
        set_row(row, 'affiliations')
        if idresp['ccDescr'] not in row['affiliations']:
            row['affiliations'].append(idresp['ccDescr'])
            dirty = True
            if not bumped:
                bumped = True
                COUNT['affiliations'] += 1
                LOGGER.warning(f"{row['given'][0]} {row['family'][0]}: {old_affiliations} -> " \
                               + f"{row['affiliations']}")
    # Add supOrgName if the supOrgSubType isn't Company or Division
    if 'supOrgName' in idresp and 'supOrgSubType' in idresp and \
        idresp['supOrgSubType'] not in ['Company', 'Division']:
        set_row(row, 'affiliations')
        if idresp['supOrgName'] not in row['affiliations']:
            row['affiliations'].append(idresp['supOrgName'])
            dirty = True
            if not bumped:
                bumped = True
                COUNT['affiliations'] += 1
                LOGGER.warning(f"{row['given'][0]} {row['family'][0]}: {old_affiliations} -> " \
                               + f"{row['affiliations']}")
    return dirty


def update_managed_teams(idresp, row):  # pylint: disable=too-many-branches
    ''' Update managed teams
        Keyword arguments:
          idresp: response from People
          row: record to update
        Returns:
          dirty: indicates if record is dirty
        '''
    if 'managedTeams' not in idresp:
        return False
    dirty = False
    lab = ''
    old_affiliations = row['affiliations'].copy() if 'affiliations' in row else []
    old_managed = row['managed'].copy() if 'managed' in row else []
    # Reset managed so it's rebuilt from scratch; old_managed holds the prior value for comparison
    row.pop('managed', None)
    for team in idresp['managedTeams']:
        if team['supOrgSubType'] == 'Lab' and team['supOrgName'].endswith(' Lab'):
            # Lab head
            if team['supOrgCode'] in IGNORE:
                continue
            if lab:
                LOGGER.warning(f"Multiple labs found for {idresp['nameFirstPreferred']} " \
                               + idresp['nameLastPreferred'])
            lab = team['supOrgName']
            if 'group' not in row or row['group'] != lab:
                dirty = True
            row['group'] = lab
            row['group_code'] = team['supOrgCode']
        else:
            # Managed team
            set_row(row, 'managed')
            if team['supOrgName'] not in row['managed'] and team['supOrgSubType']:
                if team['supOrgSubType'] != 'Lab' or not team['supOrgName'].endswith(' Lab'):
                    row['managed'].append(team['supOrgName'])
                    LOGGER.debug(f"{row['given'][0]} {row['family'][0]}: {old_managed} -> " \
                                 + f"{row['managed']}")
                    if not dirty:
                        COUNT['managed'] += 1
                        dirty = True
        set_row(row, 'affiliations')
        if team['supOrgName'] not in row['affiliations']:
            row['affiliations'].append(team['supOrgName'])
            COUNT['affiliations'] += 1
            LOGGER.debug(f"{row['given'][0]} {row['family'][0]}: {old_affiliations} -> " \
                         + f"{row['affiliations']}")
            dirty = True
    if not dirty or 'managed' not in row:
        return dirty
    if sorted(old_managed) == sorted(row['managed']):
        COUNT['managed'] -= 1
        dirty = False
    if dirty:
        LOGGER.debug(f"{row['given'][0]} {row['family'][0]}: {old_managed} -> {row['managed']}")
    return dirty


def write_record(row):
    ''' Write record to database
        Keyword arguments:
          row: record to write
        Returns:
          None
    '''
    if ARG.WRITE:
        result = DB['dis']['orcid'].replace_one({'_id': row['_id']}, row)
        if hasattr(result, 'matched_count') and result.matched_count:
            COUNT['written'] += result.matched_count


def record_updates(idresp, row):
    ''' Record updates
        Keyword arguments:
          idresp: response from People
          row: record to update
        Returns:
          dirty: indicates if record is dirty
    '''
    dirty = False
    # Update preferred name
    pdirty = update_preferred_name(idresp, row)
    # Update affiliations
    udirty = update_affiliations(idresp, row)
    # Update workerType
    if 'workerType' in idresp:
        if 'workerType' not in row or row['workerType'] != idresp['workerType']:
            row['workerType'] = idresp['workerType']
            dirty = True
            COUNT['workerType'] += 1
    # Update managed teams
    mdirty = update_managed_teams(idresp, row)
    if 'affiliations' in row and not row['affiliations']:
        del row['affiliations']
    if 'managed' in row and not row['managed']:
        del row['managed']
    # Update hire date
    if 'hireDate' not in row and 'hireDate' in idresp:
        try:
            hdate = idresp['hireDate'].split(' ')[0]
            date_object = datetime.strptime(hdate, "%m/%d/%Y")
            row['hireDate'] = date_object.strftime('%Y-%m-%d')
            dirty = True
            COUNT['hireDate'] += 1
        except Exception as err:
            LOGGER.error(f"Error updating hire date {idresp['hireDate']} {hdate} for " \
                         + f"{row['given'][0]} {row['family'][0]}: {err}")
    if pdirty or udirty or mdirty:
        dirty = True
    return dirty


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
        closing tag as literal visible text (only masked when the sibling
        content happens to be a <div> too, e.g. an empty-state message
        instead of a real table).  The spacer row substitutes for CSS
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


def html_changes_table(changes_log):
    ''' Build the "Authors with changes" table for the run-summary email: one
        row per updated author, name linked to their /userui/ record, with
        every change on that record listed underneath it.
        Keyword arguments:
          changes_log: list of {name, userId, changes} dicts from update_orcid()
        Returns:
          HTML table, or a plain "no updates" message if changes_log is empty
    '''
    if not changes_log:
        return (f'<div style="color:{EMAIL_GRAY};font-size:13px;">'
                'No authors were updated.</div>')
    rows = []
    for i, entry in enumerate(changes_log):
        striped = i % 2 == 0
        bgattr = f' bgcolor="{EMAIL_STRIPE_BG}"' if striped else ''
        bg = f'background-color:{EMAIL_STRIPE_BG};' if striped else ''
        r_l = 'border-radius:6px 0 0 6px;' if bg else ''
        r_r = 'border-radius:0 6px 6px 0;' if bg else ''
        name = html.escape(entry['name'])
        if entry['userId']:
            name = (f"<a href='https://dis.int.janelia.org/userui/{entry['userId']}' "
                    f"style='color:{EMAIL_NAVY};text-decoration:none;font-weight:600;'>"
                    f"{name}</a>")
        change_html = '<br>'.join(entry['changes']) if entry['changes'] \
                      else '<span style="color:#c9ced4;">no field-level change detected</span>'
        rows.append(
            f'<tr{bgattr} style="{bg}">'
            f'<td style="padding:8px 10px;{r_l}vertical-align:top;white-space:nowrap;">'
            f'{name}</td>'
            f'<td style="padding:8px 10px;{r_r}vertical-align:top;color:{EMAIL_GRAY};">'
            f'{change_html}</td></tr>')
    return ('<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
            'style="border-collapse:collapse;font-size:12.5px;">'
            f'<tr style="color:{EMAIL_GRAY};font-size:10.5px;text-transform:uppercase;'
            'letter-spacing:.03em;"><td style="padding:6px 10px;">Author</td>'
            '<td style="padding:6px 10px;">Changes</td></tr>'
            + "".join(rows) + '</table>')


def generate_email(changes_log):
    ''' Generate and send the HTML run-summary email: a header banner (run
        data, DRY RUN/WRITE badge), KPI stat tiles, a change-type breakdown,
        and the Authors with changes table. Built entirely from inline
        styles/tables (no <style> block) for compatibility with older email
        clients, matching the convention used by sync_citations.py and the
        acknowledgement sync scripts.
        Keyword arguments:
          changes_log: list of {name, userId, changes} dicts from update_orcid()
        Returns:
          None
    '''
    run_data = JRC.get_run_data(__file__, __version__).strip()
    mode_badge_bg = EMAIL_GREEN if ARG.WRITE else EMAIL_AMBER
    mode_label = 'WRITE' if ARG.WRITE else 'DRY RUN'

    kpis = ''.join([
        html_kpi_card(f"{COUNT['orcid']:,}", "Authors read"),
        html_kpi_card(f"{COUNT['updated']:,}", "Authors updated",
                      'good' if COUNT['updated'] else 'neutral'),
        html_kpi_card(f"{COUNT['written']:,}", "Authors written",
                      'good' if COUNT['written'] else 'neutral'),
        html_kpi_card(f"{COUNT['alumni']:,}", "Former employees",
                      'amber' if COUNT['alumni'] else 'neutral'),
    ])

    breakdown_rows = [
        ("Names updated", f"{COUNT['name']:,}"),
        ("Affiliations updated", f"{COUNT['affiliations']:,}"),
        ("Worker types updated", f"{COUNT['workerType']:,}"),
        ("Managed teams updated", f"{COUNT['managed']:,}"),
        ("Hire dates updated", f"{COUNT['hireDate']:,}"),
        ("Set to former employee", f"{COUNT['alumni']:,}"),
    ]
    breakdown_section = (html_section_header("&#128202; Change Breakdown")
                         + html_metric_rows(breakdown_rows))

    changes_section = (html_section_header(f"&#128100; Authors With Changes "
                                           f"({len(changes_log):,})")
                       + html_changes_table(changes_log))

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
        f'<tr><td style="padding:20px 28px 4px 28px;">{changes_section}</td></tr>'
        f'<tr><td bgcolor="{EMAIL_STRIPE_BG}" style="padding:18px 28px;'
        f'background-color:{EMAIL_STRIPE_BG};color:{EMAIL_GRAY};'
        f'font-size:11px;text-align:center;border-top:1px solid {EMAIL_BORDER};">'
        'Generated by update_janelians_from_people.py &middot; Data and Information Services '
        '&middot; Janelia Research Campus</td></tr>'
        '</table></td></tr></table></div>')

    subject = "Janelians updated from People system"
    email = DIS['developer'] if ARG.TEST else DIS['receivers']
    filename = 'people_orcid_updates.json'
    try:
        LOGGER.info(f"Sending email to {email}")
        JRC.send_email(msg, DIS['sender'], email, subject,
                       attachment=filename if changes_log else None, mime='html')
    except Exception as err:
        print(str(err))
        traceback.print_exc()
        terminate_program(err)


def postprocessing(audit, changes_log):
    ''' Print counts, write the audit file, and send the run-summary email.
        Keyword arguments:
          audit: list of updated records (full row dicts) for the audit file
          changes_log: list of {name, userId, changes} dicts for the email
        Returns:
          None
    '''
    msg = f"Authors read from orcid:  {COUNT['orcid']:,}\n" \
          + f"Authors updated:          {COUNT['updated']:,}\n" \
          + f"  Names updated:          {COUNT['name']:,}\n" \
          + f"  Affiliations updated:   {COUNT['affiliations']:,}\n" \
          + f"  WorkerTypes updated:    {COUNT['workerType']:,}\n" \
          + f"  Managed teams updated:  {COUNT['managed']:,}\n" \
          + f"  Hire dates updated:     {COUNT['hireDate']:,}\n" \
          + f"  Set to former employee: {COUNT['alumni']:,}\n" \
          + f"Authors written:          {COUNT['written']:,}"
    print(msg)
    filename = 'people_orcid_updates.json'
    if audit:
        with open(filename, 'w', encoding='utf-8') as outfile:
            for row in audit:
                outfile.write(f"{json.dumps(row, indent=4, default=str)}\n")
        LOGGER.info(f"Wrote {len(audit)} updates to {filename}")
    if not (ARG.TEST or (COUNT['updated'] and ARG.WRITE)):
        return
    generate_email(changes_log)


def update_orcid():  # pylint: disable=too-many-branches
    ''' Sync People to the orcid collection
        Keyword arguments:
          None
        Returns:
          None
    '''
    if ARG.ORCID:
        payload = {"orcid": ARG.ORCID}
    else:
        payload = {"employeeId": {"$exists": True},
                   "alumni": {"$ne": True}}
    try:
        cnt = DB['dis']['orcid'].count_documents(payload)
        rows = DB['dis']['orcid'].find(payload)
    except Exception as err:
        terminate_program(err)
    audit = []
    changes_log = []
    for row in tqdm(rows, total=cnt, desc="Checking People"):
        if ARG.RESET:
            reset_record(row)
        # managed is reset inside update_managed_teams after capturing old value
        COUNT['orcid'] += 1
        if 'employeeId' not in row:
            LOGGER.warning(f"No employeeId for {row.get('given', ['?'])[0]} "
                           f"{row.get('family', ['?'])[0]} — skipping")
            continue
        before = snapshot(row)
        try:
            idresp = JRC.call_people_by_id(row['employeeId'])
        except TIMEOUT as err:
            terminate_program(f"Request failed after multiple retries: {err}")
        except Exception as err:
            terminate_program(f"Error calling People by id: {err}")
        dirty = False
        if not idresp:
            if ARG.ALUMNI:
                LOGGER.warning(f"No People record for {row}")
                row['alumni'] = True
                now = datetime.now()
                row['alumni_date'] = now.strftime("%Y-%m-%d")
                COUNT['alumni'] += 1
                dirty = True
            else:
                terminate_program(f"No People record for {row}")
        else:
            dirty = record_updates(idresp, row)
        LOGGER.debug(json.dumps(row, indent=4, default=str))
        if dirty:
            audit.append(row)
            COUNT['updated'] += 1
            write_record(row)
            name = f"{row['given'][0]} {row['family'][0]}" \
                   if row.get('given') and row.get('family') else row.get('userIdO365', '?')
            changes_log.append({"name": name, "userId": row.get('userIdO365', ''),
                                "changes": describe_changes(before, row)})
    postprocessing(audit, changes_log)

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Sync People to MongoDB:orcid")
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, prod)')
    PARSER.add_argument('--orcid', dest='ORCID', action='store',
                        default=None, help='ORCID to update')
    PARSER.add_argument('--alumni', dest='ALUMNI', action='store_true',
                        default=False, help='Allow alumni processing')
    PARSER.add_argument('--reset', dest='RESET', action='store_true',
                        default=False, help='Reset affiliations and managedTeams')
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
    update_orcid()
    terminate_program()
