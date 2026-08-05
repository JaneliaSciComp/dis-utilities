''' sync_suporg_to_org_group.py
    Update the MongoDB org_group collection with data from the People system.
'''

__version__ = '1.1.0'

import argparse
import collections
import html
import json
from operator import attrgetter
import os
import sys
import jrc_common.jrc_common as JRC
import jrc_email.jrc_email as JE

# pylint: disable=broad-exception-caught,logging-fstring-interpolation,logging-not-lazy

# Database
DB = {}
# Global variables
ARG = DISCONFIG = LOGGER = None
IGNORE = {}
# Run summary for the email
COUNT = collections.defaultdict(lambda: 0, {})
CHANGES = []

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


def on_steering_committee(code, rec):
    ''' Check if the person is on the steering committee
        Keyword arguments:
          code: org code
          rec: person record
        Returns:
          True if the person is on the steering committee
    '''
    if "affiliations" not in rec or not rec['affiliations']:
        return False
    for aff in rec['affiliations']:
        if aff['supOrgCode'] == code and aff['type'] == 'Team Steering Committee Member':
            return True
    return False


def process_single_person(pid, code=None):
    ''' Call the People API for a single person
        Keyword arguments:
          pid: employee ID
          code: org code
        Returns:
          Dictionary of managed groups
    '''
    rec = JRC.call_people_by_id(pid)
    if not rec:
        terminate_program("User {pid} not found")
    LOGGER.debug(f"Processing {rec['nameFirstPreferred']} {rec['nameLastPreferred']}")
    if "managedTeams" not in rec:
        return {}
    if code and on_steering_committee(code, rec):
        return {}
    orgs = rec['managedTeams']
    managed = {}
    for org in orgs:
        if org['type'] != 'SupOrg Manager' or org['supOrgSubType'] == 'Lab':
            continue
        managed[org['supOrgCode']] = org['supOrgName']
    if managed:
        LOGGER.debug(f"{rec['nameFirstPreferred']} {rec['nameLastPreferred']} {managed}")
    return managed


def process_people_by_org(code, name):
    ''' Process people for a single org
        Keyword arguments:
          code: org code
          name: org name
        Returns:
          Dictionary of managed teams for one organization
    '''
    page = 0
    managed_teams = {}
    while True:
        LOGGER.debug(f"Getting people for {code} {name} ({page})")
        try:
            rec = JRC.call_people_by_suporg(code, page)
        except Exception as err:
            terminate_program(err)
        if not rec or 'people' not in rec or len(rec['people']) == 0:
            break
        for person in rec['people']:
            teams = process_single_person(person['userIdO365'], code)
            if teams:
                managed_teams.update(teams)
        page += 1
    return managed_teams


def process_single_org(code, name, total_orgs=None, processed_orgs=None):
    ''' Process an org recursively
        Keyword arguments:
          code: org code
          name: org name
          total_orgs: dictionary of all managed orgs found so far
          processed_orgs: set of org codes that have already been processed
        Returns:
          Dictionary of all managed orgs
    '''
    if total_orgs is None:
        total_orgs = {}
    if processed_orgs is None:
        processed_orgs = set()
    if code in processed_orgs:
        return total_orgs
    LOGGER.warning(f"Processing {code} {name}")
    processed_orgs.add(code)
    managed_orgs = process_people_by_org(code, name)
    if not managed_orgs:
        return total_orgs
    if code in managed_orgs:
        del managed_orgs[code]
    total_orgs.update(managed_orgs)
    for subcode, subname in managed_orgs.items():
        if subcode not in processed_orgs:
            process_single_org(subcode, subname, total_orgs, processed_orgs)
    return total_orgs


def process_list(raw, rec):
    ''' Process the list of organizations
        Keyword arguments:
          raw: raw list of organizations
          rec: record from the org_group collection
        Returns:
          None
    '''
    if 'members' not in rec:
        rec['members'] = []
    original_members = set(rec['members'])
    organizations = set()
    for org in raw:
        if org not in IGNORE:
            organizations.add(org)
    for org in rec['members']:
        if org not in organizations:
            organizations.add(org)
    organizations = sorted(list(organizations))
    print(json.dumps(organizations, indent=2))
    # Track the membership delta for the run-summary email. Computed regardless of
    # --write so a dry run still reports what would change.
    added = sorted(set(organizations) - original_members)
    removed = sorted(original_members - set(organizations))
    COUNT['groups'] += 1
    if added or removed:
        COUNT['groups_changed'] += 1
        COUNT['orgs_added'] += len(added)
        COUNT['orgs_removed'] += len(removed)
        CHANGES.append({'group': rec['group'], 'added': added, 'removed': removed})
    if not ARG.WRITE:
        return
    try:
        result = DB['dis']['org_group'].update_one({'group': rec['group']},
                                                   {'$set': {'members': organizations}}
                                                  )
        LOGGER.info(f"Updated {rec['group']}: {result.modified_count} " \
                    + f"record{'' if result.modified_count == 1 else 's'} modified")
        if added:
            LOGGER.info(f"Added organizations: {added}")
        if removed:
            LOGGER.info(f"Removed organizations: {removed}")
    except Exception as err:
        terminate_program(err)


def process_single_group():
    ''' Process a single organizational group
        Keyword arguments:
          None
        Returns:
          None
    '''
    payload = {}
    if ARG.ORG:
        payload = {'group': ARG.ORG}
    else:
        payload = {'manager': ARG.PID}
    try:
        rec = DB['dis']['org_group'].find_one(payload)
    except Exception as err:
        terminate_program(err)
    if ARG.ORG:
        ARG.PID = rec['manager']
    else:
        ARG.ORG = rec['group']
    LOGGER.info(f"Processing {ARG.ORG} ({ARG.PID})")
    organizations = set()
    pid = f"{ARG.PID}@hhmi.org"
    orgs = process_single_person(pid)
    # orgs is a supOrgCode: supOrgName dictionary
    for code, name in orgs.items():
        if name in organizations:
            continue
        res = process_single_org(code, name)
        for val in res.values():
            organizations.add(val)
        if name not in organizations and name not in IGNORE:
            organizations.add(name)
    process_list(organizations, rec)


def update_orgs():
    ''' Update one or more organization groups
        Keyword arguments:
          None
        Returns:
          None
    '''
    if ARG.ORG or ARG.PID:
        process_single_group()
        return
    for rec in DB['dis']['org_group'].find({}):
        ARG.ORG = rec['group']
        ARG.PID = rec['manager']
        process_single_group()


def generate_email():
    ''' Build and send the HTML run-summary email (jrc_email house style): KPI
        tiles for groups processed/changed and orgs added/removed, plus a table of
        each changed group's added/removed organizations. Recipient is the
        developer with --test, else the receivers list.
        Keyword arguments:
          None
        Returns:
          None
    '''
    run_data = JRC.get_run_data(__file__, __version__).strip()
    run_data += f" &middot; manifold: {ARG.MANIFOLD}"
    mode_label = 'WRITE' if ARG.WRITE else 'DRY RUN'
    mode_tone = 'good' if ARG.WRITE else 'warn'
    kpis = ''.join([
        JE.kpi_card(f"{COUNT['groups']:,}", "Groups processed", width='25%'),
        JE.kpi_card(f"{COUNT['groups_changed']:,}", "Groups changed",
                    'good' if COUNT['groups_changed'] else 'neutral', '25%'),
        JE.kpi_card(f"{COUNT['orgs_added']:,}", "Orgs added",
                    'good' if COUNT['orgs_added'] else 'neutral', '25%'),
        JE.kpi_card(f"{COUNT['orgs_removed']:,}", "Orgs removed",
                    'bad' if COUNT['orgs_removed'] else 'neutral', '25%'),
    ])
    if CHANGES:
        rows = ""
        for idx, chg in enumerate(CHANGES):
            bgc = "#f4f6f8" if idx % 2 == 0 else "#ffffff"
            added = html.escape(", ".join(chg['added'])) if chg['added'] else "&mdash;"
            removed = html.escape(", ".join(chg['removed'])) if chg['removed'] else "&mdash;"
            rows += (f'<tr bgcolor="{bgc}" style="background-color:{bgc};">'
                     f'<td style="padding:6px 12px;vertical-align:top;font-weight:600;">'
                     f'{html.escape(chg["group"])}</td>'
                     f'<td style="padding:6px 12px;vertical-align:top;color:#256029;">{added}</td>'
                     f'<td style="padding:6px 12px;vertical-align:top;color:#5b6b7c;">'
                     f'{removed}</td></tr>')
        table = (
            '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
            'style="border-collapse:collapse;font-size:13px;">'
            '<tr style="color:#5b6b7c;font-size:10.5px;text-transform:uppercase;'
            'letter-spacing:.03em;"><td style="padding:6px 12px;">Group</td>'
            '<td style="padding:6px 12px;">Orgs added</td>'
            '<td style="padding:6px 12px;">Orgs removed</td></tr>' + rows + '</table>')
        body = JE.body_row(
            JE.section_header(f"&#128101; Changed groups ({COUNT['groups_changed']:,})") + table)
    else:
        body = JE.body_row('<div style="font-size:13px;color:#5b6b7c;">'
                           'No org-group membership changes.</div>')
    msg = JE.render(os.path.basename(__file__), __version__, run_data,
                    mode_label, mode_tone, kpis, body)
    email = DISCONFIG['developer'] if ARG.TEST else DISCONFIG['receivers']
    try:
        LOGGER.info(f"Sending email to {email}")
        JRC.send_email(msg, DISCONFIG['sender'], email, "SupOrg to org-group sync", mime='html')
    except Exception as err:
        LOGGER.error(f"Could not send email: {err}")

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Sync People to MongoDB:orcid")
    group = PARSER.add_mutually_exclusive_group(required=False)
    group.add_argument('--pid', dest='PID', action='store',
                      help='People PID')
    group.add_argument('--org', dest='ORG', action='store',
                      help='Organization')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                      default='prod', choices=['dev', 'prod'],
                      help='MongoDB manifold (dev, prod)')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Write to database/config system')
    PARSER.add_argument('--test', dest='TEST', action='store_true',
                        default=False, help='Send email to developer only')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    DISCONFIG = JRC.simplenamespace_to_dict(JRC.get_config("dis"))
    initialize_program()
    update_orgs()
    if ARG.TEST or ARG.WRITE:
        generate_email()
    terminate_program()
