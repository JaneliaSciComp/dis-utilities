''' sync_suporg_to_org_group.py
    Update the MongoDB org_group collection with data from the People system.
'''

__version__ = '1.0.0'

import argparse
import json
from operator import attrgetter
import os
import sys
import jrc_common.jrc_common as JRC

# pylint: disable=broad-exception-caught,logging-fstring-interpolation,logging-not-lazy

# Database
DB = {}
# Global variables
LOGGER = None
ARG = None

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

def process_list(organizations, rec):
    ''' Process the list of organizations
        Keyword arguments:
          organizations: list of organizations
          rec: record from the org_group collection
        Returns:
          None
    '''
    original_members = set(rec['members'])
    for org in rec['members']:
        if org not in organizations:
            organizations.add(org)
    organizations = sorted(list(organizations))
    print(json.dumps(organizations, indent=2))
    if not ARG.WRITE:
        return
    try:
        result = DB['dis']['org_group'].update_one({'group': rec['group']},
                                                   {'$set': {'members': organizations}}
                                                  )
        LOGGER.info(f"Updated {rec['group']}: {result.modified_count} " \
                    + f"record{'' if result.modified_count == 1 else 's'} modified")
        if result.modified_count > 0:
            added = set(organizations) - original_members
            removed = original_members - set(organizations)
            if added:
                LOGGER.info(f"Added organizations: {sorted(list(added))}")
            if removed:
                LOGGER.info(f"Removed organizations: {sorted(list(removed))}")
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
        if name not in organizations:
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
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    update_orgs()
    terminate_program()
