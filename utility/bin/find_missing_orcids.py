''' find_missing_orcids.py
    Search the People system for Janelians that have groups but are missing ORCIDs.
'''

import argparse
from operator import attrgetter
import os
import sys
import requests
from tqdm import tqdm
import jrc_common.jrc_common as JRC

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Database
DB = {}
# General
GROUPS = {}
IGNORE = {}
ORCIDS = {}
MISSING = []
# Counters
COUNT = {"missing": 0, "calls": 0}

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
    ''' Initialize program
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
        rows = DB['dis'].orcid.find({"group": {"$exists": True}})
    except Exception as err:
        terminate_program(err)
    for row in rows:
        GROUPS[row['group']] = True
    LOGGER.info(f"Found {len(GROUPS)} groups")
    try:
        rows = DB['dis']['to_ignore'].find({"type": "suporg"})
        for row in rows:
            IGNORE[row['key']] = True
    except Exception as err:
        terminate_program(err)
    LOGGER.info(f"Found {len(IGNORE)} suporgs to ignore")
    try:
        rows = DB['dis'].orcid.find({"orcid": {"$exists": True}})
    except Exception as err:
        terminate_program(err)
    for row in rows:
        if 'employeeId' in row:
            ORCIDS[row['employeeId']] = True
    LOGGER.info(f"Found {len(ORCIDS)} correlated users with ORCIDs")


def process_person(person):
    ''' Process one person, and if they have a lab, check if they have an ORCID
        Keyword arguments:
          person: person record
        Returns:
          True is the record is missing, False otherwise
    '''
    try:
        rec = JRC.call_people_by_id(person['employeeId'])
    except Exception as err:
        terminate_program(err)
    COUNT['calls'] += 1
    if not rec:
        return False
    if 'managedTeams' not in rec or not rec['managedTeams']:
        return False
    for team in rec['managedTeams']:
        if team['supOrgSubType'] == 'Lab' and team['supOrgName'].endswith(' Lab'):
            if team['supOrgCode'] in IGNORE:
                continue
            lab = team['supOrgName']
            if lab not in GROUPS or rec['employeeId'] not in ORCIDS:
                COUNT['missing'] += 1
                MISSING.append({'name': ' '.join([rec['nameFirstPreferred'],
                                                  rec['nameLastPreferred']]),
                                'id': rec['employeeId'],
                                'group': lab})
                return True
    return False


def perform_search():
    ''' Search the People system for Janelians
        Keyword arguments:
          None
        Returns:
          None
    '''
    url = 'https://hhmipeople-prod.azurewebsites.net/People/Search/ByOther/JANELIA_SITE'
    headers = {'APIKey': os.environ['PEOPLE_API_KEY'],
               'Content-Type': 'application/json'}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
    except Exception as err:
        terminate_program(err)
    people = resp.json()
    LOGGER.info(f"Found {len(people):,} Janelians in People")
    pbar = tqdm(people, desc='Missing records: 0')
    for person in pbar:
        if person['locationName'] == 'Janelia Research Campus' and \
           'employeeId' in person:
            if process_person(person) :
                pbar.set_description(f"Missing records: {COUNT['missing']}")
    if not MISSING:
        return
    maxl = {'name': 0, 'id': 0, 'group': 0}
    for person in MISSING:
        for key in maxl:
            if len(person[key]) > maxl[key]:
                maxl[key] = len(person[key])
    print(f"{'Name':{maxl['name']}}  {'ID':{maxl['id']}}  {'Group':{maxl['group']}}")
    print(f"{'-'*maxl['name']}  {'-'*maxl['id']}  {'-'*maxl['group']}")
    for person in MISSING:
        print(f"{person['name']:{maxl['name']}}  {person['id']:{maxl['id']}}  " \
              + f"{person['group']:{maxl['group']}}")
    print(f"Calls to People system: {COUNT['calls']}")
    print(f"Missing ORCIDs:         {COUNT['missing']}")


# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Look up a person by name")
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, prod)')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    perform_search()
    terminate_program()
