''' update_janelians_from_people.py
    Update active Janelians in the MongoDB orcid collection with
    data (names, affiliation, employee types, teams) from the People system.
'''

__version__ = '3.4.0'

import argparse
import collections
import json
from operator import attrgetter
import os
import sys
from tqdm import tqdm
import jrc_common.jrc_common as JRC

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Database
DB = {}
# Counters
COUNT = collections.defaultdict(lambda: 0, {})
# Global variables
LOGGER = None
ARG = None
DISCONFIG = None

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
        if val in idresp and idresp[val] and idresp[val] != row[key][0]:
            if idresp[val] in row[key]:
                row[key].remove(idresp[val])
            row[key].insert(0, idresp[val])
            dirty = True
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
    ''' Reset affiliations, managed teams, and group
        Keyword arguments:
            row: record to reset
        Returns:
            None
    '''
    for key in ['affiliations', 'group', 'group_code', 'managed']:
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
    # Add ccDescr if this person doesn't already have a group
    if 'group' not in row and 'ccDescr' in idresp and idresp['ccDescr']:
        set_row(row, 'affiliations')
        if idresp['ccDescr'] not in row['affiliations']:
            row['affiliations'].append(idresp['ccDescr'])
            dirty = True
            if not bumped:
                bumped = True
                COUNT['affiliations'] += 1
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
    return dirty


def update_managed_teams(idresp, row):
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
    old_managed = row['managed'] if 'managed' in row else []
    for team in idresp['managedTeams']:
        if team['supOrgSubType'] == 'Lab' and team['supOrgName'].endswith(' Lab'):
            if team['supOrgCode'] in DISCONFIG['sup_ignore']:
                continue
            if lab:
                terminate_program(f"Multiple labs found for {idresp['nameFirstPreferred']} " \
                                  + idresp['nameLastPreferred'])
            lab = team['supOrgName']
            if 'group' not in row:
                dirty = True
            row['group'] = lab
            row['group_code'] = team['supOrgCode']
        else:
            set_row(row, 'managed')
            if team['supOrgName'] not in row['managed'] and team['supOrgSubType']:
                if team['supOrgSubType'] != 'Lab' or not team['supOrgName'].endswith(' Lab'):
                    row['managed'].append(team['supOrgName'])
                    if not dirty:
                        COUNT['managed'] += 1
                        dirty = True
        set_row(row, 'affiliations')
        if team['supOrgName'] not in row['affiliations']:
            row['affiliations'].append(team['supOrgName'])
            COUNT['affiliations'] += 1
            dirty = True
    if not dirty or 'managed' not in row:
        return dirty
    if sorted(old_managed) == sorted(row['managed']):
        COUNT['managed'] -= 1
        dirty = False
    if dirty:
        LOGGER.warning(f"{row['given'][0]} {row['family'][0]}: {old_managed} -> {row['managed']}")
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


def postprocessing(audit):
    ''' Print counts and write audit file
        Keyword arguments:
          audit: list of updates
        Returns:
          None
    '''
    print(f"Authors read from orcid:  {COUNT['orcid']:,}")
    print(f"Authors updated:          {COUNT['updated']:,}")
    print(f"  Names updated:          {COUNT['name']:,}")
    print(f"  Affiliations updated:   {COUNT['affiliations']:,}")
    print(f"  WorkerTypes updated:    {COUNT['workerType']:,}")
    print(f"  Managed teams updated:  {COUNT['managed']:,}")
    print(f"  Set to former employee: {COUNT['alumni']:,}")
    print(f"Authors written:          {COUNT['written']:,}")
    if audit:
        filename = 'people_orcid_updates.json'
        with open(filename, 'w', encoding='utf-8') as outfile:
            for row in audit:
                outfile.write(f"{json.dumps(row, indent=4, default=str)}\n")
        LOGGER.info(f"Wrote {len(audit)} updates to {filename}")


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
    if pdirty:
        COUNT['name'] += 1
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
    if pdirty or udirty or mdirty:
        dirty = True
    return dirty


def update_orcid():
    ''' Sync People to the orcid collection
        Keyword arguments:
          None
        Returns:
          None
    '''
    payload = {"employeeId": {"$exists": True},
               "alumni": {"$ne": True}}
    try:
        cnt = DB['dis']['orcid'].count_documents(payload)
        rows = DB['dis']['orcid'].find(payload)
    except Exception as err:
        terminate_program(err)
    audit = []
    for row in tqdm(rows, total=cnt, desc="Checking People"):
        if ARG.RESET:
            reset_record(row)
        COUNT['orcid'] += 1
        idresp = JRC.call_people_by_id(row['employeeId'])
        if not idresp:
            LOGGER.error(f"No People record for {row}")
            row['alumni'] = True
            COUNT['alumni'] += 1
            dirty = True
        else:
            dirty = record_updates(idresp, row)
        LOGGER.debug(json.dumps(row, indent=4, default=str))
        if dirty:
            audit.append(row)
            COUNT['updated'] += 1
            write_record(row)
    postprocessing(audit)

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Sync People to MongoDB:orcid")
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, prod)')
    PARSER.add_argument('--reset', dest='RESET', action='store_true',
                        default=False, help='Reset affiliations and managesTeams')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Write to database/config system')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    DISCONFIG = JRC.simplenamespace_to_dict(JRC.get_config("dis"))
    initialize_program()
    update_orcid()
    terminate_program()
