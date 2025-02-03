''' sync_people_to_orcid.py
    Update the MongoDB orcid collection with data from the People system.
'''

__version__ = '1.0.0'

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


def reset_record(row):
    ''' Reset affiliations and managed teams
        Keyword arguments:
            row: record to reset
        Returns:
            None
    '''
    if 'affiliations' in row:
        del row['affiliations']
    if 'managed' in row:
        del row['managed']


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
            set_row(row, 'affiliations')
            if team['supOrgName'] not in row['affiliations']:
                row['affiliations'].append(team['supOrgName'])
        else:
            set_row(row, 'managed')
            if team['supOrgName'] not in row['managed'] and team['supOrgSubType']:
                if team['supOrgSubType'] != 'Lab':
                    row['managed'].append(team['supOrgName'])
                set_row(row, 'affiliations')
                if team['supOrgName'] not in row['affiliations']:
                    row['affiliations'].append(team['supOrgName'])
                if not dirty:
                    COUNT['managed'] += 1
                    dirty = True
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
    print(f"Authors read from orcid: {COUNT['orcid']:,}")
    print(f"Authors updated:         {COUNT['updated']:,}")
    print(f"  Affiliations updated:  {COUNT['affiliations']:,}")
    print(f"  WorkerTypes updated:   {COUNT['workerType']:,}")
    print(f"  Managed teams updated: {COUNT['managed']:,}")
    print(f"Authors written:         {COUNT['written']:,}")
    if audit:
        filename = 'people_orcid_updates.txt'
        with open(filename, 'w', encoding='utf-8') as outfile:
            for row in audit:
                outfile.write(f"{json.dumps(row, indent=4, default=str)}\n")
        LOGGER.info(f"Wrote {len(audit)} updates to {filename}")


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
        dirty = False
        # Update affiliations
        if not idresp:
            LOGGER.error(f"No People record for {row}")
            continue
        if 'affiliations' in idresp and idresp['affiliations']:
            for aff in idresp['affiliations']:
                set_row(row, 'affiliations')
                if aff['supOrgName'] not in row['affiliations']:
                    row['affiliations'].append(aff['supOrgName'])
                    dirty = True
            if dirty:
                COUNT['affiliations'] += 1
        # Add ccDescr if this person doesn't already have a group
        if 'group' not in row and 'ccDescr' in idresp and idresp['ccDescr']:
            set_row(row, 'affiliations')
            if idresp['ccDescr'] not in row['affiliations']:
                row['affiliations'].append(idresp['ccDescr'])
        # Add supOrgName if we have no affiliations
        if 'affiliations' not in row and 'supOrgName' in idresp:
            row['affiliations'] = [idresp['supOrgName']]
            dirty = True
        # Update workerType
        if 'workerType' in idresp:
            if 'workerType' not in row or row['workerType'] != idresp['workerType']:
                row['workerType'] = idresp['workerType']
                dirty = True
                COUNT['workerType'] += 1
        # Update managed teams
        if update_managed_teams(idresp, row):
            dirty = True
        if 'affiliations' in row and not row['affiliations']:
            del row['affiliations']
        if 'managed' in row and not row['managed']:
            del row['managed']
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
    initialize_program()
    DISCONFIG = JRC.simplenamespace_to_dict(JRC.get_config("dis"))
    update_orcid()
    terminate_program()
