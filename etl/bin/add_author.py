''' add_author.py
    Add authors from an Excel spreadsheet or command line
'''

__version__ = '1.0.0'

import argparse
import collections
import json
from operator import attrgetter
import sys
import pandas as pd
import jrc_common.jrc_common as JRC

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Database
DB = {}
# Counters
COUNT = collections.defaultdict(lambda: 0, {})
# Global variables
ARG = LOGGER = None

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
        dbo = attrgetter(f"{source}.prod.write")(dbconfig)
        LOGGER.info(f"Connecting to {dbo.name} prod on {dbo.host} as {dbo.user}")
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)


def handle_single_author(row):
    ''' Handle a single author
        Keyword arguments:
          row: author row (dict)
        Returns:
          None
    '''
    present = False
    for family in row['Family name']:
        for given in row['Given name']:
            payload = {'family': family, 'given': given}
            try:
                cnt = DB['dis'].orcid.count_documents(payload)
                if cnt >= 1:
                    LOGGER.error(f"{given} {family} is already in the database")
                    COUNT['present'] += 1
                    present = True
                    break
            except Exception as err:
                terminate_program(err)
        if present:
            break
    if present:
        return
    payload = {'given': row['Given name'], 'family': row['Family name'],
               'employeeType': 'Employee', 'alumni': True}
    if row.get('Employee ID') and not pd.isna(row.get('Employee ID')):
        payload['employeeId'] = row['Employee ID']
    if row.get('User ID') and not pd.isna(row.get('User ID')):
        payload['userIdO365'] = row['User ID']
    else:
        payload['userIdO365'] = f"{payload['family'][0].upper()}" \
                                f"{payload['given'][0][0].upper()}@hhmi.org"
    if row.get('Hire date'):
        payload['hireDate'] = row['Hire date']
    COUNT['added'] += 1
    if not ARG.WRITE:
        print(payload)
        return
    try:
        DB['dis'].orcid.insert_one(payload)
        COUNT['inserted'] += 1
    except Exception as err:
        terminate_program(err)
    return


def processing():
    ''' Process the data
        Keyword arguments:
          None
        Returns:
          None
    '''
    if ARG.FILE:
        try:
            author_data = pd.read_excel(ARG.FILE)
        except Exception as err:
            terminate_program(f"Could not read {ARG.FILE}: {err}")
        LOGGER.info(f"Read {len(author_data)} row{'' if len(author_data) == 1 else 's'}" \
                    f" from {ARG.FILE}")
        rows = author_data.to_dict('records')
        LOGGER.info(f"Converted dataframe to {len(rows)} row{'' if len(rows) == 1 else 's'}")
    else:
        rows = [{'Given name': ARG.GIVEN, 'Family name': ARG.FAMILY}]
        if ARG.EID:
            rows[0]['Employee ID'] = ARG.EID
        if ARG.USER:
            rows[0]['User ID'] = ARG.USER
    for row in rows:
        COUNT['read'] += 1
        row['Given name'] = row['Given name'].replace("'", '"')
        try:
            row['Given name'] = json.loads(row['Given name'])
        except Exception as err:
            LOGGER.error(f"Could not load {row['Given name']}: {err}")
            terminate_program(err)
        if isinstance(row['Given name'], str):
            row['Given name'] = [row['Given name']]
        if isinstance(row['Family name'], str):
            row['Family name'] = [row['Family name']]
        else:
            row['Family name'] = json.loads(row['Family name'])
        handle_single_author(row)
    print(f"Records read:            {COUNT['read']}")
    print(f"Records already present: {COUNT['present']}")
    print(f"Records added:           {COUNT['added']}")
    if ARG.WRITE:
        print(f"Records inserted:        {COUNT['inserted']}")

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Add authors")
    GROUP = PARSER.add_mutually_exclusive_group(required=True)
    GROUP.add_argument('--file', dest='FILE', action='store',
                       help='Excel spreadsheet')
    PARSER.add_argument('--given', dest='GIVEN', action='store',
                        help='Given name')
    GROUP.add_argument('--family', dest='FAMILY', action='store',
                       help='Family name')
    PARSER.add_argument('--eid', dest='EID', action='store',
                        help='Employee ID')
    PARSER.add_argument('--user', dest='USER', action='store',
                        help='User ID')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Flag, Send email')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    DISCONFIG = JRC.simplenamespace_to_dict(JRC.get_config("dis"))
    REST = JRC.get_config("rest_services")
    initialize_program()
    processing()
    terminate_program()
