''' convert_diacritics.py
    Add new names for names with diacritics
'''

__version__ = '1.0.0'

import argparse
import collections
from operator import attrgetter
import sys
from tqdm import tqdm
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


def processing():
    ''' Process the data
        Keyword arguments:
          None
        Returns:
          None
    '''
    try:
        rows = DB['dis'].orcid.find({})
    except Exception as err:
        terminate_program(err)
    for row in tqdm(rows):
        COUNT['read'] += 1
        diacritics = []
        for given in row['given']:
            stripped = JRC.convert_diacritics(given)
            if stripped is not None and stripped not in row['given']:
                diacritics.append(stripped)
                row['given'].append(stripped)
        for family in row['family']:
            stripped = JRC.convert_diacritics(family)
            if stripped is not None and stripped not in row['family']:
                diacritics.append(stripped)
                row['family'].append(stripped)
        if diacritics:
            LOGGER.debug(f"{row['given']} {row['family']} -> Add {diacritics}")
            payload = {'_id': row['_id'], 'given': row['given'], 'family': row['family']}
            if ARG.WRITE:
                try:
                    DB['dis']['orcid'].update_one({'_id': row['_id']}, {"$set": payload})
                except Exception as err:
                    terminate_program(err)
            else:
                print(payload)
            COUNT['updated'] += 1
    print(f"Records read:    {COUNT['read']}")
    print(f"Records updated: {COUNT['updated']}")

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Add new names for names with diacritics")
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Flag, write to database')
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
