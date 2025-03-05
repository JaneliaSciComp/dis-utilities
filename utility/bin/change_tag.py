""" change_tag.py
    Change a tag in DOIs to a new tag
"""

__version__ = '1.0.0'

import argparse
import collections
import json
from operator import attrgetter
import sys
import tqdm
import jrc_common.jrc_common as JRC

# pylint: disable=broad-exception-caught,logging-fstring-interpolation,logging-not-lazy

# Database
DB = {}
# Counters
COUNT = collections.defaultdict(lambda: 0, {})
# Globals
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
    ''' Intialize the program
        Keyword arguments:
          None
        Returns:
          None
    '''
    # Database
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


def update_doi(row, suporg):
    ''' Update a DOI
        Keyword arguments:
          row: DOI record
          suporg: Suporg record
        Returns:
          None
    '''
    new_tags = []
    names = []
    for tag in row['jrc_tag']:
        if tag['name'] != ARG.OLD:
            names.append(tag['name'])
            new_tags.append(tag)
    if ARG.NEW and suporg['name'] not in names:
        new_tags.append({'name': ARG.NEW, 'code': suporg['code'], 'type': 'suporg'})
    row['jrc_tag'] = sorted(new_tags, key=lambda x: x['name'])
    LOGGER.debug(row['doi'] + "\n" + json.dumps(row['jrc_tag'], indent=2, default=str))
    COUNT['updated'] += 1
    if ARG.WRITE:
        result = DB['dis']['dois'].update_one({'_id': row['_id']},
                                              {'$set': {'jrc_tag': row['jrc_tag']}})
        if result.modified_count:
            COUNT['written'] += 1


def processing():
    ''' Process the request
        Keyword arguments:
          None
        Returns:
          None
    '''
    try:
        cnt = DB['dis']['dois'].count_documents({'jrc_tag.name': ARG.OLD})
        if not cnt:
            terminate_program(f"Tag {ARG.OLD} was not found")
        COUNT['old'] = cnt
        rows = DB['dis']['dois'].find({'jrc_tag.name': ARG.OLD})
    except Exception as err:
        terminate_program(err)
    if ARG.NEW:
        try:
            suporg = DB['dis']['suporg'].find_one({'name': ARG.NEW})
            if not suporg:
                terminate_program(f"Suporg {ARG.NEW} was not found")
        except Exception as err:
            terminate_program(err)
    for row in tqdm.tqdm(rows, desc="Processing DOIs", total=cnt):
        update_doi(row, suporg)
    print(f"DOIs found:   {COUNT['old']}")
    print(f"DOIs updated: {COUNT['updated']}")
    print(f"DOIs written: {COUNT['written']}")

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Change a tag to a new tag")
    PARSER.add_argument('--old', dest='OLD', action='store',
                        required=True, help='Old tag')
    PARSER.add_argument('--new', dest='NEW', action='store',
                        help='New tag (optional)')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, [prod])')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Write to database')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    processing()
    terminate_program()
