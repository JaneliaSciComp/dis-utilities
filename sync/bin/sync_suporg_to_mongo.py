''' sync_suporg_to_org_group.py
    Update the MongoDB suporg collection with data from the People system.
'''

__version__ = '1.0.0'

import argparse
import collections
from operator import attrgetter
import os
import sys
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL

# pylint: disable=broad-exception-caught,logging-fstring-interpolation,logging-not-lazy

# Database
DB = {}
# Global variables
ARG = DIS = LOGGER = None
IGNORE = {}
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
    try:
        rows = DB['dis']['to_ignore'].find({"type": "group"})
        for row in rows:
            IGNORE[row['key']] = True
    except Exception as err:
        terminate_program(err)


def update_suporgs():
    ''' Update supervisory organizations
        Keyword arguments:
          None
        Returns:
          None
    '''
    people = DL.get_supervisory_orgs(full=True)
    mongo = DL.get_supervisory_orgs(coll=DB['dis'].suporg)
    added = []
    for suporg, val in people.items():
        if suporg not in mongo and suporg not in IGNORE:
            LOGGER.info(f"Adding {suporg} with code {val['SUPORGCODE']}")
            payload = {'name': suporg, 'code': val['SUPORGCODE']}
            if 'active' in val and val['active']:
                payload['active'] = True
            if ARG.WRITE:
                DB['dis'].suporg.insert_one(payload)
            active = ' (active)' if 'active' in payload else ''
            added.append(f"{suporg}: {val['SUPORGCODE']}{active}")
            COUNT['added'] += 1
    print(f"Suporgs in People:  {len(people):,}")
    print(f"Suporgs in MongoDB: {len(mongo):,}")
    print(f"Suporgs added:      {COUNT['added']:,}")
    if (not added) or (not (ARG.TEST or ARG.WRITE)):
        return
    text = "The following suporgs have been added to the database:<br>"
    text += "<br>".join(added)
    email = DIS['developer'] if ARG.TEST else DIS['receivers']
    JRC.send_email(text, DIS['sender'], email, 'Suporgs added', mime='html')

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Sync People to MongoDB:suporg")
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                      default='prod', choices=['dev', 'prod'],
                      help='MongoDB manifold (dev, prod)')
    PARSER.add_argument('--test', dest='TEST', action='store_true',
                        default=False, help='Send email to developer')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Write to database')
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
    update_suporgs()
    terminate_program()
