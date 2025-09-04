''' delete_dois.py
    Delete DOIs from the dois collection
'''

import argparse
import collections
from datetime import datetime
from operator import attrgetter
import sys
from tqdm import tqdm
import jrc_common.jrc_common as JRC

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# General variables
ARG = LOGGER = None
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
    ''' Initialize program
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
        dbo = attrgetter(f"{source}.{ARG.MANIFOLD}.write")(dbconfig)
        LOGGER.info(f"Connecting to {dbo.name} {ARG.MANIFOLD} on {dbo.host} as {dbo.user}")
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)


def process_ignore(doi):
    ''' Remove a DOI from the ignore list
        Keyword arguments:
          doi: DOI to process
        Returns:
          None
    '''
    try:
        resp = DB['dis'].to_ignore.find_one({"type": "doi", "key": doi})
    except Exception as err:
        terminate_program(err)
    if not resp:
        COUNT["missing"] += 1
        return
    try:
        resp = DB['dis'].to_ignore.delete_one({"type": "doi", "key": doi})
    except Exception as err:
        terminate_program(err)
    COUNT["deleted"] += 1


def delete_dois():
    ''' Delete DOIs from the database
        Keyword arguments:
          None
        Returns:
          None
    '''
    dois = []
    if ARG.DOI:
        dois.append(ARG.DOI)
    elif ARG.FILE:
        try:
            with open(ARG.FILE, "r", encoding="ascii") as instream:
                for doi in instream.read().splitlines():
                    dois.append(doi.lower().strip())
        except Exception as err:
            LOGGER.error(f"Could not process {ARG.FILE}")
            terminate_program(err)
    for doi in tqdm(dois):
        COUNT["read"] += 1
        if ARG.IGNORE:
            process_ignore(doi)
            continue
        try:
            row = DB['dis'].dois.find_one({"doi": doi})
        except Exception as err:
            terminate_program(err)
        missing = False
        if not row:
            missing = True
            COUNT["missing"] += 1
            LOGGER.warning(f"DOI {doi} not found in local database")
        if ARG.WRITE:
            if not missing:
                try:
                    resp = DB['dis'].dois.delete_one({"doi": doi})
                    COUNT['deleted'] += resp.deleted_count
                    LOGGER.warning(f"Deleted {doi}")
                except Exception as err:
                    terminate_program(f"Could not delete {doi} from dois collection: {err}")
            payload = {"type": "doi", "key": doi,
                       "inserted": datetime.today().replace(microsecond=0)}
            if ARG.REASON:
                payload["reason"] = ARG.REASON
            try:
                resp = DB['dis'].to_ignore.find_one({"type": "doi", "key": doi})
                if not resp:
                    resp = DB['dis'].to_ignore.insert_one(payload)
                    COUNT['inserted'] += 1
            except Exception as err:
                terminate_program(f"Could not insert {doi} into to_ignore collection: {err}")
    print(f"DOIs read:                 {COUNT['read']}")
    print(f"DOIs not found:            {COUNT['missing']}")
    print(f"DOIs deleted:              {COUNT['deleted']}")
    print(f"DOIs added to ignore list: {COUNT['inserted']}")


# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Delete DOIs from the dois collection")
    group = PARSER.add_mutually_exclusive_group(required=True)
    group.add_argument('--doi', dest='DOI', action='store',
                        help='DOI to delete')
    group.add_argument('--file', dest='FILE', action='store',
                        help='File of DOIs to delete')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, prod)')
    PARSER.add_argument('--reason', dest='REASON', action='store',
                        help='Reason to delete DOI (optional)')
    PARSER.add_argument('--ignore', dest='IGNORE', action='store_true',
                        default=False, help='Remove from ignore list only')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Actually delete DOIs')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    DISCONFIG = JRC.simplenamespace_to_dict(JRC.get_config("dis"))
    initialize_program()
    delete_dois()
    terminate_program()
