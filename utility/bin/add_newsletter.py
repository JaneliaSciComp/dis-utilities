''' add_reviewed.py
    Update the jrc_newsletter date for one or more DOIs
'''

__version__ = '1.0.0'

import argparse
import collections
from datetime import datetime
from operator import attrgetter
import sys
from tqdm import tqdm
import jrc_common.jrc_common as JRC

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

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
        dbo = attrgetter(f"{source}.{ARG.MANIFOLD}.write")(dbconfig)
        LOGGER.info(f"Connecting to {dbo.name} {ARG.MANIFOLD} on {dbo.host} as {dbo.user}")
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)


def update_single_doi(doi):
    """ Update a single DOI with a newsletter date
        Keyword arguments:
          doi: DOI to update
        Returns:
          None
    """
    doi = doi.lower()
    LOGGER.info(doi)
    COUNT["dois"] += 1
    coll = DB['dis'].dois
    row = coll.find_one({"doi": doi})
    if not row:
        LOGGER.warning(f"{doi} was not found")
        COUNT["notfound"] += 1
        return
    if row.get('jrc_newsletter') and ARG.IGNORE:
        LOGGER.warning(f"{doi} already has a newsletter date")
        COUNT["ignore"] += 1
        return
    payload = {"jrc_newsletter": ARG.DATE}
    if ARG.WRITE:
        try:
            if ARG.REMOVE:
                coll.update_one({"doi": doi}, {"$unset": {"jrc_newsletter": 1}})
            else:
                coll.update_one({"doi": doi}, {"$set": payload})
        except Exception as err:
            terminate_program(err)
        COUNT["updated"] += 1


def process_dois():
    """ Process a list of DOIs
        Keyword arguments:
          None
        Returns:
          None
    """
    if not ARG.DATE:
        ARG.DATE = datetime.today().strftime('%Y-%m-%d')
    else:
        try:
            _ = datetime.strptime(ARG.DATE, '%Y-%m-%d')
        except ValueError:
            terminate_program(f"Supplied date {ARG.DATE} is not a valid date (YYYY-MM-DD)")
    if ARG.DOI:
        update_single_doi(ARG.DOI)
    elif ARG.FILE:
        try:
            with open(ARG.FILE, "r", encoding="ascii") as instream:
                for doi in tqdm(instream.read().splitlines(), desc="DOIs"):
                    update_single_doi(doi.lower().strip())
        except Exception as err:
            LOGGER.error(f"Could not process {ARG.FILE}")
            terminate_program(err)
    print(f"DOIs read:      {COUNT['dois']}")
    if COUNT['notfound']:
        print(f"DOIs not found: {COUNT['notfound']}")
    if COUNT['ignore']:
        print(f"DOIs ignored:   {COUNT['ignore']}")
    print(f"DOIs updated:   {COUNT['updated']}")
    if not ARG.WRITE:
        LOGGER.warning("Dry run successful, no updates were made")

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Add a reviewed date to one or more DOIs")
    GROUP_A = PARSER.add_mutually_exclusive_group(required=True)
    GROUP_A.add_argument('--doi', dest='DOI', action='store',
                         help='Single DOI to process')
    GROUP_A.add_argument('--file', dest='FILE', action='store',
                         help='File of DOIs to process')
    PARSER.add_argument('--date', dest='DATE', action='store',
                        help='Newsletter date (defaults to today). Format: YYYY-MM-DD')
    PARSER.add_argument('--remove', dest='REMOVE', action='store_true',
                        default=False, help='Remove jrc_newsletter from DOI(s)')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, prod)')
    PARSER.add_argument('--ignore', dest='IGNORE', action='store_true',
                        default=False, help='Ignore DOIs with newsletter dates')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Write to database')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    if ARG.DATE:
        if ARG.REMOVE:
            terminate_program("Specifying --date and --remove isn't permitted")
        try:
            datetime.strptime(ARG.DATE, '%Y-%m-%d')
        except ValueError:
            terminate_program(f"{ARG.DATE} is an invalid date")
    initialize_program()
    process_dois()
    terminate_program()
