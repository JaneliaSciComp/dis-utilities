""" sync_datacite_legal.py
    Sync legal (license) data from DataCite to the database
"""

import argparse
import collections
from operator import attrgetter
import sys
from tqdm import tqdm
import jrc_common.jrc_common as JRC
import dis_license_lib as DISL

# pylint: disable=broad-exception-caught,logging-fstring-interpolation,duplicate-code

DB = {}
COUNT = collections.defaultdict(lambda: 0, {})
ARG = LOGGER = None
LICENSE = {}

__version__ = '2.0.0'

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
    try:
        rows = DB['dis'].cvterm.find({'cv': 'license_mapping'})
    except Exception as err:
        terminate_program(err)
    for row in rows:
        LICENSE[row['name']] = row['display']
    try:
        rows = DB['dis'].cvterm.find({'cv': 'license'})
    except Exception as err:
        terminate_program(err)
    for row in rows:
        if row['name'] not in LICENSE:
            LICENSE[row['name']] = row['name']


def processing():
    ''' Processing
        Keyword arguments:
          None
        Returns:
          None
    '''
    payload = {"jrc_obtained_from": "DataCite", "jrc_license": {"$exists": False}}
    try:
        cnt = DB['dis'].dois.count_documents(payload)
        rows = DB['dis'].dois.find(payload)
    except Exception as err:
        terminate_program(err)
    LOGGER.info(f"Found {cnt:,} DOIs to process for DataCite legal data")
    COUNT['read'] = cnt
    for row in tqdm(rows, desc="Getting license data", total=cnt):
        result = DISL.resolve_license(row, LICENSE)
        if result.mapped:
            row['jrc_license'] = result.mapped
            COUNT['updated'] += 1
            if ARG.WRITE:
                try:
                    DB['dis'].dois.update_one({"doi": row['doi']},
                                               {"$set": {"jrc_license": row['jrc_license']}})
                except Exception as err:
                    terminate_program(err)
        else:
            LOGGER.error(f"No license found for {row['doi']}")
            COUNT['no_license'] += 1
        if result.pmc_skipped_no_id:
            COUNT['pmc_skipped_no_id'] += 1
        if result.pmc_429_exhausted:
            COUNT['pmc_429_exhausted'] += 1
        if result.pmc_unreachable:
            COUNT['pmc_unreachable'] += 1
        if result.unpaywall_not_indexed:
            COUNT['unpaywall_not_indexed'] += 1
        if result.unpaywall_unreachable:
            COUNT['unpaywall_unreachable'] += 1
        if result.openalex_unreachable:
            COUNT['openalex_unreachable'] += 1
    print(f"{'DOIs read:':<32}{COUNT['read']:,}")
    print(f"{'DOIs with no license:':<32}{COUNT['no_license']:,}")
    print(f"{'DOIs updated:':<32}{COUNT['updated']:,}")
    print(f"{'DOIs with no PMC ID:':<32}{COUNT['pmc_skipped_no_id']:,}")
    print(f"{'DOIs with PMC 429 exhausted:':<32}{COUNT['pmc_429_exhausted']:,}")
    print(f"{'DOIs skipped (PMC down):':<32}{COUNT['pmc_unreachable']:,}")
    print(f"{'DOIs not indexed in Unpaywall:':<32}{COUNT['unpaywall_not_indexed']:,}")
    print(f"{'DOIs skipped (Unpaywall down):':<32}{COUNT['unpaywall_unreachable']:,}")
    print(f"{'DOIs skipped (OpenAlex down):':<32}{COUNT['openalex_unreachable']:,}")

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Sync legal (license) data from DataCite to the database")
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, prod)')
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
