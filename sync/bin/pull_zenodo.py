''' pull_zenodo.py
    Sync works from Zenodo.
'''

__version__ = '1.1.0'

import argparse
import collections
import json
from operator import attrgetter
import os
import sys
from time import sleep
import requests
import jrc_common.jrc_common as JRC

# pylint: disable=broad-exception-caught,broad-exception-raised,logging-fstring-interpolation

# Database
DB = {}
# Counters
COUNT = collections.defaultdict(lambda: 0, {})
# Global variables
ARG = DISCONFIG = LOGGER = None
NOJANELIA = []

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
    ''' Initialize the program
        Keyword arguments:
          None
        Returns:
          None
    '''
    if "ZENODO_API_KEY" not in os.environ:
        terminate_program("Missing API key - set in ZENODO_API_KEY environment variable")
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


def get_janelia_works():
    ''' Get DOIs from Zenodo
        Keyword arguments:
          None
        Returns:
          List of records
    '''
    rows = []
    part = 1
    url = f"https://zenodo.org/api/records?q={ARG.TERM}&size=100"
    while True:
        try:
            response = requests.get(url, timeout=20,
                                    headers={'Authorization': f'Bearer {os.environ["ZENODO_API_KEY"]}'})
        except Exception as err:
            terminate_program(err)
        if not response:
            terminate_program(f"Error in response from Zenodo: {response}")
        resp = response.json()
        print(f"{len(resp['hits']['hits'])}/{resp['hits']['total']}")
        if 'hits' not in resp or not resp['hits']:
            terminate_program(f"Error in response from Zenodo: {resp['hits']}")
        rows.extend(resp['hits']['hits'])
        print(f"Got part {part}: found {len(rows)} works")
        part += 1
        if 'links' not in resp or 'next' not in resp['links'] or not resp['links']['next']:
            break
        url = resp['links']['next']
        sleep(.25)
    return rows


def get_dois():
    ''' Get DOIs from Zenodo
        Keyword arguments:
          None
        Returns:
          List of DOIs
    '''
    try:
        rows = DB['dis'].orcid.find({'orcid': {'$exists': True}, 'workerType': 'Employee'})
    except Exception as err:
        terminate_program(err)
    orcids = []
    for row in rows:
        orcids.append(row['orcid'])
    LOGGER.info(f"ORCIDs: {len(orcids):,}")
    rows = get_janelia_works()
    dois = []
    for hit in rows:
        COUNT['read'] += 1
        doi = hit['doi'].lower()
        if 'metadata' not in hit or 'creators' not in hit['metadata']:
            continue
        found = False
        for creator in hit['metadata']['creators']:
            if 'affiliation' in creator and creator['affiliation']:
                if 'Janelia' in creator['affiliation']:
                    dois.append(doi)
                    COUNT['found'] += 1
                    found = True
                    break
            if 'orcid' in creator and creator['orcid'] and creator['orcid'] in orcids:
                dois.append(doi)
                COUNT['found'] += 1
                found = True
                break
        if not found:
            NOJANELIA.append(hit)
    return dois


def processing():
    ''' Processing
        Keyword arguments:
          None
        Returns:
          None
    '''
    dois = get_dois()
    to_process = []
    for doi in dois:
        try:
            row = DB['dis'].dois.find_one({'doi': doi})
        except Exception as err:
            terminate_program(err)
        if row:
            COUNT['already'] += 1
            continue
        to_process.append(doi)
    COUNT['ready'] = len(to_process)
    if to_process:
        with open('zenodo_ready.txt', 'w', encoding='utf-8') as fileout:
            for doi in to_process:
                fileout.write(doi + '\n')
    if NOJANELIA:
        with open('zenodo_nojanelia.json', 'w', encoding='utf-8') as fileout:
            fileout.write(json.dumps(NOJANELIA, indent=4, default=str))
    print(f"DOIs in Zenodo:                   {COUNT['read']}")
    print(f"Janelia DOIs:                     {COUNT['found']}")
    print(f"Already in database:              {COUNT['already']}")
    print(f"DOIs with no Janelia affiliation: {len(NOJANELIA)}")
    print(f"DOIs ready for processing:        {COUNT['ready']}")

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Update the MongoDB dois collection with PMIDs from NCBI")
    PARSER.add_argument('--term', dest='TERM', action='store',
                        default='Janelia', help='Search term')
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
    processing()
    terminate_program()
