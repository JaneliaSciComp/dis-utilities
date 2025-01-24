""" template.py
    Template program that connects to DIS database
"""

__version__ = '1.0.0'

import argparse
import json
from operator import attrgetter
import os
import sys
from time import sleep
import requests
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Database
DB = {}

def terminate_program(msg=None):
    ''' Terminate the program gracefully
        Keyword arguments:
          msg: error message
        Returns:
          None
    '''
    if msg:
        LOGGER.critical(msg)
    sys.exit(-1 if msg else 0)


def initialize_program():
    ''' Intialize the program
        Keyword arguments:
          None
        Returns:
          None
    '''
    # API key
    if "S2_API_KEY" not in os.environ:
        terminate_program("Missing token - set in S2_API_KEY environment variable")
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


def s2_citation_count(doi, fmt='plain'):
    ''' Get citation count from Semantic Scholar
        Keyword arguments:
          doi: DOI
          fmt: format (plain or html)
        Returns:
          Citation count
    '''
    url = f"https://api.semanticscholar.org/graph/v1/paper/DOI:{doi}?fields=citationCount"
    headers = {'x-api-key': os.environ['S2_API_KEY']}
    loop = 0
    while loop <= 5:
        loop += 1
        try:
            LOGGER.debug(f"Try {loop} for doi")
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code == 429:
                #LOGGER.warning("Rate limit exceeded")
                sleep(1)
                continue
            elif resp.status_code == 404:
                #LOGGER.warning(f"{doi} was not found")
                return 'DOI not found in S2'
            elif resp.status_code != 200:
                LOGGER.warning(f"Failed {resp.status_code}")
                return 0
            data = resp.json()
            cnt = data['citationCount']
            if fmt == 'html':
                cnt = f"<a href='{app.config['S2']}{data['paperId']}' target='_blank'>" \
                      + f"{cnt}</a>"
            return cnt
        except Exception:
            return 0


def processing():
    ''' Main processing routine
        Keyword arguments:
          None
        Returns:
          None
    '''
    with open(ARG.FILE, 'r', encoding='ascii') as instream:
        for doi in instream.read().splitlines():
            print(f"{doi.lower().strip()}\t{s2_citation_count(doi)}")
            sleep(1)

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Template program")
    PARSER.add_argument('--file', dest='FILE', action='store',
                        help='File of DOIs')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, prod)')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Write to database/config system')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    processing()
    terminate_program()
