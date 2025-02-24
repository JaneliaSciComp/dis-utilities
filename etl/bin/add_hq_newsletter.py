""" add_hq_newsletter.py
    Add jrc_newsletter dates to DOIs that:
    - Don't have them
    - Are in Sulav's list of DOIs
    - DOIs currently exist in the dois collection
"""

__version__ = '1.0.0'

import argparse
import collections
import json
from operator import attrgetter
import sys
from rapidfuzz import fuzz, utils
from tqdm import tqdm
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Database
DB = {}
# General
ALL_TITLES = {}
TITLE = []
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


def find_doi(title):
    ''' Find a DOI for a title
        Keyword arguments:
          title: title
        Returns:
          DOI
    '''
    try:
        rec = DB['dis'].dois.find_one({'title': title})
        if rec:
            return rec['doi']
    except Exception as err:
        terminate_program(err)
    for alt in ALL_TITLES:
        score = fuzz.token_sort_ratio(title, alt, processor=utils.default_process)
        if score > 90:
            if score < 100:
                LOGGER.warning(f"Match: {title} {alt} {score}")
            rec = DB['dis'].dois.find_one({'title': alt})
            if not rec:
                LOGGER.error(f"Alternate title not found: {alt}")
                continue
            return rec['doi']
    return None


def processing():
    ''' Main processing routine
        Keyword arguments:
          None
        Returns:
          None
    '''
    with open(ARG.FILE, 'r', encoding='ascii') as instream:
        data = json.load(instream)
    for node in tqdm(data['nodes']):
        COUNT['read'] += 1
        doi = title = ''
        for key, val in node['node'].items():
            if key == 'DOI':
                doi = val.strip().lower().replace('https://doi.org/', '')
                doi = doi.replace('http://dx.doi.org/', '')
                doi = doi.replace('doi:', '').replace('doi: ', '')
                doi = doi.replace('\u200b', '')
            elif key == 'Title':
                title = val.strip()
        rec = None
        if not doi:
            doi = find_doi(title)
            if not doi:
                #LOGGER.error(json.dumps(node['node'], indent=2))
                TITLE.append(title)
                COUNT['missing_doi'] += 1
                continue
        try:
            if not rec:
                rec = DB['dis'].dois.find_one({'doi': doi})
            if not rec:
                COUNT['missing_from_dois'] += 1
                continue
            if 'jrc_newsletter' in rec:
                if rec['jrc_newsletter']:
                    COUNT['already'] += 1
                    continue
            rec['jrc_newsletter'] = '2006-01-01'
            COUNT['updated'] += 1
            if ARG.WRITE:
                DB['dis'].dois.update_one({'_id': rec['_id']}, {'$set': rec})
        except Exception as err:
            terminate_program(err)
    print(f"Read:                         {COUNT['read']:,}")
    print(f"Missing DOI:                  {COUNT['missing_doi']:,}")
    print(f"Missing from dois collection: {COUNT['missing_from_dois']:,}")
    print(f"Already has newsletter:       {COUNT['already']:,}")
    print(f"Updated:                      {COUNT['updated']:,}")
# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Add jrc_newsletter dates to DOIs")
    PARSER.add_argument('--file', dest='FILE', action='store',
                        default='export_with_title_updated.json', help='Tag JSON file')
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
