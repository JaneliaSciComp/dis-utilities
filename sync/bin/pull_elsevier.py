''' pull_elsevier.py
    Sync works from Elsevier
'''

__version__ = '1.0.0'

import argparse
import collections
from operator import attrgetter
import sys
from tqdm import tqdm
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL

# pylint: disable=broad-exception-caught,broad-exception-raised,logging-fstring-interpolation

# Database
DB = {}
# Counters
COUNT = collections.defaultdict(lambda: 0, {})
# Global variables
ARG = DISCONFIG = LOGGER = None

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
        dbo = attrgetter(f"{source}.prod.read")(dbconfig)
        LOGGER.info(f"Connecting to {dbo.name} prod on {dbo.host} as {dbo.user}")
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)


def get_janelia_works():
    ''' Get author works
        Keyword arguments:
          None
        Returns:
          List of works
    '''
    suffix = "metadata/article?query=aff%28janelia%29&httpAccept=application/json&count=200"
    rows = []
    part = 1
    while True:
        try:
            resp = JRC.call_elsevier(suffix)
        except Exception as err:
            terminate_program(err)
        for row in resp['search-results']['entry']:
            if 'prism:coverDate' in row \
               and row['prism:coverDate'] >= DISCONFIG['min_publishing_date']:
                rows.append(row['prism:doi'].lower())
        print(f"Got part {part}: found {len(rows)} works")
        part += 1
        if 'link' in resp['search-results']:
            suffix = None
            for link in resp['search-results']['link']:
                if link['@ref'] == 'next':
                    suffix = link['@href'].replace('https://api.elsevier.com/content/', '')
        if not suffix:
            break
    LOGGER.debug(f"Found {len(rows)} works")
    return rows


def processing():
    ''' Processing
        Keyword arguments:
          None
        Returns:
          None
    '''
    dois = get_janelia_works()
    COUNT['read'] = len(dois)
    to_process = []
    for doi in tqdm(dois, desc="Processing DOIs"):
        try:
            row = DL.get_doi_record(doi, coll=DB['dis']['dois'])
            if row:
                COUNT['in_dois'] += 1
            else:
                to_process.append(doi)
        except Exception as err:
            print(f"Error getting DOI record for {doi}: {err}")
    print(f"DOIs to process: {len(to_process)}")
    with open('elsevier_ready.txt', 'w', encoding='utf-8') as fileout:
        for doi in to_process:
            fileout.write(doi + '\n')
            COUNT['ready'] += 1
    print(f"DOIs read from Elsevier:         {COUNT['read']:,}")
    print(f"DOIs already in database:        {COUNT['in_dois']:,}")
    print(f"DOIs ready for processing:       {COUNT['ready']:,}")

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Find new works for current lab heads")
    PARSER.add_argument('--test', dest='TEST', action='store_true',
                        default=False, help='Send email to developer')
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
