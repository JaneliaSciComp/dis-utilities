''' find_citing_works.py
    Find citing works for a given DOI (or list of DOIs)
    Usage:
        python find_citing_works.py --doi 10.1038/s41586-020-2649-2
        python find_citing_works.py --file dois.txt
    Output:
        citing_works.json
    Example:
        python find_citing_works.py --doi 10.1038/s41586-020-2649-2
'''

import argparse
import collections
import json
from operator import attrgetter
import sys
import time
import pyalex
import tqdm
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL

#pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Parms
ARG = LOGGER = None
# Database
DB = {}
# Counters
COUNT = collections.defaultdict(int)


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
        dbo = attrgetter(f"{source}.prod.read")(dbconfig)
        LOGGER.info(f"Connecting to {dbo.name} {dbo.host} as {dbo.user}")
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)


def get_dois(works):
    ''' Get citingDOIs from the works
        Keyword arguments:
          works: list of works
        Returns:
          list of DOIs
    '''
    dois = []
    if works:
        COUNT['cited'] += 1
    else:
        COUNT['not_cited'] += 1
    for itm in works:
        dois = []
        if works:
            COUNT['cited'] += 1
        else:
            COUNT['not_cited'] += 1
        for itm in works:
            if 'doi' in itm and itm['doi']:
                dois.append(itm['doi'])
    return dois


def process_dois():
    ''' Process the DOIs
        Keyword arguments:
          None
        Returns:
          None
    '''
    cdict = {}
    pyalex.config.email = "svirskasr@hhmi.org"
    if ARG.DOI:
        dois = [ARG.DOI]
    else:
        with open(ARG.FILE, 'r', encoding='ascii') as file:
            dois = [line.strip() for line in file if line.strip()]
    for doi in tqdm.tqdm(dois, desc="Processing DOIs"):
        if len(dois) > 1:
            time.sleep(.11)
        COUNT['read'] += 1
        try:
            work = DL.get_doi_record(doi, coll=None, source='openalex')
        except Exception as err:
            LOGGER.warning(f"Error getting OpenAlex record for {doi}: {err}")
            COUNT['error'] += 1
        if not work or 'id' not in work:
            if ARG.VERBOSE:
                LOGGER.warning(f"No record found for {doi}")
            COUNT['not_found'] += 1
            continue
        COUNT['found'] += 1
        oaid = work['id'].split('/')[-1]
        try:
            works = pyalex.Works().filter(cites=oaid).get()
        except Exception as err:
            LOGGER.warning(f"Error getting citing works for {doi}: {err}")
            COUNT['error'] += 1
            continue
        dois = get_dois(works)
        cdict[doi] = dois
    # Write results to JSON file
    output_file = 'citing_works.json'
    LOGGER.info("Writing results to %s", output_file)
    try:
        with open(output_file, 'w', encoding='utf-8') as outfile:
            json.dump(cdict, outfile, indent=2)
    except Exception as err:
        terminate_program(err)
    print(f"DOIs read:                 {COUNT['read']}")
    if COUNT['not_found']:
        print(f"DOIs not found:            {COUNT['not_found']}")
    print(f"DOIs found:                {COUNT['found']}")
    print(f"DOIs with citing works:    {COUNT['cited']}")
    print(f"DOIs without citing works: {COUNT['not_cited']}")
    if COUNT['error']:
        print(f"Errors:                    {COUNT['error']}")


if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(
        description="Sync DOIs")
    group = PARSER.add_mutually_exclusive_group(required=True)
    group.add_argument('--doi', dest='DOI', action='store',
                        help='DOI')
    group.add_argument('--file', dest='FILE', action='store',
                        help='File containing DOIs')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    process_dois()
    terminate_program()
