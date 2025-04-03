""" pull_biorxiv.py
    Find DOIs from bioRxiv that can be added to the dois collection.
"""

import argparse
import collections
from datetime import date, timedelta
from operator import attrgetter
import sys
from tqdm import tqdm
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

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
        manifold = ARG.MANIFOLD if source == 'dis' else 'prod'
        dbo = attrgetter(f"{source}.{manifold}.write")(dbconfig)
        LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, manifold, dbo.host, dbo.user)
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)


def doi_exists(doi):
    ''' Check if DOI exists in the database
        Keyword arguments:
          doi: DOI to check
        Returns:
          True if exists, False otherwise
    '''
    try:
        row = DB['dis']['dois'].find_one({"doi": doi})
    except Exception as err:
        terminate_program(err)
    return bool(row)


def get_dois_from_biorxiv():
    ''' Get DOIs from bioRxiv
        Keyword arguments:
          None
        Returns:
          List of DOIs
    '''
    start = str(date.today() - timedelta(days=ARG.DAYS))
    stop = str(date.today())
    offset = 0
    done = False
    check = {}
    parts = 0
    LOGGER.info("Getting DOIs from bioRxiv")
    while not done:
        query = f"{start}/{stop}/{offset}"
        response = JRC.call_biorxiv(query)
        if 'messages' in response:
            parts += 1
            if 'count' in response['messages'][0]:
                if response['messages'][0]['count'] < 100:
                    done = True
                else:
                    offset += 100
            else:
                done = True
                continue
        if 'collection' in response:
            for item in response['collection']:
                COUNT['read'] += 1
                if doi_exists(item['doi'].lower()):
                    COUNT['in_dois'] += 1
                    continue
                check[item['doi'].lower()] = item
    LOGGER.info(f"Got {len(check):,} DOIs from bioRxiv in {parts} part(s)")
    COUNT['read'] = len(check)
    return check


def check_corresponding_institution(item, resp, ready):
    ''' Parse an author record to see if there are any Janelia authors
        Keyword arguments:
          item: bioRxiv item
          resp: response from Crossref
          ready: list of DOIs ready for processing
        Returns:
          True or False
    '''

    if 'author_corresponding_institution' in item \
        and 'Janelia' in item['author_corresponding_institution']:
        if resp and 'message' in resp:
            LOGGER.info(f"Janelia found as corresponding institution for {item['doi']}")
            ready.append(item['doi'].lower())
            return True
        else:
            COUNT['asserted_crossref'] += 1
            LOGGER.error(f"{item['doi']} with Janelia corresponding institution not in Crossref")
    return False


def parse_authors(doi, msg, ready, review):
    ''' Parse an author record to see if there are any Janelia authors
        Keyword arguments:
          doi: DOI
          msg: Crossref message
          ready: list of DOIs ready for processing
          review: list of DOIs requiring review
        Returns:
          True if there are Janelia authors, otherwise False
    '''
    adet = DL.get_author_details(msg, DB['dis']['orcid'])
    if adet:
        janelians = []
        mode = None
        for auth in adet:
            if auth['janelian']:
                janelians.append(f"{auth['given']} {auth['family']} ({auth['match']})")
                if auth['match'] in ("ORCID", "asserted"):
                    mode = auth['match']
        if janelians:
            print(f"Janelians found for {doi}: {', '.join(janelians)}")
            if mode:
                ready.append(doi)
            else:
                review.append(doi)
            return True
    return False


def run_search():
    ''' Search for DOIs on bioRxiv that can be added to the dois collection
        Keyword arguments:
          None
        Returns:
          None
    '''
    check = get_dois_from_biorxiv()
    ready = []
    review = []
    for doi, item in tqdm(check.items(), desc='Crossref check'):
        resp = JRC.call_crossref(doi)
        if check_corresponding_institution(item, resp, ready):
            continue
        if resp and 'message' in resp:
            janelians = parse_authors(doi, resp['message'], ready, review)
            if not janelians:
                COUNT['no_janelians'] += 1
    if ready:
        LOGGER.info("Writing DOIs to biorxiv_ready.txt")
        with open('biorxiv_ready.txt', 'w', encoding='ascii') as outstream:
            for item in ready:
                outstream.write(f"{item}\n")
    if review:
        LOGGER.info("Writing DOIs to biorxiv_review.txt")
        with open('biorxiv_review.txt', 'w', encoding='ascii') as outstream:
            for item in review:
                outstream.write(f"{item}\n")
    print(f"DOIs read from bioRxiv:          {COUNT['read']:,}")
    print(f"DOIs already in database:        {COUNT['in_dois']:,}")
    print(f"DOIs not in Crossref (asserted): {COUNT['asserted_crossref']:,}")
    print(f"DOIs not in Crossref:            {COUNT['no_crossref']:,}")
    print(f"DOIs with no Janelian authors:   {COUNT['no_janelians']:,}")
    print(f"DOIs ready for processing:       {len(ready):,}")
    print(f"DOIs requiring review:           {len(review):,}")

# -----------------------------------------------------------------------------

if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(
        description="Sync DOIs from bioRxiv")
    PARSER.add_argument('--days', dest='DAYS', action='store',
                        default=7, type=int,
                        help='Number of days to go back for DOIs')
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
    run_search()
    terminate_program()
