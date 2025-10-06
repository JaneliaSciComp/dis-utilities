""" pull_protocolsio.py
    Find DOIs from protocols.io that can be added to the dois collection.
"""

import argparse
import collections
import json
from operator import attrgetter
import os
import sys
from time import sleep
from tqdm import tqdm
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL

# pylint: disable=broad-exception-caught,logging-fstring-interpolation,too-many-arguments,too-many-positional-arguments

# Parms
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
    ''' Intialize the program
        Keyword arguments:
          None
        Returns:
          None
    '''
    # API key
    if "PROTOCOLS_API_TOKEN" not in os.environ:
        terminate_program("Missing token - set in PROTOCOLS_API_TOKEN environment variable")
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


def get_dois_from_protocolsio():
    ''' Get DOIs from protocols.io
        Keyword arguments:
          None
        Returns:
          List of DOIs
    '''
    page = 1
    done = False
    check = {}
    LOGGER.info("Getting DOIs from protocols.io")
    suffix = "protocols?filter=public&key=Janelia&page_size=50&fields=doi"
    while not done:
        response = None
        try:
            response = JRC.call_protocolsio(suffix)
        except Exception as err:
            terminate_program(err)
        if 'items' in response:
            LOGGER.info(f"Page {page} has {len(response['items']):,} DOIs")
            for item in response['items']:
                doi = item['doi'].replace('dx.doi.org/', '').lower()
                if doi in check:
                    LOGGER.error("Duplicate DOI found: %s", doi)
                check[doi] = item
        if 'pagination' in response:
            if 'next_page' in response['pagination']:
                if response['pagination']['next_page']:
                    suffix = response['pagination']['next_page'].split('/')[-1]
                    page += 1
                else:
                    done = True
            else:
                done = True
    LOGGER.info(f"Got {len(check):,} DOIs from protocols.io in {page} part(s)")
    return check


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


def parse_authors(doi, msg, ready, review, nojanelians, alumni):
    ''' Parse an author record to see if there are any Janelia authors
        Keyword arguments:
          doi: DOI
          msg: Crossref message
          ready: list of DOIs ready for processing
          review: list of DOIs requiring review
          nojanelians: list of DOIs with no Janelian authors
          alumni: list of DOIs with alumni authors
        Returns:
          None
    '''
    if 'doi' not in msg:
        msg['doi'] = doi
    sleep(0.25)
    try:
        adet = DL.get_author_details(msg, DB['dis']['orcid'])
    except Exception as err:
        terminate_program(err)
    if adet:
        alum = []
        janelians = []
        mode = None
        for auth in adet:
            if auth['janelian']:
                janelians.append(f"{auth['given']} {auth['family']} ({auth['match']})")
                if auth['match'] in ("ORCID", "asserted"):
                    mode = auth['match']
            elif auth['alumni']:
                alum.append(f"{auth['given']} {auth['family']} ({auth['match']})")
        if janelians:
            print(f"Janelians found for {doi}: {', '.join(janelians)}")
            if mode:
                ready.append(doi)
            else:
                review.append(json.dumps(msg, indent=4, default=str))
            return
        if alum:
            alumni.append(json.dumps(msg, indent=4, default=str))
            return
        # DOIs with no Janelia authors are an issue because protocols.io sometimes
        # has the author's middle name as part of the family name. Why?!
        nojanelians.append(json.dumps(msg, indent=4, default=str))
        LOGGER.warning(json.dumps(adet, indent=4, default=str))


def run_search():
    ''' Search for DOIs on bioRxiv that can be added to the dois collection
        Keyword arguments:
          None
        Returns:
          None
    '''
    check = get_dois_from_protocolsio()
    COUNT['read'] = len(check)
    ready = []
    review = []
    nojanelians = []
    alumni = []
    for doi, item in tqdm(check.items(), desc='Crossref check'):
        if doi_exists(doi):
            COUNT['in_dois'] += 1
            continue
        resp = JRC.call_crossref(doi)
        if resp and 'message' in resp:
            parse_authors(doi, resp['message'], ready, review, nojanelians, alumni)
    if ready:
        LOGGER.info("Writing DOIs to protocolsio_ready.txt")
        with open('protocolsio_ready.txt', 'w', encoding='ascii') as outstream:
            for item in ready:
                outstream.write(f"{item}\n")
    if review:
        LOGGER.info("Writing DOIs to protocolsio_review.txt")
        with open('protocolsio_review.txt', 'w', encoding='ascii') as outstream:
            for item in review:
                outstream.write(f"{item}\n")
    if alumni:
        LOGGER.info("Writing DOIs to protocolsio_alumni.txt")
        with open('protocolsio_alumni.txt', 'w', encoding='ascii') as outstream:
            for item in alumni:
                outstream.write(f"{item}\n")
    if nojanelians:
        LOGGER.info("Writing DOIs to protocolsio_nojanelians.txt")
        with open('protocolsio_nojanelians.txt', 'w', encoding='ascii') as outstream:
            for item in nojanelians:
                outstream.write(f"{item}\n")
    print(f"DOIs read from protocols.io:     {COUNT['read']:,}")
    print(f"DOIs already in database:        {COUNT['in_dois']:,}")
    print(f"DOIs not in Crossref:            {COUNT['no_crossref']:,}")
    print(f"DOIs with no Janelian authors:   {len(nojanelians):,}")
    print(f"DOIs with alumni authors:        {len(alumni):,}")
    print(f"DOIs ready for processing:       {len(ready):,}")
    print(f"DOIs requiring review:           {len(review):,}")

# -----------------------------------------------------------------------------

if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(
        description="Sync DOIs from protocols.io")
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
