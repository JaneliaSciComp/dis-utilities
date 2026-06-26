""" sync_figshare_legal.py
    Sync legal data from Figshare to the database
"""

import argparse
import collections
import json
from operator import attrgetter
import os
import sys
import time
from tqdm import tqdm
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

DB = {}
COUNT = collections.defaultdict(lambda: 0, {})
ARG = LOGGER = WORK = None
LICENSE = {}

__version__ = '1.0.0'

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


def get_license(row, licmap):
    """ Get the license for a DOI
        Keyword arguments:
          row: DOI record
          licmap: license map dictionary
        Returns:
          License
    """
    if row['jrc_obtained_from'] == 'DataCite':
        if 'rightsList' in row and row['rightsList']:
            for right in row['rightsList']:
                if 'rightsIdentifier' in right and right['rightsIdentifier'] in licmap:
                    return licmap[right['rightsIdentifier']]
                elif 'rights' in right and right['rights'] in licmap:
                    return licmap[right['rights']]
                elif 'rightsIdentifier' in right:
                    print(f"Unknown license (rightsIdentifier) {right['rightsIdentifier']} for {row['doi']}")
                elif 'rights' in right and right['rights']:
                    print(f"Unknown license (rights) {right['rights']} for {row['doi']}")
                elif 'rightsUri' in right and right['rightsUri'] in LICENSE:
                    return licmap[right['rightsUri']]
                else:
                    print(f"Incorrect rights format {right} for {row['doi']}")
    try:
        time.sleep(.5)
        data = DL.get_doi_record(row['doi'], source='openalex')
    except Exception as err:
        raise err
    if data:
        if 'primary_location' in data and data['primary_location'] \
           and data['primary_location']['license'] and data['primary_location']['license'] != "False":
            if data['primary_location']['license'] in licmap:
                return licmap[data['primary_location']['license']]
    if 'jrc_pmc' not in row or not row['jrc_pmc']:
        return None
    time.sleep(0.11)  # stay under NCBI's 10 req/s limit with API key
    data = None
    for attempt in range(3):
        try:
            data = DL.get_doi_record(row['jrc_pmc'], source='pmc')
            break
        except Exception as err:
            if '429' in str(err) and attempt < 2:
                wait = 60 * (attempt + 1)
                LOGGER.warning(f"PMC 429 for {row['jrc_pmc']}, retrying in {wait}s (attempt {attempt+1})")
                time.sleep(wait)
            elif '429' in str(err):
                LOGGER.warning(f"PMC 429 for {row['jrc_pmc']} after 3 attempts, skipping")
                return None
            else:
                raise
    if not data or 'OAI-PMH' not in data or 'GetRecord' not in data['OAI-PMH'] \
       or 'record' not in data['OAI-PMH']['GetRecord'] \
       or 'metadata' not in data['OAI-PMH']['GetRecord']['record'] \
       or 'article' not in data['OAI-PMH']['GetRecord']['record']['metadata'] \
       or 'front' not in data['OAI-PMH']['GetRecord']['record']['metadata']['article']:
        return None
    front = data['OAI-PMH']['GetRecord']['record']['metadata']['article']['front']
    if 'article-meta' not in front or 'custom-meta-group' not in front['article-meta'] \
       or 'custom-meta' not in front['article-meta']['custom-meta-group'] \
        or not front['article-meta']['custom-meta-group']['custom-meta']:
        return None
    for custom_meta in front['article-meta']['custom-meta-group']['custom-meta']:
        if custom_meta['meta-name'] == 'license':
            lic = custom_meta['meta-value'].replace(" ", "-").lower()
            if lic in licmap:
                return licmap[lic]
    return None


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
    for row in rows:
        if 'rightsList' in row and row['rightsList']:
            for right in row['rightsList']:
                if 'rightsIdentifier' in right and right['rightsIdentifier'] in LICENSE:
                    row['jrc_license'] = LICENSE[right['rightsIdentifier']]
                    LOGGER.info(f"Using license (rightsIdentifier) {row['jrc_license']} for {row['doi']}")
                elif 'rights' in right and right['rights'] in LICENSE:
                    row['jrc_license'] = LICENSE[right['rights']]
                    LOGGER.info(f"Using license (rights) {row['jrc_license']} for {row['doi']}")
                elif 'rightsIdentifier' in right:
                    LOGGER.error(f"Unknown license (rightsIdentifier) {right['rightsIdentifier']} for {row['doi']}")
                elif 'rights' in right and right['rights']:
                    LOGGER.error(f"Unknown license (rights) {right['rights']} for {row['doi']}")
                elif 'rightsUri' in right and right['rightsUri'] in LICENSE:
                    row['jrc_license'] = LICENSE[right['rightsUri']]
                    LOGGER.info(f"Using license (rights) {row['jrc_license']} for {row['doi']}")
                else:
                    LOGGER.error(f"Incorrect rights format {right} for {row['doi']}")
                    continue
                break
            if 'jrc_license' in row and row['jrc_license']:
                COUNT['updated'] += 1
                if ARG.WRITE:
                    try:
                        DB['dis'].dois.update_one({"doi": row['doi']}, {"$set": {"jrc_license": row['jrc_license']}})
                    except Exception as err:
                        terminate_program(err)
        else:
            lic = get_license(row, LICENSE)
            if lic:
                row['jrc_license'] = lic
                COUNT['updated'] += 1
                if ARG.WRITE:
                    try:
                        DB['dis'].dois.update_one({"doi": row['doi']}, {"$set": {"jrc_license": row['jrc_license']}})
                    except Exception as err:
                        terminate_program(err)
            else:
                LOGGER.error(f"No license found for {row['doi']}")
                COUNT['no_license'] += 1
    print(f"DOIs read:            {COUNT['read']:,}")
    print(f"DOIs with no license: {COUNT['no_license']:,}")
    print(f"DOIs updated:         {COUNT['updated']:,}")

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Sync local Workday alumni to orcid")
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, prod)')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Actually send emails')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    WORK = JRC.simplenamespace_to_dict(JRC.get_config("workday"))
    initialize_program()
    processing()
    terminate_program()
