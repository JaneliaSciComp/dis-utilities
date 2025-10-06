""" sync_openalex.py
    Sync work data from OpenAlex. DOIs will almost always make it into the local database before
    they're present in OpenAlex.
    Data brought in from OpenAlex:
      open_access.is_oa -> jrc_is_oa
      open_access.oa_status -> jrc_oa_status
      primary_location.license -> jrc_license
    This program will also look for DOIs with a "closed" Open Access status to override. If
    the OA status is "closed" and the DOI has a fulltext URL, the OA status will be set to
    "hybrid" and jrc_is_oa will be set to True. The former status will be saved as
    jrc_former_status.
"""

__version__ = '2.0.0'

import argparse
from operator import attrgetter
import sys
import time
from tqdm import tqdm
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

ARG=LOGGER = None
DB = {}
COUNT = {'dois': 0, 'notfound': 0, 'updated': 0}

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
        LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, ARG.MANIFOLD, dbo.host, dbo.user)
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)


def get_dois():
    """ Get a list of DOIs to process
        Keyword arguments:
          None
        Returns:
          List of DOIs
    """
    dois = []
    if ARG.DOI:
        dois.append(ARG.DOI.lower().strip())
    elif ARG.FILE:
        try:
            with open(ARG.FILE, "r", encoding="ascii") as instream:
                for doi in instream.read().splitlines():
                    dois.append(doi.lower().strip())
        except Exception as err:
            LOGGER.error(f"Could not process {ARG.FILE}")
            terminate_program(err)
    return dois


def get_pmc_license(pmcid):
    """ Get the license for a PMCID
        Keyword arguments:
          pmcid: PMCID
        Returns:
          License
    """
    data = DL.get_doi_record(pmcid, source='pmc')
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
            return custom_meta['meta-value'].replace(" ", "-").lower()
    return None


def update_open_access(row):
    """ Update jrc_is_oa and jrc_oa_status
        Keyword arguments:
          row: row to update
        Returns:
          None
    """
    time.sleep(.5)
    try:
        data = DL.get_doi_record(row['doi'], source='openalex')
    except Exception as err:
        terminate_program(err)
    if not data:
        if not ARG.SILENT:
            LOGGER.warning(f"{row['doi']} was not found in OpenAlex")
        COUNT["notfound"] += 1
        return
    payload = {}
    try:
        if 'jrc_is_oa' not in row and 'open_access' in data and data['open_access']:
            payload['jrc_is_oa'] = bool(data['open_access']['is_oa'])
            payload['jrc_oa_status'] = data['open_access']['oa_status']
        if 'jrc_license' not in row and 'primary_location' in data and data['primary_location']:
            payload['jrc_license'] = data['primary_location']['license']
        if ('jrc_license' not in payload or payload['jrc_license'] is None) \
           and 'jrc_pmc' in row:
            alt = get_pmc_license(row['jrc_pmc'])
            if alt:
                LOGGER.info(f"Using PMC license for {row['doi']}: {alt}")
                payload['jrc_license'] = alt
        if not payload:
            return
    except Exception as err:
        LOGGER.error(f"Could not process {row['doi']}")
        terminate_program(err)
    if ARG.WRITE:
        try:
            DB['dis']['dois'].update_one({"doi": row['doi']}, {"$set": payload})
        except Exception as err:
            terminate_program(err)
    COUNT["updated"] += 1


def override_oa_closed(row):
    """ Override OA closed status
        Keyword arguments:
          row: row to update
        Returns:
          None
    """
    payload = {'jrc_former_status': row['jrc_oa_status'],
               'jrc_is_oa': True}
    payload['jrc_oa_status'] = "hybrid"
    if ARG.WRITE:
        try:
            DB['dis']['dois'].update_one({"doi": row['doi']}, {"$set": payload})
        except Exception as err:
            terminate_program(err)
    COUNT["updated"] += 1


def show_counts():
    """ Show the counts
        Keyword arguments:
          None
        Returns:
          None
    """
    print(f"DOIs read:      {COUNT['dois']}")
    if COUNT['notfound']:
        print(f"DOIs not found: {COUNT['notfound']}")
    print(f"DOIs updated:   {COUNT['updated']}")


def process_dois():
    """ Process a list of DOIs
        Keyword arguments:
          None
        Returns:
          None
    """
    cnt = 0
    rows = []
    dois = get_dois()
    if dois:
        for doi in dois:
            data = DL.get_doi_record(doi, coll=DB['dis']['dois'])
            rows.append(data)
        cnt = len(rows)
    else:
        payload = {"doi": {"$not": {"$regex": "janelia"}},
                   "$or": [{"jrc_is_oa": {"$exists": False}},
                           {"jrc_license": {"$exists": False}}]}
        try:
            cnt = DB['dis'].dois.count_documents(payload)
            rows = DB['dis'].dois.find(payload)
        except Exception as err:
            terminate_program(err)
    # Open Access / license data
    LOGGER.info(f"Found {cnt} DOI{'s' if cnt != 1 else ''} to process for OpenAlex")
    for row in tqdm(rows, total=cnt, desc="Add OpenAlex"):
        COUNT['dois'] += 1
        update_open_access(row)
    show_counts()
    # Open Access status override
    COUNT['dois'] = COUNT["updated"] = COUNT["notfound"] = 0
    if dois:
        cnt = len(dois)
    else:
        rows = []
        payload = {"jrc_is_oa": {"$exists": True}, "jrc_oa_status": "closed",
                   "jrc_fulltext_url": {"$exists": True}
                  }
        try:
            cnt = DB['dis'].dois.count_documents(payload)
            rows = DB['dis'].dois.find(payload)
        except Exception as err:
            terminate_program(err)
    LOGGER.info(f"Found {cnt} DOI{'s' if cnt != 1 else ''} to process for OA status")
    for row in tqdm(rows, total=cnt, desc="Fix OA status"):
        COUNT["dois"] += 1
        if row['jrc_oa_status'] == "closed" and row['jrc_fulltext_url']:
            override_oa_closed(row)
    show_counts()
    if not ARG.WRITE:
        LOGGER.warning("Dry run successful, no updates were made")

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Add a reviewed date to one or more DOIs")
    GROUP_A = PARSER.add_mutually_exclusive_group(required=False)
    GROUP_A.add_argument('--doi', dest='DOI', action='store',
                         help='Single DOI to process')
    GROUP_A.add_argument('--file', dest='FILE', action='store',
                         help='File of DOIs to process')
    GROUP_A.add_argument('--all', dest='ALL', action='store_true',
                         help='Process all DOIs')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, prod)')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Write to database')
    PARSER.add_argument('--silent', dest='SILENT', action='store_true',
                        default=False, help="Don't display warnings")
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    try:
        PROJECT = DL.get_project_map(DB['dis'].project_map)
    except Exception as err:
        terminate_program(err)
    process_dois()
    terminate_program()
