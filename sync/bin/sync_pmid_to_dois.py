''' sync_pmid_to_dois.py
    Update the MongoDB dois collection with PMIDs from NCBI. This ain't gonna find everything - the
    DOI needs to be in the PubMed Central archive.
'''

__version__ = '3.0.0'

import argparse
import collections
import json
from operator import attrgetter
import os
import sys
from tqdm import tqdm
import jrc_common.jrc_common as JRC

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Database
DB = {}
# Counters
COUNT = collections.defaultdict(lambda: 0, {})
# Search parms
ALLOWED_TYPES = ["book-chapter", "journal-article", "posted-content", "proceedings-article"]
ARG = LOGGER = None
ENTREZ_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed"

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
    if "NCBI_API_KEY" not in os.environ:
        terminate_program("Missing API key - set in NCBI_API_KEY environment variable")
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


def write_record(row, payload):
    ''' Write record to database
        Keyword arguments:
          row: record to write
          payload: data to add/update
        Returns:
          None
    '''
    if ARG.WRITE:
        result = DB['dis']['dois'].update_one({'_id': row['_id']}, {"$set": payload})
        if hasattr(result, 'matched_count') and result.matched_count:
            COUNT['written'] += result.matched_count


def postprocessing(audit, error):
    ''' Print counts and write audit file
        Keyword arguments:
          audit: list of updates
          error: list of errors
        Returns:
          None
    '''
    print(f"DOIs read from dois: {COUNT['doi']:,}")
    print(f"DOIs updated:        {COUNT['updated']:,}")
    print(f"DOIs written:        {COUNT['written']:,}")
    if audit:
        filename = 'pmid_dois_updates.json'
        with open(filename, 'w', encoding='utf-8') as outfile:
            outfile.write(f"{json.dumps(audit, indent=4, default=str)}\n")
        LOGGER.info(f"Wrote {len(audit):,} updates to {filename}")
    if error:
        filename = 'pmid_dois_errors.json'
        with open(filename, 'w', encoding='utf-8') as outfile:
            outfile.write(f"{json.dumps(error, indent=4, default=str)}\n")
        LOGGER.info(f"Wrote {len(error):,} errors to {filename}")


def update_pmid(row, pmid, audit):
    ''' Update PMID
        Keyword arguments:
          row: record to update
          pmid: PMID to update
          audit: list of updates
    '''
    payload = {"jrc_pmid": pmid}
    COUNT['updated'] += 1
    write_record(row, payload)
    payload["doi"] = row['doi']
    audit.append(payload)


def update_dois():
    ''' Sync NCBI PMIDs to the dois collection
        Keyword arguments:
          None
        Returns:
          None
    '''
    payload = {"jrc_pmid": {"$exists": False},
               "type": {"$in": ALLOWED_TYPES}}
    try:
        cnt = DB['dis']['dois'].count_documents(payload)
        rows = DB['dis']['dois'].find(payload)
    except Exception as err:
        terminate_program(err)
    audit = []
    error = []
    for row in tqdm(rows, total=cnt, desc="Syncing PMIDs"):
        pmid = ''
        errmsg = {}
        COUNT['doi'] += 1
        try:
            pmid = JRC.get_pmid(row['doi'])
        except JRC.PMIDNotFound as err:
            errmsg = {"doi": row['doi'], "error": err.details}
        except Exception as err:
            terminate_program(err)
        if pmid:
            update_pmid(row, pmid, audit)
            continue
        try:
            oresp = JRC.call_oa(row['doi'])
        except Exception as err:
            terminate_program(err)
        if oresp and 'PMID' in oresp:
            update_pmid(row, oresp['PMID'], audit)
        if errmsg:
            error.append(errmsg)
    postprocessing(audit, error)

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Update the MongoDB dois collection with PMIDs from NCBI")
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
    update_dois()
    terminate_program()
