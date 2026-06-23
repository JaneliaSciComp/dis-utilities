''' pull_internal_acks.py

PURPOSE
-------
Fetches and stores acknowledgement text for Janelia-authored (internal) DOIs that
do not yet have a `jrc_acknowledgements` field in the DIS MongoDB database.
Sources queried, in order:
- eLife       – via the eLife API (doi_common.get_doi_record)
- Elsevier    – via the Elsevier full-text API (doi_common.get_acknowledgements)
- PubMed Central (PMC) – via the PMC OAI-PMH API (doi_common.get_acknowledgements
                         with a PMCID)
- arXiv        – via the arXiv HTML render (then e-print TeX source) for DataCite
                arXiv DOIs (10.48550/arxiv.*), handled inside
                doi_common.get_acknowledgements

INPUTS
------
- NCBI_API_KEY environment variable (required): API key for the NCBI E-utilities API.
- DIS MongoDB database (read/write depending on --write flag):
    - Collection `dois`      : source of DOI records; updated with acknowledgements.
    - Collection `to_ignore` : DOIs to skip entirely.
- Command-line flags:
    --doi DOI  Restrict processing to a single DOI (across all sources).
    --source   Restrict processing to a single source (elife, elsevier, pmc, or
               arxiv). Omit to process all sources.
    --write    Actually update the database (default: dry-run).
    --test     Send email to developer rather than the normal recipient list.
    --verbose  Increase logging verbosity.
    --debug    Maximum logging verbosity.

HIGH-LEVEL FLOW
---------------
1. Initialization
   - Connects to the DIS MongoDB database (read-only by default; read/write with --write).
   - Loads the list of DOIs to ignore from the `to_ignore` collection.
2. eLife pass (add_elife_internal_acks)
   - Queries `dois` for records whose DOI matches /elife/ and that lack
     `jrc_acknowledgements`.
   - Calls doi_common.get_doi_record(doi, source='elife') and concatenates the
     returned acknowledgement paragraph texts.
3. Elsevier pass (add_elsevier_internal_acks)
   - Queries `dois` for records whose DOI matches /10.1016\// and that lack
     `jrc_acknowledgements`.
   - Calls doi_common.get_acknowledgements with a 0.1 s inter-request sleep to
     stay within the Elsevier rate limit.
4. PMC pass (add_pmc_internal_acks)
   - Queries `dois` for records that have a `jrc_pmc` field but lack
     `jrc_acknowledgements`.
   - Calls doi_common.get_acknowledgements with the PMCID.
5. arXiv pass (add_arxiv_internal_acks)
   - Queries `dois` for records whose DOI matches /10.48550\/arxiv/ and that lack
     `jrc_acknowledgements`.
   - Calls doi_common.get_acknowledgements, which downloads the paper from arXiv
     (HTML render, then e-print TeX source) and extracts the Acknowledgements
     section.
6. Database update (--write mode)
   - For each collected record, performs a MongoDB update_one setting
     `jrc_acknowledgements` on the matching DOI document.
7. Output
   - Prints a per-source summary of counts.
   - Writes pmc_internal_acks.json with all collected acknowledgement records.
   - Writes internal_ack_errors.json if any source calls raised exceptions.
   - Sends a summary email when --test or --write is active and records were found.

DEPENDENCIES
------------
- jrc_common.jrc_common  (JRC): logging, config, database connection, email helpers.
- doi_common.doi_common  (DL): DOI record retrieval and acknowledgement extraction
                               (eLife API, Elsevier API, PMC OAI-PMH, arXiv full text).
- tqdm: progress bars for per-source processing loops.
'''

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

__version__ = '1.1.0'

# pylint: disable=broad-exception-caught,logging-fstring-interpolation,no-member

# Database
DB = {}
# Counters
COUNT = collections.defaultdict(lambda: 0, {})
# Global variables
ARG = DIS = LOGGER = None
IGNORE = []

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
        terminate_program("Missing NCBI API key - set in NCBI_API_KEY environment variable")
    try:
        dbconfig = JRC.get_config("databases")
    except Exception as err:
        terminate_program(err)
    dbs = ['dis']
    for source in dbs:
        dbo = attrgetter(f"{source}.prod.{'read' if not ARG.WRITE else 'write'}")(dbconfig)
        LOGGER.info(f"Connecting to {dbo.name} prod on {dbo.host} as {dbo.user}")
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)
    try:
        rows = DB['dis']['to_ignore'].find({})
    except Exception as err:
        terminate_program(err)
    for row in rows:
        IGNORE.append(row['key'])
    LOGGER.info(f"Found {len(IGNORE):,} DOIs to ignore")


def restrict_to_doi(payload):
    ''' Restrict a query payload to a single DOI if --doi was supplied
        Keyword arguments:
          payload: MongoDB query payload
        Returns:
          The (possibly restricted) query payload
    '''
    if ARG.DOI:
        return {"$and": [payload, {"doi": ARG.DOI}]}
    return payload


def add_elife_internal_acks(internal):
    ''' Add eLife acknowledgements to the internal DOIs
        Keyword arguments:
          internal: list of internal DOIs
        Returns:
          None
    '''
    payload = {"doi": {"$regex": "elife"}, "jrc_acknowledgements": {"$exists": False}}
    payload = restrict_to_doi(payload)
    try:
        cnt = DB['dis'].dois.count_documents(payload)
        LOGGER.info(f"Found {cnt:,} eLife DOIs without acknowledgements")
        rows = DB['dis'].dois.find(payload)
    except Exception as err:
        terminate_program(err)
    for row in tqdm(rows, total=cnt, desc="Finding eLife acknowledgements"):
        doi = row['doi']
        edata = DL.get_doi_record(doi, source='elife')
        if edata and 'acknowledgements' in edata and edata['acknowledgements']:
            acklist = []
            for ack in edata['acknowledgements']:
                acklist.append(ack['text'])
            acktext =  ' '.join(acklist)
            COUNT['elife_add'] += 1
            internal.append({"doi": doi,
                             "ack": acktext,
                             "source": "eLife"})


def add_elsevier_internal_acks(internal, error):
    ''' Add Elsevier acknowledgements to the internal DOIs
        Keyword arguments:
          internal: list of internal DOIs
          error: list of error records
        Returns:
          None
    '''
    payload = {"doi": {"$regex": "10.1016/"}, "jrc_acknowledgements": {"$exists": False}}
    payload = restrict_to_doi(payload)
    try:
        cnt = DB['dis'].dois.count_documents(payload)
        LOGGER.info(f"Found {cnt:,} Elsevier DOIs without acknowledgements")
        rows = DB['dis'].dois.find(payload)
    except Exception as err:
        terminate_program(err)
    for row in tqdm(rows, total=cnt, desc="Finding Elsevier acknowledgements"):
        time.sleep(0.1)
        try:
            acktext, _ = DL.get_acknowledgements(row['doi'])
        except Exception as err:
            error.append({"doi": row['doi'], "source": "elsevier", "error": str(err)})
            continue
        if acktext:
            COUNT['elsevier_add'] += 1
            internal.append({"doi": row['doi'],
                             "ack": acktext,
                             "source": "Elsevier"})


def add_pmc_internal_acks(internal, error):
    ''' Add PMC acknowledgements to the internal DOIs
        Keyword arguments:
          internal: list of internal DOIs
          error: list of error records
        Returns:
          None
    '''
    payload = {"jrc_pmc": {"$exists": True}, "jrc_acknowledgements": {"$exists": False}}
    payload = restrict_to_doi(payload)
    try:
        cnt = DB['dis'].dois.count_documents(payload)
        if cnt < 1:
            return
        LOGGER.info(f"Found {cnt:,} PMC DOIs without acknowledgements")
        rows = DB['dis'].dois.find(payload)
    except Exception as err:
        terminate_program(err)
    for row in tqdm(rows, total=cnt, desc="Finding PMC acknowledgements"):
        try:
            ack, _ = DL.get_acknowledgements(row['doi'], pmcid=row['jrc_pmc'])
        except Exception as err:
            error.append({"doi": row['doi'], "pmcid": row['jrc_pmc'],
                          "source": "pmc", "error": str(err)})
            continue
        if ack:
            COUNT['pmc_add'] += 1
            internal.append({"pmcid": row['jrc_pmc'],
                             "doi": row['doi'],
                             "ack": ack,
                             "source": "PMC"})


def add_arxiv_internal_acks(internal, error):
    ''' Add arXiv acknowledgements to the internal DOIs
        Keyword arguments:
          internal: list of internal DOIs
          error: list of error records
        Returns:
          None
    '''
    payload = {"doi": {"$regex": "10.48550/arxiv"}, "jrc_acknowledgements": {"$exists": False}}
    payload = restrict_to_doi(payload)
    try:
        cnt = DB['dis'].dois.count_documents(payload)
        if cnt < 1:
            return
        LOGGER.info(f"Found {cnt:,} arXiv DOIs without acknowledgements")
        rows = DB['dis'].dois.find(payload)
    except Exception as err:
        terminate_program(err)
    for row in tqdm(rows, total=cnt, desc="Finding arXiv acknowledgements"):
        try:
            acktext, _ = DL.get_acknowledgements(row['doi'])
        except Exception as err:
            error.append({"doi": row['doi'], "source": "arxiv", "error": str(err)})
            continue
        if acktext:
            COUNT['arxiv_add'] += 1
            internal.append({"doi": row['doi'],
                             "ack": acktext,
                             "source": "arXiv"})


def generate_email(internal, error):
    ''' Generate and send an email
        Keyword arguments:
          internal: list of internal DOIs
          error: list of error records
        Returns:
          None
    '''
    msg = JRC.get_run_data(__file__, __version__) +  "<br><br>"
    if internal:
        msg += "Internal DOIs:<br>"
        for rec in internal:
            link = f"https://dis.int.janelia.org/doiui/{rec['doi']}"
            pmcid = rec.get('pmcid')
            if pmcid:
                msg += f"<a href='{link}'>{rec['doi']}</a> " \
                       f"(Source: {rec['source']}) (PMCID: {pmcid})<br>"
            else:
                msg += f"<a href='{link}'>{rec['doi']}</a> (Source: {rec['source']})<br>"
        msg += "<br>"
    if error:
        msg += f"Found {len(error):,} error records<br>"
    email = DIS['developer'] if ARG.TEST else DIS['receivers']
    subject = "Acknowledgements updated for DOIs"
    JRC.send_email(msg, DIS['sender'], email, subject,
                   mime='html')


def processing():
    ''' Find DOIs without acknowledgements.
        Keyword arguments:
          None
        Returns:
          None
    '''
    internal = []
    error = []
    sources = {'elife': lambda: add_elife_internal_acks(internal),
               'elsevier': lambda: add_elsevier_internal_acks(internal, error),
               'pmc': lambda: add_pmc_internal_acks(internal, error),
               'arxiv': lambda: add_arxiv_internal_acks(internal, error)}
    for source, handler in sources.items():
        if ARG.SOURCE in (None, source):
            handler()
    for row in tqdm(internal, total=len(internal), desc="Updating internal DOIs"):
        if row['doi'] == 'n/a':
            continue
        if not isinstance(row['ack'], str):
            LOGGER.warning(f"Weird format for {row['doi']}")
            continue
        if ARG.WRITE:
            DB['dis']['dois'].update_one({"doi": row['doi']},
                                         {"$set": {"jrc_acknowledgements": row['ack']}})
        COUNT['updated'] += 1
    if ARG.SOURCE in (None, 'elife'):
        print(f"eLife DOIs added:    {COUNT['elife_add']:,}")
    if ARG.SOURCE in (None, 'elsevier'):
        print(f"Elsevier DOIs added: {COUNT['elsevier_add']:,}")
    if ARG.SOURCE in (None, 'pmc'):
        print(f"PMC DOIs added:      {COUNT['pmc_add']:,}")
    if ARG.SOURCE in (None, 'arxiv'):
        print(f"arXiv DOIs added:    {COUNT['arxiv_add']:,}")
    print(f"DOIs updated:        {COUNT['updated']:,}")
    if internal:
        with open('pmc_internal_acks.json', 'w', encoding='utf-8') as fileout:
            json.dump(internal, fileout, indent=4)
    if error:
        with open('internal_ack_errors.json', 'w', encoding='utf-8') as fileout:
            json.dump(error, fileout, indent=4)
    if (ARG.TEST or ARG.WRITE) and internal:
        generate_email(internal, error)

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Add acknowledgements to internal DOIs")
    PARSER.add_argument('--doi', dest='DOI', default=None,
                        help='Restrict processing to a single DOI')
    PARSER.add_argument('--source', dest='SOURCE', action='store',
                        choices=['elife', 'elsevier', 'pmc', 'arxiv'], default=None,
                        help='Restrict processing to a single source [all]')
    PARSER.add_argument('--test', dest='TEST', action='store_true',
                        default=False, help='Send email to developer')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Flag, Update database')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    DIS = JRC.simplenamespace_to_dict(JRC.get_config("dis"))
    initialize_program()
    processing()
    terminate_program()
