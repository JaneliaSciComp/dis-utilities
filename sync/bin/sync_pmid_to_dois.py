''' sync_pmid_to_dois.py
    Update the MongoDB dois collection with PMIDs from NCBI. This ain't gonna find everything - the
    DOI needs to be in the PubMed or PubMed Central archive.
'''

__version__ = '6.2.0'

import argparse
import collections
import json
from operator import attrgetter
import os
import sys
from time import sleep
from metapub import PubMedFetcher
import metapub.exceptions
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
# Global variables
DIS = None

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
        if "$unset" in payload:
            result = DB['dis']['dois'].update_one({'_id': row['_id']}, payload)
        else:
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
    msg = f"DOIs read from dois: {COUNT['doi']:,}\n" \
          + f"DOIs updated:        {COUNT['updated']:,}\n" \
          + f"  PubMed (eutils):   {COUNT['PubMed (eutils)']:,}\n" \
          + f"  PubMed:            {COUNT['PubMed']:,}\n" \
          + f"  OA:                {COUNT['OA']:,}\n" \
          + f"  MeSH updates:      {COUNT['mesh']:,}\n" \
          + f"  PMIDs removed:     {COUNT['removed']:,}\n" \
          + f"DOIs written:        {COUNT['written']:,}"
    print(msg)
    if error:
        filename = 'pmid_dois_errors.json'
        with open(filename, 'w', encoding='utf-8') as outfile:
            outfile.write(f"{json.dumps(error, indent=4, default=str)}\n")
        LOGGER.info(f"Wrote {len(error):,} errors to {filename}")
    if audit:
        filename = 'pmid_dois_updates.json'
        with open(filename, 'w', encoding='utf-8') as outfile:
            outfile.write(f"{json.dumps(audit, indent=4, default=str)}\n")
        LOGGER.info(f"Wrote {len(audit):,} update{'' if len(audit) == 1 else 's'} to {filename}")
    if (not COUNT['updated']) or (not (ARG.TEST or ARG.WRITE)):
        return
    text = f"<pre>PMIDs have been added to DOIs.\n\n{msg}</pre>"
    if audit:
        text += "<br><br>Please see the attached file for the new records."
    subject = "PMIDs added to DOIs"
    email = DIS['developer'] if ARG.TEST else DIS['receivers']
    if audit:
        JRC.send_email(text, DIS['sender'], email, subject,
                       attachment=filename, mime='html')
    else:
        JRC.send_email(text, DIS['sender'], email, subject, mime='html')


def get_mesh_array(mesh):
    ''' Get the mesh array
        Keyword arguments:
          mesh: mesh object
    '''
    mesh_array = []
    for key, val in mesh.items():
        single = val
        single['key'] = key
        mesh_array.append(single)
    if mesh_array:
        COUNT['mesh'] += 1
    return mesh_array


def update_pmid_by_article(row, article, audit):
    ''' Update PMID using an article object
        Keyword arguments:
          row: record to update
          article: article object
          audit: list of updates
    '''
    payload = {}
    if hasattr(article, 'pmid') and article.pmid:
        if 'jrc_pmid' in row and row['jrc_pmid'] != article.pmid:
            LOGGER.warning(f"PMID mismatch for {row['doi']}: {row['jrc_pmid']} != {article.pmid}")
            return
        payload['jrc_pmid'] = article.pmid
    if hasattr(article, 'pmc') and article.pmc:
        payload['jrc_pmc'] = article.pmc
    if hasattr(article, 'mesh') and article.mesh:
        payload['jrc_mesh'] = get_mesh_array(article.mesh)
    if not payload:
        #LOGGER.warning(f"No PMID for {row['doi']}")
        return
    COUNT['updated'] += 1
    COUNT['PubMed (eutils)'] += 1
    write_record(row, payload)
    payload["doi"] = row['doi']
    audit.append(payload)


def update_pmid(row, pmid, fetch, audit):
    ''' Update PMID using
        Keyword arguments:
          row: record to update
          pmid: PMID to update
          fetch: PubMedFetcher object
          audit: list of updates
    '''
    LOGGER.debug(f"Updating PMID for {row['doi']}: {pmid}")
    payload = {'jrc_pmid': pmid}
    article = None
    try:
        article = fetch.article_by_pmid(pmid)
    except metapub.exceptions.InvalidPMID:
        LOGGER.warning(f"Invalid PMID for {row['doi']}: {pmid}")
        if ARG.UPDATE:
            # There used to be a PMID, and now there isn't. Yes, this is a thing: PubMed can remove PMIDs.
            LOGGER.warning(f"Removing PMID for {row['doi']}: {pmid}")
            payload = {"$unset": {"jrc_mesh": "", "jrc_pmc": "", "jrc_pmid": ""}}
            write_record(row, payload)
            COUNT['removed'] += 1
            COUNT['PubMed'] -= 1
        return
    except metapub.exceptions.MetaPubError:
        LOGGER.warning(f"MetaPubError for {row['doi']}: {pmid}")
        return
    if article and hasattr(article, 'mesh') and article.mesh:
        payload['jrc_mesh'] = get_mesh_array(article.mesh)
    if not payload:
        return
    COUNT['updated'] += 1
    write_record(row, payload)
    payload["doi"] = row['doi']
    audit.append(payload)


def fetch_pmid(fetch, row, audit, error):
    ''' Fetch PMID for a DOI
        Keyword arguments:
          fetch: PubMedFetcher object
          doi: DOI to fetch
          audit: list of updates
          error: list of errors
    '''
    pmid = ''
    errmsg = {}
    COUNT['doi'] += 1
    # PubMed
    try:
        article = fetch.article_by_doi(row['doi'])
        if article and hasattr(article, 'pmid') and article.pmid:
            update_pmid_by_article(row, article, audit)
            return
    except Exception:
        pass
    try:
        pmid = JRC.get_pmid(row['doi'])
    except JRC.PMIDNotFound as err:
        errmsg = {"doi": row['doi'], "error": err.details}
    except Exception as err:
        LOGGER.warning(f"{type(err).__name__} error for {row['doi']}: skipping")
    if pmid:
        COUNT['PubMed'] += 1
        update_pmid(row, pmid, fetch, audit)
        return
    # OA
    try:
        oresp = JRC.call_oa(row['doi'])
    except Exception as err:
        terminate_program(err)
    if oresp and 'PMID' in oresp:
        COUNT['OA'] += 1
        update_pmid(row, oresp['PMID'], fetch, audit)
        return
    if errmsg:
        error.append(errmsg)


def update_dois():
    ''' Sync NCBI PMIDs to the dois collection
        Keyword arguments:
          None
        Returns:
          None
    '''
    if ARG.DOI:
        payload = {"doi": ARG.DOI}
    else:
        payload = {"jrc_pmid": {"$exists": False},
                   "type": {"$in": ALLOWED_TYPES}}
    try:
        cnt = DB['dis']['dois'].count_documents(payload)
        rows = DB['dis']['dois'].find(payload)
    except Exception as err:
        terminate_program(err)
    audit = []
    error = []
    fetch = PubMedFetcher()
    for row in tqdm(rows, total=cnt, desc="Syncing PMIDs"):
        fetch_pmid(fetch, row, audit, error)
        # We're rate limited to 10 transaction/sec
        sleep(.104)
    postprocessing(audit, error)


def refresh_pmids():
    ''' Refresh existing PMIDs
        Keyword arguments:
          None
        Returns:
          None
    '''
    payload = {"jrc_pmid": {"$exists": True}}
    try:
        cnt = DB['dis']['dois'].count_documents(payload)
        rows = DB['dis']['dois'].find(payload)
    except Exception as err:
        terminate_program(err)
    audit = []
    fetch = PubMedFetcher()
    for row in tqdm(rows, total=cnt, desc="Syncing PMIDs"):
        COUNT['PubMed'] += 1
        update_pmid(row, row['jrc_pmid'], fetch, audit)
    postprocessing(audit, [])

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Update the MongoDB dois collection with PMIDs from NCBI")
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, prod)')
    PARSER.add_argument('--doi', dest='DOI', action='store',
                        help='DOI')
    PARSER.add_argument('--update', dest='UPDATE', action='store_true',
                        default=False, help='Update existing PMIDs')
    PARSER.add_argument('--test', dest='TEST', action='store_true',
                        default=False, help='Send email to developer')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Write to database/config system')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    try:
        DIS = JRC.simplenamespace_to_dict(JRC.get_config("dis"))
    except Exception as err:
        terminate_program(err)
    if ARG.UPDATE:
        refresh_pmids()
    else:
        update_dois()
    terminate_program()
