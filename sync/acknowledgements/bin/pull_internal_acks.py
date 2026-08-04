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
- Europe PMC   – catch-all fallback: for any article-like DOI still without
                acknowledgements (including non-PMC-registered publishers - PNAS,
                Science, Nature, etc. - that never got a jrc_pmc), searches Europe
                PMC's open-access subset by DOI and parses the <ack> from its
                downloadable full-text JATS (fullTextXML). Scoped to article-like
                Crossref types (journal-article, posted-content, book-chapter,
                proceedings-article) so the DataCite dataset pool that Europe PMC
                does not carry is not scanned

INPUTS
------
- NCBI_API_KEY environment variable (required): API key for the NCBI E-utilities API.
- DIS MongoDB database (read/write depending on --write flag):
    - Collection `dois`      : source of DOI records; updated with acknowledgements.
- Command-line flags:
    --doi DOI  Restrict processing to a single DOI (across all sources).
    --source   Restrict processing to a single source (elife, elsevier, pmc,
               arxiv, or europepmc). Omit to process all sources.
    --write    Actually update the database (default: dry-run).
    --verbose  Increase logging verbosity.
    --debug    Maximum logging verbosity.

EMAIL RECIPIENT
----------------
The summary email always goes to the configured developer address, never the
full receivers list, and is sent any time acknowledgements are found -
--write or not (a dry run's "would update" findings are just as worth seeing
as a real run's).

HIGH-LEVEL FLOW
---------------
1. Initialization
   - Connects to the DIS MongoDB database (read-only by default; read/write with --write).
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
6. Europe PMC pass (add_europepmc_internal_acks)
   - Runs last, as a catch-all: queries `dois` for every article-like record
     (EPMC_ELIGIBLE_TYPES) still lacking `jrc_acknowledgements` (skipping any
     already resolved earlier this run), searches Europe PMC's open-access subset
     by DOI, and for a PMCID-bearing OA hit fetches its full-text JATS
     (fullTextXML) and parses the <back><ack>.
   - Covers DOIs the prefix-based passes miss - notably non-PMC-registered
     publishers whose open-access copy still lands in the Europe PMC OA subset.
     Skips the DataCite dataset/software pool, which Europe PMC does not carry.
7. Database update (--write mode)
   - For each collected record, performs a MongoDB update_one setting
     `jrc_acknowledgements` and `jrc_ack_source` (the source's display label -
     eLife/Elsevier/PMC/arXiv) on the matching DOI document.
8. Output
   - Prints a per-source summary of counts.
   - Writes internal_acks.json with all collected acknowledgement records.
   - Writes internal_ack_errors.json if any source calls raised exceptions.
   - Sends a summary email whenever records were found (--write or not):
     a header banner (run data, mode, DRY RUN/WRITE badge), KPI stat tiles per
     source plus an error tile, one card per source
     (eLife/Elsevier/PMC/arXiv/Europe PMC) listing its DOIs (linked to the DIS
     UI, with a PMCID column for the PMC and Europe PMC sources), and
     an Errors table when any source call raised. Built entirely from inline
     styles/tables (no <style> block) for compatibility with older email clients,
     matching the convention used by sync_citations.py.

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
import requests
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError
from tqdm import tqdm
import xmltodict
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL
import jrc_email.jrc_email as JE

__version__ = '1.6.1'

# pylint: disable=broad-exception-caught,logging-fstring-interpolation,no-member

# Database
DB = {}
# Counters
COUNT = collections.defaultdict(lambda: 0, {})
# Global variables
ARG = DIS = LOGGER = None
# Display order for the "source" label stored on each internal-DOI record
# (add_elife_internal_acks etc.), used to group the run-summary email.
SOURCE_LABELS = ('eLife', 'Elsevier', 'PMC', 'arXiv', 'Europe PMC')
# Europe PMC REST endpoints (add_europepmc_internal_acks). fullTextXML is served
# only for PMCID-bearing open-access articles, so the by-DOI search is filtered
# to OPEN_ACCESS:Y.
EPMC_REST = "https://www.ebi.ac.uk/europepmc/webservices/rest/"
EPMC_SEARCH = EPMC_REST + "search"
POLITE_HEADERS = {'User-Agent': 'janelia-dis/pull_internal_acks'}
# Crossref `type` values the Europe PMC pass will try. Europe PMC indexes journal
# articles, preprints, chapters and proceedings - not datasets/software, so this
# deliberately skips the large DataCite deposit pool (figshare/Zenodo etc., stored
# with type unset) that Europe PMC never carries, sparing thousands of dead
# lookups.
EPMC_ELIGIBLE_TYPES = ('journal-article', 'posted-content',
                       'proceedings-article', 'book-chapter')


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


def restrict_to_doi(payload):
    ''' Restrict a query payload to a single DOI if --doi was supplied
        Keyword arguments:
          payload: MongoDB query payload
        Returns:
          The (possibly restricted) query payload
    '''
    if ARG.DOI:
        return {"$and": [payload, {"doi": ARG.DOI.lower()}]}
    return payload


def add_elife_internal_acks(internal, error):
    ''' Add eLife acknowledgements to the internal DOIs
        Keyword arguments:
          internal: list of internal DOIs
          error: list of error records
        Returns:
          None
    '''
    payload = {"doi": {"$regex": r"10\.7554/elife"}, "jrc_acknowledgements": {"$exists": False}}
    payload = restrict_to_doi(payload)
    try:
        cnt = DB['dis'].dois.count_documents(payload)
        LOGGER.info(f"Found {cnt:,} eLife DOIs without acknowledgements")
        rows = DB['dis'].dois.find(payload)
    except Exception as err:
        terminate_program(err)
    for row in tqdm(rows, total=cnt, desc="Finding eLife acknowledgements"):
        doi = row['doi']
        time.sleep(0.1)
        # Guard the whole per-DOI fetch/parse: a single bad record (failed lookup,
        # malformed acknowledgements, missing 'text') must not abort the pass - and
        # since eLife runs first, an uncaught error here would block every source.
        try:
            edata = DL.get_doi_record(doi, source='elife')
            acklist = [ack['text'] for ack in (edata or {}).get('acknowledgements', [])
                       if ack.get('text')]
        except Exception as err:
            error.append({"doi": doi, "source": "elife", "error": str(err)})
            continue
        if acklist:
            COUNT['elife_add'] += 1
            internal.append({"doi": doi,
                             "ack": ' '.join(acklist),
                             "source": "eLife"})


def add_elsevier_internal_acks(internal, error):
    ''' Add Elsevier acknowledgements to the internal DOIs
        Keyword arguments:
          internal: list of internal DOIs
          error: list of error records
        Returns:
          None
    '''
    payload = {"doi": {"$regex": r"10\.1016/"}, "jrc_acknowledgements": {"$exists": False}}
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
        time.sleep(0.1)
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
    payload = {"doi": {"$regex": r"10\.48550/arxiv"}, "jrc_acknowledgements": {"$exists": False}}
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
        time.sleep(0.5)
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


def _europepmc_request(url, params=None, retries=3):
    ''' Europe PMC HTTP GET with a small retry on transient (429/5xx/connection)
        errors.
        Keyword arguments:
          url: request URL
          params: optional query-parameter dict
          retries: maximum number of attempts
        Returns:
          requests.Response (raises on repeated failure or a non-transient status)
    '''
    for attempt in range(retries):
        try:
            resp = requests.get(url, params=params, headers=POLITE_HEADERS, timeout=30)
        except requests.RequestException:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
                continue
            raise
        if (resp.status_code == 429 or resp.status_code >= 500) and attempt < retries - 1:
            time.sleep(2 ** attempt)
            continue
        resp.raise_for_status()
        return resp
    raise RuntimeError(f"Europe PMC request to {url} failed after {retries} retries")


def _europepmc_ack(doi):
    ''' Fetch an acknowledgement for a single DOI from Europe PMC's open-access
        full-text XML. Europe PMC serves downloadable JATS (fullTextXML) only for
        PMCID-bearing open-access articles, so the search is filtered to
        OPEN_ACCESS:Y and a hit without a PMCID is skipped. The <back><ack> is
        parsed with the same recursive flattener doi_common uses for NCBI PMC.
        Keyword arguments:
          doi: DOI to look up (lower-case)
        Returns:
          (ack_text, pmcid) - ack_text is None when there is no OA full text or
          no <ack>; pmcid is None when the DOI is not in the EPMC OA subset
    '''
    params = {"query": f'DOI:"{doi}" AND OPEN_ACCESS:Y',
              "format": "json", "resultType": "lite", "pageSize": 1}
    hits = _europepmc_request(EPMC_SEARCH, params=params).json() \
               .get("resultList", {}).get("result", [])
    if not hits:
        return None, None
    pmcid = hits[0].get("pmcid")
    if not pmcid:
        return None, None
    resp = _europepmc_request(f"{EPMC_REST}{pmcid}/fullTextXML")
    article = xmltodict.parse(resp.content).get("article") or {}
    ack = (article.get("back") or {}).get("ack")
    if not ack:
        return None, pmcid
    return (DL._parse_pmc_ack(ack) or None), pmcid


def add_europepmc_internal_acks(internal, error):
    ''' Add Europe PMC acknowledgements to the internal DOIs. Runs last, as a
        catch-all over the article-like DOIs (EPMC_ELIGIBLE_TYPES) still lacking
        an acknowledgement (skipping any already resolved earlier this run), so it
        recovers DOIs the prefix-based passes miss - notably non-PMC-registered
        publishers whose open-access copy still lands in the Europe PMC OA subset.
        Datasets/software (the DataCite deposit pool) are excluded, since Europe
        PMC does not carry them.
        Keyword arguments:
          internal: list of internal DOIs (appended to)
          error: list of error records (appended to)
        Returns:
          None
    '''
    already = {row['doi'] for row in internal}
    payload = {"jrc_acknowledgements": {"$exists": False},
               "type": {"$in": list(EPMC_ELIGIBLE_TYPES)}}
    payload = restrict_to_doi(payload)
    try:
        cnt = DB['dis'].dois.count_documents(payload)
        if cnt < 1:
            return
        LOGGER.info(f"Found {cnt:,} article-like DOIs without acknowledgements to "
                    f"try via Europe PMC")
        rows = DB['dis'].dois.find(payload, {"doi": 1})
    except Exception as err:
        terminate_program(err)
    for row in tqdm(rows, total=cnt, desc="Finding Europe PMC acknowledgements"):
        if row['doi'] in already:
            continue
        time.sleep(0.2)
        try:
            ack, pmcid = _europepmc_ack(row['doi'])
        except Exception as err:
            error.append({"doi": row['doi'], "source": "europepmc", "error": str(err)})
            continue
        if ack:
            COUNT['europepmc_add'] += 1
            internal.append({"pmcid": pmcid,
                             "doi": row['doi'],
                             "ack": ack,
                             "source": "Europe PMC"})


def generate_email(internal, error):
    ''' Generate and send the HTML run-summary email, grouping DOIs by source
        (eLife/Elsevier/PMC/arXiv) into cards rather than one flat list.
        Keyword arguments:
          internal: list of internal DOIs
          error: list of error records
        Returns:
          None
    '''
    run_data = JRC.get_run_data(__file__, __version__).strip()
    mode_label = 'WRITE' if ARG.WRITE else 'DRY RUN'
    mode_tone = 'good' if ARG.WRITE else 'warn'
    by_source = collections.defaultdict(list)
    for rec in internal:
        by_source[rec['source']].append(rec)
    kpis = ''.join(JE.kpi_card(f"{len(by_source.get(label, [])):,}", f"{label} added",
                               'good' if by_source.get(label) else 'neutral')
                   for label in SOURCE_LABELS)
    kpis += JE.kpi_card(f"{len(error):,}", "Errors", 'bad' if error else 'neutral')
    # Found section: one DOI card per source (PMCID column for the sources that
    # carry one, e.g. PMC and Europe PMC).
    found = JE.section_header(f"&#128209; Acknowledgements Found ({len(internal):,})")
    if internal:
        for label in SOURCE_LABELS:
            recs = by_source.get(label)
            if not recs:
                continue
            has_pmcid = any(rec.get('pmcid') for rec in recs)
            entries = [(rec['doi'], rec.get('pmcid')) for rec in recs]
            found += JE.doi_card(label, entries, 'good',
                                 second_header='PMCID' if has_pmcid else None)
    else:
        found += ('<div style="color:#5b6b7c;font-size:13px;">'
                  'No new acknowledgements were found.</div>')
    body = JE.body_row(found)
    if error:
        err_entries = [(e['doi'], f"{e['source']}: {e['error']}") for e in error]
        body += JE.body_row(JE.section_header(f"&#9888; Errors ({len(error):,})")
                            + JE.doi_card("Errors", err_entries, 'bad', second_header='Detail'))
    msg = JE.render(os.path.basename(__file__), __version__, run_data,
                    mode_label, mode_tone, kpis, body)
    email = DIS['developer']
    JRC.send_email(msg, DIS['sender'], email, "Acknowledgements updated for DOIs", mime='html')


def processing():
    ''' Find DOIs without acknowledgements.
        Keyword arguments:
          None
        Returns:
          None
    '''
    internal = []
    error = []
    # Europe PMC runs last on purpose: it is a catch-all over every remaining
    # DOI and dedupes against what the earlier passes already resolved this run.
    sources = {'elife': lambda: add_elife_internal_acks(internal, error),
               'elsevier': lambda: add_elsevier_internal_acks(internal, error),
               'pmc': lambda: add_pmc_internal_acks(internal, error),
               'arxiv': lambda: add_arxiv_internal_acks(internal, error),
               'europepmc': lambda: add_europepmc_internal_acks(internal, error)}
    for source, handler in sources.items():
        if ARG.SOURCE in (None, source):
            handler()
    operations = []
    for row in tqdm(internal, total=len(internal), desc="Updating internal DOIs"):
        if not isinstance(row['ack'], str):
            LOGGER.warning(f"Weird format for {row['doi']}")
            continue
        operations.append(UpdateOne({"doi": row['doi']},
                                    {"$set": {"jrc_acknowledgements": row['ack'],
                                              "jrc_ack_source": row['source']}}))
    COUNT['updated'] = len(operations)
    if ARG.WRITE and operations:
        # Unordered so one failed update doesn't block the rest of the batch.
        try:
            result = DB['dis']['dois'].bulk_write(operations, ordered=False)
            COUNT['updated'] = result.modified_count
        except BulkWriteError as err:
            # Unordered means every non-failing op in the batch already went
            # through - report the real counts and keep going instead of
            # losing this run's JSON/email output over one bad document.
            write_errors = err.details.get('writeErrors', [])
            COUNT['updated'] = err.details.get('nModified', 0)
            COUNT['write_errors'] = len(write_errors)
            LOGGER.error(f"{len(write_errors):,} of {len(operations):,} updates failed: "
                        f"{write_errors}")
        except Exception as err:
            terminate_program(err)
    if ARG.SOURCE in (None, 'elife'):
        print(f"eLife DOIs added:    {COUNT['elife_add']:,}")
    if ARG.SOURCE in (None, 'elsevier'):
        print(f"Elsevier DOIs added: {COUNT['elsevier_add']:,}")
    if ARG.SOURCE in (None, 'pmc'):
        print(f"PMC DOIs added:      {COUNT['pmc_add']:,}")
    if ARG.SOURCE in (None, 'arxiv'):
        print(f"arXiv DOIs added:    {COUNT['arxiv_add']:,}")
    if ARG.SOURCE in (None, 'europepmc'):
        print(f"Europe PMC DOIs added: {COUNT['europepmc_add']:,}")
    print(f"DOIs updated:        {COUNT['updated']:,}")
    if COUNT['write_errors']:
        print(f"DOIs failed to update: {COUNT['write_errors']:,}")
    if internal:
        with open('internal_acks.json', 'w', encoding='utf-8') as fileout:
            json.dump(internal, fileout, indent=4)
    if error:
        with open('internal_ack_errors.json', 'w', encoding='utf-8') as fileout:
            json.dump(error, fileout, indent=4)
    if internal:
        generate_email(internal, error)

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Add acknowledgements to internal DOIs")
    PARSER.add_argument('--doi', dest='DOI', default=None,
                        help='Restrict processing to a single DOI')
    PARSER.add_argument('--source', dest='SOURCE', action='store',
                        choices=['elife', 'elsevier', 'pmc', 'arxiv', 'europepmc'],
                        default=None,
                        help='Restrict processing to a single source [all]')
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
