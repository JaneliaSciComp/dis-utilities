"""pull_plos.py
Search the PLOS Solr API for Janelia-affiliated PLOS articles.

Queries https://api.plos.org/search for affiliate:"Janelia" (restricted to full
articles via doc_type:full), paging through every result. In PLOS Solr the `id`
field IS the article DOI (10.1371/journal.*).

Each candidate is confirmed to have a Janelia author using EITHER source:
  - the PLOS record's `affiliate` field (the search already matched "Janelia"), and
  - the Crossref record - an author matching a Janelia ORCID or carrying an asserted
    Janelia affiliation (via doi_common.get_author_details), which also yields the
    Janelian author names.
A candidate confirmed by neither is set aside for manual review (should be rare).
Candidates already in the dois / external_dois / to_ignore collections are skipped.

OUTPUT (files only - this program never modifies the database)
------
    janelia_plos_dois.json    Confirmed records (doi, title, authors,
                              janelia_authors_*, confirmed_by).
    plos_ready.txt            One confirmed DOI per line, ready for ingestion.
    janelia_plos_review.txt   DOIs where neither PLOS nor Crossref confirmed a
                              Janelia author.

An HTML summary email is sent on --test (developer) or --write (receivers list).

DEPENDENCIES
------------
    jrc_common.jrc_common (JRC), doi_common.doi_common (DL), jrc_email.jrc_email (JE)
"""

__version__ = '1.0.0'

import argparse
import collections
import json
import os
import sys
import time
import traceback
from operator import attrgetter

import requests
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL
import jrc_email.jrc_email as JE

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Global variables
ARG = DISCONFIG = LOGGER = None
# Database
DB = {}
DOI_CACHE = {}  # doi -> source collection name
# Counters
COUNT = collections.defaultdict(lambda: 0, {})
# PLOS Solr search: affiliate:"Janelia" over full articles; the `id` field == DOI.
PLOS_SEARCH_URL = "https://api.plos.org/search"
JANELIA_QUERY = 'affiliate:"Janelia"'
PAGE_SIZE = 100
MAX_RETRY = 3


def terminate_program(msg=None):
    ''' Terminate the program gracefully
        Keyword arguments:
          msg: error message or object
        Returns:
          None
    '''
    if msg:
        if not isinstance(msg, str):
            msg = (f"An exception of type {type(msg).__name__} occurred. "
                   f"Arguments:\n{msg.args}")
        LOGGER.critical(msg)
    sys.exit(-1 if msg else 0)


def initialize_program(manifold):
    ''' Connect to MongoDB and pre-load the DOI cache.
        Keyword arguments:
          manifold: database environment ("dev" or "prod")
        Returns:
          None
    '''
    try:
        dbconfig = JRC.get_config("databases")
    except Exception as err:
        terminate_program(err)
    dbo = attrgetter(f"dis.{manifold}.read")(dbconfig)
    print(f"Connecting to {dbo.name} ({manifold}) on {dbo.host}")
    try:
        DB['dis'] = JRC.connect_database(dbo)
    except Exception as err:
        terminate_program(err)
    build_doi_cache()


def build_doi_cache():
    ''' Pre-load known DOIs from the dois, external_dois, and to_ignore collections.
        Keyword arguments:
          None
        Returns:
          None
    '''
    try:
        for rec in DB['dis']['dois'].find({}, {"doi": 1}):
            if rec.get('doi'):
                DOI_CACHE[rec['doi'].lower()] = 'dois'
        for rec in DB['dis']['external_dois'].find({}, {"doi": 1}):
            if rec.get('doi'):
                DOI_CACHE[rec['doi'].lower()] = 'external_dois'
        for rec in DB['dis']['to_ignore'].find({"type": "doi"}, {"key": 1}):
            if rec.get('key'):
                DOI_CACHE[rec['key'].lower()] = 'to_ignore'
    except Exception as err:
        terminate_program(err)
    LOGGER.info(f"Loaded {len(DOI_CACHE):,} known DOIs into cache")


# ---------------------------------------------------------------------------
# PLOS search
# ---------------------------------------------------------------------------

def _get_json(params):
    ''' GET the PLOS Solr endpoint with a few retries.
        Keyword arguments:
          params: query-string parameters
        Returns:
          Parsed JSON dict (aborts the program after MAX_RETRY failures)
    '''
    for attempt in range(MAX_RETRY):
        try:
            resp = requests.get(PLOS_SEARCH_URL, params=params, timeout=30,
                                headers={'User-Agent': 'janelia-dis/pull_plos'})
            if resp.status_code == 200:
                return resp.json()
            LOGGER.warning(f"PLOS HTTP {resp.status_code} (attempt {attempt + 1})")
        except Exception as err:
            LOGGER.warning(f"PLOS request error: {err} (attempt {attempt + 1})")
        time.sleep(2 * (attempt + 1))
    terminate_program("PLOS search failed after retries")
    return {}


def search_plos():
    ''' Page through every PLOS full article matching affiliate:"Janelia".
        Keyword arguments:
          None
        Returns:
          List of Solr docs ({id, title, author_display, affiliate})
    '''
    docs = []
    start = 0
    print(f"Querying PLOS: {JANELIA_QUERY!r}")
    while True:
        data = _get_json({'q': JANELIA_QUERY,
                          'fl': 'id,title,author_display,affiliate',
                          'fq': 'doc_type:full', 'wt': 'json',
                          'rows': PAGE_SIZE, 'start': start})
        resp = data.get('response', {})
        num = resp.get('numFound', 0)
        batch = resp.get('docs', [])
        docs.extend(batch)
        print(f"  fetched {len(docs):,}/{num:,}", end="\r")
        start += PAGE_SIZE
        if start >= num or not batch:
            break
        time.sleep(0.25)
    print(f"\nRetrieved {len(docs):,} PLOS article(s)")
    return docs


# ---------------------------------------------------------------------------
# Janelia author confirmation (PLOS affiliate field or Crossref)
# ---------------------------------------------------------------------------

def get_crossref_record(doi):
    ''' Fetch Crossref metadata for a DOI. Returns {} on any failure. '''
    try:
        resp = JRC.call_crossref(doi, timeout=20)
        return resp.get('message', {}) if resp else {}
    except Exception:
        LOGGER.error(f"Failed to fetch Crossref record for {doi}")
        return {}


def janelia_authors(doi, msg):
    ''' Return the Janelian author dicts for a Crossref message (via
        doi_common.get_author_details), or [] if none / no record.
        Keyword arguments:
          doi: DOI
          msg: Crossref message dict
        Returns:
          List of author dicts (janelian=True entries)
    '''
    if not msg:
        return []
    if 'doi' not in msg:
        msg['doi'] = doi
    time.sleep(0.2)
    try:
        adet = DL.get_author_details(msg, DB['dis']['orcid'])
    except Exception as err:
        terminate_program(err)
    janelians = [a for a in (adet or []) if a['janelian']]
    if janelians and ARG.VERBOSE:
        names = [f"{a['given']} {a['family']} ({a['match']})" for a in janelians]
        print(f"Janelians found for {doi}: {', '.join(names)}")
    return janelians


def plos_has_janelia(doc):
    ''' True if the PLOS record's affiliate field names Janelia (the search matched
        it, so this is the PLOS-side confirmation). '''
    return any('janelia' in (aff or '').lower() for aff in (doc.get('affiliate') or []))


def doc_title(doc):
    ''' Return the article title from a Solr doc (title may be a string or list). '''
    title = doc.get('title')
    if isinstance(title, list):
        return title[0] if title else ''
    return title or ''


def extract_record(doc, doi, janelians):
    ''' Build a compact output record from a PLOS Solr doc and its Crossref Janelians.
        confirmed_by is 'crossref' (Crossref found a Janelian author), 'plos' (only the
        PLOS affiliate field names Janelia), or 'both'.
    '''
    record = {"doi": doi, "title": doc_title(doc),
              "authors": doc.get('author_display') or []}
    current = [f"{a['given']} {a['family']}" for a in janelians if a['match'] == 'ORCID']
    asserted = [f"{a['given']} {a['family']}" for a in janelians if a['match'] == 'asserted']
    if current:
        record['janelia_authors_current'] = current
    if asserted:
        record['janelia_authors_asserted'] = asserted
    if janelians and plos_has_janelia(doc):
        record['confirmed_by'] = 'both'
    elif janelians:
        record['confirmed_by'] = 'crossref'
    else:
        record['confirmed_by'] = 'plos'
    return record


# ---------------------------------------------------------------------------
# Email
# ---------------------------------------------------------------------------

def generate_email(results, summary):   # pylint: disable=unused-argument
    ''' Send the house-style HTML run-summary email. Recipient is the developer with
        --test, otherwise the configured receivers list.
    '''
    if not results and not COUNT['review']:
        return
    run_data = JRC.get_run_data(__file__, __version__).strip()
    mode_label = 'TEST' if ARG.TEST else 'LIVE'
    mode_tone = 'warn' if ARG.TEST else 'good'
    kpis = ''.join([
        JE.kpi_card(f"{COUNT['total']:,}", "Read from PLOS"),
        JE.kpi_card(f"{COUNT['in_dois']:,}", "Already in DB"),
        JE.kpi_card(f"{COUNT['ignored']:,}", "To ignore"),
        JE.kpi_card(f"{COUNT['review']:,}", "Needs review",
                    'warn' if COUNT['review'] else 'neutral'),
        JE.kpi_card(f"{len(results):,}", "Ready to add", 'good' if results else 'neutral'),
    ])
    body = ""
    if results:
        ready = [(rec['doi'], rec.get('confirmed_by')) for rec in results]
        body += JE.body_row(JE.section_header(f"&#10003; Ready to Add ({len(results):,})")
                            + JE.doi_card("Ready to Add", ready, 'good',
                                          second_header='Confirmed by'))
    if COUNT['review']:
        body += JE.body_row(
            JE.section_header(f"&#9888; Needs review ({COUNT['review']:,})")
            + f'<div style="color:{JE.GRAY};font-size:12px;padding:2px 12px;">'
            'Matched the PLOS Janelia search but neither the PLOS affiliations nor '
            'Crossref confirmed a Janelia author - see janelia_plos_review.txt.</div>')
    msg = JE.render(os.path.basename(__file__), __version__, run_data,
                    mode_label, mode_tone, kpis, body)
    try:
        email = DISCONFIG['developer'] if ARG.TEST else DISCONFIG['receivers']
        LOGGER.info(f"Sending email to {email}")
        JRC.send_email(msg, DISCONFIG['sender'], email, "PLOS DOI sync", mime='html')
    except Exception as err:
        print(str(err))
        traceback.print_exc()
        terminate_program(err)


# ---------------------------------------------------------------------------
# Main processing
# ---------------------------------------------------------------------------

def processing():
    ''' Search PLOS, confirm Janelia authorship, filter against the DOI cache, and
        write the output files.
    '''
    docs = search_plos()
    COUNT['total'] = len(docs)
    seen = set()
    results = []
    review = []
    for idx, doc in enumerate(docs, 1):
        doi = (doc.get('id') or '').strip().lower()
        if not doi:
            COUNT['no_doi'] += 1
            continue
        if doi in seen:
            COUNT['skipped_dup'] += 1
            continue
        seen.add(doi)
        if doi in DOI_CACHE:
            if DOI_CACHE[doi] == 'dois':
                COUNT['in_dois'] += 1
            else:
                COUNT['ignored'] += 1
            continue
        print(f"  Crossref lookup {idx}/{COUNT['total']}: {doi}          ", end="\r")
        janelians = janelia_authors(doi, get_crossref_record(doi))
        if janelians or plos_has_janelia(doc):
            results.append(extract_record(doc, doi, janelians))
            COUNT['confirmed_crossref' if janelians else 'confirmed_plos'] += 1
        else:
            review.append(doi)
    COUNT['review'] = len(review)
    summary = (
        f"DOIs read from PLOS:            {COUNT['total']:,}\n"
        f"Skipped (no DOI):              {COUNT['no_doi']:,}\n"
        f"Skipped (duplicate):           {COUNT['skipped_dup']:,}\n"
        f"DOIs already in database:      {COUNT['in_dois']:,}\n"
        f"DOIs to ignore:                {COUNT['ignored']:,}\n"
        f"Confirmed via Crossref:        {COUNT['confirmed_crossref']:,}\n"
        f"Confirmed via PLOS only:       {COUNT['confirmed_plos']:,}\n"
        f"Needs review (neither):        {COUNT['review']:,}\n"
        f"DOIs ready for processing:     {len(results):,}"
    )
    print("\n" + summary)
    if results:
        with open('janelia_plos_dois.json', 'w', encoding='utf-8') as fh:
            json.dump(results, fh, indent=2, ensure_ascii=False)
        with open('plos_ready.txt', 'w', encoding='utf-8') as fh:
            for rec in results:
                fh.write(rec['doi'] + "\n")
        LOGGER.info("Wrote janelia_plos_dois.json and plos_ready.txt")
    if review:
        with open('janelia_plos_review.txt', 'w', encoding='utf-8') as fh:
            for doi in review:
                fh.write(doi + "\n")
        LOGGER.info("Wrote janelia_plos_review.txt")
    if ARG.TEST or ARG.WRITE:
        generate_email(results, summary)


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    PARSER.add_argument('--manifold', dest='MANIFOLD', default='prod',
                        choices=['dev', 'prod'],
                        help='MongoDB manifold (default: prod)')
    PARSER.add_argument('--test', dest='TEST', action='store_true', default=False,
                        help='Send the summary email to the developer only')
    PARSER.add_argument('--write', dest='WRITE', action='store_true', default=False,
                        help='Live run: email the summary to the receivers list '
                             '(this tool writes only candidate files, never the DB)')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true', default=False,
                        help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true', default=False,
                        help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    DISCONFIG = JRC.simplenamespace_to_dict(JRC.get_config("dis"))
    initialize_program(ARG.MANIFOLD)
    processing()
    terminate_program()
