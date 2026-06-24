''' pull_external_acks.py

PURPOSE
-------
Searches eLife, Elsevier ScienceDirect, PubMed Central, and arXiv (via OpenAlex
full-text search) for articles that acknowledge Janelia Research Campus, then
extracts and stores the acknowledgement text against each DOI in the DIS MongoDB
external_dois collection.

INPUTS
------
- ELSEVIER_API_KEY environment variable (required): API key for the Elsevier API.
- NCBI_API_KEY environment variable (required): API key for the NCBI E-utilities API.
- DIS MongoDB database (read/write depending on --write flag).
- Command-line flags:
    --term     Search term to look for in acknowledgements (default: Janelia).
    --days     Restrict Elsevier (by publication date) and PMC (by Entrez date)
               searches to the last N days. Omit to search all available records.
    --source   Restrict processing to a single source (elife, elsevier, pmc, or
               arxiv). Omit to process all sources.
    --write    Actually update the database (default: dry-run).
    --test     Send email to developer rather than normal recipients.
    --verbose  Increase logging verbosity.
    --debug    Maximum logging verbosity.

HIGH-LEVEL FLOW
---------------
1. Initialization
   - Checks that ELSEVIER_API_KEY and NCBI_API_KEY are set.
   - Connects to the DIS MongoDB database.
   - Loads all DOIs from the dois and external_dois collections into memory
     for fast duplicate detection.
   - Loads the to_ignore collection (type="doi") into memory. These are known
     non-Janelia papers, so the Janelia-author check is skipped for them and
     they are eligible for insertion into external_dois.
2. eLife search (_process_elife_articles)
   - Pages through the eLife search API for articles matching --term,
     optionally restricted to articles published on or after a start-date
     derived from --days (server-side filtering via the eLife API).
   - For each new DOI, retrieves the eLife record and extracts acknowledgement
     text via doi_common.get_acknowledgements.
   - Passes acknowledgements to _process_ack for term checking and storage.
3. Elsevier search (_process_elsevier_articles)
   - Pages through the ScienceDirect full-text PUT search API for articles
     matching --term.
   - When --days is set, post-filters results in Python by comparing the
     result's publication date (publicationDate, coverDate, or loadDate,
     whichever is present) against the computed cutoff date.
   - For each new DOI, retrieves acknowledgement text via
     doi_common.get_acknowledgements and passes it to _process_ack.
4. PubMed Central search (_process_pmc_articles)
   - Queries the NCBI E-utilities esearch endpoint for PMC articles matching
     "{term}[Acknowledgements]", optionally restricted to the last N days
     by Entrez date (datetype=edat).
   - Fetches full XML records in batches of 200 via the efetch endpoint and
     parses acknowledgement text from the XML structure.
5. Classification (_process_ack)
   - DOIs already in the database are skipped.
   - DOIs whose acknowledgement text does not contain --term are skipped.
   - Remaining DOIs are passed to fetch_and_store_doi.
6. arXiv search via OpenAlex (_process_openalex_articles)
   - Queries OpenAlex for arXiv works whose FULL TEXT contains --term (the arXiv
     API only searches metadata, so it cannot find term-in-acknowledgement
     papers; OpenAlex indexes the body). Optionally restricted to the last N days
     by publication date.
   - For each new arXiv DOI, extracts the Acknowledgements section via
     doi_common.get_acknowledgements (which downloads the arXiv HTML render, then
     e-print TeX), and keeps it only if --term is in that section. arXiv DOIs are
     DataCite, so matches are stored via
     _store_arxiv_doi (the publication date comes from OpenAlex) rather than the
     CrossRef-based fetch_and_store_doi.
7. Janelia-author guard (_janelia_authors), applied to EVERY source before a DOI
   is written to external_dois. The DOI's metadata record (CrossRef message for
   eLife/Elsevier/PMC, DataCite attributes for arXiv) is run through
   doi_common.get_author_details against the orcid collection, exactly as in
   pull_wos.py. If the article has any (potential) Janelia authors it really
   belongs in the internal dois collection, not external_dois, so it is NOT
   written to the database; it is recorded in janelia_authored.json for review.
   Author-lookup failures are treated conservatively the same way - diverted, not
   written. A diverted DOI is marked in the DOI cache so a later source does not
   re-process it. DOIs in the to_ignore collection are known non-Janelia papers,
   so the author check is skipped and they go straight to storage.
8. Storage (fetch_and_store_doi / _store_arxiv_doi)
   - fetch_and_store_doi looks up CrossRef metadata (skips DataCite DOIs), runs
     the Janelia-author guard, and upserts a record into external_dois with doi,
     jrc_acknowledgements, jrc_publishing_date, and (for PMC) jrc_pmc.
   - _store_arxiv_doi upserts the arXiv DataCite DOI directly with doi,
     jrc_acknowledgements, and the OpenAlex publication date.
   - Both register each written DOI in the in-memory DOI cache, so a DOI written
     by one source is never written again by a later source in the same run.
9. Output
   - Prints a per-source summary of counts.
   - Writes external_acks.json with all new records.
   - Writes janelia_authored.json with the DOIs (from any source) diverted by the
     Janelia-author guard (acknowledge Janelia but are Janelia-authored, or could
     not be author-verified).
   - Sends an email summary if --write or --test is set.

DEPENDENCIES
------------
- jrc_common.jrc_common  (JRC): logging, config, database, and email helpers.
- doi_common.doi_common  (DL): DOI record retrieval and acknowledgement extraction.
- requests: HTTP calls to eLife, Elsevier, and NCBI APIs.
- xmltodict, xml.etree.ElementTree: PMC XML parsing.
- tqdm: progress bars.

NOTES
-----
- All three sources apply a 0.3–0.7 second sleep between requests to respect
  API rate limits.
- eLife date filtering is server-side via the start-date query parameter,
  filtering by publication date.
- The NCBI batch fetch sleeps 0.1 s per batch with an API key, 0.34 s without.
- Elsevier date filtering is done in Python (post-fetch) because the
  ScienceDirect full-text PUT search API does not reliably honour server-side
  date filters. The result's publicationDate, coverDate, or loadDate field
  (whichever is present) is compared against the computed cutoff. Run with
  --debug to log the available date fields on the first result if tuning
  is needed.
- PMC date filtering is server-side via the NCBI esearch datetype=edat
  parameter, i.e. filtered by when the article was added to PubMed Central.
'''

__version__ = '1.6.0'

import argparse
import collections
from datetime import datetime, timedelta
import json
from operator import attrgetter
import os
import re
import sys
import time
import traceback
import xml.etree.ElementTree as ET
import requests
import xmltodict
from tqdm import tqdm
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL

# pylint: disable=broad-exception-caught,broad-exception-raised,logging-fstring-interpolation

# Database
DB = {}
# Counters
COUNT = collections.defaultdict(lambda: 0, {})
# Global variables
ARG = DISCONFIG = LOGGER = None
ELIFE_SEARCH = "https://api.elifesciences.org/search"
SD_SEARCH_URL = "https://api.elsevier.com/content/search/sciencedirect"
PAGE_SIZE = 100
PMC_BATCH_SIZE = 200
DOI = {}
# DOIs in the to_ignore collection (type="doi") are known non-Janelia papers, so
# the Janelia-author check is skipped for them and they are eligible for insertion
# into external_dois.
IGNORE = set()
ACK_DOI_IGNORE = set()
RECORDS = []
# DOIs (any source) that acknowledge Janelia but are (potentially) Janelia-authored
# - or whose authorship could not be verified - so they belong in the internal
# dois collection rather than external_dois. These are NOT written to the database;
# they are written to a single review JSON file (janelia_authored.json).
INTERNAL_RECORDS = []
# Result totals discovered during paging (for progress bars over generators)
TOTALS = {}
# OpenAlex full-text discovery of arXiv preprints
OPENALEX_API = "https://api.openalex.org/works"
OPENALEX_MAILTO = "svirskasr@janelia.hhmi.org"
# OpenAlex source id for arXiv (Cornell University)
OPENALEX_ARXIV_SOURCE = "S4306400194"
OPENALEX_PAGE_SIZE = 200
# Polite User-Agent for OpenAlex/arXiv requests
ARXIV_HEADERS = {'User-Agent': 'janelia-dis/pull_external_acks'}
# Shared HTTP session so the many sequential search-paging calls reuse TCP/TLS
# connections (connection pooling) instead of reconnecting on every request.
SESSION = requests.Session()

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
    for key in ("ELSEVIER_API_KEY", "NCBI_API_KEY"):
        if key not in os.environ:
            terminate_program(f"Missing API key - set in {key} environment variable")
    try:
        dbconfig = JRC.get_config("databases")
    except Exception as err:
        terminate_program(err)
    dbs = ['dis']
    for source in dbs:
        dbo = attrgetter(f"{source}.prod.write")(dbconfig)
        LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, 'prod', dbo.host, dbo.user)
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)
    # DOI is used only for presence tests (de-duplication); the document body is
    # never read back, so project just the doi field to keep the cache small.
    for coll in ('dois', 'external_dois'):
        try:
            rows = DB['dis'][coll].find({}, {"doi": 1, "_id": 0})
        except Exception as err:
            terminate_program(err)
        for row in rows:
            if row.get('doi'):
                DOI[row['doi']] = True
    LOGGER.info(f"Found {len(DOI):,} DOIs in database")
    # Known non-Janelia DOIs (to_ignore, type="doi"): eligible for external_dois
    # without an author check.
    try:
        rows = DB['dis'].to_ignore.find({"type": "doi"}, {"key": 1})
    except Exception as err:
        terminate_program(err)
    for row in rows:
        if row.get('key'):
            IGNORE.add(row['key'].lower())
    LOGGER.info(f"Found {len(IGNORE):,} non-Janelia DOIs in to_ignore (author check skipped)")
    try:
        rows = DB['dis'].to_ignore.find({"type": "ack_doi"}, {"key": 1})
    except Exception as err:
        terminate_program(err)
    for row in rows:
        if row.get('key'):
            ACK_DOI_IGNORE.add(row['key'].lower())
    LOGGER.info(f"Found {len(ACK_DOI_IGNORE):,} DOIs with confirmed no Janelia ack")


def doiurl(doi):
    ''' Format a DOI as a URL
        Keyword arguments:
          doi: DOI to format
        Returns:
          Formatted DOI
    '''
    return f"&nbsp;&nbsp;<a href='https://dis.int.janelia.org/doiui/{doi}'>{doi}</a><br>"


def text_to_html_table(text):
    ''' Convert text to an HTML table
        Keyword arguments:
          text: text to convert
        Returns:
          HTML table
    '''
    rows = []
    for line in text.strip().splitlines():
        if ":" in line:
            label, value = line.rsplit(":", 1)
            rows.append((label.strip(), value.strip()))
    html = ['<table>']
    for label, value in rows:
        html.append(f'  <tr><td>{label}:</td><td>{value}</td></tr>')
    html.append('</table>')
    return "\n".join(html)


def generate_email(summary, records):
    ''' Generate and send an email
        Keyword arguments:
          summary: summary of the results
          records: list of record dicts with doi and source keys
        Returns:
          None
    '''
    if not records:
        return
    msg = "<br>The following external DOIs were added to the database:<br>"
    for rec in records:
        msg += f"  {doiurl(rec['doi'])} ({rec['source']})<br>"
    msg = JRC.get_run_data(__file__, __version__) + "<br><br>" \
        + text_to_html_table(summary) + "<br>" + msg
    try:
        email = DISCONFIG['developer'] if ARG.TEST else DISCONFIG['receivers']
        LOGGER.info(f"Sending email to {email}")
        opts = {'mime': 'html'}
        JRC.send_email(msg, DISCONFIG['sender'], email,
                       "External acknowledgement DOI sync", **opts)
    except Exception as err:
        print(str(err))
        traceback.print_exc()
        terminate_program(err)


def _request_with_retry(method, url, params=None, headers=None, body=None, retries=3):  # pylint: disable=too-many-arguments,too-many-positional-arguments
    ''' HTTP request with retry on rate-limit or server errors.
        Keyword arguments:
          method: HTTP method string ('GET', 'PUT', etc.)
          url: request URL
          params: query parameters dict
          headers: request headers dict
          body: request body for JSON payloads
          retries: maximum number of attempts
        Returns:
          requests.Response
    '''
    for attempt in range(retries):
        try:
            resp = SESSION.request(method, url, params=params, headers=headers,
                                   json=body, timeout=30)
        except requests.RequestException as err:
            LOGGER.warning(f"Request error (attempt {attempt + 1}): {err}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            continue
        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", 15))
            LOGGER.warning(f"Rate limited; waiting {wait}s...")
            time.sleep(wait)
            continue
        if resp.status_code >= 500:
            LOGGER.warning(f"Server error {resp.status_code} (attempt {attempt + 1}): {url}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            continue
        resp.raise_for_status()
        return resp
    raise RuntimeError(f"Request to {url} failed after {retries} retries")


def search_elife():
    ''' Page through the eLife search API and yield article summary dicts.
        Keyword arguments:
          None
        Yields:
          article summary dicts from the eLife search API
    '''
    page = 1
    total = None
    min_date = (datetime.now() - timedelta(days=ARG.DAYS)).strftime('%Y-%m-%d') \
               if ARG.DAYS else None
    if min_date:
        LOGGER.info(f"Restricting eLife search to articles published since {min_date}")
    while total is None or (page - 1) * PAGE_SIZE < total:
        params = {
            'for': ARG.TERM,
            'show': PAGE_SIZE,
            'page': page,
            'order': 'date',
        }
        if min_date:
            params['start-date'] = min_date
        try:
            resp = _request_with_retry('GET', ELIFE_SEARCH, params=params)
            data = resp.json()
        except Exception as err:
            LOGGER.warning(f"eLife search error on page {page}: {err}")
            break
        if total is None:
            total = data.get('total', 0)
            TOTALS['elife'] = total
            LOGGER.info(f"eLife search found {total:,} results for '{ARG.TERM}'")
        items = data.get('items', [])
        if not items:
            break
        yield from items
        page += 1
        time.sleep(0.3)


def get_doi_from_item(item):
    ''' Extract a normalised DOI from an eLife search result item.
        Keyword arguments:
          item: eLife search result dict
        Returns:
          lowercase DOI string, or None
    '''
    doi = item.get('doi', '')
    if doi:
        return doi.lower()
    article_id = item.get('id', '')
    if article_id:
        return f"10.7554/elife.{article_id}".lower()
    return None


def search_sciencedirect():
    """Yield all search result dicts for a phrase (full-text PUT search)."""
    headers = {
        "X-ELS-APIKey": os.environ['ELSEVIER_API_KEY'],
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    offset = 0
    total = None

    while total is None or offset < total:
        body = {
            "qs": f'"{ARG.TERM}"',
            "display": {"offset": offset, "show": PAGE_SIZE, "sortBy": "date"},
        }
        try:
            data = _request_with_retry('PUT', SD_SEARCH_URL, headers=headers, body=body).json()
        except Exception as err:
            LOGGER.warning(f"Elsevier search error at offset {offset}: {err}")
            break
        if total is None:
            total = data.get("resultsFound", 0)
            TOTALS['elsevier'] = total
            LOGGER.info(f"Elsevier search found {total:,} results for '{ARG.TERM}'")
        results = data.get("results", [])
        if not results:
            break
        yield from results
        offset += len(results)
        if offset < total:
            time.sleep(0.3)


def _parse_pmc_ids(ids):
    ''' Extract DOI and PMCID from a PMC article-id list.
        Keyword arguments:
          ids: list of article-id dicts from xmltodict
        Returns:
          (doi, pmcid) tuple, either value may be None
    '''
    doi = pmcid = None
    for aid in ids:
        if aid.get("@pub-id-type") == "doi":
            doi = (aid.get("#text") or "").lower()
        elif aid.get("@pub-id-type") == "pmcid":
            pmcid = aid.get("#text")
    return doi, pmcid


def _extract_ack_from_elem(elem, acks):
    ''' Append text from a single ack paragraph element into acks.
        Keyword arguments:
          elem: one element from an ackp list (dict, list, or str)
          acks: list to append extracted strings into
        Returns:
          None
    '''
    if isinstance(elem, dict) and elem.get('#text'):
        acks.append(elem['#text'])
    elif isinstance(elem, list):
        for item in elem:
            if isinstance(item, dict) and item.get('#text'):
                acks.append(item['#text'])
            elif isinstance(item, str):
                acks.append(item)
    elif isinstance(elem, str):
        acks.append(elem)


def _parse_pmc_ack_text(ack):
    ''' Extract acknowledgement strings from a PMC ack element.
        Keyword arguments:
          ack: ack dict or list from xmltodict
        Returns:
          List of acknowledgement strings (may be empty)
    '''
    acks = []
    if isinstance(ack, dict):
        ackp = ack.get('p')
        if not ackp:
            return acks
        if isinstance(ackp, dict) and ackp.get('#text'):
            acks.append(ackp['#text'])
        elif isinstance(ackp, list):
            for elem in ackp:
                _extract_ack_from_elem(elem, acks)
        else:
            acks.append(ackp)
    elif isinstance(ack, list) and ack:
        acks.extend(ack)
    return acks


def _parse_pmc_article(article):
    ''' Extract pmcid, doi, and ack text from a single PMC article dict.
        Keyword arguments:
          article: article dict from xmltodict
        Returns:
          Result dict with keys pmcid/doi/ack, or None if the article should be skipped
    '''
    ids = article.get("front", {}).get("article-meta", {}).get("article-id", [])
    if not ids:
        return None
    doi, pmcid = _parse_pmc_ids(ids)
    raw_ack = article.get("back", {}).get("ack", {})
    if not raw_ack:
        return None
    try:
        acks = _parse_pmc_ack_text(raw_ack)
    except Exception as err:
        LOGGER.warning(f"Error parsing acknowledgements for {pmcid}, skipping: {err}")
        return None
    LOGGER.debug(f"PMCID: {pmcid} DOI: {doi}")
    if not acks:
        return None
    if isinstance(acks[0], dict):
        LOGGER.warning(f"Unrecognised acknowledgement format for {doi}")
        return None
    return {"pmcid": pmcid, "doi": doi if doi else "n/a", "ack": "\n".join(acks)}


def _fetch_pmc_batch(base_url, batch_pmids, api_key):
    ''' Fetch and parse one batch of PMC articles via efetch.
        Keyword arguments:
          base_url: NCBI E-utilities base URL
          batch_pmids: list of PMC IDs to fetch
          api_key: NCBI API key (or None)
        Returns:
          List of article dicts from xmltodict
    '''
    fetch_params = {"db": "pmc", "id": ",".join(batch_pmids), "retmode": "xml"}
    if api_key:
        fetch_params["api_key"] = api_key
    fetch_response = _request_with_retry('GET', f"{base_url}efetch.fcgi", params=fetch_params)
    root = ET.fromstring(fetch_response.content)
    root_json = xmltodict.parse(ET.tostring(root))
    article = root_json.get("pmc-articleset", {}).get("article", [])
    return article if isinstance(article, list) else [article]


def search_pmc(term, max_results=5000, api_key=None, days=None):
    ''' Search PubMed Central for articles with Janelia in acknowledgements.
        Keyword arguments:
          term: NCBI field-qualified search term (e.g. "Janelia[Acknowledgements]")
          max_results: maximum number of PMC IDs to retrieve
          api_key: NCBI API key for higher rate limits
          days: if set, restrict to articles added to PMC within this many days
        Returns:
          List of dicts with keys: pmcid, doi, ack
    '''
    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
    search_params = {"db": "pmc", "term": term, "retmax": max_results, "retmode": "json"}
    if api_key:
        search_params["api_key"] = api_key
    if days:
        search_params["datetype"] = "edat"
        search_params["reldate"] = days
        LOGGER.info(f"Searching PubMed Central for '{term}' (last {days} days)")
    else:
        LOGGER.info(f"Searching PubMed Central for '{term}'")
    search_response = _request_with_retry('GET', f"{base_url}esearch.fcgi", params=search_params)
    search_data = search_response.json()
    pmids = search_data.get("esearchresult", {}).get("idlist", [])
    total_count = int(search_data.get("esearchresult", {}).get("count", 0))
    LOGGER.info(f"Found {total_count:,} PMC articles, retrieving {len(pmids):,} records")
    if total_count > len(pmids):
        LOGGER.warning(f"PMC results truncated: {total_count:,} articles matched but only "
                       f"{len(pmids):,} retrieved (max_results={max_results:,}). "
                       f"{total_count - len(pmids):,} articles were NOT processed.")
    if not pmids:
        return []
    articles = []
    for i in tqdm(range(0, len(pmids), PMC_BATCH_SIZE), desc="Fetching PMC articles"):
        batch_pmids = pmids[i:i + PMC_BATCH_SIZE]
        if i > 0:
            time.sleep(0.1 if api_key else 0.34)
        articles.extend(_fetch_pmc_batch(base_url, batch_pmids, api_key))
    return articles


# -----------------------------------------------------------------------------
# OpenAlex full-text discovery of arXiv preprints. Acknowledgement extraction is
# handled by doi_common.get_acknowledgements (arXiv HTML render / e-print TeX).
# -----------------------------------------------------------------------------

def search_openalex():
    ''' Page through OpenAlex for arXiv works whose FULL TEXT contains ARG.TERM.
        Unlike the arXiv API (metadata only), OpenAlex searches the paper body,
        so it finds preprints that mention the term only in their acknowledgements.
        Keyword arguments:
          None
        Yields:
          {arxiv_id, title, publication_date, doi} candidate dicts
    '''
    flt = f"fulltext.search:{ARG.TERM},primary_location.source.id:{OPENALEX_ARXIV_SOURCE}"
    if ARG.DAYS:
        min_date = (datetime.now() - timedelta(days=ARG.DAYS)).strftime('%Y-%m-%d')
        flt += f",from_publication_date:{min_date}"
        LOGGER.info(f"Restricting OpenAlex search to works published since {min_date}")
    cursor = '*'
    total = None
    while cursor:
        params = {'filter': flt, 'per-page': OPENALEX_PAGE_SIZE, 'cursor': cursor,
                  'mailto': OPENALEX_MAILTO,
                  'select': 'id,doi,title,publication_date,locations'}
        if os.environ.get('OPENALEX_API_KEY'):
            params['api_key'] = os.environ['OPENALEX_API_KEY']
        try:
            resp = _request_with_retry('GET', OPENALEX_API, params=params, headers=ARXIV_HEADERS)
            page = resp.json()
        except Exception as err:
            LOGGER.warning(f"OpenAlex search error: {err}")
            break
        if total is None:
            total = page.get('meta', {}).get('count', 0)
            TOTALS['openalex'] = total
            LOGGER.info(f"OpenAlex found {total:,} arXiv works with '{ARG.TERM}' in full text")
        results = page.get('results', [])
        if not results:
            break
        for work in results:
            cand = _parse_openalex(work)
            if cand:
                yield cand
        cursor = page.get('meta', {}).get('next_cursor')
        time.sleep(0.3)


def _parse_openalex(work):
    ''' Pull arXiv id, DOI, title and date from one OpenAlex work.
        Keyword arguments:
          work: OpenAlex work dict
        Returns:
          {arxiv_id, title, publication_date, doi} dict, or None
    '''
    arxiv_id = None
    for loc in work.get('locations') or []:
        for url in (loc.get('landing_page_url'), loc.get('pdf_url')):
            match = re.search(r'arxiv\.org/(?:abs|pdf)/([\w.]+?)(?:v\d+)?(?:\.pdf)?$', url or '')
            if match:
                arxiv_id = match.group(1)
                break
        if arxiv_id:
            break
    doi = (work.get('doi') or '').replace('https://doi.org/', '').lower() or None
    if not arxiv_id:
        match = re.search(r'arxiv\.(.+)$', doi or '', re.IGNORECASE)
        if match:
            arxiv_id = match.group(1)
    if not arxiv_id:
        return None
    return {'arxiv_id': arxiv_id,
            'title': re.sub(r'\s+', ' ', (work.get('title') or '').strip()),
            'publication_date': work.get('publication_date') or '',
            'doi': doi or arxiv_doi(arxiv_id)}


def arxiv_doi(arxiv_id):
    ''' Build the arXiv DataCite DOI from an arXiv id (version stripped).
        Keyword arguments:
          arxiv_id: arXiv id, possibly with a trailing version (2101.08910v1)
        Returns:
          lowercase DOI string (10.48550/arxiv.2101.08910)
    '''
    base = re.sub(r'v\d+$', '', arxiv_id)
    return f"10.48550/arxiv.{base}"


def _janelia_authors(doi, rec):
    ''' Determine whether a DOI has any (potential) Janelia authors, so it can be
        diverted away from external_dois (it really belongs in the internal dois
        collection). Mirrors the get_author_details check in pull_wos.py and works
        for any source: get_author_details reads CrossRef messages (rec['author'])
        or DataCite attributes (rec['creators']) and flags Janelians against the
        orcid collection.
        Keyword arguments:
          doi: DOI string
          rec: metadata record - a CrossRef message or DataCite attributes dict
        Returns:
          List of janelian author label strings (empty if none), or None if the
          author details could not be determined (treated conservatively).
    '''
    if not rec:
        return None
    # get_author_details augments matches with OpenAlex data via rec['doi'].
    if 'doi' not in rec:
        rec['doi'] = doi
    time.sleep(0.2)
    try:
        adet = DL.get_author_details(rec, DB['dis']['orcid'])
    except Exception as err:
        LOGGER.warning(f"get_author_details failed for {doi}: {err}")
        return None
    janelians = [a for a in (adet or []) if a.get('janelian')]
    labels = []
    for auth in janelians:
        name = f"{auth.get('given', '')} {auth.get('family') or auth.get('name', '')}".strip()
        labels.append(f"{name} ({auth.get('match')})")
    if labels:
        LOGGER.info(f"Janelia author(s) for {doi}: {', '.join(labels)}")
    return labels


def _divert_janelia_authored(doi, ack, source, janelians, pub_date=None, pmcid=None):
    ''' Record a DOI that acknowledges Janelia but is (potentially) Janelia-authored
        - or whose authorship could not be verified - so it is NOT written to
        external_dois. It is appended to INTERNAL_RECORDS (written to a single
        review file for all sources) and marked in the DOI cache so a later source
        does not re-process it.
        Keyword arguments:
          doi: DOI string
          ack: acknowledgement text
          source: source counter prefix ('elife', 'elsevier', 'pmc', 'openalex')
          janelians: list of janelian labels, or None if the check failed
          pub_date: publication date string, if known
          pmcid: PubMed Central ID, if any
        Returns:
          None
    '''
    reason = 'author check failed' if janelians is None else 'janelia authors'
    COUNT[f'{source}_author_check_failed' if janelians is None
          else f'{source}_janelia_authored'] += 1
    INTERNAL_RECORDS.append({'doi': doi, 'acknowledgement': ack, 'source': source,
                             'publication_date': pub_date, 'pmcid': pmcid,
                             'janelia_authors': janelians, 'excluded_reason': reason})
    DOI[doi] = {'doi': doi}


def _store_arxiv_doi(doi, ack, pub_date):
    ''' Upsert an arXiv (DataCite) DOI into external_dois. arXiv DOIs are DataCite,
        so this bypasses the CrossRef path in fetch_and_store_doi and uses the
        publication date supplied by OpenAlex. The in-memory DOI cache is updated
        so the same DOI is never written twice in one run (e.g. if a later source
        re-discovers it).
        Keyword arguments:
          doi: arXiv DataCite DOI (lowercase)
          ack: acknowledgement text
          pub_date: publication date string from OpenAlex (may be empty)
        Returns:
          None
    '''
    # Guard against a duplicate written by an earlier source in this same run.
    if doi in DOI:
        COUNT['openalex_in_database'] += 1
        return
    payload = {'doi': doi, 'jrc_acknowledgements': ack}
    if pub_date:
        payload['jrc_publishing_date'] = pub_date
    if ARG.WRITE:
        try:
            result = DB['dis']['external_dois'].update_one({"doi": doi},
                                                           {"$set": payload}, upsert=True)
            if result.modified_count or result.upserted_id:
                COUNT['openalex_dois_written'] += 1
                RECORDS.append({'doi': doi, 'acknowledgement': ack, 'source': 'openalex',
                                'pmcid': None})
        except Exception as err:
            terminate_program(err)
    else:
        COUNT['openalex_dois_written'] += 1
        RECORDS.append({'doi': doi, 'acknowledgement': ack, 'source': 'openalex', 'pmcid': None})
    DOI[doi] = payload


def fetch_and_store_doi(doi, ack, source, pmcid=None):
    ''' Fetch metadata from CrossRef/DataCite and upsert a DOI record into external_dois.
        Keyword arguments:
          doi: DOI string
          ack: acknowledgement text
          source: counter key prefix ('elife', 'elsevier', or 'pmc')
          pmcid: PubMed Central ID (PMC source only)
        Returns:
          True if the record was processed, False if it should be skipped
    '''
    payload = {'doi': doi, 'jrc_acknowledgements': ack}
    if pmcid:
        payload['jrc_pmc'] = pmcid
    try:
        if DL.is_datacite(doi):
            LOGGER.warning(f"Skipping DataCite DOI: {doi}")
            COUNT[f'{source}_notfound'] += 1
            return False
        rec = JRC.call_crossref(doi)
        if rec:
            rec = rec['message']
        time.sleep(.7)
    except Exception as err:
        LOGGER.warning(err)
        COUNT[f'{source}_notfound'] += 1
        return False
    if not rec:
        COUNT[f'{source}_notfound'] += 1
        return False
    payload['jrc_publishing_date'] = DL.get_publishing_date(rec)
    # Guard: a DOI with (potential) Janelia authors belongs in the internal dois
    # collection, not external_dois. Divert it to the review file instead. DOIs in
    # to_ignore are known non-Janelia papers, so the check is skipped for them.
    if doi in IGNORE:
        COUNT[f'{source}_author_check_skipped'] += 1
    else:
        janelians = _janelia_authors(doi, rec)
        if janelians is None or janelians:
            _divert_janelia_authored(doi, ack, source, janelians,
                                     pub_date=payload['jrc_publishing_date'], pmcid=pmcid)
            return False
    if ARG.WRITE:
        try:
            result = DB['dis']['external_dois'].update_one({"doi": payload['doi']},
                                                           {"$set": payload}, upsert=True)
            if result.modified_count or result.upserted_id:
                COUNT[f'{source}_dois_written'] += 1
                RECORDS.append({'doi': doi, 'acknowledgement': ack, 'source': source,
                                'pmcid': pmcid})
        except Exception as err:
            terminate_program(err)
    else:
        COUNT[f'{source}_dois_written'] += 1
        RECORDS.append({'doi': doi, 'acknowledgement': ack, 'source': source, 'pmcid': pmcid})
    # Register the DOI so a later source (e.g. OpenAlex) won't write it again.
    DOI[doi] = payload
    return True


def _process_ack(doi, ack, source, write_ignore=False):
    ''' Check acknowledgement text and store the DOI if the search term is present.
        Keyword arguments:
          doi:          DOI string
          ack:          acknowledgement text
          source:       counter key prefix ('elife' or 'elsevier')
          write_ignore: if True, upsert doi into to_ignore when term is absent
        Returns:
          None
    '''
    if not ack:
        COUNT[f'{source}_no_ack'] += 1
        return
    if ARG.TERM.lower() not in ack.lower():
        COUNT[f'{source}_term_absent'] += 1
        if write_ignore and ARG.WRITE:
            DB['dis']['to_ignore'].update_one(
                {"type": "ack_doi", "key": doi},
                {"$setOnInsert": {"type": "ack_doi", "key": doi,
                                  "reason": "Janelia not in ack text"}},
                upsert=True)
            COUNT[f'{source}_ack_doi_ignored'] += 1
        return
    fetch_and_store_doi(doi, ack, source)


def _progress(generator, desc, source):
    ''' Iterate a paging generator under a tqdm bar, filling in the total once known
        and showing live written/skipped counts as the postfix.
        Keyword arguments:
          generator: generator yielding result items
          desc: tqdm description
          source: counter/total key prefix ('elife' or 'elsevier')
        Yields:
          items from the generator
    '''
    pbar = tqdm(desc=desc)
    try:
        for item in generator:
            if pbar.total is None and source in TOTALS:
                pbar.total = TOTALS[source]
                pbar.refresh()
            yield item
            pbar.set_postfix(written=COUNT[f'{source}_dois_written'],
                             in_db=COUNT[f'{source}_in_database'], refresh=False)
            pbar.update(1)
    finally:
        pbar.close()


def _process_elife_articles():
    ''' Page through eLife search results and process acknowledgements.
        Keyword arguments:
          None
        Returns:
          None
    '''
    LOGGER.info(f"Searching for {ARG.TERM} in eLife")
    for item in _progress(search_elife(), "Scanning eLife articles", 'elife'):
        COUNT['elife_read'] += 1
        doi = get_doi_from_item(item)
        if not doi:
            COUNT['elife_no_ack'] += 1
            continue
        if doi in DOI:
            COUNT['elife_in_database'] += 1
            continue
        if doi in ACK_DOI_IGNORE:
            COUNT['elife_term_absent'] += 1
            continue
        try:
            edata = DL.get_doi_record(doi, source='elife')
        except Exception as err:
            LOGGER.debug(f"Error fetching {doi}: {err}")
            COUNT['elife_no_ack'] += 1
            continue
        result = None
        try:
            result = DL.get_acknowledgements(doi, elife_rec=edata)
        except Exception as err:
            LOGGER.warning(f"Error getting acknowledgements for {doi}: {err}")
            COUNT['elife_no_ack'] += 1
            continue
        if not result:
            COUNT['elife_no_ack'] += 1
            continue
        ack = result[0] if isinstance(result, tuple) else result
        _process_ack(doi, ack, 'elife', write_ignore=True)


def _process_elsevier_articles():
    ''' Page through Elsevier ScienceDirect results and process acknowledgements.
        Keyword arguments:
          None
        Returns:
          None
    '''
    LOGGER.info(f"Searching for {ARG.TERM} in Elsevier ScienceDirect")
    min_date = (datetime.now() - timedelta(days=ARG.DAYS)).strftime('%Y-%m-%d') \
               if ARG.DAYS else None
    if min_date:
        LOGGER.info(f"Post-filtering Elsevier results to records on or after {min_date}")
    first = True
    for result in _progress(search_sciencedirect(), "Scanning Elsevier articles", 'elsevier'):
        COUNT['elsevier_read'] += 1
        if first:
            LOGGER.debug(f"Elsevier result fields: {sorted(result.keys())}")
            first = False
        if min_date:
            rec_date = (result.get('publicationDate') or result.get('coverDate')
                        or result.get('loadDate') or min_date)
            if rec_date < min_date:
                COUNT['elsevier_date_filtered'] += 1
                continue
        doi = result.get('doi', '')
        if not doi:
            LOGGER.error(f"No DOI found for {result.get('title', '')}")
            continue
        doi = doi.lower()
        if doi in DOI:
            COUNT['elsevier_in_database'] += 1
            continue
        if doi in ACK_DOI_IGNORE:
            COUNT['elsevier_term_absent'] += 1
            continue
        ack_result = None
        try:
            ack_result = DL.get_acknowledgements(doi)
        except Exception as err:
            LOGGER.debug(f"Error getting acknowledgements for {doi}: {err}")
            COUNT['elsevier_no_ack'] += 1
            continue
        if not ack_result:
            COUNT['elsevier_no_ack'] += 1
            continue
        ack = ack_result[0] if isinstance(ack_result, tuple) else ack_result
        _process_ack(doi, ack, 'elsevier', write_ignore=True)


def _process_pmc_articles():
    ''' Search PubMed Central and process acknowledgements.
        Keyword arguments:
          None
        Returns:
          None
    '''
    term = f"{ARG.TERM}[Acknowledgements]"
    raw_articles = search_pmc(term, max_results=5000, api_key=os.environ["NCBI_API_KEY"],
                              days=ARG.DAYS)
    pbar = tqdm(raw_articles, desc="Processing PMC articles")
    for article in pbar:
        COUNT['pmc_read'] += 1
        pbar.set_postfix(written=COUNT['pmc_dois_written'],
                         in_db=COUNT['pmc_in_database'], refresh=False)
        parsed = _parse_pmc_article(article)
        if parsed is None:
            COUNT['pmc_no_ack'] += 1
            continue
        doi = parsed['doi']
        if doi == 'n/a':
            LOGGER.warning(f"No DOI found for {parsed['pmcid']}")
            COUNT['pmc_no_doi'] += 1
            continue
        if doi in DOI:
            COUNT['pmc_in_database'] += 1
            continue
        ack = parsed['ack']
        if not ack:
            COUNT['pmc_no_ack'] += 1
            continue
        if ARG.TERM.lower() not in ack.lower():
            COUNT['pmc_term_absent'] += 1
            continue
        fetch_and_store_doi(doi, ack, 'pmc', pmcid=parsed['pmcid'])


def _process_openalex_articles():
    ''' Discover arXiv preprints via OpenAlex full-text search and process their
        acknowledgements. arXiv DOIs are DataCite, so matches are stored via
        _store_arxiv_doi (not the CrossRef-based fetch_and_store_doi). DOIs already
        in the dois/external_dois collections - or written earlier this run by
        another source - are skipped via the DOI cache.
        Keyword arguments:
          None
        Returns:
          None
    '''
    LOGGER.info(f"Searching for {ARG.TERM} in arXiv full text via OpenAlex")
    for cand in _progress(search_openalex(), "Scanning arXiv articles", 'openalex'):
        COUNT['openalex_read'] += 1
        doi = cand['doi']
        if not doi:
            COUNT['openalex_no_ack'] += 1
            continue
        if doi in DOI:
            COUNT['openalex_in_database'] += 1
            continue
        if doi in ACK_DOI_IGNORE:
            COUNT['openalex_term_absent'] += 1
            continue
        ack_result = None
        try:
            ack_result = DL.get_acknowledgements(doi)
        except Exception as err:
            LOGGER.debug(f"Error getting acknowledgements for {doi}: {err}")
            COUNT['openalex_no_ack'] += 1
            continue
        ack = ack_result[0] if isinstance(ack_result, tuple) else ack_result
        if not ack:
            COUNT['openalex_no_ack'] += 1
            continue
        if ARG.TERM.lower() not in ack.lower():
            COUNT['openalex_term_absent'] += 1
            if ARG.WRITE:
                DB['dis']['to_ignore'].update_one(
                    {"type": "ack_doi", "key": doi},
                    {"$setOnInsert": {"type": "ack_doi", "key": doi,
                                      "reason": "Janelia not in ack text"}},
                    upsert=True)
                COUNT['openalex_ack_doi_ignored'] += 1
            continue
        # Guard: a DOI with (potential) Janelia authors belongs in the internal
        # dois collection, not external_dois. arXiv DOIs are DataCite, so fetch the
        # DataCite record (creators) for the author check. DOIs in to_ignore are
        # known non-Janelia papers, so the check (and the DataCite call) is skipped.
        if doi in IGNORE:
            COUNT['openalex_author_check_skipped'] += 1
        else:
            try:
                resp = JRC.call_datacite(doi)
                datacite_rec = resp['data']['attributes'] if resp and resp.get('data') else None
            except Exception as err:
                LOGGER.warning(f"DataCite lookup failed for {doi}: {err}")
                datacite_rec = None
            janelians = _janelia_authors(doi, datacite_rec)
            if janelians is None or janelians:
                _divert_janelia_authored(doi, ack, 'openalex', janelians,
                                         pub_date=cand['publication_date'])
                continue
        _store_arxiv_doi(doi, ack, cand['publication_date'])


def processing():
    ''' Processing
        Keyword arguments:
          None
        Returns:
          None
    '''
    sources = {'elife': _process_elife_articles,
               'elsevier': _process_elsevier_articles,
               'pmc': _process_pmc_articles,
               'arxiv': _process_openalex_articles}
    for source, handler in sources.items():
        if ARG.SOURCE in (None, source):
            handler()
    if RECORDS:
        jfile = 'external_acks.json'
        with open(jfile, 'w', encoding='utf-8') as jfp:
            json.dump(RECORDS, jfp, indent=2)
        LOGGER.info(f"Wrote {len(RECORDS):,} records to {jfile}")
    if INTERNAL_RECORDS:
        jfile = 'janelia_authored.json'
        with open(jfile, 'w', encoding='utf-8') as jfp:
            json.dump(INTERNAL_RECORDS, jfp, indent=2)
        LOGGER.info(f"Wrote {len(INTERNAL_RECORDS):,} (potentially) Janelia-authored "
                    f"(not stored) records to {jfile}")
    summary = "\n"
    if ARG.SOURCE in (None, 'elife'):
        summary += f"eLife records read:                {COUNT['elife_read']:,}\n"
        summary += f"eLife records in database:         {COUNT['elife_in_database']:,}\n"
        summary += f"eLife records no ack:              {COUNT['elife_no_ack']:,}\n"
        summary += f"eLife records term absent:         {COUNT['elife_term_absent']:,}\n"
        summary += f"eLife records not found:           {COUNT['elife_notfound']:,}\n"
        summary += f"eLife records Janelia-authored:    {COUNT['elife_janelia_authored']:,}\n"
        if COUNT['elife_author_check_failed']:
            summary += f"eLife records author check failed: {COUNT['elife_author_check_failed']:,}\n"
        if COUNT['elife_author_check_skipped']:
            summary += f"eLife records ignore-listed:  {COUNT['elife_author_check_skipped']:,}\n"
        summary += f"eLife records updated:             {COUNT['elife_dois_written']:,}\n"
    if ARG.SOURCE in (None, 'elsevier'):
        summary += f"Elsevier records read:             {COUNT['elsevier_read']:,}\n"
        summary += f"Elsevier records in database:      {COUNT['elsevier_in_database']:,}\n"
        if COUNT['elsevier_date_filtered']:
            summary += f"Elsevier records date filtered: {COUNT['elsevier_date_filtered']:,}\n"
        summary += f"Elsevier records no ack:           {COUNT['elsevier_no_ack']:,}\n"
        summary += f"Elsevier records term absent:      {COUNT['elsevier_term_absent']:,}\n"
        summary += f"Elsevier records not found:        {COUNT['elsevier_notfound']:,}\n"
        summary += f"Elsevier records Janelia-authored: {COUNT['elsevier_janelia_authored']:,}\n"
        if COUNT['elsevier_author_check_failed']:
            summary += f"Elsevier records author check failed:{COUNT['elsevier_author_check_failed']:,}\n"
        if COUNT['elsevier_author_check_skipped']:
            summary += f"Elsevier records ignore-listed:{COUNT['elsevier_author_check_skipped']:,}\n"
        summary += f"Elsevier records updated:          {COUNT['elsevier_dois_written']:,}\n"
    if ARG.SOURCE in (None, 'pmc'):
        summary += f"PMC records read:                  {COUNT['pmc_read']:,}\n"
        summary += f"PMC records in database:           {COUNT['pmc_in_database']:,}\n"
        summary += f"PMC records no DOI:                {COUNT['pmc_no_doi']:,}\n"
        summary += f"PMC records no ack:                {COUNT['pmc_no_ack']:,}\n"
        summary += f"PMC records term absent:           {COUNT['pmc_term_absent']:,}\n"
        summary += f"PMC records not found:             {COUNT['pmc_notfound']:,}\n"
        summary += f"PMC records Janelia-authored:      {COUNT['pmc_janelia_authored']:,}\n"
        if COUNT['pmc_author_check_failed']:
            summary += f"PMC records author check failed: {COUNT['pmc_author_check_failed']:,}\n"
        if COUNT['pmc_author_check_skipped']:
            summary += f"PMC records ignore-listed:    {COUNT['pmc_author_check_skipped']:,}\n"
        summary += f"PMC records updated:               {COUNT['pmc_dois_written']:,}\n"
    if ARG.SOURCE in (None, 'arxiv'):
        summary += f"arXiv records read:                {COUNT['openalex_read']:,}\n"
        summary += f"arXiv records in database:         {COUNT['openalex_in_database']:,}\n"
        summary += f"arXiv records no ack:              {COUNT['openalex_no_ack']:,}\n"
        summary += f"arXiv records term absent:         {COUNT['openalex_term_absent']:,}\n"
        summary += f"arXiv records Janelia-authored:    {COUNT['openalex_janelia_authored']:,}\n"
        if COUNT['openalex_author_check_failed']:
            summary += f"arXiv records author check failed: {COUNT['openalex_author_check_failed']:,}\n"
        if COUNT['openalex_author_check_skipped']:
            summary += f"arXiv records ignore-listed:  {COUNT['openalex_author_check_skipped']:,}\n"
        summary += f"arXiv records updated:             {COUNT['openalex_dois_written']:,}\n"
    summary += f"Janelia-authored (not stored):     {len(INTERNAL_RECORDS):,}\n"
    ack_doi_ignored = (COUNT['elife_ack_doi_ignored'] + COUNT['elsevier_ack_doi_ignored']
                       + COUNT['openalex_ack_doi_ignored'])
    if ack_doi_ignored:
        summary += f"DOIs added to ack ignore list:     {ack_doi_ignored:,}\n"
    print(summary)
    if ARG.TEST or ARG.WRITE:
        generate_email(summary, RECORDS)

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Find eLife, Elsevier, PMC, and arXiv DOIs that acknowledge Janelia")
    PARSER.add_argument('--term', dest='TERM', action='store',
                        default='Janelia', help='Search term [Janelia]')
    PARSER.add_argument('--days', dest='DAYS', action='store', type=int,
                        default=None,
                        help='Restrict results to last N days '
                             '(eLife/PMC: server-side; Elsevier: post-filter by publication date)')
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
    DISCONFIG = JRC.simplenamespace_to_dict(JRC.get_config("dis"))
    initialize_program()
    processing()
    terminate_program()
