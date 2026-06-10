''' sync_datacite_citations.py

    Enrich DataCite DOI citation data in the DIS "dois" MongoDB collection, because
    DataCite's native citationCount (from DataCite Event Data) severely underreports
    incoming citations for datasets, software, and preprints.

    SOURCES
        Incoming citing DOIs are gathered from the citing-DOI sources and deduped
        into a single true union (so a citing work found by more than one source
        counts once):
          - OpenAlex             (works?filter=cites:<id>; parallel, fast)
          - OpenAIRE ScholeXplorer (relation=References/Cites; parallel, fast)
          - DataCite GraphQL     (work.citations; optional via --datacite-graphql,
                                  slow ~0.8s/DOI - see NOTES)
        One further source contributes a bare citation count (no citing DOIs), used
        as a floor on the count rather than added to the union:
          - DataCite REST        (citationCount, already on the dois record)

    FIELDS WRITTEN  (with --write, only for DOIs with a non-zero citation count;
    uncited DOIs get no jrc_ fields, per the DB convention)
        jrc_citation_count    max(DataCite REST citationCount, size of citing-DOI
                              union). The bare count is kept as a floor because a
                              citing work may expose no DOI, so jrc_citing_dois can
                              be shorter than this count.
        jrc_citing_dois       sorted, lowercased union of identifiable citing DOIs
                              (only when --citing-dois is given; off by default).
        jrc_citation_sources  {datacite, openalex, scholexplorer} per-source counts
                              (datacite = GraphQL count, or the REST count when
                              GraphQL is disabled/errored; the REST datacite count is
                              a bare count, not DOIs).
        jrc_citation_updated  timestamp of this enrichment.

    FIGSHARE FIELDS WRITTEN  (with --write, for figshare DOIs only; these are
    independent of citations - a figshare DOI may have usage but no citations -
    and the figshare REST/stats API exposes no citation count of its own)
        jrc_figshare_counts   {views, downloads, shares} usage totals from the
                              figshare stats service (stats.figshare.com).
        jrc_figshare_updated  timestamp of the figshare usage fetch.

    OUTPUT FILES  (written to the current directory every run, dry-run included)
        citation_updates_<manifold>.txt   tab-delimited log of DOIs whose count rose.
        citation_records_<manifold>.json  full records (the jrc_ fields above) for
                                          every DOI that has a non-zero count.

    EMAIL
        An HTML summary is sent when --test (developer only) or --write (receivers)
        is given; sender/recipients come from the "dis" config.

    ENVIRONMENT
        OPENALEX_EMAIL contact address for the OpenAlex polite pool (required).
        MongoDB connection comes from the JRC "databases" config

    USAGE
        export OPENALEX_EMAIL=you@janelia.hhmi.org
        # fast nightly pass (OpenAlex + ScholeXplorer), persist + email receivers:
        python sync_datacite_citations.py --write
        # quick dry-run over the already-cited subset:
        python sync_datacite_citations.py --cited
        # periodic deep pass that also folds in DataCite GraphQL citing DOIs:
        python sync_datacite_citations.py --write --datacite-graphql
        # single DOI, verbose, no writes:
        python sync_datacite_citations.py --doi 10.xxxx/yyy --debug

    OPTIONS
        --doi DOI            process a single DOI (testing)
        --cited              only DOIs with citationCount > 0 (fast subset)
        --limit N            cap the number of DOIs processed (0 = no limit)
        --workers N          concurrent OpenAlex/ScholeXplorer workers (default 8)
        --datacite-graphql   include DataCite GraphQL citing DOIs (off by default)
        --citing-dois        store the jrc_citing_dois list (off by default)
        --write              persist to MongoDB (default is a dry run)
        --test               send the summary email to the developer only
        --manifold dev|prod  MongoDB manifold (default prod)
        --verbose / --debug  logging verbosity

    NOTES
        DataCite GraphQL 429s and cooldown-bans on concurrent/bursty access, so its
        requests are serialized and aliased into batches (DATACITE_BATCH). It is the
        slow lane (~0.8s/DOI server-side, more on high-latency links), so reserve
        --datacite-graphql for periodic deep passes rather than the nightly run; the
        default run uses only the fast, parallel OpenAlex and ScholeXplorer sources.

        Intended to be run nightly. The UI endpoints can read jrc_citation_count
        (falling back to the native citationCount).
'''

__version__ = '1.0.0'

import argparse
import collections
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from operator import attrgetter
import os
import re
import sys
import threading
from time import monotonic, sleep
import traceback
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError
import requests
from tqdm import tqdm
import jrc_common.jrc_common as JRC

# pylint: disable=broad-exception-caught,broad-exception-raised,logging-fstring-interpolation

# Database
DB = {}
# Counters
COUNT = collections.defaultdict(lambda: 0, {})
# Citation increases recorded this run, for the summary email
HITS = []
# Per-DOI enrichment records (doi + jrc_ fields) written to a JSON output file
RECORDS = []
# Buffered DB writes: finalize_doi queues an UpdateOne per DOI, flushed in batches
# via bulk_write to cut per-DOI round-trips (one bulk_write per WRITE_BATCH instead
# of one update_one each). Both are touched only from the main thread.
WRITE_BUFFER = []
WRITE_BATCH = 500
# Figshare usage is keyed on the article ID, so version DOIs (.v1/.v2/...) share a
# result; cache successful fetches by (article_id, scope) to avoid refetching the
# same stats across versions. Populated from the worker threads, hence the lock.
FIGSHARE_CACHE = {}
FIGSHARE_CACHE_LOCK = threading.Lock()
# Global variables
ARG = DISCONFIG = LOGGER = None
# External sources
OPENALEX_WORK = 'https://api.openalex.org/works/doi:'
OPENALEX_WORKS = 'https://api.openalex.org/works'
SCHOLIX_LINKS = 'https://api.scholexplorer.openaire.eu/v3/Links'
DATACITE_GRAPHQL = 'https://api.datacite.org/graphql'
# Figshare usage metrics: the stats service reports aggregate views/downloads/
# shares per article (one request per counter, no auth needed for public totals).
# A figshare DOI embeds its numeric article ID, so no lookup is needed. The
# Janelia institutional portal (10.25378/janelia.<id>) is scoped under /janelia
# in the stats service and returns 0 from the unscoped endpoint; generic figshare
# DOIs (10.6084/m9.figshare.<id>) use the unscoped endpoint. Each DOI pattern
# maps to its stats-service scope path segment ('' = global/unscoped). The
# trailing .v<n> version suffix is not part of the article ID.
FIGSHARE_STATS = 'https://stats.figshare.com'
FIGSHARE_COUNTERS = ('views', 'downloads', 'shares')
FIGSHARE_DOI_PATTERNS = (
    (re.compile(r'10\.25378/janelia\.(\d+)', re.IGNORECASE), 'janelia'),
    (re.compile(r'10\.6084/m9\.figshare\.(\d+)', re.IGNORECASE), ''),
)
DATACITE_QUERY = '''query($id: ID!, $after: String) {
  work(id: $id) {
    citations(first: 100, after: $after) {
      pageInfo { endCursor hasNextPage }
      nodes { doi }
    }
  }
}'''
# Scholix relationships where the source work cites our DOI (incoming citation)
CITING_RELS = {'References', 'Cites'}
# DataCite GraphQL 429s and cooldown-bans on burst/concurrent access, but is fine
# sequentially. Serialize its requests (one in flight) spaced by this interval.
DATACITE_LOCK = threading.Lock()
DATACITE_MIN_INTERVAL = 0.2
DATACITE_LAST = [0.0]
# DataCite citing DOIs are pre-fetched in aliased batches (one request per N DOIs)
# to keep request volume - and thus rate-limit exposure - tiny. Per-DOI cost is
# ~0.8s server-side, so a batch of N takes ~N*0.8s; keep N small enough that one
# request finishes well within DATACITE_TIMEOUT (a 50-alias query exceeded a 30s
# timeout and every batch failed).
DATACITE_BATCH = 25
# Generous timeout: a full batch query is legitimately slow (~N*0.8s server-side,
# more on high-latency links), so allow ample time rather than timing out + retrying.
DATACITE_TIMEOUT = 120
DATACITE_CITING = {}    # doi -> set of citing DOIs (or None on error), filled up front
# Per-thread HTTP session (a requests Session is safest used one-per-thread)
THREAD = threading.local()


def http_session():
    ''' Return this thread's requests Session, creating it on first use
        Returns:
          requests.Session for connection reuse
    '''
    session = getattr(THREAD, 'session', None)
    if session is None:
        session = requests.Session()
        THREAD.session = session
    return session


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
    ''' Initialize the program
        Keyword arguments:
          None
        Returns:
          None
    '''
    if "OPENALEX_EMAIL" not in os.environ:
        terminate_program("Missing contact email - set in OPENALEX_EMAIL environment variable")
    try:
        dbconfig = JRC.get_config("databases")
    except Exception as err:
        terminate_program(err)
    for source in ['dis']:
        dbo = attrgetter(f"{source}.{ARG.MANIFOLD}.write")(dbconfig)
        LOGGER.info(f"Connecting to {dbo.name} {ARG.MANIFOLD} on {dbo.host} as {dbo.user}")
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)


def get_with_retry(url, params, timeout, tries=3, json_body=None):
    ''' HTTP request with retry/backoff on transient failures (429, 5xx, exceptions)
        Keyword arguments:
          url: URL
          params: query parameters
          timeout: per-request timeout in seconds
          tries: maximum attempts
          json_body: if given, POST this JSON body instead of issuing a GET
        Returns:
          requests Response (possibly a non-2xx), or None if every attempt raised
    '''
    resp = None
    for attempt in range(1, tries + 1):
        try:
            if json_body is not None:
                resp = http_session().post(url, params=params, json=json_body,
                                           timeout=timeout)
            else:
                resp = http_session().get(url, params=params, timeout=timeout)
        except Exception:
            resp = None
        if resp is not None and resp.status_code not in (429, 500, 502, 503, 504):
            return resp
        if attempt < tries:
            wait = 2 ** attempt
            if resp is not None:
                try:
                    wait = int(resp.headers.get('Retry-After', wait))
                except (ValueError, TypeError):
                    pass
            sleep(wait)
    return resp


def normalize_doi(value):
    ''' Normalize a DOI to a bare lowercase form for cross-source deduping
        Keyword arguments:
          value: DOI string or URL (or None)
        Returns:
          bare lowercase DOI, or None
    '''
    if not value:
        return None
    doi = value.strip().lower()
    for prefix in ('https://doi.org/', 'http://doi.org/',
                   'https://dx.doi.org/', 'http://dx.doi.org/'):
        if doi.startswith(prefix):
            doi = doi[len(prefix):]
            break
    return doi or None


def openalex_citing(doi):  # pylint: disable=too-many-return-statements
    ''' Get the set of DOIs citing this DOI from OpenAlex
        Keyword arguments:
          doi: DOI string
        Returns:
          set of citing DOIs (empty if the work is unknown or uncited), or None on error
    '''
    resp = get_with_retry(f"{OPENALEX_WORK}{doi.lower()}",
                          {'mailto': os.environ['OPENALEX_EMAIL'],
                           'select': 'id,cited_by_count'}, 20)
    if resp is not None and resp.status_code == 404:
        return set()
    if resp is None or resp.status_code != 200:
        if ARG.DEBUG:
            tqdm.write(f"OpenAlex error for {doi}: "
                       + (f"HTTP {resp.status_code}" if resp is not None else "no response"))
        return None
    try:
        data = resp.json()
    except Exception:
        return None
    if not data.get('cited_by_count'):
        return set()
    wid = (data.get('id') or '').rsplit('/', 1)[-1]
    if not wid:
        return set()
    citing = set()
    cursor = '*'
    while cursor:
        resp = get_with_retry(OPENALEX_WORKS,
                              {'mailto': os.environ['OPENALEX_EMAIL'],
                               'filter': f"cites:{wid}", 'select': 'doi',
                               'per-page': 200, 'cursor': cursor}, 30)
        if resp is None or resp.status_code != 200:
            return None
        try:
            page = resp.json()
        except Exception:
            return None
        results = page.get('results', [])
        for item in results:
            norm = normalize_doi(item.get('doi'))
            if norm:
                citing.add(norm)
        cursor = (page.get('meta') or {}).get('next_cursor')
        if not results:
            break
        sleep(0.1)
    return citing


def scholix_citing(doi):
    ''' Get the set of DOIs citing this DOI from OpenAIRE ScholeXplorer.
        Queries only the citing relationships (server-side filter) - far fewer
        requests than paging through every link type (e.g. dataset version
        links). Picks the shortest doi-scheme identifier per citing work as its
        canonical DOI (collapses within-work version DOIs); citing works that
        expose no DOI are omitted.
        Keyword arguments:
          doi: DOI string
        Returns:
          set of citing DOIs, or None if a call failed
    '''
    citing = set()
    for rel in CITING_RELS:
        page = 0
        pages = 1
        while page < pages:
            resp = get_with_retry(SCHOLIX_LINKS,
                                  {'targetPid': doi, 'relation': rel, 'page': page}, 30)
            if resp is None or resp.status_code != 200:
                if ARG.DEBUG:
                    tqdm.write(f"ScholeXplorer error for {doi}: "
                               + (f"HTTP {resp.status_code}" if resp is not None
                                  else "no response"))
                return None
            try:
                data = resp.json()
            except Exception:
                return None
            pages = data.get('totalPages', 1) or 1
            for link in data.get('result', []):
                src = link.get('source') or {}
                dois = [i['ID'] for i in (src.get('Identifier') or [])
                        if i.get('IDScheme') == 'doi' and i.get('ID')]
                norm = normalize_doi(min(dois, key=len)) if dois else None
                if norm:
                    citing.add(norm)
            page += 1
            if pages > 1:
                sleep(0.2)
    return citing


def figshare_article(doi):
    ''' Identify a figshare DOI and its stats-service scope.
        Figshare DOIs embed the numeric article ID used by the stats service; the
        Janelia institutional portal (10.25378/janelia.<id>) is scoped under
        /janelia, while generic figshare DOIs (10.6084/m9.figshare.<id>) use the
        unscoped endpoint. The trailing .v<n> version suffix is not part of the ID.
        Keyword arguments:
          doi: DOI string
        Returns:
          (article_id, scope) for a figshare DOI - scope is '' for the global
          endpoint - or (None, None) if this is not a figshare DOI
    '''
    if doi:
        for pattern, scope in FIGSHARE_DOI_PATTERNS:
            match = pattern.search(doi)
            if match:
                return match.group(1), scope
    return None, None


def figshare_metrics(doi):
    ''' Get figshare usage metrics (views, downloads, shares) for a figshare DOI.
        Figshare's public API exposes no citation count, but its stats service
        reports aggregate usage per article (no auth needed for public totals).
        Keyword arguments:
          doi: DOI string
        Returns:
          {'views': int, 'downloads': int, 'shares': int} for a figshare DOI, or
          None if this is not a figshare DOI or any stats request failed (partial
          results are discarded so a stored count is never silently incomplete)
    '''
    fid, scope = figshare_article(doi)
    if not fid:
        return None
    # Version DOIs (.v1/.v2/...) resolve to the same article ID, so return a cached
    # per-article result instead of refetching identical stats. Only successes are
    # cached; a failure stays uncached so it is retried (a miss is simply picked up
    # later) rather than poisoning every version of this article for the whole run.
    key = (fid, scope)
    with FIGSHARE_CACHE_LOCK:
        cached = FIGSHARE_CACHE.get(key)
    if cached is not None:
        return cached
    base = f"{FIGSHARE_STATS}/{scope}/total" if scope else f"{FIGSHARE_STATS}/total"
    counts = {}
    for counter in FIGSHARE_COUNTERS:
        resp = get_with_retry(f"{base}/{counter}/article/{fid}", None, 20)
        if resp is None or resp.status_code != 200:
            if ARG.DEBUG:
                tqdm.write(f"figshare stats error for {doi} ({counter}): "
                           + (f"HTTP {resp.status_code}" if resp is not None
                              else "no response"))
            return None
        try:
            counts[counter] = int((resp.json() or {}).get('totals', 0) or 0)
        except Exception:
            return None
    with FIGSHARE_CACHE_LOCK:
        FIGSHARE_CACHE[key] = counts
    return counts


def datacite_graphql_post(json_body):
    ''' Issue a DataCite GraphQL POST, serialized and rate-limited across threads.
        DataCite GraphQL bans bursty/concurrent access, so only one request runs
        at a time, spaced by DATACITE_MIN_INTERVAL.
        Keyword arguments:
          json_body: GraphQL request body
        Returns:
          requests Response, or None if every attempt raised
    '''
    with DATACITE_LOCK:
        wait = DATACITE_MIN_INTERVAL - (monotonic() - DATACITE_LAST[0])
        if wait > 0:
            sleep(wait)
        resp = get_with_retry(DATACITE_GRAPHQL, None, DATACITE_TIMEOUT, json_body=json_body)
        DATACITE_LAST[0] = monotonic()
        return resp


def datacite_citing(doi):
    ''' Get the set of DOIs citing this DOI from the DataCite GraphQL API.
        Unlike the REST citationCount (a bare number), GraphQL exposes the
        citing works' DOIs.
        Keyword arguments:
          doi: DOI string
        Returns:
          set of citing DOIs, or None on error
    '''
    citing = set()
    cursor = None
    page = 0
    while page < 100:    # safety cap (100 x 100 = 10,000 citations) vs a stuck cursor
        page += 1
        variables = {'id': doi}
        if cursor:
            variables['after'] = cursor
        resp = datacite_graphql_post({'query': DATACITE_QUERY, 'variables': variables})
        if resp is None or resp.status_code != 200:
            if ARG.DEBUG:
                tqdm.write(f"DataCite GraphQL error for {doi}: "
                           + (f"HTTP {resp.status_code}" if resp is not None
                              else "no response"))
            return None
        try:
            data = resp.json()
        except Exception:
            return None
        if data.get('errors'):
            if ARG.DEBUG:
                tqdm.write(f"DataCite GraphQL error for {doi}: {data['errors']}")
            return None
        conn = ((data.get('data') or {}).get('work') or {}).get('citations') or {}
        for node in conn.get('nodes', []):
            norm = normalize_doi(node.get('doi'))
            if norm:
                citing.add(norm)
        info = conn.get('pageInfo') or {}
        if info.get('hasNextPage') and info.get('endCursor'):
            cursor = info['endCursor']
            sleep(0.1)
        else:
            break
    return citing


def datacite_citing_batch(dois):  # pylint: disable=too-many-locals
    ''' Fetch citing DOIs for a batch of DOIs in one aliased GraphQL request.
        A work whose citations exceed the first page is completed via individual
        pagination (datacite_citing).
        Keyword arguments:
          dois: list of DOI strings
        Returns:
          {doi: set of citing DOIs, or None on error}
    '''
    decl = ', '.join(f"$id{i}: ID!" for i in range(len(dois)))
    aliases = ' '.join(
        f"w{i}: work(id: $id{i}) {{ citations(first: 100) "
        f"{{ pageInfo {{ hasNextPage }} nodes {{ doi }} }} }}"
        for i in range(len(dois)))
    query = f"query({decl}) {{ {aliases} }}"
    variables = {f"id{i}": doi for i, doi in enumerate(dois)}
    resp = datacite_graphql_post({'query': query, 'variables': variables})
    if resp is None or resp.status_code != 200:
        if ARG.DEBUG:
            tqdm.write("DataCite GraphQL batch error: "
                       + (f"HTTP {resp.status_code}" if resp is not None else "no response"))
        return {doi: None for doi in dois}
    try:
        data = resp.json()
    except Exception:
        return {doi: None for doi in dois}
    payload = data.get('data') or {}
    if not payload and data.get('errors'):
        if ARG.DEBUG:
            tqdm.write(f"DataCite GraphQL batch error: {data['errors']}")
        return {doi: None for doi in dois}
    result = {}
    for i, doi in enumerate(dois):
        work = payload.get(f"w{i}")
        if work is None:
            result[doi] = None
            continue
        conn = work.get('citations') or {}
        citing = set()
        for node in conn.get('nodes', []):
            norm = normalize_doi(node.get('doi'))
            if norm:
                citing.add(norm)
        if (conn.get('pageInfo') or {}).get('hasNextPage'):
            full = datacite_citing(doi)    # rare: complete the remaining pages
            result[doi] = full if full is not None else citing
        else:
            result[doi] = citing
    return result


def prefetch_datacite(dois):
    ''' Pre-fetch DataCite GraphQL citing DOIs for all DOIs in aliased batches.
        Keyword arguments:
          dois: list of DOI strings
        Returns:
          {doi: set of citing DOIs, or None on error}
    '''
    result = {}
    for start in tqdm(range(0, len(dois), DATACITE_BATCH), desc='DataCite'):
        result.update(datacite_citing_batch(dois[start:start + DATACITE_BATCH]))
    return result


def fetch_doi(row):
    ''' Fetch external citation counts for one DOI (network only - thread-safe,
        no shared state). Run concurrently across DOIs by a thread pool.
        Keyword arguments:
          row: dois document (needs doi, citationCount, optionally jrc_citation_count)
        Returns:
          result dict for finalize_doi()
    '''
    doi = row['doi']
    fid = figshare_article(doi)[0]
    return {'row': row,
            'doi': doi,
            'datacite': row.get('citationCount', 0) or 0,
            'existing': row.get('jrc_citation_count'),
            'existing_dois': row.get('jrc_citing_dois') or [],
            'openalex': openalex_citing(doi),
            'scholex': scholix_citing(doi),
            'datacite_dois': DATACITE_CITING.get(doi),
            'is_figshare': bool(fid),
            'figshare': figshare_metrics(doi) if fid else None}


def combine_counts(res, graphql):
    ''' Pure citation-combination logic (no I/O, no shared state) - unit-testable.
        Builds the deduped union of identifiable citing DOIs across the available
        sources, applies the DataCite REST count as a bare floor, and protects a
        previously-stored higher count from a transient source error.
        Keyword arguments:
          res: result dict from fetch_doi()
          graphql: whether DataCite GraphQL citing DOIs are in play (ARG.GRAPHQL)
        Returns:
          dict with:
            combined   final citation count (max of the floor and the union size)
            previous   count stored on the prior run (0 if none)
            citing     sorted list of unique citing DOIs
            sources    {datacite, openalex, scholexplorer} per-source counts
            errored    True if any enabled citing-DOI source failed
            preserved  True if a stored higher count was kept despite an error
            oa_n/sx_n/dc_n  per-source counts (None on error/disabled), for reporting
    '''
    datacite = res['datacite']
    existing = res['existing']
    # Baseline for "increased": the count stored on the previous run (0 if none).
    previous = existing if isinstance(existing, int) else 0
    openalex = res['openalex']        # set of citing DOIs, or None on error
    scholex = res['scholex']
    oa_n = None if openalex is None else len(openalex)
    sx_n = None if scholex is None else len(scholex)
    # True union of identifiable citing DOIs across the available sources
    union = set()
    union.update(openalex or set())
    union.update(scholex or set())
    errored = openalex is None or scholex is None
    # DataCite GraphQL (citing DOIs) only when enabled - it is slow, so optional
    dc_n = None
    if graphql:
        datacite_dois = res['datacite_dois']
        dc_n = None if datacite_dois is None else len(datacite_dois)
        union.update(datacite_dois or set())
        errored = errored or datacite_dois is None
    # On a source error, don't lose citing DOIs we already had stored
    if errored:
        union.update(filter(None, (normalize_doi(cd) for cd in res['existing_dois'])))
    citing = sorted(union)
    # DataCite contributes a count but no DOIs, so keep it as a floor
    combined = max(datacite, len(citing))
    # Don't let a transient source error regress a previously-stored higher count
    preserved = False
    if errored and isinstance(existing, int) and existing > combined:
        combined = existing
        preserved = True
    # Per-source counts; datacite is a bare count, openalex/scholexplorer are
    # identifiable citing-DOI counts (datacite is the GraphQL citing-DOI count,
    # falling back to the REST citationCount if GraphQL errored)
    sources = {'datacite': datacite if dc_n is None else dc_n,
               'openalex': oa_n, 'scholexplorer': sx_n}
    return {'combined': combined, 'previous': previous, 'citing': citing,
            'sources': sources, 'errored': errored, 'preserved': preserved,
            'oa_n': oa_n, 'sx_n': sx_n, 'dc_n': dc_n}


def queue_write(doc_id, fields):
    ''' Queue a $set update for batched bulk_write, flushing when the buffer fills.
        Main-thread only (called from finalize_doi), so no lock is needed.
        Keyword arguments:
          doc_id: _id of the dois document
          fields: jrc_ fields to $set
        Returns:
          None
    '''
    WRITE_BUFFER.append(UpdateOne({'_id': doc_id}, {'$set': fields}))
    if len(WRITE_BUFFER) >= WRITE_BATCH:
        flush_writes()


def flush_writes():
    ''' Flush buffered writes to MongoDB in one unordered bulk_write.
        A bulk failure shouldn't abort an unattended nightly run, so partial
        results are still counted from the error details.
        Keyword arguments:
          None
        Returns:
          None
    '''
    if not WRITE_BUFFER:
        return
    ops = WRITE_BUFFER[:]
    WRITE_BUFFER.clear()
    try:
        result = DB['dis'].dois.bulk_write(ops, ordered=False)
        COUNT['matched'] += result.matched_count
        COUNT['written'] += result.modified_count
    except BulkWriteError as bwe:
        # Some ops may have applied; count what the details report, the rest as errors
        det = bwe.details or {}
        COUNT['matched'] += det.get('nMatched', 0)
        COUNT['written'] += det.get('nModified', 0)
        failed = len(det.get('writeErrors', [])) or len(ops)
        COUNT['write_error'] += failed
        if ARG.DEBUG:
            tqdm.write(f"Bulk write error: {failed} of {len(ops)} ops failed")
    except Exception as err:
        COUNT['write_error'] += len(ops)
        if ARG.DEBUG:
            tqdm.write(f"Bulk write error ({len(ops)} ops): {err}")


def finalize_doi(res, monitor=None):
    ''' Record and (optionally) queue the write for one DOI's enrichment. Runs in
        the main thread, so COUNT updates and the write buffer are serialized; the
        pure count-combination logic lives in combine_counts().
        Keyword arguments:
          res: result dict from fetch_doi()
          monitor: open file handle to record citation increases to
        Returns:
          None
    '''
    doi = res['doi']
    datacite = res['datacite']
    # Per-source lookup outcome counters (a raw None result means that source errored)
    COUNT['openalex_found' if res['openalex'] is not None else 'openalex_error'] += 1
    COUNT['scholex_found' if res['scholex'] is not None else 'scholex_error'] += 1
    if ARG.GRAPHQL:
        COUNT['datacite_found' if res['datacite_dois'] is not None
              else 'datacite_error'] += 1
    cc = combine_counts(res, ARG.GRAPHQL)
    combined, previous = cc['combined'], cc['previous']
    oa_n, sx_n, dc_n = cc['oa_n'], cc['sx_n'], cc['dc_n']
    if cc['preserved']:
        COUNT['preserved'] += 1
    # Figshare usage metrics (views/downloads/shares) are independent of citations:
    # a figshare DOI may have usage but no citations, and is still recorded/stored.
    figshare_fields = {}
    if res['is_figshare']:
        if res['figshare'] is None:
            COUNT['figshare_error'] += 1
        else:
            COUNT['figshare_found'] += 1
            figshare_fields['jrc_figshare_counts'] = res['figshare']
            figshare_fields['jrc_figshare_updated'] = datetime.now()
    # Citation jrc_ fields exist only when there is usable data, so uncited DOIs
    # get no citation fields (but a figshare DOI may still get figshare fields).
    fields = {}
    if combined:
        fields['jrc_citation_count'] = combined
        if ARG.CITING_DOIS:    # the citing-DOI list itself is optional
            fields['jrc_citing_dois'] = cc['citing']
        fields['jrc_citation_sources'] = cc['sources']
        fields['jrc_citation_updated'] = datetime.now()
        if combined > previous:
            COUNT['increased'] += 1
            HITS.append((doi, previous, datacite, combined, oa_n, sx_n, dc_n))
            dc_disp = dc_n if ARG.GRAPHQL else 'off'
            hit = (f"{doi}\t{previous} -> {combined} "
                   + f"(DataCite={datacite}, OpenAlex={oa_n}, ScholeXplorer={sx_n}, "
                   + f"DataCite-GraphQL={dc_disp})")
            if monitor:
                monitor.write(hit + "\n")
                monitor.flush()
            if ARG.DEBUG:
                tqdm.write(hit)
    # Persist citation and figshare fields together; nothing to do if both empty
    fields.update(figshare_fields)
    if not fields:
        return
    RECORDS.append({'doi': doi, **fields})
    if ARG.WRITE:
        queue_write(res['row']['_id'], fields)


def doiurl(doi):
    ''' Format a DOI as a DIS UI link
        Keyword arguments:
          doi: DOI to format
        Returns:
          HTML anchor
    '''
    return f"<a href='https://dis.int.janelia.org/doiui/{doi}'>{doi}</a>"


def text_to_html_table(text):
    ''' Convert a "label: value" summary block to an HTML table
        Keyword arguments:
          text: summary text
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


def generate_email(summary):
    ''' Generate and send the run summary email
        Keyword arguments:
          summary: run summary text
        Returns:
          None
    '''
    msg = JRC.get_run_data(__file__, __version__) + "<br><br>" \
        + text_to_html_table(summary) + "<br>"
    if HITS:
        msg += f"The following {len(HITS):,} DataCite DOI(s) had their citation " \
               + "count increased:<br>"
        for doi, previous, datacite, combined, openalex, scholex, dcite in HITS:
            oas = 'err' if openalex is None else openalex
            sxs = 'err' if scholex is None else scholex
            dcs = ('err' if dcite is None else dcite) if ARG.GRAPHQL else 'off'
            msg += f"&nbsp;&nbsp;{doiurl(doi)}: {previous} &rarr; {combined} " \
                   + f"(DataCite {datacite}, OpenAlex {oas}, ScholeXplorer {sxs}, " \
                   + f"DataCite-GraphQL {dcs})<br>"
    else:
        msg += "No citation counts were increased.<br>"
    try:
        email = DISCONFIG['developer'] if ARG.TEST else DISCONFIG['dcreceivers']
        LOGGER.info(f"Sending email to {email}")
        JRC.send_email(msg, DISCONFIG['sender'], email,
                       "DataCite citation enrichment", mime='html')
    except Exception as err:
        print(str(err))
        traceback.print_exc()
        terminate_program(err)


def processing():  # pylint: disable=too-many-locals,too-many-statements
    ''' Enrich citation counts for DataCite DOIs
        Keyword arguments:
          None
        Returns:
          None
    '''
    query = {'jrc_obtained_from': 'DataCite'}
    if ARG.DOI:
        query['doi'] = ARG.DOI.lower()
    elif ARG.CITED:
        # AND-ed with the DataCite constraint above (don't reassign query, or the
        # jrc_obtained_from filter is lost and non-DataCite DOIs could be included)
        #query['$or'] = [{"citationCount": {'$gt': 0}},
        #                {"jrc_figshare_counts.shares": {"$gt": 0}},
        #                {"jrc_figshare_counts.downloads": {"$gt": 0}}]
        query['citationCount'] = {'$gt': 0}
    try:
        cursor = DB['dis'].dois.find(query, {'doi': 1, 'citationCount': 1,
                                             'jrc_citation_count': 1,
                                             'jrc_citing_dois': 1})
        if ARG.LIMIT:
            cursor = cursor.limit(ARG.LIMIT)
        rows = list(cursor)
    except Exception as err:
        terminate_program(err)
    total = len(rows)
    LOGGER.info(f"Processing {total:,} DataCite DOIs "
                f"({'WRITE' if ARG.WRITE else 'DRY RUN'})")
    interrupted = False
    # Pre-fetch DataCite GraphQL citing DOIs in batches (serialized, low volume)
    # so the per-DOI thread pool only needs the fast, parallel-safe sources.
    # Optional: it adds ~0.8s/DOI (~80 min for a full run), so it is off by default.
    if ARG.GRAPHQL:
        LOGGER.info("Pre-fetching DataCite GraphQL citations")
        try:
            DATACITE_CITING.update(prefetch_datacite([row['doi'] for row in rows]))
        except KeyboardInterrupt:
            interrupted = True
            LOGGER.warning("Interrupted during DataCite prefetch - skipping enrichment")
    monfile = f"citation_updates_{ARG.MANIFOLD}.txt"
    with open(monfile, 'w', encoding='utf-8') as monitor, \
         ThreadPoolExecutor(max_workers=ARG.WORKERS) as pool:
        monitor.write(f"# Citation increases - {datetime.now()} "
                      + f"({'WRITE' if ARG.WRITE else 'DRY RUN'})\n")
        monitor.flush()
        # Network fetches run concurrently; finalize_doi (counts + DB writes) runs
        # here in the main thread as each result completes.
        futures = [] if interrupted else [pool.submit(fetch_doi, row) for row in rows]
        try:
            for future in tqdm(as_completed(futures), total=len(futures), desc='DOIs'):
                COUNT['read'] += 1
                try:
                    res = future.result()
                except Exception as err:
                    # One bad DOI shouldn't abort the batch
                    COUNT['fetch_error'] += 1
                    if ARG.DEBUG:
                        tqdm.write(f"Fetch error: {err}")
                    continue
                finalize_doi(res, monitor)
        except KeyboardInterrupt:
            interrupted = True
            LOGGER.warning("Interrupted - cancelling remaining work")
            pool.shutdown(wait=False, cancel_futures=True)
        finally:
            # Flush whatever remains buffered (the final partial batch, or all work
            # queued before an interrupt) so no completed write is silently dropped.
            flush_writes()
    LOGGER.info(f"Citation increases written to {monfile}")
    recfile = f"citation_records_{ARG.MANIFOLD}.json"
    with open(recfile, 'w', encoding='utf-8') as fileout:
        json.dump(RECORDS, fileout, indent=2, default=str)
    LOGGER.info(f"Wrote {len(RECORDS):,} citation records to {recfile}")
    # RECORDS is the union of DOIs that got citation fields and/or figshare fields
    cited = sum(1 for r in RECORDS if 'jrc_citation_count' in r)
    figusage = sum(1 for r in RECORDS if 'jrc_figshare_counts' in r)
    dc_line = (f"DataCite GraphQL OK (errors):       {COUNT['datacite_found']:,} "
               f"({COUNT['datacite_error']:,})\n") if ARG.GRAPHQL else \
              "DataCite GraphQL:                   disabled (use --datacite-graphql)\n"
    summary = (
        f"DataCite DOIs processed:            {COUNT['read']:,}\n"
        + f"OpenAlex lookups OK (errors):       {COUNT['openalex_found']:,} "
        + f"({COUNT['openalex_error']:,})\n"
        + f"ScholeXplorer lookups OK (errors):  {COUNT['scholex_found']:,} "
        + f"({COUNT['scholex_error']:,})\n"
        + dc_line
        + f"Fetch errors (DOIs skipped):        {COUNT['fetch_error']:,}\n"
        + f"Counts preserved (source error):    {COUNT['preserved']:,}\n"
        + f"Figshare usage OK (errors):         {COUNT['figshare_found']:,} "
        + f"({COUNT['figshare_error']:,})\n"
        + f"DOIs with citation fields:          {cited:,}\n"
        + f"DOIs with figshare fields:          {figusage:,}\n"
        + f"Total records written (set):        {len(RECORDS):,}\n"
        + f"DOIs with increased citation count: {COUNT['increased']:,}\n"
        + f"DOIs matched:                       {COUNT['matched']:,}\n"
        + f"DOIs updated (errors):              {COUNT['written']:,} "
        + f"({COUNT['write_error']:,})")
    print(summary)
    if interrupted:
        LOGGER.warning("Run interrupted - summary email not sent")
    elif ARG.TEST or ARG.WRITE:
        generate_email(summary)

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Enrich DataCite DOI citation counts from OpenAlex and ScholeXplorer")
    PARSER.add_argument('--doi', dest='DOI', action='store', default=None,
                        help='Process a single DOI (for testing)')
    PARSER.add_argument('--cited', dest='CITED', action='store_true', default=False,
                        help='Only process DOIs that already have a citationCount > 0')
    PARSER.add_argument('--limit', dest='LIMIT', action='store', type=int, default=0,
                        help='Limit number of DOIs processed (0 = no limit)')
    PARSER.add_argument('--workers', dest='WORKERS', action='store', type=int, default=8,
                        help='Concurrent DOI workers (default 8; keep modest to '
                             + 'stay within OpenAlex/ScholeXplorer rate limits)')
    PARSER.add_argument('--datacite-graphql', dest='GRAPHQL', action='store_true',
                        default=False,
                        help='Include DataCite GraphQL citing DOIs (slow, ~0.8s/DOI; '
                             + 'off by default - use for periodic deep passes)')
    PARSER.add_argument('--citing-dois', dest='CITING_DOIS', action='store_true',
                        default=False,
                        help='Store/record the jrc_citing_dois list itself (off by '
                             + 'default - only the count and per-source breakdown)')
    PARSER.add_argument('--write', dest='WRITE', action='store_true', default=False,
                        help='Persist results (default is a dry run)')
    PARSER.add_argument('--test', dest='TEST', action='store_true', default=False,
                        help='Send the summary email to the developer only')
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
    DISCONFIG = JRC.simplenamespace_to_dict(JRC.get_config("dis"))
    processing()
    terminate_program()
