''' sync_citations.py

    Enrich DOI citation data in the DIS "dois" MongoDB collection for DataCite or
    Crossref DOIs (--source, default crossref). DataCite's native citationCount
    (from DataCite Event Data) severely underreports incoming citations for
    datasets, software, and preprints; the same union-of-sources enrichment is
    available for Crossref DOIs.

    SOURCES
        Incoming citing DOIs are gathered from the citing-DOI sources and deduped
        into a single true union (so a citing work found by more than one source
        counts once):
          - OpenAlex             (works?filter=cites:<id>; parallel, fast)
          - OpenAIRE ScholeXplorer (relation=References/Cites; parallel, fast)
          - DataCite GraphQL     (work.citations; optional via --datacite-graphql,
                                  slow ~0.8s/DOI - see NOTES; --source datacite only)
        Further sources contribute a bare citation count (no citing DOIs), used
        as a floor on the count rather than added to the union:
          - DataCite REST        (citationCount, already on the dois record)
          - Crossref             (is-referenced-by-count, already on the dois record;
                                  used when --source crossref)
          - Web of Science       (Starter API citation count; --source crossref only,
                                  since WoS does not index DataCite/Zenodo deposits;
                                  opt-in via --wos; requires WOS_API_KEY)

    FIELDS WRITTEN  (with --write, only for DOIs with a non-zero citation count;
    uncited DOIs get no jrc_ fields, per the DB convention)
        jrc_citation_count    max(native registrar count, size of citing-DOI
                              union). The bare count is kept as a floor because a
                              citing work may expose no DOI, so jrc_citing_dois can
                              be shorter than this count.
        jrc_citing_dois       sorted, lowercased union of identifiable citing DOIs
                              (only when --citing-dois is given; off by default;
                              --source datacite only).
        jrc_citation_sources  per-source counts keyed by the registrar (datacite or
                              crossref) plus openalex and scholexplorer (datacite =
                              GraphQL count, or the REST count when GraphQL is
                              disabled/errored; the registrar count is a bare count,
                              not DOIs).
        jrc_citation_updated  timestamp of this enrichment.

    With --source crossref, only the three fields above (minus jrc_citing_dois)
    are dealt with: no figshare fields, no DataCite GraphQL.

    FIGSHARE FIELDS WRITTEN  (with --write, for figshare DOIs only, which are
    DataCite-registered, so --source datacite only; these are
    independent of citations - a figshare DOI may have usage but no citations -
    and the figshare REST/stats API exposes no citation count of its own)
        jrc_figshare_counts   {views, downloads, shares} usage totals from the
                              figshare stats service (stats.figshare.com).
        jrc_figshare_updated  timestamp of the figshare usage fetch.

    ZENODO FIELDS WRITTEN  (with --write, for Zenodo DOIs only, which are
    DataCite-registered, so --source datacite only; like figshare these are
    independent of citations - a Zenodo DOI may have usage but no citations)
        jrc_zenodo_counts     {views, unique_views, downloads, unique_downloads}
                              per-record usage from the Zenodo record API. The
                              all-versions version_* counters are NOT stored
                              (unreliable); deposit totals are summed across the
                              concept's versions downstream.
        jrc_zenodo_updated    timestamp of the Zenodo usage fetch.

    PROTOCOLS.IO FIELDS WRITTEN  (with --write, for protocols.io DOIs only, which
    are Crossref-registered, so --source crossref only; like figshare/Zenodo these
    are independent of citations - Crossref/OpenAlex already index protocols.io
    DOIs for citations, so no protocols.io-specific citation code is needed)
        jrc_protocolsio_counts  {views, exports, runs, forks_public, forks_private,
                                bookmarks, comments} per-protocol usage from the
                                protocols.io v4 API. Requires PROTOCOLS_API_TOKEN;
                                with no token configured, protocols.io usage is
                                skipped entirely rather than attempted per-DOI and
                                reported as an error.
        jrc_protocolsio_updated  timestamp of the protocols.io usage fetch.

    ELIFE FIELDS WRITTEN  (with --write, for eLife DOIs only, which are
    Crossref-registered, so --source crossref only; like protocols.io these are
    independent of citations - Crossref/OpenAlex already index eLife DOIs for
    citations, so no eLife-specific citation code is needed). Peer-review
    sub-documents (type "peer-review": decision letters, author responses -
    Crossref DOIs like 10.7554/elife.NNNNN.0NN) are skipped: they share their
    parent article's numeric eLife ID, so fetching usage for them would just
    duplicate the parent's numbers under a DOI that isn't really "an article".
        jrc_elife_counts      {views, downloads} per-article usage from the
                              public eLife metrics API. No auth required.
        jrc_elife_updated     timestamp of the eLife usage fetch.

    OUTPUT FILES  (written to the current directory every run, dry-run included;
    crossref/figshare/zenodo/protocolsio/elife runs use
    citation_updates_crossref.txt / _figshare / _zenodo / _protocolsio / _elife
    etc. so they never overwrite the datacite output)
        citation_updates.txt   tab-delimited log of DOIs whose count rose.
        citation_records.json  full records (the jrc_ fields above) for every
                               DOI that has a non-zero count.

    EMAIL
        A richly-formatted HTML summary is sent when --test (developer only) or
        --write (receivers) is given; sender/recipients come from the "dis" config.
        Sections: header banner (run data, mode, DRY RUN/WRITE badge), KPI stat
        tiles, a Citation Enrichment metrics table, a Usage Sources section with
        one card per active source (figshare/Zenodo/protocols.io/eLife) - each card has
        an OK/error status pill and a per-attribute "DOIs changed" table with an
        inline mini bar-chart - and a Citation Increases table. "Changed" means an
        attribute's value differs from its previously-stored value this run (a
        first-ever fetch counts every non-zero attribute as changed, from an
        implicit 0). Built entirely from inline styles/tables (no <style> block or
        percentage-width bars) for compatibility with older email clients.

    ENVIRONMENT
        OPENALEX_EMAIL contact address for the OpenAlex polite pool (required).
        OPENALEX_API_KEY OpenAlex API key (optional). OpenAlex meters by a daily
                         budget; the anonymous polite pool is ~$0.10/day (~1,000
                         requests) and returns 429 with a multi-hour Retry-After
                         when exhausted. A key raises the budget (~$1/day) and is
                         sent as the api_key query parameter when set.
        WOS_API_KEY Web of Science Starter API key (required when --wos is given).
                         Used only with --source crossref --wos; WoS does not index
                         DataCite/Zenodo deposits so the key is ignored otherwise.
        PROTOCOLS_API_TOKEN protocols.io v4 API bearer token. Every protocols.io
                         API request requires auth (no anonymous tier); with no
                         token set, protocols.io usage is silently skipped rather
                         than attempted and reported as an error.
        (eLife usage needs no environment variable - api.elifesciences.org is
        unauthenticated and public.)
        MongoDB connection comes from the JRC "databases" config

    USAGE
        export OPENALEX_EMAIL=you@janelia.hhmi.org
        # fast nightly pass (OpenAlex + ScholeXplorer), persist + email receivers:
        python sync_citations.py --write
        # quick dry-run over the already-cited subset:
        python sync_citations.py --cited
        # Crossref DOIs instead of DataCite:
        python sync_citations.py --source crossref --write
        # periodic deep pass that also folds in DataCite GraphQL citing DOIs:
        python sync_citations.py --write --datacite-graphql
        # single DOI, verbose, no writes:
        python sync_citations.py --doi 10.xxxx/yyy --debug
        # usage-only backfill for one publisher:
        python sync_citations.py --publisher elife --write

    OPTIONS
        --source SOURCE      datacite (default) or crossref; selects the
                             jrc_obtained_from subset of the dois collection
        --publisher {figshare,zenodo,protocolsio,elife}
                             restrict to one usage source's DOIs only (implies
                             --source per that source: figshare/zenodo=datacite,
                             protocolsio/elife=crossref); --cited/--notcited then
                             key on its jrc_<publisher>_counts (usage) instead of
                             citation counts; protocolsio requires
                             PROTOCOLS_API_TOKEN
        --doi DOI            process a single DOI (testing)
        --cited              only DOIs already known to be cited: citationCount > 0
                             for datacite, jrc_citation_count exists for crossref
                             (with --publisher <x>: jrc_<x>_counts exists)
        --notcited           only DOIs with no stored jrc_citation_count (not yet
                             enriched; with --publisher <x>: no stored
                             jrc_<x>_counts); mutually exclusive with --cited
        --limit N            cap the number of DOIs processed (0 = no limit)
        --workers N          concurrent OpenAlex/ScholeXplorer workers (default 8)
        --datacite-graphql   include DataCite GraphQL citing DOIs (off by default;
                             --source datacite only)
        --citing-dois        store the jrc_citing_dois list (off by default;
                             --source datacite only)
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

__version__ = '1.11.0'

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
# eLife usage is keyed on the article ID, so version DOIs (.<n>/.<n>.sa<n>) share
# a result; cache successful fetches by article_id to avoid refetching the same
# stats across versions. Populated from the worker threads, hence the lock.
ELIFE_CACHE = {}
ELIFE_CACHE_LOCK = threading.Lock()
# Global variables
ARG = DISCONFIG = LOGGER = None
# Per --source registrar: the jrc_obtained_from label selecting the DOI subset,
# and the native (registrar-provided) bare citation count already on the dois
# record, used as a floor on the citing-DOI union.
SOURCES = {'datacite': {'label': 'DataCite', 'count_field': 'citationCount'},
           'crossref': {'label': 'Crossref', 'count_field': 'is-referenced-by-count'}}
# Sentinel returned by openalex_citing when the cited_by_count matches the
# previously-stored OpenAlex count — signals "no change, skip pagination".
_OA_UNCHANGED = object()
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
# Zenodo usage metrics: the record API returns a per-record `stats` object in one
# request. A Zenodo DOI embeds its numeric record ID (10.5281/zenodo.<id>). The
# all-versions version_* counters are unreliable (sometimes lower than the
# record's own), so only the per-record counters are stored; deposit totals are
# summed across versions downstream. ZENODO_API_KEY (if set in the environment)
# raises the rate limit; public records also read without it.
ZENODO_RECORDS = 'https://zenodo.org/api/records/'
ZENODO_COUNTERS = ('views', 'unique_views', 'downloads', 'unique_downloads')
ZENODO_DOI_RE = re.compile(r'zenodo\.(\d+)', re.IGNORECASE)
# Zenodo DOIs are DataCite-registered: publisher "Zenodo", or (belt-and-suspenders)
# a 10.5281/zenodo. DOI - some carry a non-Zenodo publisher string. --publisher
# zenodo filters the dois query to this subset (and reinterprets --cited/--notcited
# on usage).
ZENODO_PUBLISHERS = ["Zenodo"]
# protocols.io usage metrics: the v4 protocols API accepts the DOI directly as the
# {id} path segment (no numeric-ID extraction needed, unlike Zenodo/figshare) and
# returns a per-protocol `stats` object in one request. protocols.io DOIs are
# Crossref-registered (prefix 10.17504), unlike figshare/Zenodo which are
# DataCite-registered - Crossref/OpenAlex already index these DOIs for citations,
# so only usage is protocols.io-specific. Every request requires a bearer token
# (PROTOCOLS_API_TOKEN, no anonymous tier); with none configured, protocols.io
# usage is skipped entirely rather than attempted per-DOI and reported as an error.
PROTOCOLSIO_API = 'https://www.protocols.io/api/v4/protocols/'
PROTOCOLSIO_COUNTERS = (('number_of_views', 'views'), ('number_of_exports', 'exports'),
                        ('number_of_runs', 'runs'), ('number_of_bookmarks', 'bookmarks'),
                        ('number_of_comments', 'comments'))
# Stored attribute names, in report order (the two fork counters are flattened
# out of number_of_forks separately, so they're not in PROTOCOLSIO_COUNTERS).
PROTOCOLSIO_OUT_ATTRS = tuple(out_key for _, out_key in PROTOCOLSIO_COUNTERS) \
                        + ('forks_public', 'forks_private')
PROTOCOLSIO_DOI_RE = re.compile(r'^10\.17504/', re.IGNORECASE)
# eLife usage metrics: the public metrics API takes eLife's own numeric article
# ID (not the DOI) as the path segment and returns {views, downloads} in one
# request. No auth needed - fully public/anonymous. eLife DOIs are
# Crossref-registered (prefix 10.7554, journal slug "elife"), like protocols.io -
# Crossref/OpenAlex already index these DOIs for citations, so only usage is
# eLife-specific. Unlike protocols.io, the numeric ID has to be extracted from
# the DOI (elife.0*<id>, leading zeros in the older DOI style are not part of
# the real ID - e.g. 10.7554/elife.00170 -> id 170). A version suffix
# (.<n>[.sa<n>]) is ignored by the regex, so every version of a "reviewed
# preprint" resolves to the same underlying article ID and gets identical
# usage - the same behavior already accepted for protocols.io/Zenodo versions.
# PEER-REVIEW SUB-DOCUMENTS (Crossref type "peer-review": decision letters,
# author responses, e.g. 10.7554/elife.00337.023) are NOT usage-eligible: they
# share their parent article's numeric ID, so fetching usage for them would
# silently duplicate the parent's numbers under a DOI that isn't really "an
# article" - excluded by checking the DOI record's own `type` field, not by a
# DOI-shape heuristic (a legitimate reviewed-preprint version has the same
# dotted shape as a peer-review sub-document, e.g. elife.106548.1).
ELIFE_METRICS = 'https://api.elifesciences.org/metrics/article/'
ELIFE_DOI_RE = re.compile(r'^10\.7554/elife\.0*(\d+)', re.IGNORECASE)
ELIFE_EXCLUDE_TYPES = {'peer-review'}
ELIFE_COUNTERS = ('views', 'downloads')
# Registry of restrictable per-publisher usage sources (--publisher <key>).
# Each entry captures everything that differs across restrict modes: which
# --source it implies, the jrc_<key>_counts field --cited/--notcited key on,
# the DOI-match query filter to AND with jrc_obtained_from, the per-attribute
# report order, and (if any) a required environment variable that gates the
# source entirely (checked at parse time so a misconfigured run fails fast).
PUBLISHERS = {
    'figshare': {'label': 'Figshare', 'source': 'datacite',
                'count_field': 'jrc_figshare_counts', 'counters': FIGSHARE_COUNTERS,
                'match': {'$or': [{'doi': {'$regex': pattern.pattern, '$options': 'i'}}
                                  for pattern, _ in FIGSHARE_DOI_PATTERNS]},
                'token_env': None},
    'zenodo': {'label': 'Zenodo', 'source': 'datacite',
              'count_field': 'jrc_zenodo_counts', 'counters': ZENODO_COUNTERS,
              'match': {'$or': [{'publisher': {'$in': ZENODO_PUBLISHERS}},
                                {'doi': {'$regex': ZENODO_DOI_RE.pattern, '$options': 'i'}}]},
              'token_env': None},
    'protocolsio': {'label': 'protocols.io', 'source': 'crossref',
                    'count_field': 'jrc_protocolsio_counts', 'counters': PROTOCOLSIO_OUT_ATTRS,
                    'match': {'doi': {'$regex': PROTOCOLSIO_DOI_RE.pattern, '$options': 'i'}},
                    'token_env': 'PROTOCOLS_API_TOKEN'},
    'elife': {'label': 'eLife', 'source': 'crossref',
             'count_field': 'jrc_elife_counts', 'counters': ELIFE_COUNTERS,
             'match': {'doi': {'$regex': ELIFE_DOI_RE.pattern, '$options': 'i'},
                      'type': {'$nin': list(ELIFE_EXCLUDE_TYPES)}},
             'token_env': None},
}
# Which PUBLISHERS keys apply under each --source, in report/card order.
SOURCE_PUBLISHERS = {'datacite': ('figshare', 'zenodo'), 'crossref': ('protocolsio', 'elife')}
DATACITE_QUERY = '''query($id: ID!, $after: String) {
  work(id: $id) {
    citations(first: 100, after: $after) {
      pageInfo { endCursor hasNextPage }
      nodes { doi }
    }
  }
}'''
# Web of Science Starter API: one request per DOI, returns a bare citation count.
# Only used for --source crossref (WoS does not index DataCite/Zenodo deposits).
# The Starter API rate-limits concurrent access, so requests are serialized with
# a minimum interval (similar to DataCite GraphQL).
WOS_BASE = 'https://api.clarivate.com/apis/wos-starter/v1/documents'
WOS_LOCK = threading.Lock()
WOS_MIN_INTERVAL = 0.2   # 5 req/s max per Starter API docs
WOS_LAST = [0.0]
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
# Cap on how long a single Retry-After is honored before giving up the attempt.
# OpenAlex budget-exhaustion 429s carry a Retry-After up to ~7h (budget resets at
# midnight UTC); without a cap every worker would sleep for hours and the run would
# appear hung. Capping lets the attempt fail fast so the run degrades to the other
# sources (combine_counts preserves any previously-stored count) instead of freezing.
RETRY_AFTER_MAX = 120
# HTML run-summary email palette/layout (generate_email and its html_* helpers).
# Colors pair a status with an icon/label, not color alone, for colorblind
# accessibility. All inline styles (no <style> block/classes) and fixed pixel
# bar widths (not percentages) for reliable rendering across email clients,
# including older Outlook.
EMAIL_NAVY = '#1f3a5f'
EMAIL_GREEN = '#1c7c3f'
EMAIL_GREEN_BG = '#eefaf1'
EMAIL_RED = '#c0392b'
EMAIL_RED_BG = '#fdecea'
EMAIL_AMBER = '#d68a1f'
EMAIL_GRAY = '#5b6b7c'
EMAIL_GRAY_BG = '#f2f4f6'
EMAIL_STRIPE_BG = '#f7f9fb'
EMAIL_BORDER = '#eef1f4'
EMAIL_BLUE = '#2f7fd1'
EMAIL_BLUE_LIGHT = '#a9c8ea'
EMAIL_BAR_PX = 140


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


def get_with_retry(url, params, timeout, tries=3, json_body=None, headers=None):
    ''' HTTP request with retry/backoff on transient failures (429, 5xx, exceptions)
        Keyword arguments:
          url: URL
          params: query parameters
          timeout: per-request timeout in seconds
          tries: maximum attempts
          json_body: if given, POST this JSON body instead of issuing a GET
          headers: optional request headers (e.g. an Authorization bearer)
        Returns:
          requests Response (possibly a non-2xx), or None if every attempt raised
    '''
    resp = None
    for attempt in range(1, tries + 1):
        try:
            if json_body is not None:
                resp = http_session().post(url, params=params, json=json_body,
                                           timeout=timeout, headers=headers)
            else:
                resp = http_session().get(url, params=params, timeout=timeout,
                                          headers=headers)
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
            sleep(min(wait, RETRY_AFTER_MAX))
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


def with_openalex_auth(params):
    ''' Add OpenAlex auth/identity to a request's query params: the polite-pool
        contact email (always) and the API key (when OPENALEX_API_KEY is set). The
        key draws on a larger daily budget than the anonymous pool, avoiding the
        budget-exhaustion 429s that otherwise stall the run.
        Keyword arguments:
          params: request query-parameter dict (mutated in place and returned)
        Returns:
          the params dict with 'mailto' (and 'api_key' if available) added
    '''
    params['mailto'] = os.environ['OPENALEX_EMAIL']
    key = os.environ.get('OPENALEX_API_KEY')
    if key:
        params['api_key'] = key
    return params


def openalex_citing(doi, existing_oa_count=None):  # pylint: disable=too-many-return-statements
    ''' Get the set of DOIs citing this DOI from OpenAlex
        Keyword arguments:
          doi: DOI string
          existing_oa_count: previously-stored OpenAlex citation count (or None).
                             If the live cited_by_count matches this, pagination is
                             skipped and _OA_UNCHANGED is returned.
        Returns:
          set of citing DOIs (empty if the work is unknown or uncited),
          _OA_UNCHANGED if the count is unchanged, or None on error
    '''
    resp = get_with_retry(f"{OPENALEX_WORK}{doi.lower()}",
                          with_openalex_auth({'select': 'id,cited_by_count'}), 20)
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
    if existing_oa_count is not None and data['cited_by_count'] == existing_oa_count:
        return _OA_UNCHANGED
    wid = (data.get('id') or '').rsplit('/', 1)[-1]
    if not wid:
        return set()
    citing = set()
    cursor = '*'
    while cursor:
        resp = get_with_retry(OPENALEX_WORKS,
                              with_openalex_auth({'filter': f"cites:{wid}", 'select': 'doi',
                                                  'per-page': 200, 'cursor': cursor}), 30)
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


def zenodo_record_id(doi):
    ''' Return the numeric Zenodo record ID embedded in a Zenodo DOI
        (10.5281/zenodo.<id>), which doubles as the "is this a Zenodo DOI" test.
        Keyword arguments:
          doi: DOI string
        Returns:
          the record ID string, or None if this is not a Zenodo DOI
    '''
    if doi:
        match = ZENODO_DOI_RE.search(doi)
        if match:
            return match.group(1)
    return None


def zenodo_metrics(doi):
    ''' Get Zenodo usage metrics (views/downloads, plus unique) for a Zenodo DOI.
        Zenodo's record API exposes a per-record `stats` object (it carries no
        citation count of its own). The all-versions version_* counters are
        ignored as unreliable; deposit totals are summed across versions
        downstream. ZENODO_API_KEY raises the rate limit when set.
        Keyword arguments:
          doi: DOI string
        Returns:
          {'views', 'unique_views', 'downloads', 'unique_downloads'} for a Zenodo
          DOI, or None if this is not a Zenodo DOI or the request failed / carried
          no usable stats (a partial result is discarded so a stored count is
          never silently incomplete)
    '''
    rid = zenodo_record_id(doi)
    if not rid:
        return None
    api_key = os.environ.get('ZENODO_API_KEY')
    headers = {'Authorization': f"Bearer {api_key}"} if api_key else None
    resp = get_with_retry(f"{ZENODO_RECORDS}{rid}", None, 20, headers=headers)
    if resp is None or resp.status_code != 200:
        if ARG.DEBUG:
            tqdm.write(f"Zenodo stats error for {doi}: "
                       + (f"HTTP {resp.status_code}" if resp is not None
                          else "no response"))
        return None
    try:
        stats = (resp.json() or {}).get('stats') or {}
    except Exception:
        return None
    if not stats:
        return None
    return {counter: int(stats.get(counter, 0) or 0) for counter in ZENODO_COUNTERS}


def protocolsio_doi(doi):
    ''' Return the DOI itself if it is a protocols.io DOI (Crossref prefix
        10.17504), which doubles as the "is this a protocols.io DOI" test. Unlike
        Zenodo/figshare, the v4 API accepts the DOI directly as the lookup id, so
        no separate numeric ID needs to be extracted.
        Keyword arguments:
          doi: DOI string
        Returns:
          the DOI string, or None if this is not a protocols.io DOI
    '''
    if doi and PROTOCOLSIO_DOI_RE.match(doi):
        return doi
    return None


def protocolsio_metrics(doi):
    ''' Get protocols.io usage metrics (views/exports/runs/forks/bookmarks/
        comments) for a protocols.io DOI. The v4 API's per-protocol `stats` object
        carries no citation count of its own - Crossref/OpenAlex already index
        these DOIs for citations. PROTOCOLS_API_TOKEN must be set (every request
        requires it, no anonymous tier); the caller only invokes this once a
        token is confirmed present.
        Keyword arguments:
          doi: DOI string
        Returns:
          {'views', 'exports', 'runs', 'forks_public', 'forks_private', 'bookmarks',
          'comments'} for a protocols.io DOI, or None if the request failed or
          carried no usable stats (a partial result is discarded so a stored count
          is never silently incomplete)
    '''
    headers = {'Authorization': f"Bearer {os.environ['PROTOCOLS_API_TOKEN']}"}
    resp = get_with_retry(f"{PROTOCOLSIO_API}{doi.lower()}", None, 20, headers=headers)
    if resp is None or resp.status_code != 200:
        if ARG.DEBUG:
            tqdm.write(f"protocols.io stats error for {doi}: "
                       + (f"HTTP {resp.status_code}" if resp is not None
                          else "no response"))
        return None
    try:
        stats = ((resp.json() or {}).get('payload') or {}).get('stats') or {}
    except Exception:
        return None
    if not stats:
        return None
    counts = {out_key: int(stats.get(json_key, 0) or 0)
              for json_key, out_key in PROTOCOLSIO_COUNTERS}
    forks = stats.get('number_of_forks') or {}
    counts['forks_public'] = int(forks.get('public', 0) or 0)
    counts['forks_private'] = int(forks.get('private', 0) or 0)
    return counts


def elife_article_id(doi, doc_type):
    ''' Return the numeric eLife article ID embedded in an eLife DOI (see
        ELIFE_DOI_RE), which doubles as the "is this a usage-eligible eLife
        DOI" test. Peer-review sub-documents (doc_type == 'peer-review') are
        excluded: they share their parent article's numeric ID, so fetching
        usage for them would just duplicate the parent's numbers under a DOI
        that isn't really "an article".
        Keyword arguments:
          doi: DOI string
          doc_type: the DOI record's Crossref `type` field
        Returns:
          the numeric ID string (leading zeros stripped), or None if this is
          not a usage-eligible eLife DOI
    '''
    if doc_type in ELIFE_EXCLUDE_TYPES:
        return None
    match = ELIFE_DOI_RE.match(doi)
    return match.group(1) if match else None


def elife_metrics(doi, doc_type):
    ''' Get eLife usage metrics (views/downloads) for an eLife DOI. The
        metrics API's crossref/pubmed/scopus sub-fields are eLife's own
        citation self-count, already superseded by the union DIS computes
        from OpenAlex/ScholeXplorer/Crossref for every Crossref DOI, so only
        views/downloads are kept. The API is fully public (no auth). NOTE: an
        invalid/nonexistent article ID returns HTTP 200 with all-zero counts
        rather than a 404, so a bad ID can't be distinguished from genuine
        zero usage - correctness here depends on elife_article_id() only ever
        matching real, usage-eligible articles.
        Keyword arguments:
          doi: DOI string
          doc_type: the DOI record's Crossref `type` field (see elife_article_id)
        Returns:
          {'views', 'downloads'} for the article, or None if this is not a
          usage-eligible eLife DOI, the request failed, or it carried no
          usable stats (a partial result is discarded so a stored count is
          never silently incomplete)
    '''
    article_id = elife_article_id(doi, doc_type)
    if not article_id:
        return None
    # Version DOIs (.<n>/.<n>.sa<n>) resolve to the same article ID, so return a
    # cached per-article result instead of refetching identical stats. Only
    # successes are cached; a failure stays uncached so it is retried rather
    # than poisoning every version of this article for the whole run.
    with ELIFE_CACHE_LOCK:
        cached = ELIFE_CACHE.get(article_id)
    if cached is not None:
        return cached
    resp = get_with_retry(f"{ELIFE_METRICS}{article_id}/summary", None, 20)
    if resp is None or resp.status_code != 200:
        if ARG.DEBUG:
            tqdm.write(f"eLife stats error for {doi}: "
                       + (f"HTTP {resp.status_code}" if resp is not None
                          else "no response"))
        return None
    try:
        items = (resp.json() or {}).get('items') or []
    except Exception:
        return None
    if not items:
        return None
    item = items[0]
    counts = {'views': int(item.get('views', 0) or 0),
              'downloads': int(item.get('downloads', 0) or 0)}
    with ELIFE_CACHE_LOCK:
        ELIFE_CACHE[article_id] = counts
    return counts


def wos_count(doi):
    ''' Get the Web of Science citation count for a DOI via the Starter API.
        Returns a bare count only (no citing DOIs). WOS_API_KEY must be set.
        Requests are serialized and rate-limited to avoid 429s from concurrent
        workers.
        Keyword arguments:
          doi: DOI string
        Returns:
          citation count (int, 0 if not found in WoS), or None on error
    '''
    api_key = os.environ.get('WOS_API_KEY')
    if not api_key:
        return None
    with WOS_LOCK:
        wait = WOS_MIN_INTERVAL - (monotonic() - WOS_LAST[0])
        if wait > 0:
            sleep(wait)
        WOS_LAST[0] = monotonic()
    resp = get_with_retry(WOS_BASE, {'db': 'WOS', 'q': f"DO={doi}", 'limit': 1},
                          20, headers={'X-ApiKey': api_key})
    if resp is None or resp.status_code != 200:
        if ARG.DEBUG:
            tqdm.write(f"WoS error for {doi}: "
                       + (f"HTTP {resp.status_code}" if resp is not None else "no response"))
        return None
    try:
        hits = resp.json().get('hits') or []
    except Exception:
        return None
    if not hits:
        return 0
    for cite in hits[0].get('citations') or []:
        if cite.get('db') == 'WOS':
            return int(cite.get('count') or 0)
    return 0


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
          row: dois document (needs doi, the native registrar count field,
               optionally jrc_citation_count)
        Returns:
          result dict for finalize_doi()
    '''
    doi = row['doi']
    # Figshare/Zenodo DOIs are DataCite-registered; Crossref runs deal only with
    # citations, so their usage fetches are skipped there. protocols.io/eLife
    # DOIs are Crossref-registered (the opposite split); protocols.io
    # additionally requires a configured token - with none set, its usage is
    # skipped rather than attempted.
    fid = figshare_article(doi)[0] if ARG.SOURCE == 'datacite' else None
    zid = zenodo_record_id(doi) if ARG.SOURCE == 'datacite' else None
    pio = bool(ARG.SOURCE == 'crossref' and protocolsio_doi(doi)
               and os.environ.get('PROTOCOLS_API_TOKEN'))
    eid = elife_article_id(doi, row.get('type')) if ARG.SOURCE == 'crossref' else None
    return {'row': row,
            'doi': doi,
            'native': row.get(SOURCES[ARG.SOURCE]['count_field'], 0) or 0,
            'existing': row.get('jrc_citation_count'),
            'existing_dois': row.get('jrc_citing_dois') or [],
            'openalex': openalex_citing(doi, (row.get('jrc_citation_sources') or {}).get('openalex')),
            'scholex': scholix_citing(doi),
            'datacite_dois': DATACITE_CITING.get(doi),
            'is_figshare': bool(fid),
            'figshare': figshare_metrics(doi) if fid else None,
            'is_zenodo': bool(zid),
            'zenodo': zenodo_metrics(doi) if zid else None,
            'is_protocolsio': pio,
            'protocolsio': protocolsio_metrics(doi) if pio else None,
            'is_elife': bool(eid),
            'elife': elife_metrics(doi, row.get('type')) if eid else None,
            'wos': wos_count(doi) if (ARG.SOURCE == 'crossref'
                                       and ARG.WOS) else None}


def combine_counts(res, graphql, source='datacite'):
    ''' Pure citation-combination logic (no I/O, no shared state) - unit-testable.
        Builds the deduped union of identifiable citing DOIs across the available
        sources, applies the native registrar count as a bare floor, and protects a
        previously-stored higher count from a transient source error.
        Keyword arguments:
          res: result dict from fetch_doi()
          graphql: whether DataCite GraphQL citing DOIs are in play (ARG.GRAPHQL)
          source: registrar key for the sources breakdown (datacite or crossref)
        Returns:
          dict with:
            combined   final citation count (max of the floor and the union size)
            previous   count stored on the prior run (0 if none)
            citing     sorted list of unique citing DOIs
            sources    {<source>, openalex, scholexplorer[, wos]} per-source counts
            errored    True if any enabled citing-DOI source failed
            preserved  True if a stored higher count was kept despite an error
            oa_n/sx_n/dc_n/wos_n  per-source counts (None on error/disabled), for reporting
    '''
    native = res['native']
    existing = res['existing']
    # Baseline for "increased": the count stored on the previous run (0 if none).
    previous = existing if isinstance(existing, int) else 0
    openalex = res['openalex']        # set of citing DOIs, _OA_UNCHANGED, or None on error
    scholex = res['scholex']
    oa_unchanged = openalex is _OA_UNCHANGED
    if oa_unchanged:
        openalex = None             # treat like a non-error skip for union purposes
    oa_n = None if openalex is None else len(openalex)
    sx_n = None if scholex is None else len(scholex)
    # True union of identifiable citing DOIs across the available sources
    union = set()
    union.update(openalex or set())
    union.update(scholex or set())
    errored = (openalex is None and not oa_unchanged) or scholex is None
    # DataCite GraphQL (citing DOIs) only when enabled - it is slow, so optional
    dc_n = None
    if graphql:
        datacite_dois = res['datacite_dois']
        dc_n = None if datacite_dois is None else len(datacite_dois)
        union.update(datacite_dois or set())
        errored = errored or datacite_dois is None
    # On a source error or an unchanged OpenAlex result, preserve existing citing DOIs
    if errored or oa_unchanged:
        union.update(filter(None, (normalize_doi(cd) for cd in res['existing_dois'])))
    citing = sorted(union)
    # WoS contributes a bare count (no DOIs), used as an additional floor
    wos_n = res.get('wos')      # int, 0, or None (disabled/error)
    # The registrar (and WoS when available) contribute counts but no DOIs; keep
    # the max of all bare counts as the floor.
    floor = max(native, wos_n or 0)
    combined = max(floor, len(citing))
    # Don't let a transient source error regress a previously-stored higher count
    preserved = False
    if errored and isinstance(existing, int) and existing > combined:
        combined = existing
        preserved = True
    # Per-source counts; the registrar's is a bare count, openalex/scholexplorer
    # are identifiable citing-DOI counts (for datacite, the GraphQL citing-DOI
    # count when available, falling back to the REST citationCount).
    # For any source that errored or was skipped (None), fall back to the
    # previously-stored value so a transient failure never writes a null.
    existing_sources = ((res.get('row') or {}).get('jrc_citation_sources')) or {}
    if oa_n is None:
        oa_n = existing_sources.get('openalex', oa_n)
    if sx_n is None:
        sx_n = existing_sources.get('scholexplorer', sx_n)
    if dc_n is None and graphql:
        dc_n = existing_sources.get(source, dc_n)
    sources = {source: native if dc_n is None else dc_n,
               'openalex': oa_n, 'scholexplorer': sx_n}
    if wos_n is not None:
        sources['wos'] = wos_n
    elif 'wos' in existing_sources:
        sources['wos'] = existing_sources['wos']
    return {'combined': combined, 'previous': previous, 'citing': citing,
            'sources': sources, 'errored': errored, 'preserved': preserved,
            'oa_n': oa_n, 'sx_n': sx_n, 'dc_n': dc_n, 'wos_n': wos_n}


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


def record_usage_changes(prefix, new_counts, previous_counts):
    ''' Bump a per-attribute "how many DOIs changed" counter for a usage source
        (figshare/Zenodo/protocols.io), for the run summary. A DOI with no
        previously-stored counts (first-ever fetch) treats every non-zero
        attribute as changed from an implicit 0, since populating usage for
        the first time is itself a real, reportable change.
        Keyword arguments:
          prefix: COUNT key prefix for this source (e.g. 'figshare')
          new_counts: this run's usage counts dict
          previous_counts: the previously-stored usage counts dict, or None
        Returns:
          None
    '''
    previous_counts = previous_counts or {}
    for attr, value in new_counts.items():
        if value != previous_counts.get(attr, 0):
            COUNT[f"{prefix}_changed_{attr}"] += 1


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
    native = res['native']
    # Per-source lookup outcome counters (a raw None result means that source errored)
    if res['openalex'] is _OA_UNCHANGED:
        COUNT['openalex_unchanged'] += 1
    else:
        COUNT['openalex_found' if res['openalex'] is not None else 'openalex_error'] += 1
    COUNT['scholex_found' if res['scholex'] is not None else 'scholex_error'] += 1
    if ARG.GRAPHQL:
        COUNT['datacite_found' if res['datacite_dois'] is not None
              else 'datacite_error'] += 1
    if ARG.WOS:
        if res.get('wos') is None:
            COUNT['wos_error'] += 1
        else:
            COUNT['wos_found'] += 1
    cc = combine_counts(res, ARG.GRAPHQL, ARG.SOURCE)
    combined, previous = cc['combined'], cc['previous']
    oa_n, sx_n, dc_n, wos_n = cc['oa_n'], cc['sx_n'], cc['dc_n'], cc['wos_n']
    if cc['preserved']:
        COUNT['preserved'] += 1
    # Figshare usage metrics (views/downloads/shares) are independent of citations:
    # a figshare DOI may have usage but no citations, and is still recorded/stored.
    usage_fields = {}
    row = res['row']
    if res['is_figshare']:
        if res['figshare'] is None:
            COUNT['figshare_error'] += 1
        else:
            COUNT['figshare_found'] += 1
            usage_fields['jrc_figshare_counts'] = res['figshare']
            usage_fields['jrc_figshare_updated'] = datetime.now()
            record_usage_changes('figshare', res['figshare'], row.get('jrc_figshare_counts'))
    # Zenodo usage metrics (views/downloads + unique) are likewise independent of
    # citations: a Zenodo DOI may have usage but no citations, and is still stored.
    if res['is_zenodo']:
        if res['zenodo'] is None:
            COUNT['zenodo_error'] += 1
        else:
            COUNT['zenodo_found'] += 1
            usage_fields['jrc_zenodo_counts'] = res['zenodo']
            usage_fields['jrc_zenodo_updated'] = datetime.now()
            record_usage_changes('zenodo', res['zenodo'], row.get('jrc_zenodo_counts'))
    # protocols.io usage metrics (views/exports/runs/forks/bookmarks/comments) are
    # likewise independent of citations: Crossref/OpenAlex already index these DOIs
    # for citations, so only usage is protocols.io-specific.
    if res['is_protocolsio']:
        if res['protocolsio'] is None:
            COUNT['protocolsio_error'] += 1
        else:
            COUNT['protocolsio_found'] += 1
            usage_fields['jrc_protocolsio_counts'] = res['protocolsio']
            record_usage_changes('protocolsio', res['protocolsio'],
                                 row.get('jrc_protocolsio_counts'))
            usage_fields['jrc_protocolsio_updated'] = datetime.now()
    # eLife usage metrics (views/downloads) are likewise independent of
    # citations: Crossref/OpenAlex already index these DOIs for citations, so
    # only usage is eLife-specific.
    if res['is_elife']:
        if res['elife'] is None:
            COUNT['elife_error'] += 1
        else:
            COUNT['elife_found'] += 1
            usage_fields['jrc_elife_counts'] = res['elife']
            record_usage_changes('elife', res['elife'], row.get('jrc_elife_counts'))
            usage_fields['jrc_elife_updated'] = datetime.now()
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
            HITS.append((doi, previous, native, combined, oa_n, sx_n, dc_n, wos_n))
            label = SOURCES[ARG.SOURCE]['label']
            graphql_disp = ('' if ARG.SOURCE == 'crossref' else
                            f", DataCite-GraphQL={dc_n if ARG.GRAPHQL else 'off'}")
            hit = (f"{doi}\t{previous} -> {combined} "
                   + f"({label}={native}, OpenAlex={oa_n}, ScholeXplorer={sx_n}"
                   + f"{graphql_disp})")
            if monitor:
                monitor.write(hit + "\n")
                monitor.flush()
            if ARG.DEBUG:
                tqdm.write(hit)
    # Persist citation and usage (figshare/Zenodo/protocols.io/eLife) fields
    # together; nothing to do if all are empty
    fields.update(usage_fields)
    if not fields:
        return
    RECORDS.append({'doi': doi, **fields})
    if ARG.WRITE:
        queue_write(res['row']['_id'], fields)


def usage_change_block(label, prefix, attrs):
    ''' Build the "<label> attribute changes" summary block for a usage source:
        how many DOIs had that attribute differ from its previously-stored value
        this run (see record_usage_changes for what counts as "changed").
        Keyword arguments:
          label: display label (e.g. "Figshare")
          prefix: COUNT key prefix for this source (e.g. 'figshare')
          attrs: ordered attribute names to report
        Returns:
          text block: a header line plus one "  attr: n" line per attribute
    '''
    lines = [f"{label} attribute changes:"]
    lines.extend(f"  {attr}: {COUNT[f'{prefix}_changed_{attr}']:,}" for attr in attrs)
    return "\n".join(lines) + "\n"


def usage_summary_line(key):
    ''' Build one usage source's plain-text summary block for the console/log
        report: its "OK (errors)" (or "disabled") line plus its
        attribute-change block. Registry-driven off PUBLISHERS so each
        source's report text lives in one place instead of being
        hand-repeated per --publisher key.
        Keyword arguments:
          key: PUBLISHERS registry key (e.g. 'figshare')
        Returns:
          formatted text block
    '''
    pub = PUBLISHERS[key]
    if pub['token_env'] and not os.environ.get(pub['token_env']):
        return f"{pub['label']} usage:".ljust(36) + f"disabled (set {pub['token_env']})\n"
    return (f"{pub['label']} usage OK (errors):".ljust(36)
            + f"{COUNT[f'{key}_found']:,} ({COUNT[f'{key}_error']:,})\n"
            + usage_change_block(pub['label'], key, pub['counters']))


def doiurl(doi):
    ''' Format a DOI as a DIS UI link
        Keyword arguments:
          doi: DOI to format
        Returns:
          HTML anchor
    '''
    return (f"<a href='https://dis.int.janelia.org/doiui/{doi}' "
            f"style='color:{EMAIL_BLUE};text-decoration:none;'>{doi}</a>")


def html_kpi_card(value, label, tone='neutral'):
    ''' Build one KPI stat tile for the run-summary email's header row.
        A single <td> carries the box look directly (bgcolor attribute +
        background-color, no nested table) - Outlook's Word rendering engine
        chokes on a percentage-width table nested inside a percentage-width
        <td> (this one used to nest a width="100%" table inside width="25%"),
        sometimes dumping raw markup as visible text instead of rendering it.
        Keyword arguments:
          value: display value (already formatted, e.g. "52" or "33/0")
          label: caption under the value
          tone: 'neutral', 'good', or 'bad' - selects the tile's color scheme
        Returns:
          HTML for one table cell
    '''
    bg, fg = {'good': (EMAIL_GREEN_BG, EMAIL_GREEN),
              'bad': (EMAIL_RED_BG, EMAIL_RED),
              'neutral': (EMAIL_GRAY_BG, EMAIL_GRAY)}[tone]
    return (f'<td width="25%" align="center" valign="top" bgcolor="{bg}" '
            f'style="padding:14px 6px;background-color:{bg};border-radius:8px;">'
            f'<div style="font-size:24px;font-weight:700;color:{fg};">{value}</div>'
            f'<div style="font-size:10.5px;color:{EMAIL_GRAY};text-transform:uppercase;'
            f'letter-spacing:.04em;margin-top:2px;">{label}</div>'
            f'</td>')


def html_section_header(title):
    ''' Build a section header bar for the run-summary email
        Keyword arguments:
          title: section title (may include an HTML entity icon prefix)
        Returns:
          HTML div block
    '''
    return (f'<div style="font-size:14px;font-weight:700;color:{EMAIL_NAVY};'
            f'border-bottom:2px solid {EMAIL_BORDER};padding-bottom:7px;'
            f'margin-bottom:10px;">{title}</div>')


def html_metric_rows(rows):
    ''' Build a zebra-striped label/value table for the run-summary email
        Keyword arguments:
          rows: list of (label, value_html) pairs
        Returns:
          HTML table
    '''
    trs = []
    for i, (mlabel, value) in enumerate(rows):
        striped = i % 2 == 0
        bgattr = f' bgcolor="{EMAIL_STRIPE_BG}"' if striped else ''
        bg = f'background-color:{EMAIL_STRIPE_BG};' if striped else ''
        r_l = 'border-radius:6px 0 0 6px;' if bg else ''
        r_r = 'border-radius:0 6px 6px 0;' if bg else ''
        trs.append(f'<tr{bgattr} style="{bg}">'
                   f'<td style="padding:8px 10px;{r_l}">{mlabel}</td>'
                   f'<td align="right" style="padding:8px 10px;text-align:right;{r_r}">'
                   f'{value}</td></tr>')
    return ('<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
            'style="border-collapse:collapse;font-size:13px;margin-top:6px;">'
            + "".join(trs) + '</table>')


def html_usage_bar(count, total):
    ''' Build one inline mini bar-chart cell for a usage-source attribute row.
        Fixed pixel widths (not percentages) are used for the track/fill, since
        nested percentage-width tables render unreliably in some email clients.
        Keyword arguments:
          count: DOIs changed for this attribute
          total: DOIs successfully fetched for this source this run (bar denominator)
        Returns:
          HTML table cell
    '''
    pct = 0 if not total else min(1.0, count / total)
    fill = max(4, round(EMAIL_BAR_PX * pct)) if count else 0
    color = EMAIL_BLUE if pct >= 0.15 else EMAIL_BLUE_LIGHT
    fill_html = (f'<table cellpadding="0" cellspacing="0"><tr>'
                 f'<td bgcolor="{color}" style="background-color:{color};'
                 f'border-radius:4px;height:7px;line-height:7px;font-size:1px;'
                 f'width:{fill}px;">&nbsp;</td></tr></table>') if fill else ''
    return (f'<td style="padding:5px 4px 5px 12px;width:{EMAIL_BAR_PX + 10}px;">'
            f'<table role="presentation" cellpadding="0" cellspacing="0"><tr>'
            f'<td bgcolor="{EMAIL_BORDER}" style="background-color:{EMAIL_BORDER};'
            f'border-radius:4px;height:7px;'
            f'line-height:7px;font-size:1px;" width="{EMAIL_BAR_PX}">{fill_html}</td>'
            f'</tr></table></td>')


def html_pill(bg, fg, text):
    ''' Build a small colored status badge as an auto-width single-cell table
        (bgcolor attribute + background-color CSS), not a <span> - Outlook's
        Word engine does not honor background-color on inline elements, and
        <span> has no bgcolor attribute to fall back on (see html_kpi_card for
        the same issue on block elements). Only safe where the badge is the
        sole content of its table cell: an auto-width table still renders as a
        block, so it cannot sit inline mid-sentence next to other text.
        Keyword arguments:
          bg: background color
          fg: text color
          text: pill text (may include an HTML entity icon prefix)
        Returns:
          HTML for a single-cell table sized to its content
    '''
    return (f'<table role="presentation" cellpadding="0" cellspacing="0"><tr>'
            f'<td bgcolor="{bg}" style="background-color:{bg};color:{fg};padding:2px 10px;'
            f'border-radius:10px;font-size:11.5px;font-weight:600;">{text}</td></tr></table>')


def html_usage_card(label, ok, err, attrs, prefix):
    ''' Build one Usage Sources card: a status pill (OK/error counts) plus a
        per-attribute "how many DOIs changed" table with an inline mini bar-chart.
        Keyword arguments:
          label: display label (e.g. "Figshare")
          ok: DOIs successfully fetched this run (also the bar-chart denominator)
          err: DOIs that errored this run
          attrs: ordered attribute names to report
          prefix: COUNT key prefix for this source (e.g. 'figshare')
        Returns:
          HTML card block
    '''
    if err:
        pill_bg, pill_fg, pill_icon = EMAIL_RED_BG, EMAIL_RED, '&#9888;'
    else:
        pill_bg, pill_fg, pill_icon = EMAIL_GREEN_BG, EMAIL_GREEN, '&#10003;'
    pill = html_pill(pill_bg, pill_fg, f'{pill_icon} {ok:,} ok &middot; {err:,} err')
    rows = []
    for attr in attrs:
        changed = COUNT[f"{prefix}_changed_{attr}"]
        fg = EMAIL_NAVY if changed else EMAIL_GRAY
        rows.append(f'<tr><td style="padding:5px 4px;">{attr}</td>'
                    f'<td style="padding:5px 4px;font-weight:700;color:{fg};" '
                    f'align="right">{changed:,}</td>'
                    + html_usage_bar(changed, ok) + '</tr>')
    table = ('<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
             f'style="font-size:12.5px;"><tr style="color:{EMAIL_GRAY};font-size:10.5px;'
             'text-transform:uppercase;letter-spacing:.03em;">'
             '<td style="padding:6px 4px;">Attribute</td>'
             '<td style="padding:6px 4px;" align="right">Changed</td><td></td></tr>'
             + "".join(rows) + '</table>')
    # Header row is two <td>s directly in the outer table (not a nested
    # width="100%" table inside one <td>) - see html_kpi_card for why: Outlook's
    # Word engine chokes on that nesting. The body row below spans both with
    # colspan="2".
    return (
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
        f'style="border:1px solid {EMAIL_BORDER};border-radius:8px;margin-bottom:14px;'
        'border-collapse:separate;">'
        f'<tr><td bgcolor="{EMAIL_STRIPE_BG}" style="background-color:{EMAIL_STRIPE_BG};'
        f'padding:10px 16px;border-radius:8px 0 0 0;font-weight:700;color:{EMAIL_NAVY};'
        f'font-size:13.5px;">{label}</td>'
        f'<td bgcolor="{EMAIL_STRIPE_BG}" align="right" style="background-color:'
        f'{EMAIL_STRIPE_BG};padding:10px 16px;border-radius:0 8px 0 0;">{pill}</td></tr>'
        f'<tr><td colspan="2" style="padding:4px 16px 10px 16px;">{table}</td></tr></table>')


def html_usage_disabled_card(label, note):
    ''' Build a "disabled" Usage Sources card (e.g. protocols.io with no token set)
        Keyword arguments:
          label: display label
          note: short explanation (e.g. how to enable it)
        Returns:
          HTML card block
    '''
    # The "disabled" badge stays a <span> (no bgcolor fallback, unlike
    # html_pill): it sits inline right after {label} on the same line, and
    # html_pill's table renders as a block, which would push it to its own
    # line. The badge text alone ("disabled") already conveys the state
    # without relying on its background color, so the degradation in Outlook
    # is cosmetic only.
    return (
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
        f'style="border:1px solid {EMAIL_BORDER};border-radius:8px;margin-bottom:14px;">'
        '<tr><td style="padding:12px 16px;">'
        f'<span style="font-weight:700;color:{EMAIL_NAVY};font-size:13.5px;">{label}</span> '
        f'<span style="background-color:{EMAIL_GRAY_BG};color:{EMAIL_GRAY};padding:2px 10px;'
        'border-radius:10px;font-size:11.5px;font-weight:600;">disabled</span>'
        f'<div style="color:{EMAIL_GRAY};font-size:12px;margin-top:6px;">{note}</div>'
        '</td></tr></table>')


def html_citation_increases():  # pylint: disable=too-many-locals
    ''' Build the Citation Increases table for the run-summary email from the
        module-level HITS list.
        Keyword arguments:
          None
        Returns:
          HTML block: a table of DOI / old->new / per-source breakdown, or a
          plain "no increases" message if HITS is empty
    '''
    if not HITS:
        return (f'<div style="color:{EMAIL_GRAY};font-size:13px;">'
                'No citation counts were increased.</div>')
    label = SOURCES[ARG.SOURCE]['label']
    rows = []
    for i, (doi, previous, native, combined, openalex, scholex, dcite, wos) in enumerate(HITS):
        oas = 'err' if openalex is None else f"{openalex:,}"
        sxs = 'err' if scholex is None else f"{scholex:,}"
        if ARG.SOURCE == 'crossref':
            extra = (f" &middot; WoS {'err' if wos is None else f'{wos:,}'}") if ARG.WOS else ""
        else:
            extra = " &middot; DataCite-GraphQL " \
                    + (('err' if dcite is None else f'{dcite:,}') if ARG.GRAPHQL else 'off')
        sources = f"{label} {native:,} &middot; OpenAlex {oas} &middot; ScholeXplorer {sxs}{extra}"
        striped = i % 2 == 0
        bgattr = f' bgcolor="{EMAIL_STRIPE_BG}"' if striped else ''
        bg = f'background-color:{EMAIL_STRIPE_BG};' if striped else ''
        r_l = 'border-radius:6px 0 0 6px;' if bg else ''
        r_r = 'border-radius:0 6px 6px 0;' if bg else ''
        rows.append(
            f'<tr{bgattr} style="{bg}"><td style="padding:8px 8px;{r_l}">{doiurl(doi)}</td>'
            f'<td style="padding:8px 8px;text-align:center;white-space:nowrap;">{previous:,} '
            f'<span style="color:#c9ced4;">&rarr;</span> '
            f'<b style="color:{EMAIL_GREEN};">{combined:,}</b></td>'
            f'<td style="padding:8px 8px;{r_r}color:{EMAIL_GRAY};">{sources}</td></tr>')
    return (
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
        'style="border-collapse:collapse;font-size:12.5px;">'
        f'<tr style="color:{EMAIL_GRAY};font-size:10.5px;text-transform:uppercase;'
        'letter-spacing:.03em;"><td style="padding:6px 8px;">DOI</td>'
        '<td style="padding:6px 8px;" align="center">Change</td>'
        '<td style="padding:6px 8px;">Sources</td></tr>'
        + "".join(rows) + '</table>')


def generate_email():  # pylint: disable=too-many-locals
    ''' Generate and send the HTML run-summary email. Built directly from the
        module-level COUNT/RECORDS/HITS/ARG state (same convention as the rest of
        this module) rather than re-parsing the plain-text console summary.
        Keyword arguments:
          None
        Returns:
          None
    '''
    label = PUBLISHERS[ARG.PUBLISHER]['label'] if ARG.PUBLISHER else SOURCES[ARG.SOURCE]['label']
    restrict = f' (--publisher {ARG.PUBLISHER})' if ARG.PUBLISHER else ''
    run_data = JRC.get_run_data(__file__, __version__).strip()
    mode_badge_bg = EMAIL_GREEN if ARG.WRITE else EMAIL_AMBER
    mode_label = 'WRITE' if ARG.WRITE else 'DRY RUN'

    kpis = ''.join([
        html_kpi_card(f"{COUNT['read']:,}", f"{label} DOIs processed"),
        html_kpi_card(f"{COUNT['openalex_found']:,}/{COUNT['openalex_error']:,}",
                      "OpenAlex OK/err", 'bad' if COUNT['openalex_error'] else 'good'),
        html_kpi_card(f"{COUNT['scholex_found']:,}/{COUNT['scholex_error']:,}",
                      "ScholeXplorer OK/err", 'bad' if COUNT['scholex_error'] else 'good'),
        html_kpi_card(f"{COUNT['fetch_error']:,}", "Fetch errors",
                      'bad' if COUNT['fetch_error'] else 'neutral'),
    ])

    citation_rows = [("OpenAlex unchanged (skipped)", f"{COUNT['openalex_unchanged']:,}"),
                     ("Counts preserved (source error)", f"{COUNT['preserved']:,}")]
    if ARG.SOURCE == 'crossref' and ARG.WOS:
        citation_rows.append(("WoS lookups OK (errors)",
                              f"{COUNT['wos_found']:,} ({COUNT['wos_error']:,})"))
    if ARG.SOURCE == 'datacite':
        dc_val = (f"{COUNT['datacite_found']:,} ({COUNT['datacite_error']:,})" if ARG.GRAPHQL
                  else "disabled (use --datacite-graphql)")
        citation_rows.append(("DataCite GraphQL", dc_val))
    cited = sum(1 for r in RECORDS if 'jrc_citation_count' in r)
    citation_rows.append(("DOIs with citation fields", f"{cited:,}"))
    citation_rows.append(("DOIs with increased citation count",
                          html_pill(EMAIL_GREEN_BG, EMAIL_GREEN, f'{COUNT["increased"]:,}')))
    citation_rows.append(("DOIs matched", f"{COUNT['matched']:,}"))
    citation_rows.append(("DOIs updated (errors)",
                          f"{COUNT['written']:,} ({COUNT['write_error']:,})"))
    citation_rows.append(("Total records written (set)", f"{len(RECORDS):,}"))
    citation_section = (html_section_header("&#128202; Citation Enrichment")
                        + html_metric_rows(citation_rows))

    usage_cards = []
    for key in SOURCE_PUBLISHERS[ARG.SOURCE]:
        # A --publisher restrict narrows the query to just that publisher's DOIs,
        # so a sibling source would always read 0/0 - just noise; omit its card
        # rather than show a fake zero.
        if ARG.PUBLISHER not in (None, key):
            continue
        pub = PUBLISHERS[key]
        if pub['token_env'] and not os.environ.get(pub['token_env']):
            usage_cards.append(html_usage_disabled_card(
                pub['label'], f"Set {pub['token_env']} to enable usage stats."))
            continue
        usage_cards.append(html_usage_card(pub['label'], COUNT[f'{key}_found'],
                                           COUNT[f'{key}_error'], pub['counters'], key))
    usage_section = html_section_header("&#128200; Usage Sources") + "".join(usage_cards)

    hits_section = (html_section_header(f"&#11014; Citation Increases ({len(HITS):,})")
                    + html_citation_increases())

    msg = (
        f'<div style="font-family:-apple-system,\'Segoe UI\',Roboto,Helvetica,Arial,sans-serif;'
        f'background-color:#eef1f4;padding:8px 0;">'
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0"><tr>'
        '<td align="center" style="padding:8px 14px 32px 14px;">'
        '<table role="presentation" width="720" cellpadding="0" cellspacing="0" '
        'bgcolor="#ffffff" '
        f'style="max-width:720px;width:100%;background-color:#ffffff;border-radius:10px;'
        f'border:1px solid {EMAIL_BORDER};overflow:hidden;">'
        f'<tr><td bgcolor="{EMAIL_NAVY}" style="background-color:{EMAIL_NAVY};'
        f'padding:22px 28px;">'
        f'<div style="color:#ffffff;font-size:19px;font-weight:600;">'
        f'{os.path.basename(__file__)}&nbsp;'
        f'<span style="font-weight:400;opacity:.7;font-size:14px;">v{__version__}</span></div>'
        f'<div style="color:#c9d6e6;font-size:12.5px;margin-top:6px;">{run_data} &middot; '
        f'{label} mode{restrict} &middot; '
        f'<span style="background-color:{mode_badge_bg};color:#fff;border-radius:10px;'
        f'padding:1px 9px;font-size:11px;font-weight:600;letter-spacing:.03em;">'
        f'{mode_label}</span> &middot; manifold: {ARG.MANIFOLD}</div></td></tr>'
        f'<tr><td style="padding:22px 22px 6px 22px;">'
        # cellspacing (not CSS margin, which <td> mostly ignores) puts a real gap
        # between the KPI tiles; Outlook's Word engine honors this old-school
        # HTML attribute far more reliably than CSS spacing tricks.
        f'<table role="presentation" width="100%" cellpadding="0" cellspacing="6"><tr>'
        f'{kpis}</tr></table></td></tr>'
        f'<tr><td style="padding:18px 28px 4px 28px;">{citation_section}</td></tr>'
        f'<tr><td style="padding:20px 28px 4px 28px;">{usage_section}</td></tr>'
        f'<tr><td style="padding:16px 28px 6px 28px;">{hits_section}</td></tr>'
        f'<tr><td bgcolor="{EMAIL_STRIPE_BG}" style="padding:18px 28px;'
        f'background-color:{EMAIL_STRIPE_BG};color:{EMAIL_GRAY};'
        f'font-size:11px;text-align:center;border-top:1px solid {EMAIL_BORDER};">'
        'Generated by sync_citations.py &middot; Data and Information Services &middot; '
        'Janelia Research Campus</td></tr>'
        '</table></td></tr></table></div>')

    try:
        if ARG.TEST:
            email = DISCONFIG['developer']
        elif ARG.SOURCE == 'datacite':
            email = DISCONFIG['dcreceivers']
        else:
            email = DISCONFIG['developer']
        LOGGER.info(f"Sending email to {email}")
        JRC.send_email(msg, DISCONFIG['sender'], email,
                       f"{label} citation enrichment", mime='html')
    except Exception as err:
        print(str(err))
        traceback.print_exc()
        terminate_program(err)


def processing():  # pylint: disable=too-many-locals,too-many-statements
    ''' Enrich citation counts for DataCite or Crossref DOIs (per --source)
        Keyword arguments:
          None
        Returns:
          None
    '''
    label = SOURCES[ARG.SOURCE]['label']
    query = {'jrc_obtained_from': label}
    if ARG.PUBLISHER and not ARG.DOI:
        # Narrow the --source subset to this publisher's DOIs (AND-ed with
        # jrc_obtained_from above - update(), not reassign, or that filter is lost).
        # Skipped when --doi targets one exact DOI: ANDing in a publisher match
        # there would find nothing if that DOI belongs to a different publisher,
        # instead of running against the one DOI the caller asked for.
        query.update(PUBLISHERS[ARG.PUBLISHER]['match'])
    if ARG.DOI:
        query['doi'] = ARG.DOI.lower()
    elif ARG.CITED:
        # In --publisher <x> mode "cited" means "already has usage data".
        if ARG.PUBLISHER:
            query[PUBLISHERS[ARG.PUBLISHER]['count_field']] = {'$exists': True}
        elif ARG.SOURCE == 'crossref':
            query['jrc_citation_count'] = {'$exists': True}
        else:
            query['citationCount'] = {'$gt': 0}
    elif ARG.NOTCITED:
        # DOIs not yet enriched; useful for a first pass / filling gaps without
        # reprocessing the done subset. --publisher <x> keys this on its usage
        # field (e.g. to retry deposits a transient error missed).
        if ARG.PUBLISHER:
            query[PUBLISHERS[ARG.PUBLISHER]['count_field']] = {'$exists': False}
        else:
            query['jrc_citation_count'] = {'$exists': False}
    try:
        # Only project the usage fields that apply under this --source; the
        # other source's fields are never read or written from this run.
        usage_fields = {PUBLISHERS[key]['count_field']: 1
                        for key in SOURCE_PUBLISHERS[ARG.SOURCE]}
        cursor = DB['dis'].dois.find(query, {'doi': 1, 'type': 1,
                                             SOURCES[ARG.SOURCE]['count_field']: 1,
                                             'jrc_citation_count': 1,
                                             'jrc_citation_sources': 1,
                                             'jrc_citing_dois': 1,
                                             **usage_fields})
        if ARG.LIMIT:
            cursor = cursor.limit(ARG.LIMIT)
        rows = list(cursor)
    except Exception as err:
        terminate_program(err)
    total = len(rows)
    kind = PUBLISHERS[ARG.PUBLISHER]['label'] if ARG.PUBLISHER else label
    LOGGER.info(f"Processing {total:,} {kind} DOIs "
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
    # Plain filenames for datacite runs; crossref and --publisher runs get a
    # qualifier suffix so they never overwrite the datacite output.
    filequal = f"_{ARG.PUBLISHER}" if ARG.PUBLISHER else \
               ('' if ARG.SOURCE == 'datacite' else f"_{ARG.SOURCE}")
    monfile = f"citation_updates{filequal}.txt"
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
    recfile = f"citation_records{filequal}.json"
    with open(recfile, 'w', encoding='utf-8') as fileout:
        json.dump(RECORDS, fileout, indent=2, default=str)
    LOGGER.info(f"Wrote {len(RECORDS):,} citation records to {recfile}")
    # RECORDS is the union of DOIs that got citation and/or usage-source fields
    cited = sum(1 for r in RECORDS if 'jrc_citation_count' in r)
    usage_counts = {key: sum(1 for r in RECORDS if f"jrc_{key}_counts" in r)
                    for key in SOURCE_PUBLISHERS[ARG.SOURCE]}
    # A --publisher restrict narrows the query to just that publisher's DOIs, so
    # a sibling source under the same --source would always read 0/0 - just
    # noise; omit it rather than report a fake zero.
    active_keys = [key for key in SOURCE_PUBLISHERS[ARG.SOURCE]
                   if ARG.PUBLISHER in (None, key)]
    usage_lines = "".join(usage_summary_line(key) for key in active_keys)
    field_lines = "".join(f"DOIs with {PUBLISHERS[key]['label']} fields:".ljust(36)
                          + f"{usage_counts[key]:,}\n" for key in active_keys)
    # DataCite GraphQL applies only to --source datacite; WoS (opt-in) only to
    # --source crossref.
    dc_line = wos_line = ""
    if ARG.SOURCE == 'datacite':
        dc_line = (f"DataCite GraphQL OK (errors):       {COUNT['datacite_found']:,} "
                   f"({COUNT['datacite_error']:,})\n") if ARG.GRAPHQL else \
                  "DataCite GraphQL:                   disabled (use --datacite-graphql)\n"
    elif ARG.WOS:
        wos_line = (f"WoS lookups OK (errors):            {COUNT['wos_found']:,} "
                    + f"({COUNT['wos_error']:,})\n")
    summary = (
        f"{label} DOIs processed:            {COUNT['read']:,}\n"
        + f"OpenAlex lookups OK (errors):       {COUNT['openalex_found']:,} "
        + f"({COUNT['openalex_error']:,})\n"
        + f"OpenAlex unchanged (skipped):       {COUNT['openalex_unchanged']:,}\n"
        + f"ScholeXplorer lookups OK (errors):  {COUNT['scholex_found']:,} "
        + f"({COUNT['scholex_error']:,})\n"
        + wos_line
        + dc_line
        + (usage_lines if ARG.SOURCE == 'crossref' else "")
        + f"Fetch errors (DOIs skipped):        {COUNT['fetch_error']:,}\n"
        + f"Counts preserved (source error):    {COUNT['preserved']:,}\n"
        + (usage_lines if ARG.SOURCE == 'datacite' else "")
        + f"DOIs with citation fields:          {cited:,}\n"
        + field_lines
        + f"Total records written (set):        {len(RECORDS):,}\n"
        + f"DOIs with increased citation count: {COUNT['increased']:,}\n"
        + f"DOIs matched:                       {COUNT['matched']:,}\n"
        + f"DOIs updated (errors):              {COUNT['written']:,} "
        + f"({COUNT['write_error']:,})")
    print(summary)
    if interrupted:
        LOGGER.warning("Run interrupted - summary email not sent")
    elif ARG.TEST or ARG.WRITE:
        generate_email()

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Enrich DataCite/Crossref DOI citation counts from OpenAlex "
                    + "and ScholeXplorer")
    PARSER.add_argument('--source', dest='SOURCE', action='store', type=str.lower,
                        default='crossref', choices=['datacite', 'crossref'],
                        help='DOI registrar source to process (default crossref, '
                             + 'case-insensitive)')
    PARSER.add_argument('--publisher', dest='PUBLISHER', action='store', type=str.lower,
                        default=None, choices=sorted(PUBLISHERS),
                        help='Restrict to one usage source\'s DOIs only (implies '
                             + '--source per that source: figshare/zenodo=datacite, '
                             + 'protocolsio/elife=crossref); --cited/--notcited then '
                             + 'key on its jrc_<publisher>_counts (usage) instead of '
                             + 'citation counts; protocolsio requires PROTOCOLS_API_TOKEN')
    PARSER.add_argument('--doi', dest='DOI', action='store', default=None,
                        help='Process a single DOI (for testing)')
    PARSER.add_argument('--cited', dest='CITED', action='store_true', default=False,
                        help='Only process DOIs already known to be cited '
                             + '(datacite: citationCount > 0; crossref: '
                             + 'jrc_citation_count exists; --publisher <x>: '
                             + 'jrc_<x>_counts exists)')
    PARSER.add_argument('--notcited', dest='NOTCITED', action='store_true', default=False,
                        help='Only process DOIs not yet enriched (no stored '
                             + 'jrc_citation_count; with --publisher <x>, no stored '
                             + 'jrc_<x>_counts)')
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
    PARSER.add_argument('--wos', dest='WOS', action='store_true',
                        default=False,
                        help='Include Web of Science citation counts (--source crossref only; '
                             + 'requires WOS_API_KEY; off by default - use for periodic deep passes)')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    if ARG.CITED and ARG.NOTCITED:
        terminate_program("--cited and --notcited are mutually exclusive")
    # A single choices-constrained --publisher value structurally rules out
    # selecting more than one restrict mode, so no separate exclusivity check
    # is needed (unlike the three standalone boolean flags this replaced).
    if ARG.PUBLISHER:
        pubcfg = PUBLISHERS[ARG.PUBLISHER]
        if ARG.SOURCE != pubcfg['source']:
            LOGGER.warning(f"--publisher {ARG.PUBLISHER} implies --source {pubcfg['source']} "
                           f"(overriding {ARG.SOURCE})")
        ARG.SOURCE = pubcfg['source']
        if pubcfg['token_env'] and not os.environ.get(pubcfg['token_env']):
            terminate_program(f"--publisher {ARG.PUBLISHER} requires {pubcfg['token_env']}")
    # Crossref runs deal only with jrc_citation_count/_sources/_updated
    if ARG.SOURCE == 'crossref' and ARG.GRAPHQL:
        terminate_program("--datacite-graphql applies only to --source datacite")
    if ARG.SOURCE == 'crossref' and ARG.CITING_DOIS:
        terminate_program("--citing-dois applies only to --source datacite")
    initialize_program()
    DISCONFIG = JRC.simplenamespace_to_dict(JRC.get_config("dis"))
    processing()
    terminate_program()
