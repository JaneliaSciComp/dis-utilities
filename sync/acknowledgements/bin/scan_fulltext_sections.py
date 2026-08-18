''' scan_fulltext_sections.py

PURPOSE
-------
Search scholarly full text for a term (default "Janelia") appearing ANYWHERE in an
article and, for every article that mentions it, report WHICH SECTION(S) the term
appears in (Title, Abstract, Author affiliations, the named body sections,
Acknowledgements, Funding, References, etc.). Results are written to a JSON file,
each record tagged with its source.

The engine walks a structured full-text document and attributes each occurrence to
its section - unlike pull_external_acks.py, which searches only the Acknowledgements
field and keeps just the ack text. The section is what separates real involvement
(author affiliation, acknowledgement, methods use) from a mere citation in the
reference list. After scanning, the tool also runs a read-only DIS-database
coverage check (flagging located papers whose author affiliations carry the term
but which are absent from the database, for human review), an acknowledgement-
coverage check (flagging located papers whose ACKNOWLEDGEMENTS mention the term but
which we hold no ack text for - absent from the DB, or present without a
jrc_acknowledgements field - written to <output>_missing_acks.json; with --write the
present-without-ack ones are backfilled into jrc_acknowledgements), and emails an
HTML run summary to the developer.

Beyond PMC, the same section machinery is fed by three other sources (each deduped
by DOI/PMCID against the earlier passes): Europe PMC (a superset of PMC that also
carries preprints/patents, served as the same JATS); Elsevier ScienceDirect
(--source elsevier/all), which reaches subscribed non-OA content invisible to PMC
by walking Elsevier's ce: full-text schema (full text is only returned for content
the running environment is ENTITLED to, and the Article Retrieval API is metered
by a weekly key quota, so bound broad runs with --max-results/--days); and bioRxiv
(--source biorxiv/all), which reaches preprints invisible to PMC/Europe PMC by
discovering full-text mentions via OpenAlex and fetching each preprint's free JATS
from its jatsxml URL - the same JATS walker reads it, no auth/S3/cost.

INPUTS
------
- NCBI_API_KEY environment variable (optional): raises the E-utilities rate limit.
  The tool runs without it, just more slowly.
- ELSEVIER_API_KEY environment variable (required only for --source elsevier/all):
  key for the Elsevier ScienceDirect search and Article Retrieval APIs.
- Command-line flags:
    --term         Term to search for (default: Janelia).
    --doi          Scan a single DOI instead of term-searching the sources. The DOI
                   is fetched directly by source (bioRxiv/medRxiv for 10.1101 or
                   10.64898, Elsevier for 10.1016 with a key, else Europe PMC by
                   DOI) and
                   section-scanned; the acknowledgement-coverage/backfill (--write)
                   checks then run on just that DOI. No run-summary email is sent in
                   this mode.
    --days         Restrict to articles added to PMC in the last N days (Entrez
                   date), Europe PMC creation date, or Elsevier publication date.
                   Omit to search all records.
    --max-results  Cap on articles retrieved and scanned per source. 0 (the
                   default) retrieves EVERY match. A positive value caps the total
                   for a quick look, and - importantly for Elsevier, whose Article
                   Retrieval API is quota-metered - bounds how many full-text
                   pulls a broad term triggers; --days narrows another way.
    --output       Output JSON file (default: fulltext_mentions.json).
    --flag-missing Path for the coverage-review file. The coverage check ALWAYS
                   runs: located papers carrying the term in their AUTHOR
                   AFFILIATIONS (i.e. likely authored by the org) but absent from
                   the DIS database are written here for human review. This is why
                   the tool always opens a read-only DIS database connection.
                   Default path: <output>_needs_review.json.
    --verify-sections  QA cross-check: re-fetch located articles from the NCBI
                   BioC API and compare its NLM-assigned section_type against our
                   heading->bin heuristic (see verify_sections). Adds no content -
                   BioC is retrieve-by-ID only - it just measures our labeling.
    --verify-limit Max located articles to cross-check against BioC; 0 = all [200].
    --write        Backfill jrc_acknowledgements (+ jrc_ack_source) for located
                   acknowledgements whose DOI is already in the database but holds
                   no ack text. Only these in-DB gaps are updated (never the
                   not-in-database ones); uses the dis.prod.write connection.
                   Without --write the tool is read-only.
    --verbose      Increase logging verbosity.
    --debug        Maximum logging verbosity.

HIGH-LEVEL FLOW
---------------
1. esearch PMC for the term across all indexed fields (server-side --days filter
   optional), paging through the full result set with retstart to collect every
   matching PMC ID (or the first --max-results, if a cap is given).
2. efetch the matching articles as JATS XML in batches, parsed with xmltodict.
3. For each article, walk its JATS regions (see iter_article_sections) and record
   every region whose flattened text contains the term, with an occurrence count
   and a surrounding-text snippet.
4. Write a JSON file of {doi, pmcid, sections:[{section, section_name, count,
   snippet}]} records - section is the normalized bin, section_name the paper's
   original heading - and print a per-section tally (how many articles mention
   the term in each section).
Steps 1-2 describe the PMC pass; Europe PMC, Elsevier, and bioRxiv discover and
fetch differently (see PURPOSE) but feed the same section walk of steps 3-4.

OUTPUT
------
A JSON file with the run parameters and one record per matching article. A mention
found only in References means the paper merely cites Janelia-affiliated work,
whereas one in Author affiliations or Acknowledgements indicates real involvement -
the section label is exactly what distinguishes these.

DEPENDENCIES
------------
- jrc_common.jrc_common (JRC): logging setup, the read-only DIS database
  connection (always opened, for the coverage check), and the HTML run-summary
  email (always sent to the configured developer address).
- doi_common.doi_common (DL): Elsevier Article Retrieval (--source elsevier/all).
- requests, xmltodict, xml.etree.ElementTree: PMC fetch and JATS parsing.
- tqdm: progress bar.
'''

__version__ = '1.9.1'

import argparse
import collections
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
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
import doi_common.doi_common as DL
import jrc_common.jrc_common as JRC
import jrc_email.jrc_email as JE

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

PMC_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
PMC_BATCH_SIZE = 200
# esearch caps a single call at retmax=10,000; paging past that with retstart is
# the documented way to retrieve more, so this is the per-page id-list size.
ESEARCH_PAGE_SIZE = 10000
# Europe PMC (EBI): a superset of PMC for full text (same JATS XML) plus non-PMC
# content (preprints, other publishers). Search is cursorMark-paginated; full text
# is one REST call per article. Deduped against the PMC pass by DOI/PMCID.
EPMC_REST = "https://www.ebi.ac.uk/europepmc/webservices/rest/"
EPMC_SEARCH = EPMC_REST + "search"
EPMC_PAGE_SIZE = 1000
# Elsevier ScienceDirect: full-text PUT search for discovery, then the Article
# Retrieval API (via doi_common) for section-structured full text. Reaches
# subscribed (entitled) non-OA content invisible to PMC/Europe PMC. NOTE: full
# text is returned only for content the running environment is ENTITLED to, and
# article retrieval is metered by a weekly key quota - use --max-results/--days.
SD_SEARCH_URL = "https://api.elsevier.com/content/search/sciencedirect"
ELSEVIER_PAGE_SIZE = 100
# Elsevier @role values that identify a back-matter section when it has no title.
ELSEVIER_ROLE_LABELS = {'acknowledgement': 'Acknowledgements',
                        'materials-methods': 'Methods'}
# bioRxiv: preprints are invisible to PMC/Europe PMC full text. The bioRxiv API
# has no full-text SEARCH, so discovery is done via OpenAlex full-text search
# (proven arXiv pattern from pull_external_acks.py; body mentions, not just
# abstract), then the free per-article jatsxml URL from the bioRxiv details API
# gives standard JATS the existing walker reads. No auth, no S3, no egress cost.
OPENALEX_API = "https://api.openalex.org/works"
OPENALEX_PAGE_SIZE = 200
OPENALEX_BIORXIV_SOURCE = "S4306402567"   # OpenAlex source id for bioRxiv
POLITE_HEADERS = {'User-Agent': 'janelia-dis/scan_fulltext_sections'}
BIORXIV_DETAILS = "https://api.biorxiv.org/details/biorxiv/"
# HTML run-summary email palette. House style shared with sync_citations.py /
# pull_external_acks.py: inline styles only (no <style> block/classes), colors
# paired with a label (not color alone) for colorblind accessibility, for
# reliable rendering across email clients including older Outlook.
EMAIL_NAVY = '#1f3a5f'
EMAIL_GREEN = '#1c7c3f'
EMAIL_GREEN_BG = '#eefaf1'
EMAIL_AMBER = '#d68a1f'
EMAIL_GRAY = '#5b6b7c'
EMAIL_STRIPE_BG = '#f7f9fb'
EMAIL_BORDER = '#eef1f4'
EMAIL_BLUE = '#2f7fd1'
# Upper bound (seconds) on how long a 429 Retry-After can make us wait.
RETRY_AFTER_CAP = 60
# Shared HTTP session so sequential calls reuse TCP/TLS connections.
SESSION = requests.Session()
# Section-label binning. Raw labels come from a JATS sec-type code or a free-text
# <title>, so the same section shows up many ways ("METHODS", "Methods:",
# "2. Materials and Methods", "STAR★METHODS"). canonical_section() normalizes
# a label to a lookup key (lowercase, leading section number and trailing
# punctuation stripped, separators collapsed) and maps known headings here to one
# display form; unknown headings keep their cleaned original text.
CANONICAL_SECTIONS = {
    'intro': 'Introduction',
    'introduction': 'Introduction',
    'method': 'Methods',
    'methods': 'Methods',
    'materials methods': 'Materials and methods',
    'materials and methods': 'Materials and methods',
    'material and methods': 'Materials and methods',
    'methods and materials': 'Materials and methods',
    'material and method': 'Materials and methods',
    'star methods': 'STAR Methods',
    # Methods-section synonyms / subsection headings (Nature "Online methods",
    # Cell-STAR "Method Details", Current Protocols "Experimental procedures").
    'online methods': 'Methods',
    'method details': 'Methods',
    'methods details': 'Methods',
    'experimental model details': 'Methods',
    'experimental procedures': 'Methods',
    'experimental procedure': 'Methods',
    'experimental methods': 'Methods',
    'experimental setup': 'Methods',
    'key resources table': 'Methods',
    'result': 'Results',
    'results': 'Results',
    'results discussion': 'Results and discussion',
    'results and discussion': 'Results and discussion',
    'discussion': 'Discussion',
    'conclusion': 'Conclusions',
    'conclusions': 'Conclusions',
    'experimental section': 'Experimental section',
    'supplementary material': 'Supplementary material',
    'references': 'References',
    'acknowledgement': 'Acknowledgements',
    'acknowledgements': 'Acknowledgements',
    'acknowledgment': 'Acknowledgements',
    'acknowledgments': 'Acknowledgements',
    'funding': 'Funding',
    'footnotes funding': 'Footnotes/funding',
    'notes': 'Notes',
    'glossary': 'Glossary',
    # Data/code/resource availability statements.
    'data availability': 'Data and code availability',
    'code availability': 'Data and code availability',
    'data and code availability': 'Data and code availability',
    'resource availability': 'Data and code availability',
    'abstract': 'Abstract',
    'title': 'Title',
    'author affiliations': 'Author affiliations',
}
# Strip a leading arabic section number ("2.", "4.", "2.1", "3)") from a title.
# Arabic only: roman-numeral stripping would wrongly eat headings like
# "C. elegans studies".
_SECTION_NUM_RE = re.compile(r'^\s*\d+(?:\.\d+)*[.):]?\s+')
# --- BioC cross-check (--verify-sections) ------------------------------------
# NCBI/NLM BioC API for PMC: retrieve-by-ID only (no search), returns the same
# PMC OA article pre-segmented into passages, each tagged with an NLM-assigned
# section_type. Used to sanity-check our heading->bin heuristic, NOT as a source
# (it can't discover anything the PMC search didn't already find).
BIOC_URL = ("https://www.ncbi.nlm.nih.gov/research/bionlp/RESTful/pmcoa.cgi/"
            "BioC_json/{}/unicode")
# BioC section_type -> coarse section family (the granularity where the two
# taxonomies actually agree). ACK_FUND merges acknowledgements+funding; FIG/TABLE
# are separate caption passages; unknown/paragraph-level types -> 'body/other'.
BIOC_FAMILY = {
    'TITLE': 'title', 'ABSTRACT': 'abstract', 'INTRO': 'introduction',
    'METHODS': 'methods', 'RESULTS': 'results', 'DISCUSS': 'discussion',
    'CONCL': 'conclusions', 'ACK_FUND': 'ack/funding', 'REF': 'references',
    'SUPPL': 'supplementary', 'FIG': 'figure', 'TABLE': 'table',
    'CASE': 'case', 'APPENDIX': 'appendix', 'COMP_INT': 'other',
    'AUTH_CONT': 'other', 'ABBR': 'other', 'KEYWORD': 'other',
    'REVIEW_INFO': 'other',
}
# Our canonical bin (CANONICAL_SECTIONS display value) -> the same family space.
OUR_FAMILY = {
    'Title': 'title', 'Abstract': 'abstract', 'Introduction': 'introduction',
    'Methods': 'methods', 'Materials and methods': 'methods',
    'STAR Methods': 'methods', 'Experimental section': 'methods',
    'Results': 'results', 'Results and discussion': 'results',
    'Discussion': 'discussion', 'Conclusions': 'conclusions',
    'Acknowledgements': 'ack/funding', 'Funding': 'ack/funding',
    'Footnotes/funding': 'ack/funding', 'References': 'references',
    'Supplementary material': 'supplementary',
    'Author affiliations': 'affiliations', 'Notes': 'other', 'Glossary': 'other',
    'Data and code availability': 'other',
}
# Families both taxonomies label the same way - agreement is scored on these.
# Excluded (structural differences, reported separately, not scored): figure,
# table, affiliations, case, appendix, other, body/other.
CORE_FAMILIES = frozenset({'title', 'abstract', 'introduction', 'methods',
                           'results', 'discussion', 'conclusions', 'ack/funding',
                           'references', 'supplementary'})
# Global variables
ARG = LOGGER = None
COUNT = collections.defaultdict(lambda: 0, {})
DB = {}   # DIS database handle, connected for the coverage check
DISCONFIG = {}   # DIS config (sender/developer addresses) for the run-summary email


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


def connect_db():
    ''' Connect to the DIS database. The coverage checks are read-only, so use
        dis.prod.read by default; with --write (which backfills jrc_acknowledgements
        for in-DB DOIs that lack it) connect to dis.prod.write instead - write creds
        also serve the read-only checks.
        Keyword arguments:
          None
        Returns:
          None
    '''
    manifold = "dis.prod.write" if ARG.WRITE else "dis.prod.read"
    try:
        dbconfig = JRC.get_config("databases")
        dbo = attrgetter(manifold)(dbconfig)
    except Exception as err:
        terminate_program(err)
    LOGGER.info(f"Connecting to {dbo.name} prod ({'write' if ARG.WRITE else 'read'}) "
                f"on {dbo.host} as {dbo.user}")
    try:
        DB['dis'] = JRC.connect_database(dbo)
    except Exception as err:
        terminate_program(err)


def _retry_after_seconds(value, default=15):
    ''' Parse a Retry-After header into a bounded number of seconds, clamped to
        RETRY_AFTER_CAP so an extreme value cannot stall the run.
        Keyword arguments:
          value: raw Retry-After header value (str or None)
          default: seconds to use when the header is absent or unparseable
        Returns:
          int seconds, clamped to [0, RETRY_AFTER_CAP]
    '''
    if value is None:
        seconds = default
    else:
        try:
            seconds = int(value)
        except (TypeError, ValueError):
            try:
                retry_dt = parsedate_to_datetime(value)
                seconds = (retry_dt - datetime.now(retry_dt.tzinfo)).total_seconds()
            except Exception:
                seconds = default
    return max(0, min(int(seconds), RETRY_AFTER_CAP))


# pylint: disable-next=too-many-arguments,too-many-positional-arguments
def _request_with_retry(method, url, params=None, retries=3, headers=None, json_body=None):
    ''' HTTP request with retry on rate-limit or server errors.
        Keyword arguments:
          method: HTTP method string
          url: request URL
          params: query parameters dict
          retries: maximum number of attempts
          headers: optional request headers dict
          json_body: optional JSON request body (for the Elsevier PUT search)
        Returns:
          requests.Response
    '''
    for attempt in range(retries):
        try:
            resp = SESSION.request(method, url, params=params, headers=headers,
                                   json=json_body, timeout=30)
        except requests.RequestException as err:
            LOGGER.warning(f"Request error (attempt {attempt + 1}): {err}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            continue
        if resp.status_code == 429:
            wait = _retry_after_seconds(resp.headers.get("Retry-After"))
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


def as_list(value):
    ''' Coerce an xmltodict value into a list (it yields a single dict for a lone
        element, a list for repeated elements).
        Keyword arguments:
          value: dict, list, or None
        Returns:
          list
    '''
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def flatten_text(node):
    ''' Recursively collect all text out of an xmltodict subtree into one string.
        JATS inline elements (xref, italic, sup, ...) split a paragraph's text
        across nested dicts/#text values; concatenating them yields searchable,
        readable text. Attribute keys (starting with '@') are skipped.
        Keyword arguments:
          node: str, dict, list, or None
        Returns:
          Space-joined text string (may be empty)
    '''
    parts = []

    def walk(item):
        if item is None:
            return
        if isinstance(item, str):
            parts.append(item)
        elif isinstance(item, dict):
            for key, val in item.items():
                if not key.startswith('@'):   # skip attributes, keep #text and children
                    walk(val)
        elif isinstance(item, list):
            for elem in item:
                walk(elem)
    walk(node)
    return ' '.join(part.strip() for part in parts if part and part.strip())


def _parse_pmc_ids(ids):
    ''' Extract DOI and PMCID from a PMC article-id list.
        Keyword arguments:
          ids: list of article-id dicts from xmltodict
        Returns:
          (doi, pmcid) tuple, either value may be None
    '''
    doi = pmcid = None
    for aid in ids:
        if not isinstance(aid, dict):
            continue
        if aid.get("@pub-id-type") == "doi":
            doi = (aid.get("#text") or "").lower()
        elif aid.get("@pub-id-type") == "pmcid":
            pmcid = aid.get("#text")
    return doi, pmcid


def section_label(sec):
    ''' Derive the raw section label for a JATS <sec> element: its <title> text
        if present, else its sec-type code. Normalization/binning of the many
        equivalent forms is done later by canonical_section().
        Keyword arguments:
          sec: a <sec> dict (or other) from xmltodict
        Returns:
          Raw section label string
    '''
    if isinstance(sec, dict):
        title = flatten_text(sec.get('title'))
        if title:
            return title.strip()
        if sec.get('@sec-type'):
            return sec['@sec-type']
    return 'Body (untitled section)'


def canonical_section(label):
    ''' Collapse trivially-different section labels into one bin. Lowercases,
        strips a leading arabic section number and trailing punctuation, and
        collapses non-alphanumeric separators (stars, slashes, "|", extra spaces)
        to single spaces to form a lookup key. Known headings map to a canonical
        display name via CANONICAL_SECTIONS; an unknown heading keeps its cleaned
        original text (casing preserved).
        Keyword arguments:
          label: raw section label
        Returns:
          Canonical display label
    '''
    cleaned = _SECTION_NUM_RE.sub('', label or '').strip(' \t.:;,')
    key = re.sub(r'[^a-z0-9]+', ' ', cleaned.lower()).strip()
    if key in CANONICAL_SECTIONS:
        return CANONICAL_SECTIONS[key]
    return cleaned or 'Body (untitled section)'


def iter_article_sections(article):
    ''' Yield (section_label, text) pairs for the searchable regions of a JATS
        article: title, abstract, author affiliations, front-matter funding, each
        top-level body <sec>, and back-matter ack/funding/notes/references.
        Keyword arguments:
          article: article dict from xmltodict
        Yields:
          (label, flattened_text) tuples
    '''
    front = article.get('front') or {}
    ameta = front.get('article-meta') or {} if isinstance(front, dict) else {}
    if isinstance(ameta, dict):
        title_group = ameta.get('title-group')
        if title_group:
            text = flatten_text(title_group.get('article-title')
                                if isinstance(title_group, dict) else title_group)
            if text:
                yield ('Title', text)
        if ameta.get('abstract'):
            yield ('Abstract', flatten_text(ameta['abstract']))
        # Author affiliations live in <aff> and/or inside <contrib-group>; combine
        # into one region so a byline mention isn't double-counted.
        aff_parts = [flatten_text(ameta[key]) for key in ('aff', 'contrib-group')
                     if ameta.get(key)]
        aff_text = ' '.join(part for part in aff_parts if part)
        if aff_text:
            yield ('Author affiliations', aff_text)
        if ameta.get('funding-group'):
            yield ('Funding', flatten_text(ameta['funding-group']))
    body = article.get('body') or {}
    if isinstance(body, dict):
        secs = as_list(body.get('sec'))
        for sec in secs:
            yield (section_label(sec), flatten_text(sec))
        # Some articles put body paragraphs directly under <body> with no <sec>.
        if body.get('p') and not secs:
            yield ('Body', flatten_text(body.get('p')))
    back = article.get('back') or {}
    if isinstance(back, dict):
        for key, label in (('ack', 'Acknowledgements'), ('fn-group', 'Footnotes/funding'),
                           ('funding-group', 'Funding'), ('notes', 'Notes'),
                           ('glossary', 'Glossary'), ('ref-list', 'References')):
            if back.get(key):
                yield (label, flatten_text(back[key]))


def snippet_for(text, term, width=60):
    ''' Return a short surrounding-text snippet for the first occurrence of term.
        Keyword arguments:
          text: full section text
          term: search term
          width: characters of context on each side
        Returns:
          Snippet string with ellipses where truncated
    '''
    idx = text.lower().find(term.lower())
    if idx < 0:
        return ""
    start = max(0, idx - width)
    end = min(len(text), idx + len(term) + width)
    snippet = text[start:end].strip()
    return ("..." if start > 0 else "") + snippet + ("..." if end < len(text) else "")


def article_ids(article):
    ''' Extract (doi, pmcid) from a JATS article's front-matter article-id list.
        Keyword arguments:
          article: article dict from xmltodict
        Returns:
          (doi, pmcid) tuple, either may be None
    '''
    ids = as_list(((article.get('front') or {}).get('article-meta') or {}).get('article-id'))
    return _parse_pmc_ids(ids)


def scan_article(article, term):
    ''' Find which JATS sections of an article contain the term.
        Keyword arguments:
          article: article dict from xmltodict
          term: search term
        Returns:
          Result dict {doi, pmcid, sections:[{section, section_name, count,
          snippet}]}, or None if the term appears in no section
    '''
    doi, pmcid = article_ids(article)
    term_l = term.lower()
    # Keyed by canonical bin; each entry carries both the bin (section) and the
    # paper's original heading (section_name, the first raw label seen for the bin).
    found = {}
    for label, text in iter_article_sections(article):
        if not text:
            continue
        occurrences = text.lower().count(term_l)
        if not occurrences:
            continue
        canonical = canonical_section(label)
        entry = found.setdefault(canonical, {"section": canonical, "section_name": label,
                                             "count": 0, "snippet": ""})
        entry["count"] += occurrences
        if not entry["snippet"]:
            entry["snippet"] = snippet_for(text, term)
        # Retain the FULL Acknowledgements text (not just the snippet) so the
        # missing-acks review can carry the complete acknowledgement. write_output
        # strips this from the main output, keeping it snippet-only.
        if canonical == 'Acknowledgements':
            entry["text"] = text
    if not found:
        return None
    return {"doi": doi or "n/a", "pmcid": pmcid, "sections": list(found.values())}


def search_pmc_ids(term, max_results, api_key, days):
    ''' esearch PubMed Central for the term (all indexed fields), paging through
        the full result set with retstart so there is no single-call retmax cap.
        Keyword arguments:
          term: search term
          max_results: cap on total IDs returned; 0 (or falsy) retrieves ALL matches
          api_key: NCBI API key or None
          days: restrict to the last N days (Entrez date) or None
        Returns:
          (list of PMC IDs, total match count)
    '''
    base_params = {"db": "pmc", "term": term, "retmode": "json"}
    if api_key:
        base_params["api_key"] = api_key
    if days:
        base_params["datetype"] = "edat"
        base_params["reldate"] = days
        LOGGER.info(f"Searching PMC for '{term}' (all fields, last {days} days)")
    else:
        LOGGER.info(f"Searching PMC for '{term}' (all fields)")
    pmids = []
    total = None
    retstart = 0
    while True:
        page = ESEARCH_PAGE_SIZE
        if max_results:
            remaining = max_results - len(pmids)
            if remaining <= 0:
                break
            page = min(page, remaining)
        params = dict(base_params, retstart=retstart, retmax=page)
        try:
            resp = _request_with_retry('GET', f"{PMC_BASE}esearch.fcgi", params=params)
            result = resp.json().get("esearchresult", {})
        except Exception as err:
            LOGGER.warning(f"PMC search error at retstart={retstart:,}: {err}")
            break
        if total is None:
            total = int(result.get("count", 0))
            target = min(total, max_results) if max_results else total
            LOGGER.info(f"{total:,} PMC articles matched; retrieving {target:,}"
                        + (f" (--max-results={max_results:,})" if max_results else " (all)"))
        batch = result.get("idlist", [])
        if not batch:
            break
        pmids.extend(batch)
        retstart += len(batch)
        if retstart >= total or (max_results and len(pmids) >= max_results):
            break
        time.sleep(0.1 if api_key else 0.34)   # pace esearch pages
    if max_results and total and total > max_results:
        LOGGER.warning(f"Capped at --max-results={max_results:,}: {total - max_results:,} "
                       f"of {total:,} matching articles not scanned (use --max-results 0 "
                       f"for all, or --days to narrow)")
    # De-duplicate defensively while preserving order (retstart paging can
    # occasionally repeat an id if the index shifts between pages).
    seen = set()
    unique = [p for p in pmids if not (p in seen or seen.add(p))]
    return unique, (total or 0)


def fetch_pmc_batch(batch_pmids, api_key):
    ''' efetch and parse one batch of PMC articles as JATS XML.
        Keyword arguments:
          batch_pmids: list of PMC IDs
          api_key: NCBI API key or None
        Returns:
          List of article dicts from xmltodict
    '''
    params = {"db": "pmc", "id": ",".join(batch_pmids), "retmode": "xml"}
    if api_key:
        params["api_key"] = api_key
    resp = _request_with_retry('GET', f"{PMC_BASE}efetch.fcgi", params=params)
    root = ET.fromstring(resp.content)
    root_json = xmltodict.parse(ET.tostring(root))
    article = root_json.get("pmc-articleset", {}).get("article", [])
    return article if isinstance(article, list) else [article]


def merge_section_casing(records):
    ''' Case-insensitively merge section labels that differ only by casing across
        all records (e.g. "Method Details" and "METHOD DETAILS"), choosing one
        display per case-folded label. Known headings already share a canonical
        display via CANONICAL_SECTIONS; this catches the free-text headings that
        fall outside that map. Preference order for the surviving display: an
        already-canonical value, then a mixed-case value (preserves acronyms like
        "RNA"), then the highest total count. Per-article entries that collapse to
        one display are merged (counts summed, first snippet kept).
        Keyword arguments:
          records: list of per-article result dicts (mutated in place)
        Returns:
          None
    '''
    canon_values = set(CANONICAL_SECTIONS.values())
    variants = collections.defaultdict(collections.Counter)
    for rec in records:
        for sec in rec['sections']:
            variants[sec['section'].lower()][sec['section']] += sec['count']
    chosen = {}
    for key, counter in variants.items():
        chosen[key] = max(counter, key=lambda v, c=counter: (v in canon_values,
                                                             not (v.isupper() or v.islower()),
                                                             c[v]))
    for rec in records:
        merged = {}
        for sec in rec['sections']:
            display = chosen[sec['section'].lower()]
            if display in merged:
                merged[display]['count'] += sec['count']
                if not merged[display].get('snippet') and sec.get('snippet'):
                    merged[display]['snippet'] = sec['snippet']
            else:
                entry = dict(sec)
                entry['section'] = display
                merged[display] = entry
        rec['sections'] = list(merged.values())


def seen_add(seen, doi, pmcid):
    ''' Register a DOI and/or PMCID as already-covered, for cross-pass dedup. '''
    if doi and doi != 'n/a':
        seen.add(doi.lower())
    if pmcid:
        seen.add(pmcid.upper())


def seen_has(seen, doi, pmcid):
    ''' True if this DOI or PMCID was already covered by an earlier pass. '''
    return bool((doi and doi.lower() in seen) or (pmcid and pmcid.upper() in seen))


def scan_pmc(api_key, seen, records):
    ''' PMC pass: esearch/efetch PubMed Central full text (JATS) and scan each
        article for the term by section. Appends matched records (tagged
        source="PMC") to `records`, and registers every fetched article's ids in
        `seen` so the Europe PMC pass can skip the shared PMC core.
        Keyword arguments:
          api_key: NCBI API key or None
          seen: cross-pass dedup set (mutated)
          records: result list (mutated)
        Returns:
          None
    '''
    pmids, total = search_pmc_ids(ARG.TERM, ARG.MAX_RESULTS, api_key, ARG.DAYS)
    COUNT['pmc_matched'] = total
    for i in tqdm(range(0, len(pmids), PMC_BATCH_SIZE), desc="Fetching/scanning PMC"):
        batch = pmids[i:i + PMC_BATCH_SIZE]
        if i > 0:
            time.sleep(0.1 if api_key else 0.34)
        try:
            articles = fetch_pmc_batch(batch, api_key)
        except Exception as err:
            LOGGER.warning(f"PMC fetch error on batch starting at {i}: {err}")
            continue
        for article in articles:
            COUNT['pmc_scanned'] += 1
            try:
                doi, pmcid = article_ids(article)
                seen_add(seen, doi, pmcid)
                rec = scan_article(article, ARG.TERM)
            except Exception as err:
                COUNT['pmc_parse_error'] += 1
                LOGGER.debug(f"PMC scan error: {err}")
                continue
            if rec:
                rec['source'] = 'PMC'
                COUNT['pmc_located'] += 1
                records.append(rec)


def search_epmc(term, max_results, days):
    ''' Search Europe PMC for the term, restricted to records with full text in
        Europe PMC (IN_EPMC:Y), paging via cursorMark.
        Keyword arguments:
          term: search term
          max_results: cap on hits returned; 0 = all
          days: restrict to records created in the last N days, or None
        Returns:
          (list of hit dicts {source, id, pmcid, doi}, total hit count)
    '''
    # OPEN_ACCESS:Y, not IN_EPMC:Y - only the open-access subset is served as
    # downloadable JATS via fullTextXML; IN_EPMC:Y also matches read-only and
    # preprint full text that 404s on that endpoint.
    query = f'"{term}" AND (OPEN_ACCESS:Y)'
    if days:
        cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        today = datetime.now().strftime('%Y-%m-%d')
        query += f' AND (CREATION_DATE:[{cutoff} TO {today}])'
        LOGGER.info(f"Searching Europe PMC for '{term}' (open access, last {days} days)")
    else:
        LOGGER.info(f"Searching Europe PMC for '{term}' (open access)")
    hits = []
    total = None
    cursor = '*'
    while True:
        page = EPMC_PAGE_SIZE
        if max_results:
            remaining = max_results - len(hits)
            if remaining <= 0:
                break
            page = min(page, remaining)
        params = {"query": query, "format": "json", "pageSize": page,
                  "cursorMark": cursor, "resultType": "lite"}
        try:
            data = _request_with_retry('GET', EPMC_SEARCH, params=params).json()
        except Exception as err:
            LOGGER.warning(f"Europe PMC search error: {err}")
            break
        if total is None:
            total = int(data.get("hitCount", 0))
            target = min(total, max_results) if max_results else total
            LOGGER.info(f"{total:,} Europe PMC open-access articles matched; "
                        f"retrieving up to {target:,}")
        results = data.get("resultList", {}).get("result", [])
        if not results:
            break
        for res in results:
            hits.append({"source": res.get("source"), "id": res.get("id"),
                         "pmcid": res.get("pmcid"), "doi": res.get("doi")})
        next_cursor = data.get("nextCursorMark")
        if not next_cursor or next_cursor == cursor or len(hits) >= (total or 0):
            break
        cursor = next_cursor
        if max_results and len(hits) >= max_results:
            break
        time.sleep(0.2)
    return hits, (total or 0)


def fetch_epmc_fulltext(pmcid):
    ''' Fetch one Europe PMC article as JATS XML (by PMCID) and return its
        <article> dict. Europe PMC only serves fullTextXML for PMCID-bearing OA
        articles - preprints (PPR) and non-OA records 404 - and returns a bare
        JATS <article> (no pmc-articleset wrapper), so the parsed 'article' key
        is returned directly.
        Keyword arguments:
          pmcid: the article's PMCID
        Returns:
          article dict from xmltodict, or None if the body is missing
    '''
    resp = _request_with_retry('GET', f"{EPMC_REST}{pmcid}/fullTextXML")
    return xmltodict.parse(resp.content).get('article')


def _epmc_pmcid_for_doi(doi):
    ''' Look up a DOI's PMCID in the Europe PMC open-access subset (fullTextXML is
        served only for PMCID-bearing OA records). Used by the single-DOI (--doi)
        path to reach the PMC core and non-PMC open access by DOI.
        Keyword arguments:
          doi: DOI to look up (lower-case)
        Returns:
          PMCID string, or None if the DOI is not in the EPMC OA subset
    '''
    params = {"query": f'DOI:"{doi}" AND OPEN_ACCESS:Y', "format": "json",
              "resultType": "lite", "pageSize": 1}
    try:
        hits = _request_with_retry('GET', EPMC_SEARCH, params=params).json() \
                   .get("resultList", {}).get("result", [])
    except Exception as err:
        LOGGER.debug(f"Europe PMC DOI lookup error for {doi}: {err}")
        return None
    return hits[0].get("pmcid") if hits else None


def scan_europepmc(seen, records):
    ''' Europe PMC pass: search, then for each hit NOT already covered by the PMC
        pass, fetch its full-text JATS and scan by section. Appends matched
        records (tagged source="Europe PMC") to `records`. Deduping against `seen`
        before fetching avoids re-pulling the shared PMC core.
        Keyword arguments:
          seen: cross-pass dedup set (mutated)
          records: result list (mutated)
        Returns:
          None
    '''
    hits, total = search_epmc(ARG.TERM, ARG.MAX_RESULTS, ARG.DAYS)
    COUNT['epmc_matched'] = total
    for hit in tqdm(hits, desc="Fetching/scanning Europe PMC"):
        doi = hit.get('doi')
        pmcid = hit.get('pmcid')
        if seen_has(seen, doi, pmcid):
            COUNT['epmc_shared_with_pmc'] += 1
            continue
        if not pmcid:
            # fullTextXML is only served for PMCID-bearing OA articles; preprints
            # (PPR) and other no-PMCID records aren't fetchable as JATS here.
            COUNT['epmc_no_pmcid'] += 1
            continue
        try:
            article = fetch_epmc_fulltext(pmcid)
        except Exception as err:
            COUNT['epmc_fetch_error'] += 1
            LOGGER.debug(f"Europe PMC fetch error for {hit.get('id')}: {err}")
            continue
        if not article:
            COUNT['epmc_no_fulltext'] += 1
            continue
        COUNT['epmc_scanned'] += 1   # count only articles actually fetched + scanned
        seen_add(seen, doi, pmcid)
        try:
            rec = scan_article(article, ARG.TERM)
        except Exception as err:
            COUNT['epmc_parse_error'] += 1
            LOGGER.debug(f"Europe PMC scan error for {hit.get('id')}: {err}")
            continue
        if rec:
            if rec['doi'] == 'n/a' and doi:
                rec['doi'] = doi.lower()
            if not rec['pmcid'] and pmcid:
                rec['pmcid'] = pmcid
            rec['source'] = 'Europe PMC'
            COUNT['epmc_located'] += 1
            records.append(rec)
        time.sleep(0.15)


def write_output(records):
    ''' Write the scan results to the output JSON file. The full Acknowledgements
        'text' captured on section entries is for the missing-acks review only, so
        it is stripped here (a filtered copy) to keep the main output snippet-only;
        the records themselves keep it for flag_missing_acks, which runs later.
        Keyword arguments:
          records: list of per-article result dicts
        Returns:
          None
    '''
    clean = [{**rec, "sections": [{k: v for k, v in sec.items() if k != "text"}
                                  for sec in rec["sections"]]} for rec in records]
    payload = {"term": ARG.TERM, "sources": ARG.SOURCE,
               "generated": datetime.now().isoformat(timespec='seconds'),
               "matched": len(records),
               "records": clean}
    with open(ARG.OUTPUT, 'w', encoding='utf-8') as stream:
        json.dump(payload, stream, indent=2, ensure_ascii=False)
    LOGGER.info(f"Wrote {len(records):,} records to {ARG.OUTPUT}")


def _summary_row(label, value, note=""):
    ''' Print one aligned two-column summary line: a left-justified label, a
        right-justified (thousands-separated) value, and an optional trailing note.
        Keyword arguments:
          label: left-column text
          value: integer count for the right column
          note: optional trailing annotation
        Returns:
          None
    '''
    line = f"{label:<42}{value:>9,}"
    if note:
        line += f"  {note}"
    print(line)


def report(records):
    ''' Print a per-source run summary and a combined per-section tally.
        Keyword arguments:
          records: list of per-article result dicts
        Returns:
          None
    '''
    print("\n=== Scan summary ===")
    if ARG.SOURCE in ('pmc', 'both', 'all'):
        _summary_row("PMC matched (search):", COUNT['pmc_matched'])
        _summary_row("PMC scanned:", COUNT['pmc_scanned'])
        _summary_row(f"PMC located '{ARG.TERM}':", COUNT['pmc_located'])
        if COUNT['pmc_parse_error']:
            _summary_row("PMC scan errors:", COUNT['pmc_parse_error'])
    if ARG.SOURCE in ('europepmc', 'both', 'all'):
        _summary_row("Europe PMC matched (search):", COUNT['epmc_matched'])
        _summary_row("Europe PMC shared with PMC:", COUNT['epmc_shared_with_pmc'], "(skipped)")
        if COUNT['epmc_no_pmcid']:
            _summary_row("Europe PMC no downloadable XML:", COUNT['epmc_no_pmcid'],
                         "(preprints/no PMCID, skipped)")
        _summary_row("Europe PMC scanned (new):", COUNT['epmc_scanned'])
        _summary_row(f"Europe PMC located '{ARG.TERM}':", COUNT['epmc_located'])
        if COUNT['epmc_no_fulltext'] or COUNT['epmc_fetch_error']:
            _summary_row("Europe PMC no full text:", COUNT['epmc_no_fulltext'])
            _summary_row("Europe PMC fetch errors:", COUNT['epmc_fetch_error'])
    if ARG.SOURCE in ('elsevier', 'all'):
        _summary_row("Elsevier matched (search):", COUNT['elsevier_matched'])
        _summary_row("Elsevier shared (earlier pass):", COUNT['elsevier_shared'], "(skipped)")
        if COUNT['elsevier_date_filtered']:
            _summary_row("Elsevier date filtered:", COUNT['elsevier_date_filtered'])
        _summary_row("Elsevier no full text (unentitled/none):", COUNT['elsevier_no_fulltext'])
        _summary_row("Elsevier scanned (new):", COUNT['elsevier_scanned'])
        _summary_row(f"Elsevier located '{ARG.TERM}':", COUNT['elsevier_located'])
    if ARG.SOURCE in ('biorxiv', 'all'):
        _summary_row("bioRxiv matched (OpenAlex full text):", COUNT['biorxiv_matched'])
        _summary_row("bioRxiv shared (earlier pass):", COUNT['biorxiv_shared'], "(skipped)")
        _summary_row("bioRxiv no full text:", COUNT['biorxiv_no_fulltext'])
        _summary_row("bioRxiv scanned (new):", COUNT['biorxiv_scanned'])
        _summary_row(f"bioRxiv located '{ARG.TERM}':", COUNT['biorxiv_located'])
        if COUNT['biorxiv_parse_error']:
            _summary_row("bioRxiv scan errors:", COUNT['biorxiv_parse_error'])
    _summary_row("Total articles located:", len(records))
    tally = collections.Counter()
    for rec in records:
        for sec in rec['sections']:
            tally[sec['section']] += 1
    if tally:
        print(f"\nArticles mentioning '{ARG.TERM}' by section:")
        print(f"{'Section':<32} {'Articles':>8}")
        print("-" * 42)
        for section, cnt in tally.most_common():
            print(f"  {section:<30} {cnt:>8,}")
    print(f"\nResults written to {ARG.OUTPUT}")


def _our_family(bin_label):
    ''' Map one of our canonical section bins to a coarse section family.
        Keyword arguments:
          bin_label: canonical bin (a CANONICAL_SECTIONS display value, or a
                     cleaned raw heading for sections we couldn't name)
        Returns:
          Family string; unrecognized/untitled bins -> 'body/other'
    '''
    return OUR_FAMILY.get(bin_label, 'body/other')


def _bioc_family(section_type):
    ''' Map a BioC section_type to the same coarse section family.
        Keyword arguments:
          section_type: BioC passage section_type infon
        Returns:
          Family string; unrecognized/paragraph-level types -> 'body/other'
    '''
    return BIOC_FAMILY.get((section_type or '').upper(), 'body/other')


def fetch_bioc_families(pmcid, term):
    ''' Fetch one article from the BioC API and return the set of section
        families whose passages contain the term (case-insensitive).
        Keyword arguments:
          pmcid: article PMCID (with or without the 'PMC' prefix)
          term: search term
        Returns:
          set of family strings, or None if the article isn't retrievable/parseable
    '''
    pmcid = pmcid if str(pmcid).upper().startswith('PMC') else f"PMC{pmcid}"
    try:
        data = _request_with_retry('GET', BIOC_URL.format(pmcid)).json()
        # BioC may return an empty list, a bare error string, or the collection
        # object - only a dict carrying documents is usable; guard the rest.
        coll = data[0] if isinstance(data, list) and data else data
        docs = (coll.get('documents') if isinstance(coll, dict) else None) or []
    except Exception as err:
        LOGGER.debug(f"BioC fetch error for {pmcid}: {err}")
        return None
    if not docs or not isinstance(docs[0], dict):
        return None
    term_l = term.lower()
    fams = set()
    for passage in docs[0].get('passages', []):
        if term_l in (passage.get('text') or '').lower():
            fams.add(_bioc_family(passage.get('infons', {}).get('section_type')))
    return fams


def verify_sections(records, api_key):
    ''' Cross-check our heading->bin section attribution against BioC's
        NLM-assigned section_type for the same PMC articles. Per article, compares
        the SET of core section families in which the term appears - the two
        taxonomies are only comparable at family granularity. Structural
        differences (figure/table captions, affiliations, which the two segment
        differently) are tallied separately, not scored as disagreements. BioC is
        retrieve-by-ID only, so this adds no content; it only measures whether our
        heading heuristic mislabels often enough to justify adopting BioC labels.
        Keyword arguments:
          records: located result dicts (only those with a PMCID are checked)
          api_key: NCBI API key or None (governs request pacing)
        Returns:
          None (prints a report and writes a per-article disagreement file)
    '''
    candidates = [rec for rec in records if rec.get('pmcid')]
    if ARG.VERIFY_LIMIT:
        candidates = candidates[:ARG.VERIFY_LIMIT]
    if not candidates:
        LOGGER.warning("Nothing to verify: no located records carry a PMCID")
        return
    LOGGER.info(f"Cross-checking {len(candidates):,} article(s) against BioC")
    exact = partial = disjoint = unavailable = no_term = 0
    bioc_only = collections.Counter()   # families BioC labels that we don't
    ours_only = collections.Counter()   # families we label that BioC doesn't
    detail = []
    for i, rec in enumerate(tqdm(candidates, desc="Verifying vs BioC")):
        if i > 0:
            time.sleep(0.1 if api_key else 0.34)
        bioc_all = fetch_bioc_families(rec['pmcid'], ARG.TERM)
        if bioc_all is None:
            unavailable += 1
            continue
        if not bioc_all:
            no_term += 1
            continue
        ours_all = {_our_family(sec['section']) for sec in rec['sections']}
        for fam in bioc_all - ours_all:
            bioc_only[fam] += 1
        for fam in ours_all - bioc_all:
            ours_only[fam] += 1
        # Agreement is scored on the comparable core families only.
        ours_core = ours_all & CORE_FAMILIES
        bioc_core = bioc_all & CORE_FAMILIES
        if ours_core == bioc_core:
            exact += 1
            verdict = 'exact'
        elif ours_core & bioc_core:
            partial += 1
            verdict = 'partial'
        else:
            disjoint += 1
            verdict = 'disjoint'
        if verdict != 'exact':
            detail.append({'pmcid': rec['pmcid'], 'doi': rec.get('doi'),
                           'verdict': verdict, 'ours': sorted(ours_all),
                           'bioc': sorted(bioc_all)})
    compared = exact + partial + disjoint
    print("\n=== BioC section cross-check ===")
    print(f"Articles selected:             {len(candidates):,}")
    print(f"  BioC unavailable:            {unavailable:,} (not in BioC OA / fetch failed)")
    print(f"  BioC found no term:          {no_term:,}")
    print(f"Compared (both found term):    {compared:,}")
    if compared:
        print("Core-family agreement (title/abstract/intro/methods/results/"
              "discussion/conclusions/ack+funding/refs/suppl):")
        print(f"  Exact set match:             {exact:,} ({exact / compared:.0%})")
        print(f"  Partial overlap:             {partial:,} ({partial / compared:.0%})")
        print(f"  Disjoint:                    {disjoint:,} ({disjoint / compared:.0%})")
    if bioc_only:
        print("Families BioC labels that we bin elsewhere/miss (top):")
        for fam, cnt in bioc_only.most_common(10):
            print(f"  {fam:<16} {cnt:>6,}")
    if ours_only:
        print("Families we label that BioC doesn't (top):")
        for fam, cnt in ours_only.most_common(10):
            print(f"  {fam:<16} {cnt:>6,}")
    if detail:
        outfile = ARG.OUTPUT.rsplit('.', 1)[0] + '_section_verify.json'
        with open(outfile, 'w', encoding='utf-8') as stream:
            json.dump(detail, stream, indent=2, ensure_ascii=False)
        print(f"\nPer-article disagreements written to {outfile}")


def search_elsevier(term, max_results):
    ''' Yield Elsevier ScienceDirect full-text search result dicts for the term,
        paged via offset (each result carries a 'doi').
        Keyword arguments:
          term: search term
          max_results: cap on results yielded; 0 = all
        Yields:
          result dicts
    '''
    headers = {"X-ELS-APIKey": os.environ['ELSEVIER_API_KEY'],
               "Accept": "application/json", "Content-Type": "application/json"}
    offset = 0
    total = None
    yielded = 0
    while total is None or offset < total:
        show = ELSEVIER_PAGE_SIZE
        if max_results:
            show = min(show, max_results - yielded)
            if show <= 0:
                return
        body = {"qs": f'"{term}"',
                "display": {"offset": offset, "show": show, "sortBy": "date"}}
        try:
            data = _request_with_retry('PUT', SD_SEARCH_URL, headers=headers,
                                       json_body=body).json()
        except Exception as err:
            LOGGER.warning(f"Elsevier search error at offset {offset}: {err}")
            return
        if total is None:
            total = int(data.get("resultsFound", 0))
            COUNT['elsevier_matched'] = total
            LOGGER.info(f"Elsevier search found {total:,} results for '{term}'")
        results = data.get("results", [])
        if not results:
            return
        for result in results:
            yield result
            yielded += 1
            if max_results and yielded >= max_results:
                return
        offset += len(results)
        if offset < total:
            time.sleep(0.3)


def _elsevier_section_label(sec):
    ''' Derive a raw section label for an Elsevier ce:section: its ce:section-title
        text if present, else a label inferred from its @role, else untitled.
        Keyword arguments:
          sec: a ce:section dict
        Returns:
          Raw section label string
    '''
    if isinstance(sec, dict):
        title = flatten_text(sec.get('ce:section-title'))
        if title:
            return title.strip()
        if sec.get('@role') in ELSEVIER_ROLE_LABELS:
            return ELSEVIER_ROLE_LABELS[sec['@role']]
    return 'Body (untitled section)'


def iter_elsevier_sections(article):
    ''' Yield (section_label, text) pairs for the searchable regions of an
        Elsevier full-text article (ce: schema): title, abstract, author
        affiliations, each top-level body ce:section, and back-matter
        acknowledgements/references. flatten_text collects nested text, so a
        section with subsections yields as one unit (matching the JATS walker).
        Keyword arguments:
          article: the 'article' dict from the Elsevier full-text response
        Yields:
          (label, flattened_text) tuples
    '''
    head = article.get('head') or {}
    if isinstance(head, dict):
        if head.get('ce:title'):
            yield ('Title', flatten_text(head['ce:title']))
        if head.get('ce:abstract'):
            yield ('Abstract', flatten_text(head['ce:abstract']))
        # ce:author-group holds author names and their ce:affiliation blocks.
        if head.get('ce:author-group'):
            yield ('Author affiliations', flatten_text(head['ce:author-group']))
    body = article.get('body') or {}
    if isinstance(body, dict) and isinstance(body.get('ce:sections'), dict):
        for sec in as_list(body['ce:sections'].get('ce:section')):
            yield (_elsevier_section_label(sec), flatten_text(sec))
    tail = article.get('tail') or {}
    if isinstance(tail, dict):
        if tail.get('ce:acknowledgment'):
            yield ('Acknowledgements', flatten_text(tail['ce:acknowledgment']))
        if tail.get('ce:bibliography'):
            yield ('References', flatten_text(tail['ce:bibliography']))


def fetch_elsevier_article(doi):
    ''' Retrieve one Elsevier article's section-structured full text via the
        Article Retrieval API (doi_common) and return the inner 'article' dict,
        or None if no full text is available (not entitled / not full-text).
        Keyword arguments:
          doi: article DOI
        Returns:
          article dict, or None
    '''
    try:
        rec = DL.get_doi_record(doi, source='elsevier')
    except Exception as err:
        LOGGER.debug(f"Elsevier fetch error for {doi}: {err}")
        return None
    otext = ((rec or {}).get('full-text-retrieval-response') or {}).get('originalText') or {}
    if not isinstance(otext, dict) or not otext:
        return None
    serial = (otext.get('xocs:doc', {}) or {}).get('xocs:serial-item', {}) or {}
    article = serial.get('article', {}) if isinstance(serial, dict) else {}
    return article or None


def scan_elsevier_article(article):
    ''' Find which Elsevier ce: sections of an article contain the term. Mirrors
        scan_article() but walks iter_elsevier_sections (Elsevier's ce: schema, not
        JATS); the caller fills in doi/source.
        Keyword arguments:
          article: Elsevier article dict (from fetch_elsevier_article)
        Returns:
          Result dict {doi:'n/a', pmcid:None, sections:[...]} or None if the term
          appears in no section
    '''
    term_l = ARG.TERM.lower()
    found = {}
    for label, text in iter_elsevier_sections(article):
        if not text:
            continue
        occurrences = text.lower().count(term_l)
        if not occurrences:
            continue
        canonical = canonical_section(label)
        entry = found.setdefault(canonical, {"section": canonical, "section_name": label,
                                             "count": 0, "snippet": ""})
        entry["count"] += occurrences
        if not entry["snippet"]:
            entry["snippet"] = snippet_for(text, ARG.TERM)
        if canonical == 'Acknowledgements':   # full ack text for the missing-acks review
            entry["text"] = text
    if not found:
        return None
    return {"doi": "n/a", "pmcid": None, "sections": list(found.values())}


def scan_elsevier(seen, records):
    ''' Elsevier pass: full-text-search ScienceDirect for the term, then for each
        result DOI not already covered by the PMC/Europe PMC passes, retrieve the
        section-structured full text and scan it by section. Appends matched
        records (tagged source="Elsevier"). Reaches subscribed non-OA content;
        DOIs we aren't entitled to return no full text and are counted.
        Keyword arguments:
          seen: cross-pass dedup set (mutated)
          records: result list (mutated)
        Returns:
          None
    '''
    min_date = (datetime.now() - timedelta(days=ARG.DAYS)).strftime('%Y-%m-%d') \
               if ARG.DAYS else None
    pbar = tqdm(desc="Fetching/scanning Elsevier")
    for result in search_elsevier(ARG.TERM, ARG.MAX_RESULTS):
        # search_elsevier reports the hit count on its first page; fill in the
        # bar total then (capped by --max-results) so it shows a real percentage.
        if pbar.total is None and COUNT['elsevier_matched']:
            pbar.total = (min(COUNT['elsevier_matched'], ARG.MAX_RESULTS)
                          if ARG.MAX_RESULTS else COUNT['elsevier_matched'])
            pbar.refresh()
        COUNT['elsevier_read'] += 1
        pbar.update(1)
        pbar.set_postfix(located=COUNT['elsevier_located'],
                         no_ft=COUNT['elsevier_no_fulltext'], refresh=False)
        if min_date:
            rec_date = (result.get('publicationDate') or result.get('coverDate')
                        or result.get('loadDate') or min_date)
            if rec_date < min_date:
                COUNT['elsevier_date_filtered'] += 1
                continue
        doi = (result.get('doi') or '').lower()
        if not doi:
            continue
        if seen_has(seen, doi, None):
            COUNT['elsevier_shared'] += 1
            continue
        time.sleep(0.2)
        article = fetch_elsevier_article(doi)
        if not article:
            COUNT['elsevier_no_fulltext'] += 1
            continue
        seen_add(seen, doi, None)
        COUNT['elsevier_scanned'] += 1
        rec = scan_elsevier_article(article)
        if rec:
            rec['doi'] = doi
            rec['source'] = 'Elsevier'
            records.append(rec)
            COUNT['elsevier_located'] += 1
    pbar.close()


def search_biorxiv_dois(term, max_results, days):
    ''' Page through OpenAlex for bioRxiv works whose FULL TEXT contains the term
        (the bioRxiv API has no full-text search) and yield their bare, lowercased
        DOIs. OpenAlex n-gram matching is fuzzy, but the scan step re-verifies with
        an exact substring match on the fetched full text, so over-discovery drops
        out harmlessly.
        Keyword arguments:
          term: search term
          max_results: cap on DOIs yielded; 0 = all
          days: restrict to works published in the last N days, or None
        Yields:
          DOI strings
    '''
    flt = f"fulltext.search:{term},primary_location.source.id:{OPENALEX_BIORXIV_SOURCE}"
    if days:
        min_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        flt += f",from_publication_date:{min_date}"
        LOGGER.info(f"Restricting OpenAlex search to works published since {min_date}")
    cursor = '*'
    total = None
    yielded = 0
    while cursor:
        params = {'filter': flt, 'per-page': OPENALEX_PAGE_SIZE, 'cursor': cursor,
                  'select': 'id,doi', 'mailto': os.environ['OPENALEX_EMAIL']}
        if os.environ.get('OPENALEX_API_KEY'):
            params['api_key'] = os.environ['OPENALEX_API_KEY']
        try:
            page = _request_with_retry('GET', OPENALEX_API, params=params,
                                       headers=POLITE_HEADERS).json()
        except Exception as err:
            LOGGER.warning(f"OpenAlex search error: {err}")
            return
        if total is None:
            total = page.get('meta', {}).get('count', 0)
            COUNT['biorxiv_matched'] = total
            LOGGER.info(f"OpenAlex found {total:,} bioRxiv works with '{term}' in full text")
        results = page.get('results', [])
        if not results:
            return
        for work in results:
            doi = work.get('doi') or ''
            if doi:
                yield doi.replace('https://doi.org/', '').lower()
                yielded += 1
                if max_results and yielded >= max_results:
                    return
        cursor = page.get('meta', {}).get('next_cursor')
        time.sleep(0.3)


def fetch_biorxiv_article(doi):
    ''' Fetch one bioRxiv preprint's full text as a JATS article dict. Resolves the
        free per-article jatsxml URL from the details API (latest version), fetches
        it, and parses it with xmltodict.
        Keyword arguments:
          doi: preprint DOI (10.1101/... or 10.64898/...)
        Returns:
          JATS article dict, or None if no full text is available
    '''
    try:
        details = _request_with_retry('GET', f"{BIORXIV_DETAILS}{doi}",
                                      headers=POLITE_HEADERS).json()
    except Exception as err:
        LOGGER.debug(f"bioRxiv details error for {doi}: {err}")
        return None
    coll = details.get('collection') or []
    jats_url = coll[-1].get('jatsxml') if coll and isinstance(coll[-1], dict) else None
    if not jats_url:
        return None
    try:
        # jatsxml is served from www.biorxiv.org (Cloudflare); a default
        # python-requests User-Agent gets throttled hard, so send a polite one.
        parsed = xmltodict.parse(_request_with_retry('GET', jats_url,
                                                     headers=POLITE_HEADERS).content)
    except Exception as err:
        LOGGER.debug(f"bioRxiv jatsxml error for {doi}: {err}")
        return None
    article = parsed.get('article') or (parsed.get('pmc-articleset', {}) or {}).get('article')
    return article or None


def scan_biorxiv(seen, records):
    ''' bioRxiv pass: discover preprints whose full text mentions the term via
        OpenAlex, then fetch each preprint's JATS from its free jatsxml URL and
        scan it by section with the same JATS walker used for PMC. Appends matched
        records (source="bioRxiv"), deduped by DOI against the earlier passes.
        Keyword arguments:
          seen: cross-pass dedup set (mutated)
          records: result list (mutated)
        Returns:
          None
    '''
    pbar = tqdm(desc="Fetching/scanning bioRxiv")
    for doi in search_biorxiv_dois(ARG.TERM, ARG.MAX_RESULTS, ARG.DAYS):
        if pbar.total is None and COUNT['biorxiv_matched']:
            pbar.total = (min(COUNT['biorxiv_matched'], ARG.MAX_RESULTS)
                          if ARG.MAX_RESULTS else COUNT['biorxiv_matched'])
            pbar.refresh()
        COUNT['biorxiv_read'] += 1
        pbar.update(1)
        pbar.set_postfix(located=COUNT['biorxiv_located'],
                         no_xml=COUNT['biorxiv_no_fulltext'], refresh=False)
        if seen_has(seen, doi, None):
            COUNT['biorxiv_shared'] += 1
            continue
        time.sleep(0.5)   # gentle pacing; bioRxiv hosts 429 aggressive access
        article = fetch_biorxiv_article(doi)
        if not article:
            COUNT['biorxiv_no_fulltext'] += 1
            continue
        seen_add(seen, doi, None)
        COUNT['biorxiv_scanned'] += 1
        try:
            rec = scan_article(article, ARG.TERM)
        except Exception as err:
            COUNT['biorxiv_parse_error'] += 1
            LOGGER.debug(f"bioRxiv scan error for {doi}: {err}")
            continue
        if rec:
            rec['doi'] = doi
            rec['pmcid'] = None
            rec['source'] = 'bioRxiv'
            COUNT['biorxiv_located'] += 1
            records.append(rec)
    pbar.close()


def flag_missing_janelia(records):
    ''' Write DOIs of likely Janelia-authored papers that are NOT yet in the DIS
        database to a review file. A record is a candidate when the term was
        located in its Author affiliations (i.e. a Janelia author is on the paper)
        and its DOI is in neither the dois nor external_dois collections. This is a
        coverage backstop - the text scanner catches papers the identity-based
        ingestion/author-matching missed. Read-only; nothing is written to the DB.
        Each flagged record also carries the paper's COMPLETE Acknowledgements text
        (verbatim, no snippet/ellipsis; empty when the paper has none) so a reviewer
        can vet the candidate without reopening the article.
        Keyword arguments:
          records: located result dicts
        Returns:
          the list of flagged review candidate dicts (also written to the file)
    '''
    cands = [rec for rec in records
             if rec.get('doi') and rec['doi'] != 'n/a'
             and any(sec['section'] == 'Author affiliations' for sec in rec['sections'])]
    dois = sorted({rec['doi'] for rec in cands})
    known = set()
    for coll in ('dois', 'external_dois'):
        try:
            for row in DB['dis'][coll].find({'doi': {'$in': dois}}, {'_id': 0, 'doi': 1}):
                known.add(row['doi'])
        except Exception as err:
            terminate_program(err)
    review = []
    emitted = set()
    for rec in cands:
        doi = rec['doi']
        if doi in known or doi in emitted:
            continue
        emitted.add(doi)
        aff = next((sec['snippet'] for sec in rec['sections']
                    if sec['section'] == 'Author affiliations'), '')
        # Include the COMPLETE Acknowledgements text (no snippet, no ellipsis) when
        # the paper has one, so a reviewer can vet the candidate without reopening
        # the article. The full text is retained on the scanned record; the elided
        # snippet is deliberately NOT used as a fallback here - a missing ack yields
        # an empty string rather than a truncated one.
        ack_sec = next((sec for sec in rec['sections']
                        if sec['section'] == 'Acknowledgements'), {})
        acknowledgement = ack_sec.get('text') or ''
        # A "present/current address: <org>" note means an author is *now* at the
        # org but the work was likely done elsewhere - a weaker signal than a
        # byline affiliation, so flag it for faster triage (hint from the snippet).
        low = aff.lower()
        present = ('present address' in low or 'present-address' in low
                   or 'current address' in low)
        review.append({'doi': doi, 'source': rec.get('source'),
                       'present_address': present, 'affiliation': aff,
                       'acknowledgement': acknowledgement})
    # Likely byline affiliations first, present-address hints last (triage order).
    review.sort(key=lambda entry: entry['present_address'])
    n_present = sum(1 for entry in review if entry['present_address'])
    outfile = ARG.FLAG_MISSING or (ARG.OUTPUT.rsplit('.', 1)[0] + '_needs_review.json')
    with open(outfile, 'w', encoding='utf-8') as stream:
        json.dump({'term': ARG.TERM, 'candidates': len(review),
                   'present_address': n_present, 'records': review},
                  stream, indent=2, ensure_ascii=False)
    print(f"\n=== Coverage check: likely '{ARG.TERM}' papers not in the DIS database ===")
    _summary_row("Author-affiliation records checked:", len(dois))
    _summary_row("Already in database:", len(known))
    _summary_row("Flagged for review:", len(review), f"-> {outfile}")
    if n_present:
        _summary_row("...present-address notes (lower priority):", n_present)
    return review


def flag_missing_acks(records):
    ''' Write DOIs whose acknowledgements mention the term but for which we hold no
        acknowledgement text, to a review file. A record is a candidate when the
        term was located in its Acknowledgements section; it is flagged when the DOI
        is either absent from both dois and external_dois, or present in one of them
        but with no jrc_acknowledgements field. This surfaces acknowledgement
        coverage the acks pipeline (pull_internal/external_acks) is missing.
        Read-only; nothing is written to the DB.
        Keyword arguments:
          records: located result dicts
        Returns:
          the list of flagged missing-ack dicts (also written to the file)
    '''
    cands = [rec for rec in records
             if rec.get('doi') and rec['doi'] != 'n/a'
             and any(sec['section'] == 'Acknowledgements' for sec in rec['sections'])]
    dois = sorted({rec['doi'] for rec in cands})
    # Per DOI: which collection holds it (or None) and whether it already has acks.
    status = {}
    for coll in ('dois', 'external_dois'):
        try:
            for row in DB['dis'][coll].find({'doi': {'$in': dois}},
                                            {'_id': 0, 'doi': 1, 'jrc_acknowledgements': 1}):
                status[row['doi']] = (coll, bool(row.get('jrc_acknowledgements')))
        except Exception as err:
            terminate_program(err)
    missing = []
    emitted = set()
    for rec in cands:
        doi = rec['doi']
        if doi in emitted:
            continue
        coll, has_acks = status.get(doi, (None, False))
        if has_acks:                       # we already hold the acknowledgement text
            continue
        emitted.add(doi)
        ack_sec = next((sec for sec in rec['sections']
                        if sec['section'] == 'Acknowledgements'), {})
        # Prefer the complete Acknowledgements text captured during scanning; fall
        # back to the snippet if (for any reason) the full text wasn't retained.
        acknowledgement = ack_sec.get('text') or ack_sec.get('snippet') or ''
        missing.append({'doi': doi, 'source': rec.get('source'),
                        'state': 'no_acknowledgements' if coll else 'not_in_database',
                        'collection': coll, 'acknowledgement': acknowledgement})
    # Not-in-database first (the bigger gap), then in-DB-without-acks.
    missing.sort(key=lambda entry: entry['state'] != 'not_in_database')
    not_in_db = sum(1 for entry in missing if entry['state'] == 'not_in_database')
    no_acks = len(missing) - not_in_db
    outfile = ARG.OUTPUT.rsplit('.', 1)[0] + '_missing_acks.json'
    with open(outfile, 'w', encoding='utf-8') as stream:
        json.dump({'term': ARG.TERM, 'missing': len(missing),
                   'not_in_database': not_in_db, 'no_acknowledgements': no_acks,
                   'records': missing}, stream, indent=2, ensure_ascii=False)
    print(f"\n=== Acknowledgement coverage: '{ARG.TERM}' acks we don't hold ===")
    _summary_row("Acknowledgement records checked:", len(dois))
    _summary_row("Already have acknowledgements:", len(dois) - len(missing))
    _summary_row("Missing - not in database:", not_in_db)
    _summary_row("Missing - in DB, no acks:", no_acks, f"-> {outfile}")
    return missing


# JATS flatten_text folds the section <title> into the ack text, so a stored ack
# would start with "Acknowledgements ...". Strip that leading heading token so the
# stored text matches the acks pipeline's heading-free convention.
_ACK_HEADING_RE = re.compile(r'^\s*acknowledge?ments?\b[\s:.—-]*', re.IGNORECASE)


def _strip_ack_heading(text):
    ''' Remove a leading "Acknowledgement(s)" heading token from flattened ack text.
        Keyword arguments:
          text: the flattened Acknowledgements section text (may be None)
        Returns:
          The text with a leading heading token removed, stripped (may be empty)
    '''
    return _ACK_HEADING_RE.sub('', text or '', count=1).strip()


def write_missing_acks(missing):
    ''' With --write, backfill jrc_acknowledgements (+ jrc_ack_source) for the
        located acknowledgements that are already IN the database but hold no ack
        text (the 'no_acknowledgements' state from flag_missing_acks). The full
        Acknowledgements section text captured during scanning is written to the
        DOI's own collection; jrc_ack_source records which full-text source it came
        from. The update is guarded by jrc_acknowledgements not existing, so an ack
        written between the scan and this pass is never clobbered. Records not in
        the database are left for human review (they can't be updated in place).
        Without --write this only reports how many could be backfilled.
        Keyword arguments:
          missing: the flagged missing-ack dicts from flag_missing_acks
        Returns:
          dict of counters (candidates, written, skipped, empty)
    '''
    counts = {'candidates': 0, 'written': 0, 'skipped': 0, 'empty': 0}
    targets = [entry for entry in missing
               if entry.get('state') == 'no_acknowledgements' and entry.get('collection')]
    counts['candidates'] = len(targets)
    if not ARG.WRITE:
        if targets:
            print(f"\n{len(targets):,} in-DB acknowledgement(s) could be backfilled "
                  "- rerun with --write to update the database")
        return counts
    for entry in targets:
        ack = _strip_ack_heading(entry.get('acknowledgement'))
        if not ack:
            counts['empty'] += 1
            continue
        try:
            result = DB['dis'][entry['collection']].update_one(
                {'doi': entry['doi'], 'jrc_acknowledgements': {'$exists': False}},
                {'$set': {'jrc_acknowledgements': ack,
                          'jrc_ack_source': entry.get('source') or 'full text'}})
        except Exception as err:
            terminate_program(err)
        if result.modified_count:
            counts['written'] += 1
            LOGGER.info(f"Wrote acknowledgement for {entry['doi']} "
                        f"({entry['collection']}, source {entry.get('source')})")
        else:
            counts['skipped'] += 1   # guard held: acks appeared between scan and write
    print("\n=== Acknowledgement backfill (--write) ===")
    _summary_row("In-DB acks missing (candidates):", counts['candidates'])
    _summary_row("Acknowledgements written:", counts['written'])
    _summary_row("Skipped (already had acks):", counts['skipped'])
    _summary_row("Skipped (empty text):", counts['empty'])
    return counts


def _esc(text):
    ''' HTML-escape text for safe insertion into the run-summary email.
        Keyword arguments:
          text: raw text (may contain &, <, >)
        Returns:
          escaped string
    '''
    return str(text).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')


def doiurl(doi):
    ''' Format a DOI as a DIS UI link for the email. The DOI is escaped for both
        the URL attribute and the anchor text (legacy SICI DOIs contain <, >, &).
        Keyword arguments:
          doi: DOI string
        Returns:
          HTML anchor
    '''
    esc = _esc(doi)
    return (f"<a href='https://dis.int.janelia.org/doiui/{esc}' "
            f"style='color:{EMAIL_BLUE};text-decoration:none;'>{esc}</a>")


def html_metric_rows(rows):
    ''' Build a zebra-striped label/value table for the email.
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


def html_pill(bg, fg, text):
    ''' Build a small colored status badge as an auto-width single-cell table
        (bgcolor + background-color), not a <span> - Outlook's Word engine does
        not honor background-color on inline elements.
        Keyword arguments:
          bg: background color
          fg: text color
          text: pill text
        Returns:
          HTML single-cell table sized to its content
    '''
    return (f'<table role="presentation" cellpadding="0" cellspacing="0"><tr>'
            f'<td bgcolor="{bg}" style="background-color:{bg};color:{fg};padding:2px 10px;'
            f'border-radius:10px;font-size:11.5px;font-weight:600;">{text}</td></tr></table>')


def html_card_shell(label, pill_html, body_html):
    ''' Build shared card chrome: a header row (label + right-aligned pill) over a
        full-width body. Header is two <td>s directly in the outer table (not a
        nested width="100%" table inside one <td>), which Outlook's Word engine
        chokes on; the body row spans both with colspan="2".
        Keyword arguments:
          label: display label
          pill_html: HTML for the header's right-aligned pill
          body_html: HTML for the card body
        Returns:
          HTML card block
    '''
    return (
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
        f'style="border:1px solid {EMAIL_BORDER};border-radius:8px;margin-bottom:14px;'
        'border-collapse:separate;">'
        f'<tr><td bgcolor="{EMAIL_STRIPE_BG}" style="background-color:{EMAIL_STRIPE_BG};'
        f'padding:10px 16px;border-radius:8px 0 0 0;font-weight:700;color:{EMAIL_NAVY};'
        f'font-size:13.5px;">{label}</td>'
        f'<td bgcolor="{EMAIL_STRIPE_BG}" align="right" style="background-color:'
        f'{EMAIL_STRIPE_BG};padding:10px 16px;border-radius:0 8px 0 0;">{pill_html}</td></tr>'
        f'<tr><td colspan="2" style="padding:4px 16px 10px 16px;">{body_html}</td></tr></table>')


def email_source_card(label, located, rows):
    ''' One per-source card: metric rows with a "located" count pill in the header.
        Keyword arguments:
          label: source display name
          located: located-count for the header pill
          rows: list of (label, value_str) metric rows
        Returns:
          HTML card block
    '''
    pill = html_pill(EMAIL_GREEN_BG, EMAIL_GREEN, f"{located:,} located")
    return html_card_shell(label, pill, html_metric_rows(rows))


def email_review_table(review):
    ''' Build the coverage-check table: likely Janelia papers absent from the DB,
        DOI-linked, with present-address rows marked (color-only text, Outlook-safe).
        Keyword arguments:
          review: list of {doi, source, present_address, affiliation} dicts
        Returns:
          HTML table (or an all-clear note)
    '''
    if not review:
        return (f'<div style="color:{EMAIL_GREEN};font-size:13px;font-weight:600;">'
                f'&#10003; Every located paper with a {_esc(ARG.TERM)} affiliation is already '
                'in the database.</div>')
    cap = 40
    head = ''.join(f'<td style="padding:7px 10px;border-bottom:2px solid {EMAIL_BORDER};'
                   f'font-size:12px;font-weight:700;color:{EMAIL_NAVY};">{col}</td>'
                   for col in ('DOI', 'Source', 'Affiliation context'))
    trs = [f'<tr>{head}</tr>']
    for i, rec in enumerate(review[:cap]):
        bgattr = f' bgcolor="{EMAIL_STRIPE_BG}"' if i % 2 == 0 else ''
        bg = f'background-color:{EMAIL_STRIPE_BG};' if i % 2 == 0 else ''
        aff = _esc(rec.get('affiliation') or '')
        if rec.get('present_address'):
            aff = (f'<span style="color:{EMAIL_AMBER};font-weight:700;">present address'
                   f' &middot; </span>{aff}')
        trs.append(f'<tr{bgattr} style="{bg}">'
                   f'<td valign="top" style="padding:7px 10px;font-size:12px;">'
                   f'{doiurl(rec["doi"])}</td>'
                   f'<td valign="top" style="padding:7px 10px;font-size:12px;'
                   f'color:{EMAIL_GRAY};">{rec.get("source", "")}</td>'
                   f'<td valign="top" style="padding:7px 10px;font-size:11.5px;'
                   f'color:{EMAIL_GRAY};">{aff}</td></tr>')
    table = ('<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
             'style="border-collapse:collapse;margin-top:6px;">' + "".join(trs) + '</table>')
    if len(review) > cap:
        table += (f'<div style="color:{EMAIL_GRAY};font-size:11.5px;margin-top:8px;">'
                  f'&hellip; and {len(review) - cap:,} more &middot; full list in the '
                  'review JSON file.</div>')
    return table


def email_missing_acks_table(missing):
    ''' Build the acknowledgement-coverage table: DOIs whose acknowledgements
        mention the term but which we hold no ack text for, DOI-linked, with the
        state (not in the DB, or in the DB without acks) color-coded.
        Keyword arguments:
          missing: list of {doi, source, state, collection, acknowledgement} dicts
        Returns:
          HTML table (or an all-clear note)
    '''
    if not missing:
        return (f'<div style="color:{EMAIL_GREEN};font-size:13px;font-weight:600;">'
                f'&#10003; We already hold acknowledgements for every located paper '
                f'that acknowledges {_esc(ARG.TERM)}.</div>')
    cap = 40
    head = ''.join(f'<td style="padding:7px 10px;border-bottom:2px solid {EMAIL_BORDER};'
                   f'font-size:12px;font-weight:700;color:{EMAIL_NAVY};">{col}</td>'
                   for col in ('DOI', 'Source', 'State', 'Acknowledgement'))
    trs = [f'<tr>{head}</tr>']
    for i, rec in enumerate(missing[:cap]):
        bgattr = f' bgcolor="{EMAIL_STRIPE_BG}"' if i % 2 == 0 else ''
        bg = f'background-color:{EMAIL_STRIPE_BG};' if i % 2 == 0 else ''
        if rec['state'] == 'not_in_database':
            state = f'<span style="color:{EMAIL_AMBER};font-weight:700;">not in database</span>'
        else:
            state = (f'<span style="color:{EMAIL_GRAY};">in {_esc(rec.get("collection") or "db")},'
                     ' no acks</span>')
        # Email shows a short preview; the complete acknowledgement is in the JSON.
        ack = rec.get('acknowledgement') or ''
        snip = _esc(ack[:160].rstrip()) + '&hellip;' if len(ack) > 160 else _esc(ack)
        trs.append(f'<tr{bgattr} style="{bg}">'
                   f'<td valign="top" style="padding:7px 10px;font-size:12px;">'
                   f'{doiurl(rec["doi"])}</td>'
                   f'<td valign="top" style="padding:7px 10px;font-size:12px;'
                   f'color:{EMAIL_GRAY};">{rec.get("source", "")}</td>'
                   f'<td valign="top" style="padding:7px 10px;font-size:11.5px;">{state}</td>'
                   f'<td valign="top" style="padding:7px 10px;font-size:11.5px;'
                   f'color:{EMAIL_GRAY};">{snip}</td></tr>')
    table = ('<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
             'style="border-collapse:collapse;margin-top:6px;">' + "".join(trs) + '</table>')
    if len(missing) > cap:
        table += (f'<div style="color:{EMAIL_GRAY};font-size:11.5px;margin-top:8px;">'
                  f'&hellip; and {len(missing) - cap:,} more &middot; full list in the '
                  'missing-acks JSON file.</div>')
    return table


def build_email_html(records, review, missing_acks, write_counts):
    ''' Build the HTML run-summary email body from the run's COUNT state, the
        located records, the coverage-review list, the missing-acknowledgement
        list, and the backfill counters. Kept separate from sending so it can be
        rendered/inspected without dispatching mail.
        Keyword arguments:
          records: located result dicts
          review: coverage-review candidate dicts
          missing_acks: missing-acknowledgement candidate dicts
          write_counts: write_missing_acks() counters (candidates/written/skipped)
        Returns:
          HTML string
    '''
    run_data = JRC.get_run_data(__file__, __version__).strip()
    term = _esc(ARG.TERM)
    scope = f"term: '{term}' &middot; source: {_esc(ARG.SOURCE)}"
    if ARG.DAYS:
        scope += f" &middot; last {ARG.DAYS:,} days"
    if ARG.MAX_RESULTS:
        scope += f" &middot; max {ARG.MAX_RESULTS:,}/source"
    net_new = len(records) - COUNT['pmc_located']
    n_present = sum(1 for rec in review if rec.get('present_address'))
    kpis = ''.join([
        JE.kpi_card(f"{len(records):,}", "Located", 'good' if records else 'neutral', '25%'),
        JE.kpi_card(f"{net_new:,}", "Net-new (non-PMC)", 'neutral', '25%'),
        JE.kpi_card(f"{len(review):,}", "Flagged for review", 'bad' if review else 'neutral', '25%'),
        JE.kpi_card(f"{n_present:,}", "Present-address", 'neutral', '25%'),
    ])
    cards = []
    if ARG.SOURCE in ('pmc', 'both', 'all'):
        cards.append(email_source_card("PubMed Central", COUNT['pmc_located'], [
            ("Matched (search)", f"{COUNT['pmc_matched']:,}"),
            ("Scanned", f"{COUNT['pmc_scanned']:,}"),
            (f"Located '{term}'",f"{COUNT['pmc_located']:,}")]))
    if ARG.SOURCE in ('europepmc', 'both', 'all'):
        cards.append(email_source_card("Europe PMC", COUNT['epmc_located'], [
            ("Matched (search)", f"{COUNT['epmc_matched']:,}"),
            ("Shared with PMC (skipped)", f"{COUNT['epmc_shared_with_pmc']:,}"),
            ("No downloadable XML (skipped)", f"{COUNT['epmc_no_pmcid']:,}"),
            ("Scanned (new)", f"{COUNT['epmc_scanned']:,}"),
            (f"Located '{term}'",f"{COUNT['epmc_located']:,}")]))
    if ARG.SOURCE in ('elsevier', 'all'):
        cards.append(email_source_card("Elsevier ScienceDirect", COUNT['elsevier_located'], [
            ("Matched (search)", f"{COUNT['elsevier_matched']:,}"),
            ("Shared (earlier pass)", f"{COUNT['elsevier_shared']:,}"),
            ("Date filtered", f"{COUNT['elsevier_date_filtered']:,}"),
            ("No full text (unentitled/none)", f"{COUNT['elsevier_no_fulltext']:,}"),
            ("Scanned (new)", f"{COUNT['elsevier_scanned']:,}"),
            (f"Located '{term}'",f"{COUNT['elsevier_located']:,}")]))
    if ARG.SOURCE in ('biorxiv', 'all'):
        cards.append(email_source_card("bioRxiv", COUNT['biorxiv_located'], [
            ("Matched (OpenAlex full text)", f"{COUNT['biorxiv_matched']:,}"),
            ("Shared (earlier pass)", f"{COUNT['biorxiv_shared']:,}"),
            ("No full text", f"{COUNT['biorxiv_no_fulltext']:,}"),
            ("Scanned (new)", f"{COUNT['biorxiv_scanned']:,}"),
            (f"Located '{term}'",f"{COUNT['biorxiv_located']:,}")]))
    sources_section = JE.section_header("&#128202; Sources") + "".join(cards)
    tally = collections.Counter()
    for rec in records:
        for sec in rec['sections']:
            tally[sec['section']] += 1
    tally_rows = [(_esc(sec), f"{cnt:,}") for sec, cnt in tally.most_common(12)]
    tally_section = (JE.section_header("&#128209; Mentions by section")
                     + (html_metric_rows(tally_rows) if tally_rows
                        else f'<div style="color:{EMAIL_GRAY};font-size:13px;">None.</div>'))
    review_section = (JE.section_header(
        f"&#9888; Likely '{term}' papers not in the database ({len(review):,})")
        + email_review_table(review))
    missing_section = (JE.section_header(
        f"&#128203; '{term}' acknowledgements we don't hold ({len(missing_acks):,})")
        + email_missing_acks_table(missing_acks))
    if ARG.WRITE:
        note = (f"Backfilled jrc_acknowledgements for {write_counts['written']:,} in-DB "
                f"DOI(s) &middot; {write_counts['skipped']:,} already had acks &middot; "
                f"{write_counts['empty']:,} empty.")
    else:
        note = (f"{write_counts['candidates']:,} in-DB DOI(s) could be backfilled "
                "- rerun with --write.")
    missing_section += (f'<div style="color:{EMAIL_GRAY};font-size:12px;'
                        f'margin-top:6px;">{note}</div>')
    body = (JE.body_row(sources_section) + JE.body_row(tally_section)
            + JE.body_row(review_section) + JE.body_row(missing_section))
    # WRITE (green) when backfilling the DB, otherwise a neutral read-only SCAN.
    mode_label = 'WRITE' if ARG.WRITE else 'SCAN'
    mode_tone = 'good' if ARG.WRITE else 'neutral'
    return JE.render(os.path.basename(__file__), __version__,
                     f"{run_data} &middot; {scope}", mode_label, mode_tone, kpis, body)


def generate_email(records, review, missing_acks, write_counts):
    ''' Build and send the HTML run-summary email. Always sent to the configured
        developer address.
        Keyword arguments:
          records: located result dicts
          review: coverage-review candidate dicts
          missing_acks: missing-acknowledgement candidate dicts
          write_counts: write_missing_acks() counters
        Returns:
          None
    '''
    msg = build_email_html(records, review, missing_acks, write_counts)
    try:
        recipient = DISCONFIG['developer']
        LOGGER.info(f"Sending run-summary email to {recipient}")
        JRC.send_email(msg, DISCONFIG['sender'], recipient,
                       f"Full-text scan for '{ARG.TERM}'", mime='html')
    except Exception as err:
        LOGGER.error(f"Could not send email: {err}")
        traceback.print_exc()


def scan_single_doi(doi):
    ''' Fetch and section-scan a single DOI's full text (bypassing the term-search
        discovery passes) for --doi. Picks the source by prefix/availability:
        bioRxiv/medRxiv (10.1101/10.64898), Elsevier (10.1016, needs ELSEVIER_API_KEY),
        else Europe PMC by DOI (the PMC core plus non-PMC open access). Sources are tried
        in that order, falling through on a miss.
        Keyword arguments:
          doi: the DOI to scan
        Returns:
          A scan record {doi, pmcid, source, sections:[...]}, or None if no full
          text was found or the term appears in no section
    '''
    doi = doi.lower()
    # bioRxiv/medRxiv preprints (JATS). 10.1101 is the legacy prefix; openRxiv
    # migrated bioRxiv/medRxiv to the 10.64898 prefix in 2025, so match both.
    if doi.startswith(('10.1101/', '10.64898/')):
        article = fetch_biorxiv_article(doi)
        if article:
            rec = scan_article(article, ARG.TERM)
            if rec:
                rec['doi'] = doi
                rec['source'] = 'bioRxiv'
                return rec
    # Elsevier subscribed/OA full text (ce: schema), needs an API key
    if doi.startswith('10.1016/') and os.environ.get('ELSEVIER_API_KEY'):
        article = fetch_elsevier_article(doi)
        if article:
            rec = scan_elsevier_article(article)
            if rec:
                rec['doi'] = doi
                rec['source'] = 'Elsevier'
                return rec
    # Europe PMC by DOI: the PMC core plus non-PMC open access (JATS)
    pmcid = _epmc_pmcid_for_doi(doi)
    if pmcid:
        try:
            article = fetch_epmc_fulltext(pmcid)
        except Exception as err:
            LOGGER.debug(f"Europe PMC fetch error for {doi}: {err}")
            article = None
        if article:
            rec = scan_article(article, ARG.TERM)
            if rec:
                rec['doi'] = doi
                rec['pmcid'] = pmcid
                rec['source'] = 'Europe PMC'
                return rec
    return None


def processing():
    ''' Scan the selected source(s) for the term by section and write results.
        PMC runs first (populating the dedup set); Europe PMC runs second and
        skips anything the PMC pass already covered. With --doi, the term-search
        discovery is skipped entirely: the one DOI is fetched directly by source,
        scanned, and run through the acknowledgement-coverage/backfill checks.
        Keyword arguments:
          None
        Returns:
          None
    '''
    if ARG.DOI:
        connect_db()   # the coverage check + optional --write need the DB
        rec = scan_single_doi(ARG.DOI)
        records = [rec] if rec else []
        if rec:
            secs = ', '.join(f"{sec['section']} ({sec['count']})" for sec in rec['sections'])
            print(f"{ARG.DOI}: '{ARG.TERM}' found in {secs}  [source: {rec['source']}]")
        else:
            print(f"{ARG.DOI}: no full text retrieved, or '{ARG.TERM}' in no section")
        missing_acks = flag_missing_acks(records)
        write_missing_acks(missing_acks)
        return
    records = []
    seen = set()   # DOIs/PMCIDs already covered, for cross-pass dedup
    run_pmc = ARG.SOURCE in ('pmc', 'both', 'all')
    run_epmc = ARG.SOURCE in ('europepmc', 'both', 'all')
    run_elsevier = ARG.SOURCE in ('elsevier', 'all')
    run_biorxiv = ARG.SOURCE in ('biorxiv', 'all')
    api_key = os.environ.get('NCBI_API_KEY')
    # NCBI is used by the PMC pass and by the BioC cross-check.
    if (run_pmc or ARG.VERIFY_SECTIONS) and not api_key:
        LOGGER.warning("NCBI_API_KEY not set - using slower anonymous rate limits")
    if run_elsevier and not os.environ.get('ELSEVIER_API_KEY'):
        terminate_program("ELSEVIER_API_KEY must be set for --source elsevier/all")
    if run_biorxiv and not os.environ.get('OPENALEX_EMAIL'):
        terminate_program("OPENALEX_EMAIL must be set for --source biorxiv/all")
    connect_db()   # coverage check runs every run, so a DB problem aborts up front
    if run_pmc:
        scan_pmc(api_key, seen, records)
    if run_epmc:
        scan_europepmc(seen, records)
    # Elsevier and bioRxiv run last: their DOIs are deduped against the earlier passes.
    if run_elsevier:
        scan_elsevier(seen, records)
    if run_biorxiv:
        scan_biorxiv(seen, records)
    merge_section_casing(records)
    write_output(records)
    report(records)
    if ARG.VERIFY_SECTIONS:
        verify_sections(records, api_key)
    review = flag_missing_janelia(records)
    missing_acks = flag_missing_acks(records)
    write_counts = write_missing_acks(missing_acks)
    generate_email(records, review, missing_acks, write_counts)


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Scan PMC, Europe PMC, Elsevier, and/or bioRxiv full text for a "
                    "term (default Janelia) and report which section it appears in")
    PARSER.add_argument('--term', dest='TERM', action='store', default='Janelia',
                        help='Search term [Janelia]')
    PARSER.add_argument('--doi', dest='DOI', action='store', default=None,
                        help='Scan a single DOI (fetched directly from bioRxiv/'
                             'Elsevier/Europe PMC by DOI), skipping the term search')
    PARSER.add_argument('--source', dest='SOURCE', action='store',
                        choices=['pmc', 'europepmc', 'elsevier', 'biorxiv', 'both', 'all'],
                        default='both',
                        help="Source(s) to scan; each is deduped by DOI/PMCID against "
                             "the earlier passes. 'both'=pmc+europepmc; 'all' adds "
                             "elsevier (needs ELSEVIER_API_KEY) + bioRxiv [both]")
    PARSER.add_argument('--days', dest='DAYS', action='store', type=int, default=None,
                        help='Restrict to the last N days (PMC: added to PMC; '
                             'Europe PMC: record creation date)')
    PARSER.add_argument('--max-results', dest='MAX_RESULTS', action='store', type=int,
                        default=0,
                        help='Cap on PMC articles retrieved/scanned; 0 = all matches [0]')
    PARSER.add_argument('--output', dest='OUTPUT', action='store',
                        default='fulltext_mentions.json',
                        help='Output JSON file [fulltext_mentions.json]')
    PARSER.add_argument('--verify-sections', dest='VERIFY_SECTIONS', action='store_true',
                        default=False,
                        help='Cross-check our section binning against BioC section_type '
                             'for located articles (QA only; adds no content)')
    PARSER.add_argument('--verify-limit', dest='VERIFY_LIMIT', action='store', type=int,
                        default=200,
                        help='Max located articles to cross-check against BioC; 0 = all [200]')
    PARSER.add_argument('--flag-missing', dest='FLAG_MISSING', action='store', default=None,
                        help='Path for the coverage-check review file (the check always '
                             'runs); default: <output>_needs_review.json')
    PARSER.add_argument('--write', dest='WRITE', action='store_true', default=False,
                        help='Backfill jrc_acknowledgements for located acks whose DOI is '
                             'already in the DB but has no ack text (uses dis.prod.write)')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    try:
        DISCONFIG.update(JRC.simplenamespace_to_dict(JRC.get_config("dis")))
    except Exception as gerr:
        terminate_program(gerr)
    processing()
    terminate_program()
