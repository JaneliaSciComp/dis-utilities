"""pull_plos.py
Search the PLOS Solr API for Janelia-affiliated PLOS articles.

Queries https://api.plos.org/search for affiliate:"Janelia" (restricted to full
articles via doc_type:full), paging through every result. In PLOS Solr the `id`
field IS the article DOI (10.1371/journal.*).

Each candidate is confirmed to have a Janelia AUTHOR - not merely a Janelia editor.
(The PLOS search and its flat `affiliate` index both include editor affiliations, so
a paper edited by a Janelian, e.g. a Janelia group leader, matches the search without
being a Janelia-authored paper.) Confirmation:
  - PRIMARY: the DIS /raw/plos JATS record - a contributor with contrib-type "author"
    (not "editor") must carry a Janelia affiliation.
  - FALLBACK: the Crossref record - an author matching a Janelia ORCID or asserted
    Janelia affiliation (via doi_common.get_author_details). Crossref usually lacks
    affiliations for PLOS, so this mainly catches ORCID matches.
A candidate with a Janelia editor but no Janelia author (or confirmed by neither) is
set aside for manual review.
Candidates already in the dois / external_dois / to_ignore collections are skipped.

OUTPUT (files only - this program never modifies the database)
------
    janelia_plos_dois.json    Confirmed records (doi, title, authors,
                              janelia_authors, confirmed_by [plos|crossref]).
    plos_ready.txt            One confirmed DOI per line, ready for ingestion.
    janelia_plos_review.json  DOIs set aside, each with a reason (Janelia editor
                              only, PLOS record unavailable, or no Janelia author).

An HTML summary email is sent on --test (developer) or --write (receivers list).

DEPENDENCIES
------------
    jrc_common.jrc_common (JRC), doi_common.doi_common (DL), jrc_email.jrc_email (JE)
"""

__version__ = '1.1.1'

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
# DIS /raw/plos returns the PLOS article JATS as JSON; used to check author (vs
# editor) affiliations. Same DIS base other sync tools use (e.g. pull_figshare).
DIS_RAW_PLOS = "https://dis.int.janelia.org/raw/plos/"


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
                          'fl': 'id,title,author_display',
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


def _aslist(value):
    ''' Coerce an xmltodict node to a list (dict/None -> [dict]/[]). '''
    return value if isinstance(value, list) else ([] if value is None else [value])


def _alltext(node):
    ''' Concatenate all non-attribute text under an xmltodict node. '''
    if isinstance(node, str):
        return node
    if isinstance(node, dict):
        return ' '.join(_alltext(v) for k, v in node.items() if not str(k).startswith('@'))
    if isinstance(node, list):
        return ' '.join(_alltext(v) for v in node)
    return ''


def plos_contributors(doi):
    ''' Fetch the PLOS article via DIS /raw/plos (JATS-as-JSON) and return
        (author_janelians, editor_janelians): contributor names whose affiliation
        names Janelia, split by role. Editors are returned separately so an
        editor-only match is flagged for review, never counted as a Janelia author.
        Keyword arguments:
          doi: DOI
        Returns:
          (list, list) on success; (None, None) if the record was unavailable
    '''
    try:
        resp = requests.get(DIS_RAW_PLOS + doi, timeout=30,
                            headers={'User-Agent': 'janelia-dis/pull_plos'})
        if resp.status_code != 200:
            return None, None
        meta = (((resp.json() or {}).get('data') or {}).get('article') or {}) \
               .get('front', {}).get('article-meta')
    except Exception:
        return None, None
    if not isinstance(meta, dict):
        return None, None
    # id -> affiliation text, to resolve each contributor's <xref ref-type="aff">
    idmap = {}
    def _collect(node):
        if isinstance(node, dict):
            if '@id' in node:
                idmap[node['@id']] = _alltext(node)
            for val in node.values():
                _collect(val)
        elif isinstance(node, list):
            for val in node:
                _collect(val)
    _collect(meta)

    def _has_janelia(contrib):
        texts = [idmap[x['@rid']] for x in _aslist(contrib.get('xref'))
                 if isinstance(x, dict) and x.get('@ref-type') == 'aff'
                 and x.get('@rid') in idmap]
        texts += [_alltext(a) for a in _aslist(contrib.get('aff'))]
        return any('janelia' in t.lower() for t in texts)

    authors, editors = [], []
    for group in _aslist(meta.get('contrib-group')):
        for contrib in _aslist(group.get('contrib')):
            if not isinstance(contrib, dict) or not _has_janelia(contrib):
                continue
            nm = contrib.get('name') or {}
            name = (f"{(nm.get('given-names') or '').strip()} "
                    f"{(nm.get('surname') or '').strip()}").strip()
            if contrib.get('@contrib-type') == 'author':
                authors.append(name)
            elif contrib.get('@contrib-type') == 'editor':
                editors.append(name)
    return authors, editors


def doc_title(doc):
    ''' Return the article title from a Solr doc (title may be a string or list). '''
    title = doc.get('title')
    if isinstance(title, list):
        return title[0] if title else ''
    return title or ''


def build_record(doc, doi, janelia_authors, confirmed_by):
    ''' Compact output record for a confirmed Janelia-authored PLOS paper.
        Keyword arguments:
          doc: PLOS Solr doc
          doi: DOI
          janelia_authors: confirmed Janelian author names (list)
          confirmed_by: 'plos' (author affiliation in the JATS) or 'crossref' (ORCID)
        Returns:
          Output record dict
    '''
    return {"doi": doi, "title": doc_title(doc),
            "authors": doc.get('author_display') or [],
            "janelia_authors": janelia_authors,
            "confirmed_by": confirmed_by}


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
        JE.kpi_card(f"{COUNT['editor_only']:,}", "Janelia editor only",
                    'warn' if COUNT['editor_only'] else 'neutral'),
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
            'Matched the PLOS Janelia search but no Janelia AUTHOR was confirmed - '
            'includes papers with a Janelia editor only. See janelia_plos_review.json.</div>')
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
        print(f"  Confirming {idx}/{COUNT['total']}: {doi}          ", end="\r")
        # PRIMARY: a Janelia AUTHOR (not editor) in the PLOS JATS record.
        authors, editors = plos_contributors(doi)
        if authors:
            results.append(build_record(doc, doi, authors, 'plos'))
            COUNT['confirmed_plos'] += 1
            continue
        # FALLBACK: a Janelian author via Crossref (ORCID / asserted affiliation).
        crossref_jan = janelia_authors(doi, get_crossref_record(doi))
        if crossref_jan:
            names = [f"{a['given']} {a['family']}" for a in crossref_jan]
            results.append(build_record(doc, doi, names, 'crossref'))
            COUNT['confirmed_crossref'] += 1
            continue
        # Neither confirmed a Janelia author - set aside for review.
        if editors:
            COUNT['editor_only'] += 1
            reason = f"Janelia editor only: {', '.join(editors)}"
        elif authors is None:
            COUNT['lookup_failed'] += 1
            reason = "PLOS record unavailable"
        else:
            reason = "no Janelia author found"
        review.append({"doi": doi, "reason": reason})
    COUNT['review'] = len(review)
    rows = [("DOIs read from PLOS", COUNT['total']),
            ("Skipped (no DOI)", COUNT['no_doi']),
            ("Skipped (duplicate)", COUNT['skipped_dup']),
            ("DOIs already in database", COUNT['in_dois']),
            ("DOIs to ignore", COUNT['ignored']),
            ("Confirmed via PLOS author", COUNT['confirmed_plos']),
            ("Confirmed via Crossref ORCID", COUNT['confirmed_crossref']),
            ("Review - Janelia editor only", COUNT['editor_only']),
            ("Review - other", COUNT['review'] - COUNT['editor_only']),
            ("DOIs ready for processing", len(results))]
    width = max(len(label) for label, _ in rows)
    summary = "\n".join(f"{label + ':':<{width + 2}}{num:>6,}" for label, num in rows)
    print("\n" + summary)
    if results:
        with open('janelia_plos_dois.json', 'w', encoding='utf-8') as fh:
            json.dump(results, fh, indent=2, ensure_ascii=False)
        with open('plos_ready.txt', 'w', encoding='utf-8') as fh:
            for rec in results:
                fh.write(rec['doi'] + "\n")
        LOGGER.info("Wrote janelia_plos_dois.json and plos_ready.txt")
    if review:
        with open('janelia_plos_review.json', 'w', encoding='utf-8') as fh:
            json.dump(review, fh, indent=2, ensure_ascii=False)
        LOGGER.info("Wrote janelia_plos_review.json")
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
