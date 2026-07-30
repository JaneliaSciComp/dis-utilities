"""
Search the Web of Science (WOS) Starter API for Janelia-affiliated publications
and write candidate DOIs to local files for downstream ingestion.

Usage:
    python pull_wos.py [--api-key KEY] [--manifold dev|prod]
                       [--start-year YEAR] [--year YEAR]
                       [--test] [--write] [--verbose] [--debug]

Environment:
    WOS_API_KEY   Web of Science API key (required unless --api-key is passed).

Searches the WOS Starter API using the topic field (TS=Janelia), which covers
title, abstract, author keywords, and Keywords Plus. This is the closest
affiliation-equivalent available in the Starter API — the Starter API does not
support the AD= (address) field tag, and Janelia is not registered in the WOS
preferred-organization index required by OG=.

Results are iterated year by year from 2006 (when Janelia opened) to the
current year because the total result count may exceed the API's 1,000-record
per-query retrieval cap.

Note: TS= may include papers that merely cite or reference Janelia rather than
being authored by Janelians. Each candidate DOI is therefore confirmed against
Crossref: an author must match a Janelia ORCID or carry an asserted Janelia
affiliation for the DOI to be considered ready. DOIs with no confirmable
Janelia author are written to a separate file for manual review.

DOIs already present in the MongoDB dois, external_dois, or to_ignore
collections are excluded from output.

Output files (written to the current working directory):
    janelia_wos_dois.json       Records confirmed to have Janelia authors.
    wos_ready.txt               Plain list of the same DOIs, one per line.
    janelia_wos_noauthors.txt   DOIs where no Janelia author could be confirmed;
                                these warrant manual review.

This program writes only the output files above; it never modifies the database.
An HTML summary email is sent when --test (to the developer) or --write (to the
full receivers list) is supplied.
"""

__version__ = '1.5.0'

import argparse
import collections
import json
import os
import sys
import time
import traceback
from datetime import date
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
# Years whose WOS hit-count exceeded MAX_RECORDS, as (year, total) - tracked so the
# per-year retrieval cap is reported rather than being a silent coverage loss.
CAPPED_YEARS = []
# General
API_BASE = "https://api.clarivate.com/apis/wos-starter/v1/documents"
DEFAULT_API_KEY = os.environ.get("WOS_API_KEY")
PAGE_SIZE = 50
MAX_RECORDS = 1000
MAX_RETRY_WAIT = 300  # cap a 429 Retry-After so a huge value can't hang the run
JANELIA_QUERY = "TS=Janelia"
JANELIA_START_YEAR = 2006


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


def initialize_program(manifold: str):
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
    dbo = attrgetter(f"dis.{manifold}.write")(dbconfig)
    print(f"Connecting to {dbo.name} ({manifold}) on {dbo.host}")
    try:
        DB['dis'] = JRC.connect_database(dbo)
    except Exception as err:
        terminate_program(err)
    build_doi_cache()


def build_doi_cache():
    """Pre-load known DOIs from dois, external_dois, and to_ignore collections."""
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
# WOS Starter API helpers
# ---------------------------------------------------------------------------

def fetch_page(query: str, api_key: str, page: int = 1) -> dict:
    """Single WOS Starter API call. Returns the parsed JSON response."""
    headers = {
        "X-ApiKey": api_key,
        "Accept": "application/json",
    }
    params = {
        "q": query,
        "limit": PAGE_SIZE,
        "page": page,
    }
    for attempt in range(5):
        try:
            resp = requests.get(API_BASE, headers=headers, params=params, timeout=30)
            if resp.status_code == 429:
                # Cap Retry-After: some APIs return a very large value (hours) on a
                # quota block, which would otherwise hang the run. Fall back to the
                # cap when the header is missing or non-numeric (it may be an
                # HTTP-date rather than a seconds count).
                try:
                    wait = min(int(resp.headers.get("Retry-After", 10)), MAX_RETRY_WAIT)
                except ValueError:
                    wait = MAX_RETRY_WAIT
                print(f"    Rate limited — waiting {wait}s (retry {attempt + 1}/5)")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.Timeout:
            wait = 2 ** attempt
            print(f"    Timeout — retrying in {wait}s (attempt {attempt + 1}/5)")
            time.sleep(wait)
        except requests.exceptions.RequestException as exc:
            terminate_program(str(exc))
    terminate_program("Max retries exceeded.")


def get_total_hits(query: str, api_key: str) -> int:
    """Return the total number of records WOS reports for a query."""
    data = fetch_page(query, api_key, page=1)
    return data.get("metadata", {}).get("total", 0)


def fetch_window(query: str, api_key: str, total: int) -> list[dict]:
    """Retrieve up to MAX_RECORDS records for a query, paging through results."""
    records = []
    retrievable = min(total, MAX_RECORDS)
    page = 1
    while len(records) < retrievable:
        data = fetch_page(query, api_key, page=page)
        batch = data.get("hits", [])
        if not batch:
            break
        records.extend(batch)
        page += 1
        time.sleep(0.25)
    return records


def fetch_year(query_base: str, api_key: str, year: int) -> list[dict]:
    ''' Retrieve WOS records for a query restricted to a single calendar year.
        Keyword arguments:
          query_base: WOS query string without a year constraint
          api_key: WOS API key
          year: calendar year to search
        Returns:
          List of raw WOS hit dicts
    '''
    query = f"{query_base} AND PY={year}"
    total = get_total_hits(query, api_key)
    if total == 0:
        return []
    print(f"  {year}: {total} hit(s)")
    if total > MAX_RECORDS:
        CAPPED_YEARS.append((year, total))
        print(f"  WARNING: {year} has {total} hits; only the first "
              f"{MAX_RECORDS} will be retrieved.")
    return fetch_window(query, api_key, total)


def search_janelia(api_key: str, start_year: int = JANELIA_START_YEAR) -> list[dict]:
    ''' Query WOS for all records matching the Janelia organization query.
        If total hits fit within MAX_RECORDS the results are fetched in one
        pass; otherwise the search iterates year by year.
        Keyword arguments:
          api_key: WOS API key
          start_year: earliest year to include in the sweep
        Returns:
          List of raw WOS hit dicts
    '''
    current_year = date.today().year
    print(f"Querying WOS Starter API: {JANELIA_QUERY!r}")
    total_all = get_total_hits(JANELIA_QUERY, api_key)
    print(f"Total hits (all years): {total_all:,}")
    if total_all <= MAX_RECORDS:
        print(f"Under {MAX_RECORDS:,} — fetching without date splitting")
        return fetch_window(JANELIA_QUERY, api_key, total_all)
    if ARG.YEAR:
        print(f"Searching for year: {ARG.YEAR}")
        return fetch_year(JANELIA_QUERY, api_key, ARG.YEAR)
    print(f"Exceeds {MAX_RECORDS:,} — iterating year by year "
          f"({start_year}–{current_year})")
    all_records = []
    for year in range(start_year, current_year + 1):
        all_records.extend(fetch_year(JANELIA_QUERY, api_key, year))
    return all_records


# ---------------------------------------------------------------------------
# Record helpers
# ---------------------------------------------------------------------------

def extract_doi(hit: dict) -> str:
    """Return the lowercased DOI from a WOS Starter hit, or an empty string."""
    return hit.get("identifiers", {}).get("doi", "").lower()


def extract_record(hit: dict, janelians: list) -> dict:
    """Build a compact output record from a WOS Starter hit dict.

    janelians is the list of confirmed Janelian author dicts returned by
    janelia_authors(); current (ORCID-matched) and asserted authors are
    recorded separately.
    """
    identifiers = hit.get("identifiers", {})
    source = hit.get("source", {})
    record = {
        "doi": extract_doi(hit),
        "uid": hit.get("uid", ""),
        "title": hit.get("title") or "",
        "year": source.get("publishYear"),
        "journal": source.get("sourceTitle", ""),
        "volume": source.get("volume", ""),
        "issue": source.get("issue", ""),
        "pages": source.get("pages", {}).get("range", ""),
        "authors": [a.get("displayName", "")
                    for a in hit.get("names", {}).get("authors", [])],
        "keywords": hit.get("keywords", {}).get("authorKeywords", []),
        "issn": identifiers.get("issn", ""),
        "eissn": identifiers.get("eissn", ""),
        "pmid": identifiers.get("pmid", ""),
        "wos_link": hit.get("links", {}).get("record", ""),
    }
    asserted = [f"{a['given']} {a['family']}" for a in janelians
                if a['match'] == 'asserted']
    current = [f"{a['given']} {a['family']}" for a in janelians
               if a['match'] == 'ORCID']
    if asserted:
        record["janelia_authors_asserted"] = asserted
    if current:
        record["janelia_authors_current"] = current
    return record


# ---------------------------------------------------------------------------
# Crossref / Janelia author confirmation
# ---------------------------------------------------------------------------

def get_crossref_record(doi: str) -> dict:
    """Fetch Crossref metadata for a DOI. Returns {} on any failure."""
    try:
        resp = JRC.call_crossref(doi, timeout=20)
        return resp.get('message', {}) if resp else {}
    except Exception:
        LOGGER.error(f"Failed to fetch Crossref record for {doi}")
        return {}


def janelia_authors(doi, msg):
    ''' Return a list of Janelian author dicts for a Crossref message,
        or an empty list if none are found.
        Keyword arguments:
          doi: DOI
          msg: Crossref message
        Returns:
          List of author dicts (janelian=True entries from get_author_details)
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
    if janelians:
        names = [f"{a['given']} {a['family']} ({a['match']})" for a in janelians]
        print(f"Janelians found for {doi}: {', '.join(names)}")
    return janelians


# ---------------------------------------------------------------------------
# Email helpers
# ---------------------------------------------------------------------------

def generate_email(results, summary):
    ''' Generate and send the HTML run-summary email (jrc_email house style): a
        header banner, a KPI stat-tile row, and a "Ready to Add" card. Recipient is
        the developer for --test and the receivers list otherwise.
        Keyword arguments:
          results: list of output record dicts ready for processing
          summary: text summary (printed to the console; not used in the email body)
        Returns:
          None
    '''
    if not results and not CAPPED_YEARS:
        return
    run_data = JRC.get_run_data(__file__, __version__).strip()
    mode_label = 'TEST' if ARG.TEST else 'LIVE'
    mode_tone = 'warn' if (ARG.TEST or CAPPED_YEARS) else 'good'
    kpis = ''.join([
        JE.kpi_card(f"{COUNT['total']:,}", "Read from WOS"),
        JE.kpi_card(f"{COUNT['in_dois']:,}", "Already in DB"),
        JE.kpi_card(f"{COUNT['ignored']:,}", "To ignore"),
        JE.kpi_card(f"{COUNT['noauthors']:,}", "No Janelia authors"),
        JE.kpi_card(f"{len(results):,}", "Ready to add", 'good'),
    ])
    body = ""
    if CAPPED_YEARS:
        dropped = sum(total - MAX_RECORDS for _, total in CAPPED_YEARS)
        rows = "".join(f"{yr}: {total:,} hits (~{total - MAX_RECORDS:,} not retrieved)<br>"
                       for yr, total in CAPPED_YEARS)
        body += JE.body_row(
            JE.section_header(f"&#9888; {len(CAPPED_YEARS)} year(s) over the "
                              f"{MAX_RECORDS:,}-record cap (~{dropped:,} not retrieved)")
            + f"<div style='padding:4px 12px;'>{rows}</div>")
    if results:
        ready = [(rec['doi'], None) for rec in results]
        body += JE.body_row(JE.section_header(f"&#10003; Ready to Add ({len(results):,})")
                            + JE.doi_card("Ready to Add", ready, 'good'))
    msg = JE.render(os.path.basename(__file__), __version__, run_data,
                    mode_label, mode_tone, kpis, body)
    try:
        email = DISCONFIG['developer'] if ARG.TEST else DISCONFIG['receivers']
        LOGGER.info(f"Sending email to {email}")
        JRC.send_email(msg, DISCONFIG['sender'], email, "WOS DOI sync", mime='html')
    except Exception as err:
        print(str(err))
        traceback.print_exc()
        terminate_program(err)


# ---------------------------------------------------------------------------
# Main processing
# ---------------------------------------------------------------------------

def processing():
    """Fetch WOS records, filter against the DOI cache, and write output files."""
    raw_records = search_janelia(ARG.api_key, ARG.START_YEAR)
    seen: set[str] = set()
    results = []
    noauthors = []
    COUNT['total'] = len(raw_records)
    LOGGER.info(f"Retrieved {COUNT['total']} raw records from WOS")
    # Janelia opened in 2006; the year-by-year sweep still lets WOS return some
    # records with an earlier publish year, so drop those per record. The Starter
    # API only exposes a publish year, so compare against the config floor's year.
    min_year = int(DISCONFIG['min_publishing_date'][:4])
    for idx, rec in enumerate(raw_records, 1):
        doi = extract_doi(rec)
        if not doi:
            COUNT['no_doi'] += 1
            continue
        if doi in seen:
            COUNT['skipped_dup'] += 1
            continue
        seen.add(doi)
        pub_year = (rec.get('source') or {}).get('publishYear')
        try:
            if pub_year is not None and int(pub_year) < min_year:
                COUNT['too_early'] += 1
                continue
        except (TypeError, ValueError):
            pass
        if doi in DOI_CACHE:
            if DOI_CACHE[doi] == 'dois':
                COUNT['in_dois'] += 1
            else:
                COUNT['ignored'] += 1
            continue
        print(f"  Crossref lookup {idx}/{COUNT['total']}: {doi}", end="\r")
        crossref_msg = get_crossref_record(doi)
        janelians = janelia_authors(doi, crossref_msg)
        if janelians:
            results.append(extract_record(rec, janelians))
        else:
            noauthors.append(doi)
    COUNT['noauthors'] = len(noauthors)
    summary = (
        f"DOIs read from WOS:             {COUNT['total']:,}\n"
        f"Skipped (no DOI):               {COUNT['no_doi']:,}\n"
        f"Skipped (duplicate in results): {COUNT['skipped_dup']:,}\n"
        f"Skipped (before {min_year}):           {COUNT['too_early']:,}\n"
        f"DOIs already in database:       {COUNT['in_dois']:,}\n"
        f"DOIs to ignore:                 {COUNT['ignored']:,}\n"
        f"DOIs with no Janelia authors:   {len(noauthors):,}\n"
        f"DOIs ready for processing:      {len(results):,}"
    )
    if CAPPED_YEARS:
        dropped = sum(total - MAX_RECORDS for _, total in CAPPED_YEARS)
        yrs = ", ".join(f"{yr} ({total:,} hits)" for yr, total in CAPPED_YEARS)
        summary += (f"\n\nWARNING: {len(CAPPED_YEARS)} year(s) exceeded the "
                    f"{MAX_RECORDS:,}-record cap; ~{dropped:,} records not retrieved.\n"
                    f"  Capped years: {yrs}")
    print(summary)
    if results:
        fname = "janelia_wos_dois.json"
        with open(fname, "w", encoding="utf-8") as fh:
            json.dump(results, fh, indent=2, ensure_ascii=False)
        LOGGER.info(f"Results written to {fname}")
        fname = "wos_ready.txt"
        with open(fname, "w", encoding="utf-8") as fh:
            for rec in results:
                fh.write(rec['doi'] + "\n")
        LOGGER.info(f"Results written to {fname}")
    if noauthors:
        fname = "janelia_wos_noauthors.txt"
        with open(fname, "w", encoding="utf-8") as fh:
            for doi in noauthors:
                fh.write(doi + "\n")
        LOGGER.info(f"Noauthors written to {fname}")
    if ARG.TEST or ARG.WRITE:
        generate_email(results, summary)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    PARSER.add_argument("--api-key", dest="api_key", default=DEFAULT_API_KEY,
                        help="WOS API key ($WOS_API_KEY)")
    PARSER.add_argument("--start-year", dest="START_YEAR", type=int,
                        default=JANELIA_START_YEAR,
                        help=f"First year to search (default: {JANELIA_START_YEAR})")
    PARSER.add_argument("--year", dest="YEAR", type=int,
                        help="Restrict search to a single calendar year")
    PARSER.add_argument('--manifold', dest='MANIFOLD', default='prod',
                        choices=['dev', 'prod'],
                        help='MongoDB manifold (default: prod)')
    PARSER.add_argument('--test', dest='TEST', action='store_true',
                        default=False, help='Send email to developer')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False,
                        help='Live run: email the summary to the receivers list '
                             '(this tool writes only candidate files, never the DB)')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    DISCONFIG = JRC.simplenamespace_to_dict(JRC.get_config("dis"))
    if not ARG.api_key:
        terminate_program("WOS API key required: set $WOS_API_KEY or use --api-key.")
    initialize_program(ARG.MANIFOLD)
    processing()
    terminate_program()
