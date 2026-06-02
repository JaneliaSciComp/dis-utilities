"""
Search the Springer Nature Meta API for Janelia-affiliated publications and
write candidate DOIs to local files for downstream ingestion.

Usage:
    python pull_springer.py [--api-key KEY] [--manifold dev|prod]
                            [--start-year YEAR] [--year YEAR]
                            [--test] [--write] [--verbose] [--debug]

Environment:
    SPRINGER_META_API_KEY   Springer Nature API key (required unless --api-key
                            is passed on the command line).

The Springer Meta API free tier caps at 1000 records per query window.
When a date range exceeds that cap the search is automatically split into
monthly sub-ranges. Years with no hits are skipped.

Janelia Research Campus opened in 2006, so the default start year is 2006.
Pass --year to restrict the search to a single calendar year, or --start-year
to begin the year-by-year sweep from a different year.

DOIs already present in the MongoDB dois, external_dois, or to_ignore
collections are excluded from output.

Output files (written to the current working directory):
    janelia_springer_dois.json      Records confirmed to have Janelia authors.
    springer_ready.txt              Plain list of the same DOIs, one per line.
    janelia_springer_noauthors.txt  DOIs where no Janelia author could be
                                    confirmed; these warrant manual review.

An HTML summary email is sent when --test or --write is supplied.
"""

import argparse
import collections
import json
import os
import sys
import time
import traceback
from datetime import date
import calendar
from operator import attrgetter
import requests
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL


__version__ = '1.0.0'

# Global variables
ARG = DISCONFIG = LOGGER = None
# Database
DB = {}
DOI_CACHE = {}  # doi -> source collection name
# Counters
COUNT = collections.defaultdict(lambda: 0, {})
# General
API_BASE = "https://api.springernature.com/meta/v2/json"
DEFAULT_API_KEY = os.environ.get("SPRINGER_META_API_KEY")
PAGE_SIZE = 25
MAX_START = 1000
JANELIA_START_YEAR = 2006
JANELIA_ROR = "013sk6x84"
USEFUL_FIELDS = [
    "doi", "title", "abstract", "publicationName", "publicationDate",
    "onlineDate", "printDate", "creators", "url", "openaccess", "keyword",
    "subject", "issn", "eissn", "publisher", "volume", "number",
    "startingPage", "endingPage", "genre", "language", "copyright",
]


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
# Springer API helpers
# ---------------------------------------------------------------------------

def fetch_page(query: str, api_key: str, start: int, page_size: int,
               date_from: str = None, date_to: str = None) -> dict:
    """Single API call. Date constraints are embedded in q."""
    q = query
    if date_from:
        q += f" onlinedatefrom:{date_from}"
    if date_to:
        q += f" onlinedateto:{date_to}"
    params = {
        "q": q,
        "p": page_size,
        "s": start,
        "api_key": api_key,
    }
    for attempt in range(5):
        try:
            resp = requests.get(API_BASE, params=params, timeout=30)
            if resp.status_code == 429:
                wait = min(int(resp.headers.get("Retry-After", 10)), 60)
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


def get_total_hits(query: str, api_key: str,
                   date_from: str = None, date_to: str = None) -> int:
    ''' Return the total number of records the API reports for a query.
        Keyword arguments:
          query: Springer Meta API query string
          api_key: Springer Nature API key
          date_from: optional ISO date lower bound (YYYY-MM-DD)
          date_to: optional ISO date upper bound (YYYY-MM-DD)
        Returns:
          Integer hit count
    '''
    page = fetch_page(query, api_key, start=1, page_size=1,
                      date_from=date_from, date_to=date_to)
    return int(page.get("result", [{}])[0].get("total", 0))


def fetch_window(query: str, api_key: str, total: int,
                 date_from: str = None, date_to: str = None) -> list[dict]:
    """Retrieve all pages for a query window that fits within MAX_START."""
    records = []
    start = 1
    retrievable = min(total, MAX_START)
    while start <= retrievable:
        page = fetch_page(query, api_key, start=start, page_size=PAGE_SIZE,
                          date_from=date_from, date_to=date_to)
        batch = page.get("records", [])
        if not batch:
            break
        records.extend(batch)
        start += PAGE_SIZE
        time.sleep(0.25)
    return records


def month_ranges(year: int):
    ''' Yield (date_from, date_to) string pairs for every month in a year.
        Keyword arguments:
          year: calendar year
        Returns:
          Generator of (YYYY-MM-01, YYYY-MM-DD) tuples
    '''
    for month in range(1, 13):
        last_day = calendar.monthrange(year, month)[1]
        yield (f"{year}-{month:02d}-01", f"{year}-{month:02d}-{last_day:02d}")


def fetch_year(query: str, api_key: str, year: int) -> list[dict]:
    ''' Retrieve all Springer records for a query within a calendar year.
        If the year's hit count exceeds MAX_START the search is split into
        monthly windows; months that still exceed MAX_START are capped with
        a warning.
        Keyword arguments:
          query: Springer Meta API query string
          api_key: Springer Nature API key
          year: calendar year to search
        Returns:
          List of raw Springer record dicts
    '''
    date_from = f"{year}-01-01"
    date_to   = f"{year}-12-31"
    total = get_total_hits(query, api_key, date_from=date_from, date_to=date_to)
    if total == 0:
        return []
    print(f"  {year}: {total} hit(s)", end="")
    if total <= MAX_START:
        print(" — fetching as one window")
        return fetch_window(query, api_key, total, date_from=date_from, date_to=date_to)
    print(f" — exceeds {MAX_START} cap, splitting into months")
    records = []
    for mfrom, mto in month_ranges(year):
        month_total = get_total_hits(query, api_key, date_from=mfrom, date_to=mto)
        if month_total == 0:
            continue
        label = mfrom[:7]
        print(f"    {label}: {month_total} hit(s)")
        if month_total > MAX_START:
            print(f"    WARNING: {label} has {month_total} hits, "
                  f"only the first {MAX_START} will be retrieved.")
        records.extend(fetch_window(query, api_key, month_total,
                                    date_from=mfrom, date_to=mto))
        time.sleep(0.1)
    return records


def search_janelia(api_key: str, start_year: int = JANELIA_START_YEAR) -> list[dict]:
    ''' Query the Springer Meta API for all records mentioning "Janelia".
        If the total hit count fits within MAX_START the results are fetched
        in one pass; otherwise the search iterates year by year (or a single
        year when ARG.YEAR is set).
        Keyword arguments:
          api_key: Springer Nature API key
          start_year: earliest year to include in the year-by-year sweep
        Returns:
          List of raw Springer record dicts
    '''
    query = "Janelia"
    current_year = date.today().year
    print(f"Querying Springer Meta API: q={query!r}")
    total_all = get_total_hits(query, api_key)
    print(f"Total hits (all years): {total_all:,}")
    if total_all <= MAX_START:
        print(f"Under {MAX_START:,} — fetching without date splitting\n")
        return fetch_window(query, api_key, total_all)
    if ARG.YEAR:
        print(f"Searching for year: {ARG.YEAR}")
    else:
        print(f"Exceeds {MAX_START:,} — iterating year by year "
              f"({start_year}–{current_year})\n")
    all_records = []
    if ARG.YEAR:
        start_year = current_year = ARG.YEAR
    for year in range(start_year, current_year + 1):
        all_records.extend(fetch_year(query, api_key, year))
    return all_records


# ---------------------------------------------------------------------------
# Field extraction
# ---------------------------------------------------------------------------

def contains_janelia(value) -> bool:
    ''' Return True if the value contains the string "janelia" (case-insensitive).
        Keyword arguments:
          value: string, list, or dict to inspect
        Returns:
          Boolean
    '''
    if isinstance(value, str):
        return "janelia" in value.lower()
    if isinstance(value, (list, dict)):
        return "janelia" in json.dumps(value).lower()
    return False


# ---------------------------------------------------------------------------
# Crossref enrichment
# ---------------------------------------------------------------------------

def get_crossref_record(doi: str) -> dict:
    """Fetch Crossref metadata for a DOI. Returns {} on any failure."""
    try:
        resp = JRC.call_crossref(doi, timeout=20)
        return resp.get('message', {}) if resp else {}
    except Exception:
        return {}



def extract_fields(record: dict, crossref_msg: dict, janelians: list) -> dict:
    ''' Build an output record from a Springer record and its Crossref metadata.
        Copies USEFUL_FIELDS from the Springer record, annotates which fields
        contain "janelia", and lists confirmed Janelian authors by match type.
        Keyword arguments:
          record: raw Springer record dict
          crossref_msg: Crossref "message" dict for the same DOI
          janelians: list of author dicts returned by janelia_authors()
        Returns:
          Dict ready for JSON output
    '''
    out = {}
    for field in USEFUL_FIELDS:
        value = record.get(field)
        if value is not None and value != "" and value != []:
            out[field] = value

    springer_fields = [f for f in record if contains_janelia(record[f])]
    crossref_fields = [f for f in crossref_msg if contains_janelia(crossref_msg[f])]
    out["janelia_found_in"] = springer_fields + crossref_fields

    asserted = [f"{a['given']} {a['family']}" for a in janelians
                if a['match'] == 'asserted']
    current = [f"{a['given']} {a['family']}" for a in janelians
               if a['match'] == 'ORCID']
    if asserted:
        out["janelia_authors_asserted"] = asserted
    if current:
        out["janelia_authors_current"] = current

    return out


def janelia_authors(doi, msg):
    ''' Return a list of Janelian author dicts for a Crossref message,
        or an empty list if none are found.
        Keyword arguments:
          doi: DOI
          msg: Crossref message
        Returns:
          List of author dicts (janelian=True entries from get_author_details)
    '''
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


def generate_email(results, summary):
    ''' Generate and send an email
        Keyword arguments:
          results: list of results
          summary: summary of the results
        Returns:
          None
    '''
    msg = ""
    if results:
        msg += "<br>The following DOIs will be added to the database:<br>"
        for rec in results:
            msg += f"&nbsp;&nbsp;{doiurl(rec['doi'])}<br>"
        msg += "<br>"
    if msg:
        msg = JRC.get_run_data(__file__, __version__) + "<br><br>" \
            + text_to_html_table(summary) + "<br>" + msg
    else:
        return
    try:
        email = DISCONFIG['developer'] if ARG.TEST else DISCONFIG['receivers']
        LOGGER.info(f"Sending email to {email}")
        opts = {'mime': 'html'}
        JRC.send_email(msg, DISCONFIG['sender'], email, "Springer DOI sync", **opts)
    except Exception as err:
        print(str(err))
        traceback.print_exc()
        terminate_program(err)


def processing():
    ''' Fetch Springer records, confirm Janelia authorship via Crossref, and
        write results to janelia_springer_dois.json / springer_ready.txt.
        DOIs that cannot be confirmed are written to
        janelia_springer_noauthors.txt for manual review.
        Keyword arguments:
          None
        Returns:
          None
    '''
    raw_records = search_janelia(ARG.api_key, ARG.START_YEAR)
    seen: set[str] = set()
    results = []
    noauthors = []
    COUNT['total'] = len(raw_records)
    LOGGER.info(f"Retrieved {COUNT['total']} raw records from Springer")
    for idx, rec in enumerate(raw_records, 1):
        doi = rec.get("doi", "").lower()
        if not doi:
            COUNT['no_doi'] += 1
            continue
        if doi in seen:
            COUNT['skipped_dup'] += 1
            continue
        seen.add(doi)
        if doi in DOI_CACHE:
            COUNT['skipped_db'] += 1
            continue
        print(f"  Crossref lookup {idx}/{COUNT['total']}: {doi}", end="\r")
        crossref_msg = get_crossref_record(doi)
        janelians = janelia_authors(doi, crossref_msg)
        if janelians:
            results.append(extract_fields(rec, crossref_msg, janelians))
        else:
            noauthors.append(doi)
    summary = f"DOIs read from Springer:        {COUNT['total']:,}\n" \
              + f"Skipped (no DOI):               {COUNT['no_doi']:,}\n" \
              + f"Skipped (duplicate in results): {COUNT['skipped_dup']:,}\n" \
              + f"Skipped (already in MongoDB):   {COUNT['skipped_db']:,}\n" \
              + f"DOIs ready for processing:      {len(results):,}\n" \
              + f"DOIs with no Janelia authors:   {len(noauthors):,}\n"
    print(summary)
    if results:
        fname = "janelia_springer_dois.json"
        with open(fname, "w", encoding="utf-8") as fh:
            json.dump(results, fh, indent=2, ensure_ascii=False)
        LOGGER.info(f"Results written to {fname}")
        fname = "springer_ready.txt"
        with open(fname, "w", encoding="utf-8") as fh:
            for rec in results:
                fh.write(rec['doi'] + "\n")
        LOGGER.info(f"Results written to {fname}")
    if noauthors:
        fname = "janelia_springer_noauthors.txt"
        with open(fname, "w", encoding="utf-8") as fh:
            for doi in noauthors:
                fh.write(doi + "\n")
        LOGGER.info(f"Noauthors written to {fname}")
    if ARG.TEST or ARG.WRITE:
        generate_email(results, summary)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(description=__doc__)
    PARSER.add_argument("--api-key", dest="api_key", default=DEFAULT_API_KEY,
                        help="Springer Nature API key ($SPRINGER_META_API_KEY)")
    PARSER.add_argument("--start-year", dest="START_YEAR", type=int,
                        default=JANELIA_START_YEAR,
                        help=f"First year to search (default: {JANELIA_START_YEAR})")
    PARSER.add_argument("--year", dest="YEAR", type=int, help="Year to search")
    PARSER.add_argument('--manifold', dest='MANIFOLD', default='prod',
                        choices=['dev', 'prod'],
                        help='MongoDB manifold (default: prod)')
    PARSER.add_argument('--test', dest='TEST', action='store_true',
                        default=False, help='Send email to developer')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Write to database')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    if not ARG.api_key:
        terminate_program("Springer API key required: "
                          "set $SPRINGER_META_API_KEY or use --api-key.")
    initialize_program(ARG.MANIFOLD)
    DISCONFIG = JRC.simplenamespace_to_dict(JRC.get_config("dis"))
    processing()
    terminate_program()

