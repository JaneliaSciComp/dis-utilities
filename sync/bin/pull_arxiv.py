"""
Query the arXiv API for Janelia-affiliated preprints and write candidate
DOIs to local files for downstream ingestion.

Usage:
    python pull_arxiv.py [--manifold dev|prod] [--test] [--write]
                         [--verbose] [--debug]

Searches arXiv for all records mentioning "janelia", pages through results
in batches of 500, and checks each DOI against DataCite to confirm Janelia
authorship via ORCID or affiliation assertion.

DOIs already present in the MongoDB dois, external_dois, or to_ignore
collections are excluded from output.

Output files (written to the current working directory):
    arxiv_ready.txt    DOIs with confirmed Janelia authorship, ready for processing.
    arxiv_review.txt   DOIs with unconfirmed Janelia authorship; require manual review.

An HTML summary email is sent when --test or --write is supplied.
"""

import argparse
import collections
import json
from operator import attrgetter
import re
import sys
import time
import traceback
import requests
from tqdm import tqdm
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

__version__ = '1.0.0'

# Database
DB = {}
DOI_CACHE = {}  # doi -> source collection name
# Counters
COUNT = collections.defaultdict(lambda: 0, {})
# Global variables
ARG = DISCONFIG = LOGGER = None


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


def is_rate_limit_error(err):
    ''' Check if an exception represents a 429 rate-limit response
        Keyword arguments:
          err: exception
        Returns:
          True if rate-limited, False otherwise
    '''
    return (err.args and isinstance(err.args[0], dict)
            and str(err.args[0].get('Status')) == '429')


def call_with_retry(func, *args, max_tries=5, delay=10, **kwargs):
    ''' Call a function with exponential-backoff retry on ReadTimeout or 429
        Keyword arguments:
          func: callable to invoke
          max_tries: maximum attempts
          delay: initial delay in seconds (doubles each retry)
        Returns:
          Return value of func
    '''
    last_err = None
    for attempt in range(1, max_tries + 1):
        try:
            return func(*args, **kwargs)
        except (requests.exceptions.ReadTimeout, Exception) as err:
            if isinstance(err, requests.exceptions.ReadTimeout) or is_rate_limit_error(err):
                last_err = err
                if attempt < max_tries:
                    wait = delay * (2 ** (attempt - 1))
                    reason = "rate limited" if is_rate_limit_error(err) else "ReadTimeout"
                    LOGGER.warning(f"{reason} on {func.__name__} "
                                   f"(attempt {attempt}/{max_tries}), retrying in {wait}s")
                    time.sleep(wait)
            else:
                raise
    raise last_err


def initialize_program():
    ''' Intialize the program
        Keyword arguments:
          None
        Returns:
          None
    '''
    # Database
    try:
        dbconfig = JRC.get_config("databases")
    except Exception as err:
        terminate_program(err)
    dbs = ['dis']
    for source in dbs:
        manifold = ARG.MANIFOLD if source == 'dis' else 'prod'
        dbo = attrgetter(f"{source}.{manifold}.write")(dbconfig)
        LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, manifold, dbo.host, dbo.user)
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)
    build_doi_cache()


def build_doi_cache():
    ''' Pre-load known DOIs from dois, external_dois, and to_ignore collections
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


def doi_exists(doi):
    ''' Check if DOI exists in the database
        Keyword arguments:
          doi: DOI to check
        Returns:
          True if exists, False otherwise
    '''
    return doi in DOI_CACHE


def get_dois_from_arxiv():
    ''' Get DOIs from arXiv
        Keyword arguments:
          None
        Returns:
          List of DOIs
    '''
    offset = 0
    batch_size = 500
    done = False
    check = {}
    parts = 0
    LOGGER.info("Getting DOIs from arXiv")
    while not done:
        post = f"&start={offset}&max_results={batch_size}&sortBy=submittedDate&sortOrder=descending"
        query = f"all:janelia{post}"
        LOGGER.debug(query)
        response = call_with_retry(JRC.call_arxiv, query, timeout=30)
        if response and 'feed' in response and 'entry' in response['feed']:
            entry = response['feed']['entry']
            if parts == 0:
                total = response['feed'].get('opensearch:totalResults', {})
                total = total.get('#text', 'unknown') if isinstance(total, dict) else total
                LOGGER.info(f"arXiv reports {total} total results for this query")
            parts += 1
            LOGGER.debug(f"Part {parts:,} with {len(entry):,} entries")
            if len(entry) < batch_size:
                done = True
            else:
                offset += batch_size
                time.sleep(3)
            for item in entry:
                COUNT['read'] += 1
                if not isinstance(item, dict):
                    LOGGER.error(f"Item is not a dictionary: {item}")
                    continue
                try:
                    doi = item['id'].split('/')[-1]
                except Exception as err:
                    print(json.dumps(item, indent=2))
                    terminate_program(err)
                doi = re.sub(r"v\d+$", "", doi)  # Remove version
                doi = f"10.48550/arxiv.{doi}"
                if doi_exists(doi.lower()):
                    COUNT['in_dois'] += 1
                    continue
                check[doi.lower()] = item
        else:
            done = True
    LOGGER.info(f"Got {len(check):,} DOIs from arXiv in {parts} part(s)")
    return check


def parse_authors(doi, msg, ready, review):
    ''' Parse an author record to see if there are any Janelia authors
        Keyword arguments:
          doi: DOI
          msg: DataCite message
          ready: list of DOIs ready for processing
          review: list of DOIs requiring review
        Returns:
          True if there are Janelia authors, otherwise False
    '''
    adet = DL.get_author_details(msg, DB['dis']['orcid'])
    if adet:
        janelians = []
        mode = None
        for auth in adet:
            if auth['janelian']:
                janelians.append(f"{auth['given']} {auth['family']} ({auth['match']})")
                if auth['match'] in ("ORCID", "asserted"):
                    mode = auth['match']
        if janelians:
            print(f"Janelians found for {doi}: {', '.join(janelians)}")
            if mode:
                COUNT['asserted'] += 1
                ready.append(doi)
            else:
                review.append(doi)
            return True
    return False


def doiurl(doi):
    ''' Format a DOI as an HTML link
        Keyword arguments:
          doi: DOI to format
        Returns:
          Formatted HTML anchor tag
    '''
    return f"&nbsp;&nbsp;<a href='https://dis.int.janelia.org/doiui/{doi}'>{doi}</a><br>"


def text_to_html_table(text):
    ''' Convert colon-delimited text lines to an HTML table
        Keyword arguments:
          text: text to convert
        Returns:
          HTML table string
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


def generate_email(ready, review, summary):
    ''' Generate and send a summary email
        Keyword arguments:
          ready: list of DOIs ready for processing
          review: list of DOIs requiring review
          summary: plain-text run summary
        Returns:
          None
    '''
    msg = ""
    if ready:
        msg += "<br>The following DOIs are ready for processing:<br>"
        for doi in ready:
            msg += doiurl(doi)
        msg += "<br>"
    if review:
        msg += "<br>The following DOIs require review:<br>"
        for doi in review:
            msg += doiurl(doi)
        msg += "<br>"
    if not msg:
        return
    msg = JRC.get_run_data(__file__, __version__) + "<br><br>" \
        + text_to_html_table(summary) + "<br>" + msg
    try:
        email = DISCONFIG['developer'] if ARG.TEST else DISCONFIG['receivers']
        LOGGER.info(f"Sending email to {email}")
        opts = {'mime': 'html'}
        JRC.send_email(msg, DISCONFIG['sender'], email, "arXiv DOI sync", **opts)
    except Exception as err:
        print(str(err))
        traceback.print_exc()
        terminate_program(err)


def run_search():
    ''' Search for DOIs on arXiv that can be added to the dois collection
        Keyword arguments:
          None
        Returns:
          None
    '''
    check = get_dois_from_arxiv()
    ready = []
    review = []
    for doi, item in tqdm(check.items(), desc='DataCite check'):
        resp = call_with_retry(JRC.call_datacite, doi)
        if resp and 'data' in resp:
            janelians = parse_authors(doi, resp['data']['attributes'], ready, review)
            if not janelians:
                COUNT['no_janelians'] += 1
        else:
            COUNT['no_datacite'] += 1
    if ready:
        LOGGER.info("Writing DOIs to arxiv_ready.txt")
        with open('arxiv_ready.txt', 'w', encoding='ascii') as outstream:
            for item in ready:
                outstream.write(f"{item}\n")
    if review:
        LOGGER.info("Writing DOIs to arxiv_review.txt")
        with open('arxiv_review.txt', 'w', encoding='ascii') as outstream:
            for item in review:
                outstream.write(f"{item}\n")
    summary = (
        f"DOIs read from arXiv:            {COUNT['read']:,}\n"
        f"DOIs already in database:        {COUNT['in_dois']:,}\n"
        f"DOIs in DataCite (asserted):     {COUNT['asserted']:,}\n"
        f"DOIs not in DataCite:            {COUNT['no_datacite']:,}\n"
        f"DOIs with no Janelian authors:   {COUNT['no_janelians']:,}\n"
        f"DOIs ready for processing:       {len(ready):,}\n"
        f"DOIs requiring review:           {len(review):,}"
    )
    print(summary)
    if ARG.TEST or ARG.WRITE:
        generate_email(ready, review, summary)

# -----------------------------------------------------------------------------

if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(
        description="Sync DOIs from arXiv")
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, prod)')
    PARSER.add_argument('--test', dest='TEST', action='store_true',
                        default=False, help='Send email to developer only')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Send email to receivers')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    DISCONFIG = JRC.simplenamespace_to_dict(JRC.get_config("dis"))
    run_search()
    terminate_program()
