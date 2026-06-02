"""
Query the figshare REST API for Janelia-affiliated articles and write
candidate DOIs to a local file for downstream ingestion.

Usage:
    python pull_figshare.py [--manifold dev|prod] [--test] [--write]
                            [--verbose] [--debug]

Configuration:
    Requires a config.ini file in the current working directory containing
    a [figshare] section with the following keys:
        base        Base URL for the figshare articles API including page-size
                    and search parameters.
        institution URL fragment used when querying by institution ID.
        group       URL fragment used when querying by group ID.

    Example:
        [figshare]
        base        = https://api.figshare.com/v2/articles?page_size=500&...
        institution = &institution=
        group       = &group_id=

Currently searches institution 295 (Janelia Research Campus). The commented-
out group loop (groups 11380 and 49461) can be re-enabled if group-level
queries are preferred or needed in addition.

DOIs are paged in batches of 500. Articles whose DOI prefix is 10.25378 are
counted as Janelia-originating. DOIs already present in the MongoDB dois,
external_dois, or to_ignore collections are excluded from output.

Output files (written to the current working directory):
    figshare_ready.txt         DOIs confirmed in DataCite, ready for processing.
    figshare_no_datacite.txt   DOIs with no DataCite record; require manual review.

An HTML summary email is sent when --test or --write is supplied.
"""

import argparse
import collections
import configparser
from operator import attrgetter
import sys
import time
import traceback
import requests
import jrc_common.jrc_common as JRC

# pylint: disable=broad-exception-caught,logging-fstring-interpolation,duplicate-code

__version__ = '1.0.0'

# Database
DB = {}
DOI_CACHE = {}  # doi -> source collection name
COUNT = collections.defaultdict(lambda: 0, {})
JANELIA_INSTITUTION = 295
JANELIA_DOI_PREFIX = "10.25378"
# Global variables
ARG = CONFIG = DISCONFIG = LOGGER = None


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


def initialize_program():
    ''' Initialize database connection
        Keyword arguments:
          None
        Returns:
          None
    '''
    try:
        dbconfig = JRC.get_config("databases")
    except Exception as err:
        terminate_program(err)
    dbo = attrgetter(f"dis.{ARG.MANIFOLD}.write")(dbconfig)
    LOGGER.info("Connecting to %s %s on %s as %s",
                dbo.name, ARG.MANIFOLD, dbo.host, dbo.user)
    try:
        DB['dis'] = JRC.connect_database(dbo)
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


def get_datacite_record(doi):
    ''' Fetch a DataCite record for a DOI.
        Keyword arguments:
          doi: DOI to look up
        Returns:
          DataCite record dict, or {} if not found or on error
    '''
    try:
        return JRC.call_datacite(doi)
    except Exception:
        return {}


def fetch_page(url):
    ''' Fetch one page from the figshare API with retry on timeout.
        Keyword arguments:
          url: full request URL including offset parameter
        Returns:
          requests.Response object
    '''
    for attempt in range(3):
        try:
            return requests.get(url, timeout=10)
        except requests.exceptions.Timeout:
            if attempt == 2:
                terminate_program("Figshare API timed out after 3 attempts")
            time.sleep(2 ** attempt)
        except requests.exceptions.RequestException as exc:
            terminate_program(str(exc))
    return None


def pull_single_group(dois, institution=None, group=None):
    ''' Pull DOIs for one figshare institution or group.
        Keyword arguments:
          dois: list to append new DOIs to
          institution: figshare institution ID
          group: figshare group ID
        Returns:
          None
    '''
    if institution is None and group is None:
        terminate_program("pull_single_group requires institution or group")
    if institution is not None:
        stype = "institution"
        sterm = institution
    else:
        stype = "group"
        sterm = group
    base = f"{CONFIG['figshare']['base']}{CONFIG['figshare'][stype]}{sterm}"
    offset = 0
    parts = 0
    checked = 0
    LOGGER.info(f"Getting DOIs from figshare for {stype} {sterm}")
    while True:
        resp = fetch_page(f"{base}&offset={offset}")
        if not resp or resp.status_code != 200:
            break
        parts += 1
        data = resp.json()
        if not data:
            break
        for art in data:
            doi = art.get('doi', '').lower()
            if not doi:
                COUNT['no_doi'] += 1
                continue
            checked += 1
            COUNT['checked'] += 1
            if doi.startswith(JANELIA_DOI_PREFIX):
                COUNT['janelia'] += 1
            if doi_exists(doi):
                COUNT['in_dois'] += 1
            else:
                dois.append(doi)
        offset += 500
    LOGGER.info(f"Checked {checked:,} DOIs from figshare in {parts} part(s)")


def doiurl(doi, mode='doi'):
    ''' Format a DOI as an HTML link
        Keyword arguments:
          doi: DOI to format
          mode: 'figshare' for raw figshare link, otherwise standard doiui link
        Returns:
          Formatted HTML anchor tag
    '''
    if mode == 'figshare':
        return f"&nbsp;&nbsp;<a href='https://dis.int.janelia.org/raw/figshare/{doi}'>{doi}</a><br>"
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


def generate_email(ready, no_datacite, summary):
    ''' Generate and send a summary email
        Keyword arguments:
          ready: list of DOIs ready for processing
          no_datacite: list of DOIs not found in DataCite
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
    if no_datacite:
        msg += "<br>The following DOIs are not in DataCite:<br>"
        for doi in no_datacite:
            msg += doiurl(doi, mode='figshare')
        msg += "<br>"
    if not msg:
        return
    msg = JRC.get_run_data(__file__, __version__) + "<br><br>" \
        + text_to_html_table(summary) + "<br>" + msg
    try:
        email = DISCONFIG['developer'] if ARG.TEST else DISCONFIG['receivers']
        LOGGER.info(f"Sending email to {email}")
        opts = {'mime': 'html'}
        JRC.send_email(msg, DISCONFIG['sender'], email, "figshare DOI sync", **opts)
    except Exception as err:
        print(str(err))
        traceback.print_exc()
        terminate_program(err)


def pull_figshare():
    ''' Orchestrate the figshare pull and write results to output files.
        Calls pull_single_group for each configured institution or group,
        deduplicates against MongoDB, verifies each DOI exists in DataCite,
        and prints a run summary.
        Keyword arguments:
          None
        Returns:
          None
    '''
    dois = []
    pull_single_group(dois, institution=JANELIA_INSTITUTION)
    #for group in (11380, 49461):
    #    pull_single_group(dois, group=group)
    ready = []
    no_datacite = []
    for doi in dois:
        if get_datacite_record(doi):
            ready.append(doi)
        else:
            no_datacite.append(doi)
            COUNT['no_datacite'] += 1
    if ready:
        fname = "figshare_ready.txt"
        LOGGER.info(f"Writing {len(ready):,} DOIs to {fname}")
        with open(fname, "w", encoding="utf-8") as outstream:
            for doi in ready:
                outstream.write(f"{doi}\n")
    if no_datacite:
        fname = "figshare_no_datacite.txt"
        LOGGER.info(f"Writing {len(no_datacite):,} DOIs not in DataCite to {fname}")
        with open(fname, "w", encoding="utf-8") as outstream:
            for doi in no_datacite:
                outstream.write(f"{doi}\n")
    summary = (
        f"DOIs read from figshare:   {COUNT['checked']:,}\n"
        f"Janelia DOIs:              {COUNT['janelia']:,}\n"
        f"DOIs already in database:  {COUNT['in_dois']:,}\n"
        f"DOIs not in DataCite:      {COUNT['no_datacite']:,}\n"
        f"DOIs ready for processing: {len(ready):,}"
    )
    print(summary)
    if ARG.TEST or ARG.WRITE:
        generate_email(ready, no_datacite, summary)

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Pull resources from figshare")
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
    CONFIG = configparser.ConfigParser()
    CONFIG.read('config.ini')
    if 'figshare' not in CONFIG:
        terminate_program("config.ini is missing or has no [figshare] section")
    pull_figshare()
