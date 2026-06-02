"""
Query the Elsevier API for Janelia-affiliated publications and write candidate
DOIs to a local file for downstream ingestion.

Usage:
    python pull_elsevier.py [--test] [--write] [--verbose] [--debug]

Searches the Elsevier Metadata API for articles with a Janelia affiliation,
filtering to records on or after the minimum publishing date configured in the
dis config. Results are paged in batches of 200.

DOIs already present in the MongoDB dois collection, or listed in the
external_dois or to_ignore collections, are excluded from output.

Output files (written to the current working directory):
    elsevier_ready.txt   DOIs not yet in the database, ready for processing.

An HTML summary email is sent when --test or --write is supplied.
"""

__version__ = '2.1.0'

import argparse
import collections
from operator import attrgetter
import sys
import traceback
from tqdm import tqdm
import jrc_common.jrc_common as JRC

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

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
    dbs = ['dis']
    for source in dbs:
        dbo = attrgetter(f"{source}.prod.read")(dbconfig)
        LOGGER.info(f"Connecting to {dbo.name} prod on {dbo.host} as {dbo.user}")
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


def get_janelia_works():
    ''' Get author works
        Keyword arguments:
          None
        Returns:
          List of works
    '''
    suffix = "metadata/article?query=aff%28janelia%29&httpAccept=application/json&count=200"
    rows = set()
    part = 1
    while True:
        try:
            resp = JRC.call_elsevier(suffix)
        except Exception as err:
            terminate_program(err)
        if 'search-results' not in resp:
            terminate_program(f"Unexpected Elsevier response: {resp}")
        for row in resp['search-results'].get('entry', []):
            if 'prism:coverDate' in row and 'prism:doi' in row \
               and row['prism:coverDate'] >= DISCONFIG['min_publishing_date']:
                rows.add(row['prism:doi'].lower())
        print(f"Got part {part}: found {len(rows)} works")
        part += 1
        suffix = None
        if 'link' in resp['search-results']:
            for link in resp['search-results']['link']:
                if link['@ref'] == 'next':
                    suffix = link['@href'].replace('https://api.elsevier.com/content/', '')
        if not suffix:
            break
    LOGGER.debug(f"Found {len(rows)} works")
    return list(rows)


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


def generate_email(summary, to_process):
    ''' Generate and send an email
        Keyword arguments:
          summary: summary of the results
          to_process: list of DOIs to process
        Returns:
          None
    '''
    msg = ""
    if to_process:
        msg += "<br>The following DOIs will be added to the database:<br>"
        for doi in to_process:
            msg += doiurl(doi)
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
        JRC.send_email(msg, DISCONFIG['sender'], email, "Elsevier DOI sync", **opts)
    except Exception as err:
        print(str(err))
        traceback.print_exc()
        terminate_program(err)


def processing():
    ''' Processing
        Keyword arguments:
          None
        Returns:
          None
    '''
    dois = get_janelia_works()
    COUNT['read'] = len(dois)
    to_process = []
    for doi in tqdm(dois, desc="Processing DOIs"):
        if doi in DOI_CACHE:
            if DOI_CACHE[doi] == 'dois':
                COUNT['in_dois'] += 1
            else:
                COUNT['ignored'] += 1
            continue
        to_process.append(doi)
    if to_process:
        with open('elsevier_ready.txt', 'w', encoding='utf-8') as fileout:
            for doi in to_process:
                fileout.write(doi + '\n')
    summary = (
        f"DOIs read from Elsevier:   {COUNT['read']:,}\n"
        f"DOIs already in database:  {COUNT['in_dois']:,}\n"
        f"DOIs to ignore:            {COUNT['ignored']:,}\n"
        f"DOIs ready for processing: {len(to_process):,}"
    )
    print(summary)
    if ARG.TEST or ARG.WRITE:
        generate_email(summary, to_process)
# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Find new works from Elsevier")
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Flag, send emails')
    PARSER.add_argument('--test', dest='TEST', action='store_true',
                        default=False, help='Flag, Test mode')
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
