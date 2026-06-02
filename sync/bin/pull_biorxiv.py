"""
Query the bioRxiv API for Janelia-affiliated preprints and write candidate
DOIs to local files for downstream ingestion.

Usage:
    python pull_biorxiv.py [--days N] [--manifold dev|prod] [--test] [--write]
                           [--verbose] [--debug]

Searches bioRxiv for records submitted within the last --days days (default 7),
pages through results in batches of 100, and checks each DOI against Crossref
to confirm Janelia authorship via corresponding institution, ORCID, or
affiliation assertion.

DOIs already present in the MongoDB dois, external_dois, or to_ignore
collections are excluded from output.

Output files (written to the current working directory):
    biorxiv_ready.txt    DOIs with confirmed Janelia authorship, ready for processing.
    biorxiv_review.txt   DOIs with unconfirmed Janelia authorship; require manual review.

An HTML summary email is sent when --test or --write is supplied.
"""

import argparse
import collections
from datetime import date, timedelta
from operator import attrgetter
import sys
import traceback
from time import sleep
from tqdm import tqdm
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL

# pylint: disable=broad-exception-caught,logging-fstring-interpolation,duplicate-code

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


def get_dois_from_biorxiv():
    ''' Get DOIs from bioRxiv
        Keyword arguments:
          None
        Returns:
          List of DOIs
    '''
    start = str(date.today() - timedelta(days=ARG.DAYS))
    stop = str(date.today())
    offset = 0
    done = False
    check = {}
    parts = 0
    LOGGER.info("Getting DOIs from bioRxiv")
    while not done:
        query = f"{start}/{stop}/{offset}"
        response = JRC.call_biorxiv(query, timeout=30)
        if 'messages' in response:
            parts += 1
            if 'count' in response['messages'][0]:
                if response['messages'][0]['count'] < 100:
                    done = True
                else:
                    offset += 100
                    sleep(2)
            else:
                done = True
                continue
        if 'collection' in response:
            for item in response['collection']:
                COUNT['read'] += 1
                if doi_exists(item['doi'].lower()):
                    COUNT['in_dois'] += 1
                    continue
                check[item['doi'].lower()] = item
    LOGGER.info(f"Got {len(check):,} DOIs from bioRxiv in {parts} part(s)")
    return check


def check_corresponding_institution(item, resp, ready):
    ''' Parse an author record to see if there are any Janelia authors
        Keyword arguments:
          item: bioRxiv item
          resp: response from Crossref
          ready: list of DOIs ready for processing
        Returns:
          True or False
    '''

    if 'author_corresponding_institution' in item \
        and 'Janelia' in item['author_corresponding_institution']:
        if resp and 'message' in resp:
            LOGGER.info(f"Janelia found as corresponding institution for {item['doi']}")
            ready.append(item['doi'].lower())
            return True
        COUNT['asserted_crossref'] += 1
        LOGGER.error(f"{item['doi']} with Janelia corresponding institution not in Crossref")
        return True
    return False


def parse_authors(doi, msg, ready, review):
    ''' Parse an author record to see if there are any Janelia authors
        Keyword arguments:
          doi: DOI
          msg: Crossref message
          ready: list of DOIs ready for processing
          review: list of DOIs requiring review
        Returns:
          True if there are Janelia authors, otherwise False
    '''
    if 'doi' not in msg:
        msg['doi'] = doi
    sleep(0.2)
    try:
        adet = DL.get_author_details(msg, DB['dis']['orcid'])
    except Exception as err:
        terminate_program(err)
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
        JRC.send_email(msg, DISCONFIG['sender'], email, "bioRxiv DOI sync", **opts)
    except Exception as err:
        print(str(err))
        traceback.print_exc()
        terminate_program(err)


def run_search():
    ''' Search for DOIs on bioRxiv that can be added to the dois collection
        Keyword arguments:
          None
        Returns:
          None
    '''
    check = get_dois_from_biorxiv()
    ready = []
    review = []
    for doi, item in tqdm(check.items(), desc='Crossref check'):
        sleep(0.1)
        resp = JRC.call_crossref(doi)
        if check_corresponding_institution(item, resp, ready):
            continue
        if resp and 'message' in resp:
            janelians = parse_authors(doi, resp['message'], ready, review)
            if not janelians:
                COUNT['no_janelians'] += 1
        else:
            COUNT['no_crossref'] += 1
    if ready:
        LOGGER.info("Writing DOIs to biorxiv_ready.txt")
        with open('biorxiv_ready.txt', 'w', encoding='ascii') as outstream:
            for item in ready:
                outstream.write(f"{item}\n")
    if review:
        LOGGER.info("Writing DOIs to biorxiv_review.txt")
        with open('biorxiv_review.txt', 'w', encoding='ascii') as outstream:
            for item in review:
                outstream.write(f"{item}\n")
    summary = (
        f"DOIs read from bioRxiv:          {COUNT['read']:,}\n"
        f"DOIs already in database:        {COUNT['in_dois']:,}\n"
        f"DOIs not in Crossref (asserted): {COUNT['asserted_crossref']:,}\n"
        f"DOIs not in Crossref:            {COUNT['no_crossref']:,}\n"
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
        description="Sync DOIs from bioRxiv")
    PARSER.add_argument('--days', dest='DAYS', action='store',
                        default=7, type=int,
                        help='Number of days to go back for DOIs')
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
