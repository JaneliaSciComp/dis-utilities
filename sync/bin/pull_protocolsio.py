""" pull_protocolsio.py
    Find DOIs from protocols.io that can be added to the dois collection.
"""

import argparse
import collections
import json
from operator import attrgetter
import os
import re
import sys
from time import sleep
import traceback
from urllib.parse import urlsplit, urlunsplit
from tqdm import tqdm
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL

# pylint: disable=broad-exception-caught,logging-fstring-interpolation
# pylint: disable=too-many-arguments,too-many-positional-arguments

__version__ = "1.0.0"

# Parms
ARG = DISCONFIG = LOGGER = None
# Database
DB = {}
IGNORE = {}
# Counters
COUNT = collections.defaultdict(int)


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
    ''' Intialize the program
        Keyword arguments:
          None
        Returns:
          None
    '''
    # API key
    if "PROTOCOLS_API_TOKEN" not in os.environ:
        terminate_program("Missing token - set in PROTOCOLS_API_TOKEN "
                          "environment variable")
    # Database
    try:
        dbconfig = JRC.get_config("databases")
    except Exception as err:
        terminate_program(err)
    dbs = ['dis']
    for source in dbs:
        manifold = ARG.MANIFOLD if source == 'dis' else 'prod'
        dbo = attrgetter(f"{source}.{manifold}.write")(dbconfig)
        LOGGER.info("Connecting to %s %s on %s as %s",
                    dbo.name, manifold, dbo.host, dbo.user)
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)
    try:
        rows = DB['dis']['to_ignore'].find({"type": "doi"})
    except Exception as err:
        terminate_program(err)
    for row in rows:
        IGNORE[row['key']] = True
    LOGGER.info(f"Found {len(IGNORE):,} DOIs to ignore")


def get_dois_from_protocolsio():
    ''' Get DOIs from protocols.io
        Keyword arguments:
          None
        Returns:
          List of DOIs
    '''
    page = 1
    done = False
    check = {}
    LOGGER.info("Getting DOIs from protocols.io")
    suffix = f"protocols?filter=public&key={ARG.TERM}&page_size=50&fields=doi"
    while not done:
        response = None
        try:
            response = JRC.call_protocolsio(suffix)
        except Exception as err:
            terminate_program(err)
        if response and 'items' in response:
            LOGGER.info(f"Page {page} has {len(response['items']):,} DOIs")
            for item in response['items']:
                doi = item.get('doi')
                if not doi:
                    continue
                doi = re.sub(r'^(https?://)?(dx\.)?doi\.org/', '', doi).lower()
                if doi in check:
                    LOGGER.error("Duplicate DOI found: %s", doi)
                check[doi] = item
        if response and 'pagination' in response:
            next_page = response['pagination'].get('next_page')
            if next_page:
                parsed = urlsplit(next_page)
                suffix = urlunsplit(('', '', parsed.path, parsed.query, ''))
                page += 1
            else:
                done = True
        else:
            done = True
    LOGGER.info(f"Got {len(check):,} DOIs from protocols.io in {page} part(s)")
    return check


def doi_exists(doi):
    ''' Check if DOI exists in the database
        Keyword arguments:
          doi: DOI to check
        Returns:
          True if exists, False otherwise
    '''
    try:
        row = DB['dis']['dois'].find_one({"doi": doi})
    except Exception as err:
        terminate_program(err)
    return bool(row)


def parse_authors(doi, msg, ready, review, nojanelians, alumni):
    ''' Parse an author record to see if there are any Janelia authors
        Keyword arguments:
          doi: DOI
          msg: Crossref message
          ready: list of DOIs ready for processing
          review: list of DOIs requiring review
          nojanelians: list of DOIs with no Janelian authors
          alumni: list of DOIs with alumni authors
        Returns:
          None
    '''
    if 'doi' not in msg:
        msg['doi'] = doi
    try:
        adet = DL.get_author_details(msg, DB['dis']['orcid'])
    except Exception as err:
        LOGGER.error(f"Error getting author details for {doi}: {err}")
        return
    if adet:
        alum = []
        janelians = []
        mode = None
        for auth in adet:
            if auth['janelian']:
                janelians.append(f"{auth['given']} {auth['family']} ({auth['match']})")
                if auth['match'] in ("ORCID", "asserted"):
                    mode = auth['match']
            elif auth['alumni']:
                alum.append(f"{auth['given']} {auth['family']} ({auth['match']})")
        if janelians:
            print(f"Janelians found for {doi}: {', '.join(janelians)}")
            if mode:
                ready.append(doi)
            else:
                review.append(json.dumps(msg, indent=4, default=str))
            return
        if alum:
            alumni.append(json.dumps(msg, indent=4, default=str))
            return
        # DOIs with no Janelia authors are an issue because protocols.io sometimes
        # has the author's middle name as part of the family name. Why?!
        nojanelians.append(json.dumps(msg, indent=4, default=str))


def doimsg(item):
    ''' Format a DOI as a message
        Keyword arguments:
          item: item to format
        Returns:
          Formatted DOI
    '''
    if 'doi' not in item:
        return ""
    doi = item['doi'].lower()
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


def generate_email(summary, ready, review, nojanelians, alumni):
    ''' Generate and send an email
        Keyword arguments:
          summary: summary of the results
          ready: list of DOIs ready for processing
          review: list of DOIs requiring review
          nojanelians: list of DOIs with no Janelian authors
          alumni: list of DOIs with alumni authors
        Returns:
          None
    '''
    msg = ""
    if ready:
        msg += "<br>The following DOIs will be added to the database:<br>"
        for item in ready:
            msg += doimsg({"doi": item})
        msg += "<br>"
    if review:
        msg += "<br>The following DOIs should be reviewed:<br>"
        for item in review:
            item_json = json.loads(item) if isinstance(item, str) else item
            msg += doimsg(item_json)
        msg += "<br>"
    if nojanelians:
        msg += "<br>The following DOIs have no Janelian authors:<br>"
        for item in nojanelians:
            item_json = json.loads(item) if isinstance(item, str) else item
            msg += doimsg(item_json)
        msg += "<br>"
    if alumni:
        msg += "<br>The following DOIs have alumni authors:<br>"
        for item in alumni:
            item_json = json.loads(item) if isinstance(item, str) else item
            msg += doimsg(item_json)
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
        JRC.send_email(msg, DISCONFIG['sender'], email, "Protocols.io DOI sync", **opts)
    except Exception as err:
        print(str(err))
        traceback.print_exc()
        terminate_program(err)


def _write_dois(filename, items):
    if items:
        LOGGER.info("Writing DOIs to %s", filename)
        with open(filename, 'w', encoding='ascii') as outstream:
            outstream.writelines(f"{item}\n" for item in items)


def run_search():
    ''' Search for DOIs on protocols.io that can be added to the dois collection
        Keyword arguments:
          None
        Returns:
          None
    '''
    check = get_dois_from_protocolsio()
    COUNT['read'] = len(check)
    ready = []
    review = []
    nojanelians = []
    alumni = []
    for doi in tqdm(check, desc='Crossref check'):
        if doi in IGNORE:
            COUNT['ignored'] += 1
            continue
        if doi_exists(doi):
            COUNT['in_dois'] += 1
            continue
        resp = JRC.call_crossref(doi)
        if resp and 'message' in resp:
            parse_authors(doi, resp['message'], ready, review, nojanelians, alumni)
            sleep(0.25)
        else:
            COUNT['no_crossref'] += 1
    _write_dois('protocolsio_ready.txt', ready)
    _write_dois('protocolsio_review.txt', review)
    _write_dois('protocolsio_alumni.txt', alumni)
    _write_dois('protocolsio_nojanelians.txt', nojanelians)
    summary = f"DOIs read from protocols.io:   {COUNT['read']:,}\n" \
              + f"DOIs already in database:      {COUNT['in_dois']:,}\n" \
              + f"DOIs to ignore:                {COUNT['ignored']:,}\n" \
              + f"DOIs not in Crossref:          {COUNT['no_crossref']:,}\n" \
              + f"DOIs with no Janelian authors: {len(nojanelians):,}\n" \
              + f"DOIs with alumni authors:      {len(alumni):,}\n" \
              + f"DOIs ready for processing:     {len(ready):,}\n" \
              + f"DOIs requiring review:         {len(review):,}\n"
    print(summary)
    if ARG.TEST or ARG.WRITE:
        generate_email(summary, ready, review, nojanelians, alumni)

# -----------------------------------------------------------------------------

if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(
        description="Sync DOIs from protocols.io")
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, prod)')
    PARSER.add_argument('--term', dest='TERM', action='store',
                        default='Janelia', help='Search term (default: Janelia)')
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
    run_search()
    terminate_program()
