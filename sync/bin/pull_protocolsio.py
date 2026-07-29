"""
Query the protocols.io API for Janelia-affiliated protocols and write candidate
DOIs to local files for downstream ingestion.

Usage:
    python pull_protocolsio.py [--term TERM] [--manifold dev|prod]
                               [--test] [--write] [--verbose] [--debug]

Searches protocols.io for public protocols matching --term (default: Janelia),
pages through results in batches of 50, and checks each DOI against Crossref
to confirm Janelia authorship via ORCID or affiliation assertion.

Requires a valid API token in the PROTOCOLS_API_TOKEN environment variable.

DOIs already present in the MongoDB dois, external_dois, or to_ignore
collections are excluded from output.

Output files (written to the current working directory):
    protocolsio_ready.txt        DOIs with confirmed Janelia authorship, ready for processing.
    protocolsio_review.txt       DOIs with unconfirmed Janelia authorship; require manual review.
    protocolsio_alumni.txt       DOIs where only alumni authors were identified.
    protocolsio_nojanelians.txt  DOIs where no Janelia or alumni authors were found.

An HTML summary email is sent when --test or --write is supplied.
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
import jrc_email.jrc_email as JE

# pylint: disable=broad-exception-caught,logging-fstring-interpolation
# pylint: disable=too-many-arguments,too-many-positional-arguments

__version__ = "1.3.0"

# Parms
ARG = DISCONFIG = LOGGER = None
# Database
DB = {}
DOI_CACHE = {}  # doi -> source collection name
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
    if not adet:
        COUNT['no_authors'] += 1
        return
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


def _bucket_dois(items):
    ''' Extract lower-cased (doi, None) entries from a protocols.io bucket whose
        items may be dicts or JSON strings.
        Keyword arguments:
          items: list of dicts or JSON strings carrying a 'doi'
        Returns:
          list of (doi, None) tuples for doi_card
    '''
    entries = []
    for item in items:
        rec = json.loads(item) if isinstance(item, str) else item
        if isinstance(rec, dict) and rec.get('doi'):
            entries.append((rec['doi'].lower(), None))
    return entries


def generate_email(summary, ready, review, nojanelians, alumni):
    ''' Generate and send the HTML run-summary email (jrc_email house style): a
        header banner, a KPI stat-tile row, a "Ready to Add" card, and - when any -
        "Requiring Review", "Alumni Authors", and "No Janelian Authors" cards.
        Recipient is the developer for --test and the receivers list otherwise; a
        --write run with nothing ready or to review sends nothing.
        Keyword arguments:
          summary: text summary (printed to the console; not used in the email body)
          ready: list of DOIs ready for processing
          review: list of records requiring review
          nojanelians: list of records with no Janelian authors
          alumni: list of records whose only Janelian authors are alumni
        Returns:
          None
    '''
    if not ready and not review and ARG.WRITE:
        return
    if not (ready or review or nojanelians or alumni):
        return
    run_data = JRC.get_run_data(__file__, __version__).strip()
    mode_label = 'TEST' if ARG.TEST else 'LIVE'
    mode_tone = 'warn' if ARG.TEST else 'good'
    kpis = ''.join([
        JE.kpi_card(f"{COUNT['read']:,}", "Read from protocols.io"),
        JE.kpi_card(f"{COUNT['in_dois']:,}", "Already in DB"),
        JE.kpi_card(f"{len(nojanelians):,}", "No Janelian"),
        JE.kpi_card(f"{len(review):,}", "Requiring review",
                    'warn' if review else 'neutral'),
        JE.kpi_card(f"{len(ready):,}", "Ready to add",
                    'good' if ready else 'neutral'),
    ])
    ready_entries = [(doi.lower(), None) for doi in ready]
    ready_body = (JE.doi_card("Ready to Add", ready_entries, 'good')
                  if ready_entries else
                  f'<div style="color:{JE.GRAY};font-size:13px;">'
                  'No new DOIs are ready to add.</div>')
    body = JE.body_row(JE.section_header(f"&#10003; Ready to Add ({len(ready):,})")
                       + ready_body)
    review_entries = _bucket_dois(review)
    if review_entries:
        card = (JE.section_header(f"&#9888; Requiring Review ({len(review_entries):,})")
                + JE.doi_card("Requiring Review", review_entries, 'warn', icon='&#9888;'))
        body += JE.body_row(card, '6px 28px 4px 28px')
    alumni_entries = _bucket_dois(alumni)
    if alumni_entries:
        card = (JE.section_header(f"&#127891; Alumni Authors ({len(alumni_entries):,})")
                + f'<div style="color:{JE.GRAY};font-size:12px;margin:-4px 0 10px 0;">'
                'The only Janelian author(s) are alumni (former staff) - confirm before '
                'ingesting.</div>'
                + JE.doi_card("Alumni Authors", alumni_entries, 'warn', icon='&#127891;'))
        body += JE.body_row(card, '6px 28px 4px 28px')
    nojanelians_entries = _bucket_dois(nojanelians)
    if nojanelians_entries:
        card = (JE.section_header(f"&#128683; No Janelian Authors "
                                  f"({len(nojanelians_entries):,})")
                + JE.doi_card("No Janelian Authors", nojanelians_entries, 'warn',
                              icon='&#128683;'))
        body += JE.body_row(card, '6px 28px 4px 28px')
    msg = JE.render(os.path.basename(__file__), __version__, run_data,
                    mode_label, mode_tone, kpis, body)
    try:
        email = DISCONFIG['developer'] if ARG.TEST else DISCONFIG['receivers']
        LOGGER.info(f"Sending email to {email}")
        JRC.send_email(msg, DISCONFIG['sender'], email, "Protocols.io DOI sync", mime='html')
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
        if doi in DOI_CACHE:
            if DOI_CACHE[doi] == 'dois':
                COUNT['in_dois'] += 1
            else:
                COUNT['ignored'] += 1
            continue
        resp = JRC.call_crossref(doi)
        sleep(0.25)
        if resp and 'message' in resp:
            parse_authors(doi, resp['message'], ready, review, nojanelians, alumni)
        else:
            COUNT['no_crossref'] += 1
    _write_dois('protocolsio_ready.txt', ready)
    _write_dois('protocolsio_review.txt', review)
    _write_dois('protocolsio_alumni.txt', alumni)
    _write_dois('protocolsio_nojanelians.txt', nojanelians)
    summary = (
        f"DOIs read from protocols.io:   {COUNT['read']:,}\n"
        f"DOIs already in database:      {COUNT['in_dois']:,}\n"
        f"DOIs to ignore:                {COUNT['ignored']:,}\n"
        f"DOIs not in Crossref:          {COUNT['no_crossref']:,}\n"
        f"DOIs with no author data:      {COUNT['no_authors']:,}\n"
        f"DOIs with no Janelian authors: {len(nojanelians):,}\n"
        f"DOIs with alumni authors:      {len(alumni):,}\n"
        f"DOIs ready for processing:     {len(ready):,}\n"
        f"DOIs requiring review:         {len(review):,}"
    )
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
