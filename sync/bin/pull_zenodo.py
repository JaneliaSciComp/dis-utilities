''' pull_zenodo.py
    Sync works from Zenodo.

    Finds Zenodo records matching the search term whose creators have a Janelia
    affiliation or a known Janelian ORCID, writes the new DOIs to zenodo_ready.txt,
    and sends an HTML run-summary email (jrc_email house style) on --test (to the
    developer) or --write (to the receivers list).
'''

import argparse
import collections
import json
from operator import attrgetter
import os
import sys
from time import sleep
import traceback
import requests
import jrc_common.jrc_common as JRC
import jrc_email.jrc_email as JE

__version__ = '1.5.0'

# pylint: disable=broad-exception-caught,broad-exception-raised,logging-fstring-interpolation

# Database
DB = {}
DOI_CACHE = {}  # doi -> source collection name
# Counters
COUNT = collections.defaultdict(lambda: 0, {})
# Global variables
ARG = DISCONFIG = LOGGER = None
NOJANELIA = []

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
    ''' Initialize the program
        Keyword arguments:
          None
        Returns:
          None
    '''
    if "ZENODO_API_KEY" not in os.environ:
        terminate_program("Missing API key - set in ZENODO_API_KEY environment variable")
    try:
        dbconfig = JRC.get_config("databases")
    except Exception as err:
        terminate_program(err)
    dbs = ['dis']
    for source in dbs:
        dbo = attrgetter(f"{source}.{ARG.MANIFOLD}.write")(dbconfig)
        LOGGER.info(f"Connecting to {dbo.name} {ARG.MANIFOLD} on {dbo.host} as {dbo.user}")
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)
    build_doi_cache()


def build_doi_cache():
    ''' Pre-load known DOIs from the dois and to_ignore collections
        Keyword arguments:
          None
        Returns:
          None
    '''
    try:
        for rec in DB['dis']['dois'].find({}, {"doi": 1}):
            if rec.get('doi'):
                DOI_CACHE[rec['doi'].lower()] = 'dois'
        for rec in DB['dis']['to_ignore'].find({"type": "doi"}, {"key": 1}):
            if rec.get('key'):
                DOI_CACHE[rec['key'].lower()] = 'to_ignore'
    except Exception as err:
        terminate_program(err)
    LOGGER.info(f"Loaded {len(DOI_CACHE):,} known DOIs into cache")


def get_janelia_works():
    ''' Get DOIs from Zenodo
        Keyword arguments:
          None
        Returns:
          List of records
    '''
    rows = []
    part = 1
    url = "https://zenodo.org/api/records"
    params = {'q': ARG.TERM, 'size': 100}
    if ARG.ALL_VERSIONS:
        # Zenodo returns only the latest version of each record concept by default;
        # this asks for every version.
        params['all_versions'] = 'true'
    while True:
        try:
            response = requests.get(url, params=params, timeout=20,
                                    headers={'Authorization':
                                             f'Bearer {os.environ["ZENODO_API_KEY"]}'})
        except Exception as err:
            terminate_program(err)
        if not response:
            terminate_program(f"Error in response from Zenodo: {response}")
        try:
            resp = response.json()
        except Exception as err:
            terminate_program(f"Could not parse Zenodo response: {err}")
        if 'hits' not in resp or not resp['hits']:
            LOGGER.warning("No hits returned from Zenodo")
            break
        print(f"{len(resp['hits']['hits'])}/{resp['hits']['total']}")
        rows.extend(resp['hits']['hits'])
        print(f"Got part {part}: found {len(rows)} works")
        part += 1
        params = None
        if 'links' not in resp or 'next' not in resp['links'] or not resp['links']['next']:
            break
        url = resp['links']['next']
        sleep(.25)
    return rows


def get_dois():
    ''' Get DOIs from Zenodo
        Keyword arguments:
          None
        Returns:
          List of DOIs
    '''
    try:
        rows = DB['dis'].orcid.find({'orcid': {'$exists': True}, 'workerType': 'Employee'})
    except Exception as err:
        terminate_program(err)
    orcids = {row['orcid'] for row in rows}
    LOGGER.info(f"ORCIDs: {len(orcids):,}")
    rows = get_janelia_works()
    dois = set()
    for hit in rows:
        COUNT['read'] += 1
        doi = hit.get('doi', '').lower()
        if not doi:
            continue
        if 'metadata' not in hit or 'creators' not in hit['metadata']:
            continue
        found = False
        for creator in hit['metadata']['creators']:
            if 'affiliation' in creator and creator['affiliation']:
                if 'Janelia' in creator['affiliation']:
                    dois.add(doi)
                    COUNT['found'] += 1
                    found = True
                    break
            if 'orcid' in creator and creator['orcid'] and creator['orcid'] in orcids:
                dois.add(doi)
                COUNT['found'] += 1
                found = True
                break
        if not found:
            NOJANELIA.append(hit)
    return dois


def generate_email(summary, ready, nojanelia):
    ''' Generate and send the HTML run-summary email (jrc_email house style): a
        header banner, a KPI stat-tile row, a "Ready to Add" card, and - when any -
        a "No Janelia Affiliation" review card. Recipient is the developer for
        --test and the receivers list otherwise; a --write run with nothing ready
        sends nothing.
        Keyword arguments:
          summary: text summary (printed to the console; not used in the email body)
          ready: list of DOIs ready for processing
          nojanelia: list of Zenodo records with no Janelian author
        Returns:
          None
    '''
    if not ready and ARG.WRITE:
        return
    if not ready and not nojanelia:
        return
    run_data = JRC.get_run_data(__file__, __version__).strip()
    mode_label = 'TEST' if ARG.TEST else 'LIVE'
    mode_tone = 'warn' if ARG.TEST else 'good'
    review = [(hit.get('doi', '').lower(), None) for hit in nojanelia if hit.get('doi')]
    kpis = ''.join([
        JE.kpi_card(f"{COUNT['read']:,}", "In Zenodo"),
        JE.kpi_card(f"{COUNT['already']:,}", "Already in DB"),
        JE.kpi_card(f"{COUNT['ignored']:,}", "To ignore"),
        JE.kpi_card(f"{len(nojanelia):,}", "No Janelia affil.",
                    'warn' if nojanelia else 'neutral'),
        JE.kpi_card(f"{len(ready):,}", "Ready to add",
                    'good' if ready else 'neutral'),
    ])
    ready_body = (JE.doi_card("Ready to Add", [(doi, None) for doi in ready], 'good')
                  if ready else
                  f'<div style="color:{JE.GRAY};font-size:13px;">'
                  'No new DOIs are ready to add.</div>')
    body = JE.body_row(JE.section_header(f"&#10003; Ready to Add ({len(ready):,})")
                       + ready_body)
    if review:
        card = (JE.section_header(f"&#9888; No Janelia Affiliation ({len(review):,})")
                + f'<div style="color:{JE.GRAY};font-size:12px;margin:-4px 0 10px 0;">'
                'Matched the Zenodo search but no creator had a Janelia affiliation or a '
                'known Janelian ORCID - review before ingesting.</div>'
                + JE.doi_card("No Janelia Affiliation", review, 'warn', icon='&#9888;'))
        body += JE.body_row(card, '6px 28px 4px 28px')
    msg = JE.render(os.path.basename(__file__), __version__, run_data,
                    mode_label, mode_tone, kpis, body)
    try:
        email = DISCONFIG['developer'] if ARG.TEST else DISCONFIG['receivers']
        LOGGER.info(f"Sending email to {email}")
        JRC.send_email(msg, DISCONFIG['sender'], email, "Zenodo DOI sync", mime='html')
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
    dois = get_dois()
    to_process = []
    for doi in dois:
        if doi in DOI_CACHE:
            if DOI_CACHE[doi] == 'dois':
                COUNT['already'] += 1
            else:
                COUNT['ignored'] += 1
            continue
        to_process.append(doi)
    COUNT['ready'] = len(to_process)
    if to_process:
        with open('zenodo_ready.txt', 'w', encoding='utf-8') as fileout:
            for doi in to_process:
                fileout.write(doi + '\n')
    if NOJANELIA:
        with open('zenodo_nojanelia.json', 'w', encoding='utf-8') as fileout:
            fileout.write(json.dumps(NOJANELIA, indent=4, default=str))
    summary = (
        f"DOIs in Zenodo:                   {COUNT['read']:,}\n"
        f"Janelia DOIs:                     {COUNT['found']:,}\n"
        f"Already in database:              {COUNT['already']:,}\n"
        f"DOIs to ignore:                   {COUNT['ignored']:,}\n"
        f"DOIs with no Janelia affiliation: {len(NOJANELIA):,}\n"
        f"DOIs ready for processing:        {COUNT['ready']:,}"
    )
    print(summary)
    if ARG.TEST or ARG.WRITE:
        generate_email(summary, to_process, NOJANELIA)

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Sync works from Zenodo into the MongoDB dois collection")
    PARSER.add_argument('--term', dest='TERM', action='store',
                        default='Janelia', help='Search term')
    PARSER.add_argument('--all-versions', dest='ALL_VERSIONS', action='store_true',
                        default=False,
                        help='Flag, include all versions of each record (not just the latest)')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, prod)')
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
