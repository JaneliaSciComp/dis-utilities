''' pull_openalex_labs.py
    Sync works from OpenAlex for current lab heads.
'''

__version__ = '0.0.1'

import argparse
import collections
from datetime import datetime
from operator import attrgetter
import os
import sys
import requests
from tqdm import tqdm
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Database
DB = {}
# Counters
COUNT = collections.defaultdict(lambda: 0, {})
# Global variables
ARG = DISCONFIG = LOGGER = REST = None
ROR = {}
MESSAGE = {"sent": [], "no_institutions": [], "institution_mismatch": []}
OUTPUT = {"sent": [], "no_institutions": [], "institution_mismatch": []}

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


def call_responder(server, endpoint, timeout=10):
    """ Call a responder
        Keyword arguments:
          server: server
          endpoint: REST endpoint
          timeout: timeout
        Returns:
          JSON response
    """
    url = ((getattr(getattr(REST, server), "url") if server else "") if "REST" in globals() \
           else (os.environ.get('CONFIG_SERVER_URL') if server else "")) + endpoint
    try:
        req = requests.get(url, timeout=timeout)
    except requests.exceptions.RequestException as err:
        terminate_program(f"Could not fetch from {url}\n{str(err)}")
    if req.status_code != 200:
        terminate_program(f"Status: {str(req.status_code)} ({url})")
    return req.json()


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
    try:
        rows = DB['dis']['cvterm'].find({'cv': 'ror'})
    except Exception as err:
        terminate_program(err)
    for row in rows:
        ROR[row['display']] = row['name']


def get_author_works(orcid):
    ''' Get author works
        Keyword arguments:
          orcid: ORCID
        Returns:
          List of works
    '''
    base = f"/works?filter=author.orcid:{orcid}&mailto={DISCONFIG['developer']}&per-page=100&cursor="
    cursor = "*"
    rows = []
    while cursor:
        resp = call_responder('openalex', base + cursor)
        rows.extend(resp['results'])
        cursor = resp['meta']['next_cursor'] if resp['meta']['next_cursor'] else None
    LOGGER.debug(f"Found {len(rows)} works for {orcid}")
    return rows


def janelia_author(row, orcid):
    ''' Check if author is a Janelia author
        Keyword arguments:
          row: row from OpenAlex
          orcid: ORCID
        Returns:
          True if author is a Janelia author
          False otherwise
    '''
    doi = row['doi'].replace('https://doi.org/', '')
    for auth in row['authorships']:
        if not auth['author']['orcid'] or orcid not in auth['author']['orcid']:
            continue
        if not auth['institutions']:
            MESSAGE['no_institutions'].append(f"{doi} {auth['author']['display_name']}")
            OUTPUT['no_institutions'].append(f"{doi}\t{auth['author']['display_name']}\t{row['publication_date']}")
            return False
        for inst in auth['institutions']:
            if ('ror' in inst and inst['ror'] == f"https://ror.org/{ROR['Janelia Research Campus']}") \
               or ('display_name' in inst and 'Janelia' in inst['display_name']):
                MESSAGE['sent'].append(f"{doi} {auth['author']['display_name']}")
                OUTPUT['sent'].append(f"{doi}\t{auth['author']['display_name']}\t{row['publication_date']}")
                return True
        MESSAGE['institution_mismatch'].append(f"{doi} {auth['author']['display_name']}")
        OUTPUT['institution_mismatch'].append(f"{doi}\t{auth['author']['display_name']}\t{row['publication_date']}")
        return False
    return False


def process_author(rec):
    ''' Process author
        Keyword arguments:
          rec: row from ORCID
        Returns:
          None
    '''
    idresp = JRC.call_people_by_id(rec['employeeId'])
    if idresp['departmentAddress1'] != '19700 Helix Drive':
        return
    dto = datetime.strptime(idresp['hireDate'].split(' ')[0], "%m/%d/%Y")
    hired = dto.strftime("%Y-%m-%d")
    rows = get_author_works(rec['orcid'])
    for row in rows:
        COUNT['dois'] += 1
        if hired > row['publication_date'] or row['publication_date'] < '2006-04-01':
            COUNT['skipped'] += 1
            continue
        if not row['doi']:
            COUNT['skipped'] += 1
            continue
        doi = row['doi'].replace('https://doi.org/', '')
        drec = DL.get_doi_record(doi, DB['dis']['dois'])
        if drec:
            COUNT['in_database'] += 1
            continue
        if not janelia_author(row, rec['orcid']):
            COUNT['no_author'] += 1
            continue
        # Additional DOI processing goes here


def generate_emails():
    ''' Generate and send an email
        Keyword arguments:
          None
        Returns:
          None
    '''
    msg = ""
    if MESSAGE['sent']:
        msg += "\nThe following DOIs will be added to the database:"
        for itm in MESSAGE['sent']:
            msg += f"\n  {itm}"
        msg += "\n"
    if MESSAGE['no_institutions']:
        msg += "\nThe following DOIs have no institutions:"
        for itm in MESSAGE['no_institutions']:
            msg += f"\n  {itm}"
        msg += "\n"
    if MESSAGE['institution_mismatch']:
        msg += "\nThe following DOIs have an institution mismatch:"
        for itm in MESSAGE['institution_mismatch']:
            msg += f"\n  {itm}"
    if msg:
        msg = JRC.get_run_data(__file__, __version__) + msg
    else:
        return
    try:
        email = DISCONFIG['developer'] if ARG.TEST else DISCONFIG['receivers']
        LOGGER.info(f"Sending email to {email}")
        JRC.send_email(msg, DISCONFIG['sender'], email, "Lab head DOI sync")
    except Exception as err:
        terminate_program(err)


def processing():
    ''' Processing
        Keyword arguments:
          None
        Returns:
          None
    '''
    payload = {'group': {'$exists': True}, 'workerType': 'Employee',
               'alumni': {'$exists': False}, 'orcid': {'$exists': True}}
    try:
        cnt = DB['dis'].orcid.count_documents(payload)
        LOGGER.info(f"Found {cnt} ORCIDs")
        rows = DB['dis'].orcid.find(payload).sort('group', 1)
    except Exception as err:
        terminate_program(err)
    for row in tqdm(rows, desc="Processing labs", total=cnt):
        process_author(row)
    if MESSAGE['sent']:
        LOGGER.info("Writing openalex_ready.txt")
        with open('openalex_ready.txt', 'w', encoding='ascii') as fileout:
            for itm in MESSAGE['sent']:
                fileout.write(itm.split(' ')[0] + '\n')
    for key in OUTPUT:
        if OUTPUT[key]:
            LOGGER.info(f"Writing openalex_{key}.tsv")
            with open(f"openalex_{key}.tsv", 'w', encoding='utf-8') as fileout:
                for itm in OUTPUT[key]:
                    fileout.write(itm + '\n')
    if ARG.TEST or ARG.WRITE:
        generate_emails()
    print(f"Labs found:                     {cnt}")
    print(f"DOIs found:                     {COUNT['dois']:,}")
    print(f"DOIs skipped:                   {COUNT['skipped']:,}")
    print(f"DOIs with no author:            {COUNT['no_author']:,}")
    print(f"DOIs already in database:       {COUNT['in_database']:,}")
    print(f"DOIs to add:                    {len(MESSAGE['sent']):,}")
    print(f"DOIs with no institutions:      {len(MESSAGE['no_institutions']):,}")
    print(f"DOIs with institution mismatch: {len(MESSAGE['institution_mismatch']):,}")

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Find new works for current lab heads")
    PARSER.add_argument('--test', dest='TEST', action='store_true',
                        default=False, help='Send email to developer')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Flag, Send email')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    DISCONFIG = JRC.simplenamespace_to_dict(JRC.get_config("dis"))
    REST = JRC.get_config("rest_services")
    initialize_program()
    processing()
    terminate_program()
