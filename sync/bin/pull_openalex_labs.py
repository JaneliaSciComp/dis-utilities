''' pull_openalex_labs.py
    Sync works from OpenAlex for current lab heads.
    DOIs are added to the database if the following conditions are met:
    - The work has an author who is a current lab head
    - The work has a publication date after the lab head's hire date
    - The lab head (or any other author) has a Janelia affiliation
'''

__version__ = '4.0.0'

import argparse
import collections
from datetime import datetime
from operator import attrgetter
import os
import sys
from time import sleep
import traceback
import requests
from tqdm import tqdm
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL

# pylint: disable=broad-exception-caught,broad-exception-raised,logging-fstring-interpolation

# Database
DB = {}
# Counters
COUNT = collections.defaultdict(lambda: 0, {})
# Global variables
ARG = DISCONFIG = LOGGER = REST = None
IGNORE = []
ROR = {}
MESSAGE = {"sent": [], "no_institutions": [], "institution_mismatch": []}
OUTPUT = {"sent": {}, "no_institutions": {}, "institution_mismatch": {}}
OAID = {}

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
    if req.status_code == 429:
        raise Exception("Rate limit exceeded")
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
    try:
        rows = DB['dis']['to_ignore'].find({})
    except Exception as err:
        terminate_program(err)
    for row in rows:
        IGNORE.append(row['key'])
    LOGGER.info(f"Found {len(IGNORE):,} DOIs to ignore")


def get_team_projects(orcids):
    ''' Get ORCID records for team project managers
        Keyword arguments:
          orcids: list of lab head ORCIDs
        Returns:
          List of team project manager ORCID records
    '''
    dis = JRC.simplenamespace_to_dict(JRC.get_config('dis'))
    payload = {'managed': {"$in": dis['team_projects']}, 'alumni': {'$exists': False},
               'orcid': {'$exists': True}}
    try:
        rows = DB['dis'].orcid.find(payload).sort('group', 1)
    except Exception as err:
        terminate_program(err)
    managers = []
    for row in rows:
        if row['orcid'] not in orcids:
            orcids.append(row['orcid'])
            managers.append(row)
    return managers


def get_author_works(orcid, author):
    ''' Get author works
        Keyword arguments:
          orcid: ORCID
          author: author name
        Returns:
          List of works
    '''
    base = f"/works?filter=author.orcid:{orcid}&mailto={DISCONFIG['developer']}" \
           + "&per-page=200&cursor="
    cursor = "*"
    rows = []
    cnt = 1
    while cursor:
        sleep(0.12)
        try:
            resp = call_responder('openalex', base + cursor)
            rows.extend(resp['results'])
        except Exception as err:
            terminate_program(f"Error getting OpenAlex works for {author} on call {cnt}: {err}")
        cnt += 1
        cursor = resp['meta']['next_cursor'] if resp['meta']['next_cursor'] else None
    LOGGER.debug(f"Found {len(rows)} works for {orcid}")
    return rows


def janelia_affiliation(inst):
    ''' Check if institution is a Janelia affiliation
        Keyword arguments:
          inst: institution
        Returns:
          True if institution is a Janelia affiliation
          False otherwise
    '''
    return ('ror' in inst \
        and inst['ror'] == f"https://ror.org/{ROR['Janelia Research Campus']}") \
       or ('display_name' in inst and 'Janelia' in inst['display_name'])


def get_title(row):
    ''' Get a work title
        Keyword arguments:
          row: row from OpenAlex
        Returns:
          title
    '''
    if 'title' in row and row['title']:
        return row['title']
    if 'titles' in row and row['titles'] and 'title' in row['titles'][0]:
        return row['titles'][0]['title']
    return ""


def janelia_author(row, orcid, doi):
    ''' Check if author is a Janelia author
        Keyword arguments:
          row: row from OpenAlex
          orcid: ORCID
          doi: DOI
        Returns:
          True if author is a Janelia author
          False otherwise
    '''
    OAID[doi] = row['id'].split('/')[-1]
    # Find lab head Janelia affiliation
    for auth in row['authorships']:
        if not auth['author']['orcid'] or orcid not in auth['author']['orcid']:
            continue
        if auth['institutions']:
            for inst in auth['institutions']:
                if janelia_affiliation(inst):
                    if doi not in OUTPUT['sent']:
                        OUTPUT['sent'][doi] = [auth['author']['display_name'],
                                               row['publication_date'], get_title(row)]
                    return True
    for auth in row['authorships']:
        # Check if *any* author is from Janelia
        if auth['institutions']:
            for inst in auth['institutions']:
                if janelia_affiliation(inst):
                    if doi not in OUTPUT['sent']:
                        OUTPUT['sent'][doi] = [auth['author']['display_name'],
                                               row['publication_date'], get_title(row)]
                    return True
        # The next check is for the lab head ORCID only
        if not auth['author']['orcid'] or orcid not in auth['author']['orcid']:
            continue
        if not auth['institutions']:
            if doi not in OUTPUT['no_institutions']:
                OUTPUT['no_institutions'][doi] = [auth['author']['display_name'],
                                                  row['publication_date'], get_title(row)]
            return False
        if doi not in OUTPUT['institution_mismatch']:
            institutions = ', '.join([inst['display_name'] for inst in auth['institutions']])
            OUTPUT['institution_mismatch'][doi] = [auth['author']['display_name'],
                                                   row['publication_date'], get_title(row),
                                                   institutions]
        return False
    return False


def process_author(rec):
    ''' Process author
        Keyword arguments:
          rec: row from ORCID
        Returns:
          None
    '''
    hired = ''
    if not ARG.ALUMNI:
        idresp = JRC.call_people_by_id(rec['employeeId'])
        if not idresp:
            terminate_program(f"No People record for {rec['given'][0]} {rec['family'][0]}")
        if 'departmentAddress1' in idresp and idresp['departmentAddress1'] != '19700 Helix Drive':
            return
        dto = datetime.strptime(idresp['hireDate'].split(' ')[0], "%m/%d/%Y")
        hired = dto.strftime("%Y-%m-%d")
    author = f"{rec['given'][0]} {rec['family'][0]}"
    rows = get_author_works(rec['orcid'], author)
    LOGGER.debug(f"{author} {len(rows)} rows")
    for row in tqdm(rows, desc=author, position=tqdm._get_free_pos(), leave=False, total=len(rows)):
        sleep(0.05)
        COUNT['dois'] += 1
        if not ARG.ALUMNI and (hired > row['publication_date'] \
            or row['publication_date'] < DISCONFIG['min_publishing_date']):
            LOGGER.debug(f"Skipping {row['doi']} ({row['publication_date']})")
            COUNT['skipped'] += 1
            continue
        if 'doi' in row and row['doi']:
            doi = row['doi'].replace('https://doi.org/', '')
        else:
            if 'best_oa_location' in row and row['best_oa_location'] \
               and 'landing_page_url' in row['best_oa_location'] \
               and row['best_oa_location']['landing_page_url'] \
               and row['best_oa_location']['landing_page_url'].startswith('https://doi.org/'):
                doi = row['best_oa_location']['landing_page_url'].replace('https://doi.org/', '')
                LOGGER.debug(f"Using best_oa_location DOI: {doi}")
            else:
                COUNT['no_doi'] += 1
                continue
        if doi in IGNORE:
            COUNT['ignored'] += 1
            continue
        drec = DL.get_doi_record(doi, DB['dis']['dois'])
        if drec:
            IGNORE.append(doi)
            COUNT['in_database'] += 1
            continue
        if not janelia_author(row, rec['orcid'], doi):
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
        msg += "<br>The following DOIs will be added to the database:<br>"
        for itm in MESSAGE['sent']:
            msg += f"  {itm}<br>"
        msg += "\n"
    if MESSAGE['no_institutions']:
        msg += "<br>The following DOIs have no institutions:<br>"
        for itm in MESSAGE['no_institutions']:
            msg += f"  {itm}<br>"
        msg += "\n"
    if MESSAGE['institution_mismatch']:
        msg += "<br>The following DOIs have an institution mismatch (also see attached file):<br>"
        for itm in MESSAGE['institution_mismatch']:
            msg += f"  {itm}<br>"
    if msg:
        msg = JRC.get_run_data(__file__, __version__) + "<br>" + msg
    else:
        return
    try:
        email = DISCONFIG['developer'] if ARG.TEST else DISCONFIG['receivers']
        LOGGER.info(f"Sending email to {email}")
        opts = {'mime': 'html'}
        if MESSAGE['institution_mismatch']:
            opts['attachment'] = 'openalex_institution_mismatch.tsv'
        JRC.send_email(msg, DISCONFIG['sender'], email, "Lab head DOI sync", **opts)
    except Exception as err:
        print(str(err))
        traceback.print_exc()
        terminate_program(err)


def oalink(doi):
    ''' Generate an OpenAlex link
        Keyword arguments:
          doi: DOI
        Returns:
          OpenAlex link
    '''
    return f"<a href='https://openalex.org/{OAID[doi]}'>{doi}</a>"


def processing():
    ''' Processing
        Keyword arguments:
          None
        Returns:
          None
    '''
    if ARG.ORCID:
        payload = {'orcid': ARG.ORCID}
    else:
        payload = {"$or": [{"group": {"$exists": True}}, {"managed": {"$exists": True}}],
                   "alumni": {"$exists": False}, "workerType": "Employee", "orcid": {"$exists": True}}
    try:
        cnt = DB['dis'].orcid.count_documents(payload)
        LOGGER.info(f"Found {cnt} ORCID{'s' if cnt != 1 else ''} for lab heads/managers")
        rows = DB['dis'].orcid.find(payload).sort('family', 1)
    except Exception as err:
        terminate_program(err)
    for row in tqdm(rows, desc="Processing ORCIDs", position=tqdm._get_free_pos(), leave=False,
                    total=cnt):
        if ARG.ORCID:
            LOGGER.info(f"Found {row['given'][0]} {row['family'][0]} ({ARG.ORCID})")
        process_author(row)
    for okey, ovalue in OUTPUT.items():
        fname = f"openalex_{okey}.tsv"
        if ovalue:
            LOGGER.info(f"Writing {fname}")
            with open(fname, 'w', encoding='utf-8') as fileout:
                for key, val in sorted(ovalue.items()):
                    fileout.write(f"{key}\t" + '\t'.join(val) + '\n')
                    MESSAGE[okey].append('\t'.join([oalink(key), val[0]]) + "\n")
        elif os.path.exists(fname):
            os.remove(fname)
    if OUTPUT['sent']:
        LOGGER.info("Writing openalex_ready.txt")
        with open('openalex_ready.txt', 'w', encoding='ascii') as fileout:
            for key in sorted(OUTPUT['sent'].keys()):
                fileout.write(key + '\n')
    elif os.path.exists('openalex_ready.txt'):
        os.remove('openalex_ready.txt')
    if ARG.TEST or ARG.WRITE:
        generate_emails()
    print(f"ORCIDs found:                    {cnt}")
    print(f"DOIs found:                      {COUNT['dois']:,}")
    print(f"DOIs ignored:                    {COUNT['ignored']:,}")
    print(f"DOIs skipped (publication date): {COUNT['skipped']:,}")
    print(f"DOIs skipped (no DOI):           {COUNT['no_doi']:,}")
    print(f"DOIs with no authors:            {COUNT['no_author']:,}")
    print(f"DOIs already in database:        {COUNT['in_database']:,}")
    print(f"DOIs to add:                     {len(MESSAGE['sent']):,}")
    print(f"DOIs with no institutions:       {len(MESSAGE['no_institutions']):,}")
    print(f"DOIs with institution mismatch:  {len(MESSAGE['institution_mismatch']):,}")

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Find new works for current lab heads")
    PARSER.add_argument('--orcid', dest='ORCID', action='store',
                        default=None, help='ORCID to process')
    PARSER.add_argument('--alumni', dest='ALUMNI', action='store_true',
                        default=False, help='Allow alumni')
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
