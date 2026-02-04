''' pull_openalex.py
    Sync works from OpenAlex for authors with a Janelia affiliation.
    DOIs are added to the database if the following conditions are met:
    - 
'''

__version__ = '2.0.0'

import argparse
import collections
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
ORCIDS = {}
OAID = {}
MESSAGE = {"ready": [], "review": []}
OUTPUT = {"ready": {}, "review": {}}

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
        req = requests.get(url, timeout=timeout,
                           headers={'Authorization': f'Bearer {os.environ["OPENALEX_API_KEY"]}'})
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
    if "OPENALEX_API_KEY" not in os.environ:
        terminate_program("Missing API key - set in OPENALEX_API_KEY environment variable")
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
    try:
        rows = DB['dis'].orcid.find({'orcid': {'$exists': True}, 'workerType': 'Employee'})
    except Exception as err:
        terminate_program(err)
    for row in rows:
        if 'hireDate' not in row:
            row['hireDate'] = ''
        ORCIDS[f"https://orcid.org/{row['orcid']}"] = row['hireDate']
    LOGGER.info(f"ORCIDs: {len(ORCIDS):,}")


def get_works():
    ''' Get Janelia works
        Keyword arguments:
          None
        Returns:
          List of works
    '''
    base = "/works?filter=authorships.affiliations.institution_ids:" \
           + f"https://openalex.org/I195573530&mailto={DISCONFIG['developer']}" \
           + "&per-page=200&cursor="
    cursor = "*"
    rows = []
    cnt = 1
    while cursor:
        sleep(0.12)
        try:
            resp = call_responder('openalex', base + cursor)
            rows.extend(resp['results'])
            LOGGER.info(f"Found {len(rows):,}/{resp['meta']['count']:,} works")
        except Exception as err:
            terminate_program(f"Error getting OpenAlex works on call {cnt}: {err}")
        cnt += 1
        cursor = resp['meta']['next_cursor'] if resp['meta']['next_cursor'] else None
    LOGGER.debug(f"Found {len(rows)} works")
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
       or ('display_name' in inst and inst['display_name'] and 'Janelia' in inst['display_name'])


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

def in_collection(auth):
    ''' Check if author is in the collection
        Keyword arguments:
          auth: author
        Returns:
          True if author is in the collection
          False otherwise
    '''
    given = auth['author']['display_name'].split(' ')[0]
    family = auth['author']['display_name'].split(' ')[-1]
    try:
        cnt = DB['dis']['orcid'].count_documents({"given": given, "family": family})
    except Exception as err:
        terminate_program(err)
    return cnt > 0


def janelia_author(row, doi):
    ''' Check if author is a Janelia author
        Keyword arguments:
          row: row from OpenAlex
          doi: DOI
        Returns:
          True if author is a Janelia author
          False otherwise
    '''
    OAID[doi] = row['id'].split('/')[-1]
    affil = orcid = name = 0
    found = []
    for auth in row['authorships']:
        if auth['author']['orcid'] and auth['author']['orcid'] in ORCIDS:
            hdate = ORCIDS[auth['author']['orcid']]
            orcid = 1
        else:
            hdate = ''
        # Check if *any* author is from Janelia
        if auth['institutions']:
            for inst in auth['institutions']:
                if janelia_affiliation(inst):
                    affil = 1
                    found = [auth['author']['display_name'],
                             row['publication_date'], hdate, get_title(row)]
        if not affil:
            continue
        if in_collection(auth):
            name = 1
        if name or orcid and doi not in OUTPUT['ready']:
            OUTPUT['ready'][doi] = [auth['author']['display_name'],
                                    row['publication_date'], hdate, get_title(row)]
            return True
    if not affil:
        LOGGER.error(f"{doi} has no Janelia affiliation")
    if not orcid and not name:
        OUTPUT['review'][doi] = found
    return False


def oalink(doi):
    ''' Generate an OpenAlex link
        Keyword arguments:
          doi: DOI
        Returns:
          OpenAlex link
    '''
    return f"<a href='https://openalex.org/{OAID[doi]}'>{doi}</a>"


def generate_emails():
    ''' Generate and send an email
        Keyword arguments:
          None
        Returns:
          None
    '''
    msg = ""
    if MESSAGE['ready']:
        msg += "<br>The following DOIs will be added to the database:<br>"
        for itm in MESSAGE['ready']:
            msg += f"  {itm}<br>"
        msg += "\n"
    if msg:
        msg = JRC.get_run_data(__file__, __version__) + "<br>" + msg
    else:
        return
    if MESSAGE['review']:
        msg += "<br>The following DOIs should be reviewed:<br>"
        for itm in MESSAGE['review']:
            msg += f"  {itm}<br>"
        msg += "\n"
    with open('openalex_status.html', 'w', encoding='utf-8') as fileout:
        fileout.write(msg)
    try:
        email = DISCONFIG['developer'] if ARG.TEST else DISCONFIG['receivers']
        LOGGER.info(f"Sending email to {email}")
        opts = {'mime': 'html'}
        JRC.send_email(msg, DISCONFIG['sender'], email, "Lab head DOI sync", **opts)
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
    rows = get_works()
    for row in tqdm(rows, desc="Processing works"):
        COUNT['dois'] += 1
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
        if 'publication_date' in row and row['publication_date'] < '2006-04-01':
            COUNT['too_early'] += 1
            continue
        if not janelia_author(row, doi):
            COUNT['no_author'] += 1
            continue
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
    if OUTPUT['ready']:
        LOGGER.info("Writing openalex_ready.txt")
        with open('openalex_ready.txt', 'w', encoding='ascii') as fileout:
            for key in sorted(OUTPUT['ready'].keys()):
                fileout.write(key + '\n')
    elif os.path.exists('openalex_ready.txt'):
        os.remove('openalex_ready.txt')
    if ARG.TEST or ARG.WRITE:
        generate_emails()
    print(f"DOIs found:                      {COUNT['dois']:,}")
    print(f"DOIs ignored:                    {COUNT['ignored']:,}")
    print(f"DOIs skipped (no DOI):           {COUNT['no_doi']:,}")
    print(f"DOIs skipped (too early):        {COUNT['too_early']:,}")
    print(f"DOIs with no authors:            {COUNT['no_author']:,}")
    print(f"DOIs already in database:        {COUNT['in_database']:,}")
    print(f"DOIs to add:                     {len(MESSAGE['ready']):,}")
    print(f"DOIs to review:                  {len(MESSAGE['review']):,}")

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Find new works from OpenAlex")
    PARSER.add_argument('--orcid', dest='ORCID', action='store_true',
                        default=False, help='Allow authors with Janelia ORCIDs')
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
