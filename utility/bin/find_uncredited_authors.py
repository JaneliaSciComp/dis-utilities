""" find_uncredited_authors.py
    Find Janelia authors on DOIs that are saved as Janelia authors.
    1) Get potential authors from orcid table that have an employee ID but are
       not alumni or contingent workers.
    2) For each potential author, get the DOIs that have a match to the author
       name but are not in the jrc_author field.
    3) For each DOI, check if the author should be credited. This will be done by
       checking if the author has an affiliation, ORCID, or name match.
    4) If the author should be credited, update the DOI with the author's information.
       This will be done by adding the author's employee ID to the jrc_author field and
       updating first/last author (if necessary).
"""

__version__ = '2.0.0'

import argparse
import collections
from datetime import datetime
import json
from operator import attrgetter
import sys
import time
import inquirer
from inquirer.themes import BlueComposure
from tqdm import tqdm
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL

# pylint: disable=broad-exception-caught,logging-fstring-interpolation,logging-not-lazy

# Database
DB = {}
# Globals
ARG = DIS = LOGGER = None
TO_ADD = {'affiliation': {}, 'orcid': {}, 'name': {}}
OUTPUT = {'affiliation': [], 'orcid': [], 'name': []}
AUDIT = {'affiliation': [], 'orcid': [], 'name': []}
IGNORE = {}
TO_UPDATE = {}
# Counters
COUNT = collections.defaultdict(lambda: 0, {})

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
        dbo = attrgetter(f"{source}.{ARG.MANIFOLD}.write")(dbconfig)
        LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, ARG.MANIFOLD, dbo.host, dbo.user)
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)
    with open('ignore_uncredited.tsv', encoding='utf-8') as instream:
        for line in instream:
            line = line.strip()
            if not line:
                continue
            parts = line.split('\t')
            if parts[0] not in IGNORE:
                IGNORE[parts[0]] = []
            IGNORE[parts[0]].append(parts[1])


def janelia_affiliation(affiliations, mode='Crossref'):
    ''' Check if there is a Janelia affiliation
        Keyword arguments:
          affiliations: list of affiliations from DOI author record
          mode: 'Crossref' or 'DataCite'
        Returns:
          True if there is a Janelia affiliation, False otherwise
    '''
    for aff in affiliations:
        if mode == 'Crossref':
            if 'name' in aff and 'Janelia' in aff['name']:
                return True
        elif mode == 'DataCite':
            if 'Janelia' in aff:
                return True
    return False


def get_doi_author_matches(auth, row):
    ''' Get the author matches for the DOI
        Keyword arguments:
          auth: record from orcid table
          row: record from dois table
        Returns:
          match type
    '''
    mtype = []
    field = 'author' if 'author' in row else 'creators'
    for pauth in row[field]:
        if field == 'author':
            # Crossref
            if not('given' in pauth and 'family' in pauth):
                continue
            if pauth['given'] in auth['given'] and pauth['family'] in auth['family']:
                LOGGER.debug(f"Matched {pauth['given']} {pauth['family']} for {row['doi']}")
                mtype.append('name')
            else:
                continue
            if 'orcid' in auth and 'ORCID' in pauth \
               and pauth['ORCID'].split('/')[-1] == auth['orcid']:
                mtype.append('orcid')
            if 'affiliation' in pauth and pauth['affiliation'] \
               and janelia_affiliation(pauth['affiliation']):
                mtype.append('affiliation')
            break
        # DataCite
        if not('givenName' in pauth and 'familyName' in pauth):
            continue
        if pauth['givenName'] in auth['given'] and pauth['familyName'] in auth['family']:
            LOGGER.debug(f"Matched {pauth['givenName']} {pauth['familyName']} for {row['doi']}")
            mtype.append('name')
        else:
            continue
        if 'affiliation' in pauth and pauth['affiliation'] \
           and janelia_affiliation(pauth['affiliation'], mode='DataCite'):
            mtype.append('affiliation')
        break
    return mtype


def update_doi(auth, row, match):
    ''' Update the DOI with the first and last author payload
        Keyword arguments:
          auth: author record from orcid table
          row: record from dois table
          match: match type
        Returns:
          None
    '''
    doi = row['doi']
    if 'jrc_author' not in row:
        row['jrc_author'] = []
    if auth['employeeId'] not in row['jrc_author']:
        row['jrc_author'].append(auth['employeeId'])
    payload = {"$set": {"jrc_author": row['jrc_author']}}
    if ARG.WRITE:
        try:
            DB['dis']['dois'].update_one({'doi': doi}, payload)
        except Exception as err:
            LOGGER.error(f"Error updating DOI jrc_author for {doi}: {err}")
    else:
        LOGGER.debug(payload)
    try:
        time.sleep(0.01)
        fl_payload = DL.get_first_last_author_payload(doi)
    except Exception as err:
        LOGGER.error(f"Error getting first/last payload for {doi}: {err}")
    if ARG.WRITE:
        try:
            DB['dis']['dois'].update_one({'doi': doi}, fl_payload)
        except Exception as err:
            LOGGER.error(f"Error updating DOI first/last authors for {doi}: {err}")
    COUNT['updates'] += 1
    if doi not in TO_ADD[match]:
        TO_ADD[match][doi] = []
    payload['author'] = auth['given'][0] + ' ' + auth['family'][0]
    TO_ADD[match][doi].append(payload)
    fl_payload['author'] = auth['given'][0] + ' ' + auth['family'][0]
    TO_ADD[match][doi].append(fl_payload)
    OUTPUT[match].append(f"{payload['author']} was added to {doi} ({match})")


def current_employee(auth):
    ''' Check if the author is a current employee
        Keyword arguments:
          auth: record from orcid table
        Returns:
          True if the author is a current worker, False otherwise
    '''
    return 'alumni' not in auth and auth['workerType'] != 'Contingent Worker'


def ignore_author(auth, row):
    ''' Check if the author should be ignored for a specified DOI
        Keyword arguments:
          auth: record from orcid table
          row: record from dois table
    '''
    full = f"{auth['given'][0]} {auth['family'][0]}"
    if full not in IGNORE:
        return False
    if row['doi'] in IGNORE[full]:
        LOGGER.warning(f"Ignoring {full} for {row['doi']}")
        return True
    return False

def delta_days(date1, date2):
    ''' Calculate the number of days between two dates
        Keyword arguments:
          date1: date1
          date2: date2
        Returns:
          number of days between two dates
    '''
    date1 = datetime.strptime(date1, '%Y-%m-%d')
    date2 = datetime.strptime(date2, '%Y-%m-%d')
    return (date1 - date2).days


def match_message(auth, row, match_type):
    ''' Get the message for the match
        Keyword arguments:
          auth: record from orcid table
          row: record from dois table
          match_type: match type
        Returns:
          message
    '''
    msg = f"{auth['given'][0]} {auth['family'][0]}"
    if 'alumni' in auth:
        msg += ' (alumni)'
    when = ''
    if 'jrc_publishing_date' in row and 'hireDate' in auth:
        delta = delta_days(row['jrc_publishing_date'], auth['hireDate'])
        if delta < 0:
            when = f" {abs(delta)} day{'s' if abs(delta) > 1 else ''} before {auth['hireDate']}"
    return f"{msg} is on {row['doi']} ({match_type}){when}"


def valid_author(auth, rows):
    ''' Check if the author is valid
        Keyword arguments:
          auth: record from orcid table
          rows: rows from dois table
        Returns:
          None
    '''
    bumped = False
    for row in rows:
        if ignore_author(auth, row):
            continue
        match_type = get_doi_author_matches(auth, row)
        if 'affiliation' in match_type:
            msg = match_message(auth, row, 'affiliation')
            LOGGER.debug(msg)
            AUDIT['affiliation'].append(msg)
            COUNT['dois'] += 1
            if not bumped:
                COUNT['affiliation'] += 1
                COUNT['uncredited'] += 1
                bumped = True
            if 'Affiliation' in TO_UPDATE:
                update_doi(auth, row, 'affiliation')
        elif 'orcid' in match_type and current_employee(auth):
            msg = match_message(auth, row, 'ORCID')
            LOGGER.debug(msg)
            AUDIT['orcid'].append(msg)
            if not bumped:
                COUNT['orcid'] += 1
                COUNT['uncredited'] += 1
                bumped = True
            if 'ORCID' in TO_UPDATE:
                update_doi(auth, row, 'orcid')
        elif 'name' in match_type and current_employee(auth):
            msg = match_message(auth, row, ', '.join(match_type))
            LOGGER.debug(msg)
            AUDIT['name'].append(msg)
            if not bumped:
                COUNT['name'] += 1
                COUNT['uncredited'] += 1
                bumped = True
            if 'Name' in TO_UPDATE:
                update_doi(auth, row, 'name')


def select_author():
    ''' Select the author from the database
        Keyword arguments:
          None
        Returns:
          None
    '''
    payload = {"employeeId": {"$exists": True},
               "$or": [{"family": ARG.NAME},
                       {"given": ARG.NAME}]
              }
    try:
        rows = DB['dis']['orcid'].find(payload).collation( {'locale': 'en_US',
                                                            'strength': 1}).sort('family', 1)
    except Exception as err:
        terminate_program(err)
    names = []
    auth = {}
    for row in rows:
        key = f"{row['given'][0]} {row['family'][0]}"
        names.append(key)
        auth[key] = row
    if not names:
        terminate_program(f"No authors found matching {ARG.NAME}")
    if len(names) == 1:
        LOGGER.info(f"Found {key}")
        return [auth[names[0]]]
    quest = [(inquirer.List('checklist',
                            message='Select author',
                            choices=names))]
    ans = inquirer.prompt(quest, theme=BlueComposure())
    if ans and ans['checklist']:
        LOGGER.info(f"Found {ans['checklist']}")
        return [auth[ans['checklist']]]
    terminate_program("No author selected")


def get_authors():
    ''' Get the authors from the database
        Keyword arguments:
          None
        Returns:
          None
    '''
    if ARG.ALUMNI:
        payload = {"employeeId": {"$exists": True}}
    else:
        payload = {"employeeId": {"$exists": True}, "alumni": {"$exists": False},
                   "workerType": {"$ne": "Contingent Worker"}}
    try:
        rows = DB['dis']['orcid'].find(payload).sort('family', 1)
    except Exception as err:
        terminate_program(err)
    authors = []
    for row in rows:
        if not('given' in row and 'family' in row):
            terminate_program(f"Missing given or family for {row['orcid']}")
        authors.append(row)
    LOGGER.info(f"Found {len(authors):,} authors")
    return authors


def send_email():
    ''' Send an email summary
        Keyword arguments:
          None
        Returns:
          None
    '''
    text = ""
    for key, value in OUTPUT.items():
        if not value:
            continue
        text += f"Changes determined by {key}:<dl>"
        for line in value:
            text += f"<dd>{line}</dd>"
        text += "</dl>"
    if not text:
        return
    subject = "Added uncredited authors to DOIs"
    email = DIS['developer'] if ARG.TEST else DIS['receivers']
    JRC.send_email(text, DIS['sender'], email, subject, mime='html')


def processing():
    ''' Process the request
        Keyword arguments:
          None
        Returns:
          None
    '''
    # Get author(s)
    if ARG.NAME:
        authors = select_author()
    else:
        authors = get_authors()
    # Get matches to update
    quest = [(inquirer.Checkbox('checklist',
                                message='Select author matches to update DOIs for',
                                choices=['Affiliation', 'ORCID', 'Name'],
                                default=['Affiliation']))]
    ans = inquirer.prompt(quest, theme=BlueComposure())
    for key in ans['checklist']:
        TO_UPDATE[key] = True
    # Check DOIs
    for auth in tqdm(authors, desc="Checking authors"):
        if 'alumni' in auth:
            COUNT['alumni'] += 1
        payload = {"$and": [{"jrc_author": {"$ne": auth['employeeId']}},
                            {"$or": [{"author.given": {"$in": auth['given']}},
                                     {"creators.givenName": {"$in": auth['given']}}]},
                            {"$or": [{"author.family": {"$in": auth['family']}},
                                     {"creators.familyName": {"$in": auth['family']}}]}]
                  }
        if ARG.DOI:
            payload['$and'].append({'doi': ARG.DOI})
        try:
            cnt = DB['dis']['dois'].count_documents(payload)
            if not cnt:
                continue
            rows = DB['dis']['dois'].find(payload)
            valid_author(auth, rows)
        except Exception as err:
            terminate_program(err)
    print(f"Janelia authors:              {len(authors):,}")
    print(f"Alumni:                       {COUNT['alumni']:,}")
    print(f"Authors with uncredited DOIs: {COUNT['uncredited']:,}")
    print(f"  Affiliation:                {COUNT['affiliation']:,}")
    print(f"  ORCID:                      {COUNT['orcid']:,}")
    print(f"  Name:                       {COUNT['name']:,}")
    print(f"DOIs:                         {COUNT['dois']:,}")
    print(f"Updates:                      {COUNT['updates']:,}")
    for key, value in TO_ADD.items():
        if not value:
            continue
        fname = f"uncredited_{key}.json"
        with open(fname, encoding='utf-8', mode="w") as outstream:
            outstream.write(json.dumps(value, indent=2))
        LOGGER.info(f"Wrote {fname}")
    for key, value in AUDIT.items():
        if not value:
            continue
        fname = f"uncredited_audit_{key}.txt"
        with open(fname, encoding='utf-8', mode="w") as outstream:
            outstream.write("\n".join(value))
        LOGGER.info(f"Wrote {fname}")
    if ARG.TEST or ARG.WRITE:
        send_email()

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Update Janelia authors on DOIs")
    PARSER.add_argument('--alumni', dest='ALUMNI', action='store_true',
                        default=False, help='Include alumni in processing')
    PARSER.add_argument('--name', dest='NAME', action='store',
                        help='Author to process (optional)')
    PARSER.add_argument('--doi', dest='DOI', action='store',
                        help='DOI to process (optional)')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, [prod])')
    PARSER.add_argument('--test', dest='TEST', action='store_true',
                        default=False, help='Flag, Send email to developer only')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Flag, Write to database')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    DIS = JRC.simplenamespace_to_dict(JRC.get_config("dis"))
    processing()
    terminate_program()
