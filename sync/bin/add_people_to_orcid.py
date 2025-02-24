''' add_people_to_orcid.py
    
'''

__version__ = '1.0.0'

import argparse
import collections
from datetime import datetime
import json
from operator import attrgetter
import os
import sys
import requests
from tqdm import tqdm
import jrc_common.jrc_common as JRC

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Database
DB = {}
# Counters
COUNT = collections.defaultdict(lambda: 0, {})
# Global variables
ARG = DIS = LOGGER = REST = None

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
    if "PEOPLE_API_KEY" not in os.environ:
        terminate_program("Missing token - set in PEOPLE_API_KEY environment variable")
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


def call_responder(server, endpoint):
    """ Call a responder
        Keyword arguments:
        server: server
        endpoint: REST endpoint
    """
    url = ((getattr(getattr(REST, server), "url") if server else "") if "REST" in globals() \
           else (os.environ.get('CONFIG_SERVER_URL') if server else "")) + endpoint
    try:
        headers = {'APIKey': os.environ['PEOPLE_API_KEY'],
                   'Content-Type': 'application/json'}
        req = requests.get(url, headers=headers, timeout=10)
    except requests.exceptions.RequestException as err:
        terminate_program(f"Could not fetch from {url}\n{str(err)}")
    if req.status_code != 200:
        terminate_program(f"Status: {str(req.status_code)} ({url})")
    return req.json()


def add_middle_name(rec, given):
    ''' Add middle name to given name
        Keyword arguments:
          rec: person record from People
          given: list of given names
        Returns:
          None
    '''
    if rec["nameMiddlePreferred"]:
        temp = given.copy()
        for first in temp:
            given.append(' '.join([first, rec["nameMiddlePreferred"]]))
            if len(rec["nameMiddlePreferred"]) > 1:
                given.append(' '.join([first, rec["nameMiddlePreferred"][0]]))
                given.append(' '.join([first, rec["nameMiddlePreferred"][0]+'.']))
    if rec["nameMiddle"]:
        temp = given.copy()
        for first in temp:
            if " " in first:
                continue
            mid = ' '.join([first, rec["nameMiddlePreferred"]])
            if mid not in temp:
                given.append(mid)
            if len(rec["nameMiddlePreferred"]) > 1:
                mid =' '.join([first, rec["nameMiddlePreferred"][0]])
                if mid not in temp:
                    given.append(mid)
                mid =' '.join([first, rec["nameMiddlePreferred"][0]+'.'])
                if mid not in temp:
                    given.append(mid)


def add_new_record(person, output):
    ''' Add a new record to the orcid collection
        Keyword arguments:
          person: person record from People
          output: output dictionary
        Returns:
          None
    '''
    rec = JRC.call_people_by_id(person['employeeId'])
    if rec['supOrgName'] in DIS['group_ignore']:
        COUNT['skipped'] += 1
        return
    COUNT['new'] += 1
    payload = {"userIdO365": rec["userIdO365"],
               "employeeId": rec["employeeId"]
              }
    family = [rec["nameLastPreferred"]]
    if rec["nameLast"] not in family:
        family.append(rec["nameLast"])
    payload['family'] = family
    given = [rec["nameFirstPreferred"]]
    if rec["nameFirst"] not in given:
        given.append(rec["nameFirst"])
    add_middle_name(rec, given)
    payload['given'] = given
    output['new'].append(json.dumps(payload, indent=2))
    if not ARG.WRITE:
        return
    try:
        result = DB['dis']['orcid'].insert_one(payload)
        if hasattr(result, 'inserted_id') and result.inserted_id:
            COUNT['insert'] += 1
    except Exception as err:
        terminate_program(err)


def unset_alumni(person, output):
    ''' Unset the alumni flag in orcid
        Keyword arguments:
          person: person record from People
          output: output dictionary
        Returns:
          None
    '''
    name = f"{person['nameFirstPreferred']} {person['nameLastPreferred']}"
    try:
        rec = DB['dis']['orcid'].find_one({'employeeId': person['employeeId']})
    except Exception as err:
        terminate_program(err)
    if not rec:
        terminate_program(f"No orcid record found for {name}")
    COUNT['boomerang'] += 1
    LOGGER.warning(f"Unsetting alumni flag for {name}")
    output['boomerang'].append(json.dumps(person, indent=2))
    if not ARG.WRITE:
        return
    try:
        result = DB['dis']['orcid'].update_one({'employeeId': person['employeeId']},
                                               {'$unset': {'alumni': None}})
        if hasattr(result, 'modified_count') and result.modified_count:
            COUNT['update'] += 1
    except Exception as err:
        terminate_program(err)

def set_alumni(person, orcid):
    ''' Set the alumni flag in orcid
        Keyword arguments:
          person: person record from People
          orcid: orcid dictionary
        Returns:
          None
    '''
    if person['employeeId'] in orcid and not orcid[person['employeeId']]:
        COUNT['people_alumni'] += 1
        return
    name = f"{person['nameFirstPreferred']} {person['nameLastPreferred']}"
    try:
        rec = DB['dis']['orcid'].find_one({'employeeId': person['employeeId']})
    except Exception as err:
        terminate_program(err)
    if not rec:
        terminate_program(f"No orcid record found for {name}")
    COUNT['set_alumni'] += 1
    LOGGER.warning(f"Setting alumni flag for {name}")
    if not ARG.WRITE:
        return
    try:
        result = DB['dis']['orcid'].update_one({'employeeId': person['employeeId']},
                                      {'$set': {'alumni': True}})
        if hasattr(result, 'modified_count') and result.modified_count:
            COUNT['update'] += 1
    except Exception as err:
        terminate_program(err)


def update_orcid():
    ''' Add people to the orcid collection
        Keyword arguments:
          None
        Returns:
          None
    '''
    try:
        rows = DB['dis']['orcid'].find({'employeeId': {"$exists": True}})
    except Exception as err:
        terminate_program(err)
    orcid = {}
    for row in rows:
        if 'alumni' in row:
            orcid[row['employeeId']] = False
        else:
            orcid[row['employeeId']] = True
    resp = call_responder("people", "People/Search/ByOther/Janelia Research Campus")
    COUNT['people'] = len(resp)
    output = {'boomerang': [], 'new': []}
    for person in tqdm(resp):
        if person['locationName'] != 'Janelia Research Campus':
            COUNT['not_janelia'] += 1
            continue
        eid = person['employeeId']
        if eid in orcid:
            if orcid[eid]:
                # Person is active in orcid
                if person['businessTitle'] == 'JRC Alumni':
                    set_alumni(person, orcid)
                else:
                    COUNT['already_active'] += 1
            elif person['businessTitle'] != 'JRC Alumni':
                # People says active, orcid says alumni - boomerang!
                unset_alumni(person, output)
            else:
                # People says alumni - update the flag in orcid if necessary
                set_alumni(person, orcid)
        else:
            # Person is in People but not orcid - insert record
            add_new_record(person, output)
    # Write output files
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    for key, val in output.items():
        if val:
            fname = f"{timestamp}_{key}.json"
            with open(fname, "w", encoding="utf-8") as outfile:
                outfile.write("[" + ",\n".join(val) + "]")
    print(f"Records from People:    {COUNT['people']:,}")
    print(f"Already active:         {COUNT['already_active']:,}")
    print(f"Skipped (organization): {COUNT['skipped']:,}")
    print(f"Not at Janelia:         {COUNT['not_janelia']:,}")
    print(f"JRC Alumni:             {COUNT['people_alumni']:,}")
    print(f"JRC Alumni set:         {COUNT['set_alumni']:,}")
    print(f"Boomerangs:             {COUNT['boomerang']:,}")
    print(f"New employees:          {COUNT['new']:,}")
    print(f"Records inserted:       {COUNT['insert']:,}")
    print(f"Records updated:        {COUNT['update']:,}")
# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Sync People to MongoDB:orcid")
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, prod)')
    PARSER.add_argument('--reset', dest='RESET', action='store_true',
                        default=False, help='Reset affiliations and managesTeams')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Write to database/config system')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    DIS = JRC.simplenamespace_to_dict(JRC.get_config("dis"))
    REST = JRC.get_config("rest_services")
    initialize_program()
    update_orcid()
    terminate_program()
