""" sync_to_alumni.py
    Find Janelia alumni in local Woroday that need to be added to orcid
"""

import argparse
import collections
import json
from operator import attrgetter
import sys
from tqdm import tqdm
import jrc_common.jrc_common as JRC
#import doi_common.doi_common as DL

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

DB = {}
COUNT = collections.defaultdict(lambda: 0, {})
ARG = LOGGER = WORK = None
DISCARD = ["Administrative Operations Contractors", "Campus Guests", "Children's Learning Lab",
           "Culinary; BOH", "Dining Room", "Facilities Engineering", "Facilities Operations",
           "Facility Support", "Food & Beverage; FOH", "JLL Administration",
           "JLL Grounds and Estate", "JLL Janitorial Services", "JLL Operations", "JLL",
           "Janelia Construction", "Janelia Facilities", "Janelia Facilities Contractors",
           "Janelia Fitness Center", "Janelia Security", "Pub"
           ]


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


def get_orcid_record(payload):
    ''' Get an orcid record
        Keyword arguments:
          payload: MongoDB payload
        Returns:
          None
    '''
    try:
        row = DB['dis']['orcid'].find_one(payload)
    except Exception as err:
        terminate_program(err)
    return row


def write_new_record(val, add_to_orcid):
    ''' Write a new record to orcid
        Keyword arguments:
          val: dictionary of values
          add_to_orcid: list of records to add to orcid
        Returns:
          None
    '''
    payload = {"userIdO365": val['email'].replace('@janelia.hhmi.org', '@hhmi.org'),
                "given": [val['first']],
                "family": [val['last']],
                "employeeId": val['id'],
                "alumni": True,
                "workdayAffiliation": val['organization'],
              }
    if val['organization'] == 'Group Leader/Lab Head':
        payload['group'] = f"{val['first']} {val['last']} Lab"
    COUNT['set_orcid'] += 1
    if 'Brenner' not in payload['family']:
        return
    add_to_orcid.append(json.dumps(payload, indent=2, default=str))
    if not ARG.WRITE:
        return
    try:
        result = DB['dis']['orcid'].insert_one(payload)
    except Exception as err:
        terminate_program(err)
    if hasattr(result, 'inserted_count') and result.inserted_count:
        COUNT['inserted'] += 1


def process_alumni():
    ''' Process local Workday alumni
        Keyword arguments:
          None
        Returns:
          None
    '''
    COUNT['workday'] = len(WORK)
    add_to_orcid = []
    for val in tqdm(WORK.values()):
        # If they're not at Janelia, skip them
        if val['location'] != 'Janelia Research Campus':
            continue
        COUNT['janelians'] += 1
        # Get name and employee ID
        name = ' '.join([val['first'], val['last']])
        eid = None
        if 'id' in val and val['id']:
            eid = val['id']
        if not eid:
            terminate_program(f"No ID for {name}")
        idresp = JRC.call_people_by_id(eid)
        if idresp:
            # If they're in People, skip them
            COUNT['eid_in_people'] += 1
            continue
        row = get_orcid_record({'employeeId': eid})
        if row:
            # If they're in ORCID with an employee ID, skip them
            COUNT['eid_in_orcid'] += 1
            continue
        row = get_orcid_record({'given': val['first'], 'family': val['last']})
        if row:
            # If they're in orcid with a name, check alumni status
            COUNT['name_in_orcid'] += 1
            if 'alumni' not in row or not row['alumni']:
                terminate_program(f"{name} {eid} is not alumni {row}")
            if 'employeeId' in row:
                # If they have an employee ID in orcid, skip them
                LOGGER.warning(f"{name} {eid} {row}")
                continue
            # Set EID
            COUNT['set_eid'] += 1
            continue
        if 'organization' not in val or not val['organization']:
            terminate_program(f"No organization for {name}")
        if val['organization'] in DISCARD:
            COUNT['group_discarded'] += 1
            continue
        write_new_record(val, add_to_orcid)
    # Write output file and show stats
    with open('added_to_orcid.json', 'w', encoding='ascii') as outstream:
        outstream.writelines(add_to_orcid)
    print(f"Entries in Workday:   {COUNT['workday']:,}")
    print(f"Janelians in Workday: {COUNT['janelians']:,}")
    print(f"EIDs in People:       {COUNT['eid_in_people']:,}")
    print(f"EIDs in ORCID:        {COUNT['eid_in_orcid']:,}")
    print(f"Names in ORCID:       {COUNT['name_in_orcid']:,}")
    print(f"Discarded (group):    {COUNT['group_discarded']:,}")
    print(f"EIDs set:             {COUNT['set_eid']:,}")
    print(f"ORCIDs set:           {COUNT['set_orcid']:,}")
    print(f"ORCIDs inserted:      {COUNT['inserted']:,}")

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Sync local Workday alumni to orcid")
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, prod)')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Actually send emails')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    WORK = JRC.simplenamespace_to_dict(JRC.get_config("workday"))
    initialize_program()
    process_alumni()
    terminate_program()
