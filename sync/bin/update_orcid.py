''' update_orcid.py
    Update the MongoDB orcid collection with ORCIDs and names for Janelia authors
'''

__version__ = '2.6.0'

import argparse
import collections
import configparser
import json
from operator import attrgetter
import os
import re
import sys
import inquirer
import requests
from tqdm import tqdm
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Database
DB = {}
# Counters
COUNT = collections.defaultdict(lambda: 0, {})
# General
PRESENT = {}
NEW_ORCID = {}
ALUMNI = []

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
    # Initialize the PRESENT dict with rows that have ORCIDs
    try:
        rows = DB['dis'].orcid.find({"orcid": {"$exists": True}})
    except Exception as err:
        terminate_program(err)
    for row in rows:
        PRESENT[row['orcid']] = row
    LOGGER.info(f"{len(PRESENT)} DOIs are already in the collection")


def add_name(oid, oids, family, given):
    ''' If the ORCID ID is new, add it to the dict. Otherwise, update it
        with new family/given name.
        Keyword arguments:
          oid: ORCID ID
          oids: ORCID ID dict
          family: family name
          given: given name
        Returns:
          None
    '''
    if oid in oids:
        if family not in oids[oid]['family']:
            oids[oid]['family'].append(family)
        if given not in oids[oid]['given']:
            oids[oid]['given'].append(given)
    else:
        oids[oid] = {"family": [family], "given": [given]}
        if oid in PRESENT:
            if not ARG.WRITE:
                COUNT['update'] += 1
        else:
            if not ARG.WRITE:
                COUNT['insert'] += 1
                print(oid, json.dumps(oids[oid], indent=2))
            NEW_ORCID[oid] = {"family": [family], "given": [given]}


def process_author(aut, oids, source="crossref"):
    ''' Process a single author record
        Keyword arguments:
          aut: author record
          oids: ORCID ID dict
        Returns:
          None
    '''
    for aff in aut['affiliation']:
        if 'Janelia' in aff['name']:
            oid = re.sub(r'.*/', '', aut['ORCID'])
            if source == "crossref":
                add_name(oid, oids, aut['family'], aut['given'])
            break


def get_name(oid):
    ''' Get an author's first and last name from ORCID
        Keyword arguments:
          oid: ORCID
        Returns:
          family and given name
    '''
    url = f"{CONFIG['orcid']['base']}{oid}"
    try:
        resp = requests.get(url, timeout=10,
                            headers={"Accept": "application/json"})
    except Exception as err:
        terminate_program(err)
    try:
        return resp.json()['person']['name']['family-name']['value'], \
               resp.json()['person']['name']['given-names']['value']
    except Exception as err:
        LOGGER.warning(resp.json()['person']['name'])
        LOGGER.warning(err)
        return None, None


def add_from_orcid(oids):
    ''' Find additional ORCID IDs using the ORCID API
        Keyword arguments:
          oids: ORCID ID dict
        Returns:
          None
    '''
    authors = []
    base = f"{CONFIG['orcid']['base']}search"
    for url in ('/?q=ror-org-id:"' + CONFIG['ror']['janelia'] + '"',
                '/?q=affiliation-org-name:"Janelia Research Campus"',
                '/?q=affiliation-org-name:"Janelia Farm Research Campus"'):
        try:
            resp = requests.get(f"{base}{url}", timeout=10,
                                headers={"Accept": "application/json"})
        except Exception as err:
            terminate_program(err)
        for orcid in resp.json()['result']:
            authors.append(orcid['orcid-identifier']['path'])
    COUNT['orcid'] = len(authors)
    for oid in tqdm(authors, desc='Janelians from ORCID'):
        family, given = get_name(oid)
        if family and given:
            add_name(oid, oids, family, given)


def people_by_name(first, surname):
    ''' Search for a surname in the people system
        Keyword arguments:
          first: first name
          surname: last name
        Returns:
          List of people
    '''
    try:
        people = JRC.call_people_by_name(surname)
    except Exception as err:
        terminate_program(err)
    filtered = []
    for person in people:
        if person['locationName'] != 'Janelia Research Campus':
            continue
        if person['nameLastPreferred'].lower() == surname.lower() \
           and person['nameFirstPreferred'].lower() == first.lower():
            filtered.append(person)
    return filtered


def update_group_status(rec, idresp):
    ''' Add group tags to the record
        Keyword arguments:
          rec: orcid record
          idresp: People service response
        Returns:
          None
    '''
    if 'managedTeams' not in idresp:
        return
    lab = ''
    for team in idresp['managedTeams']:
        if team['supOrgSubType'] == 'Lab' and team['supOrgName'].endswith(' Lab'):
            if team['supOrgCode'] in DISCONFIG['sup_ignore']:
                continue
            if lab:
                terminate_program(f"Multiple labs found for {idresp['nameFirstPreferred']} " \
                                  + idresp['nameLastPreferred'])
            lab = team['supOrgName']
            rec['group'] = lab
            rec['group_code'] = team['supOrgCode']


def get_person(people):
    ''' Get a person record
        Keyword arguments:
          people: list of people
        Returns:
          Person record and person ID record
    '''
    if len(people) == 1:
        idresp = JRC.call_people_by_id(people[0]['employeeId'])
        return people[0], idresp
    latest = ''
    saved = {"person": None, "idresp": None}
    idresp = None
    for person in people:
        first = person['nameFirstPreferred']
        last = person['nameLastPreferred']
        idresp = JRC.call_people_by_id(person['employeeId'])
        if 'terminationDate' in idresp and idresp['terminationDate']:
            LOGGER.warning(f"{first} {last} was terminated {idresp['terminationDate']}")
            continue
        if 'hireDate' in idresp and idresp['hireDate']:
            if not latest or idresp['hireDate'] > latest:
                latest = idresp['hireDate']
                saved['person'] = person
                saved['idresp'] = idresp
    if saved['person']:
        LOGGER.warning(f"Selected {first} {last} {latest}")
    return saved['person'], saved['idresp']


def add_people_information(first, surname, oids, oid):
    ''' Correlate a name from ORCID with HHMI's People service
        Keyword arguments:
          first: given name
          surname: family name
          oid: ORCID ID
          oids: ORCID ID dict
        Returns:
          None
    '''
    found = False
    people = people_by_name(first, surname)
    if people:
        person, idresp = get_person(people)
        if person:
            found = True
            oids[oid]['employeeId'] = people[0]['employeeId']
            oids[oid]['userIdO365'] = people[0]['userIdO365']
            if 'group leader' in people[0]['businessTitle'].lower():
                oids[oid]['group'] = f"{first} {surname} Lab"
            if people[0]['businessTitle'] == 'JRC Alumni':
                oids[oid]['alumni'] = True
            if idresp:
                update_group_status(oids[oid], idresp)
                DL.get_name_combinations(idresp, oids[oid])
                DL.get_affiliations(idresp, oids[oid])
        else:
            LOGGER.error(f"No usable record in People for {first} {surname}")
    return found


def correlate_person(oid, oids):
    ''' Correlate a name from ORCID with HHMI's People service
        Keyword arguments:
          oid: ORCID ID
          oids: ORCID ID dict
        Returns:
          None
    '''
    val = oids[oid]
    for surname in val['family']:
        for first in val['given']:
            found = add_people_information(first, surname, oids, oid)
            if found:
                break
        if found:
            break
    #if not found:
    #    LOGGER.warning(f"Could not find a record in People for {first} {surname}")


def preserve_mongo_names(current, oids):
    ''' Preserve names from sources other than this program in the oids dictionary
        Keyword arguments:
          oids: ORCID ID dict
        Returns:
          None
    '''
    oid = current['orcid']
    for field in ('family', 'given'):
        for name in current[field]:
            if name not in oids[oid][field]:
                oids[oid][field].append(name)


def add_janelia_info(oids):
    ''' Find Janelia information for each ORCID ID
        Keyword arguments:
          oids: ORCID ID dict
        Returns:
          None
    '''
    for oid in tqdm(oids, desc='Janelians from orcid collection'):
        if oid in PRESENT:
            preserve_mongo_names(PRESENT[oid], oids)
            if 'alumni' in PRESENT[oid]:
                continue
        if oid in PRESENT and 'employeeId' in PRESENT[oid] and not ARG.FORCE:
            continue
        correlate_person(oid, oids)


def write_records(oids):
    ''' Write records to Mongo
        Keyword arguments:
          oids: ORCID ID dict
        Returns:
          None
    '''
    coll = DB['dis'].orcid
    for oid, val in tqdm(oids.items(), desc='Updating orcid collection'):
        if oid:
            result = coll.update_one({"orcid": oid}, {"$set": val}, upsert=True)
        else:
            print(f"INSERT {val}")
            result = coll.insert_one(val)
        if hasattr(result, 'matched_count') and result.matched_count:
            COUNT['update'] += 1
        else:
            COUNT['insert'] += 1
            print(f"New entry: {val}")


def generate_email():
    ''' Generate and send an email
        Keyword arguments:
          None
        Returns:
          None
    '''
    msg = JRC.get_run_data(__file__, __version__)
    if NEW_ORCID:
        msg += f"The following ORCIDs were inserted into the {ARG.MANIFOLD} MongoDB DIS database:"
        for oid, val in NEW_ORCID.items():
            if not oid:
                oid = '(no ORCID)'
            msg += f"\n{oid}: {val}"
    if ALUMNI:
        msg += "\nThe following ORCIDs were set to alumni status:"
        for alum in ALUMNI:
            msg += f"\n{alum}"
    try:
        LOGGER.info(f"Sending email to {DISCONFIG['receivers']}")
        JRC.send_email(msg, DISCONFIG['sender'], DISCONFIG['developer'] \
                       if ARG.MANIFOLD == 'dev' else DISCONFIG['receivers'],
                       "ORCID updates")
    except Exception as err:
        LOGGER.error(err)


def handle_name(oids):
    ''' Handle a name from the command line
        Keyword arguments:
          oids: ORCID ID dict
        Returns:
          None
    '''
    add_name('', oids, ARG.FAMILY.capitalize(), ARG.GIVEN.capitalize())
    COUNT['orcid'] += 1
    correlate_person('', oids)
    if 'employeeId' not in oids['']:
        terminate_program("Could not find a record in People")
    try:
        row = DB['dis'].orcid.find_one({"employeeId": oids['']['employeeId']})
    except Exception as err:
        terminate_program(err)
    if row:
        terminate_program("Record already exists")
    if not should_continue(oids['']):
        LOGGER.warning("Record was not inserted")
        terminate_program()


def should_continue(rec):
    ''' Ask user if we should continue
        Keyword arguments:
          rec: orcid collection record
        Returns:
          True or False
    '''
    print(json.dumps(rec, indent=2))
    quest = [inquirer.Confirm("continue", message="Insert this record?", default=True)]
    ans = inquirer.prompt(quest)
    if not ans or not ans['continue']:
        return False
    return True


def perform_cleanup():
    ''' Check all ORCIDs to see if they are alumni
        Keyword arguments:
          None
        Returns:
          None
    '''
    payload = {"employeeId": {"$exists": True}, "alumni": {"$exists": False}}
    try:
        cnt = DB['dis'].orcid.count_documents(payload)
        rows = DB['dis'].orcid.find(payload)
    except Exception as err:
        terminate_program(err)
    LOGGER.info(f"Found {cnt} potential alumni")
    for row in tqdm(rows, desc='Alumni', total=cnt):
        idresp = JRC.call_people_by_id(row['employeeId'])
        if not idresp or not idresp['employeeId']:
            msg = f"{row['given']} {row['family']} ({row['employeeId']}) is now alumni"
            LOGGER.warning(msg)
            ALUMNI.append(msg)
            COUNT['alumni'] += 1
            if ARG.WRITE:
                DB['dis'].orcid.update_one({"_id": row['_id']}, {"$set": {"alumni": True}})


def update_orcid():
    ''' Update the orcid collection
        Keyword arguments:
          None
        Returns:
          None
    '''
    LOGGER.info(f"Started run (version {__version__})")
    oids = {}
    if ARG.GIVEN and ARG.FAMILY:
        handle_name(oids)
    elif ARG.ORCID:
        ARG.FORCE = True
        family, given = get_name(ARG.ORCID)
        if family and given:
            add_name(ARG.ORCID, oids, family, given)
            oids[ARG.ORCID]['orcid'] = ARG.ORCID
            COUNT['orcid'] += 1
            add_janelia_info(oids)
            if 'employeeId' not in oids[ARG.ORCID]:
                oids[ARG.ORCID]['alumni'] = True
            if not should_continue(oids[ARG.ORCID]):
                LOGGER.warning("Record was not inserted")
                terminate_program()
    else:
        # Get ORCIDs from the doi collection
        dcoll = DB['dis'].dois
        # Crossref
        payload = {"author.affiliation.name": {"$regex": "Janelia"},
                   "author.ORCID": {"$exists": True}}
        project = {"author.given": 1, "author.family": 1,
                   "author.ORCID": 1, "author.affiliation": 1, "doi": 1}
        recs = dcoll.find(payload, project)
        for rec in tqdm(recs, desc="Adding from doi collection"):
            COUNT['records'] += 1
            for aut in rec['author']:
                if 'ORCID' not in aut:
                    continue
                process_author(aut, oids, "crossref")
        add_from_orcid(oids)
        add_janelia_info(oids)
    if not (ARG.GIVEN or ARG.FAMILY or ARG.ORCID):
        perform_cleanup()
    if ARG.WRITE:
        write_records(oids)
        if NEW_ORCID or ALUMNI:
            generate_email()
    print(f"Records read from MongoDB:dois: {COUNT['records']}")
    print(f"Records read from ORCID:        {COUNT['orcid']}")
    print(f"ORCIDs inserted:                {COUNT['insert']}")
    print(f"ORCIDs updated:                 {COUNT['update']}")
    print(f"ORCIDs set to alumni:           {COUNT['alumni']}")
    if not ARG.WRITE:
        LOGGER.warning("Dry run successful, no updates were made")

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Add ORCID information to MongoDB:orcid")
    PARSER.add_argument('--orcid', dest='ORCID', action='store',
                        help='ORCID ID')
    PARSER.add_argument('--given', dest='GIVEN', action='store',
                        help='Given name')
    PARSER.add_argument('--family', dest='FAMILY', action='store',
                        help='Family name')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, prod)')
    PARSER.add_argument('--force', dest='FORCE', action='store_true',
                        default=False, help='Update ORCID ID whether correlated or not')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Write to database/config system')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    CONFIG = configparser.ConfigParser()
    CONFIG.read('config.ini')
    DISCONFIG = JRC.simplenamespace_to_dict(JRC.get_config("dis"))
    update_orcid()
    terminate_program()
