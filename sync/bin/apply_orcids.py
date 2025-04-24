''' apply_orcids.py
    Apply ORCIDs from the ORCID API to the orcid collection
'''

__version__ = '1.0.0'

import argparse
import collections
import configparser
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
ARG = CONFIG = DIS = LOGGER = None
# Output files
OUTPUT = {"name_error": [], "name_multi_records": [], "name_not_found": [], "orcid_exists": [],
          "orcid_mismatch": []}

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


def check_orcid(oid, name, family, given):
    ''' Check an ORCID record
        Keyword arguments:
          oid: ORCID
          name: name record from ORCID
          family: family name
          given: given name
    '''
    LOGGER.debug(f"{oid}: {family}, {given}")
    coll = DB['dis'].orcid
    try:
        cnt = coll.count_documents({'orcid': oid})
        if cnt:
            OUTPUT['orcid_exists'].append(name)
            return
        payload = {'orcid': oid, 'family': family, 'given': given}
        cnt = coll.count_documents(payload)
        if not cnt:
            OUTPUT['name_not_found'].append(name)
            return
        if cnt > 1:
            OUTPUT['name_multi_records'].append(name)
            return
        rec = coll.find_one(payload)
    except Exception as err:
        terminate_program(err)
    if 'orcid' in rec:
        OUTPUT['orcid_mismatch'].append(name)
        return
    OUTPUT['orcid_missing'].append(name)
    print(f"{oid}: {family}, {given}")


def process_orcid(oid):
    ''' Process an ORCID ID
        Keyword arguments:
          oid: ORCID
        Returns:
          None
    '''
    url = f"{CONFIG['orcid']['base']}{oid}"
    try:
        resp = requests.get(url, timeout=10,
                            headers={"Accept": "application/json"})
        orc = resp.json()
    except Exception as err:
        terminate_program(err)
    name = orc['person']['name']
    if not name or'family-name' not in name or 'given-names' not in name:
        LOGGER.warning(f"ORCID {oid} has no name")
        OUTPUT['name_error'].append(name)
        return
    if not (name['family-name'] and name['given-names']):
        OUTPUT['name_error'].append(name)
        return
    family = name['family-name']['value']
    given = name['given-names']['value']
    if not (family and given):
        OUTPUT['name_error'].append(name)
        return
    check_orcid(oid, name, family, given)


def apply_orcids():
    ''' Find ORCID IDs using the ORCID API
        Keyword arguments:
          None
        Returns:
          None
    '''
    base = f"{CONFIG['orcid']['base']}search"
    search = {'hhmi': ['/?q=ror-org-id:"' + CONFIG['ror']['hhmi'] + '"',
                       '/?q=affiliation-org-name:"Howard Hughes Medical Institute"'],
              'janelia': ['/?q=ror-org-id:"' + CONFIG['ror']['janelia'] + '"',
                          '/?q=affiliation-org-name:"Janelia Research Campus"',
                          '/?q=affiliation-org-name:"Janelia Farm Research Campus"']
             }
    for url in (search[ARG.INSTITUTION]):
        try:
            resp = requests.get(f"{base}{url}", timeout=10,
                                headers={"Accept": "application/json"})
        except Exception as err:
            terminate_program(err)
        oids = []
        for orcid in resp.json()['result']:
            oid = orcid['orcid-identifier']['path']
            if oid not in oids:
                oids.append(oid)
    for oid in tqdm(oids, desc="Processing ORCIDs"):
        COUNT['orcid'] += 1
        if oid in DIS['orcid_ignore']:
            COUNT['orcid_ignored'] += 1
            continue
        process_orcid(oid)
    print(f"ORCIDs read:                  {COUNT['orcid']}")
    print(f"ORCIDs ignored:               {COUNT['orcid_ignored']}")
    print(f"ORCIDs with name error:       {len(OUTPUT['name_error'])}")
    print(f"ORCIDs existing:              {len(OUTPUT['orcid_exists'])}")
    print(f"ORCIDs with name not found:   {len(OUTPUT['name_not_found'])}")
    print(f"ORCIDs with multiple records: {len(OUTPUT['name_multi_records'])}")
    print(f"ORCIDs with mismatch:         {len(OUTPUT['orcid_mismatch'])}")
    for key, value in OUTPUT.items():
        fname = f"orcid_{key}.json"
        if value:
            with open(fname, "w", encoding="utf-8") as outstream:
                outstream.write(json.dumps(value, indent=2))
        elif os.path.exists(fname):
            os.remove(fname)

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Apply ORCIDS to orcid collection")
    PARSER.add_argument('--institution', dest='INSTITUTION', action='store',
                        default='janelia', choices=['hhmi', 'janelia'],
                        help='Institution (hhmi, [janelia])')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, prod)')
    PARSER.add_argument('--test', dest='TEST', action='store_true',
                        default=False, help='Send email to developer')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Write to database/config system')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    try:
        DIS = JRC.simplenamespace_to_dict(JRC.get_config("dis"))
    except Exception as err:
        terminate_program(err)
    initialize_program()
    CONFIG = configparser.ConfigParser()
    CONFIG.read('config.ini')
    DIS = JRC.simplenamespace_to_dict(JRC.get_config("dis"))
    apply_orcids()
    terminate_program()
