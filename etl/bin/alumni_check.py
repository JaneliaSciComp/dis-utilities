""" alumni_check.py
    Check orcid collection for alumni among entries with ORCID IDs, but no employee ID
"""

__version__ = '1.1.0'

import argparse
import collections
import json
from operator import attrgetter
import os
import sys
from rapidfuzz import fuzz
import requests
from tqdm import tqdm
import jrc_common.jrc_common as JRC

# pylint: disable=broad-exception-caught,logging-fstring-interpolation,logging-not-lazy

# Database
DB = {}
# General
OUT = {'No Janelia DOIs': [], 'No other DOIs after': [], 'No affiliations': [],
       'Left Janelia': [], 'Likely alumni': [], 'Potential name match': []}
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


def call_dis(oid):
    """ Call the DIS responder
        Keyword arguments:
          oid: ORCID ID
        Returns:
          JSON
    """
    url = getattr(getattr(REST, "dis"), "url") + f"orcidworks/{oid}"
    try:
        req = requests.get(url, timeout=10)
    except requests.exceptions.RequestException as err:
        terminate_program(f"Could not fetch from {url}\n{str(err)}")
    if req.status_code != 200:
        terminate_program(f"Status: {str(req.status_code)} ({url})")
    return req.json()


def find_employee_id(row):
    ''' Fuzzy match given/family names from orcid collection to People system. Return the first
        match found with >ARG.MATCH % for both first and last names.
        Keyword arguments:
          row: row from orcid collection
        Returns:
          Employee ID
          orcid collection name
          People system name
    '''
    for family in row['family']:
        people_entries = JRC.call_people_by_name(family)
        for entry in people_entries:
            LOGGER.debug(json.dumps(entry, indent=2))
            for given in row['given']:
                gratio = fuzz.ratio(given, entry['nameFirstPreferred'])
                fratio = fuzz.ratio(family, entry['nameLastPreferred'])
                if gratio > ARG.MATCH and fratio > ARG.MATCH:
                    LOGGER.debug(f"Match {gratio} {fratio}")
                    return entry['employeeId'], \
                           ' '.join([given, family]), \
                           ' '.join([entry['nameFirstPreferred'], entry['nameLastPreferred']])
    return None, None, None


def set_output(which, msg):
    ''' Set output
        Keyword arguments:
          whick: output key
          msg: message
        Returns:
          None
    '''
    OUT[which].append(msg)


def janelia_affiliation(odoi, oresp, row, affiliations):
    ''' Check for Janelia affiliation
        Keyword arguments:
          odoi: other DOI dict
          oresp: response from OA
          row: row from orcid collection
          affiliations: affiliations dict
        Returns:
          True if Janelia affiliation found
    '''
    for author in oresp['authorships']:
        auth = author['author']
        if 'orcid_number' not in auth:
            continue
        if 'orcid_number' in auth and auth['orcid_number'] == row['orcid'] \
           and 'institutions' in author and author['institutions']:
            for inst in author['institutions']:
                if 'Janelia' in inst['display_name']:
                    LOGGER.warning(f"{row['orcid']} is a Janelia author on {odoi['doi']}")
                    # This author can't be alumni
                    return True
                if odoi['doi'] not in affiliations:
                    affiliations[odoi['doi']] = []
                affiliations[odoi['doi']].append(inst['display_name'])
    return False


def possible_alumni(row, relevant):
    ''' Check for possible alumni
        Keyword arguments:
          row: row from orcid collection
          relevant: relevant other DOIs
        Returns:
          None
    '''
    affiliations = {}
    for odoi in relevant:
        oresp = JRC.call_oa(odoi['doi'])
        if oresp and 'authorships' in oresp:
            if janelia_affiliation(odoi, oresp, row, affiliations):
                return False
    if not affiliations:
        set_output('No affiliations', f"{row} has no affiliations on other DOIs")
        return False
    return affiliations


def get_end_date(summ):
    ''' Get end date
        Keyword arguments:
          summ: employment summary
        Returns:
          end date (or False if we couldn't find one)
    '''
    if summ['employment-summary']['end-date']:
        try:
            if not summ['employment-summary']['end-date']['month']:
                summ['employment-summary']['end-date']['month'] = {'value': '01'}
            if not summ['employment-summary']['end-date']['day']:
                summ['employment-summary']['end-date']['day'] = {'value': '01'}
            enddate = summ['employment-summary']['end-date']
            edate = '-'.join([enddate['year']['value'], enddate['month']['value'],
                              enddate['day']['value']])
            return edate
        except Exception as err:
            LOGGER.error("Could not determine end date for " \
                         + f"{summ['employment-summary']['end-date']}")
            terminate_program(err)
    return False


def left_janelia(resp):
    ''' Check if the person left Janelia
        Keyword arguments:
          resp: response from DIS
        Returns:
          True if person left at Janelia
    '''
    for emp in resp['orcid']['activities-summary']['employments']['affiliation-group']:
        for summ in emp['summaries']:
            if 'Janelia' in summ['employment-summary']['organization']['name']:
                if 'end-date' in summ['employment-summary']:
                    return get_end_date(summ)
    return False


def write_output():
    ''' Write output to files
        Keyword arguments:
          None
        Returns:
          None
    '''
    for key, val in OUT.items():
        fname = key.replace(' ', '_').lower() + '.txt'
        if not val:
            if os.path.exists(fname):
                os.remove(fname)
            continue
        with open(f"{fname}", 'w', encoding='utf8') as outstream:
            for line in val:
                outstream.write(f"{line}\n")
        LOGGER.info(f"Wrote {fname} ({len(val)} entr{'y' if len(val) == 1 else 'ies'})")


def processing():
    ''' Main processing routine
        Keyword arguments:
          None
        Returns:
          None
    '''
    set_flag = []
    payload = {"orcid": {"$exists": True},
               "employeeId": {"$exists": False},
               "alumni": {"$exists": False}}
    try:
        cnt = DB['dis'].orcid.count_documents(payload)
        if not cnt:
            terminate_program("No entries found")
        rows = DB['dis'].orcid.find(payload, {"_id": 0}).sort("family")
    except Exception as err:
        terminate_program(err)
    for row in tqdm(rows, total=cnt):
        COUNT['read'] += 1
        LOGGER.debug(f"Processing {row['orcid']} ({row['given']} {row['family']})")
        resp = call_dis(row['orcid'])
        gone = left_janelia(resp)
        if gone:
            set_output('Left Janelia', f"{row} left Janelia on {gone}")
            set_flag.append(row['orcid'])
            continue
        eid, oname, pname = find_employee_id(row)
        if eid:
            set_output('Potential name match', f"{row}\t{eid}\t{oname}\t{pname}")
            continue
        if 'janelia_dois' in resp and not resp['janelia_dois']:
            set_output('No Janelia DOIs', f"{row} has no Janelia DOIs")
            continue
        if 'other_dois' in resp:
            relevant = []
            if resp['other_dois']:
                for odoi in resp['other_dois']:
                    if odoi['date'] > resp['last_janelia_doi']['jrc_publishing_date']:
                        relevant.append(odoi)
            if not relevant:
                set_output('No other DOIs after', \
                           f"{row} has no other DOIs after " \
                           + f"{resp['last_janelia_doi']['jrc_publishing_date']}")
                continue
        alum = possible_alumni(row, relevant)
        if alum:
            set_output('Likely alumni', f"{row} is likely alumni\n{alum}")
            set_flag.append(row['orcid'])
    write_output()
    for oid in set_flag:
        if ARG.WRITE:
            try:
                print(oid)
                result = DB['dis'].orcid.update_one({"orcid": oid}, {"$set": {"alumni": True}})
                if hasattr(result, 'matched_count') and result.matched_count:
                    COUNT['updated'] += 1
            except Exception as err:
                terminate_program(err)
        else:
            LOGGER.info(f"Would have updated {oid}")
            COUNT['updated'] += 1
    print(f"Entries read:    {COUNT['read']}")
    print(f"Entries updated: {COUNT['updated']}")


# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Template program")
    PARSER.add_argument('--file', dest='FILE', action='store',
                        default='export_with_title_updated.json', help='Tag JSON file')
    PARSER.add_argument('--match', dest='MATCH', action='store',
                        default=90, type=int, help='Fuzzy name match threshold')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, prod)')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Write to database/config system')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    REST = JRC.get_config("rest_services")
    processing()
    terminate_program()
