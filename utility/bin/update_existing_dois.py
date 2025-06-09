""" update_existing_dois.py
    Update existing DOIs
"""

__version__ = '1.0.0'

import argparse
import collections
from operator import attrgetter
import json
import re
import sys
from tqdm import tqdm
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Database
DB = {}
# Globals
ARG = LOGGER = None
PROJECT = {}
SUPORG = {}
AUDIT = []
# Counters
COUNT = collections.defaultdict(lambda: 0, {})

def terminate_program(msg=None):
    ''' Terminate the program gracefully
        Keyword arguments:
          msg: error message
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
    try:
        orgs = DL.get_supervisory_orgs()
    except Exception as err:
        terminate_program(err)
    for key, val in orgs.items():
        SUPORG[key] = val


def log_message(msg, row):
    ''' Log a message
        Keyword arguments:
          msg: message
        Returns:
          None
    '''
    typ = row['types']['resourceTypeGeneral'] if 'types' in row else row['type']
    msg = f"{row['doi']}\t{row['jrc_obtained_from']}\t{typ}\t{msg}"
    LOGGER.debug(msg)
    AUDIT.append(msg)


def process_journals():
    ''' Process journals
        Keyword arguments:
          None
        Returns:
          None
    '''
    payload = {"jrc_journal": {"$exists": False},
               "type": {"$nin": ["component", "grant"]}}
    try:
        cnt = DB['dis']['dois'].count_documents(payload)
        rows = DB['dis']['dois'].find(payload)
    except Exception as err:
        terminate_program(err)
    if not cnt:
        LOGGER.info("All DOIs have journals")
        return
    LOGGER.info(f"Adding journals for {cnt:,} DOIs")
    COUNT['journal_needed'] = cnt
    for row in tqdm(rows, total=cnt):
        journal = DL.get_journal(row, name_only=True)
        if journal:
            if ARG.WRITE:
                DB['dis']['dois'].update_one({"_id": row["_id"]},
                                             {"$set": {"jrc_journal": journal}})
            COUNT['journal_updated'] += 1
            log_message(f"Adding journal {journal}", row)
        else:
            log_message("Journal not found", row)
            COUNT['journal_not_found'] += 1


def needs_tagging(new_tag, jrc_tag):
    ''' Check if a DOI needs tagging
        Keyword arguments:
          new_tag: tag to check
          jrc_tag: list of tags
        Returns:
          True if needs tagging, False otherwise
    '''
    for tag in jrc_tag:
        if new_tag == tag['name']:
            return False
    return True


def process_projects():
    ''' Process projects
        Keyword arguments:
          None
        Returns:
          None
    '''
    payload = {"jrc_obtained_from": "DataCite"}
    try:
        cnt = DB['dis']['dois'].count_documents(payload)
        rows = DB['dis']['dois'].find(payload)
    except Exception as err:
        terminate_program(err)
    LOGGER.info(f"Checking projects for {cnt:,} DOIs")
    COUNT['project_check'] = cnt
    for row in tqdm(rows, total=cnt):
        if 'creators' not in row:
            continue
        for auth in row['creators']:
            if 'name' not in auth:
                continue
            if ',' in auth['name']:
                full_name = ' '.join(re.split(r'\s*,\s*', auth['name'])[::-1])
            else:
                full_name = auth['name']
            if full_name not in PROJECT or not PROJECT[full_name]:
                continue
            if 'jrc_tag' not in row:
                row['jrc_tag'] = []
            if needs_tagging(PROJECT[full_name], row['jrc_tag']):
                if PROJECT[full_name] not in SUPORG:
                    LOGGER.warning(f"Project {PROJECT[full_name]} not in supervisory orgs")
                    row['jrc_tag'].append({"name": PROJECT[full_name],
                                           "code": None, "type": "affiliation"})
                else:
                    row['jrc_tag'].append({"name": PROJECT[full_name],
                                           "code": SUPORG[PROJECT[full_name]], "type": "suporg"})
                LOGGER.debug(json.dumps(row['jrc_tag'], indent=2))
                log_message(f"Adding project {PROJECT[full_name]} to DOI", row)
                if ARG.WRITE:
                    DB['dis']['dois'].update_one({"_id": row["_id"]},
                                                 {"$set": {"jrc_tag": row['jrc_tag']}})
                COUNT['project_updated'] += 1



def processing():
    ''' Main processing routine
        Keyword arguments:
          None
        Returns:
          None
    '''
    process_journals()
    process_projects()
    print(f"DOIs needing journals:      {COUNT['journal_needed']:,}")
    print(f"DOIs updated with journals: {COUNT['journal_updated']:,}")
    print(f"DOIs without journals:      {COUNT['journal_not_found']:,}")
    print(f"DOIs checked for projects:  {COUNT['project_check']:,}")
    print(f"DOIs updated with projects: {COUNT['project_updated']:,}")
    if AUDIT:
        with open('doi_update_audit.txt', 'w', encoding='ascii') as f:
            for error in AUDIT:
                f.write(error + '\n')
        LOGGER.error("Errors written to doi_update_audit.txt")

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Fid issues in DOIs")
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, prod)')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Flag, Write to database')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    try:
        PROJECT = DL.get_project_map(DB['dis'].project_map, inactive=False)
    except Exception as gerr:
        terminate_program(gerr)
    processing()
    terminate_program()
