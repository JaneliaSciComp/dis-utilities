""" assign_authors.py
    Add/remove JRC authors for a given DOI
"""

__version__ = '4.0.0'

import argparse
import json
from operator import attrgetter
import os
import sys
from colorama import Fore, Back, Style
import inquirer
from inquirer.themes import BlueComposure
import requests
import jrc_common.jrc_common as JRC

# pylint: disable=broad-exception-caught,logging-fstring-interpolation,logging-not-lazy

# Database
DB = {}
INSENSITIVE = {'locale': 'en', 'strength': 1}
# Globals
ARG = LOGGER = REST = None

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


def get_potential_authors(authors, original):
    ''' Get the potential authors
        Keyword arguments:
          authors: list of authors from paper
          original: list of original authors
        Returns:
          list of potential authors, list of defaults
    '''
    potential = {}
    defaults = []
    for auth in authors:
        if not auth.get('given'):
            if auth.get('family'):
                LOGGER.warning(f"Author with family name [{auth['family']}] has no given name")
        if not auth.get('family'):
            if auth.get('given'):
                LOGGER.warning(f"Author with given name [{auth['given']}] has no family name")
        if not auth['in_database']:
            continue
        name = " ".join([auth['given'], auth['family']])
        notes = ''
        if auth.get('affiliations'):
            janelia = False
            for aff in auth['affiliations']:
                if 'Janelia' in aff:
                    janelia = True
                    break
            if janelia:
                notes += f" {Fore.GREEN}{Back.BLACK}Affiliation{Style.RESET_ALL}"
        if auth.get('alumni'):
            notes += f" {Fore.YELLOW}{Back.BLACK}Alumni{Style.RESET_ALL}"
        elif 'workerType' in auth and auth['workerType'] != 'Employee':
            notes += f" {Fore.YELLOW}{Back.BLACK}{auth['workerType']}{Style.RESET_ALL}"
        if 'tags' in auth and auth['tags']:
            notes = ' '.join([notes, ', '.join(auth['tags'])])
        if 'employeeId' in auth and auth['employeeId'] in original:
            defaults.append(name + notes)
        if 'employeeId' in auth and auth['employeeId']:
            potential[name + notes] = auth['employeeId']
    return potential, defaults


def auto_assign(doi, authors):
    ''' Auto assign asserted authors
        Keyword arguments:
          authors: list of authors
        Returns:
          list of JRC authors
    '''
    try:
        rec = DB['dis']['dois'].find_one({'doi': doi})
    except Exception as err:
        terminate_program(err)
    jrc_authors = rec.get('jrc_author', [])
    cnt = len(jrc_authors)
    added = []
    for auth in authors:
        if auth['asserted'] and auth.get('employeeId'):
            if auth['employeeId'] not in jrc_authors:
                jrc_authors.append(auth['employeeId'])
                added.append(f"{auth['given']} {auth['family']}")
    if len(jrc_authors) == cnt:
        LOGGER.warning(f"No additional authors to assign for {doi}")
    else:
        LOGGER.warning(f"Added to {doi}: {', '.join(added)}")
    return jrc_authors


def get_authors(doi, authors, original):
    ''' Get the JRC authors
        Keyword arguments:
          doi: DOI
          authors: list of authors
          original: list of original authors
        Returns:
          list of JRC author employee IDs
    '''
    potential, defaults = get_potential_authors(authors, original)
    if not potential:
        msg = f"No potential authors found for {doi}"
        if ARG.DOI:
            terminate_program(msg)
        else:
            LOGGER.warning(msg)
            return []
    if ARG.AUTO:
        return auto_assign(doi, authors)
    quest = [(inquirer.Checkbox('checklist', carousel=True,
                                message='Select authors',
                                choices=list(potential.keys()),
                                default=defaults))]
    answers = inquirer.prompt(quest, theme=BlueComposure())
    jrc_authors = [potential[ans] for ans in answers['checklist']]
    return jrc_authors


def get_payload(jrc_authors):
    ''' Get the payload
        Keyword arguments:
          jrc_authors: list of JRC authors
        Returns:
          payload to set/unset jrc_author
    '''
    if not jrc_authors:
        payload = {"$unset": {'jrc_author': 1, 'jrc_first_author': 1, 'jrc_last_author': 1,
                   'jrc_first_id': 1, 'jrc_last_id': 1}}
    else:
        payload = {'$set': {'jrc_author': jrc_authors}}
    return payload


def get_mongo_set(first, first_id, last, last_id):
    ''' Get the Mongo $set dictionary
        Keyword arguments:
          first: first author
          first_id: first author id
          last: last author
          last_id: last author id
        Returns:
          $set dictionary
    '''
    pset = {}
    if first:
        pset['jrc_first_author'] = first
        if first_id:
            pset['jrc_first_id'] = first_id
    if last:
        pset['jrc_last_author'] = last
        if last_id:
            pset['jrc_last_id'] = last_id
    return pset


def set_author_payload():
    ''' Set the author payload
        Keyword arguments:
          None
        Returns:
          payload to set/unset first/last author data
    '''
    try:
        headers = {"Authorization": f"Bearer {os.environ['DIS_JWT']}"}
        authors = requests.get(f"{REST['dis']['url']}doi/authors/{ARG.DOI}",
                               headers=headers, timeout=10).json()
    except Exception as err:
        terminate_program(err)
    first = []
    first_id = []
    last = None
    last_id = None
    for auth in authors['data']:
        if not auth['in_database']:
            continue
        name = ", ".join([auth['family'], auth['given']])
        if 'is_first' in auth and auth['is_first']:
            first.append(name)
            if 'employeeId' in auth and auth['employeeId']:
                first_id.append(auth['employeeId'])
        if 'is_last' in auth and auth['is_last']:
            last = name
            if 'employeeId' in auth and auth['employeeId']:
                last_id = auth['employeeId']
    pset = get_mongo_set(first, first_id, last, last_id)
    if pset:
        payload = {"$set": pset}
    else:
        payload = {"$unset": {'jrc_first_author': None, 'jrc_first_id':  None,
               'jrc_last_author': None, 'jrc_last_id': None}}
    return payload


def process_doi(doi):
    ''' Process the request
        Keyword arguments:
          doi: DOI
        Returns:
          None
    '''
    try:
        rec = DB['dis']['dois'].find_one({'doi': doi})
        if not rec:
            terminate_program(f"Could not find DOI {doi}")
        original = rec['jrc_author'] if 'jrc_author' in rec else []
    except Exception as err:
        terminate_program(err)
    try:
        headers = {"Authorization": f"Bearer {os.environ['DIS_JWT']}"}
        authors = requests.get(f"{REST['dis']['url']}doi/authors/{doi}",
                               headers=headers, timeout=10).json()
    except Exception as err:
        terminate_program(err)
    #print(json.dumps(authors['data'], indent=4))
    jrc_authors = get_authors(doi, authors['data'], original)
    if not jrc_authors:
        return
    LOGGER.debug(f"{json.dumps(jrc_authors)}")
    payload = get_payload(jrc_authors)
    LOGGER.debug(f"{doi} {len(original)} -> {len(jrc_authors)}")
    if not ARG.WRITE:
        if ARG.DEBUG:
            print(json.dumps(payload, indent=2, default=str))
    else:
        LOGGER.debug(json.dumps(payload, indent=2, default=str))
        try:
            result = DB['dis']['dois'].update_one({'doi': doi}, payload)
            if hasattr(result, 'matched_count') and result.modified_count:
                print(f"DOI {doi} updated with jrc_author")
        except Exception as err:
            terminate_program(err)
    if not jrc_authors or not ARG.WRITE:
        return
    payload = set_author_payload()
    LOGGER.debug(json.dumps(payload, indent=2, default=str))
    try:
        result = DB['dis']['dois'].update_one({'doi': doi}, payload)
        if hasattr(result, 'matched_count') and result.modified_count:
            print(f"DOI {doi} updated with first/last author information")
    except Exception as err:
        terminate_program(err)


def processing():
    ''' Process the request
        Keyword arguments:
          None
        Returns:
          None
    '''
    if ARG.DOI:
        process_doi(ARG.DOI)
    elif ARG.FILE:
        with open(ARG.FILE, 'r', encoding='ascii') as file:
            for doi in file.read().splitlines():
                process_doi(doi)
    elif ARG.FAMILY:
        if not ARG.GIVEN:
            terminate_program("Given name is required when family name is provided")
        payload = {"family": ARG.FAMILY, "given": ARG.GIVEN}
        try:
            rows = DB['dis'].orcid.find(payload).collation(INSENSITIVE)
        except Exception as err:
            terminate_program(err)
        givenl = []
        familyl = []
        for row in rows:
            givenl.extend(row['given'])
            familyl.extend(row['family'])
        if not (givenl and familyl):
            LOGGER.warning(f"No authors found for {ARG.GIVEN} {ARG.FAMILY}")
        payload = {"$or": [{"author.family": {"$in": familyl}, "author.given": {"$in": givenl}},
                           {"creators.familyName": {"$in": familyl},
                           "creators.givenName": {"$in": givenl}}]}
        try:
            rows = DB['dis'].dois.find(payload)
        except Exception as err:
            terminate_program(err)
        for row in rows:
            process_doi(row['doi'])

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Add/remove JRC authors for a given DOI")
    GROUP_A = PARSER.add_mutually_exclusive_group(required=True)
    GROUP_A.add_argument('--doi', dest='DOI', action='store',
                         help='Single DOI to process')
    GROUP_A.add_argument('--file', dest='FILE', action='store',
                         help='File of DOIs to process')
    GROUP_A.add_argument('--family', dest='FAMILY', action='store',
                         help='Family name')
    PARSER.add_argument('--given', dest='GIVEN', action='store',
                         help='Given name')
    PARSER.add_argument('--auto', dest='AUTO', action='store_true',
                        default=False, help='Auto assign asserted authors')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, [prod])')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Write to database')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    if "DIS_JWT" not in os.environ:
        terminate_program("Missing token - set in DIS_JWT environment variable")
    REST = JRC.simplenamespace_to_dict(JRC.get_config("rest_services"))
    initialize_program()
    processing()
    terminate_program()
