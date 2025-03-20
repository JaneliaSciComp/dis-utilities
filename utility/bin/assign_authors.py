""" assign_authors.py
    Add/remove JRC authors for a given DOI
"""

__version__ = '1.2.0'

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
        if not auth['in_database']:
            continue
        name = " ".join([auth['given'], auth['family']])
        notes = ''
        if 'alumni' in auth and auth['alumni']:
            notes = f" {Fore.YELLOW}{Back.BLACK}Alumni{Style.RESET_ALL}"
        elif 'workerType' in auth and auth['workerType'] != 'Employee':
            notes = f" {Fore.YELLOW}{Back.BLACK}{auth['workerType']}{Style.RESET_ALL}"
        if 'tags' in auth and auth['tags']:
            notes = ' '.join([notes, ', '.join(auth['tags'])])
        if 'employeeId' in auth and auth['employeeId'] in original:
            defaults.append(name + notes)
        if 'employeeId' in auth and auth['employeeId']:
            potential[name + notes] = auth['employeeId']
    if not potential:
        LOGGER.error("No potential authors found")
        terminate_program()
    return potential, defaults


def get_authors(authors, original):
    ''' Get the JRC authors
        Keyword arguments:
          authors: list of authors
          original: list of original authors
        Returns:
          list of JRC authors
    '''
    potential, defaults = get_potential_authors(authors, original)
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


def processing():
    ''' Process the request
        Keyword arguments:
          None
        Returns:
          None
    '''
    try:
        rec = DB['dis']['dois'].find_one({'doi': ARG.DOI})
        if not rec:
            terminate_program(f"Could not find DOI {ARG.DOI}")
        original = rec['jrc_author'] if 'jrc_author' in rec else []
    except Exception as err:
        terminate_program(err)
    try:
        headers = {"Authorization": f"Bearer {os.environ['DIS_JWT']}"}
        authors = requests.get(f"{REST['dis']['url']}doi/authors/{ARG.DOI}",
                               headers=headers, timeout=10).json()
    except Exception as err:
        terminate_program(err)
    jrc_authors = get_authors(authors['data'], original)
    LOGGER.debug(f"{json.dumps(jrc_authors)}")
    payload = get_payload(jrc_authors)
    if not ARG.WRITE:
        print(json.dumps(payload, indent=2, default=str))
    else:
        LOGGER.debug(json.dumps(payload, indent=2, default=str))
        try:
            result = DB['dis']['dois'].update_one({'doi': ARG.DOI}, payload)
            if hasattr(result, 'matched_count') and result.modified_count:
                print(f"DOI {ARG.DOI} updated with jrc_author")
        except Exception as err:
            terminate_program(err)
    if not jrc_authors or not ARG.WRITE:
        return
    payload = set_author_payload()
    LOGGER.debug(json.dumps(payload, indent=2, default=str))
    try:
        result = DB['dis']['dois'].update_one({'doi': ARG.DOI}, payload)
        if hasattr(result, 'matched_count') and result.modified_count:
            print(f"DOI {ARG.DOI} updated with first/last author information")
    except Exception as err:
        terminate_program(err)

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Add/remove JRC authors for a given DOI")
    PARSER.add_argument('--doi', dest='DOI', action='store',
                        required=True, help='DOI')
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
