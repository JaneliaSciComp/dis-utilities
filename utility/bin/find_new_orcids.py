""" find_new_orcids.py
    Find potential new ORCIDs by checking DOIs
"""

__version__ = '1.0.0'

import argparse
from operator import attrgetter
import sys
import inquirer
from inquirer.themes import BlueComposure
from tqdm import tqdm
import jrc_common.jrc_common as JRC

# pylint: disable=broad-exception-caught,logging-fstring-interpolation,logging-not-lazy

# Database
DB = {}
# Globals
ARG = LOGGER = None
CONTROL = {}

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


def new_orcid(drow):
    ''' Check if the ORCID is new
        Keyword arguments:
          drow: row from the dois table
        Returns:
          True if the ORCID is new, False otherwise
    '''
    dorc = drow['author']['ORCID'].split("/")[-1]
    try:
        payload = {"orcid": dorc}
        cnt = DB['dis']['orcid'].count_documents(payload)
        if cnt:
            return False
        payload = {"given": drow['author']['given'], "family": drow['author']['family']}
        cnt = DB['dis']['orcid'].count_documents(payload)
        if cnt == 0:
            return False
        rows = DB['dis']['orcid'].find(payload)
    except Exception as err:
        terminate_program(err)
    for row in rows:
        if 'orcid' in row or (not CONTROL['alumni'] and 'alumni' in row):
            return False
        if 'orcid' in row:
            orc = row['orcid'].split("/")[-1]
            if orc == dorc:
                return False
        return True
    return True


def janelia_afffiliation(affiliations):
    ''' Check if the affiliation is Janelia
        Keyword arguments:
          affiliations: affiliations
        Returns:
          True if the affiliation is Janelia, False otherwise
    '''
    for aff in affiliations:
        if 'name' in aff and 'Janelia' in aff['name']:
            return True
    return False


def processing():
    ''' Process the request
        Keyword arguments:
          None
        Returns:
          None
    '''
    quest = [inquirer.Checkbox('check', message="Select options",
                               choices=['Require DOI Janelia affiliation', 'Process alumni'])]
    answers = inquirer.prompt(quest, theme=BlueComposure())
    CONTROL['janelia'] = bool('Require DOI Janelia affiliation' in answers['check'])
    CONTROL['alumni'] = bool('Process alumni' in answers['check'])
    payload = [{"$match": {"author.family": {"$exists": True}, "author.ORCID": {"$exists": True},
                           "author.affiliation.name": {"$regex": "Janelia"}}},
               {"$unwind": "$author"},
              ]
    try:
        rows = DB['dis']['dois'].aggregate(payload)
    except Exception as err:
        terminate_program(err)
    checked = {}
    dois = []
    for row in tqdm(rows, desc="Checking authors"):
        if 'ORCID' not in row['author']:
            continue
        if CONTROL['janelia'] and 'affiliation' in row['author']:
            if not janelia_afffiliation(row['author']['affiliation']):
                continue
        if row['author']['ORCID'] in checked:
            continue
        checked[row['author']['ORCID']] = True
        if new_orcid(row):
            dois.append(f"{row['doi']} {row['author']['ORCID'].split('/')[-1]} " \
                        + f"{row['author']['family']}, {row['author']['given']}")
    if dois:
        print(f"Found {len(dois)} DOI{'s' if len(dois) > 1 else ''} with new ORCID")
        print("\n".join(dois))


# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Find new ORCIDs from Crossref DOIs")
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, [prod])')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    processing()
    terminate_program()
