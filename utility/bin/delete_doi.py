''' delete_dois.py
    Delete DOIs from the dois collection
'''

import argparse
import collections
from datetime import datetime
from operator import attrgetter
import sys
import inquirer
from inquirer.themes import BlueComposure
from tqdm import tqdm
import jrc_common.jrc_common as JRC

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# General variables
ARG = LOGGER = None
# Database
DB = {}
# Counters
COUNT = collections.defaultdict(lambda: 0, {})
# Canned reasons offered by the interactive menu when --reason is omitted; the
# chosen (or typed) value is stored as the to_ignore record's "reason".
DELETE_REASONS = ["No Janelia authors",
                  "Janelia editor(s) only",
                  "Work not performed at Janelia"]
OTHER_CHOICE = "Other (enter a reason)"


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
    ''' Initialize program
        Keyword arguments:
          None
        Returns:
          None
    '''
    try:
        dbconfig = JRC.get_config("databases")
    except Exception as err:
        terminate_program(err)
    dbs = ['dis']
    for source in dbs:
        dbo = attrgetter(f"{source}.{ARG.MANIFOLD}.write")(dbconfig)
        LOGGER.info(f"Connecting to {dbo.name} {ARG.MANIFOLD} on {dbo.host} as {dbo.user}")
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)


def get_reason():
    ''' Determine the (always required) deletion reason: use --reason if given,
        otherwise present an interactive menu (the canned choices plus a free-text
        "Other"). Aborts the run - deleting nothing - if no reason can be obtained.
        Keyword arguments:
          None
        Returns:
          Reason string (stripped, non-empty)
    '''
    if ARG.REASON:
        reason = ARG.REASON.strip()
        if not reason:
            terminate_program("--reason cannot be empty")
        return reason
    if not sys.stdin.isatty():
        terminate_program("A reason is required: supply --reason when running "
                          "non-interactively")
    quest = [inquirer.List('reason', carousel=True,
                           message="Reason for deleting",
                           choices=DELETE_REASONS + [OTHER_CHOICE])]
    ans = inquirer.prompt(quest, theme=BlueComposure())
    if not ans:
        # Cancelled (Ctrl-C / EOF) - abort before touching anything.
        terminate_program("No reason selected; aborting")
    reason = ans['reason']
    if reason == OTHER_CHOICE:
        tans = inquirer.prompt([inquirer.Text('reason', message="Enter a reason")],
                               theme=BlueComposure())
        reason = (tans or {}).get('reason', '').strip()
        if not reason:
            terminate_program("A reason is required; aborting")
    return reason


def process_ignore(doi):
    ''' Remove a DOI from the ignore list
        Keyword arguments:
          doi: DOI to process
        Returns:
          None
    '''
    try:
        resp = DB['dis'].to_ignore.find_one({"type": "doi", "key": doi})
    except Exception as err:
        terminate_program(err)
    if not resp:
        COUNT["missing"] += 1
        return
    try:
        resp = DB['dis'].to_ignore.delete_one({"type": "doi", "key": doi})
    except Exception as err:
        terminate_program(err)
    COUNT["deleted"] += 1


def delete_dois():
    ''' Delete DOIs from the database
        Keyword arguments:
          None
        Returns:
          None
    '''
    dois = []
    if ARG.DOI:
        dois.append(ARG.DOI)
    elif ARG.FILE:
        try:
            with open(ARG.FILE, "r", encoding="ascii") as instream:
                for doi in instream.read().splitlines():
                    dois.append(doi.lower().strip())
        except Exception as err:
            LOGGER.error(f"Could not process {ARG.FILE}")
            terminate_program(err)
    # A reason is always required for a deletion (via --reason or the interactive
    # menu). Resolve it once up front (before any writes) and apply it to every
    # DOI in the run. --ignore only removes DOIs from the ignore list, so it needs
    # no reason.
    reason = None
    if dois and not ARG.IGNORE:
        reason = get_reason()
    for doi in tqdm(dois):
        COUNT["read"] += 1
        if ARG.IGNORE:
            process_ignore(doi)
            continue
        try:
            row = DB['dis'].dois.find_one({"doi": doi})
        except Exception as err:
            terminate_program(err)
        missing = False
        if not row:
            missing = True
            COUNT["missing"] += 1
            LOGGER.warning(f"DOI {doi} not found in local database")
        if ARG.WRITE:
            if not missing:
                try:
                    resp = DB['dis'].dois.delete_one({"doi": doi})
                    COUNT['deleted'] += resp.deleted_count
                    LOGGER.warning(f"Deleted {doi}")
                except Exception as err:
                    terminate_program(f"Could not delete {doi} from dois collection: {err}")
            payload = {"type": "doi", "key": doi,
                       "inserted": datetime.today().replace(microsecond=0),
                       "reason": reason}
            try:
                resp = DB['dis'].to_ignore.find_one({"type": "doi", "key": doi})
                if not resp:
                    resp = DB['dis'].to_ignore.insert_one(payload)
                    COUNT['inserted'] += 1
            except Exception as err:
                terminate_program(f"Could not insert {doi} into to_ignore collection: {err}")
    print(f"DOIs read:                 {COUNT['read']}")
    print(f"DOIs not found:            {COUNT['missing']}")
    print(f"DOIs deleted:              {COUNT['deleted']}")
    print(f"DOIs added to ignore list: {COUNT['inserted']}")


# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Delete DOIs from the dois collection")
    group = PARSER.add_mutually_exclusive_group(required=True)
    group.add_argument('--doi', dest='DOI', action='store',
                        help='DOI to delete')
    group.add_argument('--file', dest='FILE', action='store',
                        help='File of DOIs to delete')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, prod)')
    PARSER.add_argument('--reason', dest='REASON', action='store',
                        help='Reason to delete DOI (if omitted, you are prompted '
                             'to choose one)')
    PARSER.add_argument('--ignore', dest='IGNORE', action='store_true',
                        default=False, help='Remove from ignore list only')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Actually delete DOIs')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    DISCONFIG = JRC.simplenamespace_to_dict(JRC.get_config("dis"))
    initialize_program()
    delete_dois()
    terminate_program()
