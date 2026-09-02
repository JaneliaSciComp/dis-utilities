''' delete_doi.py
    Delete DOIs from the dois collection
'''

__version__ = '1.3.2'

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
                  "Janelia authors with Present address",
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
    ''' Determine the deletion reason, required for any run that writes: use
        --reason if given, otherwise present an interactive menu (the canned
        choices plus a free-text "Other"). Aborts the run - deleting nothing -
        if no reason can be obtained.
        Keyword arguments:
          None
        Returns:
          Reason string (stripped, non-empty)
    '''
    if ARG.REASON is not None:
        # "is not None" so an explicit --reason "" is rejected here instead of
        # falling through to the interactive menu.
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
    try:
        ans = inquirer.prompt(quest, theme=BlueComposure())
    except (KeyboardInterrupt, EOFError):
        terminate_program("User cancelled program")
    if not ans:
        # Cancelled (Ctrl-C / EOF) - abort before touching anything.
        terminate_program("No reason selected; aborting")
    reason = ans['reason']
    if reason == OTHER_CHOICE:
        try:
            tans = inquirer.prompt([inquirer.Text('reason', message="Enter a reason")],
                                   theme=BlueComposure())
        except (KeyboardInterrupt, EOFError):
            terminate_program("User cancelled program")
        # "or ''" - the .get default only covers a missing key, but the renderer
        # can hand back None for an empty submission.
        reason = ((tans or {}).get('reason') or '').strip()
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
    if not ARG.WRITE:
        # Dry run: report whether the DOI is on the list, change nothing. Matches
        # the delete path, which likewise only writes under --write.
        try:
            resp = DB['dis'].to_ignore.find_one({"type": "doi", "key": doi})
        except Exception as err:
            terminate_program(err)
        if resp:
            COUNT["deleted"] += 1      # would be removed under --write
        else:
            COUNT["not_ignored"] += 1
        return
    try:
        resp = DB['dis'].to_ignore.delete_one({"type": "doi", "key": doi})
    except Exception as err:
        terminate_program(err)
    if resp.deleted_count:
        COUNT["deleted"] += resp.deleted_count
    else:
        # Counted separately from "not in the dois collection" - the two modes
        # mean different things by "missing".
        COUNT["not_ignored"] += 1


def delete_dois():
    ''' Delete DOIs from the database
        Keyword arguments:
          None
        Returns:
          None
    '''
    dois = []
    if ARG.DOI:
        # Normalize exactly as the file path does - DOIs are stored lowercase, so
        # an unnormalized --doi would miss the record and then write an
        # unmatchable key to to_ignore. Test after stripping, so a whitespace-only
        # --doi cannot become an empty key either.
        doi = ARG.DOI.lower().strip()
        if doi:
            dois.append(doi)
    elif ARG.FILE:
        try:
            with open(ARG.FILE, "r", encoding="utf-8-sig") as instream:
                for line in instream.read().splitlines():
                    doi = line.lower().strip()
                    if doi:   # skip blank lines rather than ignoring an empty DOI
                        dois.append(doi)
        except Exception as err:
            LOGGER.error(f"Could not process {ARG.FILE}")
            terminate_program(err)
    if not dois:
        LOGGER.warning("No DOIs to process")
    # A reason is required for any deletion that is actually recorded (via
    # --reason or the interactive menu). Resolve it once up front, before any
    # writes, and apply it to every DOI in the run. Dry runs write nothing so
    # they do not prompt, and --ignore only removes DOIs from the ignore list,
    # so it needs no reason either.
    reason = None
    if dois and ARG.WRITE and not ARG.IGNORE:
        reason = get_reason()
    for doi in tqdm(dois):
        COUNT["read"] += 1
        if ARG.IGNORE:
            process_ignore(doi)
            continue
        try:
            row = DB['dis'].dois.find_one({"doi": doi}, {"_id": 1})
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
            try:
                # Backfill-only upsert. A find/insert pair silently dropped the
                # reason when the DOI was already listed; a blanket $set would go
                # the other way and clobber a reason chosen deliberately in an
                # earlier run. $ifNull fills only what is missing. $literal keeps
                # a reason beginning with "$" from being read as a field path.
                now = datetime.today().replace(microsecond=0)
                resp = DB['dis'].to_ignore.update_one(
                    {"type": "doi", "key": doi},
                    [{"$set": {"reason": {"$ifNull": ["$reason", {"$literal": reason}]},
                               "inserted": {"$ifNull": ["$inserted", now]}}}],
                    upsert=True)
                if resp.upserted_id:
                    COUNT['inserted'] += 1
                elif resp.modified_count:
                    COUNT['reason_backfilled'] += 1
                else:
                    # Matched but unchanged - already listed with a reason.
                    COUNT['unchanged'] += 1
            except Exception as err:
                terminate_program(f"Could not add {doi} to to_ignore collection: {err}")
        else:
            # Dry run: report the same three outcomes the write path counts,
            # without touching anything.
            if not missing:
                COUNT['deleted'] += 1
            try:
                listed = DB['dis'].to_ignore.find_one({"type": "doi", "key": doi},
                                                      {"reason": 1})
            except Exception as err:
                terminate_program(err)
            if not listed:
                COUNT['inserted'] += 1
            elif not listed.get('reason'):
                COUNT['reason_backfilled'] += 1
            else:
                COUNT['unchanged'] += 1
    print(f"DOIs read:                 {COUNT['read']}")
    if ARG.IGNORE:
        print(f"DOIs not on ignore list:   {COUNT['not_ignored']}")
        label = "DOIs removed from ignore" if ARG.WRITE else "Would remove from ignore"
        print(f"{label + ':':<27}{COUNT['deleted']}")
    else:
        print(f"DOIs not found:            {COUNT['missing']}")
        for done, would, key in (("DOIs deleted", "Would delete", 'deleted'),
                                 ("DOIs added to ignore list", "Would add to ignore list",
                                  'inserted'),
                                 ("Ignore reasons backfilled", "Would backfill reason",
                                  'reason_backfilled')):
            print(f"{(done if ARG.WRITE else would) + ':':<27}{COUNT[key]}")
        print(f"Already on ignore list:    {COUNT['unchanged']}")
        if reason:
            # "selected", not "applied": backfill-only keeps whatever reason an
            # already-listed DOI carries, so the chosen one may not be recorded.
            print(f"Reason selected:           {reason}")
            if COUNT['unchanged']:
                print(f"  ({COUNT['unchanged']} already listed - existing reason kept)")
    if not ARG.WRITE:
        LOGGER.warning("Dry run successful, no updates were made")


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
                        default=False,
                        help='Remove DOIs from the ignore list instead of '
                             'deleting them')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False,
                        help='Actually apply changes (delete DOIs / remove from '
                             'the ignore list)')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    LOGGER.info(f"Started run (version {__version__})")
    initialize_program()
    delete_dois()
    terminate_program()
