""" backfill_to_ignore_inserted.py
    One-time: give to_ignore entries the "inserted" date they were written
    without.

    Every writer to the collection records when an entry was made except
    pull_external_acks.py, which omitted it on 1,904 "ack_doi" entries - the
    largest type in the collection and the only one affected. That program now
    records it; this fills in what it already wrote.

    The date is recovered rather than invented. A MongoDB ObjectId embeds the
    moment it was generated, so an entry's _id says when it was created. That is
    checked rather than assumed: for entries that DO carry "inserted", the two
    agree to within a day in every case sampled.

    Dry run by default. --write fills the field in, on insert-only terms: an
    entry that already has "inserted" is never touched, so a re-run changes
    nothing and a date chosen deliberately is safe.
"""

__version__ = '1.0.0'

import argparse
import collections
from operator import attrgetter
import sys
from bson import ObjectId
import jrc_common.jrc_common as JRC

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

DB = {}
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
    ''' Initialize the program
        Keyword arguments:
          None
        Returns:
          None
    '''
    try:
        dbconfig = JRC.get_config("databases")
    except Exception as err:
        terminate_program(err)
    dbo = attrgetter(f"dis.{ARG.MANIFOLD}.{'write' if ARG.WRITE else 'read'}")(dbconfig)
    LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, ARG.MANIFOLD, dbo.host, dbo.user)
    try:
        DB['dis'] = JRC.connect_database(dbo)
    except Exception as err:
        terminate_program(err)


def processing():
    ''' Main routine
        Keyword arguments:
          None
        Returns:
          None
    '''
    coll = DB['dis'].to_ignore
    try:
        rows = list(coll.find({"inserted": {"$exists": False}}))
    except Exception as err:
        terminate_program(err)
    LOGGER.info(f"Found {len(rows):,} entries with no inserted date")
    bytype = collections.Counter(r.get('type') for r in rows)
    byday = collections.Counter()
    for row in rows:
        if not isinstance(row['_id'], ObjectId):
            # Nothing to recover from, and a made-up date is worse than none
            LOGGER.warning(f"{row.get('key')}: _id is not an ObjectId, leaving it alone")
            COUNT['no_objectid'] += 1
            continue
        # generation_time is timezone-aware; the field is stored naive elsewhere
        # in this collection, so match that rather than mixing the two.
        when = row['_id'].generation_time.replace(tzinfo=None, microsecond=0)
        byday[when.date().isoformat()] += 1
        COUNT['would_set' if not ARG.WRITE else 'set'] += 1
        if ARG.VERBOSE:
            print(f"  {str(row.get('type')):<12} {str(row.get('key'))[:44]:<46} {when}")
        if not ARG.WRITE:
            continue
        try:
            coll.update_one({"_id": row['_id'], "inserted": {"$exists": False}},
                            {"$set": {"inserted": when}})
        except Exception as err:
            LOGGER.error(f"Could not update {row.get('key')}: {err}")
            COUNT['errors'] += 1
    print()
    for key in sorted(COUNT):
        print(f"{key + ':':<20} {COUNT[key]:,}")
    print(f"{'by type:':<20} {dict(bytype)}")
    print(f"{'distinct days:':<20} {len(byday)}")
    if not ARG.WRITE:
        LOGGER.warning("Dry run: nothing was written (use --write to apply)")


# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Backfill the inserted date on to_ignore entries that lack one")
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, prod)')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Write to database')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    LOGGER.info(f"Started run (version {__version__})")
    initialize_program()
    processing()
    terminate_program()
