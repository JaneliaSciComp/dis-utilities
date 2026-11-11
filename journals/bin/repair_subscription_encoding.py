''' repair_subscription_encoding.py
    Repair subscription records whose UTF-8 text was misread as Mac Roman or
    Windows-1252 before it was loaded, so "C‚ÄîH Bonds" goes back to "C—H Bonds" and
    "Universit√§tsverlag" to "Universitätsverlag".

    Damage is detected with load_kbart.repair_mojibake, the same test load_kbart.py now
    runs on every input file, so the two cannot disagree about what is damaged.

    Dry run by default. Every run writes a JSON report of each change (record, field,
    old and new value); --undo puts back the old values from a written report.
'''

__version__ = '1.0.0'

import argparse
import collections
from datetime import datetime
import json
from operator import attrgetter
import sys
from bson import ObjectId
import jrc_common.jrc_common as JRC
from load_kbart import repair_mojibake

# pylint: disable=broad-exception-caught, logging-fstring-interpolation
# Database
DB = {}
# Counters
COUNT = collections.defaultdict(int)
# Globals
ARG = LOGGER = None
EXAMPLES = 10


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
    ''' Connect to the database: read-only for a dry run, write for --write or --undo
        Keyword arguments:
          None
        Returns:
          None
    '''
    try:
        dbconfig = JRC.get_config("databases")
    except Exception as err:
        terminate_program(err)
    access = 'write' if ARG.WRITE else 'read'
    dbo = attrgetter(f"dis.{ARG.MANIFOLD}.{access}")(dbconfig)
    LOGGER.info(f"Connecting to {dbo.name} {ARG.MANIFOLD} on {dbo.host} as {dbo.user}")
    try:
        DB['dis'] = JRC.connect_database(dbo)
    except Exception as err:
        terminate_program(err)


def find_repairs(rows):
    ''' Every damaged top-level string field in the given subscription records.
        Keyword arguments:
          rows: subscription records
        Returns:
          List of dicts (_id, id_type, provider, identifier, type, title, field, old, new)
    '''
    repairs = []
    for row in rows:
        for field, value in row.items():
            if field == '_id' or not isinstance(value, str):
                continue
            fixed = repair_mojibake(value)
            if fixed is not None:
                repairs.append({'_id': row['_id'], 'id_type': type(row['_id']).__name__,
                                'provider': row.get('provider'),
                                'identifier': row.get('identifier'), 'type': row.get('type'),
                                'title': row.get('title'), 'field': field,
                                'old': value, 'new': fixed})
    return repairs


def mark_conflicts(coll, repairs):
    ''' A repaired title can collide with another record on the unique
        (title, provider, identifier) index, for example when a later, clean load
        already inserted the correct spelling. Those repairs are marked and skipped:
        the two records need merging by hand.
        Keyword arguments:
          coll: subscription collection
          repairs: output of find_repairs
        Returns:
          None (sets status on each repair)
    '''
    for rep in repairs:
        rep['status'] = 'pending'
        if rep['field'] != 'title':
            continue
        try:
            other = coll.find_one({'title': rep['new'], 'provider': rep['provider'],
                                   'identifier': rep['identifier'], '_id': {'$ne': rep['_id']}},
                                  {'_id': 1})
        except Exception as err:
            terminate_program(err)
        if other:
            rep['status'] = 'conflict'
            rep['conflicts_with'] = other['_id']
            COUNT['conflict'] += 1


def apply_repairs(coll, repairs):
    ''' Write each pending repair. The update matches the old value too, so a field that
        changed since it was read is left alone and reported as stale.
        Keyword arguments:
          coll: subscription collection
          repairs: output of find_repairs, after mark_conflicts
        Returns:
          None (sets status on each repair)
    '''
    for rep in repairs:
        if rep['status'] != 'pending':
            continue
        if not ARG.WRITE:
            rep['status'] = 'would repair'
            COUNT['would repair'] += 1
            continue
        try:
            result = coll.update_one({'_id': rep['_id'], rep['field']: rep['old']},
                                     {'$set': {rep['field']: rep['new']}})
        except Exception as err:
            LOGGER.error(f"Could not repair {rep['field']} of {rep['_id']}: {err}")
            rep['status'] = 'error'
            COUNT['error'] += 1
            continue
        rep['status'] = 'repaired' if result.modified_count else 'stale'
        COUNT[rep['status']] += 1


def undo_repairs(coll, entries):
    ''' Put back the old values from a report, for the entries that were repaired.
        Only a field still holding the repaired value is changed.
        Keyword arguments:
          coll: subscription collection
          entries: report entries
        Returns:
          None (sets status on each entry)
    '''
    for ent in entries:
        if ent.get('status') != 'repaired':
            ent['undo'] = 'not repaired'
            continue
        # the report holds _id as text; id_type says whether it was an ObjectId
        rid = ObjectId(ent['_id']) if ent.get('id_type') == 'ObjectId' else ent['_id']
        if not ARG.WRITE:
            ent['undo'] = 'would restore'
            COUNT['would restore'] += 1
            continue
        try:
            result = coll.update_one({'_id': rid, ent['field']: ent['new']},
                                     {'$set': {ent['field']: ent['old']}})
        except Exception as err:
            LOGGER.error(f"Could not restore {ent['field']} of {ent['_id']}: {err}")
            ent['undo'] = 'error'
            COUNT['error'] += 1
            continue
        ent['undo'] = 'restored' if result.modified_count else 'stale'
        COUNT[ent['undo']] += 1


def write_report(name, entries):
    ''' Write the run's entries to a timestamped JSON file.
        Keyword arguments:
          name: report kind (repair or undo)
          entries: list of dicts
        Returns:
          File name
    '''
    mode = 'written' if ARG.WRITE else 'dryrun'
    fname = f"{name}_encoding_{mode}_{datetime.now().strftime('%Y%m%dT%H%M%S')}.json"
    with open(fname, 'w', encoding='utf-8') as outstream:
        outstream.write(json.dumps(entries, default=str, indent=2, ensure_ascii=False))
    return fname


def summarize(repairs):
    ''' Print what was found and what happened to it.
        Keyword arguments:
          repairs: output of find_repairs, after apply_repairs
        Returns:
          None
    '''
    records = {rep['_id'] for rep in repairs}
    print(f"\nDamaged records: {len(records):,} (damaged values: {len(repairs):,})")
    for label, key in (("By field", 'field'), ("By provider", 'provider'), ("By type", 'type')):
        cnt = collections.Counter(rep[key] for rep in repairs)
        print(f"{label}:")
        for name, num in cnt.most_common():
            print(f"  {name}: {num:,}")
    print("Examples:")
    for rep in repairs[:EXAMPLES]:
        print(f"  {rep['field']}: {rep['old']!r}\n      -> {rep['new']!r}")
    print("Results:")
    for status in ('would repair', 'repaired', 'stale', 'conflict', 'error'):
        if COUNT[status]:
            print(f"  {status}: {COUNT[status]:,}")
    if COUNT['conflict']:
        print("Conflicts are titles whose repaired spelling already exists for the same "
              + "provider and identifier; merge those records by hand.")
    if COUNT['stale']:
        print("Stale values changed between reading and writing; rerun to pick them up.")


def processing():
    ''' Find, report and (with --write) repair damaged subscription text
        Keyword arguments:
          None
        Returns:
          None
    '''
    coll = DB['dis'].subscription
    try:
        repairs = find_repairs(coll.find({}))
    except Exception as err:
        terminate_program(err)
    if not repairs:
        print("No damaged text found")
        return
    mark_conflicts(coll, repairs)
    apply_repairs(coll, repairs)
    summarize(repairs)
    print(f"\nWrote report to {write_report('repair', repairs)}")
    if not ARG.WRITE:
        print("Dry run: nothing was changed. Rerun with --write to repair.")


def undo():
    ''' Restore old values from a repair report
        Keyword arguments:
          None
        Returns:
          None
    '''
    try:
        with open(ARG.UNDO, encoding='utf-8') as instream:
            entries = json.load(instream)
    except Exception as err:
        terminate_program(err)
    undo_repairs(DB['dis'].subscription, entries)
    print(f"Entries in report: {len(entries):,}")
    for status in ('would restore', 'restored', 'stale', 'error'):
        if COUNT[status]:
            print(f"  {status}: {COUNT[status]:,}")
    print(f"Wrote report to {write_report('undo', entries)}")
    if not ARG.WRITE:
        print("Dry run: nothing was changed. Rerun with --write to restore.")

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Repair subscription text that was misread as Mac Roman/Windows-1252")
    PARSER.add_argument('--undo', dest='UNDO', action='store', default=None,
                        help='Restore the old values from a written repair report')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Write to database')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, prod)')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    if ARG.UNDO:
        undo()
    else:
        processing()
    terminate_program()
