''' backfill_ack_source.py

PURPOSE
-------
One-time backfill of the jrc_ack_source field on DOI records that already have
jrc_acknowledgements but predate jrc_ack_source (added to pull_external_acks.py /
pull_internal_acks.py, which record the source going forward). Historical records
never stored where their acknowledgements came from, so the source is INFERRED
from the record using the same signals the pull pipeline uses to route a DOI.

INFERENCE (heuristic, precedence matches the pull pipeline's pass order so overlap
resolves the same way - e.g. an eLife DOI that also has a jrc_pmc was filled by the
eLife pass first, so it is eLife, not PMC):
  1. DOI contains "10.7554/elife"    -> eLife
  2. DOI starts with "10.1016/"       -> Elsevier
  3. record has a jrc_pmc field        -> PMC
  4. DOI contains "10.48550/arxiv"     -> arXiv
  5. otherwise                          -> unclassifiable (left unset, not guessed)

The written values match the labels the pull scripts store going forward (eLife /
Elsevier / PMC / arXiv), so backfilled and newly-recorded values are consistent.
NOTE: backfilled values are inferred, not authoritative; unclassifiable records are
left without the field rather than assigned a wrong source.

INPUTS
------
- DIS MongoDB database (read-only by default; read/write with --write).
- Command-line flags:
    --collection {dois,external_dois,both}  Collections to backfill [both].
    --write     Actually set jrc_ack_source (default: dry run).
    --verbose   Increase logging verbosity.
    --debug     Maximum logging verbosity.

Only records with jrc_acknowledgements present AND jrc_ack_source absent are
touched; an existing (authoritative) jrc_ack_source is never overwritten.

DEPENDENCIES
------------
- jrc_common.jrc_common (JRC): logging, config, database helpers.
'''

__version__ = '1.0.0'

import argparse
import collections
import json
from operator import attrgetter
import sys
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError
from tqdm import tqdm
import jrc_common.jrc_common as JRC

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

DB = {}
COUNT = collections.defaultdict(lambda: 0, {})
ARG = LOGGER = None
COLLECTIONS = ('dois', 'external_dois')
# Records whose ack source couldn't be inferred (dumped for review).
UNCLASSIFIED = []
UNCLASSIFIED_FILE = 'backfill_unclassified.json'


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
    ''' Connect to the DIS database (read-only unless --write).
        Keyword arguments:
          None
        Returns:
          None
    '''
    try:
        dbconfig = JRC.get_config("databases")
    except Exception as err:
        terminate_program(err)
    dbo = attrgetter(f"dis.prod.{'write' if ARG.WRITE else 'read'}")(dbconfig)
    LOGGER.info(f"Connecting to {dbo.name} prod on {dbo.host} as {dbo.user}")
    try:
        DB['dis'] = JRC.connect_database(dbo)
    except Exception as err:
        terminate_program(err)


def infer_ack_source(rec):
    ''' Infer the acknowledgement source for a record. Precedence mirrors the
        pull pipeline's pass order (eLife -> Elsevier -> PMC -> arXiv) so a DOI
        matching more than one signal is attributed to the pass that would have
        filled it first.
        Keyword arguments:
          rec: DOI record with at least doi and (optionally) jrc_pmc
        Returns:
          Source display label, or None if unclassifiable
    '''
    doi = (rec.get('doi') or '').lower()
    if '10.7554/elife' in doi:
        return 'eLife'
    if doi.startswith('10.1016/'):
        return 'Elsevier'
    if rec.get('jrc_pmc'):
        return 'PMC'
    if '10.48550/arxiv' in doi:
        return 'arXiv'
    return None


def backfill_collection(coll_name):
    ''' Backfill jrc_ack_source on one collection's ack-bearing records that lack it.
        Keyword arguments:
          coll_name: 'dois' or 'external_dois'
        Returns:
          None
    '''
    coll = DB['dis'][coll_name]
    match = {"jrc_acknowledgements": {"$exists": True}, "jrc_ack_source": {"$exists": False}}
    try:
        rows = list(coll.find(match, {"_id": 0, "doi": 1, "jrc_pmc": 1,
                                      "jrc_obtained_from": 1, "jrc_publishing_date": 1}))
    except Exception as err:
        terminate_program(err)
    LOGGER.info(f"{coll_name}: {len(rows):,} ack records missing jrc_ack_source")
    operations = []
    for rec in tqdm(rows, desc=f"Backfilling {coll_name}"):
        source = infer_ack_source(rec)
        if not source:
            COUNT[f'{coll_name}_unclassified'] += 1
            UNCLASSIFIED.append({"collection": coll_name, "doi": rec.get('doi'),
                                 "jrc_obtained_from": rec.get('jrc_obtained_from'),
                                 "jrc_publishing_date": rec.get('jrc_publishing_date')})
            continue
        COUNT[f'{coll_name}_{source}'] += 1
        COUNT[f'{coll_name}_classified'] += 1
        operations.append(UpdateOne({"doi": rec['doi']},
                                    {"$set": {"jrc_ack_source": source}}))
    if ARG.WRITE and operations:
        try:
            result = DB['dis'][coll_name].bulk_write(operations, ordered=False)
            COUNT[f'{coll_name}_written'] = result.modified_count
        except BulkWriteError as err:
            COUNT[f'{coll_name}_written'] = err.details.get('nModified', 0)
            errs = err.details.get('writeErrors', [])
            COUNT[f'{coll_name}_write_errors'] = len(errs)
            LOGGER.error(f"{len(errs):,} of {len(operations):,} updates failed")
        except Exception as err:
            terminate_program(err)
    else:
        COUNT[f'{coll_name}_written'] = 0


def report():
    ''' Print a per-collection, per-source backfill summary.
        Keyword arguments:
          None
        Returns:
          None
    '''
    action = "written" if ARG.WRITE else "to write (dry run)"
    for coll_name in COLLECTIONS:
        if ARG.COLLECTION not in ('both', coll_name):
            continue
        print(f"\n=== {coll_name} ===")
        for source in ('eLife', 'Elsevier', 'PMC', 'arXiv'):
            if COUNT[f'{coll_name}_{source}']:
                print(f"  {source:<10} {COUNT[f'{coll_name}_{source}']:,}")
        print(f"  {'Classified':<10} {COUNT[f'{coll_name}_classified']:,}")
        print(f"  {'Unclassified (left unset)':<10} {COUNT[f'{coll_name}_unclassified']:,}")
        print(f"  jrc_ack_source {action}: {COUNT[f'{coll_name}_written']:,}")
        if COUNT[f'{coll_name}_write_errors']:
            print(f"  write errors: {COUNT[f'{coll_name}_write_errors']:,}")


def processing():
    ''' Backfill jrc_ack_source across the selected collection(s).
        Keyword arguments:
          None
        Returns:
          None
    '''
    for coll_name in COLLECTIONS:
        if ARG.COLLECTION in ('both', coll_name):
            backfill_collection(coll_name)
    if UNCLASSIFIED:
        with open(UNCLASSIFIED_FILE, 'w', encoding='utf-8') as stream:
            json.dump(UNCLASSIFIED, stream, indent=2)
        LOGGER.info(f"Wrote {len(UNCLASSIFIED):,} unclassified records to {UNCLASSIFIED_FILE}")
    report()


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Backfill jrc_ack_source on existing ack-bearing DOI records")
    PARSER.add_argument('--collection', dest='COLLECTION', action='store',
                        choices=['dois', 'external_dois', 'both'], default='both',
                        help='Collection(s) to backfill [both]')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Flag, Update database')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    processing()
    terminate_program()
