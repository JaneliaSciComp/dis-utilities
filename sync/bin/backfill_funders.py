''' backfill_funders.py
    Populate jrc_funder and jrc_funder_ids on DOI records already in the
    collection.

    update_dois.py writes these fields for anything it loads or refreshes from
    now on, but it only revisits a record when the registrar's metadata
    changes. This walks the whole collection once so the field means the same
    thing for a 2015 paper as for yesterday's.

    Nothing is fetched from a registrar: the funder data is already on each
    record, deposited by Crossref as funder[] or by DataCite as
    fundingReferences[]. The only outbound calls resolve a funder's ancestors
    in the Crossref Funder Registry, and a ROR to its Funder Registry ID.

    Those resolutions are cached in the funder collection, and that cache is
    written even on a dry run - deliberately. It holds public registry data
    rather than anything about our DOIs, the API is rate-limited, and warming
    it means the --write pass does no network work at all. No DOI record is
    touched without --write.
'''

__version__ = '1.0.0'

import argparse
import collections
from operator import attrgetter
import sys
import pymongo
from tqdm import tqdm
import jrc_common.jrc_common as JRC
import dis_funder_lib as DFL

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Database
DB = {}
# Counters
COUNT = collections.defaultdict(lambda: 0, {})
# Global variables
ARG = LOGGER = None


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
    ''' Initialize database connection
        Keyword arguments:
          None
        Returns:
          None
    '''
    try:
        dbconfig = JRC.get_config("databases")
    except Exception as err:
        terminate_program(err)
    for source in ['dis']:
        dbo = attrgetter(f"{source}.{ARG.MANIFOLD}.write")(dbconfig)
        LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, ARG.MANIFOLD, dbo.host, dbo.user)
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)


def process_record(rec, cache):
    ''' Parse one record's funders and store them if they changed
        Keyword arguments:
          rec: DOI record
          cache: funder collection, for hierarchy and ROR lookups
        Returns:
          None
    '''
    COUNT['read'] += 1
    funders = DFL.parse_funders(rec, cache)
    if not funders:
        COUNT['no_funders'] += 1
        return
    COUNT['with_funders'] += 1
    rollup = DFL.rollup_ids(funders, cache)
    named = [f for f in funders if f.get('id')]
    COUNT['funder_entries'] += len(funders)
    COUNT['unjoinable'] += len(funders) - len(named)
    if any(f.get('awards') for f in funders):
        COUNT['with_awards'] += 1
    # A record already carrying identical values is left alone, so a re-run
    # is free and the updated timestamp does not churn.
    if rec.get('jrc_funder') == funders and (rec.get('jrc_funder_ids') or []) == rollup:
        COUNT['unchanged'] += 1
        return
    COUNT['to_update'] += 1
    if ARG.VERBOSE:
        ids = ', '.join(f.get('id') or f"({f.get('name')})" for f in funders)
        print(f"  {rec['doi']:<44} {ids}")
    if not ARG.WRITE:
        return
    payload = {'jrc_funder': funders}
    if rollup:
        payload['jrc_funder_ids'] = rollup
    try:
        DB['dis'].dois.update_one({'_id': rec['_id']}, {'$set': payload})
        COUNT['written'] += 1
    except pymongo.errors.PyMongoError as err:
        terminate_program(err)


def processing():
    ''' Walk the collection
        Keyword arguments:
          None
        Returns:
          None
    '''
    cache = DB['dis'].funder
    payload = {"$or": [{"funder": {"$exists": True}},
                       {"fundingReferences.0": {"$exists": True}}]}
    try:
        total = DB['dis'].dois.count_documents(payload)
        recs = DB['dis'].dois.find(payload)
    except Exception as err:
        terminate_program(err)
    LOGGER.info(f"Found {total:,} records carrying funder metadata")
    for rec in tqdm(recs, total=total, desc='Records'):
        process_record(rec, cache)
    print(f"\nRecords with funder metadata: {COUNT['read']:,}")
    print(f"  with funders parsed:        {COUNT['with_funders']:,}")
    print(f"  with no parseable funder:   {COUNT['no_funders']:,}")
    print(f"Funder entries:               {COUNT['funder_entries']:,}")
    print(f"  with no joinable ID:        {COUNT['unjoinable']:,}")
    print(f"Records carrying awards:      {COUNT['with_awards']:,}")
    print(f"Already up to date:           {COUNT['unchanged']:,}")
    print(f"Needing update:               {COUNT['to_update']:,}")
    print(f"Funders cached:               {DB['dis'].funder.count_documents({}):,}"
          "  (registry lookups, cached on any run)")
    if ARG.WRITE:
        print(f"Records written:              {COUNT['written']:,}")
    else:
        LOGGER.warning("Dry run - no DOI record was updated "
                       "(the funder cache above was populated)")


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Backfill jrc_funder/jrc_funder_ids on existing DOI records")
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
