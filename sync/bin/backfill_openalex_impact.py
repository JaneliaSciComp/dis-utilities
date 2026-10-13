""" backfill_openalex_impact.py
    Populate jrc_openalex_impact on DOIs that do not have it yet.

    sync_citations.py collects this field as a side effect of its OpenAlex
    lookup, so it fills in on its own over time. That is the right behavior
    going forward and the wrong way to backfill: a sync_citations pass spends
    roughly 33 seconds per DOI paginating ScholeXplorer for citing DOIs, and
    this field comes from neither ScholeXplorer nor the pagination - the
    OpenAlex work lookup it rides on takes about 0.7 seconds. Backfilling the
    whole corpus through the sync is hours; doing only the lookup is minutes.

    So this makes exactly one request per DOI and writes one field. It shares
    sync_citations' OpenAlex auth and retry helpers rather than reimplementing
    them, so the polite-pool contact, the API key and the backoff behavior
    cannot drift from the nightly sync.

    Only journal articles and preprints are considered. OpenAlex does classify
    some datasets and software, but a percentile for a deposit is not comparable
    to one for a paper, and nothing displays it.

    Idempotent and resumable: already-populated DOIs are skipped unless --all is
    given, so an interrupted run is restarted by running it again.

    Delete this program once the corpus is covered. The nightly sync keeps the
    field current from then on.
"""

__version__ = '1.0.0'

import argparse
from operator import attrgetter
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

from pymongo import UpdateOne
from tqdm import tqdm

import jrc_common.jrc_common as JRC
# The nightly sync's helpers: identical auth, identical retry/backoff.
from sync_citations import OPENALEX_WORK, get_with_retry, with_openalex_auth

# Database
DB = {}
# Globals
ARG = LOGGER = None
# Only these carry a comparable percentile - see the module docstring.
SCHOLARLY = {"$or": [{"type": "journal-article", "subtype": {"$exists": False}},
                     {"subtype": "preprint"},
                     {"types.resourceTypeGeneral": "Preprint"}]}
COUNT = {'examined': 0, 'classified': 0, 'unclassified': 0, 'errored': 0, 'written': 0}


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
    if not os.environ.get('OPENALEX_EMAIL'):
        terminate_program("OPENALEX_EMAIL is not set; OpenAlex's polite pool requires a "
                          "contact address and sync_citations uses the same variable")
    try:
        dbconfig = JRC.get_config("databases")
    except Exception as err:
        terminate_program(err)
    dbo = attrgetter(f"dis.{ARG.MANIFOLD}.write")(dbconfig)
    LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, ARG.MANIFOLD, dbo.host, dbo.user)
    try:
        DB['dis'] = JRC.connect_database(dbo)
    except Exception as err:
        terminate_program(err)


def fetch_impact(doi):
    ''' One OpenAlex lookup for one DOI
        Keyword arguments:
          doi: DOI string
        Returns:
          (doi, impact dict or None, errored bool)
    '''
    resp = get_with_retry(f"{OPENALEX_WORK}{doi.lower()}",
                          with_openalex_auth({'select': 'fwci,citation_normalized_percentile'}),
                          20)
    if resp is not None and resp.status_code == 404:
        return doi, None, False          # OpenAlex does not hold it: a finding, not an error
    if resp is None or resp.status_code != 200:
        return doi, None, True
    try:
        data = resp.json()
    except Exception:
        return doi, None, True
    pct = data.get('citation_normalized_percentile') or {}
    if data.get('fwci') is None and pct.get('value') is None:
        return doi, None, False          # held but unclassified
    return doi, {'source': 'openalex',
                 'fwci': data.get('fwci'),
                 'percentile': pct.get('value'),
                 'top_1_percent': pct.get('is_in_top_1_percent'),
                 'top_10_percent': pct.get('is_in_top_10_percent')}, False


def processing():
    ''' Backfill the field
        Keyword arguments:
          None
        Returns:
          None
    '''
    query = dict(SCHOLARLY)
    if not ARG.ALL:
        query['jrc_openalex_impact'] = {"$exists": False}
    try:
        rows = list(DB['dis'].dois.find(query, {"doi": 1}).limit(ARG.LIMIT or 0))
    except Exception as err:
        terminate_program(err)
    if not rows:
        LOGGER.info("Nothing to do: every scholarly DOI already has the field")
        return
    LOGGER.info("%s DOI(s) to look up with %s worker(s)", f"{len(rows):,}", ARG.WORKERS)
    writes = []
    with ThreadPoolExecutor(max_workers=ARG.WORKERS) as pool:
        futures = [pool.submit(fetch_impact, row['doi']) for row in rows]
        for future in tqdm(as_completed(futures), total=len(futures), desc="DOIs"):
            doi, impact, errored = future.result()
            COUNT['examined'] += 1
            if errored:
                COUNT['errored'] += 1
                continue
            if not impact:
                COUNT['unclassified'] += 1
                continue
            COUNT['classified'] += 1
            writes.append(UpdateOne({"doi": doi}, {"$set": {"jrc_openalex_impact": impact}}))
    if not ARG.WRITE:
        print(f"\n  Would write {len(writes):,} record(s). Use --write to commit.")
    elif writes:
        try:
            result = DB['dis'].dois.bulk_write(writes, ordered=False)
        except Exception as err:
            terminate_program(err)
        COUNT['written'] = result.modified_count
    print(f"\n  DOIs examined:            {COUNT['examined']:,}")
    print(f"  Classified by OpenAlex:   {COUNT['classified']:,}")
    print(f"  Held but unclassified:    {COUNT['unclassified']:,}")
    print(f"  Lookup errors:            {COUNT['errored']:,}")
    print(f"  Records written:          {COUNT['written']:,}")
    if COUNT['errored']:
        print("\n  Errored DOIs keep no field, so running this again retries exactly those.")


# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Backfill jrc_openalex_impact on journal articles and preprints",
        epilog="One OpenAlex request per DOI. Idempotent: re-run to resume or to retry errors.")
    PARSER.add_argument('--all', dest='ALL', action='store_true', default=False,
                        help='Re-check DOIs that already have the field')
    PARSER.add_argument('--limit', dest='LIMIT', action='store', type=int, default=0,
                        help='Stop after this many DOIs (0 = no limit)')
    PARSER.add_argument('--workers', dest='WORKERS', action='store', type=int, default=8,
                        help='Concurrent lookups (default 8; OpenAlex asks for 10/sec)')
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
    initialize_program()
    processing()
    terminate_program()
