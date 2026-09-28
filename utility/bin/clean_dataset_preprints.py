""" clean_dataset_preprints.py
    Remove jrc_preprint relations held on data-repository DOIs.

    A dataset is not a preprint of the article it accompanies, but 63 figshare,
    Zenodo, Dryad and Mendeley DOIs carry the article in jrc_preprint. They are
    residue: update_preprints.py's selection has since been tightened, and 61 of
    the 63 would not be picked up by it today, so this is a one-off clean-up
    rather than a recurring job.

    Two populations, and they are not equally safe to remove:

      duplicate  the same relation is already recorded in jrc_dataset_supplement,
                 where it belongs. Removing the jrc_preprint copy loses nothing.
      orphan     the relation exists ONLY in jrc_preprint. It is in the wrong
                 field, but it is the only record of a true relation, and neither
                 registrar declares it as IsSupplementTo - so link_dataset_
                 supplements.py will not recreate it.

    --scope defaults to the duplicates for that reason. --migrate moves an orphan
    into jrc_dataset_supplement instead of discarding it, recording it with a
    source of "Curated" so link_dataset_supplements.py preserves it: that program
    rebuilds the field with a whole-value $set and, from 1.4.0, folds back any
    entry whose source it does not produce. Against an earlier version a migrated
    entry would be silently deleted on the next nightly run, so --migrate checks
    the installed version before it writes anything.

    Every relation this removes is written to a JSON file first, whatever the
    scope and whether or not --write is given, so nothing is irrecoverable.
"""

__version__ = '1.2.0'

import argparse
import collections
import json
import os
from operator import attrgetter
import re
import sys
from datetime import datetime
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Data-repository prefixes, in step with link_dataset_supplements.py
DATA_PREFIXES = ('10.25378', '10.6084', '10.5281', '10.5061', '10.17632',
                 '10.7910', '10.48324')
# link_dataset_supplements.py preserves an entry whose source it does not derive.
# Anything it does derive is owned by it and would be rebuilt over.
CURATED_SOURCE = 'Curated'
# The release of link_dataset_supplements.py that preserves curated entries.
PRESERVES_CURATED = (1, 4, 0)
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


def classify():
    ''' Every data-repository DOI carrying jrc_preprint, split by what removing
        it would cost.
        Keyword arguments:
          None
        Returns:
          list of dicts describing each relation
    '''
    try:
        rows = [r for r in DB['dis'].dois.find({"jrc_preprint": {"$exists": True}})
                if r['doi'].lower().startswith(DATA_PREFIXES)]
    except Exception as err:
        terminate_program(err)
    LOGGER.info(f"Found {len(rows):,} data-repository DOIs carrying jrc_preprint")
    found = []
    for row in rows:
        doi = row['doi'].lower()
        supp = {str(e.get('doi') or '').lower()
                for e in (row.get('jrc_dataset_supplement') or [])}
        # A data-repository prefix does not mean the deposit is data. Zenodo hosts
        # preprints too, and where the registrar types one as a preprint its
        # jrc_preprint relation is correct and must be left alone - the prefix rule
        # is the weaker signal and loses. Skipped rather than warned about: an
        # earlier version only warned, and two genuine preprints were migrated into
        # jrc_dataset_supplement before anyone read the warning.
        if (row.get('subtype') == 'preprint'
                or (row.get('types') or {}).get('resourceTypeGeneral') == 'Preprint'):
            LOGGER.info(f"{doi}: the registrar types this as a preprint; leaving it alone")
            COUNT['skipped_registrar_preprint'] += len(row['jrc_preprint'])
            continue
        for target in [str(t).lower() for t in row['jrc_preprint']]:
            found.append({"doi": doi, "target": target,
                          "scope": "duplicate" if target in supp else "orphan",
                          "resource_type": (row.get('types') or {}).get('resourceTypeGeneral'),
                          "title": DL.get_title(row)})
            COUNT[f"found_{found[-1]['scope']}"] += 1
    return found


def check_linker_version():
    ''' Refuse to migrate against a link_dataset_supplements.py that would undo it
        The field is rebuilt with a whole-value $set, so before 1.4.0 an entry that
        program cannot derive is deleted on its next run. Migrating into that would
        look like it worked and quietly lose the data overnight.
        Keyword arguments:
          None
        Returns:
          None
    '''
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        '..', '..', 'sync', 'bin', 'link_dataset_supplements.py')
    try:
        with open(path, encoding='ascii') as handle:
            text = handle.read()
    except Exception:
        LOGGER.warning(f"Could not read {path} to check its version; assuming it "
                       "preserves curated entries")
        return
    match = re.search(r"^__version__ = '([\d.]+)'", text, re.M)
    if not match:
        LOGGER.warning("Could not determine link_dataset_supplements.py's version")
        return
    found = tuple(int(x) for x in match.group(1).split('.'))
    if found < PRESERVES_CURATED:
        terminate_program(
            f"link_dataset_supplements.py is {match.group(1)}; --migrate needs "
            + ".".join(str(x) for x in PRESERVES_CURATED) + " or later, which "
            "preserves entries it does not derive. Migrating now would be undone "
            "by its next run.")
    LOGGER.info(f"link_dataset_supplements.py {match.group(1)} preserves curated entries")


def migrate(item):
    ''' Record an orphaned relation as a dataset supplement, at both ends
        The data-repository DOI supplements the article; the article is
        supplemented by it. An entry already naming the same target is left alone,
        whatever its source, so a re-run adds nothing and a registrar-derived entry
        is never overwritten by a curated one.
        Keyword arguments:
          item: one relation from classify()
        Returns:
          None
    '''
    for doi, target, relation in ((item['doi'], item['target'], 'supplements'),
                                  (item['target'], item['doi'], 'supplemented_by')):
        try:
            rec = DL.get_doi_record(doi, DB['dis'].dois)
        except Exception as err:
            terminate_program(err)
        if not rec:
            LOGGER.warning(f"{doi} is not in the collection; not migrating its end")
            COUNT['migrate_target_missing'] += 1
            continue
        stored = rec.get('jrc_dataset_supplement') or []
        if any(str(e.get('doi') or '').lower() == target for e in stored):
            COUNT['migrate_already_present'] += 1
            continue
        entry = {"doi": target, "relation": relation, "source": CURATED_SOURCE,
                 "note": "Migrated from jrc_preprint"}
        # Sorted by target DOI, matching what link_dataset_supplements.py stores,
        # so its next run sees no difference and does not rewrite the record.
        merged = sorted(stored + [entry], key=lambda e: e.get('doi') or '')
        print(f"{'migrate':<10} {doi:<40} --{relation}--> {target}")
        if not ARG.WRITE:
            COUNT['would_migrate'] += 1
            continue
        try:
            DB['dis'].dois.update_one(
                {"doi": rec['doi']},
                {"$set": {"jrc_dataset_supplement": merged,
                          "jrc_dataset_supplement_updated": datetime.now()}})
            COUNT['migrated'] += 1
        except Exception as err:
            terminate_program(err)
        try:
            DL.add_doi_process(rec['doi'], action='link_dataset_supplement',
                               coll=DB['dis'].processing,
                               notes=f"{relation.replace('_', ' ')}: {target} "
                                     "(migrated from jrc_preprint)")
            COUNT['logged'] += 1
        except Exception as err:
            LOGGER.error(f"Could not log a processing event for {rec['doi']}: {err}")
            COUNT['log_failed'] += 1


def strip(doi, other):
    ''' Remove one DOI from another's jrc_preprint
        Keyword arguments:
          doi: record to modify
          other: DOI to remove from it
        Returns:
          True if the record changed
    '''
    try:
        rec = DL.get_doi_record(doi, DB['dis'].dois)
    except Exception as err:
        terminate_program(err)
    if not rec:
        LOGGER.warning(f"{doi} is not in the collection")
        COUNT['not_found'] += 1
        return False
    current = rec.get('jrc_preprint') or []
    kept = [d for d in current if str(d).lower() != other.lower()]
    if len(kept) == len(current):
        COUNT['already_absent'] += 1
        return False
    # An empty list is removed rather than stored, so "no relations" is one state
    update = {"$set": {"jrc_preprint": kept}} if kept else {"$unset": {"jrc_preprint": ""}}
    if not ARG.WRITE:
        COUNT['would_update'] += 1
        return True
    try:
        result = DB['dis'].dois.update_one({"doi": rec['doi']}, update)
    except Exception as err:
        terminate_program(err)
    if result.matched_count:
        COUNT['updated'] += 1
        try:
            DL.add_doi_process(rec['doi'], action="remove_preprint",
                               coll=DB['dis'].processing,
                               notes=f"Removed preprint relation to {other}: a data "
                                     "repository DOI is not a preprint")
            COUNT['logged'] += 1
        except Exception as err:
            LOGGER.error(f"Could not log a processing event for {rec['doi']}: {err}")
            COUNT['log_failed'] += 1
    return True


def processing():
    ''' Main routine
        Keyword arguments:
          None
        Returns:
          None
    '''
    if ARG.MIGRATE:
        check_linker_version()
    found = classify()
    selected = [f for f in found if ARG.SCOPE == 'all' or f['scope'] == 'duplicate']
    # Written before anything is changed, and on a dry run too: the orphans are
    # the only record of a relation nothing else will recreate.
    try:
        with open(ARG.OUTPUT, 'w', encoding='ascii') as handle:
            json.dump(found, handle, indent=2)
    except Exception as err:
        terminate_program(err)
    LOGGER.info(f"Wrote {len(found):,} relations to {ARG.OUTPUT}")
    for item in selected:
        # Migration first: if it fails the relation is still in jrc_preprint, which
        # is wrong but present. Removing first and failing to migrate would lose it.
        if ARG.MIGRATE and item['scope'] == 'orphan':
            migrate(item)
        print(f"{item['scope']:<10} {item['doi']:<40} -x-> {item['target']}")
        strip(item['doi'], item['target'])      # the data-repository DOI's copy
        strip(item['target'], item['doi'])      # the article's copy
    print()
    for key in sorted(COUNT):
        print(f"{key + ':':<24} {COUNT[key]:,}")
    print(f"{'selected by --scope:':<24} {len(selected):,} of {len(found):,}")
    if COUNT['skipped_registrar_preprint']:
        LOGGER.info(f"{COUNT['skipped_registrar_preprint']} relation(s) left alone on "
                    "records the registrar types as a preprint")
    if ARG.SCOPE != 'all' and COUNT['found_orphan']:
        LOGGER.warning(f"{COUNT['found_orphan']} orphaned relation(s) left in place. "
                       "They exist only in jrc_preprint and nothing will recreate "
                       f"them; see {ARG.OUTPUT} before running with --scope all")
    if not ARG.WRITE:
        LOGGER.warning("Dry run: nothing was written (use --write to apply)")


# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Remove jrc_preprint relations held on data-repository DOIs")
    PARSER.add_argument('--scope', dest='SCOPE', action='store', default='duplicates',
                        choices=['duplicates', 'all'],
                        help='duplicates: only relations already recorded in '
                             'jrc_dataset_supplement (default). all: those plus the '
                             'orphans, which nothing will recreate')
    PARSER.add_argument('--migrate', dest='MIGRATE', action='store_true', default=False,
                        help='Record an orphaned relation as a dataset supplement '
                             'before removing it from jrc_preprint, rather than '
                             'discarding it (duplicates are never migrated - the '
                             'relation is already there)')
    PARSER.add_argument('--output', dest='OUTPUT', action='store',
                        default='dataset_preprints_removed.json',
                        help='JSON file of every relation found, written every run')
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
