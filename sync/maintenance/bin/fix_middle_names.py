''' fix_middle_names.py
    Expand the given-name lists in the orcid collection so every name carrying
    a middle initial is held in both spellings - "Gerald M. Rubin" and "Gerald
    M Rubin" - and every list has a bare first name.

    DOI author matching compares publisher metadata against each entry in the
    list, and publishers are inconsistent about the period, so a name held in
    only one form silently fails to match the publishers using the other.

    This derives variants mechanically from names already present. It does not
    consult ORCID; add_orcid_name_variants.py does that. Run that one first to
    bring in the variants ORCID knows about, then this one to permute them.

    The earlier version of this program had two modes (--period and its
    inverse), each adding at most one variant per record and skipping any
    record that already held a single period. Both modes are now one pass:
    every given name is expanded, and anything missing is added.
'''

__version__ = '2.0.0'

import argparse
import collections
from operator import attrgetter
import sys
import pymongo
from tqdm import tqdm
import jrc_common.jrc_common as JRC
from dis_name_lib import (exact_key, first_name, initial_variants,
                          is_initials_only, split_against_family)
from dis_review_lib import apply_decisions, review_candidates, summarize

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Database
DB = {}
# Counters
COUNT = collections.defaultdict(lambda: 0, {})
# Global variables
ARG = LOGGER = None
# Proposals gathered for --review
CANDIDATES = []
# Record ids holding exact duplicate given names, collapsed by --dedupe
DEDUPE = []


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


def embeds_family(name, families):
    ''' Test whether a given-name variant ends with the person's family name,
        as "Alejandro A Aguilera" does. Some given lists already hold entries
        like this; expanding them would double bad data rather than widen
        matching, so they are reported instead.
        Keyword arguments:
          name: proposed given name
          families: family-name variants held in the record
        Returns:
          True if a held family name sits at the end of the given name
    '''
    given, _ = split_against_family(name, families)
    return bool(given)


def proposed_names(givens, families):
    ''' Work out which spellings a record is missing.

        Every entry is expanded, not just the first match, and the record is
        never skipped for already holding a period somewhere: the old
        per-record guard left nine records with a name whose period twin was
        still absent, generally the people with the most variants.
        Keyword arguments:
          givens: given-name variants held in the record
          families: family-name variants held in the record
        Returns:
          Sorted list of names to add
    '''
    held = {exact_key(g) for g in givens}
    missing = {}
    for given in givens:
        for form in initial_variants(given):
            # "T D" would match any author with those initials. The record may
            # legitimately hold such a form already; this only declines to add
            # a new one.
            if is_initials_only(form):
                continue
            if embeds_family(form, families):
                COUNT['embeds_family'] += 1
                continue
            if exact_key(form) not in held and exact_key(form) not in missing:
                missing[exact_key(form)] = (form, f"period variant of {given!r}")
    bare = first_name(givens)
    if bare and exact_key(bare) not in held and exact_key(bare) not in missing:
        missing[exact_key(bare)] = (bare, 'bare first name, middle initials dropped')
        COUNT['first_name'] += 1
    return sorted(missing.values())


def duplicates(givens):
    ''' Exact duplicate entries in a given list, an artifact of the old
        whole-array $set writes.
        Keyword arguments:
          givens: given-name variants held in the record
        Returns:
          List of duplicated names
    '''
    seen, dupes = set(), []
    for given in givens:
        if given in seen:
            dupes.append(given)
        seen.add(given)
    return dupes


def process_record(rec):
    ''' Expand one record's given list
        Keyword arguments:
          rec: orcid collection record
        Returns:
          None
    '''
    COUNT['read'] += 1
    givens = [g for g in (rec.get('given') or []) if g and g.strip()]
    if not givens:
        COUNT['no_given'] += 1
        return
    dupes = duplicates(rec.get('given') or [])
    if dupes:
        COUNT['with_duplicates'] += 1
        LOGGER.debug(f"{rec.get('orcid', rec['_id'])}: duplicate entries {dupes}")
        # Collected rather than written here: --review defers every write to
        # the end of the walk, and the inline version was unreachable there,
        # so --review --dedupe --write silently did nothing.
        DEDUPE.append(rec['_id'])
    additions = proposed_names(givens, rec.get('family') or [])
    if not additions:
        COUNT['complete'] += 1
        return
    name = f"{givens[0]} {(rec.get('family') or ['?'])[0]}"
    if additions:
        COUNT['needing_update'] += 1
        if ARG.REVIEW:
            for value, reason in additions:
                CANDIDATES.append({'name': name, 'orcid': rec.get('orcid'),
                                   'key_field': '_id', 'key_value': rec['_id'],
                                   'userId': rec.get('userIdO365', ''),
                                   'field': 'given', 'value': value, 'note': reason,
                                   'cur_given': ', '.join(givens),
                                   'cur_family': ', '.join(rec.get('family') or [])})
        else:
            print(f"  {name:<34} += {', '.join(repr(a) for a, _ in additions)}")
    if ARG.REVIEW or not ARG.WRITE:
        return
    # $addToSet, never a whole-array $set: the list is also written by
    # apply_orcids.py and add_orcid_name_variants.py, and rebuilding it from
    # what this process read would silently drop anything they added since.
    try:
        DB['dis'].orcid.update_one(
            {'_id': rec['_id']},
            {'$addToSet': {'given': {'$each': [a for a, _ in additions]}}})
        COUNT['written'] += len(additions)
    except pymongo.errors.PyMongoError as err:
        terminate_program(err)


def apply_dedupe():
    ''' Collapse exact duplicate entries in the given lists collected during
        the scan. This is the one write that needs the whole array, so each
        record is read back immediately beforehand rather than reusing the
        scan's copy - another process may have added a name since.
        Keyword arguments:
          None
        Returns:
          None
    '''
    for oid in DEDUPE:
        try:
            fresh = DB['dis'].orcid.find_one({'_id': oid}, {'given': 1})
            deduped = list(dict.fromkeys(fresh.get('given') or []))
            if deduped != (fresh.get('given') or []):
                DB['dis'].orcid.update_one({'_id': oid}, {'$set': {'given': deduped}})
                COUNT['deduped'] += 1
        except pymongo.errors.PyMongoError as err:
            terminate_program(err)


def process_orcid():
    ''' Expand given-name lists across the collection
        Keyword arguments:
          None
        Returns:
          None
    '''
    try:
        recs = list(DB['dis'].orcid.find({'given': {'$exists': True}}))
    except Exception as err:
        terminate_program(err)
    LOGGER.info(f"Found {len(recs):,} records with a given list")
    for rec in tqdm(recs, desc='Records', disable=not ARG.VERBOSE):
        process_record(rec)
    print(f"\nRecords read:              {COUNT['read']:,}")
    print(f"Records with no names:     {COUNT['no_given']:,}")
    print(f"Records already complete:  {COUNT['complete']:,}")
    print(f"Records needing names:     {COUNT['needing_update']:,}")
    print(f"First names to add:        {COUNT['first_name']:,}")
    print(f"Records with duplicates:   {COUNT['with_duplicates']:,}")
    if COUNT['embeds_family']:
        print(f"Skipped (family in given): {COUNT['embeds_family']:,}")
    if ARG.REVIEW:
        interactive_session()
        return
    if ARG.WRITE:
        if ARG.DEDUPE:
            apply_dedupe()
        print(f"Names written:             {COUNT['written']:,}")
        if ARG.DEDUPE:
            print(f"Records deduplicated:      {COUNT['deduped']:,}")
    else:
        LOGGER.warning("Dry run, no updates were made")


def interactive_session():
    ''' Walk the proposals and store what was accepted
        Keyword arguments:
          None
        Returns:
          None
    '''
    if not sys.stdin.isatty():
        terminate_program("--review needs a terminal; run it without a pipe or redirect")
    # Dedupes are counted before the early return: a run can have nothing to
    # review and still have duplicate entries to collapse, and returning here
    # on an empty CANDIDATES alone silently dropped them.
    pending = len(DEDUPE) if ARG.DEDUPE else 0
    if not CANDIDATES and not pending:
        print("\nNothing to review.")
        return
    accepted = []
    if CANDIDATES:
        CANDIDATES.sort(key=lambda c: c['name'].lower())
        accepted = review_candidates(CANDIDATES, no_color=ARG.NOCOLOR)
        summarize(accepted, no_color=ARG.NOCOLOR)
    else:
        print("\nNo name variants to review.")
    if pending:
        print(f"Plus {pending:,} record(s) with duplicate entries to collapse.")
    if not accepted and not pending:
        return
    if not ARG.WRITE:
        print("\nDry run - rerun with --write to store these.")
        return
    try:
        confirm = input(f"\nApply {len(accepted):,} change(s)"
                        f"{f' and {pending:,} dedupe(s)' if pending else ''}"
                        " to the orcid collection? [y/N] ")
    except (EOFError, KeyboardInterrupt):
        print("\nCancelled.")
        return
    if confirm.strip().lower() not in ('y', 'yes'):
        print("Cancelled - nothing written.")
        return
    try:
        COUNT['written'] = apply_decisions(DB['dis'].orcid, accepted, LOGGER)
    except Exception as err:
        terminate_program(err)
    if ARG.DEDUPE:
        apply_dedupe()
        print(f"Records deduplicated:      {COUNT['deduped']:,}")
    print(f"Names written:             {COUNT['written']:,}")


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Expand given-name variants in the orcid collection")
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, prod)')
    PARSER.add_argument('--review', dest='REVIEW', action='store_true',
                        default=False,
                        help='Step through each proposal interactively')
    PARSER.add_argument('--no-color', dest='NOCOLOR', action='store_true',
                        default=False, help='Flag, plain output')
    PARSER.add_argument('--dedupe', dest='DEDUPE', action='store_true',
                        default=False,
                        help='Also collapse exact duplicate entries')
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
    process_orcid()
    terminate_program()
