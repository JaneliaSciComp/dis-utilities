""" remove_preprint.py
    Remove a preprint relationship between two DOIs.

    The counterpart to add_preprint.py, for a relation that should not be there:
    a preprint matched to the wrong article, or a dataset recorded as a preprint.
    jrc_preprint is written to both ends, so both ends are cleaned - removing only
    the side you noticed leaves the other half of the relation behind, and the
    integrity report will keep reporting it.

    Dry run by default; --write applies the change and logs a processing event
    against each DOI. Removing a relation that is not there is reported and does
    nothing, so a repeated run is harmless.
"""

__version__ = '1.0.0'

import argparse
import collections
import json
from operator import attrgetter
import sys
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Database
DB = {}
# Counters
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
    dbo = attrgetter(f"dis.{ARG.MANIFOLD}.write")(dbconfig)
    LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, ARG.MANIFOLD, dbo.host, dbo.user)
    try:
        DB['dis'] = JRC.connect_database(dbo)
    except Exception as err:
        terminate_program(err)


def strip_relation(rec, other):
    ''' The record's jrc_preprint with one DOI removed.
        Comparison is case-insensitive: DOI casing is not consistent in the
        collection, and a case difference would silently leave the relation in
        place while reporting success.
        Keyword arguments:
          rec: DOI record
          other: DOI to remove from it
        Returns:
          (new list, True if anything was removed)
    '''
    current = rec.get('jrc_preprint') or []
    kept = [d for d in current if str(d).lower() != other.lower()]
    return kept, len(kept) != len(current)


def apply_change(rec, other):
    ''' Remove one end of the relation, or report that it is not there
        Keyword arguments:
          rec: DOI record to modify
          other: the DOI to remove from it
        Returns:
          None
    '''
    doi = rec['doi']
    kept, changed = strip_relation(rec, other)
    if not changed:
        LOGGER.warning(f"{doi} does not record {other}")
        COUNT['not_present'] += 1
        return
    # An empty list is removed rather than stored: "no relations" and "an empty
    # list of relations" should not be two different states in the collection,
    # and every reader tests for the field's presence.
    if kept:
        update = {"$set": {"jrc_preprint": kept}}
        after = json.dumps(kept)
    else:
        update = {"$unset": {"jrc_preprint": ""}}
        after = "(field removed)"
    print(f"{doi}: {json.dumps(rec.get('jrc_preprint') or [])} -> {after}")
    if not ARG.WRITE:
        COUNT['would_change'] += 1
        return
    try:
        result = DB['dis'].dois.update_one({"doi": doi}, update)
    except Exception as err:
        terminate_program(err)
    if result.matched_count:
        COUNT['updated'] += 1
        try:
            DL.add_doi_process(doi, action="remove_preprint", coll=DB['dis'].processing,
                               notes=f"Removed preprint relation to {other}"
                                     + (f": {ARG.REASON}" if ARG.REASON else ""))
            COUNT['logged'] += 1
        except Exception as err:
            LOGGER.error(f"Could not log a processing event for {doi}: {err}")
            COUNT['log_failed'] += 1


def remove_relation():
    ''' Remove the relation from both DOIs
        Keyword arguments:
          None
        Returns:
          None
    '''
    coll = DB['dis'].dois
    recs = {}
    for doi in (ARG.DOI, ARG.RELATED):
        try:
            rec = DL.get_doi_record(doi, coll)
        except Exception as err:
            terminate_program(err)
        if not rec:
            terminate_program(f"{doi} is not in the dois collection")
        recs[doi] = rec
    if not any(strip_relation(recs[a], b)[1]
               for a, b in ((ARG.DOI, ARG.RELATED), (ARG.RELATED, ARG.DOI))):
        terminate_program(f"Neither {ARG.DOI} nor {ARG.RELATED} records the other; "
                          "there is no relation to remove")
    apply_change(recs[ARG.DOI], ARG.RELATED)
    apply_change(recs[ARG.RELATED], ARG.DOI)
    for key in sorted(COUNT):
        print(f"{key + ':':<20} {COUNT[key]:,}")
    if not ARG.WRITE:
        LOGGER.warning("Dry run: nothing was written (use --write to apply)")


# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Remove a preprint relationship between two DOIs")
    PARSER.add_argument('--doi', dest='DOI', action='store', type=str.lower,
                        required=True, help='One DOI of the pair')
    PARSER.add_argument('--related', dest='RELATED', action='store', type=str.lower,
                        required=True, help='The DOI to unlink from it')
    PARSER.add_argument('--reason', dest='REASON', action='store', default='',
                        help='Why the relation is being removed (recorded in the '
                             'processing event)')
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
    remove_relation()
    terminate_program()
