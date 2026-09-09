""" add_preprint.py
    Associate two DOIs with a preprint relationship.

    The pair is checked for a shared author before the relation is written. This
    is the only unguarded way into jrc_preprint - update_preprints.py requires an
    author confirmation and a title match before it relates anything - and three
    relations between entirely unrelated papers reached the database this way,
    each of which then had to be found and unpicked. A mistyped DOI is the likely
    cause, and it is silent otherwise.
"""

__version__ = '1.2.0'

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
    ''' Intialize the program
        Keyword arguments:
          None
        Returns:
          None
    '''
    # Database
    try:
        dbconfig = JRC.get_config("databases")
    except Exception as err:
        terminate_program(err)
    dbs = ['dis']
    for source in dbs:
        dbo = attrgetter(f"{source}.{ARG.MANIFOLD}.write")(dbconfig)
        LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, ARG.MANIFOLD, dbo.host, dbo.user)
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)


def associate_dois(journal, preprint):
    """ Associate two DOIs
        Keyword arguments:
          journal: primary DOI record
          preprint: preprint DOI record
        Returns:
          payloadj: journal payload
          payloadp: preprint payload
    """
    if 'jrc_preprint' in journal:
        if ARG.PREPRINT in journal['jrc_preprint']:
            LOGGER.warning(f"Preprint {ARG.PREPRINT} already associated with {ARG.JOURNAL}")
        else:
            journal['jrc_preprint'].append(ARG.PREPRINT)
    else:
        journal['jrc_preprint'] = [ARG.PREPRINT]
    payloadj = {"jrc_preprint": journal['jrc_preprint']}
    if 'jrc_preprint' in preprint:
        if ARG.JOURNAL in preprint['jrc_preprint']:
            LOGGER.warning(f"Primary DOI {ARG.JOURNAL} already associated with {ARG.PREPRINT}")
        else:
            preprint['jrc_preprint'].append(ARG.JOURNAL)
    else:
        preprint['jrc_preprint'] = [ARG.JOURNAL]
    payloadp = {"jrc_preprint": preprint['jrc_preprint']}
    return payloadj, payloadp


def author_names(rec):
    ''' Author names on a record, lowercased.
        Given and family are both collected because some records store them
        swapped - 10.1101/400358 has given="Campagner", family="Dario" - and
        reading one field would make a legitimate pair look unrelated.
        Keyword arguments:
          rec: DOI record
        Returns:
          set of lowercased name tokens
    '''
    names = set()
    for auth in rec.get('author') or rec.get('creators') or []:
        for key in ('family', 'familyName', 'given', 'givenName'):
            if auth.get(key):
                names.add(str(auth[key]).lower().strip())
        if auth.get('name'):
            names.update(str(auth['name']).lower().split())
    return names


def check_same_work(journal, preprint):
    ''' Refuse a pair that shares no author, unless --force is given.
        A preprint and its published version differ by an author or two, never by
        all of them, so sharing nothing means the DOIs are not two records of one
        work. Checked on names rather than titles because a title can change
        substantially during review - "Spatial organization of the 3D genome
        encodes gene co-expression" became "Cohesin prevents cross-domain gene
        coactivation" - while the author list does not empty out.
        Keyword arguments:
          journal: primary DOI record
          preprint: preprint DOI record
        Returns:
          None
    '''
    shared = author_names(journal) & author_names(preprint)
    if shared:
        LOGGER.debug(f"Shared authors: {', '.join(sorted(shared))}")
        return
    if not author_names(journal) or not author_names(preprint):
        LOGGER.warning("One record has no author list; cannot check that these are "
                       "the same work")
        return
    msg = f"{journal['doi']} and {preprint['doi']} share no author, so they do not " \
          + "look like two records of one work"
    if not ARG.FORCE:
        terminate_program(f"{msg}. Use --force to relate them anyway.")
    LOGGER.warning(f"{msg} - relating them anyway (--force)")


def add_jrc_preprint():
    """ Update jrc_preprint for specified DOIs
        Keyword arguments:
          None
        Returns:
          None
    """
    coll = DB['dis'].dois
    # Get records
    try:
        journal = DL.get_doi_record(ARG.JOURNAL, coll)
    except Exception as err:
        terminate_program(err)
    if not journal:
        terminate_program(f"Primary DOI {ARG.JOURNAL} not found")
    try:
        if DL.is_preprint(journal):
            terminate_program(f"Primary DOI {ARG.JOURNAL} is a preprint")
    except Exception as err:
        LOGGER.error(f"Could not check preprint status for journal {ARG.JOURNAL}")
        terminate_program(err)
    try:
        preprint = DL.get_doi_record(ARG.PREPRINT, coll)
        if not preprint:
            LOGGER.error(f"Preprint {ARG.PREPRINT} not found")
            print("You may insert this DOI with the following command:")
            print(f"python3 weekly_pubs.py --sync_only --doi {ARG.PREPRINT} --write")
            sys.exit(-1)
    except Exception as err:
        terminate_program(err)
    try:
        if not DL.is_preprint(preprint):
            terminate_program(f"Preprint {ARG.PREPRINT} is not a preprint")
    except Exception as err:
        LOGGER.error(f"Could not check preprint status for preprint {ARG.PREPRINT}")
        terminate_program(err)
    check_same_work(journal, preprint)
    # Associate DOIs
    payloadj, payloadp = associate_dois(journal, preprint)
    if ARG.WRITE:
        result = coll.update_one({"doi": journal['doi']}, {"$set": payloadj})
        if hasattr(result, 'matched_count') and result.matched_count:
            COUNT['updated'] += 1
        result = coll.update_one({"doi": preprint['doi']}, {"$set": payloadp})
        if hasattr(result, 'matched_count') and result.matched_count:
            COUNT['updated'] += 1
    else:
        print(f"Primary DOI {ARG.JOURNAL} updated with preprint:\n  {json.dumps(payloadj)}")
        print(f"Preprint {ARG.PREPRINT} updated with primary DOI:\n  {json.dumps(payloadp)}")
        COUNT['updated'] = 2
    print(f"Records updated: {COUNT['updated']}")
    if not ARG.WRITE:
        LOGGER.warning("Dry run successful, no updates were made")

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Associate two DOIs with a preprint relationship")
    PARSER.add_argument('--journal', dest='JOURNAL', action='store',
                        type=str.lower, required=True, help='Primary (non-preprint) DOI')
    PARSER.add_argument('--preprint', dest='PREPRINT', action='store',
                        type=str.lower, required=True, help='Preprint DOI')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, prod)')
    PARSER.add_argument('--force', dest='FORCE', action='store_true',
                        default=False,
                        help='Relate the DOIs even if they share no author')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Write to database/config system')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    add_jrc_preprint()
    terminate_program()
