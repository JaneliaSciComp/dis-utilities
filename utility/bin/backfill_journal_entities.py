""" backfill_journal_entities.py
    One-time: remove HTML entities from stored journal names, so "Genes &amp;
    Development" reads "Genes & Development".

    Crossref sends some container titles with the entity still in them, and
    doi_common.get_journal used to return them unchanged. update_dois.py (dois) and
    update_ack_records.py (external_dois) store its result as jrc_journal, so 79 dois
    and 31 external_dois records hold an entity. doi_common 73.17.0 unescapes the name;
    this fixes what was already written.

    The stored value is unescaped rather than recomputed with get_journal. For dois
    the two give the same result (checked for all 79 records against prod); for
    external_dois it is the only option, because those records keep no
    container-title to recompute from.

    Only jrc_journal is changed. container-title, short-container-title and
    reference.journal-title are the registrar's record as received and are left as
    they are.

    Dry run by default. --write updates a record only if jrc_journal still holds the
    value that was read, so a re-run changes nothing and a later edit is safe. Every
    run writes a JSON report of each change.

    Deploy the dis-sync image with doi_common 73.17.0 or later first: until then,
    update_dois.py puts the entity back the next time it updates one of these DOIs.
"""

__version__ = '1.0.0'

import argparse
import collections
from datetime import datetime
import html
import json
from operator import attrgetter
import re
import sys
import jrc_common.jrc_common as JRC

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

DB = {}
COUNT = collections.defaultdict(lambda: 0, {})
COLLECTIONS = ('dois', 'external_dois')
# Named, decimal and hex character references, as html.unescape understands them
ENTITY = re.compile(r"&(?:[A-Za-z][A-Za-z0-9]*|#[0-9]+|#[xX][0-9A-Fa-f]+);")
EXAMPLES = 10


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


def unescaped(value):
    ''' The journal name with one level of HTML entities removed.
        Keyword arguments:
          value: stored jrc_journal
        Returns:
          Unescaped name, or None if there is nothing to change
    '''
    if not isinstance(value, str) or not ENTITY.search(value):
        return None
    fixed = html.unescape(value)
    return fixed if fixed != value else None


def backfill_collection(coll, name, write):
    ''' Find and (with write) fix jrc_journal values that hold an entity.
        Keyword arguments:
          coll: dois or external_dois collection
          name: collection name, for the report
          write: whether to update
        Returns:
          List of change dicts (collection, _id, doi, old, new, status)
    '''
    try:
        rows = list(coll.find({"jrc_journal": {"$regex": "&"}},
                              {"doi": 1, "jrc_journal": 1}))
    except Exception as err:
        terminate_program(err)
    changes = []
    for row in rows:
        new = unescaped(row.get('jrc_journal'))
        if new is None:
            continue
        change = {'collection': name, '_id': row['_id'], 'doi': row.get('doi'),
                  'old': row['jrc_journal'], 'new': new}
        if not write:
            change['status'] = 'would fix'
        else:
            try:
                result = coll.update_one({"_id": row['_id'], "jrc_journal": row['jrc_journal']},
                                         {"$set": {"jrc_journal": new}})
                change['status'] = 'fixed' if result.modified_count else 'stale'
            except Exception as err:
                LOGGER.error(f"Could not update {row.get('doi')}: {err}")
                change['status'] = 'error'
        COUNT[f"{name} {change['status']}"] += 1
        changes.append(change)
    return changes


def processing():
    ''' Main routine
        Keyword arguments:
          None
        Returns:
          None
    '''
    changes = []
    for name in COLLECTIONS:
        changes += backfill_collection(DB['dis'][name], name, ARG.WRITE)
    if not changes:
        print("No journal names hold an HTML entity")
        return
    journals = collections.Counter((chg['old'], chg['new']) for chg in changes)
    print(f"\nRecords: {len(changes):,}   distinct journals: {len(journals):,}")
    for key in sorted(COUNT):
        print(f"  {key + ':':<26} {COUNT[key]:,}")
    print("Most common:")
    for (old, new), num in journals.most_common(EXAMPLES):
        print(f"  {num:>3}  {old!r}\n       -> {new!r}")
    mode = 'written' if ARG.WRITE else 'dryrun'
    fname = f"journal_entities_{mode}_{datetime.now().strftime('%Y%m%dT%H%M%S')}.json"
    with open(fname, 'w', encoding='utf-8') as outstream:
        outstream.write(json.dumps(changes, default=str, indent=2, ensure_ascii=False))
    print(f"\nWrote report to {fname}")
    if not ARG.WRITE:
        LOGGER.warning("Dry run: nothing was written (use --write to apply)")


# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Remove HTML entities from jrc_journal in dois and external_dois")
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
