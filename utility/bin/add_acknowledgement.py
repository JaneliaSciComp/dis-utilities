""" add_acknowledgement.py
    Add or update the acknowledgement text (the jrc_acknowledgements field) for a
    single DOI in the DIS database.

    The DOI is looked up in the chosen MongoDB manifold (prod by default), in the
    dois collection first and then external_dois. If the record exists in either,
    its jrc_acknowledgements value is edited (or entered) and updated in place. If
    the DOI is in neither collection and --external is supplied, a new external_dois
    record is created from the DOI's Crossref metadata - populating jrc_ack_source,
    jrc_publishing_date, and the entered jrc_acknowledgements (the same minimal
    record pull_external_acks.py would create).

    Interactively-entered multi-line text is collapsed to a single space-separated
    string before being stored. Nothing is written unless --write is supplied;
    without it the script reports the change it would make (a dry run).

    An --external create is guarded: if the Crossref metadata lists a Janelia author
    affiliation the DOI probably belongs in the dois collection (add it via normal
    ingestion), so the add is refused unless --force is given. external_dois holds
    non-Janelia papers that acknowledge Janelia, so a manual add there is a
    deliberate, vetted operation.

    Examples:
      # Dry run against prod - update an existing dois/external_dois record
      add_acknowledgement.py --doi 10.1234/example

      # Create a new external_dois record for a manually-found DOI
      add_acknowledgement.py --doi 10.1234/example --external --write

      # Force an external add past the Janelia-affiliation guard
      add_acknowledgement.py --doi 10.1234/example --external --force --write
"""

__version__ = '1.2.0'

import argparse
from operator import attrgetter
import sys
import readline
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Database
DB = {}
# Globals
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
    dbs = ['dis']
    for source in dbs:
        dbo = attrgetter(f"{source}.{ARG.MANIFOLD}.write")(dbconfig)
        LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, ARG.MANIFOLD, dbo.host, dbo.user)
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)


def get_acknowledgement_text():
    ''' Prompt user to enter multi-line acknowledgement text
        Keyword arguments:
          None
        Returns:
          Single-line string with newlines replaced by spaces
    '''
    print("Enter acknowledgement text (press Ctrl+D when done):")
    lines = []
    try:
        while True:
            line = input()
            # Blank lines act as paragraph breaks and are dropped from the
            # collapsed result; they no longer terminate input, so a pasted
            # paragraph break can't silently truncate the text.
            if line != "":
                lines.append(line)
    except EOFError:
        pass
    except KeyboardInterrupt:
        terminate_program("User cancelled program")
    return " ".join(lines).strip()


def edit_acknowledgement_text(existing):
    ''' Display existing acknowledgement text and allow the user to edit it
        Keyword arguments:
          existing: current jrc_acknowledgements value
        Returns:
          Edited single-line string, or the original if unchanged
    '''
    print(f"Current jrc_acknowledgements:\n  {existing}\n")
    def _prefill():
        readline.insert_text(existing)
        readline.redisplay()
    readline.set_pre_input_hook(_prefill)
    try:
        result = input("Edit acknowledgement text: ")
    except (EOFError, KeyboardInterrupt):
        terminate_program("User cancelled program")
    finally:
        readline.set_pre_input_hook()
    return result.strip()


def find_record():
    ''' Locate the DOI in the dois or external_dois collection.
        Keyword arguments:
          None
        Returns:
          (record, collection_name) tuple, or (None, None) if not found
    '''
    for name in ('dois', 'external_dois'):
        try:
            rec = DL.get_doi_record(ARG.DOI, DB['dis'][name])
        except Exception as err:
            terminate_program(err)
        if rec:
            return rec, name
    return None, None


def update_acknowledgement(rec, collection):
    ''' Update jrc_acknowledgements on an existing record, in place.
        Keyword arguments:
          rec: the existing DOI record
          collection: name of the collection holding it ('dois'/'external_dois')
        Returns:
          None
    '''
    title = DL.get_title(rec)
    if title:
        print(f"Title: {title}")
    print(f"Record found in the '{collection}' collection")
    existing = rec.get('jrc_acknowledgements')
    if existing:
        ack_text = edit_acknowledgement_text(existing)
        if ack_text == existing:
            print("No changes made")
            return
    else:
        ack_text = get_acknowledgement_text()
    if not ack_text:
        terminate_program("No acknowledgement text was entered")
    LOGGER.debug(f"Acknowledgement text: {ack_text}")
    if ARG.WRITE:
        try:
            result = DB['dis'][collection].update_one(
                {"doi": ARG.DOI}, {"$set": {"jrc_acknowledgements": ack_text}})
            if result.matched_count:
                print(f"Updated {ARG.DOI} in {collection} with jrc_acknowledgements")
            else:
                LOGGER.warning(f"No record was updated for {ARG.DOI}")
        except Exception as err:
            terminate_program(err)
    else:
        print(f"Would update {ARG.DOI} in {collection} with:\n  jrc_acknowledgements: {ack_text}")
        LOGGER.warning("Dry run successful, no updates were made")


def _crossref_janelia_affiliation(rec):
    ''' Light Janelia-author guard: True if any Crossref author lists a Janelia
        affiliation. Not as thorough as pull_external_acks' ORCID-collection guard,
        but enough to flag a DOI that likely belongs in the dois collection.
        Keyword arguments:
          rec: the Crossref message
        Returns:
          True if a Janelia author affiliation is present
    '''
    for author in rec.get('author', []) or []:
        for aff in author.get('affiliation', []) or []:
            if 'janelia' in (aff.get('name', '') or '').lower():
                return True
    return False


def create_external_doi():
    ''' Create a new external_dois record for a manually-supplied external DOI,
        populating the same minimal fields pull_external_acks.py would.
        Keyword arguments:
          None
        Returns:
          None
    '''
    if DL.is_datacite(ARG.DOI):
        terminate_program(f"{ARG.DOI} is a DataCite DOI, which is not supported for an "
                          "external add (only Crossref DOIs)")
    try:
        resp = JRC.call_crossref(ARG.DOI)
    except Exception as err:
        terminate_program(err)
    rec = resp.get('message') if resp else None
    if not rec:
        terminate_program(f"{ARG.DOI} was not found in Crossref; cannot add to external_dois")
    title = DL.get_title(rec)
    if title:
        print(f"Title: {title}")
    authors = rec.get('author', []) or []
    if authors:
        names = [" ".join(filter(None, [a.get('given'), a.get('family')])) or a.get('name', '')
                 for a in authors[:5]]
        print(f"Authors: {', '.join(n for n in names if n)}"
              f"{' ...' if len(authors) > 5 else ''}")
    if _crossref_janelia_affiliation(rec) and not ARG.FORCE:
        terminate_program(
            f"{ARG.DOI} lists a Janelia author affiliation - it likely belongs in the dois "
            "collection (add it via ingestion). Re-run with --force to store it in "
            "external_dois anyway.")
    ack_text = get_acknowledgement_text()
    if not ack_text:
        terminate_program("No acknowledgement text was entered")
    payload = {'doi': ARG.DOI,
               'jrc_acknowledgements': ack_text,
               'jrc_ack_source': ARG.SOURCE,
               'jrc_publishing_date': DL.get_publishing_date(rec)}
    LOGGER.debug(f"external_dois payload: {payload}")
    if ARG.WRITE:
        try:
            result = DB['dis']['external_dois'].update_one(
                {"doi": ARG.DOI}, {"$set": payload}, upsert=True)
            if result.upserted_id or result.modified_count:
                print(f"Added {ARG.DOI} to external_dois (source: {ARG.SOURCE})")
            else:
                LOGGER.warning(f"No record was written for {ARG.DOI}")
        except Exception as err:
            terminate_program(err)
    else:
        print(f"Would add {ARG.DOI} to external_dois with:")
        for key, val in payload.items():
            print(f"  {key}: {val}")
        LOGGER.warning("Dry run successful, no updates were made")


def add_acknowledgement():
    ''' Add or update acknowledgement text: update the record in dois or
        external_dois if it exists, else (with --external) create a new
        external_dois record.
        Keyword arguments:
          None
        Returns:
          None
    '''
    rec, collection = find_record()
    if rec:
        update_acknowledgement(rec, collection)
    elif ARG.EXTERNAL:
        create_external_doi()
    else:
        terminate_program(f"{ARG.DOI} was not found in dois or external_dois; use --external "
                          "to add it to external_dois")

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Add acknowledgement text to a DOI record")
    PARSER.add_argument('--doi', dest='DOI', action='store',
                        type=str.lower, required=True, help='DOI to update')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, prod)')
    PARSER.add_argument('--external', dest='EXTERNAL', action='store_true',
                        default=False,
                        help='If the DOI is in neither collection, create it in external_dois')
    PARSER.add_argument('--force', dest='FORCE', action='store_true', default=False,
                        help='With --external, store even if a Janelia author affiliation is found')
    PARSER.add_argument('--source', dest='SOURCE', action='store', default='Manual',
                        help='jrc_ack_source label for a new external_dois record [Manual]')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Write to database')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    add_acknowledgement()
    terminate_program()
