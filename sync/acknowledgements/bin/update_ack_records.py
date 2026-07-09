""" update_acknowledgement_records.py
    Add some text
"""

__version__ = '1.0.0'

import argparse
import collections
from operator import attrgetter
import sys
import time
from tqdm import tqdm
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL

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
    dbs = ['dis']
    for source in dbs:
        dbo = attrgetter(f"{source}.{ARG.MANIFOLD}.write")(dbconfig)
        LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, ARG.MANIFOLD, dbo.host, dbo.user)
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)


def get_first_last_authors(rec, payload):
    ''' Get first and last authors from a record
        Keyword arguments:
          rec: record
          payload: payload to update
        Returns:
          None
    '''
    authors = DL.get_author_details(rec, DB['dis'].orcid)
    first = []
    last = []
    for auth in authors:
        if 'is_first' in auth and auth['is_first']:
            if 'family' in auth and 'given' in auth:
                first.append(', '.join([auth['family'], auth['given']]))
            else:
                first.append(auth['name'])
        if 'is_last' in auth and auth['is_last']:
            if 'family' in auth and 'given' in auth:
                last.append(', '.join([auth['family'], auth['given']]))
            else:
                last.append(auth['name'])
    if first:
        payload['jrc_ack_first_author'] = first
    if last:
        if len(last) > 1:
            terminate_program(f"Multiple last authors for {rec['doi']}")
        payload['jrc_ack_last_author'] = last[0]


def process_external():
    ''' Process external DOIs
        Keyword arguments:
          None
        Returns:
          None
    '''
    try:
        cnt = DB['dis'].external_dois.count_documents({})
        rows = DB['dis'].external_dois.find({})
    except Exception as err:
        terminate_program(err)
    LOGGER.info(f"Found {cnt:,} records in external_dois")
    COUNT['external_read'] = cnt
    for row in tqdm(rows, total=cnt, desc="External"):
        missing = False
        for field in ['jrc_journal', 'title', 'jrc_ack_first_author', 'jrc_ack_last_author',
                      'is_preprint', 'type']:
            if field not in row:
                LOGGER.debug(f"Missing {field} for {row['doi']}")
                missing = True
        if not missing:
            COUNT['external_ok'] += 1
            continue
        try:
            if DL.is_datacite(row['doi']):
                rec = JRC.call_datacite(row['doi'])
                rec = rec['data']['attributes']
            else:
                rec = JRC.call_crossref(row['doi'])
                if rec:
                    rec = rec['message']
            time.sleep(.7)
        except Exception as err:
            LOGGER.warning(err)
            continue
        if not rec:
            terminate_program(f"Could not find record for {row['doi']}")
        rec['doi'] = row['doi']
        payload = {}
        is_pp = DL.is_preprint(rec)
        payload['is_preprint'] = is_pp
        for transfer in ['type', 'subtype']:
            if rec.get(transfer):
                payload[transfer] = rec[transfer]
        jrn = DL.get_journal(rec, name_only=True)
        if jrn:
            payload['jrc_journal'] = jrn
        ttl = DL.get_title(rec)
        if ttl:
            payload['title'] = ttl
        get_first_last_authors(rec, payload)
        if ARG.WRITE:
            try:
                result = DB['dis'].external_dois.update_one({"doi": row['doi']}, {"$set": payload})
                if hasattr(result, 'modified_count') and result.modified_count:
                    COUNT['external_dois_written'] += 1
            except Exception as err:
                terminate_program(err)
        else:
            COUNT['external_dois_written'] += 1


def process_internal():
    ''' Process internal DOIs
        Keyword arguments:
          None
        Returns:
          None
    '''
    payload = {"jrc_acknowledgements": {"$exists": True}}
    try:
        cnt = DB['dis'].dois.count_documents(payload)
        rows = DB['dis'].dois.find(payload)
    except Exception as err:
        terminate_program(err)
    LOGGER.info(f"Found {cnt:,} records in internal_dois")
    COUNT['internal_read'] = cnt
    for row in tqdm(rows, total=cnt, desc="Internal"):
        missing = False
        for field in ['jrc_ack_first_author', 'jrc_ack_last_author']:
            if field not in row:
                LOGGER.debug(f"Missing {field} for {row['doi']}")
                missing = True
        if not missing:
            COUNT['internal_ok'] += 1
            continue
        try:
            if DL.is_datacite(row['doi']):
                rec = JRC.call_datacite(row['doi'])
                if rec:
                    rec = rec['data']['attributes']
            else:
                rec = JRC.call_crossref(row['doi'])
                if rec:
                    rec = rec['message']
                else:
                    terminate_program(f"Could not find record for {row['doi']}")
            time.sleep(.7)
        except Exception as err:
            LOGGER.warning(err)
            continue
        rec['doi'] = row['doi']
        payload = {}
        get_first_last_authors(rec, payload)
        if ARG.WRITE:
            try:
                result = DB['dis'].dois.update_one({"doi": row['doi']}, {"$set": payload})
                if hasattr(result, 'modified_count') and result.modified_count:
                    COUNT['internal_dois_written'] += 1
            except Exception as err:
                terminate_program(err)
        else:
            COUNT['internal_dois_written'] += 1


def processing():
    ''' Main processing routine
        Keyword arguments:
          None
        Returns:
          None
    '''
    process_external()
    process_internal()
    print(f"External records read:    {COUNT['external_read']:,}")
    print(f"External records ok:      {COUNT['external_ok']:,}")
    print(f"External records updated: {COUNT['external_dois_written']:,}")
    print(f"Internal records read:    {COUNT['internal_read']:,}")
    print(f"Internal records ok:      {COUNT['internal_ok']:,}")
    print(f"Internal records updated: {COUNT['internal_dois_written']:,}")


# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Check external_dois for jrc_journal field")
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
    initialize_program()
    processing()
    terminate_program()
