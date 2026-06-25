''' add_field_to_external_dois.py

PURPOSE
-------
For every DOI in the external_dois collection, fetch the record from
Crossref or DataCite and, if the requested field is present, upsert it
into the external_dois document.

INPUTS
------
- DIS MongoDB database (read/write depending on --write flag).
- Command-line flags:
    --source   API to query: crossref or datacite (required).
    --field    Field name to copy from the API response into external_dois (required).
    --write    Actually update the database (default: dry-run).
    --verbose  Increase logging verbosity.
    --debug    Maximum logging verbosity.

HIGH-LEVEL FLOW
---------------
1. Connect to the DIS MongoDB database.
2. Load all DOIs from external_dois.
3. For each DOI:
   a. Call JRC.call_crossref (Crossref) or JRC.call_datacite (DataCite).
   b. Extract the field from the API response (top-level under
      rec['message'] for Crossref, rec['data']['attributes'] for DataCite).
   c. If the field is present, upsert it into the external_dois document.
4. Print a summary of counters.

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
import time
from tqdm import tqdm
import jrc_common.jrc_common as JRC

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Database
DB = {}
# Counters
COUNT = collections.defaultdict(lambda: 0, {})
# Global variables
ARG = LOGGER = None
RECORDS = []


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
        dbo = attrgetter(f"{source}.prod.write")(dbconfig)
        LOGGER.info(f"Connecting to {dbo.name} prod on {dbo.host} as {dbo.user}")
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)


def _call_api(doi, retries=5):
    ''' Call the appropriate API for a DOI with retry on rate-limit errors.
        Keyword arguments:
          doi:     DOI string
          retries: maximum number of attempts
        Returns:
          API response dict, or None on persistent failure
    '''
    for attempt in range(retries):
        try:
            if ARG.SOURCE == 'crossref':
                return JRC.call_crossref(doi)
            return JRC.call_datacite(doi)
        except Exception as err:
            err_str = str(err)
            if '429' in err_str or 'too_many_requests' in err_str:
                wait = 2 ** (attempt + 1)
                LOGGER.warning(f"Rate limited on attempt {attempt + 1} for {doi}; "
                               f"waiting {wait}s")
                time.sleep(wait)
                continue
            raise
    LOGGER.warning(f"Rate limit persisted after {retries} retries for {doi}")
    return None


def _extract_field(api_response):
    ''' Extract the target field from an API response dict.
        Keyword arguments:
          api_response: raw response from JRC.call_crossref or JRC.call_datacite
        Returns:
          Field value, or None if absent
    '''
    if not api_response:
        return None
    if ARG.SOURCE == 'crossref':
        rec = api_response.get('message', {})
    else:
        rec = (api_response.get('data') or {}).get('attributes', {})
    return rec.get(ARG.FIELD)


def _process_doi(doi):
    ''' Fetch the API record for one DOI and upsert the target field if present.
        Keyword arguments:
          doi: DOI string
        Returns:
          None
    '''
    LOGGER.debug(f"Processing {doi}")
    time.sleep(0.5)
    try:
        api_resp = _call_api(doi)
    except Exception as err:
        LOGGER.warning(f"API call failed for {doi}: {err}")
        COUNT['api_error'] += 1
        return
    if not api_resp:
        LOGGER.debug(f"No API response for {doi}")
        COUNT['not_found'] += 1
        return
    value = _extract_field(api_resp)
    if value is None:
        LOGGER.debug(f"Field '{ARG.FIELD}' absent for {doi}")
        COUNT['field_absent'] += 1
        return
    COUNT['field_found'] += 1
    LOGGER.debug(f"  {doi}: {ARG.FIELD} = {str(value)[:80]}")
    RECORDS.append({"doi": doi, ARG.FIELD: value})
    if ARG.WRITE:
        try:
            result = DB['dis']['external_dois'].update_one(
                {"doi": doi},
                {"$set": {ARG.FIELD: value}},
                upsert=False
            )
            if result.modified_count:
                COUNT['updated'] += 1
                LOGGER.debug(f"  Updated {doi}")
            else:
                COUNT['unchanged'] += 1
        except Exception as err:
            LOGGER.warning(f"DB update failed for {doi}: {err}")
            COUNT['db_error'] += 1
    else:
        COUNT['updated'] += 1


def _print_summary():
    ''' Print a summary of processing counters.
        Keyword arguments:
          None
        Returns:
          None
    '''
    print(f"\nDOIs read:          {COUNT['read']:,}")
    if COUNT['no_doi']:
        print(f"No DOI:             {COUNT['no_doi']:,}")
    print(f"Not found in API:   {COUNT['not_found']:,}")
    if COUNT['api_error']:
        print(f"API errors:         {COUNT['api_error']:,}")
    print(f"Field absent:       {COUNT['field_absent']:,}")
    print(f"Field found:        {COUNT['field_found']:,}")
    if ARG.WRITE:
        print(f"Records updated:    {COUNT['updated']:,}")
        if COUNT['unchanged']:
            print(f"Records unchanged:  {COUNT['unchanged']:,}")
        if COUNT['db_error']:
            print(f"DB errors:          {COUNT['db_error']:,}")
    else:
        print(f"Records to update:  {COUNT['updated']:,}")
        print("(dry-run — use --write to apply changes)")


def processing():
    ''' Processing
        Keyword arguments:
          None
        Returns:
          None
    '''
    try:
        rows = list(DB['dis']['external_dois'].find({}, {"doi": 1, "_id": 0},
                                                       limit=ARG.LIMIT or 0))
    except Exception as err:
        terminate_program(err)
    COUNT['read'] = len(rows)
    LOGGER.info(f"Processing {COUNT['read']:,} DOIs from external_dois")
    for row in tqdm(rows, desc="Processing DOIs"):
        doi = row.get('doi')
        if not doi:
            COUNT['no_doi'] += 1
            continue
        _process_doi(doi)
    if RECORDS:
        jfile = f"external_dois_{ARG.FIELD}.json"
        with open(jfile, 'w', encoding='utf-8') as jfp:
            json.dump(RECORDS, jfp, indent=2)
        LOGGER.info(f"Wrote {len(RECORDS):,} records to {jfile}")
    _print_summary()


# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Copy a field from Crossref/DataCite into external_dois records")
    PARSER.add_argument('--source', dest='SOURCE', action='store',
                        choices=['crossref', 'datacite'], required=True,
                        help='API source to query (crossref or datacite)')
    PARSER.add_argument('--field', dest='FIELD', action='store', required=True,
                        help='Field name to copy into external_dois')
    PARSER.add_argument('--limit', dest='LIMIT', action='store', type=int,
                        default=0, help='Limit number of DOIs to process (0=all)')
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
