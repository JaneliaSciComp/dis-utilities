""" convert_tags.py
    Convert tag lists to tag dictionaries
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
# General
SUPORG = {}
NOCODE = {}
# Counters
COUNT = collections.defaultdict(lambda: 0, {})

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
    try:
        orgs = DL.get_supervisory_orgs()
    except Exception as err:
        terminate_program(err)
    for key, val in orgs.items():
        SUPORG[key] = val
    LOGGER.info(f"Found {len(SUPORG):,} supervisory organizations")


def processing():
    ''' Main processing routine
        Keyword arguments:
          None
        Returns:
          None
    '''
    payload = {"jrc_tag": {"$exists": True}}
    try:
        cnt = DB['dis'].dois.count_documents(payload)
        rows = DB['dis'].dois.find(payload)
    except Exception as err:
        terminate_program(err)
    LOGGER.info(f"Found {cnt:,} DOIs with jrc_tag")
    COUNT['read'] = cnt
    for row in rows:
        tags = []
        for tag in row['jrc_tag']:
            if isinstance(tag, dict):
                LOGGER.warning(f"DOI {row['doi']} {tag['name']} has a dict tag")
                COUNT['already_converted'] += 1
                break
            tagtype = 'suporg'
            code = SUPORG.get(tag)
            if not code:
                tagtype = 'affiliation'
                if tag not in NOCODE:
                    NOCODE[tag] = []
                NOCODE[tag].append(row['doi'])
            tags.append({"name": tag, "code": code, "type": tagtype})
            COUNT['tags_processed'] += 1
        if not tags:
            continue
        LOGGER.debug(f"{row['doi']} {json.dumps(tags, indent=2)}")
        if ARG.WRITE:
            try:
                result = DB['dis'].dois.update_one({"doi": row['doi']}, {"$set": {"jrc_tag": tags}})
                if hasattr(result, 'matched_count') and result.matched_count:
                    COUNT['dois_written'] += 1
            except Exception as err:
                terminate_program(err)
        COUNT['dois_processed'] += 1
    print("Tags with no SUPORG code:")
    sep = "\n  "
    for tag, dois in NOCODE.items():
        print(f"{tag}\n  {sep.join(dois)}")
    print(f"DOIs read:              {COUNT['read']:,}")
    print(f"Tags processed:         {COUNT['tags_processed']:,}")
    print(f"DOIs processed:         {COUNT['dois_processed']:,}")
    print(f"DOIs already converted: {COUNT['already_converted']:,}")
    print(f"DOIs written:           {COUNT['dois_written']:,}")

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Convert tag lists to tag dictionaries")
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, prod)')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Write to database/config system')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    processing()
    terminate_program()
