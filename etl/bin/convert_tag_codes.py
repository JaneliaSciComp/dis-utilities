""" convert_tag_codes.py
    Normalize jrc_tag entries to one shape: code is the {code, active} dict
    from the suporg collection (None when the name is not a supervisory
    organization), and type follows the code - "suporg" when there is one,
    "affiliation" when there is not - the rule every tag writer applies.

    The first pass (1.0.0) rewrote the code but kept each entry's existing
    type, which left 2,083 entries typed "affiliation" carrying a suporg code:
    tags written with code None whose name later became a supervisory
    organization. This pass retypes those too. Idempotent: an entry already
    in the target shape is left alone, and a DOI with nothing to change is
    not written.
"""

__version__ = '1.1.0'

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


def get_suporg_code(name):
    ''' Get the code for a supervisory organization
        Keyword arguments:
          name: name of the organization
        Returns:
          Code for the organization
    '''
    if name in SUPORG:
        return SUPORG[name]
    return None


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
        # Per DOI, not once for the run: a single flag set by the first dirty
        # DOI made every DOI after it a write, changed or not.
        dirty = False
        for tag in row['jrc_tag']:
            COUNT['tags_found'] += 1
            # A dict code is kept as is (the org may since have left the suporg
            # collection); anything else is resolved from the name.
            code = tag.get('code') if isinstance(tag.get('code'), dict) \
                   else get_suporg_code(tag['name'])
            tagtype = 'suporg' if code else 'affiliation'
            if tag.get('code') == code and tag.get('type') == tagtype:
                tags.append(tag)
                COUNT['already_converted'] += 1
                continue
            dirty = True
            if tag.get('code') != code:
                COUNT['recoded'] += 1
            if tag.get('type') != tagtype:
                COUNT['retyped'] += 1
            tags.append({"name": tag['name'], "code": code, "type": tagtype})
            COUNT['tags_processed'] += 1
        if not tags or not dirty:
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
    print(f"DOIs read:              {COUNT['read']:,}")
    print(f"Tags found:             {COUNT['tags_found']:,}")
    print(f"Tags already in shape:  {COUNT['already_converted']:,}")
    print(f"Tags changed:           {COUNT['tags_processed']:,}")
    print(f"  code rewritten:       {COUNT['recoded']:,}")
    print(f"  type corrected:       {COUNT['retyped']:,}")
    print(f"DOIs with changes:      {COUNT['dois_processed']:,}")
    print(f"DOIs written:           {COUNT['dois_written']:,}")
    if not ARG.WRITE:
        LOGGER.warning("Dry run: nothing was written")

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
    SUPORG = DL.get_supervisory_orgs(coll=DB['dis'].suporg)
    processing()
    terminate_program()
