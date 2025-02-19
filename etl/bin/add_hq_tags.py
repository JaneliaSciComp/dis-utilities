""" add_hq_tags.py
    Add tags from HQ database used to populate janelia.org
"""

__version__ = '1.0.0'

import argparse
import collections
import json
from operator import attrgetter
import sys
from rapidfuzz import fuzz, utils
from tqdm import tqdm
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Database
DB = {}
# General
ALL_TITLES = {}
SUPORG = {}
SUPORG_WRITE = {}
NOCODE = {}
NO_MONGO = []
NOT_FOUND = []
TITLE = []
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
        SUPORG_WRITE[key] = val
    LOGGER.info(f"Found {len(SUPORG):,} supervisory organizations")
    try:
        rows = DB['dis'].dois.find({"doi": {"$exists": True}})
        for row in rows:
            ALL_TITLES[DL.get_title(row)] = True
    except Exception as err:
        terminate_program(err)


def known_doi(doi):
    ''' Check if DOI is known to Crossref/DataCite
        Keyword arguments:
          doi: DOI
        Returns:
          True if known, False otherwise
    '''
    try:
        if DL.is_datacite(doi):
            row = JRC.call_datacite(doi)
        else:
            row = JRC.call_crossref(doi)
    except Exception as err:
        terminate_program(err)
    return row


def add_hq_tags(doi, hq_tags):
    ''' Add HQ tags to the database
        Keyword arguments:
          doi: DOI
          hq_tags: HQ tags
        Returns:
          None
    '''
    coll = DB['dis'].dois
    try:
        row = coll.find_one({'doi': doi})
        if not row:
            found = known_doi(doi)
            if found:
                COUNT['not_in_mongo'] += 1
                NO_MONGO.append(doi)
            else:
                COUNT['not_in_cd'] += 1
                NOT_FOUND.append(doi)
            return
        COUNT['in_mongo'] += 1
    except Exception as err:
        terminate_program(err)
    doi_tags = {}
    if 'jrc_tag' in row:
        for tag in row['jrc_tag']:
            doi_tags[tag['name']] = tag['code']
    for tag in hq_tags:
        for key, val in tag.items():
            if key in doi_tags.keys():
                continue
            payload = {"name": key, "code": val}
            payload['type'] = 'suporg' if key in SUPORG else 'affiliation'
            if 'jrc_tag' not in row:
                row['jrc_tag'] = []
            row['jrc_tag'].append(payload)
    if 'jrc_tag' in row:
        LOGGER.debug(json.dumps(row['jrc_tag'], indent=2))
    if 'jrc_tag' not in row:
        COUNT['no_tags'] += 1
        return
    if ARG.WRITE:
        try:
            coll.update_one({'doi': doi}, {"$set": {"jrc_tag": row['jrc_tag']}})
            COUNT['dwrite'] += 1
        except Exception as err:
            terminate_program(err)


def write_suporgs():
    ''' Write supervisory organizations to the database
        Keyword arguments:
          None
        Returns:
          None
    '''
    LOGGER.info(f"Found {len(SUPORG_WRITE):,} unique supervisory organizations")
    coll = DB['dis'].suporg
    try:
        rows = coll.find({})
    except Exception as err:
        terminate_program(err)
    present = {}
    for row in rows:
        present[row['name']] = True
    for name, code in SUPORG_WRITE.items():
        if name in present:
            continue
        payload = {'name': name, 'code': code}
        if name in SUPORG:
            payload['active'] = True
        try:
            coll.insert_one(payload)
            COUNT['swrite'] += 1
        except Exception as err:
            terminate_program(err)


def find_doi(title):
    ''' Find a DOI for a title
        Keyword arguments:
          title: title
        Returns:
          DOI
    '''
    try:
        rec = DB['dis'].dois.find_one({'title': title})
        if rec:
            return rec['doi']
    except Exception as err:
        terminate_program(err)
    for alt in ALL_TITLES.keys():
        score = fuzz.token_sort_ratio(title, alt, processor=utils.default_process)
        if score > 90:
            if score < 100:
                LOGGER.warning(f"Match: {title} {alt} {score}")
            rec = DB['dis'].dois.find_one({'title': alt})
            if not rec:
                LOGGER.error(f"Alternate title not found: {alt}")
                continue
            return rec['doi']
    return None


def processing():
    ''' Main processing routine
        Keyword arguments:
          None
        Returns:
          None
    '''
    with open(ARG.FILE, 'r', encoding='utf-8') as instream:
        data = json.load(instream)
    for node in tqdm(data['nodes']):
        COUNT['read'] += 1
        doi = ''
        hq_tags = []
        for key, val in node['node'].items():
            if key == 'DOI':
                doi = val.strip().lower().replace('https://doi.org/', '')
                doi = doi.replace('http://dx.doi.org/', '')
                doi = doi.replace('doi:', '').replace('doi: ', '')
                doi = doi.replace('\u200b', '')
                continue
            elif key == 'Title':
                title = val.strip()
                continue
            if not val:
                continue
            for tag in val:
                for name, code in tag.items():
                    COUNT['tags'] += 1
                    COUNT['suporg' if name in SUPORG else 'no_suporg'] += 1
                    if not code:
                        terminate_program(f"Empty code for {name}")
                    if name not in SUPORG:
                        if name in NOCODE and NOCODE[name]['code'] != code:
                            terminate_program(f"Multiple codes for {name}: ({code}) ({NOCODE[name]['code']})")
                        if name not in NOCODE:
                            NOCODE[name] = {'code': code, 'count': 0}
                        NOCODE[name]['count'] += 1
                        SUPORG_WRITE[name] = code
                    hq_tags.append({name: code})
        if not doi:
            doi = find_doi(title)
            if not doi:
                #LOGGER.error(json.dumps(node['node'], indent=2))
                TITLE.append(title)
                COUNT['missing_doi'] += 1
                continue
        LOGGER.debug(f"{doi}: {hq_tags}")
        add_hq_tags(doi, hq_tags)
    print("Tags not in current SupOrg list:")
    for key, val in sorted(NOCODE.items()):
        print(f"  {key}: {val['code']} ({val['count']:,})")
    if ARG.WRITE:
        write_suporgs()
    print(f"Nodes read:                    {COUNT['read']:,}")
    print(f"Missing DOIs:                  {COUNT['missing_doi']:,}")
    print(f"Tags found:                    {COUNT['tags']:,}")
    print(f"SupOrg tags:                   {COUNT['suporg']:,}")
    print(f"Non-SupOrg tags:               {COUNT['no_suporg']:,}")
    print(f"DOIs in MongoDB:               {COUNT['in_mongo']:,}")
    print(f"DOIs not in MongoDB:           {COUNT['not_in_mongo']:,}")
    print(f"DOIs not in Crossref/DataCite: {COUNT['not_in_cd']:,}")
    print(f"DOIs with no tags:             {COUNT['no_tags']:,}")
    print(f"SupOrgs written to collection: {COUNT['swrite']:,}")
    print(f"DOIs written to collection:    {COUNT['dwrite']:,}")
    if NO_MONGO:
        NO_MONGO.sort()
        filename = 'not_in_mongo.txt'
        with open(filename, 'w', encoding='ascii') as outstream:
            for doi in NO_MONGO:
                try:
                    outstream.write(f"{doi}\n")
                except Exception as err:
                    LOGGER.error(f"Could not write {repr(doi)} to {filename}")
                    #terminate_program(err)
    if NOT_FOUND:
        NOT_FOUND.sort()
        filename = 'not_in_crossref_datacite.txt'
        with open(filename, 'w', encoding='ascii') as outstream:
            for doi in NOT_FOUND:
                try:
                    outstream.write(f"{doi}\n")
                except Exception as err:
                    LOGGER.error(f"Could not write {repr(doi)} to {filename}")
    if TITLE:
        filename = 'missing_doi.txt'
        with open(filename, 'w', encoding='utf-8') as outstream:
            for title in TITLE:
                try:
                    outstream.write(f"{title}\n")
                except Exception as err:
                    LOGGER.error(f"Could not write {repr(title)} to {filename}")
                    LOGGER.error(err)

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Add tags from HQ database used to populate janelia.org")
    PARSER.add_argument('--file', dest='FILE', action='store',
                        default='export_with_title_updated.json', help='Tag JSON file')
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
