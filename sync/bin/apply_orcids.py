''' apply_orcids.py
    Apply ORCIDs from the ORCID API to the orcid collection
'''

__version__ = '1.1.0'

import argparse
import collections
import configparser
import json
from operator import attrgetter
import os
import sys
import requests
from tqdm import tqdm
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Database
DB = {}
# Counters
COUNT = collections.defaultdict(lambda: 0, {})
# Global variables
ARG = CONFIG = DIS = LOGGER = None
IGNORE = {}
# Output files
ADDED = []
OUTPUT = {"name_error": [], "name_multi_records": [], "name_not_found": [], "orcid_exists": [],
          "orcid_mismatch": [], "orcid_added": []}

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
    if "PEOPLE_API_KEY" not in os.environ:
        terminate_program("Missing token - set in PEOPLE_API_KEY environment variable")
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
        rows = DB['dis']['to_ignore'].find()
        for row in rows:
            if row['type'] not in IGNORE:
                IGNORE[row['type']] = {}
            IGNORE[row['type']][row['key']] = True
    except Exception as err:
        terminate_program(err)


def check_orcid(oid, name, family, given):
    ''' Check an ORCID record
        Keyword arguments:
          oid: ORCID
          name: name record from ORCID
          family: family name
          given: given name
    '''
    LOGGER.debug(f"{oid}: {family}, {given}")
    coll = DB['dis'].orcid
    try:
        cnt = coll.count_documents({'orcid': oid})
        if cnt:
            OUTPUT['orcid_exists'].append(name)
            return
        payload = {'family': family, 'given': given}
        cnt = coll.count_documents(payload)
        if not cnt:
            OUTPUT['name_not_found'].append(name)
            return
        if cnt > 1:
            OUTPUT['name_multi_records'].append(name)
            return
        rec = coll.find_one(payload)
    except Exception as err:
        terminate_program(err)
    if 'orcid' in rec:
        OUTPUT['orcid_mismatch'].append(name)
        return
    OUTPUT['orcid_added'].append(rec)
    email = rec['userIdO365']
    ADDED.append(f"{oid}: <a href='https://dis.int.janelia.org/userui/" \
                 + f"{email}'>{given} {family}</a>")
    if ARG.WRITE:
        try:
            coll.update_one({'_id': rec['_id']}, {'$set': {'orcid': oid}})
        except Exception as err:
            terminate_program(err)


def process_crossref_author(aut):
    ''' Process a Crossref author
        Keyword arguments:
          aut: author record
        Returns:
          None
    '''
    if not any('Janelia' in arec['name'] for arec in aut['affiliation']):
        return None
    if 'ORCID' not in aut:
        return None
    oid = aut['ORCID'].split('orcid.org/')[-1]
    return oid


def process_datacite_author(aut):
    ''' Process a DataCite author
        Keyword arguments:
          aut: author record
        Returns:
          None
    '''
    if not any('Janelia' in arec for arec in aut['affiliation']):
        return None
    if 'nameIdentifiers' not in aut:
        return None
    oid = None
    for findorcid in aut['nameIdentifiers']:
        if findorcid['nameIdentifierScheme'] == 'ORCID':
            oid = findorcid['nameIdentifier'].split('/')[-1]
            break
    return oid


def get_orcids_from_doi(oids, existing):
    ''' Get ORCIDs from the doi collection
        Keyword arguments:
          oids: list of ORCIDs
          existing: list of existing ORCIDs
          None
        Returns:
          None
    '''
    # Get ORCIDs from the doi collection
    dcoll = DB['dis'].dois
    # Crossref
    payload = {"author.affiliation.name": {"$regex": "Janelia"},
               "author.ORCID": {"$exists": True}}
    project = {"author.given": 1, "author.family": 1,
               "author.ORCID": 1, "author.affiliation": 1, "doi": 1}
    try:
        recs = dcoll.find(payload, project)
    except Exception as err:
        terminate_program(err)
    for rec in tqdm(recs, desc="Adding Crossref ORCIDs from doi collection"):
        if 'author' not in rec or rec['doi'] in IGNORE['doi']:
            continue
        for aut in rec['author']:
            oid = process_crossref_author(aut)
            if not oid or oid in existing or oid in IGNORE['orcid']:
                continue
            if oid not in oids:
                COUNT['read'] += 1
                oids.append(oid)
    # DataCite
    payload = {"creators.affiliation": {"$regex": "Janelia"},
               "creators.nameIdentifiers.nameIdentifierScheme": "ORCID"}
    project = {"creators": 1, "doi": 1}
    try:
        recs = dcoll.find(payload, project)
    except Exception as err:
        terminate_program(err)
    for rec in tqdm(recs, desc="Adding DataCite ORCIDs from doi collection"):
        if rec['doi'] in IGNORE['doi']:
            continue
        for aut in rec['creators']:
            oid = process_datacite_author(aut)
            if not oid or oid in existing or oid in IGNORE['orcid']:
                continue
            if oid not in oids:
                COUNT['read'] += 1
                oids.append(oid)


def process_orcid(oid):
    ''' Process an ORCID ID
        Keyword arguments:
          oid: ORCID
        Returns:
          None
    '''
    url = f"{CONFIG['orcid']['base']}{oid}"
    try:
        resp = requests.get(url, timeout=10,
                            headers={"Accept": "application/json"})
        orc = resp.json()
    except Exception as err:
        terminate_program(err)
    name = orc['person']['name']
    if not name or 'family-name' not in name or 'given-names' not in name:
        LOGGER.warning(f"ORCID {oid} has no name")
        OUTPUT['name_error'].append(name)
        return
    if not (name['family-name'] and name['given-names']):
        OUTPUT['name_error'].append(name)
        return
    family = name['family-name']['value']
    given = name['given-names']['value']
    if not (family and given):
        OUTPUT['name_error'].append(name)
        return
    check_orcid(oid, name, family, given)


def send_mail(dois, fname):
    ''' Send an email
        Keyword arguments:
          dois: list of DOIs
          fname: filename for DOIs
        Returns:
          None
    '''
    if dois:
        with open(fname, "w", encoding="ascii") as outstream:
            for doi in dois:
                outstream.write(f"{doi}\n")
    if not (ARG.TEST or ARG.WRITE):
        return
    text = f"{'Added' if ARG.WRITE else 'Would have added'} the following ORCIDs " \
           + "for authors in the orcid collection:<br>"
    for rec in ADDED:
        text += f"  {rec}<br>"
    subject = "ORCIDs added to orcid collection"
    email = DIS['developer'] if ARG.TEST else DIS['receivers']
    if dois:
        text += f"<br>DOIs to update: {len(dois)}<br>Please update DOIs using the attached file"
        JRC.send_email(text, DIS['sender'], email, subject, attachment=fname, mime='html')
    else:
        JRC.send_email(text, DIS['sender'], email, subject, mime='html')


def postprocessing():
    ''' Postprocessing
        Keyword arguments:
          None
        Returns:
          None
    '''
    for key, value in OUTPUT.items():
        fname = f"orcid_{key}.json"
        if value:
            with open(fname, "w", encoding="utf-8") as outstream:
                outstream.write(json.dumps(value, indent=2, default=str))
        elif os.path.exists(fname):
            os.remove(fname)
    dois = []
    if OUTPUT['orcid_added']:
        for rec in OUTPUT['orcid_added']:
            try:
                adois = DL.get_dois_by_author(rec, coll=DB['dis'].dois)
            except Exception as err:
                terminate_program(err)
            for doi in adois:
                dois.append(doi)
    print(f"ORCIDs read:                  {COUNT['read']:,}")
    print(f"ORCIDs considered:            {COUNT['considered']:,}")
    print(f"ORCIDs ignored:               {COUNT['orcid_ignored']:,}")
    print(f"ORCIDs with name error:       {len(OUTPUT['name_error']):,}")
    print(f"ORCIDs existing:              {len(OUTPUT['orcid_exists']):,}")
    print(f"ORCIDs with name not found:   {len(OUTPUT['name_not_found']):,}")
    print(f"ORCIDs with multiple records: {len(OUTPUT['name_multi_records'])}")
    print(f"ORCIDs with mismatch:         {len(OUTPUT['orcid_mismatch']):,}")
    print(f"ORCIDs added:                 {len(OUTPUT['orcid_added']):,}")
    if dois:
        print(f"DOIS to update:               {len(dois):,}")
    fname = "dois_to_update.txt"
    if os.path.exists(fname):
        os.remove(fname)
    if OUTPUT['orcid_added']:
        send_mail(dois, fname)


def apply_orcids():
    ''' Find ORCID IDs using the ORCID API
        Keyword arguments:
          None
        Returns:
          None
    '''
    # Get existing DOIs from the orcid collection
    existing = []
    try:
        existing = list(DB['dis']['orcid'].find({"orcid": {"$exists": True}}))
    except Exception as err:
        terminate_program(err)
    existing = [rec['orcid'] for rec in existing]
    # Get ORCIDs from the doi collection
    oids = []
    get_orcids_from_doi(oids, existing)
    if oids and ARG.VERBOSE:
        LOGGER.info(f"ORCIDs from doi collection: {len(oids)}")
    # Get ORCIDs from the ORCID API
    base = f"{CONFIG['orcid']['base']}search"
    search = {'hhmi': ['/?q=ror-org-id:"' + CONFIG['ror']['hhmi'] + '"',
                       '/?q=affiliation-org-name:"Howard Hughes Medical Institute"'],
              'janelia': ['/?q=ror-org-id:"' + CONFIG['ror']['janelia'] + '"',
                          '/?q=affiliation-org-name:"Janelia Research Campus"',
                          '/?q=affiliation-org-name:"Janelia Farm Research Campus"']
             }
    for url in (search[ARG.INSTITUTION]):
        try:
            resp = requests.get(f"{base}{url}", timeout=10,
                                headers={"Accept": "application/json"})
        except Exception as err:
            terminate_program(err)
        for orcid in resp.json()['result']:
            COUNT['read'] += 1
            oid = orcid['orcid-identifier']['path']
            if oid in existing:
                COUNT['orcid_exists'] += 1
                continue
            if oid not in oids:
                oids.append(oid)
    for oid in tqdm(sorted(oids), desc="Processing ORCIDs"):
        COUNT['considered'] += 1
        if oid in IGNORE['orcid']:
            COUNT['orcid_ignored'] += 1
            continue
        process_orcid(oid)
    postprocessing()


# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Apply ORCIDS to orcid collection")
    PARSER.add_argument('--institution', dest='INSTITUTION', action='store',
                        default='janelia', choices=['hhmi', 'janelia'],
                        help='Institution (hhmi, [janelia])')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, prod)')
    PARSER.add_argument('--test', dest='TEST', action='store_true',
                        default=False, help='Send email to developer')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Write to database/config system')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    try:
        DIS = JRC.simplenamespace_to_dict(JRC.get_config("dis"))
    except Exception as err:
        terminate_program(err)
    initialize_program()
    CONFIG = configparser.ConfigParser()
    CONFIG.read('config.ini')
    DIS = JRC.simplenamespace_to_dict(JRC.get_config("dis"))
    apply_orcids()
    terminate_program()
