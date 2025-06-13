""" update_dois.py
    Synchronize DOI information from an input source to databases.
    If a single DOI or file of DOIs is specified, these are updated in FlyBoy/config or DIS MongoDB.
    Otherwise, DOIs are synced according to target:
    - flyboy: FLYF2 to FlyBoy and the config system
    - dis: FLYF2, Crossref, DataCite, ALPS releases, EM datasets, and "to process" DOIs
           to DIS MongoDB.
"""

__version__ = '12.0.0'

import argparse
import configparser
from datetime import datetime
import json
from operator import attrgetter
import os
import re
import select
import sys
from time import sleep, strftime
from unidecode import unidecode
import MySQLdb
import requests
from tqdm import tqdm
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL

# pylint: disable=broad-exception-caught,broad-exception-raised,logging-fstring-interpolation,logging-not-lazy,too-many-lines

# Database
DB = {}
READ = {'dois': "SELECT doi FROM doi_data",}
WRITE = {'doi': "INSERT INTO doi_data (doi,title,first_author,"
                + "publication_date) VALUES (%s,%s,%s,%s) ON "
                + "DUPLICATE KEY UPDATE title=%s,first_author=%s,"
                + "publication_date=%s",
         'delete_doi': "DELETE FROM doi_data WHERE doi=%s",
        }
# Configuration
CKEY = {"flyboy": "dois"}
ALUMNI = {}
CROSSREF = {}
DATACITE = {}
CROSSREF_CALL = {}
DATACITE_CALL = {}
INSERTED = {}
UPDATED = {}
MISSING = {}
NO_AUTHOR = {}
TO_BE_PROCESSED = []
MAX_DOI_TRIES = 3
# General
ARG = CONFIG = DISCONFIG = EXISTING = LOGGER = REST = START_TIME = None
PROJECT = {}
SUPORG = {}
DEFAULT_TAGS = ['Janelia Experimental Technology (jET)', 'Scientific Computing Software']
COUNT = {'crossref': 0, 'datacite': 0, 'found': 0, 'foundc': 0, 'foundd': 0,
         'notfound': 0, 'noupdate': 0, 'noauthor': 0,
         'insert': 0, 'update': 0, 'delete': 0, 'foundfb': 0, 'flyboy': 0}

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


def call_responder(server, endpoint, payload=None, timeout=10):
    """ Call a responder
        Keyword arguments:
        server: server
        endpoint: REST endpoint
    """
    url = ((getattr(getattr(REST, server), "url") if server else "") if "REST" in globals() \
           else (os.environ.get('CONFIG_SERVER_URL') if server else "")) + endpoint
    try:
        if payload:
            return requests.post(url, data=payload, timeout=timeout)
        req = requests.get(url, timeout=timeout)
    except requests.exceptions.RequestException as err:
        terminate_program(f"Could not fetch from {url}\n{str(err)}")
    if req.status_code != 200:
        terminate_program(f"Status: {str(req.status_code)} ({url})")
    return req.json()


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
    dbs = ['flyboy']
    if ARG.TARGET == 'dis':
        dbs.append('dis')
    for source in dbs:
        manifold = ARG.MANIFOLD if source == 'dis' else 'prod'
        dbo = attrgetter(f"{source}.{manifold}.write")(dbconfig)
        LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, manifold, dbo.host, dbo.user)
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)
    if ARG.TARGET == 'flyboy':
        return
    try:
        orgs = DL.get_supervisory_orgs()
    except Exception as err:
        terminate_program(err)
    for key, val in orgs.items():
        SUPORG[key] = val


def get_dis_dois_from_mongo():
    ''' Get DOIs from MongoDB
        Keyword arguments:
          None
        Returns:
          Dict keyed by DOI with value set up update date
    '''
    coll = DB['dis'].dois
    result = {}
    recs = coll.find({}, {"doi": 1, "updated": 1, "deposited": 1})
    for rec in recs:
        if DL.is_datacite(rec['doi']):
            if "updated" not in rec:
                terminate_program(f"Could not find updated field for {rec['doi']} (DataCite)")
            result[rec['doi']] = {"updated": rec['updated']}
        else:
            if "deposited" not in rec:
                terminate_program(f"Could not find deposited field for {rec['doi']} (Crossref)")
            result[rec['doi']] = {"deposited": {'date-time': rec['deposited']['date-time']}}
    LOGGER.info(f"Got {len(result):,} DOIs from DIS Mongo")
    return result


def get_dois_from_crossref():
    ''' Get DOIs from Crossref
        Keyword arguments:
          None
        Returns:
          List of unique DOIs
    '''
    dlist = []
    LOGGER.info("Getting DOIs from Crossref")
    suffix = CONFIG['crossref']['janelia']
    complete = False
    parts = 0
    while not complete:
        try:
            if parts:
                resp = JRC.call_crossref(f"{suffix}&offset={parts*1000}", timeout=20)
            else:
                resp = JRC.call_crossref(suffix, timeout=20)
        except Exception as err:
            terminate_program(err)
        recs = resp['message']['items']
        if not recs:
            break
        parts += 1
        for rec in recs:
            COUNT['crossref'] += 1
            doi = rec['doi'] = rec['DOI']
            rec['jrc_obtained_from'] = 'Crossref'
            if doi in CROSSREF:
                continue
            dlist.append(doi)
            CROSSREF[doi] = {"message": rec}
        if len(dlist) >= resp['message']['total-results']:
            complete = True
    LOGGER.info(f"Got {len(dlist):,} DOIs from Crossref in {parts} part(s)")
    return dlist


def get_dois_from_datacite(query):
    ''' Get DOIs from DataCite
        Keyword arguments:
          query: query type
        Returns:
          List of unique DOIs
    '''
    dlist = []
    LOGGER.info(f"Getting DOIs from DataCite ({query})")
    complete = False
    suffix = CONFIG['datacite'][query]
    parts = 0
    while not complete:
        try:
            recs = call_responder('datacite', suffix, timeout=20)
        except Exception as err:
            terminate_program(err)
        parts += 1
        for rec in recs['data']:
            COUNT['datacite'] += 1
            rec['jrc_obtained_from'] = 'DataCite'
            doi = rec['attributes']['doi']
            if doi in DATACITE:
                continue
            dlist.append(doi)
            DATACITE[doi] = {"data": {"attributes": rec['attributes']}}
        if 'links' in recs and 'next' in recs['links']:
            suffix = recs['links']['next'].replace('https://api.datacite.org/dois', '')
            suffix += "&sort=created"
        else:
            complete = True
    LOGGER.info(f"Got {len(dlist):,} DOIs from DataCite in {parts} part(s) for {query}")
    LOGGER.info(f"Writing DOIs to datacite_{query}_dois.txt")
    with open(f"datacite_{query}_dois.txt", "w", encoding='ascii') as outstream:
        for doi in dlist:
            outstream.write(f"{doi}\n")
    return dlist


def add_to_be_processed(dlist):
    ''' Add DOIs from the dois_to_process collection
        Keyword arguments:
          dlist: list of DOIs
        Returns:
          None
    '''
    try:
        rows = DB['dis'].dois_to_process.find({})
    except Exception as err:
        terminate_program(err)
    for row in rows:
        doi = row['doi']
        if doi not in dlist:
            TO_BE_PROCESSED.append(doi)
            dlist.append(doi)
    if TO_BE_PROCESSED:
        LOGGER.info(f"Got {len(TO_BE_PROCESSED):,} DOIs from dois_to_process")

def add_alps_releases(dlist):
    ''' Add ALPS releases to the list of DOIs to process
        Keyword arguments:
          dlist: list of DOIs
        Returns:
          None
    '''
    releases = JRC.simplenamespace_to_dict(JRC.get_config('releases'))
    cnt = 0
    for val in releases.values():
        if 'doi' in val:
            for dtype in ('dataset', 'preprint', 'publication'):
                if dtype in val['doi'] and val['doi'][dtype] not in dlist:
                    cnt += 1
                    dlist.append(val['doi'][dtype])
    LOGGER.info(f"Got {cnt:,} DOIs from ALPS releases")


def get_dois_for_dis(flycore):
    ''' Get a list of DOIs to process for an update of the DIS database. Sources are:
        - DOIs with an affiliation of Janelia from Crossref
        - All Janelia-prefixed DOIs from DataCite
        - DOIs with an affiliation of Janelia from DataCite
        - DOIs in use by FLYF2
        - DOIs associated with ALPs releases
        - DOIs associated with FlyEM datasets
        - DOIs from dois_to_process collection
        - DOIs that are already in the DIS database
        Keyword arguments:
          flycore: list of DOIs from FlyCore
        Returns:
          Dict with a single "dois" key and value of a list of DOIs
    '''
    # Crossref
    dlist = get_dois_from_crossref()
    # DataCite
    dlist.extend(get_dois_from_datacite("janelia"))
    dlist.extend(get_dois_from_datacite("affiliation"))
    # FlyCore
    for doistring in flycore['dois']:
        for doi in split_raw_doi(doistring):
            if doi not in dlist and 'in prep' not in doi:
                dlist.append(doi)
    # ALPS releases
    add_alps_releases(dlist)
    # EM datasets
    emdois = JRC.simplenamespace_to_dict(JRC.get_config('em_dois'))
    cnt = 0
    for key, val in emdois.items():
        if key in DISCONFIG['em_dataset_ignore']:
            continue
        if val and isinstance(val, str):
            cnt += 1
            dlist.append(val)
        elif val and isinstance(val, list):
            for dval in val:
                cnt += 1
                dlist.append(dval)
    # DOIs to be processed
    add_to_be_processed(dlist)
    LOGGER.info(f"Got {cnt:,} DOIs from EM releases")
    # Previously inserted
    for doi in EXISTING:
        if doi not in dlist:
            dlist.append(doi)
    return {"dois": dlist}


def split_raw_doi(doi_string):
    """ Split a raw DOI string into multiple DOIs
        Keyword arguments:
          doi: DOI
        Returns:
          dois: list of DOIs
    """
    return re.split(r"\s*\|\s*", doi_string)


def get_dois():
    ''' Get a list of DOIs to process. This will be one of four things:
        - a single DOI from ARG.DOI
        - a list of DOIs from ARG.FILE
        - DOIs needed for an update of the DIS database
        - DOIs from FLYF2
        Keyword arguments:
          None
        Returns:
          Dict with a single "dois" key and value of a list of DOIs
    '''
    if ARG.DOI:
        return {"dois": [ARG.DOI]}
    if ARG.FILE:
        dois = []
        for doi in ARG.FILE.read().splitlines():
            dois.extend(split_raw_doi(doi))
        LOGGER.info(f"Got {len(dois):,} DOIs from {ARG.FILE.name}")
        return {"dois": dois}
    if ARG.PIPE:
        # Handle input from STDIN
        inp = ""
        piped = False
        while sys.stdin in select.select([sys.stdin], [], [], 0)[0]:
            piped = True
            line = sys.stdin.readline()
            if line:
                inp += line
            else:
                break
        if piped:
            return {"dois": inp.splitlines()}
    flycore = call_responder('flycore', '?request=doilist')
    LOGGER.info(f"Got {len(flycore['dois']):,} DOIs from FLYF2")
    if ARG.TARGET == 'dis':
        return get_dois_for_dis(flycore)
    # Default is to pull from FlyCore
    pflycore = []
    for doistring in flycore['dois']:
        dois = split_raw_doi(doistring)
        for doi in dois:
            if 'in prep' not in doi and doi not in pflycore:
                pflycore.append(doi)
    return {'dois': pflycore}


class DOINotFound(Exception):
    """ DOINotFound exception
    """
    pass


def call_crossref(doi):
    """ Get DOI information from crossref
        Keyword arguments:
        doi: DOI
    """
    try:
        req = JRC.call_crossref(doi)
    except requests.exceptions.RequestException as err:
        if type(err).__name__ in ['ConnectTimeout', 'ReadTimeout']:
            raise err
        terminate_program(err)
    if req:
        return req
    COUNT['notfound'] += 1
    MISSING[f"Could not find {doi} in Crossref"] = True
    raise DOINotFound(f"Could not find {doi} in Crossref")


def call_crossref_with_retry(doi):
    """ Looping function for call_crossref
        Keyword arguments:
          doi: DOI
        Returns:
          msg: response from crossref.org
    """
    attempt = MAX_DOI_TRIES
    msg = None
    while attempt:
        try:
            msg = call_crossref(doi)
            return msg
        except DOINotFound as err:
            raise err
        except Exception as err:
            if type(err).__name__ in ['ConnectTimeout', 'ReadTimeout']:
                attempt -= 1
                if not attempt:
                    raise Exception(f"Could not find {doi} in Crossref: multiple timeouts")
                LOGGER.warning(f"crossref.org timeout for {doi}: retrying ({attempt})")
                sleep(1)
                continue
            raise Exception(err) from err


def call_datacite(doi):
    """ Get record from DataCite
        Keyword arguments:
          doi: DOI
        Returns:
          rec: response from crossref.org
    """
    try:
        rec = DATACITE[doi] if doi in DATACITE else JRC.call_datacite(doi)
    except requests.exceptions.RequestException as err:
        if type(err).__name__ in ['ConnectTimeout', 'ReadTimeout']:
            raise err
        terminate_program(err)
    if rec:
        return rec
    COUNT['notfound'] += 1
    MISSING[f"Could not find {doi} in DataCite"] = True
    raise Exception(f"Could not find {doi} in DataCite")


def call_datacite_with_retry(doi):
    """ Looping function for call_datacite
        Keyword arguments:
          doi: DOI
        Returns:
          msg: response from datacite.org
    """
    attempt = MAX_DOI_TRIES
    msg = None
    while attempt:
        try:
            msg = call_datacite(doi)
            return msg
        except Exception as err:
            if type(err).__name__ in ['ConnectTimeout', 'ReadTimeout']:
                attempt -= 1
                if not attempt:
                    raise Exception(f"Could not find {doi} in DataCite: multiple timeouts")
                LOGGER.warning(f"datacite.org timeout for {doi}: retrying ({attempt})")
                sleep(1)
                continue
            raise Exception(err) from err


def get_doi_record(doi):
    """ Return the record for a single DOI
        Keyword arguments:
          doi: DOI
        Returns:
          record for a single DOI
    """
    msg = None
    if DL.is_datacite(doi):
        # DataCite
        if doi in DATACITE:
            msg = DATACITE[doi]
        else:
            try:
                msg = call_datacite_with_retry(doi)
                DATACITE_CALL[doi] = True
            except Exception as err:
                terminate_program(err)
    else:
        # Crossref
        if doi in CROSSREF:
            msg = CROSSREF[doi]
        else:
            try:
                msg = call_crossref_with_retry(doi)
                if 'author' not in msg['message'] and 'editor' not in msg['message']:
                    MISSING[f"No author for {doi}"] = True
                    LOGGER.warning(f"No author for {doi}")
                    COUNT['noauthor'] += 1
                    return None
                if 'title' not in msg['message']:
                    LOGGER.warning(f"No title for {doi}")
                    MISSING[f"No title for {doi}"] = True
                    return None
                CROSSREF_CALL[doi] = True
            except DOINotFound as err:
                LOGGER.warning(err)
            except Exception as err:
                terminate_program(err)
    return msg


def convert_timestamp(stamp):
    """ Convert a Crossref or DataCite stamp to a standard format
        Keyword arguments:
          stamp: timestamp
        Returns:
          Converted timestamp
    """
    return re.sub(r'\.\d+Z', 'Z', stamp)


def too_old(doi, msg):
    """ Determine if a DOI was published before 2006-04
        Keyword arguments:
          doi: DOI
          msg: record from Crossref or DataCite
        Returns:
          True or False
    """
    pdate = DL.get_publishing_date(msg)
    if pdate < '2006-04-01':
        LOGGER.warning(f"Skipping {doi} because it was published before 2006-04")
        return True
    return False


def crossref_needs_update(doi, msg):
    """ Determine if a Crossref DOI needs updating on our system
        Keyword arguments:
          doi: DOI
          msg: record from Crossref
        Returns:
          True or False
    """
    if too_old(doi, msg):
        return False
    if 'deposited' not in msg or 'date-time' not in msg['deposited']:
        return True
    if doi not in EXISTING:
        return True
    rec = EXISTING[doi]
    if 'deposited' not in rec or 'date-time' not in rec['deposited']:
        return True
    stored = convert_timestamp(rec['deposited']['date-time'])
    new = convert_timestamp(msg['deposited']['date-time'])
    needs_update = bool(stored != new)
    if ARG.FORCE:
        needs_update = True
    if needs_update:
        LOGGER.debug(f"Update {doi} {stored} -> {new}")
        UPDATED[doi] = f"Deposited {stored} -> {new}"
    else:
        COUNT['noupdate'] += 1
    return needs_update


def datacite_needs_update(doi, msg):
    """ Determine if a DataCite DOI needs updating on our system
        Keyword arguments:
          doi: DOI
          msg: record from DataCite
        Returns:
          True or False
    """
    if 'attributes' not in msg or 'updated' not in msg['attributes']:
        return True
    if too_old(doi, msg['attributes']):
        return False
    if doi not in EXISTING:
        return True
    rec = EXISTING[doi]
    stored = convert_timestamp(rec['updated'])
    new = convert_timestamp(msg['attributes']['updated'])
    needs_update = bool(stored != new)
    if ARG.FORCE:
        needs_update = True
    if needs_update:
        LOGGER.debug(f"Update {doi} {stored} -> {new}")
        UPDATED[doi] = f"Updated {stored} -> {new}"
    else:
        COUNT['noupdate'] += 1
    return needs_update


def get_flyboy_attributes(msg):
    """ Get needed attributed from a Crossref or DataCite record
        Keyword arguments:
          msg: Crossref or DataCite record
        Returns:
          title: article title
          author: article first author
          date: publication year
    """
    title = author = None
    date = DL.get_publishing_date(msg)
    if 'DOI' in msg:
        # Crossref
        if 'title' in msg:
            title = msg['title'][0]
        if 'author' in msg:
            author = msg['author'][0]['family']
        date = date.split('-')[0] if '-' in date else date
    else:
        # DataCite
        if 'titles' in msg:
            title = msg['titles'][0]['title']
        if 'creators' in msg and 'familyName' in msg['creators'][0]:
            author = msg['creators'][0]['familyName']
        if 'publicationYear' in msg:
            date = str(msg['publicationYear'])
        else:
            date = date.split('-')[0] if '-' in date else date
    return title, author, date


def update_flyboy(persist):
    """ Update FlyBoy for a single DOI
        Keyword arguments:
          persist: persist dict
        Returns:
          None
    """
    for doi, val in persist.items():
        title, author, date = get_flyboy_attributes(val)
        if not title:
            LOGGER.error("Missing title for %s", doi)
        if not author:
            LOGGER.error("Missing author for %s (%s)", doi, title)
        LOGGER.debug("%s: %s (%s, %s)", doi, title, author, date)
        COUNT['flyboy'] += 1
        title = unidecode(title)
        LOGGER.debug(WRITE['doi'], doi, title, author, date, title, author, date)
        if ARG.WRITE:
            try:
                DB['flyboy']['cursor'].execute(WRITE['doi'], (doi, title, author, date,
                                                              title, author, date))
            except MySQLdb.Error as err:
                terminate_program(err)


def perform_backcheck(cdict):
    """ Find and delete records that are in FlyBoy that aren't in our config
        Keyword arguments:
          cdict: dict of DOIs in config
        Returns:
          None
    """
    try:
        DB['flyboy']['cursor'].execute(READ['dois'])
    except MySQLdb.Error as err:
        terminate_program(err)
    rows = DB['flyboy']['cursor'].fetchall()
    for row in tqdm(rows, desc='Backcheck'):
        COUNT['foundfb'] += 1
        if row['doi'] not in cdict:
            LOGGER.warning(WRITE['delete_doi'], (row['doi']))
            if ARG.WRITE:
                try:
                    DB['flyboy']['cursor'].execute(WRITE['delete_doi'], (row['doi'],))
                except MySQLdb.Error as err:
                    terminate_program(err)
            COUNT['delete'] += 1


def update_config_database(persist):
    """ Update the configuration database
        Keyword arguments:
          persist: dict of DOIs to update
        Returns:
          None
    """
    if not ARG.WRITE:
        return
    for key, val in tqdm(persist.items(), desc='Update config'):
        LOGGER.debug(f"Updating {key} in config database")
        resp = call_responder('config', f"importjson/{CKEY[ARG.TARGET]}/{key}",
                              {"config": json.dumps(val)})
        if resp.status_code != 200:
            LOGGER.error(resp.json()['rest']['message'])
        else:
            rest = resp.json()
            if 'inserted' in rest['rest']:
                COUNT['insert'] += rest['rest']['inserted']
            elif 'updated' in rest['rest']:
                COUNT['update'] += rest['rest']['updated']


def get_tags_and_projects(authors, rec):
    ''' Find tags for a DOI using the authors from the orcid collection and the DOI record
        Keyword arguments:
          authors: list of detailed authors
          rec: DOI record
        Returns:
          List of tags
    '''
    new_tags = []
    for auth in authors:
        # Add Lab for the Group Leader
        if 'group' in auth and auth['group'] not in new_tags:
            new_tags.append(auth['group'])
        # Add tags for default departments and projects
        if 'tags' in auth:
            for dtag in DEFAULT_TAGS:
                if dtag in auth['tags'] and dtag not in new_tags:
                    new_tags.append(dtag)
        # Add project tags for names that are in the project_map collection
        if 'name' in auth:
            if auth['name'] not in PROJECT:
                LOGGER.warning(f"Project {auth['name']} is not defined")
            elif PROJECT[auth['name']] and auth['name'] not in new_tags:
                new_tags.append(PROJECT[auth['name']])
    field, _ = get_field(rec)
    if field in rec:
        for auth in rec[field]:
            if 'name' not in auth:
                continue
            if ',' in auth['name']:
                fullname = ' '.join(re.split(r'\s*,\s*', auth['name'])[::-1])
            else:
                fullname = auth['name']
            if fullname not in PROJECT:
                continue
            display = rec['doi'] if 'doi' in rec else rec
            LOGGER.warning(f"Found {fullname} -> {PROJECT[fullname]} in author name " \
                           + f"for {display}")
            if PROJECT[fullname] and fullname not in new_tags:
                new_tags.append(PROJECT[fullname])
    return new_tags


def persist_author(key, authors, persist):
    ''' Add authors to be persisted
        Keyword arguments:
          key: DOI
          authors: list of detailed authors
          persist: dict keyed by DOI with value of the Crossref/DataCite record
        Returns:
          None
    '''
    # Update jrc_author
    jrc_author = []
    alumni = []
    for auth in authors:
        if auth['janelian'] and 'employeeId' in auth and auth['employeeId']:
            jrc_author.append(auth['employeeId'])
        elif 'alumni' in auth and auth['alumni']:
            alumni.append(f"{auth['given'][0]} {auth['family'][0]} is alumni")
    if jrc_author:
        LOGGER.debug(f"Added jrc_author {jrc_author} to {key}")
        persist[key]['jrc_author'] = jrc_author
    elif alumni:
        LOGGER.warning(f"No Janelia authors for {key} (alumni)")
        ALUMNI[key] = json.dumps(authors, indent=2 ,default=str)
    else:
        LOGGER.warning(f"No Janelia authors for {key}")
        if authors:
            NO_AUTHOR[key] = json.dumps(authors, indent=2 ,default=str)


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


def add_pmid(key, persist):
    ''' Add the PMID to a DOI record
        Keyword arguments:
          key: DOI
          persist: dict keyed by DOI with value of the Crossref/DataCite record
        Returns:
          None
    '''
    try:
        pmid = JRC.get_pmid(key)
    except JRC.PMIDNotFound:
        return
    except Exception as err:
        terminate_program(err)
    if pmid:
        persist[key]['jrc_pmid'] = pmid


def get_tags(persist, rec):
    ''' Get tags and names from a DOI record
        Keyword arguments:
          persist: dict keyed by DOI with value of the Crossref/DataCite record
          rec: Crossref/DataCite record
        Returns:
          List of tags
    '''
    tags = []
    if 'jrc_tag' in persist:
        # We already have jrc_tag in the record
        tags.extend(persist['jrc_tag'])
    elif rec and 'jrc_tag' in rec:
        # This will be a new jrc_tag field
        tags.extend(rec['jrc_tag'])
    return tags


def add_tags(persist):
    ''' Add tags to DOI records that will be persisted (jrc_author, jrc_tag)
        Keyword arguments:
          persist: dict keyed by DOI with value of the Crossref/DataCite record
        Returns:
          None
    '''
    coll = DB['dis'].orcid
    for key, val in tqdm(persist.items(), desc='Add jrc_author and jrc_tag'):
        try:
            rec = DB['dis'].dois.find_one({"doi": key})
        except Exception as err:
            terminate_program(err)
        # Try to add the PMID if we don't have it
        if 'jrc_pmid' not in val:
            add_pmid(key, persist)
        try:
            authors = DL.get_author_details(val, coll)
        except Exception as err:
            terminate_program(err)
        if not authors:
            continue
        # Update jrc_tag using the authors
        new_tags = get_tags_and_projects(authors, val)
        tags = get_tags(persist, rec)
        names = [etag['name'] for etag in tags]
        # Add new tags to the record
        for tag in new_tags:
            if tag not in names:
                code = get_suporg_code(tag)
                tagtype = 'suporg' if code else 'affiliation'
                tags.append({"name": tag, "code": code, "type": tagtype})
        if tags:
            LOGGER.debug(f"Added jrc_tag {list(t['name'] for t in tags)} to {key}")
            persist[key]['jrc_tag'] = tags
        if rec and 'jrc_newsletter' in rec:
            LOGGER.warning(f"Skipping jrc_author update for {key}")
        else:
            persist_author(key, authors, persist)


def get_field(rec):
    ''' Get the field name for the authors
        Keyword arguments:
          rec: Crossref/DataCite record
          datacite: True if DataCite, else False
        Returns:
          Field name and True if DataCite
    '''
    if 'jrc_obtained_from' in rec and rec['jrc_obtained_from'] == "DataCite":
        return 'creators', True
    return 'author', False


def add_first_last_authors(rec):
    ''' Add first and last authors to record
        Keyword arguments:
          rec: Crossref/DataCite record
        Returns:
          None
    '''
    first = []
    field, datacite = get_field(rec)
    if field in rec:
        if not datacite:
            # First author(s)
            for auth in rec[field]:
                if 'sequence' in auth and auth['sequence'] == 'additional':
                    break
                if not('given' in auth and 'family' in auth):
                    LOGGER.warning(f"Missing author name in {rec['doi']} author {auth}")
                    break
                try:
                    janelian = DL.is_janelia_author(auth, DB['dis'].orcid, PROJECT)
                except Exception as err:
                    LOGGER.error(auth)
                    LOGGER.error("Could not process author in for DL.is_janelia_author" \
                                 + rec['doi'])
                    terminate_program(err)
                if janelian:
                    first.append(janelian)
        else:
            janelian = DL.is_janelia_author(rec[field][0], DB['dis'].orcid, PROJECT)
            if janelian:
                first.append(janelian)
        okay = True
        if not datacite:
            if not('given' in rec[field][-1] and 'family' in rec[field][-1]):
                okay = False
        elif not('givenName' in rec[field][-1] and 'familyName' in rec[field][-1]):
            okay = False
        if okay:
            janelian = DL.is_janelia_author(rec[field][-1], DB['dis'].orcid, PROJECT)
            if janelian:
                rec["jrc_last_author"] = janelian
        else:
            LOGGER.warning(f"Missing author name in {rec['doi']} author {rec[field][-1]}")
    if first:
        rec["jrc_first_author"] = first
    if (not first) and ('jrc_last_author' not in rec):
        return
    first = []
    det = DL.get_author_details(rec, DB['dis']['orcid'])
    for auth in det:
        if auth['janelian'] and 'employeeId' in auth and 'is_first' in auth:
            first.append(auth['employeeId'])
        if auth['janelian'] and 'employeeId' in auth and 'is_last' in auth:
            rec["jrc_last_id"] = auth['employeeId']
    if first:
        rec["jrc_first_id"] = first


def update_mongodb(persist):
    ''' Persist DOI records in MongoDB
        Keyword arguments:
          persist: dict keyed by DOI with value of the Crossref/DataCite record
        Returns:
          None
    '''
    coll = DB['dis'].dois
    for key, val in tqdm(persist.items(), desc='Update DIS Mongo'):
        val['doi'] = key
        # Publishing date and journal
        val['jrc_publishing_date'] = DL.get_publishing_date(val)
        tmp = DL.get_journal(val, name_only=True)
        if tmp:
            val['jrc_journal'] = tmp
        # First/last authors
        add_first_last_authors(val)
        for aname in ('jrc_first_author', 'jrc_first_id', 'jrc_last_author', 'jrc_last_id'):
            if aname in val:
                LOGGER.debug(f"Added {aname} {val[aname]} to {key}")
        # Insert/update timestamps
        if key not in EXISTING:
            val['jrc_inserted'] = datetime.today().replace(microsecond=0)
        val['jrc_updated'] = datetime.today().replace(microsecond=0)
        # LOGGER.debug(val)
        if ARG.WRITE:
            if ARG.DOI or ARG.FILE:
                val['jrc_load_source'] = "Manual"
                uname = JRC.get_user_name()
                if uname and uname != "root":
                    val['jrc_loaded_by'] = uname
            else:
                val['jrc_load_source'] = "Sync"
            coll.update_one({"doi": key}, {"$set": val}, upsert=True)
            if key in TO_BE_PROCESSED:
                try:
                    DB['dis'].dois_to_process.delete_one({"doi": key})
                except Exception as err:
                    LOGGER.error(f"Could not delete {key} from dois_to_process: {err}")
        if key in EXISTING:
            COUNT['update'] += 1
            if key not in UPDATED:
                UPDATED[key] = "Unknown"
        else:
            COUNT['insert'] += 1
            INSERTED[key] = DL.get_publishing_date(val)


def update_dois(specified, persist):
    """ Persist new or updated DOIs
        Keyword arguments:
          specified: distinct input DOIs
          persist: DOIs that need persisting
        Returns:
          None
    """
    if ARG.TARGET == 'flyboy':
        update_flyboy(persist)
        if not ARG.DOI and not ARG.FILE:
            perform_backcheck(specified)
        update_config_database(persist)
    elif ARG.TARGET == 'dis':
        add_tags(persist)
        update_mongodb(persist)


def persist_if_updated(doi, msg, persist):
    """ Decide if we need to persist a DOI
        Keyword arguments:
          doi: DOI
          msg: message from DOI record
          persist: dict of DOIs to persist
        Returns:
          None
    """
    if DL.is_datacite(doi):
        # DataCite
        if datacite_needs_update(doi, msg['data']):
            persist[doi] = msg['data']['attributes']
            persist[doi]['jrc_obtained_from'] = 'DataCite'
        COUNT['foundd'] += 1
    else:
        # Crossref
        if crossref_needs_update(doi, msg['message']):
            persist[doi] = msg['message']
            persist[doi]['jrc_obtained_from'] = 'Crossref'
        COUNT['foundc'] += 1


def process_dois():
    """ Process a list of DOIs
        Keyword arguments:
          None
        Returns:
          None
    """
    LOGGER.info(f"Started run (version {__version__})")
    rows = get_dois()
    if not rows:
        terminate_program("No DOIs were found")
    specified = {} # Dict of distinct DOIs received as input (value is True)
    persist = {} # DOIs that will be persisted in a database (value is record)
    for odoi in tqdm(rows['dois'], desc='DOIs'):
        if '//' in odoi:
            terminate_program(f"Invalid DOI: {odoi}")
        doi = odoi if ARG.TARGET == 'flyboy' else odoi.lower().strip()
        COUNT['found'] += 1
        if doi in specified:
            continue
        specified[doi] = True
        if ARG.INSERT:
            if doi in EXISTING:
                continue
            if DL.is_datacite(doi):
                msg = get_doi_record(doi)
                if msg:
                    if too_old(doi, msg['data']['attributes']):
                        continue
                    persist[doi] = msg['data']['attributes']
                    persist[doi]['jrc_obtained_from'] = 'DataCite'
            else:
                msg = get_doi_record(doi)
                if msg:
                    if too_old(doi, msg['message']):
                        continue
                    persist[doi] = msg['message']
                    persist[doi]['jrc_obtained_from'] = 'Crossref'
            continue
        msg = get_doi_record(doi)
        if not msg:
            continue
        persist_if_updated(doi, msg, persist)
    update_dois(specified, persist)


def generate_emails():
    ''' Generate and send an email
        Keyword arguments:
          None
        Returns:
          None
    '''
    msg = JRC.get_run_data(__file__, __version__)
    if ARG.SOURCE:
        msg += f"DOIs passed in from {ARG.SOURCE}\n"
    msg += f"The following DOIs were inserted into the {ARG.MANIFOLD} MongoDB DIS database:"
    for doi in INSERTED:
        msg += f"\n{doi}"
    try:
        LOGGER.info(f"Sending email to {DISCONFIG['receivers']}")
        JRC.send_email(msg, DISCONFIG['sender'], DISCONFIG['developer'] \
                       if ARG.MANIFOLD == 'dev' else DISCONFIG['receivers'],
                       "New DOIs")
    except Exception as err:
        LOGGER.error(err)
    if not TO_BE_PROCESSED:
        return
    msg = JRC.get_run_data(__file__, __version__)

    msg += "The following DOIs from a previous weekly cycle have been added to the database. " \
           + "Metadata should be updated as soon as possible."
    for doi in TO_BE_PROCESSED:
        msg += f"\n{doi}"
    try:
        LOGGER.info(f"Sending email to {DISCONFIG['librarian']}")
        JRC.send_email(msg, DISCONFIG['sender'], DISCONFIG['developer'] if ARG.MANIFOLD == 'dev' \
                                                                        else DISCONFIG['librarian'],
                       "Action needed: new DOIs")
    except Exception as err:
        LOGGER.error(err)


def post_activities():
    """ Write output files and report on program operations
        Keyword arguments:
          None
        Returns:
          None
    """
    if ARG.OUTPUT:
        # Write files
        timestamp = strftime("%Y%m%dT%H%M%S")
        for ftype in ('INSERTED', 'UPDATED', 'CROSSREF', 'DATACITE',
                      'CROSSREF_CALL', 'DATACITE_CALL', 'MISSING', 'NO_AUTHOR', 'ALUMNI'):
            if not globals()[ftype]:
                continue
            fname = f"doi_{ftype.lower()}_{timestamp}.txt"
            with open(fname, 'w', encoding='ascii') as outstream:
                for key, val in globals()[ftype].items():
                    if ftype in ('INSERTED', 'NO_AUTHOR', 'ALUMNI', 'UPDATED'):
                        outstream.write(f"{key}\t{val}\n")
                    else:
                        outstream.write(f"{key}\n")
    # Report
    if ARG.SOURCE:
        print(f"Source:                          {ARG.SOURCE}")
    if ARG.TARGET == 'dis' and (not ARG.DOI and not ARG.FILE):
        print(f"DOIs fetched from Crossref:      {COUNT['crossref']:,}")
        print(f"DOIs fetched from DataCite:      {COUNT['datacite']:,}")
    print(f"DOIs specified:                  {COUNT['found']:,}")
    print(f"DOIs found in Crossref:          {COUNT['foundc']:,}")
    print(f"DOIs found in DataCite:          {COUNT['foundd']:,}")
    print(f"DOIs with no author:             {COUNT['noauthor']:,}")
    print(f"DOIs not found:                  {COUNT['notfound']:,}")
    print(f"DOIs not needing updates:        {COUNT['noupdate']:,}")
    if ARG.TARGET == 'flyboy':
        print(f"DOIs found in FlyBoy:            {COUNT['foundfb']:,}")
        print(f"DOIs inserted/updated in FlyBoy: {COUNT['flyboy']:,}")
        print(f"DOIs deleted from FlyBoy:        {COUNT['delete']:,}")
    print(f"DOIs inserted:                   {COUNT['insert']:,}")
    print(f"DOIs updated:                    {COUNT['update']:,}")
    print(f"Elapsed time: {datetime.now() - START_TIME}")
    print(f"DOI calls to Crossref: {len(CROSSREF_CALL):,}")
    print(f"DOI calls to DataCite: {len(DATACITE_CALL):,}")
    # Email
    if INSERTED and ARG.WRITE:
        generate_emails()
    if not ARG.WRITE:
        LOGGER.warning("Dry run successful, no updates were made")

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Sync DOIs")
    PARSER.add_argument('--doi', dest='DOI', action='store',
                        help='Single DOI to process')
    PARSER.add_argument('--source', dest='SOURCE', action='store',
                        help='Source of DOIs (arXiv, figshae, etc.)')
    PARSER.add_argument('--target', dest='TARGET', action='store',
                        default='dis', choices=['flyboy', 'dis'],
                        help='Target system (flyboy or dis)')
    PARSER.add_argument('--file', dest='FILE', action='store',
                        type=argparse.FileType("r", encoding="ascii"),
                        help='File of DOIs to process')
    PARSER.add_argument('--pipe', dest='PIPE', action='store_true',
                        default=False, help='Accepted input from STDIN')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, prod)')
    PARSER.add_argument('--insert', dest='INSERT', action='store_true',
                        default=False, help='Only look for new records')
    PARSER.add_argument('--force', dest='FORCE', action='store_true',
                        default=False, help='Force update')
    PARSER.add_argument('--output', dest='OUTPUT', action='store_true',
                        default=False, help='Produce output files')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Write to database/config system')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    CONFIG = configparser.ConfigParser()
    CONFIG.read('config.ini')
    initialize_program()
    DISCONFIG = JRC.simplenamespace_to_dict(JRC.get_config("dis"))
    REST = JRC.get_config("rest_services")
    START_TIME = datetime.now()
    if ARG.TARGET == 'flyboy':
        EXISTING = JRC.simplenamespace_to_dict(JRC.get_config(CKEY[ARG.TARGET]))
    else:
        EXISTING = get_dis_dois_from_mongo()
        try:
            PROJECT = DL.get_project_map(DB['dis'].project_map)
        except Exception as gerr:
            terminate_program(gerr)
    process_dois()
    post_activities()
    terminate_program()
