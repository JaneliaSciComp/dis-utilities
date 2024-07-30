""" update_dois.py
    Synchronize DOI information from an input source to databases.
    If a single DOI or file of DOIs is specified, these are updated in FlyBoy/config or DIS MongoDB.
    Otherwise, DOIs are synced according to target:
    - flyboy: FLYF2 to FlyBoy and the config system
    - dis: FLYF2, Crossref, DataCite, ALPS releases, and EM datasets to DIS MongoDB.
"""

__version__ = '4.0.0'

import argparse
import configparser
from datetime import datetime
import json
from operator import attrgetter
import os
import re
import sys
from time import sleep, strftime
from unidecode import unidecode
import MySQLdb
import requests
from tqdm import tqdm
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL


# pylint: disable=broad-exception-caught,broad-exception-raised,logging-fstring-interpolation

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
CROSSREF = {}
DATACITE = {}
CROSSREF_CALL = {}
DATACITE_CALL = {}
INSERTED = {}
UPDATED = {}
MISSING = {}
MAX_CROSSREF_TRIES = 3
# Email
SENDER = 'svirskasr@hhmi.org'
RECEIVERS = ['scarlettv@hhmi.org', 'svirskasr@hhmi.org']
# General
PROJECT = {}
DEFAULT_TAGS = ['Janelia Experimental Technology (jET)', 'Scientific Computing Software']
COUNT = {'crossref': 0, 'datacite': 0, 'duplicate': 0, 'found': 0, 'foundc': 0, 'foundd': 0,
         'notfound': 0, 'noupdate': 0, 'noauthor': 0,
         'insert': 0, 'update': 0, 'delete': 0, 'foundfb': 0, 'flyboy': 0}

def terminate_program(msg=None):
    ''' Terminate the program gracefully
        Keyword arguments:
          msg: error message
        Returns:
          None
    '''
    if msg:
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
        rows = DB['dis'].project_map.find({})
    except Exception as err:
        terminate_program(err)
    for row in rows:
        PROJECT[row['name']] = row['project']


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
                COUNT['duplicate'] += 1
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
                COUNT['duplicate'] += 1
                continue
            dlist.append(doi)
            DATACITE[doi] = {"data": {"attributes": rec['attributes']}}
        if 'links' in recs and 'next' in recs['links']:
            suffix = recs['links']['next'].replace('https://api.datacite.org/dois', '')
        else:
            complete = True
    LOGGER.info(f"Got {len(dlist):,} DOIs from DataCite in {parts} part(s) for {query}")
    return dlist


def get_dois_for_dis(flycore):
    ''' Get a list of DOIs to process for an update of the DIS database. Sources are:
        - DOIs with an affiliation of Janelia from Crossref
        - All Janelia-prefixed DOIs from DataCite
        - DOIs with an affiliation of Janelia from DataCite
        - DOIs in use by FLYF2
        - DOIs associated with ALPs releases
        - DOIs associated with FlyEM datasets
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
    for doi in flycore['dois']:
        if doi not in dlist and 'in prep' not in doi:
            dlist.append(doi)
    # ALPS releases
    releases = JRC.simplenamespace_to_dict(JRC.get_config('releases'))
    cnt = 0
    for val in releases.values():
        if 'doi' in val:
            for dtype in ('dataset', 'preprint', 'publication'):
                if dtype in val['doi'] and val['doi'][dtype] not in dlist:
                    cnt += 1
                    dlist.append(val['doi'][dtype])
    LOGGER.info(f"Got {cnt:,} DOIs from ALPS releases")
    # EM datasets
    emdois = JRC.simplenamespace_to_dict(JRC.get_config('em_dois'))
    cnt = 0
    for val in emdois.values():
        if val:
            cnt += 1
            dlist.append(val)
    LOGGER.info(f"Got {cnt:,} DOIs from EM releases")
    # Previously inserted
    for doi in EXISTING:
        if doi not in dlist:
            dlist.append(doi)
    return {"dois": dlist}


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
        return {"dois": ARG.FILE.read().splitlines()}
    flycore = call_responder('flycore', '?request=doilist')
    LOGGER.info(f"Got {len(flycore['dois']):,} DOIs from FLYF2")
    if ARG.TARGET == 'dis':
        return get_dois_for_dis(flycore)
    # Default is to pull from FlyCore
    return flycore


def call_crossref(doi):
    """ Get DOI information from crossref
        Keyword arguments:
        doi: DOI
    """
    try:
        req = JRC.call_crossref(doi)
    except requests.exceptions.RequestException as err:
        terminate_program(err)
    if req:
        return req
    COUNT['notfound'] += 1
    MISSING[f"Could not find {doi} in Crossref"] = True
    raise Exception(f"Could not find {doi} in Crossref")


def call_crossref_with_retry(doi):
    """ Looping function for call_crossref
        Keyword arguments:
          doi: DOI
        Returns:
          msg: response from crossref.org
    """
    attempt = MAX_CROSSREF_TRIES
    msg = None
    while attempt:
        try:
            msg = call_crossref(doi)
        except Exception as err:
            raise Exception(err) from err
        if 'title' in msg['message']:
            if 'author' in msg['message']:
                break
            MISSING[f"No author for {doi}"] = True
            LOGGER.warning(f"No author for {doi}")
            COUNT['noauthor'] += 1
            return None
        LOGGER.warning(f"No title for {doi}")
        MISSING[f"No title for {doi}"] = True
        attempt -= 1
        LOGGER.warning(f"Missing data from crossref.org for {doi}: retrying ({attempt})")
        sleep(0.5)
    return msg


def call_datacite(doi):
    """ Get record from DataCite
        Keyword arguments:
          doi: DOI
        Returns:
          rec: response from crossref.org
    """
    rec = DATACITE[doi] if doi in DATACITE else JRC.call_datacite(doi)
    if rec:
        return rec
    COUNT['notfound'] += 1
    MISSING[f"Could not find {doi} in DataCite"] = True
    raise Exception(f"Could not find {doi} in DataCite")


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
                msg = call_datacite(doi)
                DATACITE_CALL[doi] = True
            except Exception as err:
                LOGGER.warning(err)
    else:
        # Crossref
        if doi in CROSSREF:
            msg = CROSSREF[doi]
        else:
            try:
                msg = call_crossref_with_retry(doi)
                CROSSREF_CALL[doi] = True
            except Exception as err:
                LOGGER.warning(err)
    return msg


def convert_timestamp(stamp):
    """ Convert a Crossref or DataCite stamp to a standard format
        Keyword arguments:
          stamp: timestamp
        Returns:
          Converted timestamp
    """
    return re.sub(r'\.\d+Z', 'Z', stamp)


def crossref_needs_update(doi, msg):
    """ Determine if a Crossref DOI needs updating on our system
        Keyword arguments:
          doi: DOI
          msg: record from Crossref
        Returns:
          True or False
    """
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


def get_tags(authors):
    ''' Find tags for a DOI using the authors
        Keyword arguments:
          authors: list of detailed authors
        Returns:
          List of tags
    '''
    new_tags = []
    for auth in authors:
        if 'group' in auth and auth['group'] not in new_tags:
            new_tags.append(auth['group'])
        if 'tags' in auth:
            for dtag in DEFAULT_TAGS:
                if dtag in auth['tags'] and dtag not in new_tags:
                    new_tags.append(dtag)
        if 'name' in auth:
            if auth['name'] not in PROJECT:
                LOGGER.warning(f"Project {auth['name']} is not defined")
            elif PROJECT[auth['name']] and auth['name'] not in new_tags:
                new_tags.append(PROJECT[auth['name']])
    return new_tags


def persist_author(key, authors, tags, persist):
    ''' Add authors to be persisted
        Keyword arguments:
          persist: dict keyed by DOI with value of the Crossref/DataCite record
        Returns:
          None
    '''
    if tags:
        LOGGER.info(f"Added jrc_tag {tags} to {key}")
        persist[key]['jrc_tag'] = tags
    # Update jrc_author
    jrc_author = []
    for auth in authors:
        if auth['janelian'] and 'employeeId' in auth and auth['employeeId']:
            jrc_author.append(auth['employeeId'])
    if jrc_author:
        LOGGER.info(f"Added jrc_author {jrc_author} to {key}")
        persist[key]['jrc_author'] = jrc_author


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
            authors = DL.get_author_details(val, coll)
        except Exception as err:
            terminate_program(err)
        if not authors:
            continue
        # Update jrc_tag
        new_tags = get_tags(authors)
        tags = []
        if 'jrc_tag' in persist:
            tags.extend(persist['jrc_tag'])
        else:
            try:
                rec = DB['dis'].dois.find_one({"doi": key})
            except Exception as err:
                terminate_program(err)
            if rec and 'jrc_tag' in rec:
                tags.extend(rec['jrc_tag'])
        for tag in new_tags:
            if tag not in tags:
                tags.append(tag)
        persist_author(key, authors, tags, persist)


def add_first_last_authors(rec):
    ''' Add first and last authors to record
        Keyword arguments:
          rec: Crossref/DataCite record
        Returns:
          None
    '''
    first = []
    if 'jrc_obtained_from' in rec and rec['jrc_obtained_from'] == "DataCite":
        field = 'creators'
        datacite = True
    else:
        field = 'author'
        datacite = False
    if field in rec:
        if not datacite:
            for auth in rec[field]:
                if 'sequence' in auth and auth['sequence'] == 'additional':
                    break
                try:
                    janelian = DL.is_janelia_author(auth, DB['dis'].orcid, PROJECT)
                except Exception as err:
                    LOGGER.error(f"Could not process {rec['doi']}")
                    terminate_program(err)
                if janelian:
                    first.append(janelian)
        else:
            janelian = DL.is_janelia_author(rec[field][0], DB['dis'].orcid, PROJECT)
            if janelian:
                first.append(janelian)
        janelian = DL.is_janelia_author(rec[field][-1], DB['dis'].orcid, PROJECT)
        if janelian:
            rec["jrc_last_author"] = janelian
    if first:
        rec["jrc_first_author"] = first


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
        # Publishing date
        val['jrc_publishing_date'] = DL.get_publishing_date(val)
        # First/last authors
        add_first_last_authors(val)
        for aname in ('jrc_first_author', 'jrc_last_author'):
            if aname in val:
                LOGGER.info(f"Added {aname} {val[aname]} to {key}")
        # Insert/update timestamps
        if key not in EXISTING:
            val['jrc_inserted'] = datetime.today().replace(microsecond=0)
        val['jrc_updated'] = datetime.today().replace(microsecond=0)
        LOGGER.debug(val)
        if ARG.WRITE:
            if ARG.DOI or ARG.FILE:
                val['jrc_load_source'] = "Manual"
                uname = JRC.get_user_name()
                if uname and uname != "root":
                    val['jrc_loaded_by'] = uname
            else:
                val['jrc_load_source'] = "Sync"
            coll.update_one({"doi": key}, {"$set": val}, upsert=True)
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
        doi = odoi if ARG.TARGET == 'flyboy' else odoi.lower()
        COUNT['found'] += 1
        if doi in specified:
            COUNT['duplicate'] += 1
            LOGGER.warning(f"{doi} appears in input more than once")
            continue
        specified[doi] = True
        if ARG.INSERT:
            if doi in EXISTING:
                continue
            if DL.is_datacite(doi):
                msg = get_doi_record(doi)
                if msg:
                    persist[doi] = msg['data']['attributes']
                    persist[doi]['jrc_obtained_from'] = 'DataCite'
            else:
                msg = get_doi_record(doi)
                if msg:
                    persist[doi] = msg['message']
                    persist[doi]['jrc_obtained_from'] = 'Crossref'
            continue
        msg = get_doi_record(doi)
        if not msg:
            continue
        persist_if_updated(doi, msg, persist)
    update_dois(specified, persist)


def generate_email():
    ''' Generate and send an email
        Keyword arguments:
          None
        Returns:
          None
    '''
    msg = JRC.get_run_data(__file__, __version__)
    msg += f"The following DOIs were inserted into the {ARG.MANIFOLD} MongoDB DIS database:"
    for doi in INSERTED:
        msg += f"\n{doi}"
    try:
        LOGGER.info(f"Sending email to {RECEIVERS}")
        JRC.send_email(msg, SENDER, ['svirskasr@hhmi.org'] if ARG.MANIFOLD == 'dev' else RECEIVERS,
                       "New DOIs")
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
                      'CROSSREF_CALL', 'DATACITE_CALL', 'MISSING'):
            if not globals()[ftype]:
                continue
            fname = f"doi_{ftype.lower()}_{timestamp}.txt"
            with open(fname, 'w', encoding='ascii') as outstream:
                for key, val in globals()[ftype].items():
                    if ftype in ('INSERTED', 'UPDATED'):
                        outstream.write(f"{key}\t{val}\n")
                    else:
                        outstream.write(f"{key}\n")
    # Report
    if ARG.TARGET == 'dis' and (not ARG.DOI and not ARG.FILE):
        print(f"DOIs fetched from Crossref:      {COUNT['crossref']:,}")
        print(f"DOIs fetched from DataCite:      {COUNT['datacite']:,}")
    print(f"DOIs specified:                  {COUNT['found']:,}")
    print(f"DOIs found in Crossref:          {COUNT['foundc']:,}")
    print(f"DOIs found in DataCite:          {COUNT['foundd']:,}")
    print(f"DOIs with no author:             {COUNT['noauthor']:,}")
    print(f"DOIs not found:                  {COUNT['notfound']:,}")
    print(f"Duplicate DOIs:                  {COUNT['duplicate']:,}")
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
        generate_email()
    if not ARG.WRITE:
        LOGGER.warning("Dry run successful, no updates were made")

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Sync DOIs")
    PARSER.add_argument('--doi', dest='DOI', action='store',
                        help='Single DOI to process')
    PARSER.add_argument('--target', dest='TARGET', action='store',
                        default='dis', choices=['flyboy', 'dis'],
                        help='Target system (flyboy or dis)')
    PARSER.add_argument('--file', dest='FILE', action='store',
                        type=argparse.FileType("r", encoding="ascii"),
                        help='File of DOIs to process')
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
