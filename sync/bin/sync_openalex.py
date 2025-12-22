""" sync_openalex.py
    Sync work data from OpenAlex. DOIs will almost always make it into the local database before
    they're present in OpenAlex.
    Data brought in from OpenAlex:
      open_access.is_oa -> jrc_is_oa
      open_access.oa_status -> jrc_oa_status
      primary_location.license -> jrc_license
    This program will also look for DOIs with a "closed" Open Access status to override. If
    the OA status is "closed" and the DOI has a fulltext URL, the OA status will be set to
    "hybrid" and jrc_is_oa will be set to True. The former status will be saved as
    jrc_former_status.
"""

__version__ = '2.1.0'

import argparse
import collections
import json
from operator import attrgetter
import sys
import time
import traceback
from tqdm import tqdm
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL

# pylint: disable=broad-exception-caught,logging-fstring-interpolation,logging-not-lazy

ARG = DISCONFIG = LOGGER = None
DB = {}
LICENSE = {}
OUTPUT = []
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
    try:
        rows = DB['dis'].cvterm.find({'cv': 'license_mapping'})
    except Exception as err:
        terminate_program(err)
    for row in rows:
        LICENSE[row['name']] = row['display']


def get_dois():
    """ Get a list of DOIs to process
        Keyword arguments:
          None
        Returns:
          List of DOIs
    """
    dois = []
    if ARG.DOI:
        dois.append(ARG.DOI.lower().strip())
    elif ARG.FILE:
        try:
            with open(ARG.FILE, "r", encoding="ascii") as instream:
                for doi in instream.read().splitlines():
                    dois.append(doi.lower().strip())
        except Exception as err:
            LOGGER.error(f"Could not process {ARG.FILE}")
            terminate_program(err)
    return dois


def get_pmc_license(pmcid):
    """ Get the license for a PMCID
        Keyword arguments:
          pmcid: PMCID
        Returns:
          License
    """
    data = DL.get_doi_record(pmcid, source='pmc')
    if not data or 'OAI-PMH' not in data or 'GetRecord' not in data['OAI-PMH'] \
       or 'record' not in data['OAI-PMH']['GetRecord'] \
       or 'metadata' not in data['OAI-PMH']['GetRecord']['record'] \
       or 'article' not in data['OAI-PMH']['GetRecord']['record']['metadata'] \
       or 'front' not in data['OAI-PMH']['GetRecord']['record']['metadata']['article']:
        return None
    front = data['OAI-PMH']['GetRecord']['record']['metadata']['article']['front']
    if 'article-meta' not in front or 'custom-meta-group' not in front['article-meta'] \
       or 'custom-meta' not in front['article-meta']['custom-meta-group'] \
        or not front['article-meta']['custom-meta-group']['custom-meta']:
        return None
    for custom_meta in front['article-meta']['custom-meta-group']['custom-meta']:
        if custom_meta['meta-name'] == 'license':
            return custom_meta['meta-value'].replace(" ", "-").lower()
    return None


def update_datacite_license(row):
    """ Update jrc_license from DataCite rightsList
        Keyword arguments:
          row: row to update from dois collection
        Returns:
          None
    """
    if 'jrc_license' in row and row['jrc_license']:
        return
    payload = {}
    if 'rightsList' in row and row['rightsList']:
        for right in row['rightsList']:
            if 'rightsIdentifier' in right and right['rightsIdentifier'] in LICENSE:
                payload['jrc_license'] = LICENSE[right['rightsIdentifier']]
                LOGGER.info(f"Using license (rightsIdentifier) {payload['jrc_license']} " \
                            + f"for {row['doi']}")
                break
            elif 'rights' in right and right['rights'] in LICENSE:
                payload['jrc_license'] = LICENSE[right['rights']]
                LOGGER.info(f"Using license (rights) {payload['jrc_license']} for {row['doi']}")
    if payload:
        write_record(row, payload)


def update_open_access(row):
    """ Update jrc_is_oa and jrc_oa_status
        Keyword arguments:
          row: row to update from dois collection
        Returns:
          None
    """
    payload = {}
    if 'jrc_obtained_from' in row and row['jrc_obtained_from'] == 'DataCite':
        update_datacite_license(row)
    time.sleep(.5)
    try:
        data = DL.get_doi_record(row['doi'], source='openalex')
    except Exception as err:
        terminate_program(err)
    if not data:
        if not ARG.SILENT:
            LOGGER.warning(f"{row['doi']} was not found in OpenAlex")
        COUNT["notfound"] += 1
        return
    try_pmc = True
    try:
        # Open Access
        if 'jrc_is_oa' not in row and 'open_access' in data and data['open_access']:
            payload['jrc_is_oa'] = bool(data['open_access']['is_oa'])
            payload['jrc_oa_status'] = data['open_access']['oa_status']
        # License
        if ('jrc_license' not in row or not row['jrc_license']) \
           and 'primary_location' in data and data['primary_location'] \
           and data['primary_location']['license'] \
           and data['primary_location']['license'] != "False":
            if data['primary_location']['license'] in LICENSE:
                payload['jrc_license'] = LICENSE[data['primary_location']['license']]
                LOGGER.info(f"Using license (primary_location) {payload['jrc_license']} " \
                            + f"for {row['doi']}")
            else:
                LOGGER.warning(f"Unknown license {data['primary_location']['license']} " \
                               + f"for {row['doi']}")
            try_pmc = False
        if ('jrc_license' not in payload or payload['jrc_license'] is None) and try_pmc \
           and 'jrc_pmc' in row:
            alt = get_pmc_license(row['jrc_pmc'])
            if alt:
                if alt in LICENSE:
                    payload['jrc_license'] = alt
                    LOGGER.info(f"Using PMC license for {row['doi']}: {alt}")
                else:
                    LOGGER.warning(f"Unknown PMC license {alt} for {row['doi']}")
        if not payload:
            return
    except Exception as err:
        LOGGER.error(f"Could not process {row['doi']}")
        terminate_program(err)
    write_record(row, payload)


def write_record(row, payload):
    """ Write record to database
        Keyword arguments:
          row: record to write
          payload: data to add/update
        Returns:
          None
    """
    if ARG.WRITE:
        try:
            DB['dis']['dois'].update_one({"doi": row['doi']}, {"$set": payload})
        except Exception as err:
            terminate_program(err)
    payload['doi'] = row['doi']
    OUTPUT.append(payload)
    COUNT["updated"] += 1


def override_oa_closed(row):
    """ Override OA closed status
        Keyword arguments:
          row: row to update
        Returns:
          None
    """
    payload = {'jrc_former_status': row['jrc_oa_status'],
               'jrc_is_oa': True}
    payload['jrc_oa_status'] = "hybrid"
    if ARG.WRITE:
        try:
            DB['dis']['dois'].update_one({"doi": row['doi']}, {"$set": payload})
        except Exception as err:
            terminate_program(err)
    payload['doi'] = row['doi']
    OUTPUT.append(payload)
    COUNT["updated"] += 1


def show_counts():
    """ Show the counts
        Keyword arguments:
          None
        Returns:
          None
    """
    msg = f"DOIs read:      {COUNT['dois']:,}\n"
    if COUNT['notfound']:
        msg += f"DOIs not found: {COUNT['notfound']:,}\n"
    if COUNT['updated']:
        msg += f"DOIs updated:   {COUNT['updated']:,}\n"
    return msg


def generate_email(counts):
    ''' Generate and send an email
        Keyword arguments:
          counts: counts message
        Returns:
          None
    '''
    msg = JRC.get_run_data(__file__, __version__) + "<br><br>" + counts.replace("\n", "<br>")
    try:
        email = DISCONFIG['developer'] if ARG.TEST else DISCONFIG['receivers']
        LOGGER.info(f"Sending email to {email}")
        opts = {'attachment': 'sync_openalex.json', 'mime': 'html'}
        JRC.send_email(msg, DISCONFIG['sender'], email, "OpenAlex OA/license sync", **opts)
    except Exception as err:
        print(str(err))
        traceback.print_exc()
        terminate_program(err)


def process_dois():
    """ Process a list of DOIs
        Keyword arguments:
          None
        Returns:
          None
    """
    cnt = 0
    rows = []
    dois = get_dois()
    if dois:
        for doi in dois:
            data = DL.get_doi_record(doi, coll=DB['dis']['dois'])
            rows.append(data)
        cnt = len(rows)
    else:
        payload = {"doi": {"$not": {"$regex": "janelia"}}}
        if ARG.NEW:
            payload["$and"] = [{"jrc_is_oa": {"$exists": False}},
                               {"jrc_license": {"$exists": False}}]
        else:
            payload["$or"] = [{"jrc_is_oa": {"$exists": False}},
                              {"jrc_license": {"$exists": False}}]
        try:
            cnt = DB['dis'].dois.count_documents(payload)
            rows = DB['dis'].dois.find(payload)
        except Exception as err:
            terminate_program(err)
    # Open Access / license data
    LOGGER.info(f"Found {cnt} DOI{'s' if cnt != 1 else ''} to process for OpenAlex")
    for row in tqdm(rows, total=cnt, desc="Add OpenAlex"):
        COUNT['dois'] += 1
        update_open_access(row)
    msg1 = show_counts()
    print(msg1)
    # Open Access status override
    COUNT['dois'] = COUNT["updated"] = COUNT["notfound"] = 0
    if dois:
        cnt = len(dois)
    else:
        rows = []
        payload = {"jrc_is_oa": {"$exists": True}, "jrc_oa_status": "closed",
                   "jrc_fulltext_url": {"$exists": True}
                  }
        try:
            cnt = DB['dis'].dois.count_documents(payload)
            rows = DB['dis'].dois.find(payload)
        except Exception as err:
            terminate_program(err)
    LOGGER.info(f"Found {cnt} DOI{'s' if cnt != 1 else ''} to process for OA status")
    for row in tqdm(rows, total=cnt, desc="Fix OA status"):
        if 'jrc_oa_status' not in row or not row['jrc_oa_status']:
            continue
        COUNT["dois"] += 1
        if row['jrc_oa_status'] == "closed" and row['jrc_fulltext_url']:
            override_oa_closed(row)
    msg2 = show_counts()
    print(msg2)
    if OUTPUT:
        LOGGER.info("Writing output to sync_openalex.json")
        with open('sync_openalex.json', 'w', encoding='utf-8') as fileout:
            json.dump(OUTPUT, fileout, indent=4)
        if ARG.TEST or ARG.WRITE:
            generate_email(f"Updating Open Access/license data:\n{msg1}\nFixing OA status:\n{msg2}")
    if not ARG.WRITE:
        LOGGER.warning("Dry run successful, no updates were made")

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Sync Open Access/license data from OpenAlex")
    GROUP_A = PARSER.add_mutually_exclusive_group(required=False)
    GROUP_A.add_argument('--doi', dest='DOI', action='store',
                         help='Single DOI to process')
    GROUP_A.add_argument('--file', dest='FILE', action='store',
                         help='File of DOIs to process')
    GROUP_A.add_argument('--new', dest='NEW', action='store_true',
                         help='Process DOIs with no OpenAlex data')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, prod)')
    PARSER.add_argument('--test', dest='TEST', action='store_true',
                        default=False, help='Send email to developer')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Write to database')
    PARSER.add_argument('--silent', dest='SILENT', action='store_true',
                        default=False, help="Don't display warnings")
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    try:
        DISCONFIG = JRC.simplenamespace_to_dict(JRC.get_config("dis"))
    except Exception as err:
        terminate_program(err)
    process_dois()
    terminate_program()
