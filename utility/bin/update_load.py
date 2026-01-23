import argparse
from operator import attrgetter
import sys
import time
from tqdm import tqdm
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

DB = {}
COUNT = {'dois': 0, 'notfound': 0, 'updated': 0}

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


def update_authors(row):
    """ Update first and last authors
        Keyword arguments:
          doi: DOI to update
        Returns:
          None
    """
    first = []
    last = None
    if 'jrc_obtained_from' in row and row['jrc_obtained_from'] == "DataCite":
        field = 'creators'
        datacite = True
    else:
        field = 'author'
        datacite = False
    if field in row:
        if datacite:
           for auth in row[field]:
                if 'sequence' in auth and auth['sequence'] == 'additional':
                    break
                try:
                    janelian = DL.is_janelia_author(auth, DB['dis'].orcid, PROJECT)
                except Exception as err:
                    LOGGER.error(f"Could not process {row['doi']}")
                    terminate_program(err)
                if janelian:
                    first.append(janelian)
        else:
            janelian = DL.is_janelia_author(row[field][0], DB['dis'].orcid, PROJECT)
            if janelian:
                first.append(janelian)
        janelian = DL.is_janelia_author(row[field][-1], DB['dis'].orcid, PROJECT)
        if janelian:
            last = janelian
    if not first and not last:
        return
    payload = {}
    if first:
        payload["jrc_first_author"] = first
    if last:
        payload["jrc_last_author"] = last
    if first or last:
        COUNT["updated"] += 1
        if ARG.WRITE:
            try:
                DB['dis']['dois'].update_one({"doi": row['doi']}, {"$set": payload})
            except Exception as err:
                terminate_program(err)


def update_load(doi):
    """ Update jrc_load_source and jrc_loaded_by
        Keyword arguments:
          doi: DOI to update
        Returns:
          None
    """
    doi = doi.lower()
    LOGGER.info(doi)
    coll = DB['dis'].dois
    row = coll.find_one({"doi": doi})
    if not row:
        LOGGER.warning(f"{doi} was not found")
        COUNT["notfound"] += 1
        return
    payload = {"jrc_load_source": "Manual", "jrc_loaded_by": "Virginia Scarlett"}
    if ARG.WRITE:
        try:
            coll.update_one({"doi": doi}, {"$set": payload})
        except Exception as err:
            terminate_program(err)
        COUNT["updated"] += 1


def process_dois():
    """ Process a list of DOIs
        Keyword arguments:
          None
        Returns:
          None
    """
    dois = []
    if ARG.DOI:
        dois = [ARG.DOI]
    elif ARG.FILE:
        try:
            with open(ARG.FILE, "r", encoding="ascii") as instream:
                for doi in instream.read().splitlines():
                    dois.append(doi.lower().strip())
        except Exception as err:
            LOGGER.error(f"Could not process {ARG.FILE}")
            terminate_program(err)
    else:
        payload = {"jrc_is_oa": {"$exists": True}, "jrc_oa_status": "closed",
                   "jrc_fulltext_url": {"$exists": True}
                  }
        try:
            cnt = DB['dis'].dois.count_documents(payload)
            rows = DB['dis'].dois.find(payload)
        except Exception as err:
            terminate_program(err)
        for row in tqdm(rows, total=cnt, desc="Payload"):
            dois.append(row)
            #dois.append(row['doi'])

    for doi in tqdm(dois, desc="DOIs"):
        COUNT["dois"] += 1
        #update_load(doi)
        #update_authors(doi)
    print(f"DOIs read:      {COUNT['dois']}")
    if COUNT['notfound']:
        print(f"DOIs not found: {COUNT['notfound']}")
    print(f"DOIs updated:   {COUNT['updated']}")
    if not ARG.WRITE:
        LOGGER.warning("Dry run successful, no updates were made")

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Add a reviewed date to one or more DOIs")
    GROUP_A = PARSER.add_mutually_exclusive_group(required=False)
    GROUP_A.add_argument('--doi', dest='DOI', action='store',
                         help='Single DOI to process')
    GROUP_A.add_argument('--file', dest='FILE', action='store',
                         help='File of DOIs to process')
    GROUP_A.add_argument('--all', dest='ALL', action='store_true',
                         help='Process all DOIs')
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
    try:
        PROJECT = DL.get_project_map(DB['dis'].project_map)
    except Exception as err:
        terminate_program(err)
    process_dois()
    terminate_program()
