""" apply_version_tags.py
    Apply version tags to DOIs. Potential tags are found by recursively searching for all
    versioned DOIs ("is" and "has").
"""

__version__ = '1.0.0'

import argparse
import collections
import json
from operator import attrgetter
import sys
import inquirer
from inquirer.themes import BlueComposure
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Database
DB = {}
# Globals
ARG = LOGGER = None
TAG_RECORD = {}
# Counters
COUNT = collections.defaultdict(lambda: 0, {})

def terminate_program(msg=None):
    ''' Terminate the program gracefully
        Keyword arguments:
          msg: error message
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

def process_version_doi(vdoi, rec, dois):
    ''' Process a single version DOI by adding it to the dois list and returning the record
        Keyword arguments:
          vdoi: version DOI
          rec: record
          dois: list of DOIs
        Returns:
          versioned DOI record if the DOI should be processed, otherwise None
    '''
    if vdoi in dois:
        return None
    if rec['doi'] not in dois:
        dois.append(rec['doi'])
    try:
        vrec = DB['dis'].dois.find_one({'doi': vdoi})
    except Exception as err:
        terminate_program(err)
    if not vrec:
        LOGGER.warning(f"DOI {vdoi} not found")
        return None
    return vrec


def get_versions(rec, dois):
    ''' Get versions for a seed DOI by recursively searching for all versions ("is" and "has")
        Keyword arguments:
          rec: record
          dois: list of DOIs
        Returns:
          None
    '''
    LOGGER.debug(f"Entered get_versions for {rec['doi']}")
    if 'relation' in rec:
        for reltype in ('is-version-of', 'has-version'):
            if reltype not in rec['relation']:
                continue
            for rel in rec['relation'][reltype]:
                if 'id-type' in rel and rel['id-type'] == 'doi':
                    vdoi = rel['id'].lower()
                    vrec = process_version_doi(vdoi, rec, dois)
                    if vrec:
                        get_versions(vrec, dois)
    if 'relatedIdentifiers' in rec and rec['relatedIdentifiers']:
        for rel in rec['relatedIdentifiers']:
            if rel['relatedIdentifierType'] == 'DOI' \
               and rel['relationType'] in ('HasVersion', 'IsVersionOf'):
                vdoi = rel['relatedIdentifier'].lower()
                vrec = process_version_doi(vdoi, rec, dois)
                if vrec:
                    get_versions(vrec, dois)
    if rec['doi'] not in dois:
        dois.append(rec['doi'])


def get_new_tags(dois):
    ''' Get potential new tags for DOIs
        Keyword arguments:
          dois: list of DOIs
        Returns:
          None
    '''
    tag_count = collections.defaultdict(lambda: [], {})
    print("Versioned DOIs:")
    for doi in dois:
        try:
            rec = DB['dis'].dois.find_one({'doi': doi})
            title = DL.get_title(rec)
            print(f"  {doi} {title}")
        except Exception as err:
            terminate_program(err)
        if not 'jrc_tag' in rec:
            continue
        for tag in rec['jrc_tag']:
            tag_count[tag['name']].append(doi)
            if tag['name'] not in TAG_RECORD:
                TAG_RECORD[tag['name']] = tag
    tags_all = []
    tags_some = {}
    for tag, tdois in tag_count.items():
        if len(tdois) == len(dois):
            tags_all.append(tag)
        else:
            tags_some[tag] = tdois
    if tags_all:
        print("\nThe following tags are already on all DOIs:\n ",
              ", ".join(tags_all))
    doitags = []
    default_tags = []
    for tag, tdois in tags_some.items():
        msg = f"{tag}:  {', '.join(tdois)}"
        doitags.append(msg)
        # Use this tag by default if it's on more than 50% of the version DOIs
        if len(tdois)/len(dois) > 0.5:
            default_tags.append(msg)
    return sorted(doitags), default_tags


def apply_tags(dois, tags):
    ''' Apply new tags to DOIs
        Keyword arguments:
          dois: list of DOIs
          tags: list of tags
        Returns:
          None
    '''
    to_add = [tag.split(':  ')[0] for tag in tags]
    COUNT['tags_added'] = len(to_add)
    for doi in dois:
        try:
            rec = DB['dis'].dois.find_one({'doi': doi}, {'jrc_tag': 1})
        except Exception as err:
            terminate_program(err)
        dtags = [tag['name'] for tag in rec['jrc_tag']]
        appended = False
        for tag in to_add:
            if tag not in dtags:
                appended = True
                rec['jrc_tag'].append(TAG_RECORD[tag])
        if not appended:
            continue
        COUNT['dois_updated'] += 1
        if ARG.WRITE:
            DB['dis'].dois.update_one({'doi': doi}, {'$set': {'jrc_tag': rec['jrc_tag']}})
        else:
            print(f"Would update {doi} to:\n{json.dumps(rec['jrc_tag'], indent=2)}")


def choose_tags_to_apply(dois, tags_to_apply, default_tags):
    ''' Choose tags to apply to DOIs
        Keyword arguments:
          dois: list of DOIs
          tags_to_apply: list of tags to apply
          default_tags: list of default tags
        Returns:
          None
    '''
    try:
        quest = [inquirer.Checkbox('doi',
                                   message="Select tags to add to all versioned DOIs",
                                   choices=tags_to_apply,
                                   default=default_tags)]
        ans = inquirer.prompt(quest, theme=BlueComposure())
    except KeyboardInterrupt:
        terminate_program("User cancelled program")
    except Exception as err:
        terminate_program(err)
    apply_tags(dois, ans['doi'])


def processing():
    ''' Main processing routine
        Keyword arguments:
          None
        Returns:
          None
    '''
    try:
        rec = DB['dis'].dois.find_one({'doi': ARG.DOI})
    except Exception as err:
        terminate_program(err)
    if not rec:
        terminate_program(f"DOI {ARG.DOI} not found")
    dois = []
    get_versions(rec, dois)
    if len(dois) <= 1:
        terminate_program("No DOI versions were found")
    tags_to_apply, default_tags = get_new_tags(sorted(dois))
    if tags_to_apply:
        choose_tags_to_apply(dois, tags_to_apply, default_tags)
    else:
        LOGGER.warning("No additional tags to apply")
    print(f"DOIs found:             {len(dois)}")
    print(f"Tags found:             {len(TAG_RECORD)}")
    if tags_to_apply:
        print(f"Potential tags to add:  {len(tags_to_apply)}")
        print(f"Tags added:             {COUNT['tags_added']}")
        print(f"DOIs updated:           {COUNT['dois_updated']}")

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Apply tags for versioned DOIs")
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, prod)')
    PARSER.add_argument('--doi', dest='DOI', action='store',
                        required=True, help='DOI to process')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Flag, Write to database')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    try:
        PROJECT = DL.get_project_map(DB['dis'].project_map, inactive=False)
    except Exception as gerr:
        terminate_program(gerr)
    processing()
    terminate_program()
