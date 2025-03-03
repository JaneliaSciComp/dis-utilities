""" update_tags.py
    Update tags for selected DOIs
"""

__version__ = '3.2.0'

import argparse
import collections
from datetime import datetime, timedelta
import json
from operator import attrgetter
import sys
from colorama import Fore, Back, Style
import inquirer
from inquirer.themes import BlueComposure
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Database
DB = {}
PROJECT = {}
SUPORG = {}
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
    try:
        rows = DB['dis'].project_map.find({})
    except Exception as err:
        terminate_program(err)
    for row in rows:
        PROJECT[row['name']] = row['project']
    try:
        orgs = DL.get_supervisory_orgs(coll=DB['dis'].suporg)
    except Exception as err:
        terminate_program(err)
    for key, val in orgs.items():
        SUPORG[key] = val


def get_dois():
    ''' Get a list of DOIs to process. This will be one of four things:
        - a single DOI from ARG.DOI
        - a list of DOIs from ARG.FILE
        Keyword arguments:
          None
        Returns:
          List of DOIs
    '''
    if ARG.DOI:
        COUNT['specified'] = 1
        return [ARG.DOI]
    if ARG.FILE:
        COUNT['specified'] = 1
        return ARG.FILE.read().splitlines()
    LOGGER.info(f"Finding DOIs from the last {ARG.DAYS} day{'' if ARG.DAYS == 1 else 's'}")
    week_ago = (datetime.today() - timedelta(days=ARG.DAYS))
    try:
        rows = DB['dis'].dois.find({"jrc_inserted": {"$gte": week_ago}})
    except Exception as err:
        terminate_program(err)
    dois = []
    for row in rows:
        dois.append(row['doi'])
    COUNT['specified'] = len(dois)
    return dois


def append_tags(auth, janelians, atags):
    """ Update "janelians" and "atags" lists
        Keyword arguments:
          auth: author record
          janelians: list of Janelia author names
          atags: list of tags
        Returns:
          None
    """
    if auth['janelian']:
        janelians.append(f"{auth['given']} {auth['family']}")
    if 'group' in auth:
        if auth['group'] not in atags:
            atags.append(auth['group'])
    if 'tags' in auth:
        for tag in auth['tags']:
            if tag not in atags:
                atags.append(tag)
    if 'name' in auth:
        if auth['name'] not in PROJECT:
            LOGGER.warning(f"Project {auth['name']} is not defined")
        elif PROJECT[auth['name']] and PROJECT[auth['name']] not in atags:
            atags.append(PROJECT[auth['name']])


def get_tags(authors):
    """ Get tags from a list of authors
        Keyword arguments:
          authors: list of detailed authors
        Returns:
          tags: list of tags
          janelians: list of Janelia author names
          tagauth: dict of authors by tag
    """
    tags = []
    janelians = []
    tagauth = {}
    for auth in authors:
        atags = []
        append_tags(auth, janelians, atags)
        for tag in atags:
            if tag not in tags:
                tags.append(tag)
            if tag not in tagauth:
                tagauth[tag] = []
            if 'family' in auth and auth['family'] not in tagauth[tag]:
                tagauth[tag].append(auth['family'])
                tagauth[tag].sort()
    return tags, janelians, tagauth


def get_tag_choices(tags, tagauth, rec):
    """ Get tag choices for checklist prompt
        Keyword arguments:
          tags: list of tags
          tagauth: dict of authors by tag
          rec: DOI record
        Returns:
          tagd: dict of tags by tag name
          current: list of current tags
    """
    tags.sort()
    tagd = {}
    current = []
    tagnames = []
    if 'jrc_tag' in rec:
        tagnames = [etag['name'] for etag in rec['jrc_tag']]
    for tag in tags:
        alert = ""
        if tag not in SUPORG:
            alert = f" {Fore.RED}{Back.BLACK}(not a supervisory organization){Style.RESET_ALL}"
        newtag = f"{tag} ({', '.join(tagauth[tag])}) {alert}"
        if tag in tagnames:
            current.append(newtag)
        tagd[newtag] = tag
    return tagd, current


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


def add_non_author_tags(payload):
    """ Add suporg tags to a DOI's payload
        Keyword arguments:
          payload: DOI payload
        Returns:
          None
    """
    orgs = DL.get_supervisory_orgs(coll=DB['dis'].suporg)
    if 'jrc_tag' in payload:
        tags = [tag['name'] for tag in payload['jrc_tag']]
        for tag in tags:
            if tag in orgs:
                del orgs[tag]
    quest = [(inquirer.Checkbox('checklist', carousel=True,
                                message='Select additional tags',
                                choices=sorted(orgs.keys())))]
    try:
        ans = inquirer.prompt(quest, theme=BlueComposure())
    except KeyboardInterrupt:
        terminate_program("User cancelled program")
    tags = []
    for tag in ans['checklist']:
        code = orgs[tag]
        tagtype = 'suporg'
        tags.append({"name": tag, "code": code, "type": tagtype})
    if not tags:
        return
    if 'jrc_tag' not in payload:
        payload['jrc_tag'] = []
    payload['jrc_tag'].extend(tags)


def process_tags(ans, tagd):
    """ Process the tags from the prompt
        Keyword arguments:
          ans: prompt answers
          tagd: dict of tags by tag name
        Returns:
          payload: DOI payload
    """
    payload = {}
    if 'checklist' in ans:
        tags = []
        for tag in ans['checklist']:
            code = get_suporg_code(tagd[tag])
            tagtype = 'suporg' if code else 'affiliation'
            tags.append({"name": tagd[tag], "code": code, "type": tagtype})
        if tags:
            payload["jrc_tag"] = tags
    # Additional tags
    if 'additional' in ans and ans['additional'] == 'Yes':
        add_non_author_tags(payload)
    return payload


def update_single_doi(rec):
    """ Update tags for a single DOI
        Keyword arguments:
          rec: DOI record
        Returns:
          None
    """
    authors = DL.get_author_details(rec, DB['dis'].orcid)
    tags, janelians, tagauth = get_tags(authors)
    if not tags:
        LOGGER.warning(f"No tags for DOI {rec['doi']}")
    tagd, current = get_tag_choices(tags, tagauth, rec)
    print(f"DOI: {rec['doi']}")
    print(f"{DL.get_title(rec)}")
    print('Janelia authors:', ', '.join(janelians))
    if 'jrc_newsletter' in rec and rec['jrc_newsletter']:
        print(f"{Fore.LIGHTYELLOW_EX}{Back.BLACK}DOI has newsletter date of " \
              + f"{rec['jrc_newsletter']}{Style.RESET_ALL}")
    today = datetime.today().strftime('%Y-%m-%d')
    quest = []
    try:
        if tagd:
            quest.append(inquirer.Checkbox('checklist', carousel=True,
                                           message='Select tags',
                                           choices=tagd, default=current))
        quest.append(inquirer.List('additional',
                                   message="Would you like to add any additional tags?",
                                   choices=['Yes', 'No'], default='No'))
        quest.append(inquirer.List('newsletter',
                                   message=f"Set jrc_newsletter to {today}",
                                   choices=['Yes', 'No']))
        ans = inquirer.prompt(quest, theme=BlueComposure())
    except KeyboardInterrupt:
        terminate_program("User cancelled program")
    if not ans:
        return
    payload = process_tags(ans, tagd)
    # Newsletter
    if 'newsletter' in ans and ans['newsletter'] == 'Yes':
        payload['jrc_newsletter'] = today
    COUNT['selected'] += 1
    if not payload:
        return
    if ARG.WRITE:
        coll = DB['dis'].dois
        #if not tags:
        #   result = coll.update_one({"doi": rec['doi']}, {"$unset": {"jrc_tag":1}})
        result = coll.update_one({"doi": rec['doi']}, {"$set": payload})
        if hasattr(result, 'matched_count') and result.matched_count:
            COUNT['updated'] += 1
    else:
        print(f"{rec['doi']}\n{json.dumps(payload, indent=2)}")
        COUNT['updated'] += 1


def update_tags():
    """ Update tags for specified DOIs
        Keyword arguments:
          None
        Returns:
          None
    """
    LOGGER.info(f"Started run (version {__version__})")
    dois = get_dois()
    if not dois:
        terminate_program("No DOIs were found")
    coll = DB['dis'].dois
    for odoi in dois:
        doi = odoi.lower().strip()
        try:
            rec = coll.find_one({"doi": doi})
        except Exception as err:
            terminate_program(err)
        if not rec:
            LOGGER.warning(f"DOI {doi} not found")
            COUNT['notfound'] += 1
            continue
        update_single_doi(rec)
    print(f"DOIs specified:           {COUNT['specified']}")
    print(f"DOIs not found:           {COUNT['notfound']}")
    print(f"DOIs selected for update: {COUNT['selected']}")
    print(f"DOIs updated:             {COUNT['updated']}")
    if not ARG.WRITE:
        LOGGER.warning("Dry run successful, no updates were made")

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Update tags")
    PARSER.add_argument('--doi', dest='DOI', action='store',
                        help='Single DOI to process')
    PARSER.add_argument('--file', dest='FILE', action='store',
                        type=argparse.FileType("r", encoding="ascii"),
                        help='File of DOIs to process')
    PARSER.add_argument('--days', dest='DAYS', action='store', type=int,
                        default=7, help='Number of days to go back for DOIs')
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
    update_tags()
    terminate_program()
