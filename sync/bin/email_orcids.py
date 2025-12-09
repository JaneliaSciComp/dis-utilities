''' email_orcids.py
    Email a plea to get an ORCID for Janelians without ORCIDs
'''

import argparse
import collections
from operator import attrgetter
import sys
from tqdm import tqdm
import jrc_common.jrc_common as JRC

# pylint: disable=broad-exception-caught,logging-fstring-interpolation,logging-not-lazy

# Database
DB = {}
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
    ''' Initialize program
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


def name_search_payload(given, family):
    ''' Generate a payload for searching the orcid collection by name
        Keyword arguments:
          given: list of given names
          family: list of family names
        Returns:
          Payload
    '''
    return {"$and": [{"$or": [{"author.given": {"$in": given}},
                              {"creators.givenName": {"$in": given}}]},
                     {"$or": [{"author.family": {"$in": family}},
                              {"creators.familyName": {"$in": family}}]}]
           }


def author_doi_count(given, family):
    ''' Get the number of DOIs for a given author
        Keyword arguments:
          given: list of given names
          family: list of family names
        Returns:
          Number of DOIs
    '''
    payload = name_search_payload(given, family)
    rows = DB['dis'].dois.find(payload, {"doi": 1, "author": 1, "creators": 1})
    dois = 0
    for row in rows:
        field = 'author' if 'author' in row else 'creators'
        for aut in row[field]:
            if field == 'author' and 'given' in aut:
                if aut['given'] in given and aut['family'] in family:
                    dois += 1
                    break
            elif 'givenName' in aut and aut['givenName'] in given and aut['familyName'] in family:
                dois += 1
                break
    return dois if dois else 0


def email_janelians(receivers):
    ''' Create and send emails to Janelians missing ORCIDs
        Keyword arguments:
          receivers: list of dictionaries of Janelians that need an ORCID
        Returns:
          None
    '''
    for row in tqdm(receivers, desc='Send emails'):
        resp = JRC.call_people_by_id(row['employeeId'])
        if not resp or 'employeeId' not in resp or not resp['employeeId']:
            LOGGER.warning(f"No People information found for {row}")
            continue
        name = ' '.join([resp['nameFirstPreferred'], resp['nameLastPreferred']])
        subject = "Please register for your Janelia-affiliated ORCID"
        text = f'''\
Hello {resp['nameFirstPreferred']},<br><br>
You are receiving this email because you still need to obtain your ORCID
(Open Researcher and Contributor ID) identification or if you have an ORCID
to update your affiliation. This is your very own persistent identifier that
you can take with you to any institution and will help identify you as a researcher at Janelia.
<br><br>
The process to obtain an ORCID should take no more than 2 minutes out of your day. ORCID is an important tool to help identify the work which you helped to create here at Janelia. The ORCID helps the Data and Information Services (DIS) team track Janelia publications, and it benefits you in a multitude of ways.
<br><br>
Why get an ORCID (besides helping the DIS team):
<ol>
<li>It will help distinguish your name and help you avoid any publication name confusion because you will
have your own unique ORCID even if you share your name with another researcher.</li>
<li>Over 7,000 publishers ask for an ORCID when publishing and the publisher can sync your work to your
ORCID account.</li>
<li>It connects you and your works with your institutions. Ex. You can change your current organization
in your ORCID profile easily and still be connected to all your prior works.</li>
<li>Increase your visibility by adding your ORCID to CVs and having your ORCID profile display your work.</li>
<li>ORCID goes through life with you regardless of changes in name, location, institutional changes, etc.</li>
</ol>
<br><br>
To view the directions on how to obtain your ORCID with a Janelia affiliation please view
<a href='https://hhmionline.sharepoint.com/SitePages/Janelia/DataInformationServices/ORCID.aspx#how-can-i-create-an-orcid'>this document</a>. 
<br><br>
If you need assistance, please email datainfo@hhmi.org.
<br><br>
Thank you very much,<br>
Lauren Acquarole<br>
Librarian, Data and Information Services<br>
        '''
        if ARG.LIMIT and COUNT['Emails sent'] >= ARG.LIMIT:
            return
        COUNT['Emails sent'] += 1
        if not (ARG.WRITE or ARG.TEST):
            LOGGER.info(f"Would send email to {name} ({resp['email']})")
            continue
        email = [DISCONFIG['developer'] if ARG.TEST else resp['email']]
        try:
            JRC.send_email(text, DISCONFIG['sender'], email, subject, mime='html', server="mail.hhmi.org")
            LOGGER.info(f"Email sent to {name} ({email})")
        except Exception as err:
            terminate_program(f"Error sending email to {name} ({email}): {err}")
        if ARG.TEST:
            return


def process():
    ''' Get and process a list of Janelians missing ORCIDs
        Keyword arguments:
          None
        Returns:
          None
    '''
    payload = {"orcid": {"$exists": False}, "alumni": {"$exists": False},
               "workerType": "Employee"}
    cnt = DB['dis'].orcid.count_documents(payload)
    rows = DB['dis'].orcid.find(payload).sort("family", 1)
    receivers = []
    for row in tqdm(rows, desc='Get DOIs', total=cnt):
        COUNT['Janelians'] += 1
        dois = author_doi_count(row['given'], row['family'])
        if not dois:
            continue
        COUNT['Janelians with DOIs'] += 1
        row['dois'] = dois
        receivers.append(row)
    email_janelians(receivers)
    print(f"Total Janelians:     {COUNT['Janelians']}")
    print(f"Janelians with DOIs: {COUNT['Janelians with DOIs']}")
    print(f"Emails sent:         {COUNT['Emails sent']}")

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Email information on newly-added DOIs to author")
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, prod)')
    PARSER.add_argument('--limit', dest='LIMIT', action='store',
                        type=int, default=0, help='Number of Janelians to process')
    PARSER.add_argument('--test', dest='TEST', action='store_true',
                        default=False, help='Send emails to developer')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Actually send emails')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    DISCONFIG = JRC.simplenamespace_to_dict(JRC.get_config("dis"))
    initialize_program()
    process()
    terminate_program()
