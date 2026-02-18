''' load_kbart.py
    This program will load publications from a KBART file
'''

__version__ = '2.0.0'

import argparse
import collections
from datetime import datetime
import json
from operator import attrgetter
import sys
import inquirer
from inquirer.themes import BlueComposure
import pandas as pd
from tqdm import tqdm
import jrc_common.jrc_common as JRC

# pylint: disable=broad-exception-caught, logging-fstring-interpolation, no-member, too-many-arguments
# Database
DB = {}
CORRELATE = {}
SUBSCRIBED = {}
# Counters
COUNT = collections.defaultdict(lambda: 0, {})
# Globals
ARG = LOGGER = None

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
        dbo = attrgetter(f"{source}.prod.write")(dbconfig)
        LOGGER.info(f"Connecting to {dbo.name} prod on {dbo.host} as {dbo.user}")
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)


def fallback_publisher(title):
    ''' Fallback publisher
        Keyword arguments:
          title: title of the journal
        Returns:
          fallback publisher
    '''
    try:
        rec = DB['dis'].dois.find_one({'jrc_journal': title, 'publisher': {'$exists': True}})
        if rec and rec.get('publisher'):
            LOGGER.debug(f"Fallback publisher for {title}: {rec['publisher']}")
            return rec['publisher']
    except Exception as err:
        terminate_program(err)
    return None


def get_identifier(row):
    ''' Get identifier for a single publication
        Keyword arguments:
          row: row from data frame
        Returns:
          identifier
    '''
    ident = row['online_identifier'].strip() if row.get('online_identifier') \
                                                and not pd.isna(row['online_identifier']) else ''
    if not ident:
        ident = row['print_identifier'].strip() if row.get('print_identifier') \
                                                   and not pd.isna(row['print_identifier']) else ''
    if not ident:
        ident = row['title_id'].strip() if row.get('title_id') \
                                        and not pd.isna(row['title_id']) else '-'
    if not ident:
        ident = '-'
    return ident


def add_volume(row, payload):
    ''' Add volume information to payload
        Keyword arguments:
          row: row from data frame
          payload: publication payload to set
        Returns:
          None
    '''
    vol = {}
    for additional in ['date_first_issue_online', 'num_first_vol_online',
                       'num_first_issue_online', 'date_last_issue_online', 'num_last_vol_online',
                       'num_last_issue_online']:
        if row.get(additional) and not pd.isna(row[additional]):
            vol[additional] = row[additional]
    if vol:
        payload['volumes'].append(vol)


def add_correlation(payload):
    ''' Process a row
        Keyword arguments:
          payload: publication payload to update
        Returns:
          None
    '''
    if payload.get('access') != 'Subscription':
        return
    cost = {}
    crec = CORRELATE.get(payload['title'])
    for year in range(2011, datetime.now().year + 1):
        if not pd.isna(crec[f'FY{str(year)}']):
            cost[str(year)] = crec[f'FY{str(year)}']
    if not cost:
        LOGGER.warning("No cost found for %s", crec['Publication'])
        return
    payload['cost']= cost
    LOGGER.warning(f"Correlated {payload['title']}")
    COUNT['correlated'] += 1


def set_payload(row, payload):
    ''' Set payload for a single publication
        Keyword arguments:
          row: row from data frame
          payload: payload to set
        Returns:
          payload
    '''
    if ARG.PROVIDER:
        provider = ARG.PROVIDER
    elif row.get('publisher_name', ''):
        provider = str(row['publisher_name']).strip()
    else:
        #terminate_program(f"Provider not found for {row['publication_title']}")
        COUNT['skipped'] += 1
        return
    title = str(row['publication_title']).strip()
    if title == 'nan':
        terminate_program(f"Title not found for {row['publication_title']}")
    # Get publisher name
    if pd.isna(row['publisher_name']):
        fallback = fallback_publisher(title)
        if fallback:
            row['publisher_name'] = fallback
        else:
            LOGGER.warning(f"{field} not found for {row['publication_title']}")
            COUNT['skipped'] += 1
            return
    if pd.isna(row['publication_title']) or pd.isna(row['title_url']) \
       or pd.isna(row['publisher_name']):
        COUNT['skipped'] += 1
        return
    title_url = str(row['title_url']).strip()
    # Get identifier
    ident = get_identifier(row)
    # Get publication type
    ptype = row['publication_type'].strip() if row.get('publication_type', '') \
        in ['Journal', 'Book', 'Book series', 'Monograph', 'Repository'] else ARG.TYPE
    # Set payload
    initial_load = False
    if not payload.get(title):
        initial_load = True
        payload[title] = {'title': title,
                          'identifier': ident,
                          'provider': provider,
                          'publisher': str(row['publisher_name']).strip(),
                          'urls': [],
                          'title-id': '-' if pd.isna(row.get('title_id')) \
                                          else row.get('title_id').strip(),
                          'type': ptype,
                          'access': 'Subscription',
                          'volumes': []}
    if ident != payload[title]['identifier']:
        terminate_program(f"identifier mismatch for {title} " \
                          + f"({ident} != {payload[title]['identifier']})")
    if row.get('publisher_name') and not pd.isna(row['publisher_name']):
        payload[title]['publisher'] = row.get('publisher_name')
    if ARG.TYPE == 'Book':
        payload[title]['type'] = 'Book series' if 'bookseries' in title_url else 'Book'
    if 'access_type' in row and row['access_type'] in ['Complimentary', 'Free-To-Read', 'F']:
        payload[title]['access'] = 'Free to read'
    # Add URL information
    if title_url not in payload[title]['urls']:
        payload[title]['urls'].append(title_url)
    add_volume(row, payload[title])
    # Add info from correlation file
    if initial_load and title in CORRELATE:
        add_correlation(payload[title])


def insert_record(val):
    ''' Insert record
        Keyword arguments:
          val: value of the record
        Returns:
          None
    '''
    if not ARG.WRITE:
        COUNT['inserted'] += 1
        if ARG.DEBUG:
            print(json.dumps(val, indent=2))
        return
    try:
        result = DB['dis'].subscription.update_one(
            {'title': val['title'], 'identifier': val['identifier']},
            {'$set': val},
            upsert=True
        )
        if result.upserted_id:
            COUNT['inserted'] += 1
        else:
            COUNT['updated'] += 1
    except Exception as err:
        terminate_program(err)


def get_subscriptions():
    ''' Get subscriptions from the subscription collection
        Keyword arguments:
          None
        Returns:
          subscriptions
    '''
    try:
        rows = DB['dis'].subscription.find({})
    except Exception as err:
        terminate_program(err)
    cnt = 0
    for row in rows:
        SUBSCRIBED[row['title']] = row
        cnt += 1
    LOGGER.info(f"Found {cnt:,} current subscriptions")


def processing():
    ''' Processing
    '''
    if '.xls' in ARG.KBART:
        tabs = pd.ExcelFile(ARG.KBART).sheet_names
        LOGGER.info(f"Available sheets: {', '.join(tabs)}")
        if len(tabs) > 1:
            if ARG.SHEET:
                selected_tab = ARG.SHEET
            else:
                questions = [inquirer.List('sheet',
                                            message="Which sheet would you like to process?",
                                            choices=tabs)]
                answers = inquirer.prompt(questions, theme=BlueComposure())
                selected_tab = answers['sheet']
            pdf = pd.read_excel(ARG.KBART, sheet_name=selected_tab, header=0, dtype=str)
        else:
            pdf = pd.read_excel(ARG.KBART, header=0, dtype=str)
    else:
        pdf = pd.read_csv(ARG.KBART, header=0, sep="\t",  dtype=str)
    #ARG.CORRELATE = 'HHMICollectionsBudgetPivots_7-31-2025.xlsx'
    if ARG.CORRELATE:
        correlate = pd.read_excel(ARG.CORRELATE, header=0, sheet_name='Libraries Site Licenses', dtype=str)
        for _, row in correlate.iterrows():
            if pd.isna(row['Publication']):
                continue
            COUNT['correlate'] += 1
            CORRELATE[row['Publication']] = row
        LOGGER.info(f"Found {COUNT['correlate']:,} publications in correlation file")
    payload = {}
    for _, row in tqdm(pdf.iterrows(), total=len(pdf), desc="Processing"):
        COUNT['read'] += 1
        if not row.get('title_id'):
            row['title_id'] = '-'
        if row['publication_title'] and row['title_id'] and row['title_url'] \
           and row['publisher_name']:
            if 'access_type' in row and row['access_type'] in ['Token']:
                COUNT['token'] += 1
                continue
            set_payload(row, payload)
        else:
            COUNT['skipped'] += 1
            LOGGER.warning("Skipping row %s", row['publication_title'])
    if payload:
        for val in tqdm(payload.values(), desc="Inserting records"):
            insert_record(val)
    print(f"Journals read:    {COUNT['read']:,}")
    print(f"Records skipped:  {COUNT['skipped']:,}")
    print(f"Tokens:           {COUNT['token']:,}")
    print(f"Correlated:       {COUNT['correlated']:,}")
    print(f"Records inserted: {COUNT['inserted']:,}")
    print(f"Records updated:  {COUNT['updated']:,}")

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Load journals for one publisher")
    PARSER.add_argument('--provider', dest='PROVIDER', action='store',
                        help='Provider')
    PARSER.add_argument('--kbart', dest='KBART', action='store',
                        required=True, help='KBART file (text or Excel)')
    PARSER.add_argument('--sheet', dest='SHEET', action='store',
                        default=None, help='Sheet name for KBART file (Excel only)')
    PARSER.add_argument('--correlate', dest='CORRELATE', action='store',
                        help='Excel correlation file')
    PARSER.add_argument('--type', dest='TYPE', action='store',
                        default='Journal', choices=['Journal', 'Book', 'Monograph'],
                        help='Resource type (Journal, Book, Monograph, etc.)')
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
