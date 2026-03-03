''' correlate.py
    This program will correlate a budgeting spreadsheet with the subscription collection
'''

__version__ = '4.0.0'

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
# Counters
COUNT = collections.defaultdict(lambda: 0, {})
# Globals
ARG = LOGGER = None

# Output file
OUTPUT = []

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
        LOGGER.info(f"Connecting to {dbo.name} {ARG.MANIFOLD} on {dbo.host} as {dbo.user}")
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)


def is_valid_cost(cost):
    ''' Check if a cost is valid
        Keyword arguments:
          cost: cost to check
        Returns:
          True if cost is valid, False otherwise
    '''
    if pd.isna(cost):
        return False
    if cost.isdigit():
        return int(cost) > 0
    try:
        float(cost)
        return float(cost) > 0
    except Exception:
        return False
    return False


def find_cost(row):
    ''' Find the cost for a row
        Keyword arguments:
          row: row to process
        Returns:
          cost: cost dictionary
    '''
    cost = {}
    for year in range(2011, datetime.now().year + 1):
        LOGGER.debug(f"{row['Publication']} FY{str(year)}: {row[f'FY{str(year)}']}")
        if is_valid_cost(row[f'FY{str(year)}']):
            cost[str(year)] = row[f'FY{str(year)}']
    if not cost:
        LOGGER.warning(f"No cost found for {row['Publication']}")
    return cost


def process_row(row, subscribed):
    ''' Process a row
        Keyword arguments:
          row: row to process
          subscribed: subscribed record
        Returns:
          None
    '''
    COUNT['subscription'] += 1
    cost = {}
    LOGGER.debug(f"Process row {row['Publication']}")
    for year in range(2011, datetime.now().year + 1):
        LOGGER.debug(f"{row['Publication']} FY{str(year)}: {row[f'FY{str(year)}']}")
        if is_valid_cost(row[f'FY{str(year)}']):
            cost[str(year)] = row[f'FY{str(year)}']
    if not cost:
        LOGGER.warning(f"No costs found for {row['Publication']}")
        return
    if subscribed.get('access') != 'Subscription':
        LOGGER.debug(f"{row['Publication']} is {subscribed.get('access')} but has costs")
    payload = {"$set": {"cost": cost}}
    OUTPUT.append({row['Publication']: cost})
    if ARG.WRITE:
        try:
            result = DB['dis'].subscription.update_one({'_id': subscribed['_id']}, payload)
            if hasattr(result, 'modified_count') and result.modified_count:
                COUNT['updated'] += result.modified_count
        except Exception as err:
            terminate_program(err)
    else:
        COUNT['updated'] += 1


def process_collection(row):
    ''' Process a collection row
        Keyword arguments:
          row: row to process
        Returns:
          None
    '''
    try:
        srow = DB['dis'].subscription.find_one({'publisher': row['Publisher']})
    except Exception as err:
        terminate_program(err)
    if row['Publisher'] == 'Wiley':
        srow = {'provider': 'Wiley', 'publisher': 'Wiley'}
    elif ARG.PUBLISHER and row['Publisher'] == ARG.PUBLISHER:
        srow = {'provider': ARG.PUBLISHER, 'publisher': ARG.PUBLISHER}
    if not srow:
        LOGGER.debug(f"Collection publisher {row['Publisher']} not found")
        return
    cost = find_cost(row)
    if not cost:
        COUNT['skipped'] += 1
        return
    payload = {"type": DIS['sub_cost_map'].get(row['Type'], "Collection"),
               "title": row['Publication'],
               "provider": srow['provider'],
               "publisher": srow['publisher'],
               "online-identifier": "-",
               "print-identifier": "-",
               "title-id": "-",
               "identifier": "-",
               "access": "Subscription",
               "cost": cost
              }
    OUTPUT.append(payload)
    if ARG.WRITE:
        try:
            match = {'title': payload['title'], 'provider': payload['provider']}
            result = DB['dis'].subscription.update_one(match, {"$set": payload}, upsert=True)
            if hasattr(result, 'upserted_count') and result.upserted_id:
                COUNT['inserted'] += 1
            elif hasattr(result, 'modified_count') and result.modified_count:
                COUNT['updated'] += result.modified_count
        except Exception as err:
            terminate_program(err)
    else:
        print(payload)
        COUNT['inserted'] += 1


def processing():
    ''' Processing
    '''
    if '.xls' in ARG.FILE:
        tabs = pd.ExcelFile(ARG.FILE).sheet_names
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
            pdf = pd.read_excel(ARG.FILE, sheet_name=selected_tab, header=0, dtype=str)
        else:
            pdf = pd.read_excel(ARG.FILE, header=0, dtype=str)
    else:
        pdf = pd.read_csv(ARG.FILE, header=0, sep="\t",  dtype=str)
    try:
        rows = DB['dis'].subscription.find({})
    except Exception as err:
        terminate_program(err)
    subscribed = {}
    for row in rows:
        if subscribed.get(row['title']) and row['type'] == subscribed[row['title']]['type']:
            LOGGER.error(f"Duplicate subscription found for {row['title']} {row['publisher']}")
        subscribed[row['title']] = row
    for _, row in tqdm(pdf.iterrows(), total=len(pdf), desc="Processing"):
        if ARG.PUBLISHER and row['Publisher'] != ARG.PUBLISHER:
            continue
        COUNT['read'] += 1
        LOGGER.debug(json.dumps(row, default=str))
        if pd.isna(row['Publication']):
            COUNT['skipped'] += 1
            continue
        if row['Publication'] in ['Science', 'Science Signaling']:
            continue
        if row['Publication'] in subscribed:
            # The publisher is known, so we can process the row
            COUNT['matched'] += 1
            process_row(row, subscribed[row['Publication']])
        elif row.get('Type') in DIS['sub_cost_map'].keys():
            # The publisher is not known, but the type is valid, so we can process the row
            COUNT['matched'] += 1
            process_collection(row)
    if OUTPUT:
        with open("cost_updates.json", 'w', encoding='utf-8') as outfile:
            outfile.write(json.dumps(OUTPUT, default=str, indent=2))
    print(f"Licenses read:     {COUNT['read']:,}")
    print(f"Subscriptions:     {COUNT['subscription']:,}")
    print(f"Licenses skipped:  {COUNT['skipped']:,}")
    print(f"Licenses matched:  {COUNT['matched']:,}")
    print(f"Records inserted:  {COUNT['inserted']:,}")
    print(f"Records updated:   {COUNT['updated']:,}")

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Correlate publications")
    PARSER.add_argument('--file', dest='FILE', action='store',
                        required=True, help='Excel file')
    PARSER.add_argument('--sheet', dest='SHEET', action='store',
                        default='Libraries Site Licenses', help='Sheet name')
    PARSER.add_argument('--publisher', dest='PUBLISHER', action='store',
                        default=None, help='Publisher')
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
    DIS = JRC.simplenamespace_to_dict(JRC.get_config("dis"))
    processing()
    terminate_program()
