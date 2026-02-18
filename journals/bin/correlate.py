''' correlate.py
    This program will correlate a budgeting spreadsheet with the subscription collection
'''

__version__ = '1.0.0'

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
        LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, ARG.MANIFOLD, dbo.host, dbo.user)
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)


def process_row(row, subscribed):
    ''' Process a row
        Keyword arguments:
          row: row to process
          subscribed: subscribed record
        Returns:
          None
    '''
    if subscribed.get('access') != 'Subscription':
        print(row['Publication'], subscribed.get('access'))
        return
    COUNT['subscription'] += 1
    cost = {}
    print("Process row", row['Publication'])
    for year in range(2011, datetime.now().year + 1):
        print(f"{row['Publication']} FY{str(year)}: {row[f'FY{str(year)}']}")
        if not pd.isna(row[f'FY{str(year)}']):
            cost[str(year)] = row[f'FY{str(year)}']
    if not cost:
        LOGGER.warning("No cost found for %s", row['Publication'])
        return
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
        subscribed[row['title']] = row
    for _, row in tqdm(pdf.iterrows(), total=len(pdf), desc="Processing"):
        COUNT['read'] += 1
        LOGGER.debug(json.dumps(row, default=str))
        if pd.isna(row['Publication']):
            COUNT['skipped'] += 1
        if row['Publication'] in subscribed:
            if row['Publication'] == 'Nature':
                print(json.dumps(row, default=str))
            COUNT['matched'] += 1
            process_row(row, subscribed[row['Publication']])
    with open("cost_updates.json", 'w') as outfile:
        outfile.write(json.dumps(OUTPUT, default=str, indent=2))
    print(f"Licenses read:     {COUNT['read']:,}")
    print(f"Subscriptions:     {COUNT['subscription']:,}")
    print(f"Licenses skipped:  {COUNT['skipped']:,}")
    print(f"Licenses matched:  {COUNT['matched']:,}")
    print(f"Records updated:   {COUNT['updated']:,}")

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Correlate publications")
    PARSER.add_argument('--file', dest='FILE', action='store',
                        required=True, help='Excel file')
    PARSER.add_argument('--sheet', dest='SHEET', action='store',
                        default=None, help='Sheet name')
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
    processing()
    terminate_program()
