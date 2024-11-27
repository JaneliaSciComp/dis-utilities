""" remove_jrc_author.py
    Remove a JRC author from a given DOI
"""

__version__ = '1.0.0'

import argparse
from operator import attrgetter
import sys
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL

# pylint: disable=broad-exception-caught,logging-fstring-interpolation,logging-not-lazy

# Database
DB = {}

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


def check_first_last(rec):
    ''' Check for first and last author
        Keyword arguments:
          rec: DOI record
        Returns:
          None
    '''
    for which in ('first', 'last'):
        testvar = f"jrc_{which}_id"
        LOGGER.info(f"Checking {testvar} for {rec['doi']}")
        if testvar in rec and ARG.EMPLOYEE in rec[testvar]:
            othervar = f"jrc_{which}_author"
            if len(rec[testvar]) > 1:
                LOGGER.warning(f"You'll need to manually update {testvar} and {othervar} " \
                               + f"for {rec['doi']}")
                continue
            print(f"Removing {testvar} and {othervar} from DOI {rec['doi']}")
            if ARG.WRITE:
                try:
                    result = DB['dis']['dois'].update_one({'doi': rec['doi']},
                                                          {'$unset': {testvar: None,
                                                                      othervar: None}})
                except Exception as err:
                    terminate_program(err)
                if hasattr(result, 'matched_count') and result.modified_count:
                    print(f"DOI {rec['doi']} updated to remove {which} author")


def processing():
    ''' Process the request
        Keyword arguments:
          None
        Returns:
          None
    '''
    rec = DL.get_doi_record(ARG.DOI.lower(), DB['dis']['dois'])
    if not rec:
        terminate_program(f"DOI {ARG.DOI} not found")
    if 'jrc_author' not in rec:
        terminate_program(f"DOI {ARG.DOI} does not have any JRC authors defined")
    original = list(rec['jrc_author'])
    if ARG.EMPLOYEE not in rec['jrc_author']:
        terminate_program(f"Employee {ARG.EMPLOYEE} not found in JRC authors")
    rec['jrc_author'].remove(ARG.EMPLOYEE)
    print(f"jrc_author changed from\n{original}\n   to\n{rec['jrc_author']}")
    # jrc_author
    if ARG.WRITE:
        try:
            result = DB['dis']['dois'].update_one({'doi': rec['doi']},
                                                  {'$set': {'jrc_author': rec['jrc_author']}})
        except Exception as err:
            terminate_program(err)
        if hasattr(result, 'matched_count') and result.modified_count:
            print(f"DOI {rec['doi']} updated to change jrc_author")
    check_first_last(rec)
    if not ARG.WRITE:
        LOGGER.warning("Dry run successful, no updates were made")

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Remove JRC author from a given DOI")
    PARSER.add_argument('--doi', dest='DOI', action='store',
                        required=True, help='DOI')
    PARSER.add_argument('--employee', dest='EMPLOYEE', action='store',
                        help='Employee ID to remove')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, [prod])')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Write to database')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    processing()
    terminate_program()
