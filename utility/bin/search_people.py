"""search_people.py
Search the People system for a name
"""

import argparse
import json
import os
import sys
import inquirer
from inquirer.themes import BlueComposure
import jrc_common.jrc_common as JRC

# pylint: disable=broad-exception-caught


def terminate_program(msg=None):
    """Terminate the program gracefully
    Keyword arguments:
      msg: error message or object
    Returns:
      None
    """
    if msg:
        if not isinstance(msg, str):
            msg = f"An exception of type {type(msg).__name__} occurred. Arguments:\n{msg.args}"
        LOGGER.critical(msg)
    sys.exit(-1 if msg else 0)


def initialize_program():
    """Initialize program
    Keyword arguments:
      None
    Returns:
      None
    """
    if "PEOPLE_API_KEY" not in os.environ:
        terminate_program("Missing token - set in PEOPLE_API_KEY environment variable")


def perform_search():
    """Search the People system
    Keyword arguments:
      None
    Returns:
      None
    """
    try:
        response = JRC.call_people_by_name(ARG.NAME)
    except Exception as err:
        terminate_program(err)
    if not response:
        terminate_program(f"{ARG.NAME} was not found")
    people = {}
    for rec in response:
        if ARG.JANELIA:
            if "Janelia" not in rec["locationName"]:
                continue
            key = f"{rec['nameFirstPreferred']} {rec['nameLastPreferred']} {rec['employeeId']}"
        else:
            key = f"{rec['nameFirstPreferred']} {rec['nameLastPreferred']} {rec['employeeId']} ({rec['locationName']})"
        people[key] = rec
    if not people:
        terminate_program(f"{ARG.NAME} was not found")
    if len(people) == 1:
        ans = {"who": list(people.keys())[0]}
    else:
        quest = [inquirer.List("who", message="Select person", choices=people.keys())]
        ans = inquirer.prompt(quest, theme=BlueComposure())
    if not ans:
        terminate_program()
    print(json.dumps(people[ans["who"]], indent=2))
    try:
        response = JRC.call_people_by_id(people[ans["who"]]["employeeId"])
    except Exception as err:
        terminate_program(err)
    print(f"{'-'*79}")
    print(json.dumps(response, indent=2))


# -----------------------------------------------------------------------------

if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(description="Look up a person by name")
    PARSER.add_argument(
        "--name", dest="NAME", action="store", required=True, help="Name to look up"
    )
    PARSER.add_argument(
        "--janelia",
        dest="JANELIA",
        action="store_true",
        default=False,
        help="Janelia employees only",
    )
    PARSER.add_argument(
        "--verbose",
        dest="VERBOSE",
        action="store_true",
        default=False,
        help="Flag, Chatty",
    )
    PARSER.add_argument(
        "--debug",
        dest="DEBUG",
        action="store_true",
        default=False,
        help="Flag, Very chatty",
    )
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    perform_search()
    terminate_program()
