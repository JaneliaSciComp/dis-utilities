"""fix_middle_names.py
Expand the number of given names in the orcid collection.
There are two different modes:
(default): Add a given name without a period following a middle initial
(--period): Add a period to the end of the given name if it is a space
            followed by a middle initial
Both modes will also look for the opportunity to add just a first name.
Be sure to first run this without --write - there may be some strange given names
that will generate equally strage results!
"""

import argparse
from operator import attrgetter
import re
import sys
import jrc_common.jrc_common as JRC

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Database
DB = {}
# Counters
COUNT = {"read": 0, "found": 0}


def terminate_program(msg=None):
    """Terminate the program gracefully
    Keyword arguments:
      msg: error message
    Returns:
      None
    """
    if msg:
        LOGGER.critical(msg)
    sys.exit(-1 if msg else 0)


def initialize_program():
    """Intialize the program
    Keyword arguments:
      None
    Returns:
      None
    """
    # Database
    try:
        dbconfig = JRC.get_config("databases")
    except Exception as err:
        terminate_program(err)
    dbs = ["dis"]
    for source in dbs:
        dbo = attrgetter(f"{source}.{ARG.MANIFOLD}.write")(dbconfig)
        LOGGER.info(
            "Connecting to %s %s on %s as %s",
            dbo.name,
            ARG.MANIFOLD,
            dbo.host,
            dbo.user,
        )
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)


def process_single_add_period(row):
    """Add a given name with a period after the middle initial
    Keyword arguments:
      row: a single row from the orcid collection
    Returns:
      None
    """
    COUNT["read"] += 1
    found = False
    for given in row["given"]:
        if given[-1] == ".":
            found = True
            COUNT["found"] += 1
            break
    if not found:
        LOGGER.warning(f"Adding given name(s) {row['given']}")
        payload = {"given": row["given"]}
        for given in row["given"]:
            if re.search(r" [A-Z]$", given):
                payload["given"].append(given + ".")
                break
        found = False
        for given in row["given"]:
            if re.search(r"^[A-Za-z]+$", given):
                found = True
                break
        if not found:
            given = row["given"][0].split(" ")[0]
            payload["given"].append(given)
        print(payload)
        if ARG.WRITE:
            try:
                DB["dis"].orcid.update_one({"_id": row["_id"]}, {"$set": payload})
            except Exception as err:
                terminate_program(err)


def process_single_add_no_period(row):
    """Add a given name with no period after the middle initial
    Keyword arguments:
      row: a single row from the orcid collection
    Returns:
      None
    """
    COUNT["read"] += 1
    found = False
    for given in row["given"]:
        if re.search(r"^[A-Z]\. [A-Z]\.$", given) or re.search(r" [A-Z]$", given):
            found = True
            COUNT["found"] += 1
            break
    if not found:
        LOGGER.warning(f"Adding given name(s) to {row['given']}")
        payload = {"given": row["given"]}
        for given in row["given"]:
            if re.search(r" [A-Z].$", given):
                payload["given"].append(given.replace(".", ""))
                break
        found = False
        for given in row["given"]:
            if re.search(r"^[A-Za-z]+$", given):
                found = True
                break
        if not found:
            given = row["given"][0].split(" ")[0]
            payload["given"].append(given)
        print(payload)
        if ARG.WRITE:
            try:
                DB["dis"].orcid.update_one({"_id": row["_id"]}, {"$set": payload})
            except Exception as err:
                terminate_program(err)


def process_orcid():
    """Find and process given names in the orcid collection
    Keyword arguments:
      None
    Returns:
      None
    """
    payload = (
        {"given": {"$regex": " [A-Z]$"}}
        if ARG.PERIOD
        else {"given": {"$regex": r" [A-Z]\.$"}}
    )
    try:
        rows = DB["dis"].orcid.find(payload)
    except Exception as err:
        terminate_program(err)
    for row in rows:
        if ARG.PERIOD:
            process_single_add_period(row)
        else:
            process_single_add_no_period(row)
    print(f"ORCID read:                {COUNT['read']}")
    print(f"ORCIDs not needing update: {COUNT['found']}")
    if not ARG.WRITE:
        LOGGER.warning("Dry run successful, no updates were made")


# -----------------------------------------------------------------------------

if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(
        description="Update orcid collection with additional given names"
    )
    PARSER.add_argument(
        "--period",
        dest="PERIOD",
        action="store_true",
        help="Add a period to middle initials",
    )
    PARSER.add_argument(
        "--manifold",
        dest="MANIFOLD",
        action="store",
        default="prod",
        choices=["dev", "prod"],
        help="MongoDB manifold (dev, prod)",
    )
    PARSER.add_argument(
        "--write",
        dest="WRITE",
        action="store_true",
        default=False,
        help="Write to database/config system",
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
    process_orcid()
    terminate_program()
