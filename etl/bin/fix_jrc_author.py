"""fix_jrc_author.py
Add jrc_author field to DOIs. DOIs are selected by employee ID or by
the absence of the jrc_author field.
"""

__version__ = "1.1.0"

import argparse
import collections
from operator import attrgetter
import sys
from tqdm import tqdm
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Database
DB = {}
# Counters
COUNT = collections.defaultdict(lambda: 0, {})


def terminate_program(msg=None):
    """Terminate the program gracefully
    Keyword arguments:
      msg: error message
    Returns:
      None
    """
    if msg:
        if not isinstance(msg, str):
            msg = f"An exception of type {type(msg).__name__} occurred. Arguments:\n{msg.args}"
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


def get_dois():
    """Get a list of DOIs to process
    Keyword arguments:
      None
    Returns:
      cnt: row count
      rows: rows object
    """
    if ARG.EMPLOYEE:
        try:
            orc = DB["dis"].orcid.find_one({"employeeId": ARG.EMPLOYEE})
        except Exception as err:
            terminate_program(err)
        if not orc:
            terminate_program(f"Employee ID {ARG.EMPLOYEE} not found")
        payload = {
            "$and": [
                {
                    "$or": [
                        {"author.given": {"$in": orc["given"]}},
                        {"creators.givenName": {"$in": orc["given"]}},
                    ]
                },
                {
                    "$or": [
                        {"author.family": {"$in": orc["family"]}},
                        {"creators.familyName": {"$in": orc["family"]}},
                    ]
                },
            ]
        }
    else:
        payload = {
            "$or": [{"author": {"$exists": True}}, {"creators": {"$exists": True}}],
            "jrc_author": {"$exists": False},
        }
    try:
        cnt = DB["dis"].dois.count_documents(payload)
        rows = DB["dis"].dois.find(payload)
    except Exception as err:
        terminate_program(err)
    return cnt, rows


def add_jrc_author():
    """Update tags for specified DOIs
    Keyword arguments:
      None
    Returns:
      None
    """
    LOGGER.info(f"Started run (version {__version__})")
    cnt, rows = get_dois()
    for row in tqdm(rows, total=cnt):
        COUNT["read"] += 1
        auth = DL.update_jrc_author(
            row["doi"], DB["dis"].dois, DB["dis"].orcid, write=ARG.WRITE
        )
        if auth:
            COUNT["updated"] += 1
            LOGGER.debug(f"{row['doi']} {auth}")
    print(f"DOIs read:    {COUNT['read']:,}")
    print(f"DOIs updated: {COUNT['updated']:,}")
    if not ARG.WRITE:
        LOGGER.warning("Dry run successful, no updates were made")


# -----------------------------------------------------------------------------

if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(description="Add jrc_author")
    PARSER.add_argument(
        "--employee", dest="EMPLOYEE", action="store", help="Employee ID"
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
    add_jrc_author()
    terminate_program()
