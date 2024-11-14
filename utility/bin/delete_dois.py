"""delete_dois.py
Delete DOIs from the dois collection
"""

import argparse
import collections
from operator import attrgetter
import sys
from tqdm import tqdm
import jrc_common.jrc_common as JRC

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Database
DB = {}

# Counters
COUNT = collections.defaultdict(lambda: 0, {})


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


def delete_dois():
    """Delete DOIs from the database
    Keyword arguments:
      None
    Returns:
      None
    """
    dois = []
    try:
        with open(ARG.FILE, "r", encoding="ascii") as instream:
            for doi in instream.read().splitlines():
                dois.append(doi.lower().strip())
    except Exception as err:
        LOGGER.error(f"Could not process {ARG.FILE}")
        terminate_program(err)
    for doi in tqdm(dois):
        COUNT["read"] += 1
        try:
            row = DB["dis"].dois.find_one({"doi": doi})
        except Exception as err:
            terminate_program(err)
        if not row:
            COUNT["missing"] += 1
            LOGGER.warning(f"DOI {doi} not found")
            continue
        if (
            "jrc_authors" in row
            or "jrc_first_auohor" in row
            or "jrc_last_auohor" in row
        ):
            LOGGER.error(f"DOI {doi} has Janelia authors")
            continue
        if ARG.WRITE:
            try:
                resp = DB["dis"].dois.delete_one({"doi": doi})
                COUNT["deleted"] += resp.deleted_count
            except Exception as err:
                terminate_program(err)
    print(f"DOIs read:    {COUNT['read']}")
    print(f"DOIs missing: {COUNT['missing']}")
    print(f"DOIs deleted: {COUNT['deleted']}")


# -----------------------------------------------------------------------------

if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(description="Delete DOIs from the dois collection")
    PARSER.add_argument(
        "--file", dest="FILE", action="store", help="File of DOIs to process"
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
        help="Actually delete DOIs",
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
    DISCONFIG = JRC.simplenamespace_to_dict(JRC.get_config("dis"))
    initialize_program()
    delete_dois()
    terminate_program()
