"""find_unloaded_relations.py
Find referenced DOIs that have not been loaded
"""

__version__ = "1.0.0"

import argparse
from operator import attrgetter
import sys
import jrc_common.jrc_common as JRC

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Database
DB = {}
# References
REFERENCES = (
    "has-preprint",
    "is-preprint-of",
    "is-supplement-to",
    "is-supplemented-by",
)


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
        dbo = attrgetter(f"{source}.{ARG.MANIFOLD}.read")(dbconfig)
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


def processing():
    """Main processing routine
    Keyword arguments:
      None
    Returns:
      None
    """
    coll = DB["dis"].dois
    loaded_dois = {}
    LOGGER.info("Finding DOIs")
    try:
        rows = coll.find({})
    except Exception as err:
        terminate_program(err)
    for row in rows:
        loaded_dois[row["doi"]] = True
    LOGGER.info(f"Loaded DOIs: {len(loaded_dois):,}")
    unloaded = {}
    LOGGER.info("Finding unloaded supplements")
    try:
        payload = {"relation": {"$exists": True}}
        rows = coll.find(payload)
    except Exception as err:
        terminate_program(err)
    for row in rows:
        relations = row["relation"]
        for rel in relations:
            if rel in REFERENCES:
                for itm in relations[rel]:
                    if itm["id-type"] == "doi" and itm["id"] not in loaded_dois:
                        unloaded[itm["id"]] = True
    if unloaded:
        with open("unloaded_relations.txt", "w", encoding="ascii") as file:
            for doi in unloaded:
                file.write(f"{doi}\n")
    print(f"Unloaded relations: {len(unloaded):,}")


# -----------------------------------------------------------------------------

if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(
        description="Find referenced DOIs that have not been loaded"
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
    processing()
    terminate_program()
