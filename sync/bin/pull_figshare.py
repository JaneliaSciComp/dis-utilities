"""pull_figshare.py
Pull resources from figshare
"""

import argparse
import collections
import configparser
from operator import attrgetter
import sys
import requests
import jrc_common.jrc_common as JRC

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Database
DB = {}
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
    """Initialize database connection
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


def doi_exists(doi):
    """Check if DOI exists in the database
    Keyword arguments:
      doi: DOI to check
    Returns:
      True if exists, False otherwise
    """
    try:
        row = DB["dis"]["dois"].find_one({"doi": doi})
    except Exception as err:
        terminate_program(err)
    return bool(row)


def pull_single_group(dois, institution=None, group=None):
    """Pull DOIs for one group
    Keyword arguments:
      dois: list of DOIs to process
      institution: institution to process
      group: figshare group to process
    Returns:
      None
    """
    if institution:
        stype = "institution"
        sterm = institution
    else:
        stype = "group"
        sterm = group
    base = f"{CONFIG['figshare']['base']}{CONFIG['figshare'][stype]}{sterm}"
    offset = 0
    parts = 0
    done = False
    LOGGER.info(f"Getting DOIs from figshare for {stype} {sterm}")
    while not done:
        resp = requests.get(f"{base}&offset={offset}", timeout=10)
        if resp.status_code == 200:
            parts += 1
            data = resp.json()
            for art in data:
                COUNT["checked"] += 1
                if art["doi"].startswith("10.25378"):
                    COUNT["janelia"] += 1
                if doi_exists(art["doi"]):
                    COUNT["in_dois"] += 1
                else:
                    dois.append(art["doi"].lower())
            offset += 500
        else:
            done = True
    LOGGER.info(f"Checked {COUNT['checked']:,} DOIs from figshare in {parts} part(s)")


def pull_figshare():
    """Pull DOIs from figshare
    Keyword arguments:
      None
    Returns:
      None
    """

    dois = []
    pull_single_group(dois, institution=295)
    # for group in (11380, 49461):
    #    pull_single_group(dois, group=group)
    if dois:
        LOGGER.info(f"Got {len(dois):,} DOIs from figshare")
        LOGGER.info("Writing DOIs to figshare_dois.txt")
        with open("figshare_dois.txt", "w", encoding="ascii") as outstream:
            for doi in dois:
                outstream.write(f"{doi}\n")
    print(f"DOIs read from figshare:   {COUNT['checked']:,}")
    print(f"Janelia DOIs:              {COUNT['janelia']:,}")
    print(f"DOIs already in database:  {COUNT['in_dois']:,}")
    print(f"DOIs ready for processing: {len(dois)}")


# -----------------------------------------------------------------------------

if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(description="Pull resources from figshare")
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
    CONFIG = configparser.ConfigParser()
    CONFIG.read("config.ini")
    pull_figshare()
