"""group_search.py
Find resources authored by groups (non-individuals) and write to a file
"""

import argparse
import configparser
from operator import attrgetter
import sys
import doi_common.doi_common as DL
import jrc_common.jrc_common as JRC

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

DB = {}


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
        manifold = ARG.MANIFOLD if source == "dis" else "prod"
        dbo = attrgetter(f"{source}.{manifold}.write")(dbconfig)
        LOGGER.info(
            "Connecting to %s %s on %s as %s", dbo.name, manifold, dbo.host, dbo.user
        )
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)


def search_single_group(group):
    """Find DOIs that are authored by a given group
    Keyword arguments:
      group: group to check
    Returns:
      List of DOI records
    """
    suffix = CONFIG["crossref"]["name"]
    complete = False
    parts = 0
    records = []
    coll = DB["dis"].dois
    while not complete:
        try:
            if parts:
                resp = JRC.call_crossref(
                    f"{suffix}{group}&offset={parts*1000}", timeout=20
                )
            else:
                resp = JRC.call_crossref(f"{suffix}{group}", timeout=20)
        except Exception as err:
            terminate_program(err)
        recs = resp["message"]["items"]
        if not recs:
            break
        parts += 1
        for rec in recs:
            row = coll.find_one({"doi": rec["DOI"]})
            if not row:
                records.append(rec)
    return records


def perform_search():
    """Find DOIs that are authored by groups
    Keyword arguments:
      None
    Returns:
      None
    """
    new_doi = {}
    for group in ("COSEM", "CellMap", "FlyLight", "FlyEM", "GENIE"):
        print(f"Getting group-authored resources for {group}")
        rows = search_single_group(group)
        LOGGER.info(f"{group}: {len(rows)}")
        if not rows:
            continue
        for row in rows:
            authors = DL.get_author_list(row)
            try:
                authors = DL.get_author_list(row)
                if not authors:
                    continue
            except Exception as _:
                LOGGER.warning(f"Could not find authors for {row['DOI']}")
                continue
            if "Project" not in authors:
                continue
            if group == "GENIE" and "The GENIE Project" not in authors:
                continue
            new_doi[row["DOI"]] = authors
    if new_doi:
        with open("new_group_dois.txt", "w", encoding="ascii") as outstream:
            for doi in new_doi:
                outstream.write(f"{doi}\n")
        LOGGER.warning("Wrote DOI file new_group_dois.txt")


# -----------------------------------------------------------------------------

if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(
        description="Search Crossref for named (group) authors"
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
    CONFIG = configparser.ConfigParser()
    CONFIG.read("config.ini")
    initialize_program()
    perform_search()
    terminate_program()
