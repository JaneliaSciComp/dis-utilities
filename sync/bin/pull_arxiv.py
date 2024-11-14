"""pull_arxiv.py
Find DOIs from arXiv that can be added to the dois collection.
"""

import argparse
import collections
import json
from operator import attrgetter
import re
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


def get_dois_from_arxiv():
    """Get DOIs from arXiv
    Keyword arguments:
      None
    Returns:
      List of DOIs
    """
    offset = 0
    batch_size = 10
    done = False
    check = {}
    parts = 0
    LOGGER.info("Getting DOIs from arXiv")
    while not done:
        post = f"&start={offset}&max_results={batch_size}"
        query = f"all:janelia{post}"
        LOGGER.debug(query)
        response = JRC.call_arxiv(query)
        if "feed" in response and "entry" in response["feed"]:
            entry = response["feed"]["entry"]
            parts += 1
            LOGGER.debug(f"Part {parts:,} with {len(entry):,} entries")
            if len(entry) < batch_size:
                done = True
            else:
                offset += batch_size
            for item in entry:
                COUNT["read"] += 1
                if not isinstance(item, dict):
                    LOGGER.error(f"Item is not a dictionary: {item}")
                    continue
                try:
                    doi = item["id"].split("/")[-1]
                except Exception as err:
                    print(json.dumps(item, indent=2))
                    terminate_program(err)
                doi = re.sub(r"v\d+$", "", doi)  # Remove version
                doi = f"10.48550/arxiv.{doi}"
                if doi_exists(doi.lower()):
                    COUNT["in_dois"] += 1
                    continue
                check[doi.lower()] = item
        else:
            done = True
    LOGGER.info(f"Got {len(check):,} DOIs from arXiv in {parts} part(s)")
    return check


def parse_authors(doi, msg, ready, review):
    """Parse an author record to see if there are any Janelia authors
    Keyword arguments:
      doi: DOI
      msg: DataCite message
      ready: list of DOIs ready for processing
      review: list of DOIs requiring review
    Returns:
      True if there are Janelia authors, otherwise False
    """
    adet = DL.get_author_details(msg, DB["dis"]["orcid"])
    if adet:
        janelians = []
        mode = None
        for auth in adet:
            if auth["janelian"]:
                janelians.append(f"{auth['given']} {auth['family']} ({auth['match']})")
                if auth["match"] in ("ORCID", "asserted"):
                    COUNT["asserted"] += 1
                    mode = auth["match"]
        if janelians:
            print(f"Janelians found for {doi}: {', '.join(janelians)}")
            if mode:
                ready.append(doi)
            else:
                review.append(doi)
            return True
    return False


def run_search():
    """Search for DOIs on arXiv that can be added to the dois collection
    Keyword arguments:
      None
    Returns:
      None
    """
    check = get_dois_from_arxiv()
    ready = []
    review = []
    for doi, item in tqdm(check.items(), desc="DataCite check"):
        resp = JRC.call_datacite(doi)
        if resp and "data" in resp:
            janelians = parse_authors(doi, resp["data"]["attributes"], ready, review)
            if not janelians:
                COUNT["no_janelians"] += 1
    if ready:
        LOGGER.info("Writing DOIs to arxiv_ready.txt")
        with open("arxiv_ready.txt", "w", encoding="ascii") as outstream:
            for item in ready:
                outstream.write(f"{item}\n")
    if review:
        LOGGER.info("Writing DOIs to arxiv_review.txt")
        with open("arxiv_review.txt", "w", encoding="ascii") as outstream:
            for item in review:
                outstream.write(f"{item}\n")
    print(f"DOIs read from arXiv:            {COUNT['read']:,}")
    print(f"DOIs already in database:        {COUNT['in_dois']:,}")
    print(f"DOIs in DataCite (asserted):     {COUNT['asserted']:,}")
    print(f"DOIs not in DataCite:            {COUNT['no_crossref']:,}")
    print(f"DOIs with no Janelian authors:   {COUNT['no_janelians']:,}")
    print(f"DOIs ready for processing:       {len(ready):,}")
    print(f"DOIs requiring review:           {len(review):,}")


# -----------------------------------------------------------------------------

if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(description="Sync DOIs from arXiv")
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
    run_search()
    terminate_program()
