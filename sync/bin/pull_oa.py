"""pull_oa.py
Find DOIs from OA that can be added to the dois collection.
"""

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


def get_dois_from_oa():
    """Get DOIs from oa
    Keyword arguments:
      None
    Returns:
      List of DOIs
    """
    size = 250
    start = 0
    done = False
    check = {}
    parts = 0
    LOGGER.info("Getting DOIs from OA")
    suffix = ""
    while not done:
        if start:
            suffix = f"&size={size}&from={start}"
        else:
            suffix = f"&size={size}"
        response = JRC.call_oa(suffix=suffix)
        if "hits" not in response:
            terminate_program(f"Error in response from OA: {response}")
        for hit in response["hits"]["hits"]:
            COUNT["read"] += 1
            if "_source" in hit and "DOI" in hit["_source"] and hit["_source"]["DOI"]:
                doi = hit["_source"]["DOI"].lower()
            if doi_exists(doi.lower()):
                COUNT["in_dois"] += 1
                continue
            check[doi] = hit
        if (
            "hits" in response
            and "hits" in response["hits"]
            and len(response["hits"]["hits"]) > 0
        ):
            parts += 1
            start += size
        else:
            done = True
    LOGGER.info(f"Got {len(check):,} DOIs from OA in {parts} part(s)")
    return check


def parse_authors(doi, msg, ready):
    """Parse an author record to see if there are any Janelia authors
    Keyword arguments:
      doi: DOI
      msg: Crossref message
      ready: list of DOIs ready for processing
    Returns:
      True if there are Janelia authors, otherwise False
    """
    adet = DL.get_author_details(msg, DB["dis"]["orcid"])
    if adet:
        janelians = []
        for auth in adet:
            if auth["janelian"]:
                janelians.append(f"{auth['given']} {auth['family']} ({auth['match']})")
        if janelians:
            print(f"Janelians found for {doi}: {', '.join(janelians)}")
            ready.append(doi)
            return True
        # If just name matches were found, we're still goint to trust OA that there
        # is at least one [likely alumni] Janelian on the paper.
        ready.append(doi)
    return False


def run_search():
    """Search for DOIs on OA that can be added to the dois collection
    Keyword arguments:
      None
    Returns:
      None
    """
    check = get_dois_from_oa()
    ready = []
    no_janelians = []
    for doi, item in tqdm(check.items(), desc="Crossref check"):
        if DL.is_datacite(doi):
            LOGGER.warning(f"DOI {doi} is a DataCite DOI")
        resp = JRC.call_crossref(doi)
        if resp and "message" in resp:
            janelians = parse_authors(doi, resp["message"], ready)
            if not janelians:
                COUNT["no_janelians"] += 1
                no_janelians.append(doi)
    if ready:
        LOGGER.info("Writing DOIs to oa_ready.txt")
        with open("oa_ready.txt", "w", encoding="ascii") as outstream:
            for item in ready:
                outstream.write(f"{item}\n")
    if no_janelians:
        LOGGER.info("Writing DOIs to oa_no_janelians.txt")
        with open("oa_no_janelians.txt", "w", encoding="ascii") as outstream:
            for item in no_janelians:
                outstream.write(f"{item}\n")
    print(f"DOIs read from OA:               {COUNT['read']:,}")
    print(f"DOIs already in database:        {COUNT['in_dois']:,}")
    print(f"DOIs not in Crossref (asserted): {COUNT['asserted_crossref']:,}")
    print(f"DOIs not in Crossref:            {COUNT['no_crossref']:,}")
    print(f"DOIs with no Janelian authors:   {COUNT['no_janelians']:,}")
    print(f"DOIs ready for processing:       {len(ready):,}")


# -----------------------------------------------------------------------------

if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(description="Sync DOIs from bioRxiv")
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
