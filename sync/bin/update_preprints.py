"""update_preprints.py
Update the jrc_preprint field in the dois collection for all locally-stored DOIs.
Every preprint (from DataCite and Crossref) will be compared to every "primary" DOI
(from Crossref) to determine if each pair is the same publication. The publication
pair must have a RapidFuzz score greater than or equal to the threshold value.
The first and last author for each pair must also match using the same criteria.
For each pair with a title/author match, a relationship will be created between the
DOIs. When all DOIs have been processed, the relationships will be written to the
jrc_preprint field in the dois collection.
"""

__version__ = "1.0.0"

import argparse
import collections
from datetime import datetime
from operator import attrgetter
import sys
import pandas as pd
from rapidfuzz import fuzz, utils
from tqdm import tqdm
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Database
DB = {}
# Counters
COUNT = collections.defaultdict(lambda: 0, {})
# DOIs
PRIMARY = {}
PREPRINT = {}
PRIMARYREL = {}
PREPRINTREL = {}
# Output data
AUDIT = []
MATCH = {
    "DOI": [],
    "Title": [],
    "Score": [],
    "First author": [],
    "First author score": [],
    "Last author": [],
    "Last author score": [],
    "Publishing date": [],
    "Decision": [],
}
MISSING = {}


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
            f"Connecting to {dbo.name} {ARG.MANIFOLD} on {dbo.host} as {dbo.user}"
        )
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)
    LOGGER.info("Getting DOIs")
    projection = {
        "_id": 0,
        "DOI": 1,
        "doi": 1,
        "title": 1,
        "titles": 1,
        "author": 1,
        "creators": 1,
        "relation": 1,
        "published": 1,
        "published-print": 1,
        "published-online": 1,
        "posted": 1,
        "created": 1,
        "registered": 1,
    }
    try:
        # Primary DOIs will all be from Crossref
        rows = DB["dis"].dois.find({"type": "journal-article"}, projection)
    except Exception as err:
        terminate_program(err)
    for row in rows:
        PRIMARY[row["doi"]] = row
    LOGGER.info(f"Primary DOIs: {len(PRIMARY):,}")
    try:
        # Preprints will be from Crossref or DataCite
        rows = DB["dis"].dois.find(
            {
                "$or": [{"type": "posted-content"}, {"jrc_obtained_from": "DataCite"}],
                "doi": {"$not": {"$regex": "^10.25378/janelia."}},
            },
            projection,
        )
    except Exception as err:
        terminate_program(err)
    for row in rows:
        PREPRINT[row["doi"]] = row
    LOGGER.info(f"Preprint DOIs: {len(PREPRINT):,}")


def make_relationships(prerec, primrec):
    """Make relationships based on DOI record "relation" field
    Keyword arguments:
      prerec: preprint record
      primrec: primary record
    Returns:
      None
    """
    if "relation" in primrec and "has-preprint" in primrec["relation"]:
        for rec in primrec["relation"]["has-preprint"]:
            if "id-type" in rec and rec["id-type"] == "doi":
                make_doi_relationships(rec["id"], primrec["doi"])
    if "relation" in prerec and "is-preprint-of" in prerec["relation"]:
        for rec in prerec["relation"]["is-preprint-of"]:
            if "id-type" in rec and rec["id-type"] == "doi":
                make_doi_relationships(prerec["doi"], rec["id"])


def make_doi_relationships(predoi, primdoi):
    """Make relationships between two DOIs
    Keyword arguments:
      predoi: preprint DOI
      primdoi: primary DOI
    Returns:
      None
    """
    # Find DOIs mnissing from dois collection
    predoi = predoi.lower()
    if predoi not in PREPRINT:
        MISSING[predoi] = True
    primdoi = primdoi.lower()
    if primdoi not in PRIMARY:
        MISSING[primdoi] = True
    # Preprint -> Primary
    if predoi not in PREPRINTREL:
        PREPRINTREL[predoi] = []
    if primdoi not in PREPRINTREL[predoi]:
        LOGGER.debug(f"Adding primary {primdoi} to {PREPRINTREL[predoi]}")
        PREPRINTREL[predoi].append(primdoi)
        COUNT["preprint_relations"] += 1
    # Primary -> Preprint
    if primdoi not in PRIMARYREL:
        PRIMARYREL[primdoi] = []
    if predoi not in PRIMARYREL[primdoi]:
        LOGGER.debug(f"Adding preprint {predoi} to {PRIMARYREL[primdoi]}")
        PRIMARYREL[primdoi].append(predoi)
        COUNT["primary_relations"] += 1


def process_pair(prerec, primrec):
    predoi = prerec["doi"]
    primdoi = primrec["doi"]
    pretitle = DL.get_title(prerec)
    primtitle = DL.get_title(primrec)
    if "relation" in prerec or "relation" in primrec:
        make_relationships(prerec, primrec)
    COUNT["comparisons"] += 1
    score = fuzz.token_sort_ratio(pretitle, primtitle, processor=utils.default_process)
    if score < ARG.THRESHOLD:
        return
    authors = DL.get_author_list(prerec, returntype="list")
    prefirst = authors[0]
    prelast = authors[-1]
    authors = DL.get_author_list(primrec, returntype="list")
    primfirst = authors[0]
    primlast = authors[-1]
    MATCH["DOI"].extend([predoi, primdoi])
    MATCH["Title"].extend([pretitle, primtitle])
    MATCH["Score"].extend([score, score])
    MATCH["First author"].extend([prefirst, primfirst])
    first_score = fuzz.token_sort_ratio(
        prefirst, primfirst, processor=utils.default_process
    )
    MATCH["First author score"].extend([first_score, first_score])
    MATCH["Last author"].extend([prelast, primlast])
    last_score = fuzz.token_sort_ratio(
        prelast, primlast, processor=utils.default_process
    )
    MATCH["Last author score"].extend([last_score, last_score])
    MATCH["Publishing date"].extend(
        [DL.get_publishing_date(prerec), DL.get_publishing_date(primrec)]
    )
    COUNT["title_match"] += 1
    if (first_score >= ARG.THRESHOLD) and (last_score >= ARG.THRESHOLD):
        make_doi_relationships(predoi, primdoi)
        MATCH["Decision"].extend(["Relate", "Relate"])
        COUNT["title_author_match"] += 1
    else:
        MATCH["Decision"].extend(["", ""])


def write_to_database():
    """Write relationships to the database
    Keyword arguments:
      None
    Returns:
      None
    """
    for predoi, primdois in tqdm(PREPRINTREL.items(), desc="Write preprints"):
        AUDIT.append(f"{predoi} -> {primdois}")
        if not ARG.WRITE:
            continue
        try:
            DB["dis"].dois.update_one(
                {"doi": predoi}, {"$set": {"jrc_preprint": primdois}}
            )
        except Exception as err:
            terminate_program(err)
    for primdoi, predois in tqdm(PRIMARYREL.items(), desc="Write primaries"):
        AUDIT.append(f"{primdoi} -> {predois}")
        if not ARG.WRITE:
            continue
        try:
            DB["dis"].dois.update_one(
                {"doi": primdoi}, {"$set": {"jrc_preprint": predois}}
            )
        except Exception as err:
            terminate_program(err)


def add_jrc_preprint():
    """Update the jrc_preprint field in the dois collection
    Keyword arguments:
      None
    Returns:
      None
    """
    for prerec in tqdm(PREPRINT.values(), desc="Preprints"):
        for primrec in PRIMARY.values():
            process_pair(prerec, primrec)
    # Write to dois collection
    write_to_database()
    # Output files
    timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    if AUDIT:
        file_name = f"audit_{timestamp}.txt"
        with open(file_name, "w", encoding="utf-8") as ostream:
            for line in AUDIT:
                ostream.write(f"{line}\n")
        LOGGER.warning(f"Audit written to {file_name}")
    if MATCH["DOI"]:
        file_name = f"title_matches_{timestamp}.xlsx"
        df = pd.DataFrame.from_dict(MATCH)
        df.to_excel(file_name, index=False)
        LOGGER.warning(f"Title matches written to {file_name}")
    if MISSING:
        file_name = f"missing_dois_{timestamp}.txt"
        with open(file_name, "w", encoding="utf-8") as ostream:
            for line in MISSING:
                ostream.write(f"{line}\n")
        LOGGER.warning(f"Missing DOIs written to {file_name}")
    # Summary
    print(f"Primary DOIs:                 {len(PRIMARY):,}")
    print(f"Preprint DOIs:                {len(PREPRINT):,}")
    print(f"Comparisons:                  {COUNT['comparisons']:,}")
    print(f"Title matches:                {COUNT['title_match']:,}")
    print(f"Title/author matches:         {COUNT['title_author_match']:,}")
    print(f"Preprint DOIs with relations: {len(PREPRINTREL):,}")
    print(f"Primary DOIs with relations:  {len(PRIMARYREL):,}")
    print(f"Preprint relations:           {COUNT['preprint_relations']:,}")
    print(f"Primary relations:            {COUNT['primary_relations']:,}")
    if not ARG.WRITE:
        LOGGER.warning("Dry run successful, no updates were made")


# -----------------------------------------------------------------------------

if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(
        description="Update jrc_preprint in the dois collection"
    )
    PARSER.add_argument(
        "--threshold",
        dest="THRESHOLD",
        action="store",
        default=90,
        type=int,
        help="Fuzzy matching threshold",
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
    add_jrc_preprint()
    terminate_program()
