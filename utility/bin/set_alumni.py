"""set_alumni.py
Set (or unset) the alumni tag for a given user
"""

__version__ = "1.0.0"

import argparse
import json
from operator import attrgetter
import sys
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Database
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


def processing():
    """Set alumni tag
    Keyword arguments:
      None
    Returns:
      None
    """
    coll = DB["dis"].orcid
    if ARG.ORCID:
        lookup = ARG.ORCID
        lookup_by = "orcid"
    else:
        lookup = ARG.EMPLOYEE
        lookup_by = "employeeId"
    try:
        row = DL.single_orcid_lookup(lookup, coll, lookup_by)
    except Exception as err:
        terminate_program(err)
    if not row:
        terminate_program(f"User for {lookup} not found")
    print(json.dumps(row, indent=2, default=str))
    if ARG.UNSET and "alumni" not in row:
        terminate_program("The alumni tag is not set for this user")
    elif not ARG.UNSET and "alumni" in row:
        terminate_program("The alumni tag is already set for this user")
    oper = "unset" if ARG.UNSET else "set"
    if ARG.WRITE:
        LOGGER.warning(f"Alumni tag will be {oper}")
    if ARG.WRITE:
        try:
            if ARG.UNSET:
                coll.update_one({"_id": row["_id"]}, {"$unset": {"alumni": ""}})
            else:
                coll.update_one({"_id": row["_id"]}, {"$set": {"alumni": True}})
        except Exception as err:
            terminate_program(err)
        LOGGER.warning(f"The alumni tag has been {oper}")
    else:
        LOGGER.warning(f"The alumni tag would have been {oper}")


# -----------------------------------------------------------------------------

if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(description="Set alumni tag")
    UGROUP = PARSER.add_mutually_exclusive_group(required=True)
    UGROUP.add_argument("--orcid", dest="ORCID", action="store", help="ORCID")
    UGROUP.add_argument(
        "--employee", dest="EMPLOYEE", action="store", help="Employee ID"
    )
    PARSER.add_argument(
        "--unset",
        dest="UNSET",
        action="store_true",
        default=False,
        help="Unset alumni tag",
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
        help="Write to database",
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
