"""edit_orcid.py
Edit a record in the orcid collection
"""

__version__ = "1.0.0"

import argparse
import json
from operator import attrgetter
import sys
from bson import json_util
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Database
DB = {}


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


def add_names(lookup_by, lookup, coll):
    """Update names in the orcid record
    Keyword arguments:
      lookup_by: "orcid" or "employeeId"
      lookup: lookup value
      coll: orcid collection
    Returns:
      None
    """
    family = []
    given = []
    if ARG.FAMILY:
        family = [ARG.FAMILY]
    if ARG.GIVEN:
        given = [ARG.GIVEN]
    resp = DL.add_orcid_name(
        lookup_by=lookup_by, lookup=lookup, family=family, given=given, coll=coll
    )
    if resp:
        print(json.dumps(resp, indent=2, default=str))


def update_orcid():
    """Update the orcid record
    Keyword arguments:
      None
    Returns:
      None
    """
    coll = DB["dis"].orcid
    if ARG.FAMILY or ARG.GIVEN:
        lookup_by = "orcid" if ARG.ORCID else "employeeId"
    else:
        lookup_by = "orcid" if ARG.UPDATE == "employeeId" else "employeeId"
    if not ARG.WRITE:
        if ARG.FAMILY or ARG.GIVEN:
            lookup = ARG.ORCID if ARG.ORCID else ARG.EMPLOYEE
        else:
            lookup = ARG.ORCID if ARG.UPDATE == "employeeId" else ARG.EMPLOYEE
        try:
            row = DL.single_orcid_lookup(lookup, coll, lookup_by)
        except Exception as err:
            raise err
        if not row:
            terminate_program(f"Record not found for {lookup_by} {lookup}")
        if ARG.FAMILY or ARG.GIVEN:
            if ARG.FAMILY and ARG.FAMILY not in row["family"]:
                row["family"].append(ARG.FAMILY)
            if ARG.GIVEN and ARG.GIVEN not in row["given"]:
                row["given"].append(ARG.GIVEN)
        else:
            if (
                "employeeId" in row
                and "orcid" in row
                and row["employeeId"] == ARG.EMPLOYEE
                and row["orcid"] == ARG.ORCID
            ):
                print(json_util.dumps(row, indent=2, default=str))
                terminate_program("Record already has entered values")
            row[ARG.UPDATE] = ARG.EMPLOYEE if ARG.UPDATE == "employeeId" else ARG.ORCID
        LOGGER.warning("Would have updated record")
        print(json_util.dumps(row, indent=2, default=str))
        terminate_program()
    if ARG.FAMILY or ARG.GIVEN:
        add_names(lookup_by, ARG.ORCID if ARG.ORCID else ARG.EMPLOYEE, coll)
        terminate_program()
    try:
        if ARG.UPDATE == "orcid":
            print(ARG.EMPLOYEE, ARG.ORCID, ARG.UPDATE)
            resp = DL.update_existing_orcid(ARG.EMPLOYEE, ARG.ORCID, coll, lookup_by)
        else:
            resp = DL.update_existing_orcid(ARG.ORCID, ARG.EMPLOYEE, coll, lookup_by)
    except Exception as err:
        terminate_program(err)
    if resp:
        print(json.dumps(resp, indent=2, default=str))
    else:
        terminate_program("Did not update record")
    if not ARG.WRITE:
        LOGGER.warning("Dry run successful, no updates were made")


# -----------------------------------------------------------------------------

if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(description="Update orcid record")
    PARSER.add_argument("--orcid", dest="ORCID", action="store", help="ORCID")
    PARSER.add_argument(
        "--employee", dest="EMPLOYEE", action="store", help="Employee ID"
    )
    PARSER.add_argument(
        "--family", dest="FAMILY", action="store", help="Family name to add"
    )
    PARSER.add_argument(
        "--given", dest="GIVEN", action="store", help="Given name to add"
    )
    PARSER.add_argument(
        "--update",
        dest="UPDATE",
        action="store",
        default="orcid",
        choices=["orcid", "employeeId"],
        help="Field to update",
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
    if ARG.ORCID:
        if not ARG.EMPLOYEE and not ARG.FAMILY and not ARG.GIVEN:
            terminate_program(
                "At least one of --employee, --family, and --given is required"
            )
    elif ARG.EMPLOYEE:
        if not ARG.ORCID and not ARG.FAMILY and not ARG.GIVEN:
            terminate_program(
                "At least one of --orcid, --family, and --given is required"
            )
    else:
        terminate_program("At least one of --orcid and  --employee is required")
    initialize_program()
    update_orcid()
    terminate_program()
