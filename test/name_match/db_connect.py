from operator import attrgetter
import sys
import jrc_common.jrc_common as JRC


class DummyArg:
    def __init__(self):
        self.VERBOSE = False
        self.DEBUG = False


DB = {}
PROJECT = {}


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
        manifold = "prod"
        dbo = attrgetter(f"{source}.{manifold}.write")(dbconfig)
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)
    try:
        rows = DB["dis"].project_map.find({})
    except Exception as err:
        terminate_program(err)
    for row in rows:
        PROJECT[row["name"]] = row["project"]


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
