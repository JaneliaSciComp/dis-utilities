""" manage_restricted_users.py
    Manage who may sign in to the restricted DIS pages.

    This is the INTERIM sign-in, in place until the site authenticates against
    orcid.org. A person signs in with their ORCID iD and a password assigned
    here. The iD is the point: the session it establishes holds a real ORCID
    iD, which is exactly what the orcid.org callback will set later, so
    everything built on top of it - the avatar, /profile, authorizations -
    is already working against real orcid documents and survives the switch
    untouched. Only the password half is thrown away.

    Credentials live in the `user_auth` collection, keyed by ORCID iD, and are
    deliberately NOT on the orcid document itself: /orcid/<oid>, /orcid/active,
    /orcid/works/<oid> and /orcidapi/<oid> all project with {'_id': 0}, which
    excludes the id and nothing else, so any field added to an orcid document
    is published as JSON by endpoints that are unauthenticated today.

    Passwords are hashed with scrypt in werkzeug's format (the application
    verifies them with werkzeug); the hash is produced here with the standard
    library so this runs under any python. tests/test_restricted_users.py pins
    the two formats together.

    Granting access requires the iD to be in the orcid collection AND to pass
    the access rule:

        (alumni is not True) or (workerType == 'Contingent Worker')

    so a person who leaves is refused at their next sign-in with no action
    here, and access cannot be granted to somebody the roster does not know.

    When orcid.org sign-in lands: drop this collection, delete this program,
    and remove the password branch from the application.
"""

__version__ = '2.0.0'

import argparse
from getpass import getpass
import hashlib
import re
import secrets
import string
import sys
from datetime import datetime
from operator import attrgetter

import jrc_common.jrc_common as JRC

# Database
DB = {}
# Globals
ARG = LOGGER = None
# The collection holding interim credentials. Separate from `orcid` on purpose
# - see the module docstring.
COLLECTION = 'user_auth'
# 0000-0002-1825-0097 : four groups of four, final character may be X.
ORCID_RE = re.compile(r'^\d{4}-\d{4}-\d{4}-\d{3}[\dX]$')
# werkzeug's own defaults, reproduced with the standard library so this program
# needs no web framework to hash a password. Byte-for-byte identical output.
SCRYPT_N, SCRYPT_R, SCRYPT_P = 32768, 8, 1
SCRYPT_SALT_LEN = 16
SCRYPT_DKLEN = 64
# n=32768, r=8 needs about 128 MiB; the default maxmem of 0 refuses it.
SCRYPT_MAXMEM = 132 * 1024 * 1024
MIN_PASSWORD = 8


def terminate_program(msg=None):
    ''' Terminate the program gracefully
        Keyword arguments:
          msg: error message or object
        Returns:
          None
    '''
    if msg:
        if not isinstance(msg, str):
            msg = f"An exception of type {type(msg).__name__} occurred. Arguments:\n{msg.args}"
        LOGGER.critical(msg)
    sys.exit(-1 if msg else 0)


def initialize_program():
    ''' Intialize the program
        Keyword arguments:
          None
        Returns:
          None
    '''
    try:
        dbconfig = JRC.get_config("databases")
    except Exception as err:
        terminate_program(err)
    dbo = attrgetter(f"dis.{ARG.MANIFOLD}.write")(dbconfig)
    LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, ARG.MANIFOLD, dbo.host, dbo.user)
    try:
        DB['dis'] = JRC.connect_database(dbo)
    except Exception as err:
        terminate_program(err)


def generate_password_hash(password):
    ''' Hash a password in werkzeug's scrypt format
        Keyword arguments:
          password: plain-text password
        Returns:
          "scrypt:N:r:p$salt$hex", as werkzeug would produce
    '''
    salt = ''.join(secrets.choice(string.ascii_letters + string.digits)
                   for _ in range(SCRYPT_SALT_LEN))
    digest = hashlib.scrypt(password.encode(), salt=salt.encode(), n=SCRYPT_N,
                            r=SCRYPT_R, p=SCRYPT_P, maxmem=SCRYPT_MAXMEM,
                            dklen=SCRYPT_DKLEN).hex()
    return f"scrypt:{SCRYPT_N}:{SCRYPT_R}:{SCRYPT_P}${salt}${digest}"


def may_sign_in(person):
    ''' Whether an orcid document is allowed to sign in
        Keyword arguments:
          person: orcid collection document
        Returns:
          (True, None) or (False, reason)
    '''
    if person.get('alumni') is True and person.get('workerType') != 'Contingent Worker':
        return False, (f"{person.get('workerType') or 'This person'} is flagged as alumni and is "
                       "not a contingent worker, so the access rule excludes them")
    return True, None


def person_for(orcid):
    ''' The orcid document for an iD, if it may sign in
        Keyword arguments:
          orcid: ORCID iD
        Returns:
          The document, or None after reporting why not
    '''
    try:
        person = DB['dis'].orcid.find_one({"orcid": orcid})
    except Exception as err:
        terminate_program(err)
    if not person:
        LOGGER.error("%s is not in the orcid collection, so access cannot be granted", orcid)
        return None
    allowed, why = may_sign_in(person)
    if not allowed:
        LOGGER.error("%s: %s", orcid, why)
        return None
    return person


def display_name(person):
    ''' A readable name from an orcid document
        Keyword arguments:
          person: orcid collection document
        Returns:
          Name string
    '''
    given = (person.get('given') or [''])[0]
    family = (person.get('family') or [''])[0]
    return f"{given} {family}".strip() or '(no name)'


def ask_password(name):
    ''' Prompt twice for a password, without echoing it
        Keyword arguments:
          name: who the password is for, for the prompt
        Returns:
          Password string
    '''
    while True:
        first = getpass(f"Password for {name}: ")
        if not first:
            print("  Empty password - try again.")
            continue
        if first != getpass("Repeat password: "):
            print("  They did not match - try again.")
            continue
        if len(first) < MIN_PASSWORD:
            # A short password on a page that is only mildly sensitive is still
            # a password somebody will reuse. Refused rather than warned about.
            print(f"  Please use at least {MIN_PASSWORD} characters.")
            continue
        return first


def describe_activity(last_seen):
    ''' How recently somebody was using the site
        Keyword arguments:
          last_seen: datetime, or None
        Returns:
          Readable string
    '''
    if not last_seen:
        return '-'
    secs = (datetime.now() - last_seen).total_seconds()
    if secs < 120:
        return 'now'
    if secs < 3600:
        return f"{int(secs // 60)} min ago"
    if secs < 86400:
        return f"{int(secs // 3600)} hr ago"
    return f"{int(secs // 86400)} days ago"


def list_users():
    ''' Report who may sign in
        Keyword arguments:
          None
        Returns:
          None
    '''
    try:
        rows = list(DB['dis'][COLLECTION].find({}).sort("orcid", 1))
    except Exception as err:
        terminate_program(err)
    if not rows:
        print(f"No credentials in {COLLECTION}. Nobody can sign in; the restricted "
              "pages will send everyone to the sign-in form and refuse them.")
        return
    print(f"{len(rows)} credential(s):\n")
    print(f"  {'ORCID iD':22} {'name':26} {'added':12} {'last sign-in':20} {'active'}")
    for row in rows:
        person = DB['dis'].orcid.find_one({"orcid": row['orcid']})
        if person:
            name = display_name(person)
            allowed, _ = may_sign_in(person)
            if not allowed:
                name += "  [BLOCKED]"
        else:
            name = "*** not in orcid collection ***"
        last = row.get('last_login')
        print(f"  {row['orcid']:22} {name:26} {str(row.get('created', ''))[:10]:12} "
              f"{(str(last)[:19] if last else 'never'):20} {describe_activity(row.get('last_seen'))}")
    print("\nActivity is when the person last loaded a page, recorded at most once a")
    print("minute. A signed cookie leaves no server-side record, so this is the")
    print("closest thing to a session list - somebody idle for an hour may still")
    print("hold a valid session, and closing the browser ends it without a trace.")


def add_user():
    ''' Add or update a credential
        Keyword arguments:
          None
        Returns:
          None
    '''
    orcid = (ARG.ORCID or input("ORCID iD: ")).strip()
    if not ORCID_RE.match(orcid):
        terminate_program(f"{orcid} is not an ORCID iD (expected 0000-0000-0000-0000)")
    person = person_for(orcid)
    if not person:
        terminate_program("Refusing to grant access")
    try:
        existing = DB['dis'][COLLECTION].find_one({"orcid": orcid})
    except Exception as err:
        terminate_program(err)
    if existing and not ARG.FORCE:
        terminate_program(f"{orcid} already has a password. Use --force to change it.")
    name = display_name(person)
    print(f"\n  {orcid}  {name}")
    if existing:
        print("  (changing an existing password)")
    password = ask_password(name)
    now = datetime.now()
    record = {"orcid": orcid, "password": generate_password_hash(password), "updated": now}
    if not existing:
        record['created'] = now
    if not ARG.WRITE:
        print(f"\n  Would {'update' if existing else 'add'} {orcid} ({name}) "
              f"in {COLLECTION}. Use --write to commit.")
        return
    try:
        DB['dis'][COLLECTION].update_one({"orcid": orcid}, {"$set": record}, upsert=True)
    except Exception as err:
        terminate_program(err)
    print(f"\n  {'Updated' if existing else 'Added'} {orcid} ({name}).")
    print("  Effective immediately - no deployment restart is needed.")


def remove_user():
    ''' Remove a credential
        Keyword arguments:
          None
        Returns:
          None
    '''
    orcid = ARG.REMOVE.strip()
    try:
        existing = DB['dis'][COLLECTION].find_one({"orcid": orcid})
    except Exception as err:
        terminate_program(err)
    if not existing:
        terminate_program(f"{orcid} has no credential in {COLLECTION}")
    person = DB['dis'].orcid.find_one({"orcid": orcid})
    name = display_name(person) if person else '(not in orcid collection)'
    if not ARG.WRITE:
        print(f"  Would remove {orcid} ({name}) from {COLLECTION}. Use --write to commit.")
        return
    try:
        DB['dis'][COLLECTION].delete_one({"orcid": orcid})
    except Exception as err:
        terminate_program(err)
    print(f"  Removed {orcid} ({name}).")
    print("  Any session they already hold stays valid until it is cleared - rotate")
    print("  DIS_SESSION_SECRET if you need them out immediately.")


def audit():
    ''' Report credentials whose person no longer qualifies
        Keyword arguments:
          None
        Returns:
          None
    '''
    try:
        rows = list(DB['dis'][COLLECTION].find({}))
    except Exception as err:
        terminate_program(err)
    stale = []
    for row in rows:
        person = DB['dis'].orcid.find_one({"orcid": row['orcid']})
        if not person:
            stale.append((row['orcid'], 'no longer in the orcid collection'))
            continue
        allowed, why = may_sign_in(person)
        if not allowed:
            stale.append((row['orcid'], why))
    if not stale:
        print(f"All {len(rows)} credential(s) still qualify.")
        return
    print(f"{len(stale)} of {len(rows)} credential(s) no longer qualify:\n")
    for orcid, why in stale:
        print(f"  {orcid}  {why}")
    print("\nThese people are already refused at sign-in - the application applies the")
    print("same rule. Removing them here is tidying, not a security fix.")


def processing():
    ''' Dispatch
        Keyword arguments:
          None
        Returns:
          None
    '''
    if ARG.LIST:
        list_users()
    elif ARG.AUDIT:
        audit()
    elif ARG.REMOVE:
        remove_user()
    else:
        add_user()


# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Manage interim sign-in credentials for the restricted DIS pages",
        epilog="Credentials are keyed by ORCID iD and stored in the user_auth collection. "
               "This is temporary, pending orcid.org sign-in.")
    PARSER.add_argument('--orcid', dest='ORCID', action='store',
                        help='ORCID iD to add (prompted for if omitted)')
    PARSER.add_argument('--remove', dest='REMOVE', action='store',
                        help='Remove this ORCID iD\'s credential')
    PARSER.add_argument('--list', dest='LIST', action='store_true', default=False,
                        help='List who may sign in')
    PARSER.add_argument('--audit', dest='AUDIT', action='store_true', default=False,
                        help='Report credentials whose person no longer qualifies')
    PARSER.add_argument('--force', dest='FORCE', action='store_true', default=False,
                        help='Allow changing an existing password')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, [prod])')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Write to database')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    processing()
    terminate_program()
