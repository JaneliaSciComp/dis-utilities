""" manage_restricted_users.py
    Manage who may read the restricted platform user pages (/figshare_users,
    /protocolsio_users, /zenodo_users, /dryad_users).

    Those pages are gated by two environment variables, both of which belong in
    the dis-utilities-secrets Secret that the OpenShift deployment already pulls
    in through envFrom:

      DIS_SESSION_SECRET  signs the session cookie
      DIS_RESTRICTED_USERS    name:hash pairs, comma-separated

    There is no file to edit - the values live in the Secret (production) or in
    your shell (local development). This program builds the strings and prints
    the commands that install them; it never contacts the cluster itself, so a
    mistake here cannot change production.

    Passwords are hashed before they go anywhere, so no password is stored or
    printed back. The hash is werkzeug's scrypt format, which is what the
    application verifies against, but it is produced here with the standard
    library alone: werkzeug arrives with Flask and so lives in the api venv,
    and this is a command-line tool that should run under whichever python is
    to hand. tests/test_restricted_users.py pins the two formats together.

    Adding somebody MERGES with the existing roster rather than replacing it -
    rewriting DIS_RESTRICTED_USERS with a single entry is how you would silently
    lock out everybody else.
"""

__version__ = '1.0.0'

import argparse
from getpass import getpass
import hashlib
import os
import secrets
import string
import sys

ARG = None
# Matches the application's reader: split on the FIRST colon only, because a
# werkzeug hash ("scrypt:32768:8:1$...") contains colons of its own.
SEPARATOR = ':'
# werkzeug's own defaults, reproduced here so this program needs nothing but the
# standard library. werkzeug ships with Flask and so lives in the api venv; this
# is a command-line tool that should run under whichever python is to hand, and
# a web framework is a strange thing to install to hash a password.
# The output is byte-for-byte what werkzeug's generate_password_hash produces,
# which test_restricted_users.py asserts against the real library.
SCRYPT_N, SCRYPT_R, SCRYPT_P = 32768, 8, 1
SCRYPT_SALT_LEN = 16
SCRYPT_DKLEN = 64
# n=32768, r=8 needs about 128 MiB; the default maxmem of 0 refuses it.
SCRYPT_MAXMEM = 132 * 1024 * 1024


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


def parse_users(raw):
    ''' Parse a DIS_RESTRICTED_USERS value
        Keyword arguments:
          raw: the variable's value, or None
        Returns:
          Dict of name to hash, insertion-ordered
    '''
    users = {}
    for entry in (raw or '').split(','):
        entry = entry.strip()
        if SEPARATOR not in entry:
            continue
        name, _, digest = entry.partition(SEPARATOR)
        name, digest = name.strip(), digest.strip()
        if name and digest:
            users[name] = digest
    return users


def format_users(users):
    ''' Render a roster back into a DIS_RESTRICTED_USERS value
        Keyword arguments:
          users: dict of name to hash
        Returns:
          Comma-separated string
    '''
    return ','.join(f"{name}{SEPARATOR}{digest}" for name, digest in users.items())


def current_roster():
    ''' The roster to start from: --current wins, else the environment
        Returns:
          Dict of name to hash
    '''
    if ARG.CURRENT:
        return parse_users(ARG.CURRENT)
    return parse_users(os.environ.get('DIS_RESTRICTED_USERS'))


def ask_password(name):
    ''' Prompt twice for a password, without echoing it
        Keyword arguments:
          name: user name, for the prompt
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
        if len(first) < 8:
            # A short password on a page that is only mildly sensitive is still
            # a password somebody will reuse. Refused rather than warned about.
            print("  Please use at least 8 characters.")
            continue
        return first


def show_install(users, session_secret=None):
    ''' Print the commands that install these values
        Keyword arguments:
          users: dict of name to hash
          session_secret: a freshly generated secret, or None
        Returns:
          None
    '''
    value = format_users(users)
    print("\n" + "=" * 78)
    print(f"DIS_RESTRICTED_USERS now holds {len(users)} reader(s): {', '.join(users) or '(none)'}")
    print("=" * 78)
    if session_secret:
        print("\nA new DIS_SESSION_SECRET was generated. Note that installing it signs")
        print("EVERYONE out, which is also how you revoke access in a hurry.\n")
    print("OpenShift (production) - both replicas read the same Secret, which is what\n"
          "keeps a session alive when the Service moves a browser between pods:\n")
    cmd = "  oc -n dis-utilities set data secret/dis-utilities-secrets \\\n" \
          f"      DIS_RESTRICTED_USERS='{value}'"
    if session_secret:
        cmd += f" \\\n      DIS_SESSION_SECRET='{session_secret}'"
    print(cmd)
    print("\n  oc -n dis-utilities rollout restart deployment/dis-utilities")
    print("\nLocal development - note DIS_ALLOW_HTTP_SESSION, without which the signed\n"
          "cookie is dropped over plain HTTP and the sign-in appears to fail:\n")
    print(f"  export DIS_RESTRICTED_USERS='{value}'")
    print(f"  export DIS_SESSION_SECRET='{session_secret or '<your existing secret>'}'")
    print("  export DIS_ALLOW_HTTP_SESSION=1")
    print()


def show_secret_only(session_secret):
    ''' Print the commands that install a new session secret, roster untouched
        Keyword arguments:
          session_secret: the generated secret
        Returns:
          None
    '''
    print("\n" + "=" * 78)
    print("New DIS_SESSION_SECRET - installing this signs EVERYONE out immediately,")
    print("which is how you revoke access in a hurry. The roster is unchanged.")
    print("=" * 78)
    print("\n  oc -n dis-utilities set data secret/dis-utilities-secrets \\")
    print(f"      DIS_SESSION_SECRET='{session_secret}'")
    print("\n  oc -n dis-utilities rollout restart deployment/dis-utilities")
    print(f"\n  export DIS_SESSION_SECRET='{session_secret}'   # local development")
    print()


def processing():
    ''' Add, remove or list readers
        Keyword arguments:
          None
        Returns:
          None
    '''
    users = current_roster()
    session_secret = secrets.token_hex(32) if ARG.NEW_SECRET else None
    if ARG.LIST:
        if not users:
            print("No readers configured (DIS_RESTRICTED_USERS is empty or unset).")
            print("Nobody can open the restricted pages; they answer 503.")
        else:
            print(f"{len(users)} reader(s) configured:")
            for name in users:
                print(f"  {name}")
        if session_secret:
            # --list --new-secret is the revoke path: rotate the signing key and
            # every existing cookie dies, without disturbing the roster. Listing
            # and returning early here would have dropped the secret silently.
            show_secret_only(session_secret)
        return
    if ARG.REMOVE:
        if ARG.REMOVE not in users:
            print(f"{ARG.REMOVE} is not in the roster. Present: {', '.join(users) or '(none)'}")
            sys.exit(1)
        del users[ARG.REMOVE]
        show_install(users, session_secret)
        return
    name = ARG.USER or input("User name: ").strip()
    if not name:
        print("A user name is required.")
        sys.exit(1)
    if SEPARATOR in name or ',' in name:
        # Either character would corrupt the parse on the way back in.
        print(f"A user name cannot contain '{SEPARATOR}' or ','.")
        sys.exit(1)
    if name in users and not ARG.FORCE:
        print(f"{name} already has access. Re-run with --force to change the password.")
        sys.exit(1)
    users[name] = generate_password_hash(ask_password(name))
    show_install(users, session_secret)


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Manage readers of the restricted platform user pages",
        epilog="The roster is read from DIS_RESTRICTED_USERS in the environment unless "
               "--current is given. Nothing is written anywhere: the commands to "
               "install the result are printed for you to run.")
    PARSER.add_argument('--user', dest='USER', action='store',
                        help='User name to add (prompted for if omitted)')
    PARSER.add_argument('--remove', dest='REMOVE', action='store',
                        help='Remove this user from the roster')
    PARSER.add_argument('--list', dest='LIST', action='store_true', default=False,
                        help='List the configured readers and exit')
    PARSER.add_argument('--current', dest='CURRENT', action='store',
                        help='Existing DIS_RESTRICTED_USERS value to merge into, if it '
                             'is not in this shell (e.g. copied out of the Secret)')
    PARSER.add_argument('--new-secret', dest='NEW_SECRET', action='store_true',
                        default=False,
                        help='Also generate a DIS_SESSION_SECRET (signs everyone out)')
    PARSER.add_argument('--force', dest='FORCE', action='store_true', default=False,
                        help='Allow changing the password of an existing user')
    ARG = PARSER.parse_args()
    processing()
