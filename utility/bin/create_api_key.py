''' create_api_key.py
    Create a JWT token for a user
'''

import argparse
from datetime import datetime, timedelta, timezone
import os
import sys
import jwt
import jrc_common.jrc_common as JRC

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

ARG = LOGGER = None

def terminate_program(msg=None):
    ''' Terminate the program gracefully
        Keyword arguments:
          msg: error message
        Returns:
          None
    '''
    if msg:
        LOGGER.critical(msg)
    sys.exit(-1 if msg else 0)


def create_jwt_token():
    ''' Create a JWT token for a user
        Keyword arguments:
          None
        Returns:
          JWT token
    '''
    # Set expiration to 1 year from now
    exp = datetime.now(timezone.utc) + timedelta(days=365*2)
    # Create the JWT payload
    payload = {
        'name': ARG.NAME,
        'email': ARG.EMAIL,
        'exp': exp,
        'verify_signature': False
    }
    # Create the JWT using JWS Compact Serialization
    token = jwt.encode(
        payload,
        os.environ["DIS_KEY"],
        algorithm='HS256'  # HMAC with SHA-256
    )
    return token

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(description='Create a JWT token for a user')
    PARSER.add_argument('--name', dest='NAME', required=True, help='User name')
    PARSER.add_argument('--email', dest='EMAIL', required=True, help='User email')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    if "DIS_KEY" not in os.environ:
        terminate_program("Missing secret key - set in DIS_KEY environment variable")
    print(f"Generated JWT Token:\n{create_jwt_token()}")
