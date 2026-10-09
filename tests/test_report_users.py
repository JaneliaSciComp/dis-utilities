''' The roster format shared by manage_report_users.py and the application.

    manage_report_users.py builds a werkzeug-format scrypt hash using only the
    standard library, so that adding a reader does not require a web framework.
    That is a copy of somebody else's format, and a copy can drift - so the
    compatibility test here hashes with the tool and verifies with the real
    werkzeug, which is the only assertion that would actually catch drift.

    The parsing tests need nothing installed: they pin the one genuinely
    delicate rule, which is that a hash contains colons of its own and so only
    the FIRST colon separates a name from it.
'''

import importlib.util
from pathlib import Path

import pytest

TOOL = Path(__file__).resolve().parents[1] / 'utility' / 'bin' / 'manage_report_users.py'


def _load():
    spec = importlib.util.spec_from_file_location('manage_report_users', TOOL)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


MRU = _load()


# --- the format the application has to be able to read ---------------------

def test_hash_is_werkzeug_scrypt_shaped():
    digest = MRU.generate_password_hash('hunter2hunter2')
    method, salt, body = digest.split('$')
    assert method == 'scrypt:32768:8:1'
    assert len(salt) == MRU.SCRYPT_SALT_LEN
    assert len(body) == MRU.SCRYPT_DKLEN * 2       # hex


def test_werkzeug_accepts_what_the_tool_produces():
    ''' The assertion that matters: our hash, their verifier. '''
    security = pytest.importorskip('werkzeug.security')
    digest = MRU.generate_password_hash('hunter2hunter2')
    assert security.check_password_hash(digest, 'hunter2hunter2')
    assert not security.check_password_hash(digest, 'hunter2hunter3')


def test_salt_differs_between_calls():
    ''' Same password, different hash - or the salt is not doing its job. '''
    assert MRU.generate_password_hash('samepassword') \
        != MRU.generate_password_hash('samepassword')


# --- parsing ---------------------------------------------------------------

def test_a_hash_keeps_its_own_colons():
    ''' scrypt:32768:8:1$... is mostly colons. Splitting on all of them would
        hand the application a truncated hash that never verifies.
    '''
    digest = MRU.generate_password_hash('hunter2hunter2')
    users = MRU.parse_users(f"rob:{digest}")
    assert users == {'rob': digest}


def test_round_trip_through_format_and_parse():
    users = {'alice': MRU.generate_password_hash('alicepassword'),
             'bob': MRU.generate_password_hash('bobpassword11')}
    assert MRU.parse_users(MRU.format_users(users)) == users


@pytest.mark.parametrize('raw', ['', None, '   ', 'no-colon-here', ',,,'])
def test_unusable_values_yield_nobody(raw):
    ''' An empty roster must mean nobody, not everybody - the application
        returns 503 on this rather than opening the page.
    '''
    assert MRU.parse_users(raw) == {}


def test_entries_are_stripped_of_incidental_whitespace():
    digest = MRU.generate_password_hash('hunter2hunter2')
    assert MRU.parse_users(f"  rob : {digest}  ") == {'rob': digest}
