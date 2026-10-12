''' The interim ORCID sign-in: hash format, iD format, and the access rule.

    manage_restricted_users.py builds a werkzeug-format scrypt hash using only
    the standard library, so that adding a reader does not require a web
    framework. That is a copy of somebody else's format, and a copy can drift -
    so the compatibility test here hashes with the tool and verifies with the
    real werkzeug, which is the only assertion that would actually catch drift.

    The access rule is the other thing worth pinning. It decides who may sign
    in, it is not obvious (alumni means *departed*), and it is written out in
    two places - the tool and the application - which is exactly the shape of
    thing that drifts apart silently.
'''

import importlib.util
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
TOOL = ROOT / 'utility' / 'bin' / 'manage_restricted_users.py'


def _load():
    ''' Import the tool without running it. jrc_common is not importable in
        every environment, so stub it out: nothing under test touches it.
    '''
    import sys
    import types
    if 'jrc_common' not in sys.modules:
        pkg = types.ModuleType('jrc_common')
        sub = types.ModuleType('jrc_common.jrc_common')
        sub.get_config = sub.connect_database = sub.setup_logging = lambda *a, **k: None
        pkg.jrc_common = sub
        sys.modules['jrc_common'] = pkg
        sys.modules['jrc_common.jrc_common'] = sub
    spec = importlib.util.spec_from_file_location('manage_restricted_users', TOOL)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


MRU = _load()


# --- the hash the application has to be able to verify ---------------------

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


# --- the iD format ---------------------------------------------------------

@pytest.mark.parametrize('orcid', ['0000-0002-1825-0097', '0000-0001-2345-6789',
                                   '0000-0002-9830-241X'])
def test_real_orcid_ids_are_accepted(orcid):
    assert MRU.ORCID_RE.match(orcid)


@pytest.mark.parametrize('bad', ['', 'rob', '0000-0002-1825', '0000-0002-1825-00977',
                                 '0000000218250097', '0000-0002-1825-009Y',
                                 'https://orcid.org/0000-0002-1825-0097',
                                 ' 0000-0002-1825-0097'])
def test_malformed_ids_are_rejected(bad):
    ''' A malformed iD must not reach a database lookup. '''
    assert not MRU.ORCID_RE.match(bad)


# --- the access rule -------------------------------------------------------
#
# DECIDED 2026-10-12: an alumnus keeps access if and only if they are a
# contingent worker. `alumni` means DEPARTED - it is set with an alumni_date
# when someone drops out of People, and cleared when they return.

@pytest.mark.parametrize('person,allowed,why', [
    ({'workerType': 'Employee'}, True, 'current employee'),
    ({'workerType': 'Contingent Worker'}, True, 'current contingent worker'),
    ({'alumni': True, 'workerType': 'Contingent Worker'}, True,
     'departed contingent worker - the exception the rule exists for'),
    ({'alumni': True, 'workerType': 'Employee'}, False, 'departed employee'),
    ({'alumni': True, 'workerType': 'SVC'}, False, 'departed, some other type'),
    ({'alumni': True}, False, 'departed, no worker type at all'),
    ({'alumni': False, 'workerType': 'Employee'}, True, 'explicitly not alumni'),
    ({}, True, 'nothing known against them'),
])
def test_access_rule(person, allowed, why):
    assert MRU.may_sign_in(person)[0] is allowed, why


def test_alumni_is_only_a_bar_when_exactly_true():
    ''' A truthy-but-not-True value (a date string, say) must not be read as
        "departed" by one implementation and "present" by the other.
    '''
    assert MRU.may_sign_in({'alumni': '2024-01-01', 'workerType': 'Employee'})[0] is True


def test_the_rule_is_written_the_same_way_in_both_places():
    ''' The tool decides who to GRANT a password to; the application decides
        who to LET IN. They must agree, and they are separate code because the
        tool cannot import the Flask app. Compare the expressions directly so
        a change to one without the other fails here rather than in production.
    '''
    pattern = re.compile(
        r"alumni'\)\s*is\s*True\s*and\s*person\.get\('workerType'\)\s*!=\s*'Contingent Worker'")
    tool = TOOL.read_text(encoding='utf-8')
    app = (ROOT / 'api' / 'dis_responder.py').read_text(encoding='utf-8')
    assert pattern.search(tool), "the tool's access rule changed shape"
    assert pattern.search(app), "the application's access rule no longer matches the tool's"
