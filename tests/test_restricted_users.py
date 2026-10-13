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


def test_the_html_pattern_is_not_eaten_by_the_f_string():
    ''' {4} inside an f-string is a replacement field, so writing the ORCID
        quantifier inline rendered pattern="\\d4-\\d4-\\d4-\\d3[\\dX]" and the
        browser refused every real iD with "Please match the requested format".
        The attribute is now derived from the server-side regex; this asserts
        nobody puts the literal back.
    '''
    app = (ROOT / 'api' / 'dis_responder.py').read_text(encoding='utf-8')
    assert 'pattern="{html_pattern}"' in app, "the iD pattern is no longer derived"
    assert 'html_pattern = ORCID_RE.pattern' in app, "derive it from ORCID_RE, do not retype it"


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


# --- the avatar ------------------------------------------------------------
#
# avatar_html lives in dis_responder, which needs Flask and a good deal else,
# so these import it only when that is possible.

def _responder():
    return pytest.importorskip('dis_responder')


def test_initials_come_from_given_and_family():
    D = _responder()
    out = str(D.avatar_html({'orcid': '0000-0001-2345-6789',
                             'given': ['Nelson'], 'family': ['Spruston']}))
    assert '>NS</span>' in out
    assert 'dis-avatar-initials' in out


def test_a_person_with_no_name_still_gets_a_badge():
    ''' Rendering must not fail on a sparse record. '''
    D = _responder()
    out = str(D.avatar_html({'orcid': '0000-0001-2345-6789', 'given': [], 'family': []}))
    assert '>?</span>' in out


def test_a_photograph_wins_over_initials():
    D = _responder()
    out = str(D.avatar_html({'orcid': '0000-0001-2345-6789', 'given': ['A'], 'family': ['B']},
                            photo_url='https://example.org/a.jpg'))
    assert '<img src=' in out
    assert 'dis-avatar-initials' not in out


def test_the_badge_colour_is_stable_for_a_person():
    ''' Someone's badge should not change between page loads. '''
    D = _responder()
    person = {'orcid': '0000-0001-2345-6789', 'given': ['A'], 'family': ['B']}
    assert str(D.avatar_html(person)) == str(D.avatar_html(person))


def test_two_people_do_not_share_a_clustered_colour():
    ''' ORCID iDs are nearly all digits, so summing their characters put two
        colleagues two degrees apart. The hue comes from a digest instead.
    '''
    import re
    D = _responder()
    hues = []
    for oid in ('0000-0003-3118-1636', '0000-0002-9830-2415', '0000-0002-5795-0635'):
        out = str(D.avatar_html({'orcid': oid, 'given': ['A'], 'family': ['B']}))
        hues.append(int(re.search(r'hsl\((\d+)', out).group(1)))
    assert len(set(hues)) == 3
    assert max(hues) - min(hues) > 10, f"hues are clustered: {hues}"


def test_a_hostile_photo_url_cannot_break_out_of_the_attribute():
    D = _responder()
    out = str(D.avatar_html({'orcid': '0000-0001-2345-6789', 'given': ['A'], 'family': ['B']},
                            photo_url="x' onerror='alert(1)"))
    assert "onerror='alert(1)" not in out


# --- a shared ORCID iD -----------------------------------------------------

def test_a_shared_orcid_is_refused_rather_than_guessed():
    ''' Two roster records on one iD must not resolve to "whichever Mongo
        returned first" on pages that show a named person their own
        publications and citation metrics. /mypapers already refuses to guess
        (merge_roster_records); current_user has to refuse too.
    '''
    app = (ROOT / 'api' / 'dis_responder.py').read_text(encoding='utf-8')
    assert 'find({"orcid": orcid}).limit(2)' in app, \
        'current_user must detect a shared iD, not find_one past it'
    assert 'dis_user_ambiguous' in app
    assert 'More than one person on the Janelia roster' in app, \
        'the ambiguous case needs an explanation, not a redirect loop'
