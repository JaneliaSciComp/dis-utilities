''' Authorization: who may hold one, who holds what, and which pages demand it.

    Authorizations live on the orcid record as a list of cvterm names under the
    cv "authorization". Two rules carry the weight here and are easy to get
    wrong in either direction, so both are pinned hard:

      - "admin" means everything. An administrator passes any check without the
        specific term being listed.
      - A stored grant is not a standing permission. Somebody who loses their
        eligibility keeps the stored list and stops passing every check.

    The functions are extracted with ast rather than imported, because importing
    dis_responder needs Flask, a config file and a database, none of which these
    rules depend on.
'''

import ast
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / 'api' / 'dis_responder.py').read_text(encoding='utf-8')

# The cv as it stands in the database; the helper reads whatever is there.
CV = {'admin': {'display': 'Administrator', 'definition': 'Administrator (full access)'},
      'datacite': {'display': 'DataCite', 'definition': 'Access to DataCite data'}}


def _extract(*names):
    tree = ast.parse(SOURCE)
    ns = {'CVTERM': {'authorization': CV}}
    for name in names:
        node = next(n for n in tree.body
                    if isinstance(n, ast.FunctionDef) and n.name == name)
        exec(ast.get_source_segment(SOURCE, node), ns)    # pylint: disable=W0122
    return ns


NS = _extract('may_be_authorized', 'authorizations')
auths = NS['authorizations']
may_be_authorized = NS['may_be_authorized']


def _p(**kwargs):
    ''' An eligible person: has an iD, not alumni. Anything less holds nothing,
        which the eligibility tests below cover separately.
    '''
    rec = {'orcid': '0000-0002-1825-0097'}
    rec.update(kwargs)
    return rec


# --- eligibility: an ORCID iD, and not alumni ------------------------------

def test_a_record_without_an_orcid_id_cannot_be_authorized():
    ''' A session is keyed by the iD, so a record without one can never be the
        signed-in person. 1,398 of 2,383 roster records are in this state.
    '''
    ok, why = may_be_authorized({'family': ['Nobody']})
    assert ok is False
    assert why == 'no ORCID iD'


def test_an_alumnus_cannot_be_authorized():
    ok, why = may_be_authorized(_p(alumni=True))
    assert ok is False
    assert why == 'alumni'


def test_eligibility_is_stricter_than_signing_in():
    ''' may_sign_in lets a DEPARTED CONTINGENT WORKER keep access. They may sign
        in; they may not hold an authorization. If those two ever have to agree,
        this is the test that will say so.
    '''
    assert may_be_authorized(_p(alumni=True, workerType='Contingent Worker'))[0] is False


def test_alumni_only_bars_when_exactly_true():
    ''' A truthy-but-not-True value (a date string) must not read as departed -
        the same rule may_sign_in follows.
    '''
    assert may_be_authorized(_p(alumni='2024-01-01'))[0] is True


def test_an_eligible_person_passes():
    assert may_be_authorized(_p())[0] is True


def test_nobody_is_not_eligible():
    assert may_be_authorized(None)[0] is False


# --- who holds what --------------------------------------------------------

@pytest.mark.parametrize('person,expected,why', [
    (None, set(), 'signed out'),
    (_p(), set(), 'no field at all - the default'),
    (_p(authorization=[]), set(), 'empty list'),
    (_p(authorization=None), set(), 'null, as a half-written record might have'),
    (_p(authorization=['datacite']), {'datacite'}, 'one grant'),
])
def test_plain_cases(person, expected, why):
    assert auths(person) == expected, why


def test_admin_implies_every_term_in_the_cv():
    ''' The decided rule: "admin" is everything. An administrator must not need
        "datacite" listed as well, or every new term means re-granting to every
        admin.
    '''
    held = auths(_p(authorization=['admin']))
    assert 'datacite' in held
    assert 'admin' in held
    assert held >= set(CV)


def test_a_non_admin_gets_only_what_is_listed():
    assert auths(_p(authorization=['datacite'])) == {'datacite'}
    assert 'admin' not in auths(_p(authorization=['datacite']))


def test_an_unknown_term_is_carried_but_grants_nothing_else():
    ''' A term removed from the cv but still on a record must not vanish
        silently, nor widen anyone's access.
    '''
    assert auths(_p(authorization=['retired_term'])) == {'retired_term'}


def test_empty_strings_are_dropped():
    assert auths(_p(authorization=['', 'datacite'])) == {'datacite'}


def test_a_stored_grant_does_not_survive_losing_eligibility():
    ''' The stored list is a grant, not a standing permission. '''
    assert auths(_p(authorization=['admin'], alumni=True)) == set()
    assert auths({'authorization': ['datacite']}) == set()        # no iD


# --- which pages demand it -------------------------------------------------

def _decorator_above(route):
    ''' The @restricted line directly above a route, if any.

        The route decorator may carry more than the rule - methods=[...] on the
        editable pages - so match to the end of that line rather than assuming
        the rule is the whole call.
    '''
    match = re.search(re.escape(f"@app.route('{route}'") + r"[^\n]*\n(@restricted[^\n]*)\n",
                      SOURCE)
    return match.group(1) if match else None


@pytest.mark.parametrize('route', ['/figshare_users', '/protocolsio_users',
                                   '/dryad_users', '/zenodo_users'])
def test_the_datacite_user_reports_require_datacite(route):
    ''' The four *_users pages in the DataCite menu. '''
    assert _decorator_above(route) == "@restricted('datacite')", route


@pytest.mark.parametrize('route', ['/authorizations', '/authorization/<string:rid>'])
def test_the_admin_pages_require_admin(route):
    assert _decorator_above(route) == "@restricted('admin')", route


def test_the_personal_pages_need_only_a_sign_in():
    ''' These are about the viewer's own record, so any signed-in reader. '''
    for route in ('/mypapers', '/mymetrics', '/newhome', '/profile'):
        assert _decorator_above(route) == '@restricted', route


def test_the_decorator_supports_both_forms():
    ''' @restricted and @restricted('term') must both work, or applying one
        would silently drop the sign-in check beneath it.
    '''
    assert 'term = None if callable(arg) else arg' in SOURCE
    assert 'return decorate(arg) if callable(arg) else decorate' in SOURCE


def test_the_menu_knows_what_each_page_requires():
    ''' The navbar hides what a viewer cannot open; it reads the requirement off
        the view rather than from a hand-kept list.
    '''
    assert 'wrapper.dis_authorization = term' in SOURCE
    gated = SOURCE.split('def _gated_targets')[1].split('\ndef ')[0]
    assert 'dis_authorization' in gated, '_gated_targets must read the requirement'


# --- the guards ------------------------------------------------------------

def test_an_admin_cannot_remove_their_own_admin():
    ''' With one administrator, self-demotion would leave nobody able to grant
        it back short of a hand-written database update.
    '''
    assert 'cannot remove your own Administrator' in SOURCE


def test_a_grant_records_who_and_when():
    for field in ('authorization_updated', 'authorization_updated_by'):
        assert field in SOURCE, field


def test_only_terms_in_the_cv_can_be_granted():
    ''' A posted checkbox name is filtered against the cv, so a crafted form
        cannot invent an authorization.
    '''
    assert "if t in (CVTERM.get('authorization') or {})" in SOURCE


def test_people_are_keyed_by_record_id_not_orcid():
    ''' 1,398 of 2,383 roster records have no ORCID iD. They cannot be granted
        anything, but they are still reachable - and keying the editor on an iD
        would put most of the roster out of reach of even being looked at.
    '''
    assert "@app.route('/authorization/<string:rid>'" in SOURCE
    assert 'bson.ObjectId(rid)' in SOURCE


def test_the_editor_refuses_an_ineligible_grant():
    ''' Enforced on the POST, not only hidden in the UI: the form is not
        rendered for an ineligible person, but a hand-made POST must still fail.
    '''
    assert 'cannot hold an authorization' in SOURCE
    assert "if request.method == 'POST' and not eligible:" in SOURCE


# --- rendering -------------------------------------------------------------

def test_person_label_is_marked_as_html():
    ''' render_table escapes any cell that is not Safe, so a label returning a
        bare str printed its own markup as text on the page. Marked at the
        source rather than at each call site, so a new caller cannot forget.
    '''
    label = SOURCE.split('def _person_label')[1].split('\ndef ')[0]
    assert 'return safe(out)' in label, '_person_label must return a Safe value'


def test_table_cells_carrying_markup_are_marked_safe():
    ''' Every <a> and <span> built into a row of the admin tables sits inside a
        safe(), or render_table will show the tags to the reader.
    '''
    block = SOURCE.split('def show_authorizations')[1].split('\ndef ')[0]
    rows = block.split('rows_html.append(')[1].split(')])')[0] + \
           block.split('frows.append(')[1].split(')])')[0]
    for fragment in re.findall(r"[\"']<(?:a|span)[^\"']*", rows):
        start = rows.index(fragment)
        assert 'safe(' in rows[max(0, start - 200):start], fragment[:50]


def test_the_403_message_is_plain_text():
    ''' message= is autoescaped by the template (Safe.__html__), so HTML
        entities reach the reader literally - "&quot;" printed as &quot; - and a
        pre-escaped value is escaped a second time. The three sinks differ and
        are easy to confuse: render_table cells need safe(), render_warning
        escapes its own argument, and template message=/title= autoescape.
    '''
    block = SOURCE.split('def restricted(')[1].split('\ndef ')[0]
    msg = block.split('This page needs')[1].split('), 403)')[0]
    assert '&quot;' not in msg, 'entities are shown literally in an escaped sink'
    assert 'escape(' not in msg, 'message= is already escaped by the template'


def test_the_profile_reads_authorizations_off_the_orcid_record():
    ''' /profile holds two documents: person (the orcid record, where
        authorizations live) and record (the user_auth row, which carries the
        password and last_login). Reading the wrong one shows "None" to
        everybody, and looks right.
    '''
    block = SOURCE.split('def profile()')[1].split('\ndef ')[0]
    auth = block.split('granted = sorted')[1].split('<tr><td>Authorizations')[0]
    assert 'record.get(' not in auth, 'authorizations live on person, not record'
    assert "person.get('authorization')" in auth


def test_the_profile_shows_what_was_granted_not_what_it_expands_to():
    ''' An administrator holds every term. Listing them all would read as four
        separate grants rather than one, so the implication is stated instead.
    '''
    block = SOURCE.split('def profile()')[1].split('\ndef ')[0]
    assert 'Administrator carries every' in block
