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
HTML_SOURCE = (ROOT / 'api' / 'dis_html.py').read_text(encoding='utf-8')

# The cv as it stands in the database; the helper reads whatever is there.
CV = {'admin': {'display': 'Administrator', 'definition': 'Administrator (full access)'},
      'datacite': {'display': 'DataCite', 'definition': 'Access to DataCite data'}}


def _extract(*names):
    tree = ast.parse(SOURCE)
    ns = {'CVTERM': {'authorization': CV}, 'SERVICE_KIND': 'service'}
    for name in names:
        node = next(n for n in tree.body
                    if isinstance(n, ast.FunctionDef) and n.name == name)
        exec(ast.get_source_segment(SOURCE, node), ns)    # pylint: disable=W0122
    return ns


# is_service_account is pulled in because authorizations() now branches on it;
# may_act too, which that branch calls. Neither runs for a person document.
NS = _extract('may_be_authorized', 'is_service_account', 'may_act', 'authorizations')
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


@pytest.mark.parametrize('route', ['/authorizations', '/person/<string:rid>'])
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
    for field in ('updated', 'updated_by'):
        assert field in SOURCE, field


def test_only_terms_in_the_cv_can_be_granted():
    ''' A posted checkbox name is filtered against the cv, so a crafted form
        cannot invent an authorization.
    '''
    assert "if t in vocabulary" in SOURCE


def test_people_are_keyed_by_record_id_not_orcid():
    ''' 1,398 of 2,383 roster records have no ORCID iD. They cannot be granted
        anything, but they are still reachable - and keying the editor on an iD
        would put most of the roster out of reach of even being looked at.
    '''
    assert "@app.route('/person/<string:rid>'" in SOURCE
    assert 'bson.ObjectId(rid)' in SOURCE


def test_the_editor_refuses_an_ineligible_grant():
    ''' Enforced on the POST, not only hidden in the UI: the form is not
        rendered for an ineligible person, but a hand-made POST must still fail.
    '''
    assert 'cannot hold an authorization' in SOURCE
    assert 'elif not eligible:' in SOURCE


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


# --- review findings -------------------------------------------------------

def test_an_admin_passes_even_if_the_cv_failed_to_load():
    ''' authorizations() expands "admin" by reading the cv. If the cvterm load
        failed at start-up, that expansion is empty and an administrator would
        be silently stripped of everything but "admin" itself - so the checks
        test for admin directly rather than relying on the expansion.
    '''
    block = SOURCE.split('def restricted(')[1].split('\ndef ')[0]
    assert "not {'admin', term} & authorizations(person)" in block
    nav = SOURCE.split('def generate_navbar')[1].split('\ndef ')[0]
    assert "not {'admin', term} & held" in nav


def test_the_checkboxes_are_built_disabled_not_patched_afterwards():
    ''' They were produced with single quotes and then string-replaced on
        double quotes, so the replacement never fired and an ineligible
        person's boxes rendered live.
    '''
    block = SOURCE.split('def edit_person')[1].split('\ndef ')[0]
    assert 'locked = "" if eligible else " disabled"' in block
    assert "boxes.replace(" not in block, 'build the state, do not patch it'


def test_an_inert_grant_is_marked_in_the_admin_list():
    ''' A holder can lose eligibility after being granted. /profile tells the
        holder; the admin list has to tell the administrator, or the grant
        reads as active.
    '''
    block = SOURCE.split('def show_authorizations')[1].split('\ndef ')[0]
    assert 'not in effect' in block
    assert 'may_be_authorized(orc)' in block


# --- editing name variants --------------------------------------------------

class _Form(dict):
    def getlist(self, key):
        return []


def _save_name_harness():
    ''' _save_name with its Flask and Mongo dependencies stubbed, plus the list
        of update operations it issued.
    '''
    import datetime as _dt
    writes = []

    class _Coll:
        def update_one(self, _flt, operation):
            writes.append(operation)

    req = type('R', (), {'form': _Form()})()
    ns = {'request': req,
          'render_warning': lambda msg, kind='warning': (kind, msg),
          'DB': {'dis': type('D', (), {'orcid': _Coll()})()},
          'print': lambda *a, **k: None,
          'datetime': _dt.datetime}
    tree = ast.parse(SOURCE)
    node = next(n for n in tree.body
                if isinstance(n, ast.FunctionDef) and n.name == '_save_name')
    exec(ast.get_source_segment(SOURCE, node), ns)    # pylint: disable=W0122

    def call(orc, action, field, value):
        writes.clear()
        req.form = _Form({'action': action, 'field': field, 'value': value})
        return ns['_save_name'](orc, {'orcid': 'ADMIN'}), list(writes)
    return call


save_name = _save_name_harness()
PERSON = {'_id': 1, 'given': ['Rob', 'Robert'], 'family': ['Svirskas']}


def test_the_last_given_name_cannot_be_removed():
    ''' orc['given'][0] is indexed without a guard in five places in
        dis_responder and in several sync programs, so an empty list is an
        IndexError on the person pages and a crash in the nightly run.
    '''
    (kind, msg), writes = save_name({'_id': 1, 'given': ['Rob'], 'family': ['S']},
                                    'remove', 'given', 'Rob')
    assert kind == 'warning'
    assert writes == [], 'nothing may be written'
    assert 'only given name' in msg


def test_the_last_family_name_cannot_be_removed():
    (kind, _), writes = save_name({'_id': 1, 'given': ['Rob'], 'family': ['S']},
                                  'remove', 'family', 'S')
    assert kind == 'warning'
    assert writes == []


def test_removing_one_of_several_pulls_it():
    ''' Not the first entry - see the preferred-name test below. '''
    (kind, _), writes = save_name(PERSON, 'remove', 'given', 'Robert')
    assert kind == 'success'
    assert writes[0]['$pull'] == {'given': 'Robert'}


def test_removing_the_first_entry_warns_but_still_removes():
    ''' Position 0 is where update_janelians_from_people writes the name HHMI
        holds, re-inserting it on every run. The removal is allowed - an admin
        may have a reason - but it is said plainly, or it looks like this page
        failed when the value reappears overnight.
    '''
    (kind, msg), writes = save_name(PERSON, 'remove', 'given', 'Rob')
    assert writes[0]['$pull'] == {'given': 'Rob'}, 'the removal must still happen'
    assert kind == 'warning'
    assert 'People sync' in msg


def test_the_first_entry_is_marked_in_the_list():
    ''' The warning is unattachable to anything the reader can see unless the
        entries are shown in stored order, so the list is not sorted.
    '''
    block = SOURCE.split('def _name_rows')[1].split('\ndef ')[0]
    assert 'values = [v for v in (orc.get(field) or []) if v]' in block, \
        'stored order, not sorted'
    assert 'first = idx == 0' in block


def test_adding_uses_addtoset():
    ''' Never a whole-array $set: add_orcid_name_variants, fix_middle_names and
        update_janelians_from_people all write these lists, and rebuilding the
        array from a stale read would discard what they added in between.
    '''
    (kind, _), writes = save_name(PERSON, 'add', 'given', 'Bob')
    assert kind == 'success'
    assert writes[0]['$addToSet'] == {'given': 'Bob'}


def test_no_name_write_ever_sets_the_whole_array():
    for action, value in (('add', 'Bob'), ('remove', 'Rob')):
        _, writes = save_name(PERSON, action, 'given', value)
        for operation in writes:
            assert set(operation.get('$set', {})) <= {'updated', 'updated_by'}, \
                '$set may only carry the audit fields, never given/family'


@pytest.mark.parametrize('action,field,value,why', [
    ('remove', 'given', 'Nope', 'not on the record'),
    ('add', 'given', 'Rob', 'already there'),
    ('add', 'given', '   ', 'blank'),
    ('add', 'nickname', 'X', 'not an editable field'),
])
def test_rejected_without_writing(action, field, value, why):
    (kind, _), writes = save_name(PERSON, action, field, value)
    assert kind == 'warning', why
    assert writes == [], why


def test_a_name_change_records_who_and_when():
    _, writes = save_name(PERSON, 'add', 'given', 'Bob')
    assert writes[0]['$set']['updated_by'] == 'ADMIN'
    assert 'updated' in writes[0]['$set']


# --- alumni status and adding an ORCID iD ----------------------------------

def _flat(text):
    """ Join adjacent string-concatenation pieces.

        Prose in this file is built by concatenation, so a phrase routinely
        spans a literal boundary and a plain substring check misses it. Four
        separate assertions have been written wrong this way.
    """
    return re.sub(r'"\s*\\?\s*\n\s*\+?\s*(f?)"', '', text)


def _person_field():
    return _flat(SOURCE[SOURCE.index('def _save_person_field'):
                        SOURCE.index('def _save_authorizations')])


def test_an_admin_cannot_mark_themselves_a_former_employee():
    ''' Alumni revokes sign-in (unless contingent worker) AND every
        authorization, so self-marking would lose the page it was done from -
        the self-demotion hole reached by a different route.
    '''
    block = _person_field()
    assert "if wanted and str(orc['_id']) == str(me.get('_id')):" in block
    assert 'cannot mark yourself a former employee' in block


def test_clearing_alumni_also_clears_the_date():
    ''' Left behind, alumni_date reads as a departure date for somebody who has
        not departed.
    '''
    block = _person_field()
    assert '"$unset": {"alumni": "", "alumni_date": ""}' in block


def test_clearing_alumni_preserves_the_date_rather_than_destroying_it():
    ''' The departure date cannot be recomputed - People has no record of the
        person, which is why they were marked alumni at all. Unchecking the box
        must not be the end of it.
    '''
    block = _person_field()
    assert "keep['alumni_date_prior'] = str(orc['alumni_date'])[:10]" in block
    assert '"$set": keep' in block


def test_prior_date_is_not_restored_automatically():
    ''' Unchecking the box is the only way the flag clears, so a prior date means
        somebody left, returned, and is leaving again - their first departure is
        the wrong date for their second. Re-marking still dates today.
    '''
    block = _person_field()
    setting = block.split('if wanted:')[1].split('        else:')[0]
    assert '"alumni_date": datetime.now().strftime("%Y-%m-%d")' in setting
    assert 'alumni_date_prior' not in setting


def test_setting_alumni_records_a_date():
    assert '"alumni_date": datetime.now().strftime("%Y-%m-%d")' in _person_field()


def test_an_orcid_id_is_add_only():
    ''' The iD is what a session is keyed by and what apply_orcids fills in, so
        changing or removing one here would detach a person from their own
        sign-in and from the works matched by it.
    '''
    block = _person_field()
    assert "if orc.get('orcid'):" in block
    assert 'cannot be changed or removed here' in block


def test_an_added_orcid_is_validated_and_unique():
    ''' Two roster records on one iD is exactly the ambiguity current_user()
        refuses to resolve, so allowing it would manufacture a lockout.
    '''
    block = _person_field()
    assert 'ORCID_RE.match(oid)' in block
    assert 'find_one({"orcid": oid})' in block
    assert 'already on another roster record' in block


def test_the_orcid_pattern_is_derived_in_this_function():
    ''' The login view has a local of the same name; referencing it from here
        was a NameError waiting for the first record without an iD.
    '''
    block = SOURCE[SOURCE.index('def edit_person'):]
    block = block[:block.index('\n@app.route')]
    assert "html_pattern = ORCID_RE.pattern.strip('^$')" in block


def test_the_page_says_the_people_sync_owns_alumni():
    ''' Clearing it here lasts only if People holds them. '''
    block = SOURCE[SOURCE.index('def edit_person'):]
    block = block[:block.index('\n@app.route')]
    assert 'People sync sets this' in block


# --- _save_person_field, exercised rather than grepped ---------------------
#
# The assertions above this point match strings in the source, which is cheap
# and brittle: four of them have been written wrong because a phrase spanned a
# literal boundary, and none of them would catch a branch that falls through to
# the wrong handler. These run the function.

def _person_field_harness():
    ''' _save_person_field with Flask and Mongo stubbed, plus the operations it
        issued. find_one answers for one "taken" iD so the clash path is real.
    '''
    import datetime as _dt
    writes = []
    taken = '0000-0002-1825-0097'

    class _Coll:
        def update_one(self, _flt, operation):
            writes.append(operation)

        def find_one(self, query):
            return ({'orcid': taken, 'given': ['Taken'], 'family': ['Person']}
                    if query.get('orcid') == taken else None)

    req = type('R', (), {'form': {}})()
    ns = {'request': req, 'datetime': _dt.datetime,
          'render_warning': lambda msg, kind='warning': (kind, msg),
          'DB': {'dis': type('D', (), {'orcid': _Coll()})()},
          'print': lambda *a, **k: None,
          'ORCID_RE': re.compile(r'^\d{4}-\d{4}-\d{4}-\d{3}[\dX]$'),
          'display_person_name': lambda person: 'Taken Person'}
    tree = ast.parse(SOURCE)
    node = next(n for n in tree.body
                if isinstance(n, ast.FunctionDef) and n.name == '_save_person_field')
    exec(ast.get_source_segment(SOURCE, node), ns)    # pylint: disable=W0122

    def call(orc, form, me=None):
        writes.clear()
        req.form = form
        result = ns['_save_person_field'](orc, me or {'_id': 1, 'orcid': 'ADMIN'})
        return result, [key for op in writes for key in op]
    return call, taken


save_field, TAKEN_ID = _person_field_harness()


def test_marking_someone_else_alumni_writes_the_flag_and_a_date():
    (kind, _), ops = save_field({'_id': 2}, {'action': 'alumni', 'alumni': 'on'})
    assert kind == 'success'
    assert ops == ['$set']


def test_unmarking_alumni_unsets_both_fields():
    (kind, _), ops = save_field({'_id': 2, 'alumni': True}, {'action': 'alumni'})
    assert kind == 'success'
    assert '$unset' in ops


def test_marking_yourself_alumni_writes_nothing():
    ''' It would revoke the access needed to undo it. '''
    (kind, msg), ops = save_field({'_id': 1}, {'action': 'alumni', 'alumni': 'on'})
    assert kind == 'warning'
    assert ops == []
    assert 'yourself' in msg


def test_unmarking_yourself_is_allowed():
    ''' Only the revoking direction is dangerous. '''
    (kind, _), ops = save_field({'_id': 1, 'alumni': True}, {'action': 'alumni'})
    assert kind == 'success'
    assert '$unset' in ops


def test_adding_an_orcid_to_a_record_without_one():
    (kind, _), ops = save_field({'_id': 2},
                                {'action': 'orcid', 'orcid': '0000-0001-2345-6789'})
    assert kind == 'success'
    assert ops == ['$set']


@pytest.mark.parametrize('orc,form,why', [
    ({'_id': 2, 'orcid': '0000-0003-3118-1636'},
     {'action': 'orcid', 'orcid': '0000-0001-2345-6789'}, 'already has one'),
    ({'_id': 2}, {'action': 'orcid', 'orcid': 'not-an-id'}, 'malformed'),
    ({'_id': 2}, {'action': 'orcid', 'orcid': TAKEN_ID}, 'another record has it'),
    ({'_id': 2}, {'action': 'orcid', 'orcid': ''}, 'empty'),
])
def test_an_orcid_is_refused_without_writing(orc, form, why):
    (kind, _), ops = save_field(orc, form)
    assert kind == 'warning', why
    assert ops == [], why


def test_an_unrecognised_action_is_refused_not_treated_as_an_orcid():
    ''' The ORCID branch used to be reached by exhausting the alumni one, so an
        unknown action answered "that is not a well-formed ORCID iD" - and a
        third action added to the dispatcher would have been handled as an
        ORCID add.
    '''
    (kind, msg), ops = save_field({'_id': 2}, {'action': 'something_else'})
    assert kind == 'warning'
    assert ops == []
    assert msg == 'Unknown action.'


# ---------------------------------------------------------------------------
# The "Edit this record" control on the public person pages
#
# /userui, /orcidui and /unvaluserui carry no @restricted, so this badge is the
# one admin-only thing on an otherwise public page. These run the real function
# rather than matching its source: the record passed in has neither 'orcid' nor
# 'userIdO365', so the two outbound-call branches above are skipped and only the
# gate under test executes.
# ---------------------------------------------------------------------------

def _controls(user, held):
    ''' add_orcid_controls with current_user/authorizations stubbed '''
    ns = {}
    # tiny_badge lives in dis_html, add_orcid_controls in dis_responder.
    for src, name in ((HTML_SOURCE, 'tiny_badge'), (SOURCE, 'add_orcid_controls')):
        node = next(n for n in ast.parse(src).body
                    if isinstance(n, ast.FunctionDef) and n.name == name)
        exec(ast.get_source_segment(src, node), ns)       # pylint: disable=W0122
    ns['current_user'] = lambda: user
    ns['authorizations'] = lambda person: held
    return ns['add_orcid_controls']


REC = {'_id': 'abc123', 'given': ['Ann'], 'family': ['Lee']}


def test_admin_sees_the_edit_control():
    html = _controls({'orcid': 'x'}, {'admin'})(REC, '')
    assert 'Edit this record' in html
    assert '/person/abc123' in html


def test_signed_out_reader_does_not_see_the_edit_control():
    ''' These pages are public. An ungated link would dead-end every ordinary
        reader on the denied page and advertise a surface they cannot use.
    '''
    html = _controls(None, set())(REC, '')
    assert 'Edit this record' not in html
    assert '/person/' not in html


def test_a_non_admin_authorization_does_not_unlock_the_edit_control():
    ''' Holding some other term - datacite, say - is not administration. '''
    html = _controls({'orcid': 'x'}, {'datacite'})(REC, '')
    assert 'Edit this record' not in html


def test_edit_control_is_not_styled_as_a_write():
    ''' Amber means this click writes; this one only navigates to a page where
        writing is possible. See WRITE_COLOR/REMOVE_COLOR.
    '''
    html = _controls({'orcid': 'x'}, {'admin'})(REC, '')
    assert 'badge-admin' in html
    assert '#f0ad4e' not in html and '#e74c3c' not in html


def test_badge_admin_class_actually_exists():
    ''' A badge class with no CSS renders as unstyled text, silently. '''
    css = (ROOT / 'api' / 'static' / 'css' / 'main.css').read_text(encoding='utf-8')
    assert '.badge-admin {' in css
