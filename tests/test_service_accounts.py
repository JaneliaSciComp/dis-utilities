''' Service accounts: non-people that hold an API token.

    They live in their own collection on purpose. `orcid` is the author roster -
    a dozen sync programs read every document in it as a real Janelia author -
    so a synthetic record there would be name-matched to publications, counted
    in author totals and emailed.

    The asymmetry this file mostly pins down: a person's token needs no lifecycle
    because may_sign_in is re-read every request, so leaving Janelia revokes it.
    A service account has no such lifecycle and would otherwise be the first
    credential here that never lapses.
'''

import ast
from datetime import datetime, timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / 'api' / 'dis_responder.py').read_text(encoding='utf-8')


def _extract(*names, **extra):
    tree = ast.parse(SOURCE)
    ns = {'datetime': datetime, 'SERVICE_KIND': 'service',
          'SERVICE_FORBIDDEN': {'admin'}, 'print': lambda *a, **k: None}
    ns.update(extra)
    for name in names:
        node = next(n for n in tree.body
                    if isinstance(n, ast.FunctionDef) and n.name == name)
        exec(ast.get_source_segment(SOURCE, node), ns)    # pylint: disable=W0122
    return ns


class _Orcid:
    ''' Stands in for DB['dis'].orcid: one owner, in good standing. '''
    def __init__(self, person):
        self.person = person
    def find_one(self, query):
        return self.person if query.get('orcid') == 'OWNER' else None


def _ns(owner=None):
    owner = owner if owner is not None else {'orcid': 'OWNER', 'given': ['O'],
                                             'family': ['Wner']}
    return _extract('may_be_authorized', 'is_service_account', 'may_act',
                    'authorizations',
                    DB={'dis': type('D', (), {'orcid': _Orcid(owner)})()},
                    CVTERM={'authorization': {'admin': {}, 'datacite': {}}})


ACCT = {'name': 'jenkins-x', 'kind': 'service', 'owner': 'OWNER',
        'enabled': True, 'expires': None, 'authorization': ['datacite']}


def test_a_service_account_is_recognised():
    ns = _ns()
    assert ns['is_service_account'](ACCT) is True
    assert ns['is_service_account']({'orcid': '0000-…'}) is False
    assert ns['is_service_account'](None) is False


@pytest.mark.parametrize('patch, allowed, why', [
    ({}, True, 'healthy'),
    ({'enabled': False}, False, 'disabled'),
    ({'expires': datetime.now() - timedelta(days=1)}, False, 'expired'),
    ({'expires': datetime.now() + timedelta(days=30)}, True, 'not yet expired'),
    ({'owner': None}, False, 'no owner'),
    ({'owner': 'SOMEBODY-ELSE'}, False, 'owner not on the roster'),
])
def test_may_act(patch, allowed, why):
    ns = _ns()
    assert ns['may_act']({**ACCT, **patch})[0] is allowed, why


def test_an_account_lapses_when_its_owner_does():
    ''' The whole reason owner is mandatory: accounts outlive the people who
        set them up, and nothing else would ever stop this one.
    '''
    ns = _ns(owner={'orcid': 'OWNER', 'alumni': True})
    allowed, why = ns['may_act'](ACCT)
    assert allowed is False
    assert 'alumni' in why


def test_a_service_account_holds_its_authorizations():
    ns = _ns()
    assert ns['authorizations'](ACCT) == {'datacite'}


def test_a_lapsed_account_holds_nothing():
    ''' A stored grant is not a standing permission - the same rule people get. '''
    ns = _ns()
    assert ns['authorizations']({**ACCT, 'enabled': False}) == set()


def test_admin_is_never_granted_to_a_service_account():
    """ "admin" is a person who grants access and edits the vocabulary - not
        something a Jenkins job does. It also means EVERYTHING: for a person,
        authorizations() expands it to the whole cv, so an account holding it
        would silently acquire every term added from then on. Dropped here even
        if the document was written by hand.
    """
    ns = _ns()
    assert ns['authorizations']({**ACCT, 'authorization': ['admin']}) == set()
    assert ns['authorizations']({**ACCT, 'authorization': ['admin', 'datacite']}) \
        == {'datacite'}


def test_service_accounts_are_refused_on_html_pages():
    ''' Every @restricted route renders a template built for a person, calling
        display_person_name and person['given'][0] on a document with neither.
    '''
    node = next(n for n in ast.parse(SOURCE).body
                if isinstance(n, ast.FunctionDef) and n.name == 'restricted')
    block = ast.get_source_segment(SOURCE, node)
    assert 'is_service_account(person)' in block
    guard = block.index('is_service_account(person)')
    assert block.index('if not person:') > guard, \
        "the service-account guard must run before the signed-out path"


def test_the_collection_is_not_orcid():
    assert "SERVICE_COLLECTION = 'service_account'" in SOURCE
    assert "DB['dis'].orcid.insert_one" not in SOURCE


# ---------------------------------------------------------------------------
# Editing an existing account
# ---------------------------------------------------------------------------

def _edit_source():
    node = next(n for n in ast.parse(SOURCE).body
                if isinstance(n, ast.FunctionDef) and n.name == '_save_service_edit')
    return ast.get_source_segment(SOURCE, node)


def test_authorizations_are_set_whole_not_merged():
    ''' The checkboxes are a complete statement of what the account holds, so a
        $addToSet would make unticking a box do nothing.
    '''
    block = _edit_source()
    assert '"authorization": chosen' in block
    assert '$addToSet' not in block
    assert '$pull' not in block


def test_only_terms_in_the_cv_can_be_granted():
    ''' A forged checkbox value must not be stored. The cv is the vocabulary. '''
    block = _edit_source()
    assert 'valid = set(service_terms())' in block
    assert 'set(request.form.getlist(\'term\')) & valid' in block


def test_changing_the_owner_revalidates_them():
    ''' An owner who cannot hold an authorization would make the account
        permanently lapsed, reported by may_act as a lapse rather than as the
        mistake it was.
    '''
    block = _edit_source()
    owner = block.index("action == 'details'")
    assert 'may_be_authorized' in block[owner:]
    assert 'ORCID_RE.match(owner)' in block[owner:]


def test_rotating_clears_last_used():
    ''' Otherwise the new token inherits the old one's usage history and
        "last used" stops meaning anything.
    '''
    block = _edit_source()
    rotate = block.index("action == 'rotate'")
    tail = block[rotate:]
    assert 'f"{API_TOKEN_FIELD}.digest": token_digest(token)' in tail
    assert 'f"{API_TOKEN_FIELD}.last_used": None' in tail


def test_the_editor_is_admin_only():
    block = SOURCE[SOURCE.index("@app.route('/service_account/<string:sid>'"):]
    assert "@restricted('admin')" in block[:200]


def test_delete_redirects_rather_than_rendering_a_dead_record():
    block = SOURCE[SOURCE.index('def edit_service_account'):]
    block = block[:block.index('\ndef ', 10)]
    assert "redirect(url_for('service_accounts'))" in block
    assert "session['dis_sa_deleted']" in block


def _lifecycle_forms():
    """ The source of each <form> in the lifecycle section, one per action """
    body = next(n for n in ast.parse(SOURCE).body
                if isinstance(n, ast.FunctionDef) and n.name == '_service_account_body')
    block = ast.get_source_segment(SOURCE, body)
    tail = block[block.index('Token and lifecycle'):]
    return [f for f in tail.split("<form method='POST'")[1:]]


@pytest.mark.parametrize('action', ['delete', 'rotate'])
def test_the_disruptive_actions_ask_for_confirmation(action):
    """ Delete cannot be undone; rotate breaks anything using the old token. """
    forms = [f for f in _lifecycle_forms() if f"value='{action}'" in f]
    assert len(forms) == 1, f"expected exactly one {action} form"
    assert 'return confirm(' in forms[0]


def test_enabling_is_not_guarded_by_a_prompt():
    """ A confirm that fires when nothing dangerous is happening is one people
        learn to click through, which is how it stops working on the two that
        matter.
    """
    for form in _lifecycle_forms():
        if "value='delete'" in form or "value='rotate'" in form:
            continue
        assert 'return confirm(' not in form


def test_each_confirmation_guards_exactly_one_action():
    for form in _lifecycle_forms():
        if 'return confirm(' not in form:
            continue
        actions = [a for a in ('delete', 'rotate', 'enable', 'disable')
                   if f"value='{a}'" in form]
        assert len(actions) == 1, f"a prompt covers {actions}"


def test_the_confirmation_names_the_account():
    ''' This page is reachable in several tabs at once; "Delete this?" does not
        say which one is about to go.
    '''
    body = next(n for n in ast.parse(SOURCE).body
                if isinstance(n, ast.FunctionDef) and n.name == '_service_account_body')
    block = ast.get_source_segment(SOURCE, body)
    assert "Delete the service account {acct.get('name', '')}?" in block


def test_a_confirmation_survives_an_apostrophe_in_the_name():
    ''' html.escape turns ' into &#x27;, the browser decodes it before the JS
        parses, and confirm('...') terminates early: the handler throws and the
        form submits WITHOUT asking. The guard disappears exactly when the name
        is awkward, which is the worst way for a guard to fail.
    '''
    import json
    from html import escape
    name = "O'Brien-sync"
    attr = escape(json.dumps(f"Delete the service account {name}?"))
    # what the browser hands to the JS parser
    decoded = attr.replace('&quot;', '"').replace('&#x27;', "'").replace('&amp;', '&')
    assert decoded.startswith('"') and decoded.endswith('"')
    assert json.loads(decoded) == f"Delete the service account {name}?"
    # and the source really does build it this way, everywhere
    helper = next(n for n in ast.parse(SOURCE).body
                  if isinstance(n, ast.FunctionDef) and n.name == 'js_string')
    assert 'escape(json.dumps(message))' in ast.get_source_segment(SOURCE, helper)
    # No call site may still build one by hand. js_string's own docstring quotes
    # the wrong pattern in order to explain it, so exclude the helper itself.
    elsewhere = SOURCE.replace(ast.get_source_segment(SOURCE, helper), '')
    assert elsewhere.count("confirm('") == 0, \
        "an inline confirm built with a single-quoted literal loses its prompt " \
        "to an apostrophe - use js_string()"



def test_a_malformed_expiry_is_a_message_not_a_crash():
    fn = next(n for n in ast.parse(SOURCE).body
              if isinstance(n, ast.FunctionDef) and n.name == '_expiry_or_error')
    ns = {'datetime': __import__('datetime').datetime}
    exec(ast.get_source_segment(SOURCE, fn), ns)    # pylint: disable=W0122
    parse = ns['_expiry_or_error']
    assert parse('2027-01-31')[0].year == 2027
    assert parse('')[0] is None and parse('')[1] == ''
    assert parse(None)[0] is None
    for junk in ('tomorrow', '31/01/2027', '2027-13-45', "'; drop"):
        when, err = parse(junk)
        assert when is None and err, junk


def test_an_unloaded_cv_does_not_wipe_authorizations():
    ''' No checkboxes render, so Save would otherwise write an empty list and
        strip everything the account held.
    '''
    block = _edit_source()
    guard = block.index('if not valid:')
    write = block.index('"authorization": chosen')
    assert guard < write
    assert 'return render_warning' in block[guard:write]
