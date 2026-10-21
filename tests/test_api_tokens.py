''' API tokens: how a Bearer token resolves to a person.

    Replaces app.config['KEYS'] - four JWTs in config.cfg compared by literal
    string membership, never decoded, so the exp claims they carried were never
    enforced. A token now hashes to a digest stored on its owner's orcid
    document, and resolves through the same current_user() a browser session
    produces, so one authorization model serves both.

    Extracted with ast rather than imported: importing dis_responder needs
    Flask, a config file and a database, none of which this logic depends on.
'''

import ast
import hashlib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / 'api' / 'dis_responder.py').read_text(encoding='utf-8')


def _extract(name, ns=None):
    node = next(n for n in ast.parse(SOURCE).body
                if isinstance(n, ast.FunctionDef) and n.name == name)
    ns = ns if ns is not None else {}
    exec(ast.get_source_segment(SOURCE, node), ns)    # pylint: disable=W0122
    return ns[name]


token_digest = _extract('token_digest', {'hashlib': hashlib})


def test_digest_is_sha256_of_the_token():
    assert token_digest('abc') == hashlib.sha256(b'abc').hexdigest()


def test_digest_ignores_surrounding_whitespace():
    ''' A token pasted from a terminal often carries a trailing newline. '''
    assert token_digest('  abc\n') == token_digest('abc')


def test_digest_does_not_contain_the_token():
    ''' Only the digest is stored; a database leak must not yield a usable key. '''
    secret = 's3cret-token-value'
    assert secret not in token_digest(secret)


def test_different_tokens_give_different_digests():
    assert token_digest('abc') != token_digest('abd')


def _bearer():
    class _Req:
        headers = {}
    ns = {'re': __import__('re'), 'request': _Req()}
    fn = _extract('bearer_token', ns)
    return fn, ns['request']


def test_bearer_token_is_read_from_the_header():
    fn, req = _bearer()
    req.headers = {'Authorization': 'Bearer abc123'}
    assert fn() == 'abc123'


def test_bearer_token_tolerates_extra_whitespace():
    fn, req = _bearer()
    req.headers = {'Authorization': '  Bearer    abc123  '}
    assert fn() == 'abc123'


def test_no_header_is_not_a_token():
    fn, req = _bearer()
    req.headers = {}
    assert fn() is None


def test_a_non_bearer_scheme_is_refused():
    ''' The old code did re.sub('Bearer\\s+', '', header), which accepted
        "token abc123" and "Basic abc123" by simply not matching and passing
        the whole header through as the key.
    '''
    fn, req = _bearer()
    for header in ('token abc123', 'Basic abc123', 'abc123'):
        req.headers = {'Authorization': header}
        assert fn() is None, header


def test_bearer_with_no_value_is_not_a_token():
    fn, req = _bearer()
    req.headers = {'Authorization': 'Bearer '}
    assert fn() is None


def test_the_retired_keys_are_gone():
    ''' Four JWTs lived in config.cfg and were never decoded. They are to be
        treated as compromised, so nothing may reference them again.
    '''
    assert "app.config['KEYS']" not in SOURCE
    cfg = ROOT / 'api' / 'config.cfg'
    if cfg.exists():
        assert 'KEYS' not in cfg.read_text(encoding='utf-8')


def test_authorized_is_derived_from_the_resolved_person():
    ''' Not from membership of a literal list. '''
    assert "result['rest']['authorized'] = bool(current_user())" in SOURCE


# ---------------------------------------------------------------------------
# Refreshing /profile must not mint a second token
#
# A POST that renders its result directly is re-submitted by a browser refresh.
# Found in testing: refreshing after generating silently issued another token
# and invalidated the one just copied, with nothing on screen saying so.
# ---------------------------------------------------------------------------

def _profile_source():
    node = next(n for n in ast.parse(SOURCE).body
                if isinstance(n, ast.FunctionDef) and n.name == 'profile')
    return ast.get_source_segment(SOURCE, node)


def test_generating_consumes_a_one_time_nonce():
    block = _profile_source()
    assert "session.pop('dis_token_nonce', None)" in block
    assert 'already been used' in block


def test_the_nonce_is_compared_before_issuing():
    ''' Order matters: issue first and the guard protects nothing. '''
    block = _profile_source()
    guard = block.index("session.pop('dis_token_nonce', None)")
    issue = block.index('_issue_api_token(person)')
    assert guard < issue, "the nonce must be checked before a token is minted"


def test_a_refused_resubmission_cannot_reach_the_issue_call():
    """ The structural property, not its current spelling: every call to
        _issue_api_token must sit on the branch where the nonce matched. If the
        refusal ever stops short-circuiting, a refresh mints a token again.
    """
    tree = ast.parse(_profile_source())
    guards = [n for n in ast.walk(tree)
              if isinstance(n, ast.If) and 'dis_token_nonce' in ast.dump(n.test)]
    assert guards, "no nonce check found in profile()"
    issuing = [n for n in ast.walk(tree) if isinstance(n, ast.Call)
               and getattr(n.func, 'id', '') == '_issue_api_token']
    assert issuing, "profile() never issues a token"
    for call in issuing:
        on_match = any(call in ast.walk(stmt) for g in guards for stmt in g.orelse)
        assert on_match, "a token is minted outside the nonce-matched branch"


def test_revoking_redirects_rather_than_rendering():
    ''' Post/Redirect/Get: nothing needs displaying, so a refresh is a GET. '''
    block = _profile_source()
    assert "return redirect(url_for('profile'))" in block


def test_the_form_carries_a_fresh_nonce():
    section = next(n for n in ast.parse(SOURCE).body
                   if isinstance(n, ast.FunctionDef) and n.name == '_api_token_section')
    block = ast.get_source_segment(SOURCE, section)
    assert "session['dis_token_nonce'] = nonce" in block
    assert "name='nonce'" in block


def test_profile_never_returns_a_bare_warning_fragment():
    ''' render_warning builds a fragment for embedding in a template. Returned
        from a view it becomes the whole response: an unstyled snippet with no
        navbar and no way back. Messages belong in the page.
    '''
    block = _profile_source()
    for line in block.splitlines():
        stripped = line.strip()
        assert not stripped.startswith('return render_warning'), stripped


# ---------------------------------------------------------------------------
# The OpenAPI spec has to say which operations read a token
#
# bearerAuth was declared but referenced by no operation, so Swagger UI's
# Authorize button took a key, stored it, and sent no header at all. The request
# arrived anonymous and came back unauthorized, which looked like a broken token
# rather than a documentation gap.
# ---------------------------------------------------------------------------

def _operation_spec(func_name):
    """ The YAML block of a view's docstring, as text.

        Parsed by hand rather than with PyYAML: the test environment installs
        pytest plus jrc_common and doi_common and nothing else, so importing
        yaml here would either fail in CI or skip silently and test nothing.
    """
    fn = next(n for n in ast.walk(ast.parse(SOURCE))
              if isinstance(n, ast.FunctionDef) and n.name == func_name)
    doc = ast.get_docstring(fn) or ''
    return doc.split('---', 1)[1] if '---' in doc else None


def test_the_endpoint_that_reads_a_token_declares_it():
    block = _operation_spec('get_doi_authors')
    assert block is not None, "/doi/authors is documented and must stay so"
    assert 'security:' in block and '- bearerAuth: []' in block, \
        "Swagger only sends the Authorization header for an operation that " \
        "declares the scheme"


def test_optional_auth_is_expressed_as_an_alternative():
    ''' The empty requirement is what makes it optional. Without it the operation
        reads as requiring a token and Swagger marks it locked - which would be
        wrong: it answers unauthenticated, just with less in it.
    '''
    block = _operation_spec('get_doi_authors')
    sec = block[block.index('security:'):]
    sec = sec[:sec.index('parameters:')]
    assert '- {}' in sec, "optional auth needs an empty alternative"


LINES = SOURCE.splitlines()


def _code_of(node):
    """ A function's body as text, without the docstring.

        Line slicing, not ast.get_source_segment: that re-scans the whole
        25,000-line file per call, and calling it per statement made two tests
        here take 59 and 55 seconds - three quarters of the whole suite.
    """
    first = node.body[0]
    start = first.lineno - 1
    if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant):
        start = first.end_lineno
    return '\n'.join(LINES[start:node.end_lineno])


def _documented_views():
    ''' (name, spec block, code, decorator names) for every documented view '''
    out = []
    for node in ast.walk(ast.parse(SOURCE)):
        if not isinstance(node, ast.FunctionDef):
            continue
        doc = ast.get_docstring(node) or ''
        if '---' not in doc:
            continue
        code = _code_of(node)
        decs = []
        for dec in node.decorator_list:
            func = dec.func if isinstance(dec, ast.Call) else dec
            decs.append(getattr(func, 'id', getattr(func, 'attr', '')))
        out.append((node.name, doc.split('---', 1)[1], code, decs))
    return out


def test_every_documented_operation_declares_what_it_actually_does():
    ''' Derived, not a hand-kept list. Three cases, and the spec must match:

          - requires a token (@api_restricted): declares the scheme and NOT the
            empty alternative, so the padlock reads as closed
          - redacts (reads rest.authorized): declares the scheme AND the empty
            alternative, because it answers without one
          - neither: declares nothing, or a padlock implies that authenticating
            would change something

        A hardcoded list already missed one consumer (get_published_dois), then
        missed the third case entirely when the migration endpoints stopped
        redacting and started requiring.
    '''
    wrong = []
    for name, block, code, decs in _documented_views():
        requires = 'api_restricted' in decs
        # Redaction moved from an inline rest.authorized check to the shared
        # strip_identifiers helper; both count.
        redacts = ("result['rest']['authorized']" in code
                   or 'strip_identifiers' in code)
        declares = 'security:' in block
        optional = '- {}' in block
        if requires:
            good = declares and not optional
        elif redacts:
            good = declares and optional
        else:
            good = not declares
        if not good:
            wrong.append((name, {'requires': requires, 'redacts': redacts,
                                 'declares': declares, 'optional': optional}))
    assert not wrong, f"spec disagrees with code: {wrong}"


def _view(name):
    return next(n for n in ast.walk(ast.parse(SOURCE))
                if isinstance(n, ast.FunctionDef) and n.name == name)


def test_the_migration_endpoints_require_an_authorization():
    ''' They expose DO_NOT_DISPLAY fields and jrc_author, so they are the first
        endpoints to demand a token rather than quietly returning less.
    '''
    for name in ('get_doi_migration', 'get_doi_migrations'):
        node = _view(name)
        calls = [d for d in node.decorator_list if isinstance(d, ast.Call)
                 and getattr(d.func, 'id', '') == 'api_restricted']
        assert calls, f"{name} must be gated"
        assert [a.value for a in calls[0].args] == ['orcid'], name


def test_a_gated_endpoint_no_longer_redacts():
    ''' Once nobody without the authorization can reach the body, an
        "if not authorized" branch there is dead code that reads as live.
    '''
    for name in ('get_doi_migration', 'get_doi_migrations'):
        assert "if not result['rest']['authorized']" not in _code_of(_view(name)), name


def test_the_gate_distinguishes_401_from_403():
    ''' 401 means fix your token; 403 means ask for access. A script cannot act
        on the two the same way.
    '''
    block = ast.get_source_segment(SOURCE, _view('api_restricted'))
    assert '401)' in block and '403)' in block
    assert block.index('401)') < block.index('403)'), \
        "identity is checked before authorization"


def test_admin_always_admits():
    ''' Consistent with @restricted and authorizations(): admin means everything,
        so an administrator never needs the specific term listed.
    '''
    block = ast.get_source_segment(SOURCE, _view('api_restricted'))
    assert "{'admin'} | set(terms)" in block


# ---------------------------------------------------------------------------
# Employee identifiers
#
# They travel under two names: employeeId on an author object, and jrc_author,
# which is a bare list of employee IDs. A fix applied to one spelling, or to the
# wrong function, leaves the other route open - which is exactly what happened:
# a redaction was removed from get_published_dois while the comment left behind
# claimed @api_restricted protected it, and that view has no decorator.
# ---------------------------------------------------------------------------

IDENTIFIER_ENDPOINTS = ('get_doi_api', 'get_published_dois', 'get_doi_authors')


def test_every_identifier_endpoint_strips_by_name_not_by_luck():
    ''' Asserted per function. The bug this replaces passed a count==1 check on
        a code block that lived in a different view than intended.
    '''
    for name in IDENTIFIER_ENDPOINTS:
        assert 'strip_identifiers' in _code_of(_view(name)), \
            f"{name} must route its output through strip_identifiers"


def test_the_gate_is_an_authorization_not_merely_a_token():
    ''' Any valid token used to be enough. Roster identity needs the orcid
        authorization, or admin.
    '''
    block = ast.get_source_segment(SOURCE, _view('may_see_identifiers'))
    assert 'IDENTIFIER_TERMS & authorizations(current_user())' in block
    assert "IDENTIFIER_TERMS = {'admin', 'orcid'}" in SOURCE


def test_both_spellings_of_the_identifier_are_covered():
    ''' jrc_author is a list of employee IDs. Gating employeeId alone leaves
        11,736 DOIs handing them out under the other name.
    '''
    assert "IDENTIFIER_FIELDS = ('employeeId', 'jrc_author')" in SOURCE
    block = ast.get_source_segment(SOURCE, _view('strip_identifiers'))
    assert 'for field in IDENTIFIER_FIELDS' in block


def test_no_endpoint_redacts_identifiers_by_hand_any_more():
    ''' A second, divergent copy of the rule is how the two spellings drifted
        apart in the first place.
    '''
    for node in ast.walk(ast.parse(SOURCE)):
        if not isinstance(node, ast.FunctionDef) or node.name == 'strip_identifiers':
            continue
        code = _code_of(node)
        assert "del auth['employeeId']" not in code, node.name
        assert "del rec['jrc_author']" not in code, node.name
