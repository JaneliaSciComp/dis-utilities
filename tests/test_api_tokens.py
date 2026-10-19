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
