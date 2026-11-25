''' The public gate: on a public-facing host, nothing answers an anonymous
    request except the sign-in pages, the probe, robots.txt and static files.

    One image serves two hosts. The internal one sets DIS_REQUIRE_SIGNIN=0 and
    is unchanged; the DMZ host leaves it on. The gate is the floor under the
    authorization model, not a replacement for it: @restricted('term'),
    @api_restricted and @redacts still decide what a signed-in person may do.

    Extracted with ast rather than imported (dis_responder needs Flask, a
    config file and a database).
'''

import ast
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / 'api' / 'dis_responder.py').read_text(encoding='utf-8')
_TREE = ast.parse(SOURCE)
_FUNCS = {n.name: n for n in _TREE.body if isinstance(n, ast.FunctionDef)}


def _block(start, end):
    seg = SOURCE[SOURCE.index(start):]
    return seg[:seg.index(end)]


GATE = _block('def public_gate(', 'def _wants_html(')
REFUSE = _block('def _refuse_anonymous(', '# ****')


def _ns(*names):
    ns = {}
    for name in names:
        exec(ast.get_source_segment(SOURCE, _FUNCS[name]), ns)    # pylint: disable=W0122
    return ns


# --- the switch ----------------------------------------------------------------

@pytest.mark.parametrize('raw,required', [
    (None, True), ('', True), ('1', True), ('true', True), ('yes', True), ('anything', True),
    ('0', False), ('false', False), ('No', False), (' off ', False),
])
def test_the_switch_fails_closed(raw, required):
    ''' Unset means required. The internal host must say 0 to stay open; a
        public host that forgets the variable asks everyone to sign in rather
        than publishing the roster. '''
    assert _ns('_signin_required')['_signin_required'](raw) is required


def test_the_switch_is_read_once_from_the_environment():
    assert "REQUIRE_SIGNIN = _signin_required(os.environ.get('DIS_REQUIRE_SIGNIN'))" in SOURCE


# --- what stays public ------------------------------------------------------------

def test_the_public_set_is_exactly_the_sign_in_pages_probe_robots_and_static():
    match = re.search(r"PUBLIC_ENDPOINTS = frozenset\(\{(.*?)\}\)", SOURCE)
    names = {n.strip(" '") for n in match.group(1).split(',')}
    assert names == {'login', 'logout', 'healthz', 'robots', 'static'}
    # Every declared endpoint is a real view (static is Flask's own).
    for name in names - {'static'}:
        assert name in _FUNCS, f"PUBLIC_ENDPOINTS names {name}, which has no view"


def test_healthz_answers_without_the_database_or_a_template():
    body = ast.get_source_segment(SOURCE, _FUNCS['healthz'])
    assert "return 'ok', 200" in body
    assert 'DB[' not in body and 'render_template' not in body
    assert "@app.route('/healthz')" in SOURCE


def test_robots_follows_the_switch():
    body = ast.get_source_segment(SOURCE, _FUNCS['robots'])
    assert 'Disallow: /' in body and 'if REQUIRE_SIGNIN' in body
    assert "@app.route('/robots.txt')" in SOURCE


# --- the hook ---------------------------------------------------------------------

def test_the_gate_is_a_before_request_hook_registered_after_initialisation():
    ''' Flask runs before_request hooks in registration order; the gate needs
        the database connected (current_user reads the roster), so it must
        follow before_request. '''
    hooks = re.findall(r"@app\.before_request\ndef (\w+)\(", SOURCE)
    assert hooks == ['before_request', 'public_gate']


def test_the_gate_does_nothing_when_the_switch_is_off_or_for_options_and_public_routes():
    assert "if not REQUIRE_SIGNIN or request.method == 'OPTIONS':\n        return None" in GATE
    assert "if request.endpoint in PUBLIC_ENDPOINTS:\n        return None" in GATE


def test_a_signed_in_person_or_token_passes_and_anyone_else_is_refused():
    ''' current_user() resolves a session cookie or a Bearer token, so the
        page's own JavaScript and a script with a token both pass. '''
    assert "if current_user():\n        return None\n    return _refuse_anonymous()" in GATE


def test_an_unmatched_path_is_refused_not_404():
    ''' A 404 for anonymous callers would say which paths exist. '''
    assert 'request.endpoint is None' in GATE
    assert 'abort(404)' not in GATE


def test_a_missing_secret_is_a_503_not_an_open_door():
    assert "problem = _restriction_unavailable()" in GATE
    assert "return _refuse_anonymous(503, problem)" in GATE


# --- how a refusal is phrased ---------------------------------------------------

def test_browser_detection_comes_from_the_request_not_the_route():
    ''' Several routes serve both HTML and JSON (/peoplerec decides by the
        Accept header), so the gate decides the same way. */* must not count
        as HTML, or curl would be redirected to a login form. '''
    wants = _ns('_wants_html')['_wants_html']
    assert wants(['text/html', 'application/xhtml+xml', '*/*']) is True
    assert wants(['application/xhtml+xml']) is True
    assert wants(['*/*']) is False
    assert wants(['application/json']) is False
    assert wants([]) is False
    assert wants(['*/*'], fetch_mode='navigate') is True
    assert 'accept_html' not in ast.get_source_segment(SOURCE, _FUNCS['_wants_html']).split("'''")[2]


def test_a_browser_is_sent_to_sign_in_and_brought_back():
    assert "return redirect(url_for('login', next=target))" in REFUSE
    assert "target = request.full_path.rstrip('?')" in REFUSE
    # A POST cannot be replayed by the GET after sign-in: back to the form's page.
    assert "ref = urlsplit(request.referrer or '')" in REFUSE
    assert "if ref.path.startswith('/') else '/'" in REFUSE


def test_a_program_gets_a_json_401_with_www_authenticate():
    assert 'response.status_code = status' in REFUSE
    assert "response.headers['WWW-Authenticate'] = 'Bearer realm=\"DIS\"'" in REFUSE
    assert "Authorization: Bearer <token>" in REFUSE


def test_the_restrictions_page_shows_the_mode():
    page = ast.get_source_segment(SOURCE, _FUNCS['show_restrictions'])
    assert '"Sign-in required everywhere"' in page
    assert 'PUBLIC_ENDPOINTS' in page
