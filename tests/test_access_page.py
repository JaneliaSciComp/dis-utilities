''' /access: who can reach the application, and when they last did.

    The thing this page must not do is claim to know who is "logged in".
    Sessions are Flask signed cookies with no server-side store, so nothing
    records a sign-out and nothing can be enumerated - a closed tab is
    indistinguishable from an open one. What exists is last_seen, written on
    each authenticated request and throttled to one write a minute. That is
    activity, not presence.
'''

import ast
import re
from datetime import datetime, timedelta
from html import escape
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / 'api' / 'dis_responder.py').read_text(encoding='utf-8')
HTML_SOURCE = (ROOT / 'api' / 'dis_html.py').read_text(encoding='utf-8')


def _fn(name, **extra):
    ns = {'datetime': datetime, 'timedelta': timedelta, 'escape': escape}
    ns.update(extra)
    node = next(n for n in ast.parse(SOURCE).body
                if isinstance(n, ast.FunctionDef) and n.name == name)
    exec(ast.get_source_segment(SOURCE, node), ns)    # pylint: disable=W0122
    return ns[name]


def _flat(text):
    """ Join adjacent string-concatenation pieces.

        Prose in dis_responder is built by concatenation, so a phrase routinely
        spans a literal boundary and a plain substring check misses it. Same
        regex as test_authorization._flat; not shared because helpers.py is
        scoped to the update_dois fakes, and one regex is a thin reason to add
        a module.
    """
    return re.sub(r'"\s*\\?\s*\n\s*\+?\s*(f?)"', '', text)


def _plain(html):
    return ' '.join(re.sub(r'<[^>]+>', ' ', html).split())


AGO = _fn('_ago')


def test_a_missing_timestamp_reads_as_never():
    assert _plain(AGO(None)) == 'never'


def test_recent_activity_is_relative_and_absolute():
    ''' Relative alone is unreadable for anything old; absolute alone makes the
        reader do arithmetic to answer "is anyone on right now".
    '''
    out = _plain(AGO(datetime.now() - timedelta(minutes=30)))
    assert '30 min ago' in out
    assert re.search(r'\d{4}-\d{2}-\d{2}', out), "the exact timestamp is kept"


def test_units_are_singular_when_they_should_be():
    for mins, want in ((60, '1 hr ago'), (120, '2 hrs ago'),
                       (1440, '1 day ago'), (2880, '2 days ago')):
        assert want in _plain(AGO(datetime.now() - timedelta(minutes=mins))), want


def test_the_page_is_admin_only():
    block = SOURCE[SOURCE.index("@app.route('/access')"):]
    assert "@restricted('admin')" in block[:120]


def test_the_navbar_entry_matches_the_route():
    ''' A NAV value that does not match a route is shown to everyone and bounces
        them, because the gating filter never finds it.
    '''
    nav = re.search(r'"Access": "([^"]+)"', HTML_SOURCE).group(1)
    assert f"@app.route('/{nav}')" in SOURCE


def test_the_page_does_not_claim_to_know_who_is_logged_in():
    ''' The honesty requirement. There is no session store to enumerate, so any
        wording implying live presence would be inventing it.
    '''
    node = next(n for n in ast.parse(SOURCE).body
                if isinstance(n, ast.FunctionDef) and n.name == 'show_access')
    # The docstring is excluded on purpose: it contains the banned phrase in
    # order to warn against it, which is the opposite of the problem.
    code = _flat('\n'.join(ast.get_source_segment(SOURCE, st) or ''
                           for st in node.body[1:]))
    lowered = code.lower()
    for banned in ('logged in', 'online now', 'currently signed in'):
        assert banned not in lowered, f"page implies presence: {banned!r}"
    assert 'last seen' in lowered
    assert 'closing the tab tells the server nothing' in lowered


def test_unused_tokens_are_flagged_rather_than_merely_listed():
    ''' A credential issued and never used is the actionable row - it is either
        forgotten or was never needed.
    '''
    block = SOURCE[SOURCE.index('def _token_rows():'):]
    block = block[:block.index('\ndef ', 10)]
    assert 'STALE_TOKEN_DAYS' in SOURCE
    assert "counts['unused']" in block
    assert 'not used and created and created < stale' in block


def test_service_accounts_and_people_appear_in_one_table():
    ''' Both hold tokens. Two tables would let one be reviewed and the other
        forgotten.
    '''
    block = SOURCE[SOURCE.index('def _token_rows():'):]
    block = block[:block.index('\ndef ', 10)]
    assert 'SERVICE_COLLECTION' in block and 'orcid.find' in block
    assert '"person"' in block and '"service"' in block


def test_the_table_shows_what_was_granted_not_the_expansion():
    ''' authorizations() expands "admin" to the whole cv. That is right for
        deciding access and wrong for a table: all three current admins hold
        exactly ['admin'], and the expanded form rendered as three separate
        grants. /authorizations shows the stored field, and two pages describing
        the same people differently is worse than either choice alone.
    '''
    def body(name):
        # Docstrings are excluded: _granted's explains why it does NOT call
        # authorizations(), and a substring check over prose would trip on the
        # very sentence describing the rule.
        node = next(n for n in ast.parse(SOURCE).body
                    if isinstance(n, ast.FunctionDef) and n.name == name)
        return '\n'.join(ast.get_source_segment(SOURCE, st) or ''
                         for st in node.body[1:])

    assert "doc or {}).get('authorization')" in body('_granted')
    assert 'authorizations(' not in body('_granted'), \
        "_granted must read the stored field, not the expanded set"
    for builder in ('_signin_rows', '_token_rows'):
        assert 'sorted(authorizations(' not in body(builder), \
            f"{builder} must render _granted(), not the expansion"


def test_admin_is_marked_as_implying_everything():
    ''' Showing the bare term would understate it in the other direction. '''
    granted = _fn('_granted', escape=escape)
    assert 'implies all' in granted({'authorization': ['admin']})
    assert 'implies all' not in granted({'authorization': ['orcid']})
    assert _plain(granted({'authorization': []})) == '-'
    assert _plain(granted(None)) == '-'


def _builder_body(name):
    node = next(n for n in ast.parse(SOURCE).body
                if isinstance(n, ast.FunctionDef) and n.name == name)
    return '\n'.join(ast.get_source_segment(SOURCE, st) or ''
                     for st in node.body[1:])


def test_no_table_declares_a_nameless_column():
    ''' The token table carried a sixth column with an empty header, holding a
        flag that only fires for a token unused for a week. With neither current
        token old enough it rendered as dead space with nothing to explain it,
        which reads as a rendering fault rather than a design.
    '''
    for builder in ('_signin_rows', '_token_rows'):
        body = _builder_body(builder)
        headers = re.search(r"return \(\[(.*?)\]", body, re.S)
        assert headers, f"{builder} must return a header list"
        names = re.findall(r"'([^']*)'", headers.group(1))
        assert all(n.strip() for n in names), \
            f"{builder} declares a column with no heading: {names}"


def test_the_unused_flag_rides_in_the_state_column():
    ''' Rare flags belong beside a value that is always present. '''
    body = _builder_body('_token_rows')
    assert 'never used in' in body
    assert 'state +=' in body, "the flag must extend State, not add a column"
