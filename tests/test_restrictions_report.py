''' /restrictions: which endpoints demand what, read from the live route table.

    Three ways an endpoint can care about who is asking, and they are not
    interchangeable:

      - a page REFUSES an unauthenticated reader (@restricted), and an
        authenticated one lacking the term - redirect, then 403
      - an API endpoint refuses as JSON (@api_restricted) - 401, then 403
      - a redacting endpoint answers everybody and returns LESS (@redacts)

    The third is the one that goes wrong quietly: nothing fails, the response is
    simply thinner, so an under-privileged caller can go months without a
    complaint. It is listed beside the other two rather than left to be found in
    a function body.
'''

import ast
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / 'api' / 'dis_responder.py').read_text(encoding='utf-8')
HTML_SOURCE = (ROOT / 'api' / 'dis_html.py').read_text(encoding='utf-8')


def _body(name):
    node = next(n for n in ast.parse(SOURCE).body
                if isinstance(n, ast.FunctionDef) and n.name == name)
    return '\n'.join(ast.get_source_segment(SOURCE, st) or ''
                     for st in node.body[1:])


def test_the_inventory_is_read_from_the_route_table():
    ''' Not from a hand-kept list. A route added tomorrow must appear without
        anyone remembering, and a decorator removed by accident must vanish.
    '''
    body = _body('_restriction_inventory')
    assert 'app.url_map.iter_rules()' in body
    assert 'app.view_functions.get(rule.endpoint)' in body


def test_all_three_kinds_of_restriction_are_discoverable():
    body = _body('_restriction_inventory')
    for attr in ('dis_restricted', 'dis_api_restricted', 'dis_redacts_terms'):
        assert attr in body, f"{attr} is not read by the inventory"


def test_the_marker_decorator_changes_no_behaviour():
    ''' @redacts records metadata only - the withholding is strip_identifiers
        inside the view. A marker that also enforced would be two sources of
        truth for one rule.
    '''
    body = _body('redacts')
    assert 'dis_redacts_terms' in body and 'dis_redacts_fields' in body
    assert 'return view' in body
    for banned in ('current_user', 'authorizations(', 'InvalidUsage', 'abort'):
        assert banned not in body, f"@redacts must not enforce ({banned})"


def test_every_redacting_endpoint_carries_the_marker():
    ''' Otherwise the report understates what is authorization-sensitive, which
        is worse than not having the report.

        Uses line ranges rather than ast.get_source_segment per statement: the
        segment call re-scans the whole 25,000-line file each time, and doing it
        per statement made this test take minutes.
    '''
    lines = SOURCE.splitlines()
    tree = ast.parse(SOURCE)
    marked, calls = set(), set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef):
            continue
        for dec in node.decorator_list:
            func = dec.func if isinstance(dec, ast.Call) else dec
            if getattr(func, 'id', '') == 'redacts':
                marked.add(node.name)
        if node.name == 'strip_identifiers':
            continue
        body = '\n'.join(lines[node.body[0].lineno - 1:node.end_lineno])
        if 'strip_identifiers(' in body:
            calls.add(node.name)
    assert calls, "no endpoint calls strip_identifiers - has it been renamed?"
    assert calls <= marked, f"redacting but unmarked: {sorted(calls - marked)}"


def test_the_report_is_admin_only():
    block = SOURCE[SOURCE.index("@app.route('/restrictions')"):]
    assert "@restricted('admin')" in block[:140]


def test_the_navbar_entry_matches_the_route():
    nav = re.search(r'"Endpoint restrictions": "([^"]+)"', HTML_SOURCE).group(1)
    assert f"@app.route('/{nav}')" in SOURCE


def test_the_report_counts_itself():
    ''' It is a restricted page, so it must appear in its own listing. A report
        that excludes itself is one that has stopped reading the route table.
    '''
    block = SOURCE[SOURCE.index("@app.route('/restrictions')"):]
    assert "@restricted('admin')" in block[:140]
    assert 'dis_restricted' in _body('_restriction_inventory')


def _report_body():
    return _body('show_restrictions')


def test_the_pages_table_can_be_filtered_to_named_authorizations():
    ''' 24 restricted pages, but only some demand a named term - the rest want
        any signed-in reader. Mixing them in one list makes "what does admin
        actually gate?" an eyeball exercise.
    '''
    body = _report_body()
    assert "toggleClass('uipages', 'signinonly'" in body
    assert "'termed' if term else 'signinonly'" in body


def test_the_toggle_updates_the_counter_it_names():
    ''' toggleClass writes the visible-row count into the element id it is
        given. A counter id that does not match the span is the silent failure
        here: the filter works, the number freezes, and the page quietly lies.
    '''
    body = _report_body()
    named = re.search(r"toggleClass\([^)]*'([a-z]+)'\);", body)
    assert named, "no toggleClass call found"
    counter = named.group(1)
    assert f"id='{counter}'" in body, \
        f"toggleClass updates #{counter}, which the page never renders"


def test_the_toggle_labels_describe_the_resulting_state():
    ''' whenHidden is shown AFTER the rows are hidden. Swapping the two is the
        classic error and leaves the button describing what you just left.
    '''
    body = _report_body()
    call = body[body.index('toggleClass('):]
    call = call[:call.index(');')]
    args = re.findall(r"'([^']*)'", call)
    # tid, cls, whenHidden, whenShown, counter  (this is passed separately)
    assert args[2] == 'Show all pages'
    assert args[3] == 'Authorization required only'
    # And the button starts in the unfiltered state, inviting the filter. The
    # leading ">" is deliberately not matched: it closes the <button> tag in a
    # different string literal, and a phrase spanning a concatenation boundary
    # is how assertions in this repo get written wrong.
    assert "Authorization required only</button>" in body


def test_the_row_classes_cover_every_row():
    ''' A classes list shorter than the rows list silently leaves the tail
        unclassified, and those rows would never hide.
    '''
    body = _report_body()
    assert 'rows.append(' in body and 'classes.append(' in body
    assert body.index('rows.append(') < body.index('classes.append(')
    assert 'row_classes=classes' in body


def test_the_only_writing_api_endpoint_is_gated():
    ''' set_jrc_author recomputes and writes jrc_author - the credit field the
        read side spends three endpoints protecting. It was reachable by anyone
        who could reach the host: no token, no authorization, POST only.

        Found by auditing which routed views mutate data, which is the report
        this file tests. The audit is only worth having if what it finds gets
        fixed, so the fix is pinned here.
    '''
    node = next(n for n in ast.parse(SOURCE).body
                if isinstance(n, ast.FunctionDef) and n.name == 'set_jrc_author')
    calls = [d for d in node.decorator_list if isinstance(d, ast.Call)
             and getattr(d.func, 'id', '') == 'api_restricted']
    assert calls, "set_jrc_author writes to dois and must be gated"
    assert [a.value for a in calls[0].args] == ['author']


def test_no_routed_view_mutates_data_without_a_gate():
    ''' Derived. Telemetry is excluded by name - a hit counter, a last-seen
        stamp and an avatar cache all write on ordinary page renders, which is
        why a naive scan lights up nearly every view in the app.
    '''
    lines = SOURCE.splitlines()
    tree = ast.parse(SOURCE)
    writes = ('insert_one', 'insert_many', 'update_one', 'update_many',
              'delete_one', 'delete_many', 'replace_one', 'bulk_write',
              'find_one_and_update')
    telemetry = {'endpoint_access', '_touch_last_seen', '_refresh_photo',
                 'api_token_user', 'login'}
    ungated = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef) or node.name in telemetry:
            continue
        routes = [d for d in node.decorator_list if isinstance(d, ast.Call)
                  and getattr(d.func, 'attr', '') == 'route']
        if not routes:
            continue
        body = '\n'.join(lines[node.body[0].lineno - 1:node.end_lineno])
        if not any(w in body for w in writes):
            continue
        names = []
        for dec in node.decorator_list:
            func = dec.func if isinstance(dec, ast.Call) else dec
            names.append(getattr(func, 'id', getattr(func, 'attr', '')))
        if 'restricted' not in names and 'api_restricted' not in names:
            ungated.append(node.name)
    assert not ungated, f"routed views writing with no gate: {ungated}"
