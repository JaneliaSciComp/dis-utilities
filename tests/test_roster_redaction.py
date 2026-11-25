''' The roster listing endpoints (/orcid, /orcid/active, /orcid/<oid>) hand an
    anonymous caller names and ORCIDs only.

    They used to return the whole orcid document - userIdO365, employeeId,
    hireDate, workerType, the authorization list, the API-token stamp - to
    anyone who knew the path; "Do not display" in the Swagger docstring was
    the only protection. The reduction is an allowlist (strip_roster), because
    the document grows fields faster than a denylist would be maintained, and
    a Bearer token holding admin or orcid still gets everything, which is what
    a script that needs hireDate or employeeId from the API must send.

    Extracted with ast rather than imported (dis_responder needs Flask, a
    config file and a database).
'''

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / 'api' / 'dis_responder.py').read_text(encoding='utf-8')
_TREE = ast.parse(SOURCE)
_TOP = {getattr(n, 'name', None) or getattr(getattr(n, 'targets', [None])[0], 'id', None): n
        for n in _TREE.body if isinstance(n, (ast.FunctionDef, ast.Assign))}


def _ns(entitled):
    ns = {'may_see_identifiers': lambda: entitled}
    for name in ('ROSTER_PUBLIC_FIELDS', 'strip_roster'):
        exec(ast.get_source_segment(SOURCE, _TOP[name]), ns)    # pylint: disable=W0122
    return ns


FULL = {'orcid': '0000-0001-8374-6008', 'given': ['Rob', 'Robert'], 'family': ['Svirskas'],
        'affiliations': ['Scientific Computing Software'], 'userIdO365': 'x@example.org',
        'employeeId': '12345', 'hireDate': '2001-01-01', 'workerType': 'Employee',
        'authorization': ['admin'], 'api_token': {'digest': 'abc'}, 'managed': [],
        'alumni': False, 'updated': '2026-01-01', 'updated_by': 'someone'}


def test_an_anonymous_caller_gets_the_public_fields_only():
    ns = _ns(entitled=False)
    assert ns['ROSTER_PUBLIC_FIELDS'] == ('orcid', 'given', 'family', 'affiliations')
    out = ns['strip_roster'](dict(FULL))
    assert out == {'orcid': '0000-0001-8374-6008', 'given': ['Rob', 'Robert'],
                   'family': ['Svirskas'], 'affiliations': ['Scientific Computing Software']}


def test_an_entitled_caller_gets_the_whole_document():
    out = _ns(entitled=True)['strip_roster'](dict(FULL))
    assert out == FULL


def test_it_is_an_allowlist_so_a_new_field_is_private_by_default():
    ''' The point of the design: a field added to the orcid document next year
        does not leak because nobody remembered to add it to a denylist. '''
    out = _ns(entitled=False)['strip_roster']({'orcid': 'x', 'brand_new_secret': 1})
    assert out == {'orcid': 'x'}


def test_lists_and_non_dicts_are_handled_like_strip_identifiers():
    strip = _ns(entitled=False)['strip_roster']
    assert strip([dict(FULL), {'given': ['A'], 'hireDate': 'x'}]) == [
        {'orcid': '0000-0001-8374-6008', 'given': ['Rob', 'Robert'], 'family': ['Svirskas'],
         'affiliations': ['Scientific Computing Software']},
        {'given': ['A']}]
    assert strip('not a record') == 'not a record'


def test_all_three_listing_routes_are_marked_and_reduced():
    for view in ('show_oids', 'show_active_oids', 'show_oid'):
        node = _TOP[view]
        decorators = [ast.get_source_segment(SOURCE, d) for d in node.decorator_list]
        assert any(d.startswith("redacts('admin', 'orcid'") for d in decorators), view
        body = ast.get_source_segment(SOURCE, node)
        assert "result['data'].append(strip_roster(row))" in body, view
        assert "result['data'].append(row)" not in body, f"{view} still appends a raw row"
        assert 'Without authorization' in (ast.get_docstring(node) or ''), view


def test_the_works_route_is_not_a_roster_listing():
    ''' /orcid/works/<oid> returns DOIs, not roster documents; it is out of
        scope here and must not have been swept in by accident. '''
    body = ast.get_source_segment(SOURCE, _TOP['show_oid_works'])
    assert 'strip_roster' not in body
