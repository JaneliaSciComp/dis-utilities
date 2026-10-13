''' How a work is matched to a person, and what the pages claim about it.

    Three clauses decide whether a DOI belongs to someone: Janelia credited
    them, their name matches, or their ORCID iD is in the deposit. The third
    one was dead for years - it queried a top-level "orcid" field that no dois
    document has ever carried - while three pages told readers it was running.
    These tests pin the clause shapes so it cannot quietly die again.
'''

import ast
import json
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / 'api' / 'dis_responder.py').read_text(encoding='utf-8')
ORCID_RE = re.compile(r'^\d{4}-\d{4}-\d{4}-\d{3}[\dX]$')


def _ns():
    tree = ast.parse(SOURCE)
    ns = {'re': re, 'ORCID_RE': ORCID_RE}
    for name in ('name_search_payload', 'deposit_orcid_payload', 'orcid_payload'):
        node = next(n for n in tree.body
                    if isinstance(n, ast.FunctionDef) and n.name == name)
        exec(ast.get_source_segment(SOURCE, node), ns)   # pylint: disable=W0122
    return ns


NS = _ns()
deposit_orcid_payload = NS['deposit_orcid_payload']
orcid_payload = NS['orcid_payload']

PERSON = {'orcid': '0000-0002-1825-0097', 'employeeId': '12345',
          'given': ['Ada'], 'family': ['Lovelace']}


# --- where the iD actually lives -------------------------------------------

def test_the_deposit_clause_looks_where_registrars_put_the_id():
    ''' Crossref: author[].ORCID. DataCite: creators[].nameIdentifiers[]. '''
    fields = {list(c)[0] for c in deposit_orcid_payload(PERSON['orcid'])['$or']}
    assert fields == {'author.ORCID', 'creators.nameIdentifiers.nameIdentifier'}


def test_the_id_is_anchored_so_it_cannot_match_by_prefix():
    ''' Stored as a URL, so the iD is matched at the end of the value. A bare
        substring match would let one iD match another that extends it.
    '''
    for clause in deposit_orcid_payload(PERSON['orcid'])['$or']:
        pattern = list(clause.values())[0]['$regex']
        assert pattern.endswith('$'), pattern
        assert re.search(pattern, 'https://orcid.org/0000-0002-1825-0097')
        assert re.search(pattern, 'http://orcid.org/0000-0002-1825-0097')
        assert not re.search(pattern, 'https://orcid.org/0000-0002-1825-00971')


def test_a_dot_in_an_id_cannot_act_as_a_wildcard():
    ''' The iD is escaped before it becomes a regex. '''
    pattern = list(deposit_orcid_payload('0000-0002-1825-009X')['$or'][0].values())[0]['$regex']
    assert '\\-' in pattern or re.escape('0000-0002-1825-009X') in pattern


# --- the three clauses -----------------------------------------------------

def _clause_json(payload):
    return json.dumps(payload, sort_keys=True)


def test_all_three_clauses_are_present():
    payload = orcid_payload(PERSON['orcid'], PERSON, PERSON['employeeId'])
    blob = _clause_json(payload)
    assert '"jrc_author"' in blob, 'Janelia credit'
    assert '"author.ORCID"' in blob, 'iD in the deposit'
    assert 'family' in blob or 'familyName' in blob, 'name match'


def test_no_clause_queries_a_top_level_orcid_field():
    ''' Nothing writes one. A clause on it can only ever match zero documents,
        which is what made this arm dead while the prose advertised it.
    '''
    payload = orcid_payload(PERSON['orcid'], PERSON, PERSON['employeeId'])
    assert '"orcid":' not in _clause_json(payload)


def test_an_employee_id_passed_as_oid_still_uses_the_real_id():
    ''' /userui passes orc['employeeId'] as oid. Taking the iD from the roster
        record instead means both person pages search the same way.
    '''
    by_orcid = orcid_payload(PERSON['orcid'], PERSON, PERSON['employeeId'])
    by_eid = orcid_payload(PERSON['employeeId'], PERSON, PERSON['employeeId'])
    assert _clause_json(by_orcid) == _clause_json(by_eid)
    assert '"author.ORCID"' in _clause_json(by_eid)
    # ...and the employee ID is not smuggled into the iD clause, where it would
    # build a regex that cannot match a real ORCID URL.
    deposit = [c for c in by_eid['$or'] if 'author.ORCID' in _clause_json(c)][0]
    assert PERSON['employeeId'] not in _clause_json(deposit)
    # Assert on what the regex DOES, not how it is spelled - re.escape renders
    # the hyphens as \- so the iD is not a literal substring of the pattern.
    pattern = deposit['$or'][0]['author.ORCID']['$regex']
    assert re.search(pattern, f"https://orcid.org/{PERSON['orcid']}")


def test_a_person_with_no_id_gets_the_other_two_clauses_only():
    person = {k: v for k, v in PERSON.items() if k != 'orcid'}
    blob = _clause_json(orcid_payload(None, person, person['employeeId']))
    assert '"author.ORCID"' not in blob
    assert '"jrc_author"' in blob


def test_a_person_with_no_employee_id_still_matches_by_id_and_name():
    person = {k: v for k, v in PERSON.items() if k != 'employeeId'}
    blob = _clause_json(orcid_payload(person['orcid'], person, None))
    assert '"jrc_author"' not in blob
    assert '"author.ORCID"' in blob


def test_a_name_only_person_gets_a_bare_name_payload():
    person = {'given': ['Ada'], 'family': ['Lovelace']}
    payload = orcid_payload(None, person, None)
    assert '$or' not in payload or '"author.ORCID"' not in _clause_json(payload)
