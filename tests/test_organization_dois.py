''' show_organization_dois: GET /organization/dois/<org>.

    Runs the real view, extracted with ast and given fake collections, because
    importing dis_responder needs Flask, a config file and a database.
'''

import ast
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / 'api' / 'dis_responder.py').read_text(encoding='utf-8')

SCS = 'Scientific Computing Software'


class FakeInvalidUsage(Exception):
    ''' Stands in for the real one, which only formats a JSON envelope. '''
    def __init__(self, message, status_code=400):
        super().__init__(message)
        self.status_code = status_code


class FakeCursor(list):
    ''' find() result; the view only sorts it by one key. '''
    def sort(self, key, _direction):                      # pylint: disable=W0221
        return sorted(self, key=lambda row: row.get(key) or '')


class FakeCollection:
    ''' Equality find_one, and find() on jrc_tag.name $in, which is all the view uses. '''
    def __init__(self, docs):
        self.docs = docs

    def find_one(self, query, _proj=None):
        key, val = next(iter(query.items()))
        return next((d for d in self.docs if d.get(key) == val), None)

    def find(self, query, _proj):
        names = query['jrc_tag.name']['$in']
        return FakeCursor(d for d in self.docs
                          if any(t['name'] in names for t in d['jrc_tag']))


def _tag(name):
    return {'name': name, 'code': 'x', 'type': 'suporg'}


DOIS = [
    {'doi': '10.1/b', 'jrc_publishing_date': '2024-05-01', 'jrc_tag': [_tag(SCS)],
     'type': 'journal-article', 'subtype': 'preprint', 'jrc_obtained_from': 'Crossref',
     'jrc_fulltext_url': 'http://full'},
    {'doi': '10.2/a', 'jrc_publishing_date': '2023-01-01',
     'jrc_tag': [_tag('Team A'), _tag('Team B')], 'type': 'other',
     'types': {'resourceTypeGeneral': 'Dataset'}, 'jrc_obtained_from': 'DataCite'},
    {'doi': '10.3/c', 'jrc_publishing_date': '2022-01-01', 'jrc_tag': [_tag('Other')],
     'jrc_obtained_from': 'Crossref'},
]


@pytest.fixture(name='view')
def _view():
    node = next(n for n in ast.parse(SOURCE).body
                if isinstance(n, ast.FunctionDef) and n.name == 'show_organization_dois')
    node.decorator_list = []
    db = type('Db', (), {
        'dois': FakeCollection(DOIS),
        'suporg': FakeCollection([{'name': SCS}, {'name': 'Team A'}, {'name': 'Team B'}]),
        # Both members tag the same DOI: it must come back once
        'org_group': FakeCollection([{'group': 'Project Teams',
                                      'members': ['Team A', 'Team B']}])})
    ns = {'DB': {'dis': db}, 'InvalidUsage': FakeInvalidUsage,
          'initialize_result': lambda: {'rest': {}}, 'generate_response': lambda r: r}
    exec(ast.get_source_segment(SOURCE, node), ns)        # pylint: disable=W0122
    return ns['show_organization_dois']


def test_a_single_organization_returns_its_dois_with_every_field(view):
    result = view(SCS)
    assert result['rest']['row_count'] == 1
    assert result['data'] == [{'doi': '10.1/b', 'jrc_publishing_date': '2024-05-01',
                               'tags': [_tag(SCS)], 'type': 'journal-article',
                               'subtype': 'preprint', 'jrc_fulltext_url': 'http://full'}]


def test_a_group_expands_to_its_members_and_lists_each_doi_once(view):
    result = view('Project Teams')
    assert [r['doi'] for r in result['data']] == ['10.2/a']
    assert len(result['data'][0]['tags']) == 2     # all tags, not only the matching one


def test_a_datacite_record_reports_its_general_resource_type(view):
    assert view('Project Teams')['data'][0]['type'] == 'Dataset'


def test_missing_optional_fields_come_back_as_none(view):
    row = view('Project Teams')['data'][0]
    assert row['subtype'] is None and row['jrc_fulltext_url'] is None


def test_an_unknown_name_is_a_404(view):
    with pytest.raises(FakeInvalidUsage) as err:
        view('Nope')
    assert err.value.status_code == 404
