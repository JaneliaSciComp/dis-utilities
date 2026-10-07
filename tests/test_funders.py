''' Parsing funders out of registrar metadata, and rolling them up the
    Crossref Funder Registry hierarchy.

    Every test here is offline. dis_funder_lib resolves a funder's ancestors
    and crosswalks a ROR through live APIs, but only on a cache miss - so the
    cache is pre-seeded and no request is ever made. A test that started
    hitting the network would be a bug in the caching, and is worth noticing.
'''

import types

import pytest

import dis_funder_lib as DFL


class FakeFunderCache:
    ''' Stands in for the funder collection. Unlike helpers.FakeCollection it
        matches on the keys this cache actually uses - id and ror - so a
        lookup hits and the library never calls out.
    '''

    def __init__(self, docs=None):
        self.docs = list(docs or [])
        self.updates = []

    def find_one(self, query, *_args, **_kwargs):
        for doc in self.docs:
            if all(doc.get(k) == v for k, v in query.items()):
                return doc
        return None

    def update_one(self, query, update, upsert=False):
        self.updates.append({'query': query, 'update': update, 'upsert': upsert})
        return types.SimpleNamespace(matched_count=1, modified_count=1)


@pytest.fixture(name='cache')
def fixture_cache():
    ''' NINDS under NIH under HHS, plus HHMI at the top level and its ROR. '''
    return FakeFunderCache([
        {'id': '100000065', 'name': 'National Institute of Neurological Disorders and Stroke',
         'ancestors': ['100000016', '100000002']},
        {'id': '100000002', 'name': 'National Institutes of Health',
         'ancestors': ['100000016']},
        {'id': '100000016', 'name': 'U.S. Department of Health & Human Services',
         'ancestors': []},
        {'id': '100000011', 'name': 'Howard Hughes Medical Institute',
         'ancestors': [], 'ror': '006w34k90'},
    ])


def crossref(*funders):
    return {'doi': '10.1038/test', 'funder': list(funders)}


# --- normalization --------------------------------------------------------

@pytest.mark.parametrize('value,expected', [
    ('10.13039/100000011', '100000011'),
    ('https://doi.org/10.13039/100000011', '100000011'),
    ('http://dx.doi.org/10.13039/100000011', '100000011'),
    ('100000011', '100000011'),
    ('10.13039/abc', None),
    ('https://ror.org/006w34k90', None),
    ('10.1101/2024.01.01', None),
    ('', None),
    (None, None),
])
def test_funder_id_normalization(value, expected):
    assert DFL.normalize_funder_id(value) == expected


@pytest.mark.parametrize('value,expected', [
    ('https://ror.org/006w34k90', '006w34k90'),
    ('006w34k90', '006w34k90'),
    ('013SK6X84', '013sk6x84'),
    ('10.13039/100000011', None),
    (None, None),
])
def test_ror_normalization(value, expected):
    assert DFL.normalize_ror(value) == expected


# --- Crossref parsing -----------------------------------------------------

def test_funder_doi_and_awards_are_kept(cache):
    rec = crossref({'DOI': '10.13039/100000065', 'name': 'NINDS',
                    'award': ['R01NS104670', 'R01NS123456']})
    out = DFL.parse_funders(rec, cache)
    assert out == [{'id': '100000065', 'name': 'NINDS',
                    'awards': ['R01NS104670', 'R01NS123456']}]


def test_id_is_found_in_the_parallel_id_array(cache):
    ''' Some deposits leave the DOI field empty and carry the ID only in id[]. '''
    rec = crossref({'name': 'NIH',
                    'id': [{'id': '10.13039/100000002', 'id-type': 'DOI'}]})
    assert DFL.parse_funders(rec, cache)[0]['id'] == '100000002'


def test_a_ror_in_the_id_array_is_crosswalked(cache):
    ''' Crossref now accepts a ROR in place of a Funder Registry DOI. Looking
        only for id-type DOI drops these silently - 494 entries' worth.
    '''
    rec = crossref({'name': None,
                    'id': [{'id': 'https://ror.org/006w34k90', 'id-type': 'ROR'}]})
    out = DFL.parse_funders(rec, cache)
    assert out[0]['id'] == '100000011'
    assert out[0]['ror'] == '006w34k90'


def test_a_comma_truncated_doi_is_salvaged(cache):
    ''' Seen in the wild: the funder DOI with the funder name run into it. '''
    rec = crossref({'DOI': '10.13039/100000002,%22national%20institutes%20of%20health%22',
                    'name': 'NIH'})
    assert DFL.parse_funders(rec, cache)[0]['id'] == '100000002'


def test_a_funder_with_no_id_is_kept_not_dropped(cache):
    ''' Real funding we cannot join. Dropping it understates what we hold;
        guessing an ID from the name would misattribute it.
    '''
    out = DFL.parse_funders(crossref({'name': 'Klingenstein-Simons Fellowship Award'}), cache)
    assert out == [{'id': None, 'name': 'Klingenstein-Simons Fellowship Award', 'awards': []}]


def test_an_entry_with_neither_id_nor_name_is_dropped(cache):
    assert DFL.parse_funders(crossref({'award': ['X1']}), cache) == []


def test_repeated_funders_merge_and_pool_their_awards(cache):
    rec = crossref({'DOI': '10.13039/100000002', 'name': 'NIH', 'award': ['A1']},
                   {'DOI': '10.13039/100000002', 'name': 'NIH', 'award': ['A2', 'A1']})
    out = DFL.parse_funders(rec, cache)
    assert len(out) == 1
    assert out[0]['awards'] == ['A1', 'A2']


# --- DataCite parsing -----------------------------------------------------

def test_datacite_ror_is_crosswalked(cache):
    rec = {'doi': '10.5061/dryad.x', 'fundingReferences': [
        {'funderName': 'Howard Hughes Medical Institute',
         'funderIdentifier': 'https://ror.org/006w34k90',
         'funderIdentifierType': 'ROR', 'awardNumber': 'GT1234'}]}
    out = DFL.parse_funders(rec, cache)
    assert out[0]['id'] == '100000011'
    assert out[0]['awards'] == ['GT1234']


def test_datacite_crossref_funder_id_is_taken_as_is(cache):
    rec = {'doi': '10.5061/dryad.x', 'fundingReferences': [
        {'funderName': 'NIH', 'funderIdentifier': '10.13039/100000002',
         'funderIdentifierType': 'Crossref Funder ID'}]}
    assert DFL.parse_funders(rec, cache)[0]['id'] == '100000002'


def test_both_registrars_on_one_record_merge(cache):
    rec = {'doi': '10.1038/x',
           'funder': [{'DOI': '10.13039/100000011', 'name': 'HHMI', 'award': ['A1']}],
           'fundingReferences': [{'funderName': 'HHMI',
                                  'funderIdentifier': 'https://ror.org/006w34k90',
                                  'funderIdentifierType': 'ROR'}]}
    out = DFL.parse_funders(rec, cache)
    assert len(out) == 1
    assert out[0]['id'] == '100000011'
    assert out[0]['ror'] == '006w34k90'


# --- rollup ---------------------------------------------------------------

def test_rollup_includes_every_ancestor(cache):
    ''' The point of the whole exercise: a paper naming only NINDS has to
        answer a query for NIH.
    '''
    funders = [{'id': '100000065', 'name': 'NINDS', 'awards': []}]
    assert DFL.rollup_ids(funders, cache) == ['100000002', '100000016', '100000065']


def test_rollup_deduplicates_shared_ancestors(cache):
    funders = [{'id': '100000065', 'name': 'NINDS', 'awards': []},
               {'id': '100000002', 'name': 'NIH', 'awards': []}]
    assert DFL.rollup_ids(funders, cache) == ['100000002', '100000016', '100000065']


def test_rollup_skips_funders_with_no_id(cache):
    funders = [{'id': None, 'name': 'Some Foundation', 'awards': []}]
    assert DFL.rollup_ids(funders, cache) == []


def test_a_top_level_funder_rolls_up_to_itself(cache):
    funders = [{'id': '100000011', 'name': 'HHMI', 'awards': []}]
    assert DFL.rollup_ids(funders, cache) == ['100000011']


# --- hierarchy walk -------------------------------------------------------

def test_ancestor_walk_finds_the_path():
    tree = {'100000016': {'100000002': {'100000065': {}, '100000057': {}},
                          '100000058': {'more': True}}}
    assert DFL._ancestors_from_tree(tree, '100000065') == ['100000016', '100000002']
    assert DFL._ancestors_from_tree(tree, '100000002') == ['100000016']
    assert DFL._ancestors_from_tree(tree, '100000016') == []


def test_ancestor_walk_ignores_the_truncation_marker():
    ''' "more": True marks a subtree the API cut off - a marker, not a node. '''
    assert DFL._ancestors_from_tree({'100000016': {'more': True}}, 'more') == []


def test_ancestor_walk_tolerates_an_absent_funder():
    assert DFL._ancestors_from_tree({'100000016': {}}, '999999') == []


def test_rollup_resolves_ancestors_into_the_cache(cache):
    ''' An ancestor lands in jrc_funder_ids and so becomes a /funder/<id> page
        of its own, but nothing names it directly, so nothing else would ever
        resolve it. Left unresolved, HHS was rolled up onto 803 DOIs and
        rendered as "Funder 100000016".
    '''
    looked_up = []
    original = DFL.funder_record

    def spy(fid, coll=None, write=True):
        looked_up.append(fid)
        return original(fid, coll, write)

    DFL.funder_record = spy
    try:
        DFL.rollup_ids([{'id': '100000065', 'name': 'NINDS', 'awards': []}], cache)
    finally:
        DFL.funder_record = original
    assert '100000065' in looked_up
    assert '100000002' in looked_up, "NIH (an ancestor) was never resolved"
    assert '100000016' in looked_up, "HHS (an ancestor) was never resolved"


def test_an_unnamed_cache_entry_is_not_treated_as_a_hit():
    ''' ror_to_funder upserts {id, ror} without resolving the funder. Treating
        that as a cache hit made the gap permanent: the short-circuit returned
        the nameless record and nothing ever went back for the name, so the
        funder rendered as "Funder 501100001671" forever.
    '''
    cache = FakeFunderCache([{'id': '501100001671', 'ror': '02550n020'}])
    calls = []
    DFL._get_json, original = lambda url: calls.append(url) or None, DFL._get_json
    try:
        DFL.funder_record('501100001671', cache)
    finally:
        DFL._get_json = original
    assert calls, "an incomplete cache entry must trigger a lookup, not short-circuit"


def test_a_complete_cache_entry_does_short_circuit():
    cache = FakeFunderCache([{'id': '100000011', 'name': 'HHMI', 'ancestors': []}])
    calls = []
    DFL._get_json, original = lambda url: calls.append(url) or None, DFL._get_json
    try:
        rec = DFL.funder_record('100000011', cache)
    finally:
        DFL._get_json = original
    assert not calls, "a named cache entry must not hit the network"
    assert rec['name'] == 'HHMI'
