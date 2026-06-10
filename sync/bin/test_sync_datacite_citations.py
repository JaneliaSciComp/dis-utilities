''' test_sync_datacite_citations.py

    Unit tests for the pure citation-combination logic (combine_counts) extracted
    from sync_datacite_citations.py. No network or DB: this exercises the union
    dedup, the DataCite REST floor, the GraphQL on/off paths, and the
    preserve-on-error regression guard.

    Run (no pytest needed):
        python test_sync_datacite_citations.py
'''

import sys
import types

# Stub tqdm so the module imports in environments without it (only used for
# progress bars / debug writes, none of which combine_counts touches).
if 'tqdm' not in sys.modules:
    _tq = types.ModuleType('tqdm')
    class _T:    # pylint: disable=too-few-public-methods,missing-class-docstring
        def __init__(self, *a, **k):
            pass
        @staticmethod
        def write(*a, **k):
            pass
    _tq.tqdm = _T
    sys.modules['tqdm'] = _tq

import sync_datacite_citations as M    # noqa: E402


def make_res(**over):
    ''' A fetch_doi()-shaped result with sensible defaults, overridable per test '''
    res = {'row': {'_id': 'x'}, 'doi': '10.x/y',
           'datacite': 0, 'existing': None, 'existing_dois': [],
           'openalex': set(), 'scholex': set(), 'datacite_dois': None,
           'is_figshare': False, 'figshare': None}
    res.update(over)
    return res


def test_union_dedups_across_sources():
    ''' A citing work found by two sources counts once '''
    res = make_res(openalex={'10.1/a', '10.1/b'}, scholex={'10.1/b', '10.1/c'})
    cc = M.combine_counts(res, graphql=False)
    assert cc['combined'] == 3, cc
    assert cc['citing'] == ['10.1/a', '10.1/b', '10.1/c'], cc
    assert cc['sources'] == {'datacite': 0, 'openalex': 2, 'scholexplorer': 2}, cc
    assert cc['errored'] is False and cc['preserved'] is False


def test_datacite_rest_count_is_a_floor():
    ''' The bare REST count floors the union when it is larger '''
    res = make_res(datacite=10, openalex={'10.1/a'}, scholex=set())
    cc = M.combine_counts(res, graphql=False)
    assert cc['combined'] == 10, cc            # floor wins over union size (1)
    assert cc['sources']['datacite'] == 10, cc


def test_union_beats_floor_when_larger():
    ''' A union larger than the REST floor wins '''
    res = make_res(datacite=2, openalex={'10.1/a', '10.1/b', '10.1/c'})
    cc = M.combine_counts(res, graphql=False)
    assert cc['combined'] == 3, cc


def test_graphql_off_ignores_datacite_dois():
    ''' With GraphQL off, datacite_dois is not folded in and dc_n stays None '''
    res = make_res(openalex={'10.1/a'}, datacite_dois={'10.1/z', '10.1/zz'})
    cc = M.combine_counts(res, graphql=False)
    assert cc['dc_n'] is None, cc
    assert cc['combined'] == 1, cc             # only OpenAlex counted
    assert 'datacite' in cc['sources'] and cc['sources']['datacite'] == 0


def test_graphql_on_folds_in_and_sets_datacite_source():
    ''' With GraphQL on, its citing DOIs join the union and become the datacite count
        (datacite REST floor left at 0 here to isolate the union behavior) '''
    res = make_res(datacite=0, openalex={'10.1/a'},
                   datacite_dois={'10.1/a', '10.1/z'})
    cc = M.combine_counts(res, graphql=True)
    assert cc['dc_n'] == 2, cc
    assert cc['combined'] == 2, cc             # union {a, z}
    assert cc['sources']['datacite'] == 2, cc  # GraphQL count, not the REST count


def test_error_preserves_previous_higher_count():
    ''' A source error must not regress a previously-stored higher count '''
    res = make_res(openalex=None, scholex={'10.1/a'}, existing=50)
    cc = M.combine_counts(res, graphql=False)
    assert cc['errored'] is True, cc
    assert cc['preserved'] is True, cc
    assert cc['combined'] == 50, cc            # kept the stored 50, not the new 1


def test_error_does_not_inflate_when_previous_lower():
    ''' On error, a lower stored count is not used to inflate this run's result '''
    res = make_res(openalex=None, scholex={'10.1/a', '10.1/b'}, existing=1)
    cc = M.combine_counts(res, graphql=False)
    assert cc['errored'] is True and cc['preserved'] is False, cc
    assert cc['combined'] == 2, cc


def test_error_recovers_stored_citing_dois():
    ''' On error, previously-stored citing DOIs are re-folded into the union '''
    res = make_res(openalex=None, scholex={'10.1/a'},
                   existing_dois=['https://doi.org/10.1/OLD', '10.1/a'])
    cc = M.combine_counts(res, graphql=False)
    # 10.1/old (normalized + deduped) plus 10.1/a
    assert cc['citing'] == ['10.1/a', '10.1/old'], cc
    assert cc['combined'] == 2, cc


if __name__ == '__main__':
    tests = [v for k, v in sorted(globals().items()) if k.startswith('test_')]
    failures = 0
    for test in tests:
        try:
            test()
            print(f"PASS  {test.__name__}")
        except AssertionError as exc:
            failures += 1
            print(f"FAIL  {test.__name__}: {exc}")
    print(f"\n{len(tests) - failures}/{len(tests)} passed")
    sys.exit(1 if failures else 0)
