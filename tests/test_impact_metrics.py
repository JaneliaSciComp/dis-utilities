''' The impact calculations: h, g, i10, i100, and what counts as one work.

    These are the numbers people read as a judgement of themselves and of the
    institution, so the definitions are pinned here rather than left to the
    reader of a loop. Two things are tested: that each index matches its
    textbook definition, and that one publication counts once.

    The functions are extracted from dis_responder with ast rather than
    imported, because importing it needs Flask, a config file and a database,
    none of which the arithmetic depends on.
'''

import ast
import statistics
from datetime import datetime
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / 'api' / 'dis_responder.py').read_text(encoding='utf-8')


def _extract(*names):
    ''' Pull named top-level functions out of dis_responder into a namespace. '''
    import doi_common.doi_common as DL
    tree = ast.parse(SOURCE)
    ns = {'statistics': statistics, 'datetime': datetime, 'DL': DL}
    for name in names:
        node = next(n for n in tree.body
                    if isinstance(n, ast.FunctionDef) and n.name == name)
        exec(ast.get_source_segment(SOURCE, node), ns)      # pylint: disable=W0122
    return ns


pytest.importorskip('doi_common.doi_common')
NS = _extract('citation_impact_indices', 'merge_preprint_pairs')
indices = NS['citation_impact_indices']
merge = NS['merge_preprint_pairs']


# --- the indices -----------------------------------------------------------

@pytest.mark.parametrize('counts,h,why', [
    ([], 0, 'no works'),
    ([0, 0, 0], 0, 'works, no citations'),
    ([1], 1, 'one paper cited once'),
    ([100], 1, 'one paper cited a hundred times is still h=1'),
    ([5, 5, 5, 5, 5], 5, 'five papers, five citations each'),
    ([6, 5, 3, 1, 0], 3, 'the textbook case'),
    ([10, 8, 5, 4, 3], 4, 'h stops where the count falls below the rank'),
])
def test_h_index(counts, h, why):
    assert indices(counts)['h_index'] == h, why


def test_h_is_order_independent():
    ''' Callers pass sorted lists; the function must not rely on it. '''
    assert indices([3, 1, 6, 0, 5])['h_index'] == indices([6, 5, 3, 1, 0])['h_index']


@pytest.mark.parametrize('counts,g', [
    ([], 0),
    ([10, 8, 5, 4, 3], 5),      # cumulative 30 >= 25
    ([1, 1, 1], 1),             # cumulative 3 < 9, 2 < 4, 1 >= 1
])
def test_g_index(counts, g):
    assert indices(counts)['g_index'] == g


def test_g_is_at_least_h():
    ''' True by definition, and the cheapest check that one of them is wrong. '''
    for counts in ([6, 5, 3, 1, 0], [10, 8, 5, 4, 3], [1], [7, 7, 7, 7]):
        idx = indices(counts)
        assert idx['g_index'] >= idx['h_index'], counts


def test_i10_and_i100_are_plain_thresholds():
    idx = indices([500, 100, 99, 10, 9, 0])
    assert idx['i10'] == 4          # 500, 100, 99, 10
    assert idx['i100'] == 2         # 500, 100 - the boundary counts


def test_uncited_works_do_not_change_any_index():
    ''' Padding with zeros is the classic way to inflate g. '''
    assert indices([6, 5, 3, 1]) == indices([6, 5, 3, 1, 0, 0, 0, 0, 0])


# --- one publication, one work ---------------------------------------------

def _pre(doi, cnt):
    return {'doi': doi, 'jrc_citation_count': cnt, 'subtype': 'preprint'}


def _art(doi, cnt, preprints=None):
    row = {'doi': doi, 'jrc_citation_count': cnt, 'type': 'journal-article'}
    if preprints:
        row['jrc_preprint'] = preprints
    return row


def test_a_preprint_and_its_article_are_one_work():
    works = merge([_art('10.1/art', 30, ['10.1/pre']), _pre('10.1/pre', 4)])
    assert len(works) == 1
    assert works[0][1] == 30, 'citations are maxed, not summed'
    assert works[0][0]['doi'] == '10.1/art', 'the article represents the work'


def test_the_link_is_followed_in_either_direction():
    ''' jrc_preprint usually sits on the article, but not always. '''
    works = merge([_art('10.1/art', 30), {'doi': '10.1/pre', 'jrc_citation_count': 4,
                                          'subtype': 'preprint',
                                          'jrc_preprint': ['10.1/art']}])
    assert len(works) == 1


def test_doi_matching_ignores_case():
    works = merge([_art('10.1/ART', 30, ['10.1/pre']), _pre('10.1/pre', 4)])
    assert len(works) == 1


def test_a_link_to_something_we_do_not_hold_is_left_alone():
    works = merge([_art('10.1/art', 30, ['10.1/never-ingested'])])
    assert len(works) == 1
    assert works[0][1] == 30


def test_unlinked_records_are_untouched():
    assert len(merge([_art('10.1/a', 5), _art('10.1/b', 3), _pre('10.1/c', 1)])) == 3


def test_a_record_with_no_doi_is_skipped_not_crashed_on():
    assert merge([{'jrc_citation_count': 9}]) == []


def test_merging_cannot_increase_the_work_count():
    rows = [_art('10.1/art', 30, ['10.1/pre']), _pre('10.1/pre', 4), _art('10.1/x', 2)]
    assert len(merge(rows)) <= len(rows)


def test_merging_lowers_h_when_both_halves_cleared_the_bar():
    ''' The reason this matters: two halves of one paper each clearing the h
        bar is how an h-index gets overstated.
    '''
    # Ten papers, each held twice and each cited 20 times. Counted as 20
    # records the h-index is 20; counted as the 10 works they are, it is 10.
    rows = [_art(f'10.1/art{i}', 20, [f'10.1/pre{i}']) for i in range(10)]
    rows += [_pre(f'10.1/pre{i}', 20) for i in range(10)]
    split = indices([r['jrc_citation_count'] for r in rows])['h_index']
    merged = indices([c for _, c in merge(rows)])['h_index']
    assert split == 20, 'twenty records each cited twenty times'
    assert merged == 10, 'ten works each cited twenty times'


# --- drift guard -----------------------------------------------------------

def test_nobody_hand_rolls_an_h_index_loop():
    ''' There were two implementations; /citation_metrics had its own copy and
        knew nothing about the g-index or the zero-stripping the shared one had
        grown. One definition, or the same corpus gets two h-indices.
    '''
    assert SOURCE.count('def citation_impact_indices') == 1
    assert SOURCE.count('h_index = idx') == 1, \
        "an inline h-index loop is back; call citation_impact_indices instead"
