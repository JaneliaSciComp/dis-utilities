''' /external_dois_metrics and its drill-down: the query behind each clickable count.

    The invariant that matters is that a count on the page and the list it links to agree,
    so these test _external_query against the same labeling the page counts with. The
    functions are extracted with ast, because importing dis_responder needs Flask, a config
    file and a database.
'''

import ast
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / 'api' / 'dis_responder.py').read_text(encoding='utf-8')
TREE = ast.parse(SOURCE)


def _namespace():
    ns = {}
    for node in TREE.body:
        named = getattr(node, 'name', None)
        targets = [t.id for t in getattr(node, 'targets', []) if isinstance(t, ast.Name)]
        if named in ('_external_scope', '_external_query', '_external_type_label',
                     '_external_year_bucket') or 'DOI_TYPE_LABELS' in targets:
            exec(ast.get_source_segment(SOURCE, node), ns)    # pylint: disable=W0122
    return ns


NS = _namespace()
LABELS, QUERY, SCOPE = NS['DOI_TYPE_LABELS'], NS['_external_query'], NS['_external_scope']
LABEL, BUCKET = NS['_external_type_label'], NS['_external_year_bucket']
EMPTY = {'$in': [None, '']}

# Every type value the external collection holds today, plus the empty and the unmapped.
STORED = ['journal-article', 'posted-content', 'book-chapter', 'proceedings-article', 'Preprint',
          None, '', 'something-new']


@pytest.mark.parametrize('stored', STORED)
def test_every_stored_type_is_reachable_from_the_label_it_is_counted_under(stored):
    query = QUERY('type', LABEL(stored), 'All')['type']
    if not stored:
        assert query == EMPTY
    else:
        assert stored in query['$in'], f"{stored!r} is counted but its drill-down would not list it"


@pytest.mark.parametrize('stored', STORED)
def test_by_year_and_by_type_put_every_record_in_the_same_place(stored):
    label = LABEL(stored)
    assert BUCKET(stored) == (label if label in ('JournalArticle', 'Preprint') else 'Other')


def test_a_stored_label_is_a_preprint_on_the_by_year_tab_too():
    ''' The one external record stored as type "Preprint" used to fall into Other there. '''
    assert BUCKET('Preprint') == 'Preprint' and BUCKET('posted-content') == 'Preprint'
    assert BUCKET('JournalArticle') == 'JournalArticle'
    assert BUCKET('book-chapter') == 'Other' and BUCKET(None) == 'Other'


def test_a_label_lists_every_type_folded_into_it():
    assert set(QUERY('type', 'JournalArticle', 'All')['type']['$in']) >= {'journal-article', 'DataPaper'}
    assert set(QUERY('type', 'BookChapter', 'All')['type']['$in']) >= {'book', 'book-chapter'}


def test_the_labels_are_the_ones_dois_metrics_uses():
    ''' One table, so the two pages cannot drift apart. '''
    internal = next(n for n in TREE.body if getattr(n, 'name', '') == 'show_dois_metrics')
    text = ast.get_source_segment(SOURCE, internal)
    assert 'type_merge = DOI_TYPE_LABELS' in text
    assert '"posted-content": "Preprint"' not in text, "dois_metrics grew its own copy"
    assert SOURCE.count('"posted-content": "Preprint"') == 1


def test_the_year_scopes_every_kind():
    assert QUERY('journal', 'Nature', '2024')['jrc_publishing_date'] == {'$regex': '^2024'}
    assert 'jrc_publishing_date' not in QUERY('journal', 'Nature', 'All')
    assert SCOPE('All') == {}


def test_source_and_journal_and_tags():
    assert QUERY('source', 'PMC', 'All')['jrc_ack_source'] == 'PMC'
    # the $group folds missing, null and '' into these rows, so the list must match all three
    assert QUERY('source', '(not recorded)', 'All')['jrc_ack_source'] == EMPTY
    assert QUERY('journal', '(none)', 'All')['jrc_journal'] == EMPTY
    assert QUERY('tags', 'tagged', 'All')['jrc_acknowledge.0'] == {'$exists': True}
    assert QUERY('tags', 'untagged', 'All')['jrc_acknowledge.0'] == {'$exists': False}


def test_an_unknown_grouping_is_refused():
    assert QUERY('nonsense', 'x', 'All') is None


def test_the_pages_are_linked_to_each_other_and_to_the_menu():
    html_src = (ROOT / 'api' / 'dis_html.py').read_text(encoding='utf-8')
    assert '"External DOI metrics": "external_dois_metrics"' in html_src
    assert '("External DOI metrics", "/external_dois_metrics")' in SOURCE
    assert '("Internal DOI metrics", "/dois_metrics")' in SOURCE


def test_the_list_title_is_left_for_the_template_to_escape():
    view = next(n for n in TREE.body if getattr(n, 'name', '') == 'show_external_dois_list')
    text = ast.get_source_segment(SOURCE, view)
    assert 'escape(' not in text, "general.html autoescapes the title; escaping here doubles it"


def test_a_record_without_acknowledgement_text_does_not_reach_the_table_bare():
    view = next(n for n in TREE.body if getattr(n, 'name', '') == 'show_external_dois_list')
    text = ast.get_source_segment(SOURCE, view)
    assert "row['jrc_acknowledgements'] = row.get('jrc_acknowledgements') or ''" in text
