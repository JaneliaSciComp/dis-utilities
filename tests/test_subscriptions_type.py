''' /subscriptions/type/<jtype>: the helpers behind the "Where Janelia publishes" tab.

    Extracted with ast, because importing dis_responder needs Flask, a config file and a
    database.
'''

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / 'api' / 'dis_responder.py').read_text(encoding='utf-8')
TREE = ast.parse(SOURCE)


def _namespace():
    ns = {}
    for node in TREE.body:
        if getattr(node, 'name', None) in ('_latest_apc', '_janelia_journal_use'):
            exec(ast.get_source_segment(SOURCE, node), ns)    # pylint: disable=W0122
    return ns


NS = _namespace()
LATEST, USE = NS['_latest_apc'], NS['_janelia_journal_use']


def _sub(title, **kw):
    return {'_id': title.lower(), 'title': title, **kw}


def test_latest_apc_takes_the_most_recent_year():
    assert LATEST({'2024': 3000, '2026': 4370.0, '2025': '3990'}) == ('2026', 4370.0)


def test_latest_apc_tolerates_missing_and_bad_values():
    assert LATEST(None) == (None, None)
    assert LATEST({}) == (None, None)
    assert LATEST("{'2026': 1}") == (None, None)       # stringified, as some records are
    assert LATEST({'2026': 'n/a'}) == (None, None)


def test_journals_match_ignoring_case_and_sort_by_papers():
    subs = [_sub('Nature'), _sub('Cell'), _sub('Never Used')]
    use = USE(subs, {'nature': 5, 'Cell': 9, 'Not Subscribed': 40})
    assert [(e['sub']['title'], e['papers']) for e in use] == [('Cell', 9), ('Nature', 5)]


def test_spellings_of_one_title_add_up_and_the_most_used_one_is_linked():
    use = USE([_sub('eLife')], {'eLife': 30, 'ELIFE': 2, 'elife': 1})
    assert len(use) == 1
    assert use[0]['papers'] == 33 and use[0]['journal'] == 'eLife'


def test_a_duplicate_subscription_title_is_counted_once():
    use = USE([_sub('Science', provider='AAAS'), _sub('Science', provider='JSTOR')],
              {'Science': 7})
    assert len(use) == 1 and use[0]['sub']['provider'] == 'AAAS'


def test_empty_journal_names_are_ignored():
    assert not USE([_sub('Nature')], {None: 3, '': 2})


def test_ties_sort_by_title():
    use = USE([_sub('beta'), _sub('Alpha')], {'beta': 1, 'Alpha': 1})
    assert [e['sub']['title'] for e in use] == ['Alpha', 'beta']


def _view_source():
    view = next(n for n in TREE.body if getattr(n, 'name', '') == 'show_subscriptions')
    return ast.get_source_segment(SOURCE, view)


def test_the_title_table_reaches_the_page():
    ''' It used to be built and then overwritten. '''
    text = _view_source()
    assert 'all_table' in text.split('html = (stat_cards')[0]
    assert '+ tabs)' in text and 'tabs = all_table' in text


def test_both_tables_have_a_filter_box():
    text = _view_source()
    assert "_row_filter('journals'" in text and "_row_filter('janelia_journals'" in text


def test_the_publisher_column_links_to_that_publishers_list():
    text = _view_source()
    assert "/subscriptionlist/{quote(row['publisher'], safe='')}/publisher/" in text


def test_the_find_subscription_form_is_gone():
    ''' The filter boxes and publisher links replace it. '''
    template = (ROOT / 'api' / 'templates' / 'subscription.html').read_text(encoding='utf-8')
    assert 'Find subscription' not in template and '<datalist' not in template
    assert 'tableInitialize();' in template
