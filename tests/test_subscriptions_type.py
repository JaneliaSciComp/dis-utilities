''' /subscriptions/type/<jtype>: the helpers behind the "Janelia use" tab, the APC
    summary and the row toggles.

    Extracted with ast, because importing dis_responder needs Flask, a config file and a
    database.
'''

import ast
from html import escape
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / 'api' / 'dis_responder.py').read_text(encoding='utf-8')
TREE = ast.parse(SOURCE)


def _namespace():
    ns = {'escape': escape}
    for node in TREE.body:
        if getattr(node, 'name', None) in ('_latest_apc', '_janelia_journal_use',
                                           '_apc_summary', '_row_toggle'):
            exec(ast.get_source_segment(SOURCE, node), ns)    # pylint: disable=W0122
    return ns


NS = _namespace()
LATEST, USE = NS['_latest_apc'], NS['_janelia_journal_use']
APC, TOGGLE = NS['_apc_summary'], NS['_row_toggle']


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


def test_acknowledgements_are_matched_like_janelia_papers():
    subs = [_sub('Nature'), _sub('Cell'), _sub('Neuron')]
    use = USE(subs, {'Nature': 5, 'cell': 2}, {'NATURE': 3, 'Neuron': 4, 'Elsewhere': 9})
    got = {e['sub']['title']: (e['papers'], e['acks']) for e in use}
    assert got == {'Nature': (5, 3), 'Cell': (2, 0), 'Neuron': (0, 4)}


def test_a_journal_with_only_acknowledgements_is_listed_after_the_published_ones():
    use = USE([_sub('Nature'), _sub('Neuron'), _sub('Cell')],
              {'Nature': 1}, {'Neuron': 9, 'Cell': 2})
    assert [e['sub']['title'] for e in use] == ['Nature', 'Neuron', 'Cell']
    neuron = use[1]
    assert neuron['journal'] is None and neuron['ack_journal'] == 'Neuron'


def test_the_most_used_acknowledgement_spelling_is_linked():
    use = USE([_sub('PLOS ONE')], {}, {'PLOS ONE': 13, 'PLoS ONE': 11})
    assert use[0]['acks'] == 24 and use[0]['ack_journal'] == 'PLOS ONE'


def test_acknowledgements_are_optional():
    assert USE([_sub('Nature')], {'Nature': 1})[0]['acks'] == 0


def test_apc_summary_uses_the_latest_year_only():
    subs = [_sub('A', apc={'2026': 3000}), _sub('B', apc={'2026': 5000}),
            _sub('C', apc={'2026': 4000}), _sub('D', apc={'2024': 99999}), _sub('E')]
    assert APC(subs) == ('2026', 3, 4000, 3000, 5000)


def test_apc_summary_median_of_an_even_count():
    subs = [_sub('A', apc={'2026': 1000}), _sub('B', apc={'2026': 2000})]
    assert APC(subs)[2] == 1500


def test_apc_summary_with_no_apcs():
    assert APC([_sub('A'), _sub('B', apc={})]) is None


def test_the_row_toggle_starts_by_offering_to_hide():
    html = TOGGLE('janelia_journals', 'no-apc', 12, 'journals without an APC')
    assert ">Hide journals without an APC (12)</button>" in html
    assert "toggleClass('janelia_journals', 'no-apc', this, " in html
    assert "'janelia_journals_count')" in html


def test_the_view_marks_rows_for_both_toggles():
    view = next(n for n in TREE.body if getattr(n, 'name', '') == 'show_subscriptions')
    text = ast.get_source_segment(SOURCE, view)
    assert "tclasses.append('ended' if row.get('last_issue') else '')" in text
    assert "uclasses.append('no-apc' if amount is None else '')" in text
    assert "row_classes=tclasses" in text and "row_classes=uclasses" in text


def test_the_janelia_use_links_match_names_in_any_case():
    ''' The counts add case variants together, so the lists they link to must too. '''
    view = next(n for n in TREE.body if getattr(n, 'name', '') == 'show_subscriptions')
    text = ast.get_source_segment(SOURCE, view)
    assert "safe='')}?any_case=1'>" in text
    assert text.count("?any_case=1'>") == 2          # the two count links
    journal = next(n for n in TREE.body if getattr(n, 'name', '') == 'show_journal_ui')
    assert "_name_any_case(jname) if request.args.get('any_case')" in \
           ast.get_source_segment(SOURCE, journal)
