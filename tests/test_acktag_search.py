''' Search acknowledgements and tag the matches in bulk (/acktag_search,
    /acktag_bulk).

    The worklist behind the Curate tab's one-at-a-time tagging. Every DOI goes
    through the same _ack_tag_change the tab uses, so the rules and the audit
    are shared; what is tested here is the roll-up, the form, the query, and
    the table's checkbox column. Functions are extracted from dis_responder
    with ast (importing it needs Flask, a config file and a database).
'''

import ast
import re
from datetime import datetime
from html import escape
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / 'api' / 'dis_responder.py').read_text(encoding='utf-8')
JS = (ROOT / 'api' / 'static' / 'js' / 'dis.js').read_text(encoding='utf-8')
HTML_MOD = (ROOT / 'api' / 'dis_html.py').read_text(encoding='utf-8')


def _block(start, end):
    seg = SOURCE[SOURCE.index(start):]
    return seg[:seg.index(end)]


SEARCH = _block('def show_acktag_search(', 'def acktag_bulk(')
BULK = _block('def acktag_bulk(', '# Evidence that an author on the paper')
TABLE = _block('def standard_ack_table(', 'def ack_stat_cards(')
_TREE = ast.parse(SOURCE)
_FUNCS = {n.name: n for n in _TREE.body if isinstance(n, ast.FunctionDef)}
_ASSIGNS = {getattr(n.targets[0], 'id', None): n for n in _TREE.body if isinstance(n, ast.Assign)}


class _Collection:
    def __init__(self):
        self.calls = []

    def update_one(self, query, update, upsert=False):
        self.calls.append((query, update, upsert))


def _ns():
    dois, processing = _Collection(), _Collection()
    ns = {'datetime': datetime, 're': re, 'escape': escape, '__version__': '1.2.3',
          'IRIS_CURATOR': 'IRIS', 'ACKTAG_SEARCH_LIMIT': 500, 'ACKTAG_TERM_MAX': 200,
          'DB': {'dis': type('D', (), {'dois': dois, 'processing': processing})()},
          'print': lambda *a, **k: None}
    exec(ast.get_source_segment(SOURCE, _ASSIGNS['RISKY_REGEX']), ns)    # pylint: disable=W0122
    for name in ('record_doi_event', '_ack_tag_change', '_bulk_ack_tags', '_acktag_search_form',
                 '_regex_problem'):
        exec(ast.get_source_segment(SOURCE, _FUNCS[name]), ns)    # pylint: disable=W0122
    ns['_dois'], ns['_processing'] = dois, processing
    return ns


ME = {'orcid': '0000-0003-0000-0001', 'userIdO365': 'Tagger@hhmi.org'}
VOCAB = {'FlyLight': r'fly\s*light', 'Vivarium': r'vivarium'}
RECORDS = {
    '10.1/a': {'doi': '10.1/a', 'jrc_acknowledgements': 'We thank FlyLight.'},
    '10.1/b': {'doi': '10.1/b', 'jrc_acknowledgements': 'FlyLight again.',
               'jrc_acknowledge': [{'name': 'FlyLight', 'curator': 'IRIS'}]},
    '10.1/c': {'doi': '10.1/c'},
}


# --- the door ----------------------------------------------------------------

def test_both_routes_share_the_curate_door():
    for route in ("@app.route('/acktag_search')", "@app.route('/acktag_bulk', methods=['POST'])"):
        head = SOURCE[SOURCE.index(route):]
        head = head[:head.index('def ')]
        assert '@restricted(CURATE_TERMS)' in head, route
    # The menu entry is gated by the same mark, so a reader without either
    # term never sees it; the route table, not a hand-kept list, decides.
    assert '"Search and tag acknowledgements": "acktag_search"' in HTML_MOD


# --- the roll-up -------------------------------------------------------------

def test_bulk_tagging_goes_through_the_same_change_as_the_curate_tab():
    ''' One write and one processing event per DOI tagged; a refusal is
        reported with the Curate tab's own words rather than hidden.
    '''
    ns = _ns()
    tagged, skipped, unattempted = ns['_bulk_ack_tags'](
        ['10.1/a', '10.1/b', '10.1/c', '10.1/zz'], 'FlyLight', ME, VOCAB, {}, RECORDS.get)
    assert tagged == ['10.1/a'] and unattempted == 0
    assert skipped == [('10.1/b', '"FlyLight" is already an acknowledgement tag on this DOI.'),
                       ('10.1/c', 'This DOI has no acknowledgement text, so there is nothing '
                                  'for a tag to describe. Add the text first.'),
                       ('10.1/zz', 'not in the database')]
    (query, update, _), = ns['_dois'].calls
    assert query == {'doi': '10.1/a'}
    entry = update['$push']['jrc_acknowledge']
    assert entry['name'] == 'FlyLight' and entry['curator'] == 'Tagger@hhmi.org'
    events = [c[1]['$push']['processes'] for c in ns['_processing'].calls]
    assert [(e['action'], e['user']) for e in events] == [('acktag_added', 'tagger')]


def test_bulk_input_is_normalised_deduplicated_and_capped():
    ns = _ns()
    seen = []
    def fetch(doi):
        seen.append(doi)
        return RECORDS.get(doi)
    tagged, skipped, unattempted = ns['_bulk_ack_tags']([' 10.1/A ', '10.1/a', '', None, '10.1/a'],
                                                        'FlyLight', ME, VOCAB, {}, fetch)
    assert seen == ['10.1/a'], 'lowercased, stripped, each DOI once'
    assert tagged == ['10.1/a'] and skipped == [] and unattempted == 0
    # Past the cap: the rest are counted, not silently dropped.
    ns = _ns()
    many = [f'10.1/{i}' for i in range(600)]
    tagged, skipped, unattempted = ns['_bulk_ack_tags'](many, 'FlyLight', ME, VOCAB, {},
                                                        lambda d: None)
    assert len(skipped) == 500 and unattempted == 100
    assert "not attempted" in BULK and "{unattempted:,}" in BULK


def test_a_database_error_is_not_reported_as_a_missing_doi():
    def fetch(doi):
        if doi == '10.1/boom':
            raise RuntimeError('mongo is down')
        return RECORDS.get(doi)
    ns = _ns()
    tagged, skipped, _ = ns['_bulk_ack_tags'](['10.1/boom', '10.1/zz', '10.1/a'], 'FlyLight',
                                              ME, VOCAB, {}, fetch)
    assert tagged == ['10.1/a']
    assert skipped == [('10.1/boom', 'could not be read from the database'),
                       ('10.1/zz', 'not in the database')]


def test_an_unknown_entity_is_refused_per_doi_with_no_write():
    ns = _ns()
    tagged, skipped, _ = ns['_bulk_ack_tags'](['10.1/a'], 'Imaginary Lab', ME, VOCAB, {},
                                              RECORDS.get)
    assert tagged == [] and 'not an entity' in skipped[0][1]
    assert ns['_dois'].calls == [] and ns['_processing'].calls == []


def test_an_add_is_refused_when_the_suporg_list_could_not_be_read():
    ''' An empty dict would type every entity "acknowledgement" and the write
        would look normal; None is the "could not read" signal and refuses.
    '''
    ns = _ns()
    message, severity = ns['_ack_tag_change'](RECORDS['10.1/a'], ME, 'acktag_add', 'FlyLight',
                                              VOCAB, None)
    assert severity == 'error' and 'supervisory-organization list could not be read' in message
    assert ns['_dois'].calls == [] and ns['_processing'].calls == []
    # Both callers take their suporgs from the None-on-failure loader.
    assert "suporgs = _suporgs_or_none() if action == 'acktag_add' else {}" in SOURCE
    assert "_suporgs_or_none()," in BULK
    loader = ast.get_source_segment(SOURCE, _FUNCS['_suporgs_or_none'])
    assert "return None" in loader and "or {}" in loader


def test_catastrophic_patterns_are_refused_before_they_run():
    problem = _ns()['_regex_problem']
    assert problem('fly light') is None
    assert problem(r'fly\s*light|vivarium') is None
    assert problem(r'(?i)janelia') is None
    assert problem('(') and 'not a valid pattern' in problem('(')
    for bad in (r'(a+)+', r'(\w+\s?)*Janelia', r'(a*)*', r'(ab|a)*c'):
        assert problem(bad) and 'repeats a repeated group' in problem(bad), bad
    # Refused one way or the other: Python's re rejects these outright on
    # some versions (possessive quantifiers, multiple repeat) and the
    # heuristic catches them on the versions that accept them.
    for bad in ('a++', r'(x|y)+{2,}'):
        assert problem(bad), bad


def test_the_single_doi_note_is_the_change_rendered():
    ''' _save_ack_tag kept its contract for the Curate tab: the same (message,
        severity) the bulk page reads, passed through render_warning.
    '''
    body = ast.get_source_segment(SOURCE, _FUNCS['_save_ack_tag'])
    assert "message, severity = _ack_tag_change(row, me, action, name, vocabulary, suporgs)" in body
    assert "return render_warning(message, severity)" in body


# --- the search page ---------------------------------------------------------

def test_the_form_echoes_the_search_and_requires_an_entity():
    ''' Without an entity the page would be /acksui with a smaller scope and a
        gate in front, so the select is required and its placeholder cannot be
        submitted; a plain search is pointed at /acksui.
    '''
    ns = _ns()
    html = ns['_acktag_search_form']("fly <light>", 'Vivarium', VOCAB)
    assert "value='fly &lt;light&gt;'" in html
    assert "<select class='form-control' name='tag' required>" in html
    assert "<option value='' disabled>Entity to tag with</option>" in html
    assert "<option value='FlyLight'>FlyLight</option>" in html
    assert "<option value='Vivarium' selected>Vivarium</option>" in html
    assert "method='GET' action='/acktag_search'" in html
    assert "maxlength='200'" in html
    assert "Any tagging" not in html
    # No entity yet: the placeholder is the selected (blank) option.
    assert "<option value='' disabled selected>Entity to tag with</option>" in \
        ns['_acktag_search_form']('', '', VOCAB)


def test_a_term_without_an_entity_is_refused_and_sent_to_acksui():
    assert "if not tag:" in SEARCH
    assert 'render_warning("Choose the entity to tag with.", \'warning\')' in SEARCH
    # Outside render_warning, which escapes its message: a link inside it
    # would reach the reader as angle brackets.
    assert "<a href='/acksui/{escape(quote(term, safe=''))}'>" in SEARCH
    assert "render_warning(safe(" not in SEARCH
    # The refusal comes before the query, so nothing is searched for it.
    assert SEARCH.index("if not tag:") < SEARCH.index(".limit(ACKTAG_SEARCH_LIMIT + 1)")


def test_the_query_is_a_case_insensitive_regex_minus_the_chosen_tag():
    assert "problem = _regex_problem(term)" in SEARCH
    assert ('payload = {"jrc_acknowledgements": {"$regex": term, "$options": "i"},\n'
            '               "jrc_acknowledge.name": {"$ne": tag}}') in SEARCH
    assert "if tag not in vocabulary:" in SEARCH
    # dois only: the Curate tab and _ack_tag_change write dois, and the page
    # should not list what the button cannot tag.
    assert "DB['dis'].dois.find(payload)" in SEARCH and 'external_dois' not in SEARCH
    # One scan, not two: limit+1 says "there are more" without a count.
    assert ".limit(ACKTAG_SEARCH_LIMIT + 1)" in SEARCH
    assert "more = len(rows) > ACKTAG_SEARCH_LIMIT" in SEARCH
    assert "count_documents" not in SEARCH and "Narrow the search" in SEARCH


def test_every_listed_row_is_selectable():
    ''' With the entity required, every row the page lists is one the button
        can tag, so the checkbox column is unconditional.
    '''
    assert "select=('doi', 'acktag-bulk'))" in SEARCH and "if tag else None" not in SEARCH
    assert "action='/acktag_bulk' id='acktag-bulk'" in SEARCH
    assert "data-selected-count='dois'" in SEARCH


def test_the_bulk_post_renders_its_report_and_links_back_to_the_search():
    ''' Not a redirect with the report in the session: the session is a
        cookie, a report of hundreds of skipped rows overflows it, and the
        browser dropping the cookie would also drop the sign-in. Re-posting
        is harmless (duplicates are refused), so the POST renders the page.
    '''
    assert 'session[' not in BULK and 'session.pop' not in SEARCH
    assert "url_for('show_acktag_search', term=term, tag=tag)" in BULK
    assert "Back to the search</a>" in BULK
    assert "dois = request.form.getlist('doi')" in BULK
    assert "No DOIs were selected." in BULK
    assert "table_id='acktag-skipped'" in BULK
    # The vocabulary is checked here too, not only in the per-DOI change: a
    # hand-made POST with an unknown tag gets one note, not 500 identical rows.
    assert "elif tag not in vocabulary:" in BULK
    # A DOI in the skipped table came from the form, so it is escaped before
    # doi_link, which does not escape.
    assert "safe(doi_link(escape(doi)))" in BULK


def test_acknowledgement_text_is_escaped_before_it_becomes_markup():
    ''' Pre-existing on every ack page: the text went into the table as-is.
        quote=False so a term with ' or " still matches what Mongo matched.
    '''
    assert ("row['jrc_ack2'] = escape(row['jrc_acknowledgements'], quote=False)"
            ".replace('\\n', '<br>')") in TABLE


# --- the table's checkbox column ---------------------------------------------

def test_the_checkbox_column_binds_to_the_form_without_nesting_the_table_in_it():
    ''' The table's own filter buttons would submit a form they sat inside, so
        each box names the form with form= instead.
    '''
    assert "form='{escape(select[1])}'" in TABLE
    assert "name='{escape(select[0])}'" in TABLE and "value='{escape(row['doi'])}'" in TABLE
    assert "<th class='sorter-false'>" in TABLE
    assert "selectVisibleRows('dois', this, event)" in TABLE and "countSelected('dois')" in TABLE
    # Not in the download: header (used for the TSV) is unchanged by select.
    assert "''.join([f\"<th>{itm}</th>\" for itm in header])" in TABLE


def test_select_all_takes_only_the_rows_the_filters_show():
    assert "function selectVisibleRows(tid, box, event)" in JS
    assert "> tbody > tr:visible input[type=checkbox]').prop('checked', box.checked)" in JS
    assert "function countSelected(tid)" in JS
    assert "[data-selected-count=\"' + tid + '\"]" in JS


def test_the_header_box_can_clear_as_well_as_select():
    ''' The bundled tablesorter (2.0) sorts on any header click and returns
        false from its handler, which also cancels the checkbox's toggle: the
        box could never be seen checked, so a second click selected all
        again. The click is stopped at the box; the box then mirrors the rows
        (checked / indeterminate / clear), and a "clear" link stands beside
        the count for anyone who does not think to click the header twice.
    '''
    assert "if (event) { event.stopPropagation(); }" in JS
    assert "this.indeterminate = ticked > 0 && ticked < visible.length;" in JS
    assert "this.checked = visible.length > 0 && ticked === visible.length;" in JS
    assert "function clearSelected(tid)" in JS
    assert "clearSelected('dois')" in SEARCH and ">clear</a>" in SEARCH


def test_the_help_and_the_metrics_gap_callout_point_here():
    assert "<a href='/acktag_search'>search the acknowledgements</a>" in SOURCE
    assert "<a href='/acktag_search'>search and tag</a>" in SOURCE
