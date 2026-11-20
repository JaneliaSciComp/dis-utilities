''' Optional table columns the reader can hide.

    A table may carry columns some readers want and others find noise. The
    control is a checkbox per optional column; hiding is by a class stamped on
    the column's cells (render_table's col_classes), not by position, so adding
    or reordering a column can never make a checkbox hide the wrong one. The
    choice lives in the reader's browser; the download, built on the server,
    always carries every column.

    The inserted-DOIs page is the first user. Its row builder is extracted
    from dis_responder with ast rather than imported (importing it needs
    Flask, a config file and a database) and run against the real dis_html
    helpers, so the page tests assert what a row renders, not what the
    source text says.
'''

import ast
import re
from datetime import date
from pathlib import Path

import dis_html as DH

ROOT = Path(__file__).resolve().parents[1]
RESPONDER = (ROOT / 'api' / 'dis_responder.py').read_text(encoding='utf-8')
JS = (ROOT / 'api' / 'static' / 'js' / 'dis.js').read_text(encoding='utf-8')
CSS = (ROOT / 'api' / 'static' / 'css' / 'main.css').read_text(encoding='utf-8')


def _block(start, end):
    seg = RESPONDER[RESPONDER.index(start):]
    return seg[:seg.index(end)]


INSERT = _block('def show_insert(', "@app.route('/doiui/custom/")


def _extract(*names):
    ''' Pull named top-level functions and assignments out of dis_responder. '''
    tree = ast.parse(RESPONDER)
    ns = {'safe': DH.safe, 'doi_link': DH.doi_link, 'cell': DH.cell, 'yes_no': DH.yes_no}
    for name in names:
        node = next(n for n in tree.body
                    if (isinstance(n, ast.FunctionDef) and n.name == name)
                    or (isinstance(n, ast.Assign)
                        and getattr(n.targets[0], 'id', None) == name))
        exec(ast.get_source_segment(RESPONDER, node), ns)      # pylint: disable=W0122
    return ns


NS = _extract('INSERT_COLUMNS', '_doi_list', '_insert_row')
COLUMNS = NS['INSERT_COLUMNS']
insert_row = NS['_insert_row']
LIMIT = date(2026, 11, 6)


def _row(**over):
    base = {'doi': '10.1/abc', 'type': 'journal-article', 'publisher': 'Elsevier',
            'jrc_publishing_date': '2026-11-10', 'jrc_inserted': '2026-11-19 11:45:14',
            'jrc_obtained_from': 'Crossref'}
    base.update(over)
    return base


# --- render_table col_classes ----------------------------------------------

def test_a_column_class_lands_on_the_header_and_every_cell():
    html = DH.render_table(['A', 'B', 'C'], [[1, 2, 3], [4, 5, 6]],
                           col_classes=[None, 'col-b', None])
    assert "<th class='col-b'>B</th>" in html
    assert html.count("<td class='col-b'>") == 2
    assert "<td class='col-b'>2</td>" in html and "<td class='col-b'>5</td>" in html
    # The other columns are untouched.
    assert "<th>A</th>" in html and "<td>1</td>" in html and "<td>6</td>" in html


def test_a_prerendered_cell_gets_the_class_too():
    ''' cell() returns a finished <td>; the class is spliced into its tag so a
        sortable numeric column can still be optional.
    '''
    html = DH.render_table(['A', 'N'], [[1, DH.cell('1,234', sort=1234)]],
                           col_classes=[None, 'col-n'])
    assert re.search(r"<td class='col-n' data-sort=\"1234\">1,234</td>", html)


def test_footer_cells_get_the_class_so_hiding_a_column_keeps_totals_aligned():
    ''' Without it the tfoot keeps its full cell count when a column is hidden
        and every total right of it sits under the wrong header.
    '''
    html = DH.render_table(['A', 'N', 'M'], [[1, 2, 3]],
                           footer=['Total', DH.fcell(2, align='right'), DH.fcell(3, header=False)],
                           col_classes=[None, 'col-n', 'col-m'])
    foot = html[html.index('<tfoot>'):]
    assert "<th>Total</th>" in foot
    assert "<th class='col-n' style='text-align: right;'>2</th>" in foot
    assert "<td class='col-m'>3</td>" in foot


def test_omitting_col_classes_changes_nothing():
    before = DH.render_table(['A', 'B'], [[1, 2]], footer=['x', DH.fcell('y')])
    assert before == DH.render_table(['A', 'B'], [[1, 2]], footer=['x', DH.fcell('y')],
                                     col_classes=None)
    assert before == DH.render_table(['A', 'B'], [[1, 2]], footer=['x', DH.fcell('y')],
                                     col_classes=[])
    assert "class=" not in before.split('<thead>')[1]


def test_a_short_col_classes_list_covers_the_leading_columns_only():
    html = DH.render_table(['A', 'B', 'C'], [[1, 2, 3]], col_classes=['col-a'])
    assert "<th class='col-a'>A</th>" in html
    assert "<th>B</th>" in html and "<th>C</th>" in html


# --- the checkbox row --------------------------------------------------------

def test_column_toggles_emit_one_checked_box_per_optional_column():
    html = DH.column_toggles('dois', [('Is version of', 'col-version'), ('Tags', 'col-tags')])
    assert html.count('<input type=\'checkbox\' checked') == 2
    assert "data-col-toggle='dois' data-col-class='col-version'" in html
    assert "data-col-toggle='dois' data-col-class='col-tags'" in html
    assert '> Is version of</label>' in html and '> Tags</label>' in html


def test_the_handler_reads_its_data_attributes_not_literals():
    ''' One copy of table id, class and store, in data-* attributes; the
        onchange has nothing to quote for a JavaScript string.
    '''
    html = DH.column_toggles("it's", [("x", "col-x")])
    assert html.count("onchange='toggleColumn(this)'") == 1
    assert "toggleColumn('" not in html
    assert "data-col-toggle='it&#x27;s'" in html
    assert "function toggleColumn(box)" in JS
    assert "box.dataset.colToggle" in JS and "box.dataset.colClass" in JS


def test_choices_are_stored_under_the_table_id_unless_the_page_names_a_store():
    ''' Several pages render a table called 'dois'. A store name keeps one
        page's choices from being applied to another's table of the same id.
    '''
    assert "data-col-store='t'" in DH.column_toggles('t', [('A', 'col-a')])
    assert "data-col-store='my-page'" in DH.column_toggles('t', [('A', 'col-a')], store='my-page')
    assert "const store = box.dataset.colStore || tid;" in JS
    assert "localStorage.setItem('cols:' + store" in JS
    assert "columnStore(box.dataset.colStore || tid)" in JS


def test_a_column_can_start_hidden():
    ''' A third tuple element, shown=False: the box renders unchecked and a
        stylesheet rule hides the column before the script runs, so it never
        flashes into view. The reader's own choice, once made, wins.
    '''
    html = DH.column_toggles('dois', [('Tags', 'col-tags'), ('Acks', 'col-acks', False)])
    assert html.startswith("<style>#dois .col-acks { display: none; }</style>")
    assert "<input type='checkbox' checked data-col-toggle='dois' data-col-class='col-tags'" in html
    assert "<input type='checkbox' data-col-toggle='dois' data-col-class='col-acks'" in html
    # No style block when everything starts shown.
    assert '<style>' not in DH.column_toggles('t', [('A', 'col-a')])


def test_the_script_keeps_choices_not_a_hidden_set():
    ''' A column the reader never touched must keep the server's default, so
        the store holds {class: shown} and the load hook falls back to the
        rendered checkbox state.
    '''
    assert "choices[cls] = box.checked;" in JS
    assert "const shown = (cls in choices) ? Boolean(choices[cls]) : box.checked;" in JS
    # Explicit display both ways, or the stylesheet rule for a default-hidden
    # column would win over an emptied inline style and the column never show.
    assert "el.style.display = shown ? 'table-cell' : 'none';" in JS


def test_column_toggle_labels_are_escaped():
    html = DH.column_toggles('t', [("<b>x</b> & 'y'", 'col-x')])
    assert '<b>x</b>' not in html
    assert '&lt;b&gt;x&lt;/b&gt; &amp; &#x27;y&#x27;' in html


# --- the script ------------------------------------------------------------

def test_the_script_hides_by_class():
    assert "querySelectorAll('.' + cls)" in JS
    assert "localStorage.getItem('cols:' + store" in JS


def test_storage_failures_fall_back_to_the_server_s_defaults():
    ''' Browser storage can be absent or throw (private window, blocked site
        data). Every read and write is wrapped, and the fallback is the server's
        own rendering: each column in its default state.
    '''
    store = JS[JS.index('function columnStore'):JS.index('// An explicit display')]
    assert 'try {' in store and 'return {};' in store
    toggle = JS[JS.index('function toggleColumn'):JS.index("document.addEventListener('DOMContentLoaded'")]
    assert 'try {' in toggle and 'catch (e)' in toggle


def test_remembered_choices_are_reapplied_on_load_and_the_boxes_synced():
    init = JS[JS.index("document.addEventListener('DOMContentLoaded'"):]
    init = init[:init.index('});', init.index('forEach')) + 3]
    assert "querySelectorAll('input[data-col-toggle]')" in init
    assert 'box.checked = shown;' in init
    assert 'applyColumn(tid, cls, shown);' in init


# --- the page: wiring ----------------------------------------------------------

def test_insert_page_makes_exactly_the_optional_columns_toggleable():
    ''' DOI, type, published, inserted and newsletter are required and get no
        control; the rest are optional. Publisher, Is version of and Tags start
        shown; Preprints, Acknowledgements and Janelia authors start hidden.
    '''
    assert COLUMNS == ['DOI', 'Type', 'Published', 'Publisher', 'Inserted', 'Is version of',
                       'Preprints', 'Newsletter', 'Tags', 'Acknowledgements', 'Janelia authors']
    classes = re.search(r"col_classes=\[(.*?)\]\)", INSERT, re.S).group(1)
    classes = [c.strip(" '\n") for c in classes.split(',')]
    assert dict(zip(COLUMNS, classes)) == {
        'DOI': 'None', 'Type': 'None', 'Published': 'None', 'Publisher': 'col-publisher',
        'Inserted': 'None', 'Is version of': 'col-version', 'Preprints': 'col-preprint',
        'Newsletter': 'None', 'Tags': 'col-tags', 'Acknowledgements': 'col-acks',
        'Janelia authors': 'col-author'}
    toggles = re.search(r"column_toggles\('dois', \[(.*?)\],", INSERT, re.S).group(1)
    assert re.findall(r"\('([^']+)', '([^']+)'(?:, (False))?\)", toggles) == [
        ('Publisher', 'col-publisher', ''),
        ('Is version of', 'col-version', ''), ('Preprints', 'col-preprint', 'False'),
        ('Tags', 'col-tags', ''), ('Acknowledgements', 'col-acks', 'False'),
        ('Janelia authors', 'col-author', 'False')]
    assert "store='doiui-insert'" in INSERT
    assert "render_table(INSERT_COLUMNS, trows" in INSERT
    assert "create_downloadable('jrc_inserted', INSERT_COLUMNS, fileoutput)" in INSERT


# --- the page: what a row renders ----------------------------------------------

def test_a_row_has_one_screen_cell_and_one_download_field_per_column():
    screen, download, _ = insert_row(_row(), 'Crossref', LIMIT)
    assert len(screen) == len(COLUMNS) == len(download)
    assert download[:5] == ['10.1/abc', 'journal-article', '2026-11-10', 'Elsevier',
                            '2026-11-19 11:45:14']
    assert '<a ' not in '\t'.join(download)


def test_the_download_has_a_header_matching_its_columns():
    ''' The header is the same list the table is built from, so the two can
        not drift; the Publisher column is in the file like every other.
    '''
    html = DH.render_table(COLUMNS, [insert_row(_row(), 'Crossref', LIMIT)[0]])
    assert len(re.findall(r'<th[ >]', html)) == len(COLUMNS)
    assert "<th>Publisher</th>" in html


def test_version_and_preprints_are_links_on_screen_and_bare_dois_in_the_file():
    row = _row(relation={'is-version-of': [{'id-type': 'doi', 'id': '10.2/v1'},
                                           {'id-type': 'doi', 'id': '10.2/v1'},
                                           {'id-type': 'url', 'id': 'http://x'}]},
               jrc_preprint=['10.1101/pp1', '10.1101/pp2'])
    screen, download, _ = insert_row(row, 'Crossref', LIMIT)
    version, preprints = screen[COLUMNS.index('Is version of')], screen[COLUMNS.index('Preprints')]
    assert version == "<a href='/doiui/10.2/v1'>10.2/v1</a>"
    assert preprints == ("<a href='/doiui/10.1101/pp1'>10.1101/pp1</a>, "
                         "<a href='/doiui/10.1101/pp2'>10.1101/pp2</a>")
    assert download[COLUMNS.index('Is version of')] == '10.2/v1'
    assert download[COLUMNS.index('Preprints')] == '10.1101/pp1, 10.1101/pp2'


def test_a_malformed_jrc_preprint_does_not_fail_the_page():
    ''' A lone string is one DOI; a non-string element is skipped. The old
        ', '.join() would have raised for the whole date range.
    '''
    assert NS['_doi_list']('10.1101/x') == ['10.1101/x']
    assert NS['_doi_list'](['10.1101/x', None, {'doi': 'y'}, '']) == ['10.1101/x']
    assert NS['_doi_list'](None) == []
    screen, download, _ = insert_row(_row(jrc_preprint=[None, '10.1101/x']), 'Crossref', LIMIT)
    assert download[COLUMNS.index('Preprints')] == '10.1101/x'
    assert '10.1101/x' in screen[COLUMNS.index('Preprints')]
    # And no preprint at all is an empty cell, not a crash.
    screen, download, _ = insert_row(_row(), 'Crossref', LIMIT)
    assert screen[COLUMNS.index('Preprints')] == '' and download[COLUMNS.index('Preprints')] == ''


def test_the_yes_no_columns_are_icons_on_screen_and_words_in_the_download():
    ''' A glyph sorts as nothing, so the cell carries a 1/0 data-sort; the
        download, which has no glyphs, gets yes/no.
    '''
    acks, auth = COLUMNS.index('Acknowledgements'), COLUMNS.index('Janelia authors')
    screen, download, _ = insert_row(_row(jrc_acknowledgements='We thank...',
                                          jrc_author=['12345']), 'Crossref', LIMIT)
    assert 'fa-circle-check' in screen[acks] and 'data-sort="1"' in screen[acks]
    assert 'fa-circle-check' in screen[auth] and 'data-sort="1"' in screen[auth]
    assert download[acks] == 'yes' and download[auth] == 'yes'
    screen, download, _ = insert_row(_row(jrc_author=[]), 'Crossref', LIMIT)
    assert 'fa-circle-xmark' in screen[acks] and 'data-sort="0"' in screen[acks]
    assert 'fa-circle-xmark' in screen[auth] and 'data-sort="0"' in screen[auth]
    assert download[acks] == 'no' and download[auth] == 'no'


def test_yes_no_is_the_site_s_check_and_a_red_x():
    yes = DH.yes_no(True, 'has it', 'lacks it')
    no = DH.yes_no(False, 'has it', 'lacks it')
    assert "fa-circle-check' style='color: lime' title='has it'" in yes
    assert "fa-circle-xmark' style='color: red' title='lacks it'" in no
    # Safe, so render_table does not escape the markup; the title is escaped.
    assert isinstance(yes, DH.Safe)
    assert "title='a &lt;b&gt;'" in DH.yes_no(True, 'a <b>')


def test_candidate_rows_are_recent_unreviewed_articles_or_preprints():
    ''' The row class drives the "Filter for candidate DOIs" button. '''
    assert insert_row(_row(), 'Crossref', LIMIT)[2] == 'candidate'
    assert insert_row(_row(jrc_newsletter='2026-11-12'), 'Crossref', LIMIT)[2] == 'other'
    assert insert_row(_row(jrc_publishing_date='2026-10-01'), 'Crossref', LIMIT)[2] == 'other'
    screen, _, _ = insert_row(_row(jrc_publishing_date='2026-10-01'), 'Crossref', LIMIT)
    assert "color: gray" in screen[COLUMNS.index('Published')]
    # DataCite datasets qualify when the page is the DataCite page.
    row = _row(type=None, types={'resourceTypeGeneral': 'Dataset'}, jrc_obtained_from='DataCite')
    assert insert_row(row, 'DataCite', LIMIT)[2] == 'candidate'
    assert insert_row(row, 'Crossref', LIMIT)[2] == 'other'


def test_a_record_without_a_publisher_renders():
    ''' Absent or null: an empty cell and an empty field, never a KeyError on
        screen or a None in the tab-join.
    '''
    col = COLUMNS.index('Publisher')
    row = _row()
    del row['publisher']
    for rec in (row, _row(publisher=None)):
        screen, download, _ = insert_row(rec, 'Crossref', LIMIT)
        assert screen[col] == '' and download[col] == ''
        assert '\t'.join(download)


def test_the_control_row_has_its_styles():
    for cls in ('.col-toggles', '.col-toggles-label', '.col-toggle'):
        assert cls in CSS
    # The muted label color is a token, defined once and used by reference.
    assert CSS.count('#a8c4e0') == 1 and '--muted-label:' in CSS
    assert CSS.count('var(--muted-label)') >= 4
