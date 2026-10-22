''' Year selections: "All", a single year, or "<year>-present".

    The value travels through 143 `year == 'All'` checks and 18 regex queries,
    so the new form is OPT-IN per page: year_pulldown(to_present=True) renders
    the second target, and a page that has not been taught to read "-present"
    never offers it.

    Spelled "-present" rather than "2024-" because both "^2024-" and "^2024+"
    are VALID regexes that quietly match a single year - an unconverted page
    would show one year's results under a button claiming a range. "-present"
    matches nothing anywhere it is not understood.
'''

import ast
import re
from html import escape
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / 'api' / 'dis_responder.py').read_text(encoding='utf-8')
HTML_SOURCE = (ROOT / 'api' / 'dis_html.py').read_text(encoding='utf-8')


def _ns():
    ns = {'re': re, 'YEAR_SELECTION': re.compile(r'^(?:All|(\d{4})(-present)?)$')}
    for name in ('parse_year', 'year_clause', 'year_includes', 'year_label'):
        node = next(n for n in ast.parse(SOURCE).body
                    if isinstance(n, ast.FunctionDef) and n.name == name)
        exec(ast.get_source_segment(SOURCE, node), ns)   # pylint: disable=W0122
    return ns


NS = _ns()


@pytest.mark.parametrize('value, kind, year', [
    ('All', 'all', None),
    ('2024', 'single', '2024'),
    ('2024-present', 'from', '2024'),
])
def test_the_three_shapes_parse(value, kind, year):
    assert NS['parse_year'](value) == (kind, year)


@pytest.mark.parametrize('junk', ['.*', '2024-', 'present', '', None, '20249',
                                  '2024-future', '../etc', '$ne'])
def test_anything_else_is_treated_as_all_rather_than_trusted(junk):
    ''' The old form interpolated the path segment into a regex, so
        /dois/mytags/.* matched every DOI in the collection.
    '''
    assert NS['parse_year'](junk) == ('all', None)
    assert NS['year_clause'](junk) is None


def test_a_range_is_a_date_comparison_not_a_regex():
    ''' jrc_publishing_date is an ISO string, so a lexicographic $gte is a date
        comparison - and unlike a regex it cannot be made to match everything.
    '''
    assert NS['year_clause']('2024-present') == {"$gte": "2024"}
    assert NS['year_clause']('2024') == {"$regex": "^2024"}


def test_the_query_and_the_python_filter_agree():
    ''' /dois/mytags filters in Mongo and /mypapers filters in Python. Two
        implementations of one rule is how the same selection comes to mean
        different things on two pages.
    '''
    dates = ['2019-05-01', '2023-12-31', '2024-01-01', '2024-07-15',
             '2025-02-02', '2026-10-01', '', None]
    for sel in ('All', '2024', '2024-present', '2026', '2019'):
        clause = NS['year_clause'](sel)
        for date in dates:
            text = str(date or '')
            if clause is None:
                mongo = True
            elif '$gte' in clause:
                mongo = text >= clause['$gte']
            else:
                mongo = bool(re.match(clause['$regex'], text))
            assert mongo == NS['year_includes'](sel, date), (sel, date)


def test_the_label_reads_as_english():
    assert NS['year_label']('2024-present') == '2024 to present'
    assert NS['year_label']('2024') == '2024'
    assert NS['year_label']('All') == 'All'


# --- the pulldown ----------------------------------------------------------

def _pulldown(**kwargs):
    from datetime import datetime
    ns = {'escape': escape, 'datetime': datetime}
    node = next(n for n in ast.parse(HTML_SOURCE).body
                if isinstance(n, ast.FunctionDef) and n.name == 'year_pulldown')
    exec(ast.get_source_segment(HTML_SOURCE, node), ns)   # pylint: disable=W0122
    return ns['year_pulldown']('mypapers/full', **kwargs)


def test_the_second_column_is_opt_in():
    ''' A page that cannot read "-present" must never offer it. '''
    assert 'yearlater' not in _pulldown(query=True)
    assert 'yearlater' in _pulldown(query=True, to_present=True)


def test_the_year_itself_stays_the_primary_target():
    ''' Clicking the year gives exactly today's behaviour; "to present" is the
        deliberate choice.
    '''
    out = _pulldown(query=True, to_present=True)
    assert "href='/mypapers/full?year=2026'>2026</a>" in out
    assert "?year=2026-present'>to present</a>" in out
    assert out.index("?year=2026'") < out.index("?year=2026-present'")


def test_all_years_keeps_one_target():
    ''' "All to present" would be nonsense. '''
    out = _pulldown(query=True, to_present=True)
    head = out[:out.index('yearrow')]
    assert '>All</a>' in head and 'yearlater' not in head


def test_both_columns_are_labelled():
    out = _pulldown(query=True, to_present=True)
    assert 'that year only' in out and 'that year to present' in out


def test_the_converted_pages_opt_in_and_the_rest_do_not():
    ''' /mypapers and /dois/mytags read the new form; nothing else offers it. '''
    offering = SOURCE.count('to_present=True')
    assert offering == 3, f"{offering} pulldowns offer it; expected 3"
    for fn in ('dois_mytags', 'my_papers_body'):
        node = next(n for n in ast.parse(SOURCE).body
                    if isinstance(n, ast.FunctionDef) and n.name == fn)
        body = '\n'.join(SOURCE.splitlines()[node.body[0].lineno - 1:node.end_lineno])
        assert 'to_present=True' in body, fn
        assert "year == 'All'" not in body and 'startswith(year' not in body, \
            f"{fn} still interprets the year itself"


def test_the_wrapper_accepts_everything_the_inner_pulldown_does():
    ''' dis_responder.year_pulldown SHADOWS dis_html.year_pulldown, adding the
        request coupling that keeps dis_html request-free. Callers reach the
        wrapper, so a parameter added to the inner one and not the wrapper is a
        TypeError at render time on whichever page uses it first - which is
        exactly what happened on /mypapers/full.
    '''
    def params(source, name):
        node = next(n for n in ast.parse(source).body
                    if isinstance(n, ast.FunctionDef) and n.name == name)
        return [a.arg for a in node.args.args]

    inner = params(HTML_SOURCE, 'year_pulldown')
    outer = params(SOURCE, 'year_pulldown')
    missing = [p for p in inner if p not in outer]
    assert not missing, f"the wrapper cannot pass on: {missing}"


def test_the_wrapper_passes_the_new_argument_on():
    ''' Accepting it and dropping it would be worse than rejecting it: the page
        would render the old control and silently ignore the request.
    '''
    node = next(n for n in ast.parse(SOURCE).body
                if isinstance(n, ast.FunctionDef) and n.name == 'year_pulldown')
    body = '\n'.join(SOURCE.splitlines()[node.body[0].lineno - 1:node.end_lineno])
    assert 'to_present=to_present' in body


def test_a_derived_year_is_labelled_for_display():
    ''' The wrapper reads the year from the URL, where it arrives raw. Without
        this the button reads "2024-present" instead of "2024 to present".
    '''
    node = next(n for n in ast.parse(SOURCE).body
                if isinstance(n, ast.FunctionDef) and n.name == 'year_pulldown')
    body = '\n'.join(SOURCE.splitlines()[node.body[0].lineno - 1:node.end_lineno])
    assert 'year_label(yr) if to_present else yr' in body


def test_an_unconverted_page_returns_nothing_rather_than_the_wrong_thing():
    ''' A hand-typed /tag/X/2024-present reaches a page that has not been
        taught the form. It must come back empty, not come back with a single
        year's results under a heading claiming a range.

        This is why the spelling is "-present": "^2024-" and "^2024+" are both
        valid regexes that match 2024 dates.
    '''
    for bad_spelling in ('2024-', '2024+'):
        assert re.match('^' + bad_spelling, '2024-05-01'), \
            f"{bad_spelling} would have matched silently"
    assert not re.match('^2024-present', '2024-05-01')
    assert not '2024-05-01'.startswith('2024-present')


def test_a_range_has_no_upper_bound():
    ''' One DOI is dated tomorrow - early-online papers carry a future
        publishing date. Capping at today would make "2026 to present" exclude
        a paper that "2026", the target immediately to its left in the same
        menu row, includes.
    '''
    assert NS['year_clause']('2026-present') == {"$gte": "2026"}
    assert NS['year_includes']('2026-present', '2027-01-01') is True


def test_the_empty_message_matches_the_kind_of_selection():
    ''' "nothing in that year" after choosing "2020 to present" reads as though
        the range was not honoured.
    '''
    node = next(n for n in ast.parse(SOURCE).body
                if isinstance(n, ast.FunctionDef) and n.name == 'my_papers_body')
    body = '\n'.join(SOURCE.splitlines()[node.body[0].lineno - 1:node.end_lineno])
    assert "'in that year' if parse_year(year)[0] == 'single'" in body
    assert "'in that period'" in body
