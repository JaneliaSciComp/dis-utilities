''' load_kbart.py: finding and repairing UTF-8 text that was misread as another encoding.

    The functions are extracted with ast, because importing load_kbart needs inquirer,
    jrc_common and a database configuration.
'''

import ast
import collections
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / 'journals' / 'bin' / 'load_kbart.py').read_text(encoding='utf-8')
TREE = ast.parse(SOURCE)

# Damaged values found in the subscription collection, with what they should say
DAMAGED = [
    ('Activation and Functionalization of C‚ÄîH Bonds',
     'Activation and Functionalization of C—H Bonds'),
    ('40 Years of Chemometrics ‚Äì From Bruce Kowalski to the Future',
     '40 Years of Chemometrics – From Bruce Kowalski to the Future'),
    ('Universit√§tsverlag WINTER Gmbh', 'Universitätsverlag WINTER Gmbh'),
    ('Staatliche Museen zu Berlin -- Preu√üischer Kulturbesitz',
     'Staatliche Museen zu Berlin -- Preußischer Kulturbesitz'),
    ('Afro-√Åsia', 'Afro-Ásia'),
    ('√Üther: A Journal of Strategic Airpower & Spacepower',
     'Æther: A Journal of Strategic Airpower & Spacepower'),
    ('Ã‰tudes', 'Études'),                            # Windows-1252, not seen yet
]
# Correct text that must be left alone
CORRECT = ['Nature', 'Études photographiques', 'Zeitschrift für Naturforschung',
           'C—H Activation', 'Revista de Ciências Sociais', 'Æther', '√2 and π', '',
           None, float('nan')]


class Stop(Exception):
    ''' terminate_program() exits; here it raises so the test can see it. '''


def _load(repair=False, write=False):
    ns = {'collections': collections,
          'ARG': SimpleNamespace(REPAIR_ENCODING=repair, WRITE=write),
          'LOGGER': SimpleNamespace(warning=lambda msg: None),
          'COUNT': collections.defaultdict(int)}

    def terminate_program(msg=None):
        raise Stop(msg)
    ns['terminate_program'] = terminate_program
    for node in TREE.body:
        targets = [t.id for t in getattr(node, 'targets', []) if isinstance(t, ast.Name)]
        if getattr(node, 'name', None) in ('repair_mojibake', 'check_encoding') \
           or targets in (['MISREAD_AS'], ['ENCODING_EXAMPLES']):
            exec(ast.get_source_segment(SOURCE, node), ns)    # pylint: disable=W0122
    return ns


NS = _load()


@pytest.mark.parametrize('bad, good', DAMAGED)
def test_damaged_text_is_repaired(bad, good):
    assert NS['repair_mojibake'](bad) == good


@pytest.mark.parametrize('text', CORRECT)
def test_correct_text_is_left_alone(text):
    assert NS['repair_mojibake'](text) is None


def _frame():
    return pd.DataFrame({'publication_title': [DAMAGED[0][0], 'Nature', DAMAGED[5][0]],
                         'publisher_name': ['Springer', DAMAGED[2][0], 'Springer']},
                        dtype=str)


def test_a_dry_run_reports_and_changes_nothing():
    ns = _load()
    pdf = _frame()
    assert ns['check_encoding'](pdf, 'KBART file') == 3
    assert pdf.at[0, 'publication_title'] == DAMAGED[0][0]
    assert ns['COUNT']['encoding'] == 3


def test_write_stops_before_loading_damaged_text():
    ns = _load(write=True)
    with pytest.raises(Stop, match='--repair-encoding'):
        ns['check_encoding'](_frame(), 'KBART file')


def test_repair_fixes_every_damaged_cell_and_nothing_else():
    ns = _load(repair=True, write=True)
    pdf = _frame()
    assert ns['check_encoding'](pdf, 'KBART file') == 3
    assert list(pdf['publication_title']) == [DAMAGED[0][1], 'Nature', DAMAGED[5][1]]
    assert list(pdf['publisher_name']) == ['Springer', DAMAGED[2][1], 'Springer']


def test_a_clean_file_passes_even_with_write():
    ns = _load(write=True)
    pdf = pd.DataFrame({'publication_title': CORRECT[:7]}, dtype=str)
    assert ns['check_encoding'](pdf, 'KBART file') == 0


def test_the_correlation_file_is_checked_too():
    ''' Its titles must match the repaired KBART titles for costs to attach. '''
    assert 'check_encoding(correlate, "Correlation file")' in SOURCE
