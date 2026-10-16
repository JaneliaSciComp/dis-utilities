''' usable_name: turning an ORCID name block into a roster lookup key.

    apply_orcids matches a person in the orcid collection by {given, family},
    so an ORCID record that fills in only some of its name fields used to be
    skipped outright. ORCID carries three: given-names, family-name, and
    credit-name, which it defines as the name to use when publishing.

    The function is extracted with ast rather than imported, because importing
    apply_orcids reads a config and opens a database connection, neither of
    which this logic depends on.
'''

import ast
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / 'sync' / 'bin' / 'apply_orcids.py').read_text(encoding='utf-8')


def _extract(name):
    node = next(n for n in ast.parse(SOURCE).body
                if isinstance(n, ast.FunctionDef) and n.name == name)
    ns = {}
    exec(ast.get_source_segment(SOURCE, node), ns)    # pylint: disable=W0122
    return ns[name]


usable_name = _extract('usable_name')


def _blk(given=None, family=None, credit=None):
    ''' An ORCID person.name block, with absent fields as None like the API '''
    return {'given-names': {'value': given} if given else None,
            'family-name': {'value': family} if family else None,
            'credit-name': {'value': credit} if credit else None}


@pytest.mark.parametrize('block, expected, why', [
    (_blk('Ann', 'Lee'), ('Ann', 'Lee'), 'both structured fields present'),
    (_blk('Ann', 'Lee', 'Zed Zed'), ('Ann', 'Lee'),
     'structured fields win over credit-name'),
    (_blk(None, None, 'Hyung-song Nam'), ('Hyung-song', 'Nam'),
     'credit-name, last token is the family name'),
    (_blk(None, None, 'Nam, Hyung-Song'), ('Hyung-Song', 'Nam'),
     'credit-name inverted, as ORCID allows'),
    (_blk(None, None, '  Ann   Lee  '), ('Ann', 'Lee'),
     'surrounding whitespace is not part of the name'),
])
def test_a_name_is_recovered(block, expected, why):
    assert usable_name(block) == expected, why


@pytest.mark.parametrize('block, why', [
    (None, 'name withheld entirely - private, and nothing here will fix it'),
    (_blk('Joseph', None), 'given name alone cannot key a {given, family} lookup'),
    (_blk(None, None, 'Prince'), 'one token gives no split point'),
    (_blk(None, None, '   '), 'credit-name present but empty'),
])
def test_no_name_is_invented(block, why):
    assert usable_name(block) == (None, None), why


def test_the_korean_case_that_prompted_this():
    ''' 0000-0002-0429-5446 as the live API returns it: a public record whose
        given-names is Hangul, family-name empty, publishing name in credit-name.
        Insisting on given+family dropped a person whose name was never private.
    '''
    block = {'given-names': {'value': '남형송'},
             'family-name': None,
             'credit-name': {'value': 'Hyung-song Nam'}}
    assert usable_name(block) == ('Hyung-song', 'Nam')
