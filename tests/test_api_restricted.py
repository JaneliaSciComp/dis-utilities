''' api_restricted: the decorator that refuses a JSON caller.

    These RUN the decorator rather than checking it is present. Everything else
    pinning this mechanism asserts on source text, which cannot tell a working
    gate from one whose branches are the wrong way round.

    Extracted with ast and given stubs, because importing dis_responder needs
    Flask, a config file and a database, none of which the gate depends on.
'''

import ast
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / 'api' / 'dis_responder.py').read_text(encoding='utf-8')


class FakeInvalidUsage(Exception):
    ''' Stands in for the real one, which only formats a JSON envelope. '''
    def __init__(self, message, status_code=400):
        super().__init__(message)
        self.message = message
        self.status_code = status_code


def _gate(person, held):
    ''' api_restricted with current_user and authorizations stubbed '''
    ns = {'wraps': __import__('functools').wraps,
          'InvalidUsage': FakeInvalidUsage,
          'current_user': lambda: person,
          'authorizations': lambda _p: set(held)}
    node = next(n for n in ast.parse(SOURCE).body
                if isinstance(n, ast.FunctionDef) and n.name == 'api_restricted')
    exec(ast.get_source_segment(SOURCE, node), ns)    # pylint: disable=W0122
    return ns['api_restricted']


PERSON = {'orcid': '0000-0001-0000-0000'}


def _view(gate, *terms):
    @gate(*terms)
    def view(doi='x'):
        return f"ran:{doi}"
    return view


def test_the_right_authorization_is_let_through():
    view = _view(_gate(PERSON, {'orcid'}), 'orcid')
    assert view(doi='10.1/abc') == 'ran:10.1/abc'


def test_no_identity_is_401_not_403():
    ''' 401 means fix your token, 403 means ask for access. A script cannot act
        on them the same way, and conflating them sends people to the wrong
        person for help.
    '''
    view = _view(_gate(None, set()), 'orcid')
    with pytest.raises(FakeInvalidUsage) as err:
        view()
    assert err.value.status_code == 401
    assert 'token' in err.value.message.lower()


def test_an_identity_without_the_term_is_403():
    view = _view(_gate(PERSON, {'datacite'}), 'orcid')
    with pytest.raises(FakeInvalidUsage) as err:
        view()
    assert err.value.status_code == 403
    assert 'orcid' in err.value.message


def test_admin_admits_without_the_specific_term():
    ''' Consistent with authorizations() and @restricted: admin means
        everything, so an administrator never needs the term listed.
    '''
    view = _view(_gate(PERSON, {'admin'}), 'orcid')
    assert view() == 'ran:x'


def test_any_one_of_several_terms_admits():
    gate = _gate(PERSON, {'author'})
    assert _view(gate, 'orcid', 'author')() == 'ran:x'


def test_holding_an_unrelated_authorization_is_not_enough():
    ''' The failure this guards: treating "has a token" as "may do this". '''
    view = _view(_gate(PERSON, {'datacite', 'orcid'}), 'author')
    with pytest.raises(FakeInvalidUsage) as err:
        view()
    assert err.value.status_code == 403


def test_an_empty_authorization_set_is_refused():
    view = _view(_gate(PERSON, set()), 'orcid')
    with pytest.raises(FakeInvalidUsage) as err:
        view()
    assert err.value.status_code == 403


def test_arguments_reach_the_view_untouched():
    ''' A decorator that drops kwargs breaks every parameterised route it
        guards, and only on the paths nobody tested.
    '''
    gate = _gate(PERSON, {'admin'})

    @gate('orcid')
    def view(a, b=2, *, c=3):
        return (a, b, c)

    assert view(1, b=20, c=30) == (1, 20, 30)


def test_the_view_keeps_its_identity():
    ''' Flask registers views by __name__; losing it collides endpoints. '''
    gate = _gate(PERSON, {'admin'})

    @gate('orcid')
    def distinctive_name():
        return 'ok'

    assert distinctive_name.__name__ == 'distinctive_name'


def test_the_gate_records_its_terms_for_the_report():
    ''' /restrictions reads these off the view; without them a gated endpoint
        is invisible to the report that exists to list it.
    '''
    gate = _gate(PERSON, {'admin'})

    @gate('orcid', 'author')
    def view():
        return 'ok'

    assert view.dis_api_restricted is True
    assert view.dis_api_terms == ('orcid', 'author')


# --- CORS -------------------------------------------------------------------

def test_cors_is_not_open_to_every_origin():
    ''' It was CORS(app, supports_credentials=True) with no origins argument,
        which tells a browser that ANY site may make credentialed requests to
        every route. Harmless while a session carried nothing; not harmless once
        sign-in became real.
    '''
    assert 'CORS(app, supports_credentials=True)' not in SOURCE, \
        "credentialed CORS must name its origins"
    assert 'CORS(app, origins=CORS_ORIGINS, supports_credentials=True)' in SOURCE


def test_the_allowlist_has_no_wildcard():
    block = SOURCE[SOURCE.index('CORS_ORIGINS = ['):]
    block = block[:block.index('CORS(app')]
    assert "'*'" not in block and '"*"' not in block


def test_the_allowlist_is_configurable_without_a_rebuild():
    ''' config.cfg ships inside the image, so changing an origin there means a
        new build. An environment variable does not.
    '''
    block = SOURCE[SOURCE.index('CORS_ORIGINS = ['):]
    block = block[:block.index('CORS(app')]
    assert "os.environ.get('DIS_CORS_ORIGINS'" in block


def test_the_default_allowlist_parses_to_real_origins():
    import os as _os
    ns = {'os': _os}
    start = SOURCE.index('CORS_ORIGINS = [')
    exec(SOURCE[start:SOURCE.index('CORS(app', start)], ns)  # pylint: disable=W0122
    origins = ns['CORS_ORIGINS']
    assert origins, "an empty allowlist would block every cross-origin call"
    assert all(o.startswith(('http://', 'https://')) for o in origins), origins
    assert all(not o.endswith('/') for o in origins), "trailing slash never matches"
    assert 'https://dis.int.janelia.org' in origins
