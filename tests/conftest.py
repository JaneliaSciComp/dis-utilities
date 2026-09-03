''' Shared fixtures for the sync-program tests.

    update_dois.py imports a stack of third-party and in-house modules that a
    given checkout may not have installed. Rather than mandate the full runtime,
    stub only what is genuinely missing, so the real doi_common is used whenever
    it is available - its is_datacite() drives the registrar routing under test.
'''

import importlib
import importlib.util
import sys
import types
from pathlib import Path

import pytest

MODULE_PATH = Path(__file__).resolve().parents[1] / 'sync' / 'bin' / 'update_dois.py'

# Minimal stand-in for doi_common.is_datacite, used only when the real library
# is unavailable. Kept deliberately small: the DataCite prefixes these tests use.
_DC_HINTS = ('/janelia', '/arxiv', '/zenodo', 'figshare', '/dryad', '/d1.')


def _fallback_is_datacite(doi):
    # Deliberately no broader than the real doi_common.is_datacite: a fallback
    # that accepted more would let a stubbed run pass where the library fails.
    low = str(doi).lower()
    return any(h in low for h in _DC_HINTS)


def _stub(name, **attrs):
    ''' Register a stub module under `name` (creating parent packages). '''
    mod = types.ModuleType(name)
    for key, val in attrs.items():
        setattr(mod, key, val)
    sys.modules[name] = mod
    if '.' in name:
        parent, _, child = name.rpartition('.')
        if parent in sys.modules:
            setattr(sys.modules[parent], child, mod)
    return mod


def _ensure(name, **attrs):
    ''' Import `name`, stubbing it only if it is not installed. '''
    try:
        return importlib.import_module(name)
    except ImportError:
        return _stub(name, **attrs)


def _prepare_imports():
    ''' Satisfy update_dois' imports without requiring the full runtime. '''
    for name in ('MySQLdb', 'pymysql', 'pyalex', 'xmltodict', 'inquirer', 'bs4'):
        _ensure(name)
    _ensure('unidecode', unidecode=lambda s: s)
    if 'tqdm' not in sys.modules:
        try:
            importlib.import_module('tqdm')
        except ImportError:
            _stub('tqdm', tqdm=lambda x, **kw: x)
    for pkg, sub in (('jrc_common', 'jrc_common'), ('jrc_email', 'jrc_email'),
                     ('doi_common', 'doi_common')):
        try:
            importlib.import_module(f'{pkg}.{sub}')
        except ImportError:
            _stub(pkg)
            attrs = {'is_datacite': _fallback_is_datacite} if pkg == 'doi_common' else {}
            _stub(f'{pkg}.{sub}', **attrs)


_prepare_imports()


class NullLogger:
    ''' Logger that records messages instead of emitting them. '''

    def __init__(self):
        self.messages = []

    def _record(self, level):
        def log(msg, *args):
            self.messages.append((level, str(msg)))
        return log

    def __getattr__(self, level):
        return self._record(level)


@pytest.fixture(name='ud')
def fixture_ud():
    ''' update_dois imported fresh, so the module-level counters and queues
        (COUNT, QUEUED, TO_BE_PROCESSED, ...) start clean for every test.
        Throttling is disabled and sleep calls are recorded instead.
    '''
    spec = importlib.util.spec_from_file_location('update_dois', MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.LOGGER = NullLogger()
    module.sleeps = []
    module.sleep = module.sleeps.append      # record instead of waiting
    return module


@pytest.fixture(name='arg')
def fixture_arg():
    ''' Build an ARG namespace with the defaults argparse would supply. '''
    def build(**overrides):
        opts = {'DOI': None, 'FILE': None, 'PIPE': False, 'TARGET': 'dis',
                'MODE': 'both', 'REGISTRAR': 'both', 'SOURCE': None,
                'MANIFOLD': 'prod', 'WRITE': False, 'FORCE': False,
                'OUTPUT': False, 'INSERT': False}
        opts.update(overrides)
        return types.SimpleNamespace(**opts)
    return build
