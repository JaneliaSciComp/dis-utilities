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

import helpers

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
    ''' Import `name`, stubbing it if it is missing or fails to import.

        The catch is deliberately broad. A dependency can be installed yet
        unusable - MySQLdb whose _mysql extension is not linked raises
        NameError, not ImportError - and update_dois only needs the name to
        resolve, so a stub serves these tests either way.
    '''
    try:
        return importlib.import_module(name)
    except Exception:                                  # pylint: disable=broad-except
        sys.modules.pop(name, None)                    # drop a half-built module
        return _stub(name, **attrs)


def _prepare_imports():
    ''' Satisfy update_dois' imports without requiring the full runtime. '''
    for name in ('MySQLdb', 'pymysql', 'pyalex', 'xmltodict', 'inquirer', 'bs4'):
        _ensure(name)
    _ensure('unidecode', unidecode=lambda s: s)
    _ensure('tqdm', tqdm=lambda x, **kw: x)
    for pkg, sub in (('jrc_common', 'jrc_common'), ('jrc_email', 'jrc_email'),
                     ('doi_common', 'doi_common')):
        dotted = f'{pkg}.{sub}'
        try:
            importlib.import_module(dotted)
        except Exception:                              # pylint: disable=broad-except
            sys.modules.pop(dotted, None)
            _stub(pkg)                                 # parent must resolve too
            # doi_common only needs the one function the registrar routing uses.
            attrs = {'is_datacite': _fallback_is_datacite} if pkg == 'doi_common' else {}
            _stub(dotted, **attrs)


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
    # Compile from source rather than importing, so a cached .pyc can never be
    # used. Python invalidates bytecode on (mtime, size), and an edit that keeps
    # the size and lands in the same second - '2006' -> '2007', say - slips
    # through, silently testing the previous version of the file.
    module = types.ModuleType('update_dois')
    module.__file__ = str(MODULE_PATH)
    source = MODULE_PATH.read_text(encoding='utf-8')
    exec(compile(source, str(MODULE_PATH), 'exec'), module.__dict__)  # pylint: disable=exec-used
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


# ---------------------------------------------------------------------------
# Builders that wire the loaded module for a scenario. These are fixtures
# rather than plain functions because each needs the per-test `ud` module.
# ---------------------------------------------------------------------------

@pytest.fixture(name='sweeps')
def fixture_sweeps(ud):
    ''' Replace every outward call get_dois_for_dis() makes, recording them.
        Returns the list the calls are appended to.
    '''
    calls = []
    ud.get_dois_from_crossref = lambda flt='janelia': calls.append(f'crossref:{flt}') or []
    ud.get_dois_from_datacite = lambda query: calls.append(f'datacite:{query}') or []
    ud.call_responder = lambda *a, **k: calls.append('flycore') or {'dois': []}
    ud.add_alps_releases = lambda dlist: calls.append('alps')
    ud.add_to_be_processed = lambda dlist: calls.append('to_process')
    return calls


@pytest.fixture(name='fetches')
def fixture_fetches(ud, arg):
    ''' Run the loop over the given input, returning the DOIs the fetcher was
        actually asked for. Used to assert on normalisation and skipping.
    '''
    def run(incoming, **argkw):
        fetched = []
        ud.ARG = arg(**argkw)
        ud.IGNORE = {'doi': {}}
        ud.EXISTING = {}
        ud.get_dois = lambda: {'dois': list(incoming)}
        ud.get_doi_record = lambda doi: fetched.append(doi) or None
        ud.persist_if_updated = lambda *a: None
        ud.update_dois = lambda *a: None
        ud.process_dois()
        return fetched
    return run


@pytest.fixture(name='routing')
def fixture_routing(ud, arg):
    ''' Drive process_dois over one known and one new DOI per registrar,
        reporting which DOIs reached the insert and update paths.
    '''
    def run(mode, registrar='both'):
        ud.ARG = arg(MODE=mode, REGISTRAR=registrar)
        ud.IGNORE = {'doi': {}}
        ud.EXISTING = {'10.1038/known': {'jrc_obtained_from': 'Crossref'},
                       '10.5281/zenodo.known': {'jrc_obtained_from': 'DataCite'}}
        ud.get_dois = lambda: {'dois': ['10.1038/known', '10.1038/new',
                                        '10.5281/zenodo.known', '10.5281/zenodo.new']}
        ud.get_doi_record = lambda doi: ({'data': {'attributes': {'updated': 'x'}}}
                                         if 'zenodo' in doi
                                         else {'message': {'deposited': {'date-time': 'x'}}})
        ud.too_old = lambda doi, msg: False
        seen = {'inserted': [], 'updated': []}
        ud.persist_if_updated = lambda doi, msg, persist: seen['updated'].append(doi)
        ud.update_dois = lambda spec, persist: seen['inserted'].extend(sorted(persist))
        ud.process_dois()
        return seen
    return run


@pytest.fixture(name='pipeline')
def fixture_pipeline(ud, arg):
    ''' Wire the real process_dois -> update_mongodb path, replacing only the
        database and (optionally) the record fetcher. Returns the fake
        collection so a test can assert on what was written.
    '''
    def build(existing, incoming, deposited=helpers.DEPOSITED_NEWER,
              fetch=True, **argkw):
        coll = helpers.FakeCollection()
        ud.ARG = arg(**argkw)
        ud.IGNORE = {'doi': {}}
        ud.EXISTING = dict(existing)
        ud.DB = {'dis': types.SimpleNamespace(dois=coll,
                                              processing=helpers.FakeCollection())}
        ud.get_dois = lambda: {'dois': list(incoming)}
        if fetch:
            ud.get_doi_record = lambda doi: helpers.crossref_msg(deposited)
        # Enrichment that reaches beyond what these tests assert on.
        ud.add_first_last_authors = lambda val: None
        ud.add_openalex = lambda val: None
        ud.add_datacite = lambda val: None
        ud.add_tags_and_authors = lambda persist: None
        ud.DL = types.SimpleNamespace(
            is_datacite=lambda doi: False,
            get_publishing_date=lambda rec: '2024-05-01',
            get_journal=lambda rec, name_only=False: 'Test Journal',
            get_doi_record=lambda doi, coll=None: None,
            add_doi_process=helpers.fake_add_doi_process)
        return coll
    return build


@pytest.fixture(name='lookup')
def fixture_lookup(ud, pipeline):
    ''' As `pipeline`, but keeps the real record-lookup chain and fakes the
        registrar call itself, so not-found accounting is genuinely exercised.
        `found` maps DOI -> record; anything absent is unknown to the registrar.
    '''
    def build(existing, incoming, found, **argkw):
        coll = pipeline(existing, incoming, fetch=False, **argkw)
        ud.JRC = types.SimpleNamespace(call_crossref=lambda doi: found.get(doi),
                                       call_datacite=lambda doi: found.get(doi),
                                       get_user_name=lambda: 'tester')
        return coll
    return build


@pytest.fixture(name='datacite_lookup')
def fixture_datacite_lookup(ud, arg):
    ''' Real lookup and persist path for one DataCite DOI, with only the
        registrar call, the database and the enrichment helpers replaced.
    '''
    def build(record):
        coll = helpers.FakeCollection()
        ud.ARG = arg()
        ud.IGNORE = {'doi': {}}
        ud.EXISTING = {}
        ud.DB = {'dis': types.SimpleNamespace(dois=coll,
                                              processing=helpers.FakeCollection())}
        ud.DL = types.SimpleNamespace(
            is_datacite=lambda doi: True,
            get_publishing_date=lambda rec: '2024-05-01',
            get_journal=lambda rec, name_only=False: None,
            get_doi_record=lambda doi, coll=None: None,
            add_doi_process=helpers.fake_add_doi_process)
        ud.JRC = types.SimpleNamespace(call_datacite=lambda doi: record,
                                       call_crossref=lambda doi: record,
                                       get_user_name=lambda: 'tester')
        ud.add_first_last_authors = lambda val: None
        ud.add_openalex = lambda val: None
        ud.add_datacite = lambda val: None
        ud.add_tags_and_authors = lambda persist: None
        ud.get_dois = lambda: {'dois': ['10.5281/zenodo.1']}
        return coll
    return build
