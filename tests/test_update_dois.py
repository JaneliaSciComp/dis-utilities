''' Tests for sync/bin/update_dois.py - the only program that inserts or updates
    DOIs, so its routing and bookkeeping are worth locking down.

    These are unit tests: the database and the Crossref/DataCite fetchers are
    replaced with fakes. Every case here corresponds to real behavior of the
    program, and several are regression tests for defects found in production.
'''

import types

import pytest


# --------------------------------------------------------------------------
# Fakes
# --------------------------------------------------------------------------

class FakeCollection:
    ''' Records the queries and writes it is asked to perform. '''

    def __init__(self, docs=None, deleted=1):
        self.docs = list(docs or [])
        self.deleted = deleted
        self.deletes = []
        self.finds = []

    def find(self, *_args, **_kwargs):
        return list(self.docs)

    def find_one(self, query, *_args, **_kwargs):
        self.finds.append(query)
        key = query.get('doi') or query.get('key')
        for doc in self.docs:
            if doc.get('doi') == key or doc.get('key') == key:
                return doc
        return None

    def delete_one(self, query):
        self.deletes.append(query.get('doi') or query.get('key'))
        return types.SimpleNamespace(deleted_count=self.deleted)


def crossref_msg(date='2024-01-01'):
    return {'message': {'deposited': {'date-time': f'{date}T00:00:00Z'}}}


def datacite_msg(date='2024-01-01'):
    return {'data': {'attributes': {'updated': f'{date}T00:00:00Z'}}}


def stub_fetchers(ud, calls):
    ''' Replace every outward call get_dois_for_dis() makes. '''
    ud.get_dois_from_crossref = lambda flt='janelia': calls.append(f'crossref:{flt}') or []
    ud.get_dois_from_datacite = lambda query: calls.append(f'datacite:{query}') or []
    ud.call_responder = lambda *a, **k: calls.append('flycore') or {'dois': []}
    ud.add_alps_releases = lambda dlist: calls.append('alps')
    ud.add_to_be_processed = lambda dlist: calls.append('to_process')


# --------------------------------------------------------------------------
# Pure helpers
# --------------------------------------------------------------------------

def test_split_raw_doi_splits_on_pipe(ud):
    assert ud.split_raw_doi('10.1/a | 10.2/b') == ['10.1/a', '10.2/b']
    assert ud.split_raw_doi('10.1/a') == ['10.1/a']


# --------------------------------------------------------------------------
# --registrar filtering
# --------------------------------------------------------------------------

def test_registrar_both_accepts_everything(ud, arg):
    ud.ARG = arg(REGISTRAR='both')
    ud.EXISTING = {}
    assert ud.wanted_registrar('10.1038/anything')
    assert ud.wanted_registrar('10.25378/janelia.1')


def test_registrar_uses_stored_value_over_heuristic(ud, arg):
    ''' jrc_obtained_from is authoritative for a DOI we already hold; the
        is_datacite() prefix heuristic is only a fallback.
    '''
    ud.ARG = arg(REGISTRAR='crossref')
    ud.EXISTING = {'10.25378/janelia.1': {'jrc_obtained_from': 'Crossref'}}
    assert ud.wanted_registrar('10.25378/janelia.1')      # stored value wins
    ud.ARG = arg(REGISTRAR='datacite')
    assert not ud.wanted_registrar('10.25378/janelia.1')


def test_registrar_falls_back_to_heuristic_when_unseen(ud, arg):
    ud.ARG = arg(REGISTRAR='datacite')
    ud.EXISTING = {}
    assert ud.wanted_registrar('10.25378/janelia.9')      # unseen -> heuristic
    ud.ARG = arg(REGISTRAR='crossref')
    assert ud.wanted_registrar('10.1038/unseen')


# --------------------------------------------------------------------------
# --mode update: population comes from what we already hold
# --------------------------------------------------------------------------

def test_update_mode_skips_all_discovery(ud, arg):
    ''' Regression: update mode must not run the Crossref/DataCite/FLYF2 sweeps
        just to discard everything they find.
    '''
    calls = []
    stub_fetchers(ud, calls)
    ud.ARG = arg(MODE='update')
    ud.EXISTING = {'10.1038/a': {'jrc_obtained_from': 'Crossref'},
                   '10.5281/zenodo.b': {'jrc_obtained_from': 'DataCite'}}
    result = ud.get_dois()
    assert sorted(result['dois']) == ['10.1038/a', '10.5281/zenodo.b']
    assert calls == [], f"update mode made discovery calls: {calls}"


def test_update_mode_honors_registrar(ud, arg):
    calls = []
    stub_fetchers(ud, calls)
    ud.ARG = arg(MODE='update', REGISTRAR='crossref')
    ud.EXISTING = {'10.1038/a': {'jrc_obtained_from': 'Crossref'},
                   '10.5281/zenodo.b': {'jrc_obtained_from': 'DataCite'}}
    assert ud.get_dois()['dois'] == ['10.1038/a']


@pytest.mark.parametrize('registrar,expected', [
    ('both', ['crossref:janelia', 'crossref:ror',
              'datacite:janelia', 'datacite:affiliation']),
    ('crossref', ['crossref:janelia', 'crossref:ror']),
    ('datacite', ['datacite:janelia', 'datacite:affiliation']),
])
def test_registrar_skips_unneeded_sweeps(ud, arg, registrar, expected):
    calls = []
    stub_fetchers(ud, calls)
    ud.ARG = arg(MODE='insert', REGISTRAR=registrar)
    ud.EXISTING = {}
    ud.IGNORE = {'doi': {}, 'em_dataset': {}}
    ud.JRC = types.SimpleNamespace(get_config=lambda k: {},
                                   simplenamespace_to_dict=lambda x: {})
    ud.get_dois_for_dis({'dois': []})
    assert [c for c in calls if ':' in c] == expected


# --------------------------------------------------------------------------
# process_dois routing
# --------------------------------------------------------------------------

def run_loop(ud, arg, mode, registrar='both'):
    ''' Drive process_dois over one known and one new DOI per registrar. '''
    ud.ARG = arg(MODE=mode, REGISTRAR=registrar)
    ud.IGNORE = {'doi': {}}
    ud.EXISTING = {'10.1038/known': {'jrc_obtained_from': 'Crossref'},
                   '10.5281/zenodo.known': {'jrc_obtained_from': 'DataCite'}}
    ud.get_dois = lambda: {'dois': ['10.1038/known', '10.1038/new',
                                    '10.5281/zenodo.known', '10.5281/zenodo.new']}
    ud.get_doi_record = lambda doi: (datacite_msg() if 'zenodo' in doi
                                     else crossref_msg())
    ud.too_old = lambda doi, msg: False
    seen = {'inserted': [], 'updated': []}
    ud.persist_if_updated = lambda doi, msg, persist: seen['updated'].append(doi)
    ud.update_dois = lambda spec, persist: seen['inserted'].extend(sorted(persist))
    ud.process_dois()
    return seen


def test_insert_mode_only_touches_new_dois(ud, arg):
    seen = run_loop(ud, arg, 'insert')
    assert seen['inserted'] == ['10.1038/new', '10.5281/zenodo.new']
    assert seen['updated'] == []


def test_update_mode_only_touches_known_dois(ud, arg):
    seen = run_loop(ud, arg, 'update')
    assert seen['updated'] == ['10.1038/known', '10.5281/zenodo.known']
    assert ud.COUNT['skipped_new'] == 2


def test_both_mode_touches_everything(ud, arg):
    seen = run_loop(ud, arg, 'both')
    assert len(seen['updated']) == 4


def test_registrar_filter_applies_in_the_loop(ud, arg):
    seen = run_loop(ud, arg, 'both', registrar='crossref')
    assert seen['updated'] == ['10.1038/known', '10.1038/new']
    assert ud.COUNT['skipped_registrar'] == 2


def test_ignored_dois_never_reach_the_api(ud, arg):
    ud.ARG = arg(MODE='both')
    ud.IGNORE = {'doi': {'10.1038/bad': True}}
    ud.EXISTING = {}
    fetched = []
    ud.get_dois = lambda: {'dois': ['10.1038/bad', '10.1038/ok']}
    ud.get_doi_record = lambda doi: fetched.append(doi) or crossref_msg()
    ud.persist_if_updated = lambda *a: None
    ud.update_dois = lambda *a: None
    ud.process_dois()
    assert fetched == ['10.1038/ok']
    assert ud.COUNT['skipped'] == 1


def test_throttle_tracks_api_calls_not_input_size(ud, arg):
    ''' Regression: the sleep used to hang off the --insert flag, so insert runs
        fetched unthrottled while other runs slept on DOIs they then skipped.
    '''
    run_loop(ud, arg, 'insert')
    assert len(ud.sleeps) == 2, "one sleep per fetched DOI, not per input DOI"


def test_insert_mode_counts_records_it_fetched(ud, arg):
    ''' Regression: foundc/foundd stayed 0 on the insert path, which bypasses
        persist_if_updated where they are normally incremented.
    '''
    run_loop(ud, arg, 'insert')
    assert ud.COUNT['foundc'] == 1
    assert ud.COUNT['foundd'] == 1


# --------------------------------------------------------------------------
# dois_to_process reconciliation
# --------------------------------------------------------------------------

def test_ignored_dois_are_not_queued_and_are_marked_for_removal(ud, arg):
    ud.ARG = arg()
    ud.IGNORE = {'doi': {'10.1038/bad': True}}
    ud.DB = {'dis': types.SimpleNamespace(
        dois_to_process=FakeCollection([{'doi': '10.1038/bad'},
                                        {'doi': '10.1038/ok'}]))}
    dlist = []
    ud.add_to_be_processed(dlist)
    assert ud.TO_BE_PROCESSED == ['10.1038/ok']
    assert ud.IGNORED_TO_PROCESS == ['10.1038/bad']
    assert ud.QUEUED == ['10.1038/bad', '10.1038/ok']


def test_queue_tracking_is_independent_of_prior_discovery(ud, arg):
    ''' Regression: a queued DOI a sweep had already found never entered
        TO_BE_PROCESSED, so reconciling off that list left it queued forever.
    '''
    ud.ARG = arg()
    ud.IGNORE = {'doi': {}}
    ud.DB = {'dis': types.SimpleNamespace(
        dois_to_process=FakeCollection([{'doi': '10.1038/seen'},
                                        {'doi': '10.1038/fresh'}]))}
    ud.add_to_be_processed(['10.1038/seen'])          # already discovered
    assert ud.TO_BE_PROCESSED == ['10.1038/fresh']
    assert ud.QUEUED == ['10.1038/seen', '10.1038/fresh']


def test_stored_dois_are_dequeued_even_if_not_persisted_this_run(ud, arg):
    ''' Regression: rows were only dropped when the DOI was persisted in the
        same run, so an already-stored DOI stayed queued indefinitely.
    '''
    queue = FakeCollection()
    ud.ARG = arg(WRITE=True)
    ud.DB = {'dis': types.SimpleNamespace(dois_to_process=queue)}
    ud.QUEUED.extend(['10.1038/stored', '10.1038/pending'])
    ud.EXISTING = {'10.1038/stored': {}}
    ud.INSERTED = {}
    ud.reconcile_to_process()
    assert queue.deletes == ['10.1038/stored']
    assert ud.COUNT['dequeued'] == 1


def test_reconcile_writes_nothing_without_write(ud, arg):
    queue = FakeCollection()
    ud.ARG = arg(WRITE=False)
    ud.DB = {'dis': types.SimpleNamespace(dois_to_process=queue)}
    ud.QUEUED.append('10.1038/stored')
    ud.EXISTING = {'10.1038/stored': {}}
    ud.INSERTED = {}
    ud.reconcile_to_process()
    assert queue.deletes == []
    assert any('Would remove 1' in m for _, m in ud.LOGGER.messages)


def test_reconcile_purges_ignored_rows(ud, arg):
    queue = FakeCollection()
    ud.ARG = arg(WRITE=True)
    ud.DB = {'dis': types.SimpleNamespace(dois_to_process=queue)}
    ud.IGNORED_TO_PROCESS.append('10.1038/bad')
    ud.QUEUED.append('10.1038/bad')
    ud.EXISTING = {}
    ud.INSERTED = {}
    ud.reconcile_to_process()
    assert queue.deletes == ['10.1038/bad']


# --------------------------------------------------------------------------
# needs-update decisions
# --------------------------------------------------------------------------

def test_unseen_doi_always_needs_update(ud, arg):
    ud.ARG = arg()
    ud.EXISTING = {}
    assert ud.crossref_needs_update('10.1038/new', crossref_msg()['message'])
    assert ud.datacite_needs_update('10.5281/zenodo.new', datacite_msg()['data'])


def test_unchanged_timestamp_needs_no_update(ud, arg):
    ud.ARG = arg()
    ud.EXISTING = {'10.1038/a': {'deposited': {'date-time': '2024-01-01T00:00:00Z'}}}
    assert not ud.crossref_needs_update('10.1038/a', crossref_msg('2024-01-01')['message'])
    assert ud.COUNT['noupdate'] == 1


def test_changed_timestamp_needs_update(ud, arg):
    ud.ARG = arg()
    ud.EXISTING = {'10.1038/a': {'deposited': {'date-time': '2024-01-01T00:00:00Z'}}}
    assert ud.crossref_needs_update('10.1038/a', crossref_msg('2025-06-01')['message'])


def test_force_overrides_an_unchanged_timestamp(ud, arg):
    ud.ARG = arg(FORCE=True)
    ud.EXISTING = {'10.1038/a': {'deposited': {'date-time': '2024-01-01T00:00:00Z'}}}
    assert ud.crossref_needs_update('10.1038/a', crossref_msg('2024-01-01')['message'])
