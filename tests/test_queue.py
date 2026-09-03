''' The dois_to_process queue: what gets enqueued, and when a row is dropped.

    A row used to be removed only when its DOI was persisted in that same run,
    so anything already stored stayed queued indefinitely and kept being
    reported to the librarian as newly added.
'''

import types

from helpers import FakeCollection


def test_ignored_dois_are_not_queued_and_are_marked_for_removal(ud, arg):
    ud.ARG = arg()
    ud.IGNORE = {'doi': {'10.1038/bad': True}}
    ud.DB = {'dis': types.SimpleNamespace(
        dois_to_process=FakeCollection([{'doi': '10.1038/bad'},
                                        {'doi': '10.1038/ok'}]))}
    ud.add_to_be_processed([])
    assert ud.TO_BE_PROCESSED == ['10.1038/ok']
    assert ud.IGNORED_TO_PROCESS == ['10.1038/bad']
    assert ud.QUEUED == ['10.1038/bad', '10.1038/ok']


def test_queue_tracking_is_independent_of_prior_discovery(ud, arg):
    ''' A queued DOI a sweep had already found never entered TO_BE_PROCESSED,
        so reconciling off that list left it queued forever.
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
    assert any('Would remove 1' in msg for _, msg in ud.LOGGER.messages)


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
