''' Fetching a DOI's record: what happens when the registrar has nothing, or
    returns something unusable. Each outcome is counted separately so a run's
    summary distinguishes "never existed" from "arrived but unusable".

    These drive the real call_crossref -> DOINotFound -> get_doi_record chain,
    faking only the outermost HTTP call.
'''

import pytest

from helpers import DEPOSITED_NEWER, crossref_record


def test_phony_doi_is_counted_as_not_found(ud, lookup):
    lookup(existing={}, incoming=['10.9999/not.a.real.doi'], found={})
    ud.process_dois()
    assert ud.COUNT['notfound'] == 1
    assert 'Could not find 10.9999/not.a.real.doi in Crossref' in ud.MISSING


def test_phony_doi_is_neither_inserted_nor_updated(ud, lookup):
    coll = lookup(existing={}, incoming=['10.9999/not.a.real.doi'],
                  found={}, WRITE=True)
    ud.process_dois()
    assert (ud.COUNT['insert'], ud.COUNT['update']) == (0, 0)
    assert ud.INSERTED == {}
    assert coll.updates == [], "a DOI the registrar does not have must not be written"


def test_phony_doi_is_logged_rather_than_raised(ud, lookup):
    lookup(existing={}, incoming=['10.9999/not.a.real.doi'], found={})
    ud.process_dois()          # must not raise
    assert any('Could not find 10.9999/not.a.real.doi' in msg
               for _, msg in ud.LOGGER.messages)


def test_one_phony_doi_does_not_stop_the_run(ud, lookup):
    ''' A missing DOI is skipped, not fatal - the rest of the batch still loads. '''
    coll = lookup(existing={}, found={'10.1038/real': crossref_record()},
                  incoming=['10.9999/phony', '10.1038/real', '10.9998/alsophony'],
                  WRITE=True)
    ud.process_dois()
    assert ud.COUNT['notfound'] == 2
    assert ud.COUNT['insert'] == 1
    assert [w['doi'] for w in coll.updates] == ['10.1038/real']


def test_phony_datacite_doi_is_counted_as_not_found(ud, lookup):
    lookup(existing={}, incoming=['10.5281/zenodo.99999999'], found={})
    ud.DL.is_datacite = lambda doi: True
    ud.process_dois()
    assert ud.COUNT['notfound'] == 1
    assert 'Could not find 10.5281/zenodo.99999999 in DataCite' in ud.MISSING


def test_crossref_record_without_an_author_is_skipped(ud, lookup):
    ''' A record with no author, editor or investigator is not loaded; it is
        counted separately from a DOI the registrar never had.
    '''
    authorless = {'message': {'deposited': {'date-time': DEPOSITED_NEWER},
                              'title': ['Has a title but no author']}}
    coll = lookup(existing={}, incoming=['10.1038/authorless'],
                  found={'10.1038/authorless': authorless}, WRITE=True)
    ud.process_dois()
    assert ud.COUNT['noauthor'] == 1
    assert ud.COUNT['notfound'] == 0        # it was found, just unusable
    assert ud.COUNT['insert'] == 0
    assert coll.updates == []


def test_crossref_record_without_a_title_is_skipped(ud, lookup):
    ''' Distinct from the no-author case, and distinct again from not-found. '''
    untitled = {'message': {'deposited': {'date-time': DEPOSITED_NEWER},
                            'author': [{'family': 'Doe', 'given': 'J'}]}}
    coll = lookup(existing={}, incoming=['10.1038/untitled'],
                  found={'10.1038/untitled': untitled}, WRITE=True)
    ud.process_dois()
    assert ud.COUNT['insert'] == 0
    assert ud.COUNT['notfound'] == 0
    assert 'No title for 10.1038/untitled' in ud.MISSING
    assert coll.updates == []


def test_empty_registrar_response_counts_as_not_found(ud, datacite_lookup):
    ''' An empty response is falsy, so call_datacite() treats it as a DOI the
        registrar does not have - distinct from a response that is present but
        malformed.
    '''
    datacite_lookup({})
    ud.process_dois()
    assert ud.COUNT['notfound'] == 1
    assert ud.COUNT['malformed'] == 0


@pytest.mark.parametrize('record', [{'data': {}}, {'data': None},
                                    {'data': 'not-a-dict'}])
def test_malformed_datacite_record_is_skipped_not_fatal(ud, datacite_lookup, record):
    ''' A truncated registrar response is skipped and counted, the same way a
        not-found DOI is, instead of raising out of the run.
    '''
    datacite_lookup(record)
    ud.process_dois()                     # must not raise
    assert ud.COUNT['malformed'] == 1
    assert ud.COUNT['insert'] == 0
    assert 'No attributes for 10.5281/zenodo.1' in ud.MISSING


def test_well_formed_datacite_record_still_loads(ud, datacite_lookup):
    ''' The guard must not reject a good record. '''
    datacite_lookup({'data': {'attributes': {'updated': '2024-01-01T00:00:00Z'}}})
    ud.process_dois()
    assert ud.COUNT['malformed'] == 0
    assert ud.COUNT['insert'] == 1
