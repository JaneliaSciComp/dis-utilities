''' Deciding whether a DOI is inserted or updated, and whether anything is
    written at all. This is the question the program exists to get right, so
    these drive the real process_dois -> update_mongodb path with only the
    database and the record fetcher replaced.
'''

import types

import pytest

from helpers import DEPOSITED_STORED, DEPOSITED_NEWER, crossref_msg, datacite_msg


def stored(date=DEPOSITED_STORED):
    return {'10.1038/known': {'deposited': {'date-time': date}}}


# --- needs-update decisions ----------------------------------------------

def test_unseen_doi_always_needs_update(ud, arg):
    ud.ARG = arg()
    ud.EXISTING = {}
    assert ud.crossref_needs_update('10.1038/new', crossref_msg()['message'])
    assert ud.datacite_needs_update('10.5281/zenodo.new', datacite_msg()['data'])


def test_unchanged_timestamp_needs_no_update(ud, arg):
    ud.ARG = arg()
    ud.EXISTING = stored()
    assert not ud.crossref_needs_update('10.1038/known',
                                        crossref_msg(DEPOSITED_STORED)['message'])
    assert ud.COUNT['noupdate'] == 1


def test_changed_timestamp_needs_update(ud, arg):
    ud.ARG = arg()
    ud.EXISTING = stored()
    assert ud.crossref_needs_update('10.1038/known',
                                    crossref_msg(DEPOSITED_NEWER)['message'])


def test_force_overrides_an_unchanged_timestamp(ud, arg):
    ud.ARG = arg(FORCE=True)
    ud.EXISTING = stored()
    assert ud.crossref_needs_update('10.1038/known',
                                    crossref_msg(DEPOSITED_STORED)['message'])


# --- insert vs update -----------------------------------------------------

def test_doi_already_in_the_database_is_updated_not_inserted(ud, pipeline):
    pipeline(existing=stored(), incoming=['10.1038/known'])
    ud.process_dois()
    assert ud.COUNT['update'] == 1
    assert ud.COUNT['insert'] == 0
    assert '10.1038/known' not in ud.INSERTED


def test_doi_not_in_the_database_would_be_inserted(ud, pipeline):
    coll = pipeline(existing={}, incoming=['10.1038/brandnew'])
    ud.process_dois()
    assert ud.COUNT['insert'] == 1
    assert ud.COUNT['update'] == 0
    assert '10.1038/brandnew' in ud.INSERTED
    assert coll.updates == [], "a dry run must not write"


def test_unchanged_doi_is_neither_inserted_nor_updated(ud, pipeline):
    ''' Present with an identical deposited timestamp: nothing to do. '''
    coll = pipeline(existing=stored(), incoming=['10.1038/known'],
                    deposited=DEPOSITED_STORED)
    ud.process_dois()
    assert (ud.COUNT['update'], ud.COUNT['insert']) == (0, 0)
    assert ud.COUNT['noupdate'] == 1
    assert coll.updates == []


def test_force_persists_even_an_unchanged_doi(ud, pipeline):
    pipeline(existing=stored(), incoming=['10.1038/known'],
             deposited=DEPOSITED_STORED, FORCE=True)
    ud.process_dois()
    assert ud.COUNT['update'] == 1


# --- the --write gate -----------------------------------------------------

def test_dry_run_reports_but_writes_nothing(ud, pipeline):
    coll = pipeline(existing=stored(),
                    incoming=['10.1038/known', '10.1038/brandnew'], WRITE=False)
    ud.process_dois()
    assert (ud.COUNT['update'], ud.COUNT['insert']) == (1, 1)
    assert coll.updates == []


def test_write_run_persists_both_and_stamps_the_load_source(ud, pipeline):
    coll = pipeline(existing=stored(),
                    incoming=['10.1038/known', '10.1038/brandnew'], WRITE=True)
    ud.process_dois()
    assert {w['doi'] for w in coll.updates} == {'10.1038/known', '10.1038/brandnew'}
    assert all(w['upsert'] for w in coll.updates)
    for write in coll.updates:
        fields = write['update']['$set']
        assert fields['jrc_load_source'] == 'Sync'
        assert 'jrc_updated' in fields
        # only a DOI we did not already hold gets an insertion stamp
        assert ('jrc_inserted' in fields) == (write['doi'] == '10.1038/brandnew')


# --- timestamp comparison -------------------------------------------------

def test_fractional_seconds_do_not_count_as_a_change(ud):
    ''' Crossref and DataCite vary on sub-second precision. If these did not
        normalise, every stored DOI would look changed on every run.
    '''
    assert (ud.convert_timestamp('2024-01-01T00:00:00.123Z')
            == ud.convert_timestamp('2024-01-01T00:00:00Z'))
    assert (ud.convert_timestamp('2024-01-01T00:00:00.123456Z')
            == ud.convert_timestamp('2024-01-01T00:00:00Z'))


def test_differing_precision_alone_triggers_no_update(ud, arg):
    ud.ARG = arg()
    ud.EXISTING = stored()
    assert not ud.crossref_needs_update(
        '10.1038/known', {'deposited': {'date-time': '2024-01-01T00:00:00.987Z'}})


# --- publication cutoff ---------------------------------------------------

@pytest.mark.parametrize('published,expected', [
    ('2006-03-31', True),      # before the cutoff
    ('2006-04-01', False),     # the cutoff itself is kept
    ('2006-04-02', False),
])
def test_publication_cutoff_boundary(ud, published, expected):
    ud.DL = types.SimpleNamespace(get_publishing_date=lambda rec: published)
    assert ud.too_old('10.1038/x', {}) is expected


def test_unreadable_publishing_date_is_treated_as_too_old(ud):
    ''' Fail closed: a record whose date cannot be read is skipped, not loaded. '''
    def boom(_rec):
        raise ValueError('no date')
    ud.DL = types.SimpleNamespace(get_publishing_date=boom)
    assert ud.too_old('10.1038/x', {}) is True
