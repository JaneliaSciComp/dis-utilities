''' --mode and --registrar routing: which DOIs a run touches, and what it
    skips before spending an API call on them.
'''

import pytest


# --- --registrar ----------------------------------------------------------

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
    assert ud.wanted_registrar('10.25378/janelia.1')
    ud.ARG = arg(REGISTRAR='datacite')
    assert not ud.wanted_registrar('10.25378/janelia.1')


def test_registrar_falls_back_to_heuristic_when_unseen(ud, arg):
    ud.ARG = arg(REGISTRAR='datacite')
    ud.EXISTING = {}
    assert ud.wanted_registrar('10.25378/janelia.9')
    ud.ARG = arg(REGISTRAR='crossref')
    assert ud.wanted_registrar('10.1038/unseen')


@pytest.mark.parametrize('registrar,expected', [
    ('both', ['crossref:janelia', 'crossref:ror',
              'datacite:janelia', 'datacite:affiliation']),
    ('crossref', ['crossref:janelia', 'crossref:ror']),
    ('datacite', ['datacite:janelia', 'datacite:affiliation']),
])
def test_registrar_skips_unneeded_sweeps(ud, arg, sweeps, registrar, expected):
    import types
    ud.ARG = arg(MODE='insert', REGISTRAR=registrar)
    ud.EXISTING = {}
    ud.IGNORE = {'doi': {}, 'em_dataset': {}}
    ud.JRC = types.SimpleNamespace(get_config=lambda k: {},
                                   simplenamespace_to_dict=lambda x: {})
    ud.get_dois_for_dis({'dois': []})
    assert [c for c in sweeps if ':' in c] == expected


# --- --mode ---------------------------------------------------------------

def test_update_mode_skips_all_discovery(ud, arg, sweeps):
    ''' Update mode must not run the Crossref/DataCite/FLYF2 sweeps just to
        discard everything they find.
    '''
    ud.ARG = arg(MODE='update')
    ud.EXISTING = {'10.1038/a': {'jrc_obtained_from': 'Crossref'},
                   '10.5281/zenodo.b': {'jrc_obtained_from': 'DataCite'}}
    assert sorted(ud.get_dois()['dois']) == ['10.1038/a', '10.5281/zenodo.b']
    assert sweeps == [], f"update mode made discovery calls: {sweeps}"


def test_update_mode_honors_registrar(ud, arg, sweeps):
    ud.ARG = arg(MODE='update', REGISTRAR='crossref')
    ud.EXISTING = {'10.1038/a': {'jrc_obtained_from': 'Crossref'},
                   '10.5281/zenodo.b': {'jrc_obtained_from': 'DataCite'}}
    assert ud.get_dois()['dois'] == ['10.1038/a']
    assert sweeps == []


def test_insert_mode_only_touches_new_dois(routing):
    seen = routing('insert')
    assert seen['inserted'] == ['10.1038/new', '10.5281/zenodo.new']
    assert seen['updated'] == []


def test_update_mode_only_touches_known_dois(ud, routing):
    seen = routing('update')
    assert seen['updated'] == ['10.1038/known', '10.5281/zenodo.known']
    assert ud.COUNT['skipped_new'] == 2


def test_both_mode_touches_everything(routing):
    assert len(routing('both')['updated']) == 4


def test_registrar_filter_applies_in_the_loop(ud, routing):
    seen = routing('both', registrar='crossref')
    assert seen['updated'] == ['10.1038/known', '10.1038/new']
    assert ud.COUNT['skipped_registrar'] == 2


# --- throttling and counters ---------------------------------------------

def test_throttle_tracks_api_calls_not_input_size(ud, routing):
    ''' The sleep used to hang off the --insert flag, so insert runs fetched
        unthrottled while other runs slept on DOIs they then skipped.
    '''
    routing('insert')
    assert len(ud.sleeps) == 2, "one sleep per fetched DOI, not per input DOI"


def test_insert_mode_counts_records_it_fetched(ud, routing):
    ''' foundc/foundd stayed 0 on the insert path, which bypasses
        persist_if_updated where they are normally incremented.
    '''
    routing('insert')
    assert ud.COUNT['foundc'] == 1
    assert ud.COUNT['foundd'] == 1
