''' Input handling for update_dois.py.

    A malformed entry must never cost more than itself: the run continues, the
    entry is counted, and everything after it is still processed.
'''


def test_split_raw_doi_splits_on_pipe(ud):
    assert ud.split_raw_doi('10.1/a | 10.2/b') == ['10.1/a', '10.2/b']
    assert ud.split_raw_doi('10.1/a') == ['10.1/a']


def test_pipe_separated_dois_are_split(ud):
    assert ud.split_raw_doi('10.1/a | 10.2/b|10.3/c') == ['10.1/a', '10.2/b', '10.3/c']


def test_doi_is_lowercased_and_stripped_before_lookup(fetches):
    ''' DOIs are stored lowercase, so an unnormalised one would never match. '''
    assert fetches(['  10.1038/MiXeD.Case  ']) == ['10.1038/mixed.case']


def test_a_doi_repeated_in_the_input_is_processed_once(fetches):
    assert fetches(['10.1038/a', '10.1038/a', '10.1038/A']) == ['10.1038/a']


def test_blank_input_line_is_skipped_not_looked_up(ud, fetches):
    ''' A blank line in a --file survives split_raw_doi() as "". It must not
        reach the registrar, and must not become an empty key anywhere.
    '''
    assert fetches(['10.1038/a', '', '   ']) == ['10.1038/a']
    assert ud.COUNT['blank'] == 2


def test_an_invalid_doi_is_skipped_and_the_run_continues(ud, fetches):
    ''' A pasted URL-form DOI used to call terminate_program(), abandoning every
        DOI after it. It is now counted and stepped over.
    '''
    assert fetches(['10.1038/good', 'https://doi.org/10.1038/bad',
                    '10.1038/after']) == ['10.1038/good', '10.1038/after']
    assert ud.COUNT['invalid'] == 1
    assert 'Invalid DOI: https://doi.org/10.1038/bad' in ud.MISSING


def test_ignored_dois_never_reach_the_api(ud, arg):
    fetched = []
    ud.ARG = arg()
    ud.IGNORE = {'doi': {'10.1038/bad': True}}
    ud.EXISTING = {}
    ud.get_dois = lambda: {'dois': ['10.1038/bad', '10.1038/ok']}
    ud.get_doi_record = lambda doi: fetched.append(doi) or None
    ud.persist_if_updated = lambda *a: None
    ud.update_dois = lambda *a: None
    ud.process_dois()
    assert fetched == ['10.1038/ok']
    assert ud.COUNT['skipped'] == 1
