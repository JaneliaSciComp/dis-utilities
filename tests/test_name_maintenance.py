''' The write paths in sync/maintenance/bin: apply_decisions (the --review
    path), apply_dedupe (the --dedupe path), and the name rules they act on.

    These programs edit the orcid collection, which apply_orcids.py and
    add_orcid_name_variants.py also write. Every test here is offline: the
    collection is a fake that records what it was asked to do, so the shape of
    each update - $addToSet vs a whole-array $set - is asserted directly. That
    distinction is the whole safety argument for these writes, and it is not
    visible from the outcome of a single-writer test.
'''

import types

import pytest

import fix_middle_names as FMN
from dis_name_lib import (exact_key, first_name, initial_variants,
                          is_initials_only, is_latin, split_against_family,
                          split_against_given)
from dis_review_lib import apply_decisions


class FakeOrcidCollection:
    ''' Records updates instead of applying them, and answers find_one from a
        dict of seeded documents.
    '''

    def __init__(self, docs=None):
        self.docs = {d['_id']: dict(d) for d in (docs or [])}
        self.updates = []

    def find_one(self, query, _projection=None):
        return self.docs.get(query.get('_id'))

    def update_one(self, query, update, upsert=False):
        self.updates.append({'query': query, 'update': update})
        # Only $addToSet is modelled; a $set is recorded but not applied, since
        # no test asserts on a read following one.
        modified = 0
        doc = self.docs.get(query.get('_id')) or {}
        for field, spec in (update.get('$addToSet') or {}).items():
            have = doc.get(field) or []
            for value in spec['$each']:
                if value not in have:
                    have.append(value)
                    modified = 1
            doc[field] = have
        if '$set' in update:
            modified = 1
        return types.SimpleNamespace(matched_count=1, modified_count=modified)


class FakeLogger:
    def __init__(self):
        self.warnings = []

    def warning(self, msg):
        self.warnings.append(str(msg))


# --- apply_decisions: the --review write ----------------------------------

def test_accepted_variants_are_added_not_assigned():
    ''' $addToSet, never a whole-array $set. apply_orcids.py writes this same
        list, and rebuilding it from a stale read would drop whatever it added
        between our read and our write.
    '''
    coll = FakeOrcidCollection([{'_id': 1, 'given': ['Gerald']}])
    written = apply_decisions(coll, [((('_id'), 1), 'given', 'Gerald M.')], FakeLogger())
    assert written == 1
    update = coll.updates[0]['update']
    assert '$set' not in update
    assert update['$addToSet'] == {'given': {'$each': ['Gerald M.']}}


def test_one_update_per_record_not_per_name():
    coll = FakeOrcidCollection([{'_id': 1, 'given': ['Gerald']}])
    accepted = [((('_id'), 1), 'given', 'Gerald M.'),
                ((('_id'), 1), 'given', 'Gerald M')]
    assert apply_decisions(coll, accepted, FakeLogger()) == 2
    assert len(coll.updates) == 1, "both names belong in one $each"
    assert coll.updates[0]['update']['$addToSet']['given']['$each'] == ['Gerald M.', 'Gerald M']


def test_separate_records_get_separate_updates():
    coll = FakeOrcidCollection([{'_id': 1, 'given': ['A']}, {'_id': 2, 'given': ['B']}])
    accepted = [((('_id'), 1), 'given', 'A. B'), ((('_id'), 2), 'given', 'B. C')]
    apply_decisions(coll, accepted, FakeLogger())
    assert {u['query']['_id'] for u in coll.updates} == {1, 2}


def test_a_name_already_present_is_reported_not_counted():
    ''' Re-running the program must not inflate the written count or fail. '''
    coll = FakeOrcidCollection([{'_id': 1, 'given': ['Gerald', 'Gerald M.']}])
    logger = FakeLogger()
    assert apply_decisions(coll, [((('_id'), 1), 'given', 'Gerald M.')], logger) == 0
    assert logger.warnings, "a no-op update should say so"


# --- apply_dedupe: the --dedupe write -------------------------------------

@pytest.fixture(name='dedupe_env')
def fixture_dedupe_env(monkeypatch):
    ''' apply_dedupe reads module globals; hand it a fake collection. '''
    def build(docs, ids):
        coll = FakeOrcidCollection(docs)
        monkeypatch.setattr(FMN, 'DB', {'dis': types.SimpleNamespace(orcid=coll)})
        monkeypatch.setattr(FMN, 'DEDUPE', list(ids))
        FMN.COUNT.clear()
        return coll
    return build


def test_dedupe_rereads_the_record_before_writing(dedupe_env):
    ''' The scan's copy may be stale by the time this runs - this is the one
        write that needs the whole array, so it must read back first.
    '''
    coll = dedupe_env([{'_id': 1, 'given': ['Gerald', 'Gerald', 'Gerald M.']}], [1])
    FMN.apply_dedupe()
    assert coll.updates[0]['update']['$set']['given'] == ['Gerald', 'Gerald M.']
    assert FMN.COUNT['deduped'] == 1


def test_dedupe_preserves_order(dedupe_env):
    coll = dedupe_env([{'_id': 1, 'given': ['B', 'A', 'B', 'C']}], [1])
    FMN.apply_dedupe()
    assert coll.updates[0]['update']['$set']['given'] == ['B', 'A', 'C']


def test_dedupe_writes_nothing_when_already_unique(dedupe_env):
    coll = dedupe_env([{'_id': 1, 'given': ['Gerald', 'Gerald M.']}], [1])
    FMN.apply_dedupe()
    assert not coll.updates
    assert not FMN.COUNT['deduped']


# --- the name rules the writes are derived from ---------------------------

def test_both_spellings_of_a_middle_initial_are_proposed():
    ''' Publishers are inconsistent about the period, so a name held in only
        one form silently fails to match the other half of them.
    '''
    assert set(initial_variants('Gerald M. Rubin')) == {'Gerald M. Rubin', 'Gerald M Rubin'}


def test_exact_key_is_period_sensitive():
    ''' normalize() folds the period away, which would make the two spellings
        look like the same name and defeat the whole point.
    '''
    assert exact_key('Gerald M. Rubin') != exact_key('Gerald M Rubin')


def test_first_name_is_taken_only_from_a_real_given_name():
    """ Operates on given-name variants, which do not carry the family name.
        A trailing word that is not an initial means the entry is a full name,
        not a given name, and nothing is proposed from it.
    """
    assert first_name(['Gerald M.']) == 'Gerald'
    assert first_name(['G. M.']) is None, "initials are not a first name"
    assert first_name(['Gerald']) is None, "a bare first name is already there"
    assert first_name(['Gerald M. Rubin']) is None, "that is a full name"


def test_initials_only_is_recognized():
    assert is_initials_only('G. M.')
    assert not is_initials_only('Gerald M.')


def test_an_accented_latin_name_is_latin():
    ''' The people running this read Latin script; an accent is not a reason
        to withhold a proposal.
    '''
    assert is_latin('Lösche')
    assert is_latin('Nguyễn')
    assert not is_latin('塩崎')


def test_a_maiden_name_is_not_a_new_family_name():
    ''' "Allison Sowell" on a record whose given name is Allison is a first
        name plus a maiden name, not a family name.
    '''
    assert split_against_given('Allison Sowell', ['Allison']) == 'Sowell'


def test_a_given_name_carrying_the_family_name_is_split():
    assert split_against_family('Gerald Rubin', ['Rubin']) == ('Gerald', 'Rubin')


def test_the_longest_family_name_wins():
    """ Matched token by token from the end, so a two-word family name beats
        the one-word name that is its tail.
    """
    assert split_against_family('Maria Alves Ferreira',
                                ['Ferreira', 'Alves Ferreira']) == ('Maria', 'Alves Ferreira')


def test_an_unmatched_family_name_splits_to_nothing():
    assert split_against_family('Gerald Rubin', ['Smith']) == (None, None)


# --- add_orcid_name_variants: not re-proposing what we already hold --------

@pytest.fixture(name='variants_env')
def fixture_variants_env(monkeypatch):
    ''' process_alternate_name reaches for module globals; supply them and
        hand back the module so a test can read REVIEW and COUNT.
    '''
    import add_orcid_name_variants as AONV
    monkeypatch.setattr(AONV, 'ARG', types.SimpleNamespace(NONLATIN=False), raising=False)
    monkeypatch.setattr(AONV, 'APPLY', [])
    monkeypatch.setattr(AONV, 'REVIEW', [])
    AONV.COUNT.clear()
    return AONV


def held_sets(given, family):
    ''' The same four sets process_alternate_name is handed in the real run. '''
    from dis_name_lib import exact_key as ek, normalize as nz
    return {'given': {nz(g) for g in given}, 'given_exact': {ek(g) for g in given},
            'family': {nz(f) for f in family}, 'families': list(family)}


def test_initials_we_already_hold_are_not_proposed_again(variants_env):
    ''' The initials branch returned before the already-held check, so the
        same initials came back for review on every run no matter how often
        they had been accepted - three of them on the first real --review run.
    '''
    rec = {'_id': 1, 'orcid': '0000-0001-8396-1533',
           'given': ['Wyatt', 'Wyatt L', 'Wyatt L.', 'W', 'WL'], 'family': ['Korff']}
    variants_env.process_alternate_name(rec, 'other-name', 'W Korff', {},
                                        held_sets(rec['given'], rec['family']))
    assert not variants_env.REVIEW, "already held - nothing to review"
    assert variants_env.COUNT['already_held'] == 1


def test_a_differently_spelled_initial_is_still_proposed(variants_env):
    ''' Skipping on the normalized key would fold "M A" into "M.A." and drop
        it. Publishers deposit both, and holding both is the entire point.
    '''
    rec = {'_id': 1, 'orcid': '0000-0002-0470-6911',
           'given': ['Miguel', 'M.A.'], 'family': ['Nunez']}
    variants_env.process_alternate_name(rec, 'credit-name', 'M A Nunez', {},
                                        held_sets(rec['given'], rec['family']))
    assert len(variants_env.REVIEW) == 1
    assert variants_env.REVIEW[0]['value'] == 'M A'
