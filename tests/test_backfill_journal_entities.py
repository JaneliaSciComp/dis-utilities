''' backfill_journal_entities.py: removing HTML entities from stored jrc_journal.

    The functions are extracted with ast, because importing the script needs
    jrc_common and a database configuration.
'''

import ast
import collections
import html
import re
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / 'utility' / 'bin' / 'backfill_journal_entities.py').read_text(encoding='utf-8')


def _load():
    ns = {'collections': collections, 'html': html, 're': re,
          'COUNT': collections.defaultdict(int),
          'LOGGER': SimpleNamespace(error=lambda msg: None)}
    for node in ast.parse(SOURCE).body:
        targets = [t.id for t in getattr(node, 'targets', []) if isinstance(t, ast.Name)]
        if getattr(node, 'name', None) in ('unescaped', 'backfill_collection') \
           or targets == ['ENTITY']:
            exec(ast.get_source_segment(SOURCE, node), ns)    # pylint: disable=W0122
    return ns


class FakeCollection:
    ''' find on a jrc_journal $regex, and update_one on _id plus the old value. '''
    def __init__(self, docs):
        self.docs = docs

    def find(self, query, _proj):
        pat = re.compile(query['jrc_journal']['$regex'])
        return [dict(d) for d in self.docs
                if isinstance(d.get('jrc_journal'), str) and pat.search(d['jrc_journal'])]

    def update_one(self, query, update):
        doc = next((d for d in self.docs if all(d.get(k) == v for k, v in query.items())),
                   None)
        if doc:
            doc.update(update['$set'])
        return SimpleNamespace(modified_count=1 if doc else 0)


@pytest.mark.parametrize('stored, fixed', [
    ('Genes &amp; Development', 'Genes & Development'),
    ('Arthritis &amp; Rheumatology', 'Arthritis & Rheumatology'),
    ('Journal &#38; Review', 'Journal & Review'),
    ('Journal &#x26; Review', 'Journal & Review'),
    ('A &amp;amp; B', 'A &amp; B'),                # one level only
])
def test_entities_are_removed(stored, fixed):
    assert _load()['unescaped'](stored) == fixed


@pytest.mark.parametrize('stored', ['Genes & Development', 'R&D Journal', 'AT&T Technical',
                                    'Nature', '', None])
def test_names_without_an_entity_are_left_alone(stored):
    ''' A bare ampersand ("R&D") is not an entity, though the query matches it. '''
    assert _load()['unescaped'](stored) is None


def _docs():
    return [{'_id': 1, 'doi': '10.1/a', 'jrc_journal': 'Genes &amp; Development',
             'container-title': ['Genes &amp; Development']},
            {'_id': 2, 'doi': '10.1/b', 'jrc_journal': 'R&D Journal'},
            {'_id': 3, 'doi': '10.1/c', 'jrc_journal': 'Nature'}]


def test_a_dry_run_reports_and_changes_nothing():
    ns, docs = _load(), _docs()
    changes = ns['backfill_collection'](FakeCollection(docs), 'dois', write=False)
    assert [(c['doi'], c['new'], c['status']) for c in changes] == \
           [('10.1/a', 'Genes & Development', 'would fix')]
    assert docs[0]['jrc_journal'] == 'Genes &amp; Development'


def test_write_fixes_jrc_journal_only():
    ns, docs = _load(), _docs()
    changes = ns['backfill_collection'](FakeCollection(docs), 'dois', write=True)
    assert changes[0]['status'] == 'fixed'
    assert docs[0]['jrc_journal'] == 'Genes & Development'
    assert docs[0]['container-title'] == ['Genes &amp; Development']    # registrar's record
    assert docs[1]['jrc_journal'] == 'R&D Journal'


def test_a_value_changed_since_it_was_read_is_left_alone():
    ns, docs = _load(), _docs()
    coll = FakeCollection(docs)
    original = coll.update_one

    def edit_first(query, update):
        docs[0]['jrc_journal'] = 'Edited by hand'
        return original(query, update)
    coll.update_one = edit_first
    changes = ns['backfill_collection'](coll, 'dois', write=True)
    assert changes[0]['status'] == 'stale' and docs[0]['jrc_journal'] == 'Edited by hand'


def test_a_rerun_changes_nothing():
    ns, docs = _load(), _docs()
    ns['backfill_collection'](FakeCollection(docs), 'dois', write=True)
    assert not ns['backfill_collection'](FakeCollection(docs), 'dois', write=True)
