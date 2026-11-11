''' repair_subscription_encoding.py: find, repair and undo against a fake collection.

    The functions are extracted with ast (as is repair_mojibake from load_kbart.py),
    because importing either script needs jrc_common and a database configuration.
    The suite runs without pymongo (CI installs only pytest, jrc_common and
    doi_common), so ObjectId below stands in for bson's.
'''

import ast
import collections
import json
from pathlib import Path
from types import SimpleNamespace

import itertools

import pytest

ROOT = Path(__file__).resolve().parents[1]
BIN = ROOT / 'journals' / 'bin'


def _extract(path, names, ns):
    source = path.read_text(encoding='utf-8')
    for node in ast.parse(source).body:
        targets = [t.id for t in getattr(node, 'targets', []) if isinstance(t, ast.Name)]
        if getattr(node, 'name', None) in names or any(t in names for t in targets):
            exec(ast.get_source_segment(source, node), ns)    # pylint: disable=W0122


class ObjectId:
    ''' Stand-in for bson.ObjectId: the script only checks the type name, converts a
        report's text id back with ObjectId(text), and compares ids. '''
    _ids = itertools.count()

    def __init__(self, text=None):
        self.text = text if text is not None else f"{next(self._ids):024x}"

    def __str__(self):
        return self.text

    def __eq__(self, other):
        return isinstance(other, ObjectId) and other.text == self.text

    def __hash__(self):
        return hash(self.text)


class Stop(Exception):
    ''' terminate_program() exits; here it raises. '''


class FakeCollection:
    ''' Equality matching plus $ne, which is all the script uses. '''
    def __init__(self, docs):
        self.docs = docs

    @staticmethod
    def _match(doc, query):
        for key, val in query.items():
            if isinstance(val, dict) and '$ne' in val:
                if doc.get(key) == val['$ne']:
                    return False
            elif doc.get(key) != val:
                return False
        return True

    def find(self, query):
        return [dict(d) for d in self.docs if self._match(d, query)]

    def find_one(self, query, _proj=None):
        return next((d for d in self.docs if self._match(d, query)), None)

    def update_one(self, query, update):
        doc = self.find_one(query)
        if doc:
            doc.update(update['$set'])
        return SimpleNamespace(modified_count=1 if doc else 0)


def _script(write):
    ns = {'collections': collections, 'json': json, 'ObjectId': ObjectId,
          'ARG': SimpleNamespace(WRITE=write), 'COUNT': collections.defaultdict(int),
          'LOGGER': SimpleNamespace(error=lambda msg: None)}

    def terminate_program(msg=None):
        raise Stop(msg)
    ns['terminate_program'] = terminate_program
    _extract(BIN / 'load_kbart.py', ('repair_mojibake', 'MISREAD_AS'), ns)
    _extract(BIN / 'repair_subscription_encoding.py',
             ('find_repairs', 'mark_conflicts', 'apply_repairs', 'undo_repairs'), ns)
    return ns


BAD_TITLE, GOOD_TITLE = 'Activation of C‚ÄîH Bonds', 'Activation of C—H Bonds'
BAD_PUB, GOOD_PUB = 'Universit√§tsverlag WINTER', 'Universitätsverlag WINTER'


def _docs():
    return [
        {'_id': ObjectId(), 'title': BAD_TITLE, 'publisher': BAD_PUB, 'provider': 'Springer',
         'identifier': '1', 'type': 'Monograph', 'cost': {'2026': 1}},
        {'_id': ObjectId(), 'title': 'Zeitschrift für Naturforschung', 'publisher': 'De Gruyter',
         'provider': 'Springer', 'identifier': '2', 'type': 'Journal'},
        {'_id': ObjectId(), 'title': 'Nature', 'publisher': 'Springer Nature',
         'provider': 'Springer', 'identifier': '3', 'type': 'Journal'},
    ]


def _run(ns, coll):
    repairs = ns['find_repairs'](coll.find({}))
    ns['mark_conflicts'](coll, repairs)
    ns['apply_repairs'](coll, repairs)
    return repairs


def test_only_damaged_fields_are_found():
    ns = _script(write=False)
    repairs = ns['find_repairs'](_docs())
    assert [(r['field'], r['new']) for r in repairs] == [('title', GOOD_TITLE),
                                                        ('publisher', GOOD_PUB)]
    assert repairs[0]['id_type'] == 'ObjectId'


def test_a_dry_run_changes_nothing():
    ns, docs = _script(write=False), _docs()
    repairs = _run(ns, FakeCollection(docs))
    assert {r['status'] for r in repairs} == {'would repair'}
    assert docs[0]['title'] == BAD_TITLE and docs[0]['publisher'] == BAD_PUB


def test_write_repairs_the_damaged_fields_and_nothing_else():
    ns, docs = _script(write=True), _docs()
    before = [dict(d) for d in docs]
    repairs = _run(ns, FakeCollection(docs))
    assert {r['status'] for r in repairs} == {'repaired'}
    assert docs[0]['title'] == GOOD_TITLE and docs[0]['publisher'] == GOOD_PUB
    assert docs[0]['cost'] == {'2026': 1}
    assert docs[1:] == before[1:]


def test_a_title_that_would_collide_is_skipped_not_written():
    ns, docs = _script(write=True), _docs()
    docs.append({'_id': ObjectId(), 'title': GOOD_TITLE, 'publisher': GOOD_PUB,
                 'provider': 'Springer', 'identifier': '1', 'type': 'Monograph'})
    repairs = _run(ns, FakeCollection(docs))
    status = {r['field']: r['status'] for r in repairs}
    assert status == {'title': 'conflict', 'publisher': 'repaired'}
    assert docs[0]['title'] == BAD_TITLE
    assert ns['COUNT']['conflict'] == 1


def test_the_same_title_under_another_provider_is_not_a_conflict():
    ns, docs = _script(write=True), _docs()
    docs.append({'_id': ObjectId(), 'title': GOOD_TITLE, 'provider': 'Wiley', 'identifier': '1'})
    repairs = _run(ns, FakeCollection(docs))
    assert repairs[0]['status'] == 'repaired'


def test_a_value_changed_since_it_was_read_is_left_alone():
    ns, docs = _script(write=True), _docs()
    coll = FakeCollection(docs)
    repairs = ns['find_repairs'](coll.find({}))
    ns['mark_conflicts'](coll, repairs)
    docs[0]['title'] = 'Edited by hand'
    ns['apply_repairs'](coll, repairs)
    assert repairs[0]['status'] == 'stale' and docs[0]['title'] == 'Edited by hand'


def test_undo_restores_from_the_report():
    ns, docs = _script(write=True), _docs()
    coll = FakeCollection(docs)
    report = json.loads(json.dumps(_run(ns, coll), default=str))   # as written to disk
    ns['undo_repairs'](coll, report)
    assert {e['undo'] for e in report} == {'restored'}
    assert docs[0]['title'] == BAD_TITLE and docs[0]['publisher'] == BAD_PUB


def test_undo_dry_run_changes_nothing_and_skips_unrepaired_entries():
    ns, docs = _script(write=True), _docs()
    coll = FakeCollection(docs)
    report = json.loads(json.dumps(_run(ns, coll), default=str))
    report[1]['status'] = 'conflict'
    ns['ARG'].WRITE = False
    ns['undo_repairs'](coll, report)
    assert [e['undo'] for e in report] == ['would restore', 'not repaired']
    assert docs[0]['title'] == GOOD_TITLE


def test_undo_leaves_a_field_edited_after_the_repair():
    ns, docs = _script(write=True), _docs()
    coll = FakeCollection(docs)
    report = json.loads(json.dumps(_run(ns, coll), default=str))
    docs[0]['title'] = 'Edited after the repair'
    ns['undo_repairs'](coll, report)
    assert report[0]['undo'] == 'stale' and docs[0]['title'] == 'Edited after the repair'


def test_the_script_uses_the_loaders_detection():
    source = (BIN / 'repair_subscription_encoding.py').read_text(encoding='utf-8')
    assert 'from load_kbart import repair_mojibake' in source
    assert 'def repair_mojibake' not in source
