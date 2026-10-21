''' The audit trail: what changed, who changed it, and when.

    updated_by on a record says who touched it LAST. That answers "who do I ask
    about this", and not "who granted this authorization, and when did someone
    else lose it". The trail is append-only and never pruned, because one that
    can be edited or expires is not a trail.

    It reuses the processing collection - the same {key, type, processes: [...]}
    shape DOIs already use - rather than inventing a collection: one thing to
    back up, one shape to read. doi_common.add_doi_process is NOT reusable, as
    it keys by DOI and takes the actor from the run, which for a web request is
    the server process rather than the person who clicked.
'''

import ast
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / 'api' / 'dis_responder.py').read_text(encoding='utf-8')
LINES = SOURCE.splitlines()


class _Processing:
    ''' Enough of a collection to record what was asked of it. '''
    def __init__(self):
        self.calls = []
        self.doc = None
    def update_one(self, query, update, upsert=False):
        self.calls.append((query, update, upsert))
        self.doc = {**query, "processes": [update['$push']['processes']]}
    def find_one(self, _query):
        return self.doc


def _ns(coll=None, explode=False):
    coll = coll or _Processing()
    if explode:
        def _boom(*_a, **_k):
            raise RuntimeError("mongo is down")
        coll.update_one = _boom
    ns = {'datetime': datetime, '__version__': '1.2.3',
          'AUDIT_PERSON': 'orcid', 'AUDIT_SERVICE': 'service_account',
          'DB': {'dis': type('D', (), {'processing': coll})()},
          'print': lambda *a, **k: None}
    for name in ('record_access_event', 'access_events'):
        node = next(n for n in ast.parse(SOURCE).body
                    if isinstance(n, ast.FunctionDef) and n.name == name)
        exec(ast.get_source_segment(SOURCE, node), ns)    # pylint: disable=W0122
    ns['_coll'] = coll
    return ns


def test_an_event_is_appended_never_overwritten():
    ''' $push, not $set. A trail that replaces its last entry is a latest-change
        field with extra steps.
    '''
    ns = _ns()
    ns['record_access_event']('abc', 'orcid', 'authorization_set', 'ORCID-1', 'x')
    (query, update, upsert) = ns['_coll'].calls[0]
    assert '$push' in update and '$set' not in update
    assert upsert is True, "the first event for a record must create the document"
    assert query == {"key": "abc", "type": "orcid"}


def test_the_actor_is_recorded_as_an_orcid_id():
    ns = _ns()
    ns['record_access_event']('abc', 'orcid', 'alumni_set', '0000-0001-8374-6008', '')
    event = ns['_coll'].calls[0][1]['$push']['processes']
    assert event['actor'] == '0000-0001-8374-6008'
    for field in ('action', 'timestamp', 'notes', 'program', 'version'):
        assert field in event, field


def test_the_key_is_stringified():
    ''' An ObjectId key would not match the string the page looks up with. '''
    ns = _ns()
    ns['record_access_event'](12345, 'orcid', 'x', 'ORCID-1', '')
    assert ns['_coll'].calls[0][0]['key'] == '12345'


def test_a_failed_audit_write_does_not_raise():
    ''' This runs AFTER the change it describes. Raising here would report
        failure for a save that already succeeded - worse than a lost event.
    '''
    ns = _ns(explode=True)
    ns['record_access_event']('abc', 'orcid', 'x', 'ORCID-1', '')   # must not raise


def test_events_come_back_newest_first():
    ns = _ns()
    coll = ns['_coll']
    coll.doc = {"key": "abc", "type": "orcid", "processes": [
        {"action": "old", "timestamp": "2026-01-01T00:00:00"},
        {"action": "new", "timestamp": "2026-10-01T00:00:00"},
        {"action": "mid", "timestamp": "2026-05-01T00:00:00"}]}
    assert [e['action'] for e in ns['access_events']('abc')] == ['new', 'mid', 'old']


def test_a_record_with_no_history_reads_as_empty_not_an_error():
    ns = _ns()
    ns['_coll'].doc = None
    assert ns['access_events']('nobody') == []


def _writers_calling(name):
    ''' Functions whose body calls `name` '''
    out = set()
    for node in ast.walk(ast.parse(SOURCE)):
        if not isinstance(node, ast.FunctionDef):
            continue
        body = '\n'.join(LINES[node.body[0].lineno - 1:node.end_lineno])
        if f'{name}(' in body:
            out.add(node.name)
    return out


def test_every_authorization_change_is_recorded():
    ''' The point of the trail. A write path that skips it leaves a silent gap
        exactly where someone will later look.
    '''
    recorded = _writers_calling('record_access_event')
    expected = {'_save_authorizations', '_save_person_field', '_issue_api_token',
                '_revoke_api_token', '_save_service_account', '_save_service_edit'}
    assert expected <= recorded, f"not recording: {sorted(expected - recorded)}"


def test_both_pages_show_the_history():
    ''' Service-account events were being recorded into a table nothing
        displayed: the first version rendered history on the person page only,
        and a service account has no person page.
    '''
    assert "history_html(orc['_id'], AUDIT_PERSON)" in SOURCE
    assert "history_html(acct['_id'], AUDIT_SERVICE)" in SOURCE


def test_one_renderer_serves_both():
    ''' Two copies would drift, and the second would be the one nobody looks
        at.
    '''
    assert SOURCE.count('def history_html(') == 1
    assert SOURCE.count("render_table(['When', 'What', 'Who', 'Detail']") == 1


def test_history_cells_are_not_pre_escaped():
    ''' render_table escapes every cell not wrapped in safe(). Escaping first
        showed the reader &#x27; and &gt; where quotes and arrows belonged - the
        same mistake test_escaping.py guards against for render_warning.
    '''
    node = next(n for n in ast.parse(SOURCE).body
                if isinstance(n, ast.FunctionDef) and n.name == 'history_html')
    body = '\n'.join(LINES[node.body[0].lineno - 1:node.end_lineno])
    appended = body[body.index('rows.append('):body.index('render_table')]
    assert 'escape(' not in appended, \
        "cells reach render_table raw; only the actor link is safe()-wrapped"


def test_a_term_list_reads_as_prose_not_a_python_repr():
    ''' sorted(['author','orcid']) renders as "['author', 'orcid']" - brackets,
        quotes and all - which is a repr leaking into a UI table.
    '''
    ns = {}
    node = next(n for n in ast.parse(SOURCE).body
                if isinstance(n, ast.FunctionDef) and n.name == '_terms_phrase')
    exec(ast.get_source_segment(SOURCE, node), ns)    # pylint: disable=W0122
    phrase = ns['_terms_phrase']
    assert phrase(['orcid', 'author']) == 'author, orcid'
    assert phrase([]) == 'none'
    for bad in ('[', ']', "'"):
        assert bad not in phrase(['author', 'orcid']), bad


def test_no_audit_note_builds_a_list_repr():
    ''' Every before/after note must go through _terms_phrase.

        Scoped to the CALL arguments, not the enclosing function: sorted() is
        correct for storing a list and fine in a server log line, and checking
        the whole body flagged both.
    '''
    tree = ast.parse(SOURCE)
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call)
                and getattr(node.func, 'id', '') == 'record_access_event'):
            continue
        for arg in node.args:
            text = ast.get_source_segment(SOURCE, arg) or ''
            assert 'sorted(' not in text, \
                f"audit note interpolates a sorted() list: {text[:60]}"
