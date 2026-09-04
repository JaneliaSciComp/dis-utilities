''' Fakes and record builders shared by the update_dois tests.

    Stateless things live here rather than in conftest.py: they are plain data,
    and an explicit import reads better at the top of a test file than an
    invisible fixture. Anything that has to reach into the loaded module (and so
    needs the `ud` fixture) is a fixture in conftest.py instead.
'''

import types

DEPOSITED_STORED = '2024-01-01T00:00:00Z'
DEPOSITED_NEWER = '2025-06-01T00:00:00Z'


def fake_add_doi_process(doi, action=None, coll=None, notes=None):
    ''' Stand in for doi_common.add_doi_process, writing the same shape into
        the supplied (fake) collection so a test can assert on the event. The
        real one also derives program/version from the calling module and
        decides whether to record a user; neither is what these tests are
        checking.
    '''
    payload = {'action': action, 'program': 'update_dois.py'}
    if notes is not None:
        payload['notes'] = notes
    coll.update_one({'type': 'doi', 'key': doi},
                    {'$push': {'processes': payload}}, upsert=True)


class FakeCollection:
    ''' Stands in for a MongoDB collection, recording what it was asked to do. '''

    def __init__(self, docs=None, deleted=1):
        self.docs = list(docs or [])
        self.deleted = deleted
        self.deletes = []
        self.finds = []
        self.updates = []

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

    def update_one(self, query, update, upsert=False):
        # 'key' covers the processing collection, which is keyed {type, key}
        # rather than by doi - same convention find_one already follows.
        self.updates.append({'doi': query.get('doi') or query.get('key'),
                             'update': update, 'upsert': upsert})
        return types.SimpleNamespace(matched_count=1, modified_count=1,
                                     upserted_id=None)


def crossref_msg(date=DEPOSITED_STORED):
    ''' Bare Crossref response - enough for the needs-update comparison. '''
    return {'message': {'deposited': {'date-time': date}}}


def datacite_msg(date=DEPOSITED_STORED):
    ''' Bare DataCite response - enough for the needs-update comparison. '''
    return {'data': {'attributes': {'updated': date}}}


def crossref_record(deposited=DEPOSITED_NEWER, **extra):
    ''' A Crossref record carrying the fields get_doi_record() insists on:
        an author (or editor/investigator) and a title. Records missing either
        are rejected, so tests that expect a DOI to load need both.
    '''
    rec = {'deposited': {'date-time': deposited},
           'author': [{'family': 'Doe', 'given': 'J'}],
           'title': ['A test article']}
    rec.update(extra)
    return {'message': rec}
