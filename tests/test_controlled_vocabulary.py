''' Controlled vocabularies: who may edit them, what may change, and what a
    deletion is allowed to orphan.

    Three things carry the weight here:

      - A term's NAME is a lookup key. CVTERM['jrc']['jrc_inserted'], a stored
        jrc_license value, a granted authorization on an orcid record - all key
        off it, so renaming would orphan every reference silently.
      - CVTERM is a process-local cache and this app runs two replicas, so an
        edit is invisible to the other pod until something reloads it.
      - Deleting a term that records still reference loses the label for data
        that is still there.
'''

import ast
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / 'api' / 'dis_responder.py').read_text(encoding='utf-8')


def _block(start, end=None):
    seg = SOURCE[SOURCE.index(start):]
    return seg[:seg.index(end)] if end else seg


# --- who may edit ----------------------------------------------------------

def test_the_vocabulary_pages_require_admin():
    ''' These are the app's own plumbing - field labels, licence names, and the
        authorization terms that gate these very pages.
    '''
    head = SOURCE[SOURCE.index("@app.route('/cv',"):SOURCE.index('def cvs(')]
    assert "@restricted('admin')" in head


def test_both_routes_accept_a_post():
    for rule in ("@app.route('/cv', methods=['GET', 'POST'])",
                 "@app.route('/cv/<string:cv>', methods=['GET', 'POST'])"):
        assert rule in SOURCE, rule


# --- a name is never editable ----------------------------------------------

def test_saving_a_term_cannot_rename_it():
    ''' The name is the lookup key. Renaming would orphan every reference, so
        the edit form posts it as a hidden field and the update uses it only to
        find the record.
    '''
    block = _block("elif action == 'save_term':", "elif action == 'del_term':")
    written = re.findall(r'"(\w+)":', block[block.index('$set'):])
    assert 'name' not in written, 'save_term must not write the name'
    # updated/updated_by arrive via **stamp, so they are not literal keys here.
    assert set(written) == {'display', 'definition', 'format'}
    assert '**stamp' in block, 'the edit is still audited'
    assert '{"cv": cv, "name": name}' in block, 'name is the filter, not the payload'


def test_the_page_says_why_a_name_cannot_change():
    assert 'A name is never editable' in SOURCE


# --- deletion guards -------------------------------------------------------

def test_a_term_in_use_cannot_be_deleted():
    block = _block("elif action == 'del_term':", "else:")
    assert 'cvterm_usage(cv, name)' in block
    assert 'if blocking and count:' in block
    assert 'orphaned' in block


def test_the_usage_map_covers_the_vocabularies_that_store_values():
    ''' authorization lands on orcid.authorization, licence and OA status on
        dois fields. A cv absent from the map is reported as uncountable rather
        than implied safe.
    '''
    block = _block('CV_USAGE = {', '\ndef cvterm_usage')
    for cv, field in (('authorization', 'authorization'), ('license', 'jrc_license'),
                      ('oa_status', 'jrc_oa_status')):
        assert f"'{cv}'" in block and f"'{field}'" in block, cv
    assert "'jrc'" in block and 'field_name' in block


def test_a_jrc_term_is_a_field_name_not_a_value():
    ''' Deleting one removes a display label rather than orphaning data, so it
        warns instead of refusing.
    '''
    block = _block('def cvterm_usage', '\n@app.route')
    assert "if kind == 'field_name':" in block
    assert '{name: {"$exists": True}}' in block
    assert 'return DB[\'dis\'][collection].count_documents({name: {"$exists": True}}), False' \
        in block, 'field-name usage must be non-blocking'


def test_an_uncountable_vocabulary_is_not_reported_as_unused():
    block = _block('def cvterm_usage', '\n@app.route')
    assert 'if not probe:' in block
    assert 'return None, False' in block


def test_a_vocabulary_can_only_be_deleted_when_empty():
    block = _block("elif action == 'del_cv':", "elif action == 'add_term':")
    assert 'count_documents({"cv": cv})' in block
    assert 'Remove them' in block


# --- the cache -------------------------------------------------------------

def test_a_write_refreshes_the_cache_on_this_pod():
    ''' Without it the editor does not see their own change until a deploy. '''
    block = _block('def _save_cv_change', '\ndef _cv_term_rows')
    assert 'refresh_cvterm()' in block


def test_the_cache_expires_so_the_other_replica_catches_up():
    ''' replicas: 2, and CVTERM is process-local. '''
    assert 'CVTERM_TTL' in SOURCE
    block = _block('def before_request', '\ndef ')
    assert "time() - app.config.get('CVTERM_LOADED', 0) > CVTERM_TTL" in block


def test_the_refresh_clears_before_it_updates():
    ''' update() alone never removes a key, so a deleted term would survive in
        memory and keep granting whatever it granted.
    '''
    block = _block('def refresh_cvterm', '\n@app.before_request')
    assert 'CVTERM.clear()' in block
    assert block.index('CVTERM.clear()') < block.index('CVTERM.update(local)')


def test_the_refresh_commits_only_after_a_successful_read():
    ''' Built into a local first, so a transient failure leaves the previous
        vocabulary serving rather than an empty one.
    '''
    block = _block('def refresh_cvterm', '\n@app.before_request')
    assert block.index('local = {}') < block.index('CVTERM.clear()')


def test_a_cache_refresh_failure_does_not_take_the_app_down():
    block = _block('def before_request', '\ndef ')
    assert 'Could not refresh CVTERM' in block


# --- audit -----------------------------------------------------------------

def test_every_change_records_who_and_when():
    block = _block('def _save_cv_change', '\ndef _cv_term_rows')
    assert '"updated": datetime.now()' in block
    assert '"updated_by": me.get(\'orcid\')' in block
    assert block.count('**stamp') >= 3, 'add_cv, add_term and save_term all stamp'
