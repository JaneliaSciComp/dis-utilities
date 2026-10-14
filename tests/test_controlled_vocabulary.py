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


def test_a_post_leaves_edit_mode():
    ''' Every form on the page posts to the current URL, which still carries
        ?edit=<name>. Honouring it on a POST put a just-saved row back into the
        editor, and reopened it after deleting an unrelated term.
    '''
    block = _block('def cvs(cv=None)', 'endpoint_access()')
    assert "request.args.get('edit') if request.method == 'GET' else None" in block


def test_the_edit_form_posts_to_the_clean_url():
    block = _block('def _cv_term_rows', '\n@app.route')
    assert "action='/cv/{escape(cv)}'" in block


def test_a_refused_create_does_not_navigate_to_the_existing_vocabulary():
    ''' The posted "cv" on an add_cv is the name being CREATED, not the one
        being viewed. Following it unconditionally landed a refused create on
        the vocabulary that already existed, which reads as success even though
        the warning was right there.
    '''
    block = _block('def cvs(cv=None)', 'endpoint_access()')
    assert "elif action == 'add_cv':" in block
    assert "if getattr(g, 'cv_change_ok', False):" in block


def test_the_handler_reports_whether_anything_was_written():
    save = _block('def _save_cv_change', '\ndef _cv_term_rows')
    assert 'g.cv_change_ok = False' in save
    assert 'g.cv_change_ok = True' in save
    assert save.index('g.cv_change_ok = False') < save.index('g.cv_change_ok = True')


# --- display name ----------------------------------------------------------

def test_a_display_name_is_required_and_never_defaults_to_the_name():
    ''' Defaulting to the name is right for a licence ("CC BY" is both) and
        wrong for the vocabulary that exists to give fields readable names - a
        blank would have rendered jrc_dataset_supplement_updated as a label.
        The form cannot tell which case it is in, so it asks.
    '''
    block = _block('def _save_cv_change', '\ndef _cv_term_rows')
    assert 'A display name is required.' in block
    assert "action in ('add_cv', 'add_term', 'save_term')" in block
    assert 'strip() or cv' not in block, 'add_cv must not fall back to the cv name'
    assert 'strip() or name' not in block, 'terms must not fall back to the term name'


def test_editing_cannot_silently_wipe_an_existing_display():
    ''' Different failure from the add case: on edit, clearing the box used to
        reset the display to the raw name rather than failing.
    '''
    block = _block("elif action == 'save_term':", "elif action == 'del_term':")
    assert '"display": display,' in block


def test_all_three_display_inputs_are_required():
    ''' The two add forms and the inline edit input. '''
    import re
    flat = re.sub(r'"\s*\\?\s*\n\s*\+\s*(f?)"', '', SOURCE)
    inputs = re.findall(r"<input[^>]*name='display'[^>]*>", flat)
    assert len(inputs) == 3, f'expected 3 display inputs, found {len(inputs)}'
    for tag in inputs:
        assert 'required' in tag, tag[:80]


def test_format_keeps_its_default():
    ''' Unlike display, format is a select with no empty option, so it cannot
        arrive blank from the form - the default only guards a forged POST.
    '''
    block = _block('def _save_cv_change', '\ndef _cv_term_rows')
    assert block.count("strip() or 'string'") == 2
