''' Editing the project map.

    project_map is the one collection here that NO program writes - it is
    maintained by hand - so a UI for it cannot be overwritten by a sync. What it
    can do is orphan tags: a row's "project" is live data, appearing as
    jrc_tag.name on real DOIs (1,186 for CellMap alone), while its "name" is a
    synonym that almost never is. That asymmetry is why there is no rename here.
'''

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / 'api' / 'dis_responder.py').read_text(encoding='utf-8')


def _block(start, end):
    seg = SOURCE[SOURCE.index(start):]
    return seg[:seg.index(end)]


HANDLER = _block('def project_edit()', "@app.route('/projects')")
VIEW = _block('def show_projects(', '@app.route(')


# --- what it will and will not do ------------------------------------------

def test_there_is_no_rename():
    ''' A project name is also a DOI tag. Renaming it here would orphan tags
        this page cannot retag, so renames belong to a script that can do both
        halves.
    '''
    assert "'rename'" not in HANDLER
    # Flattened first: the explanatory blurb is built by string concatenation,
    # so the phrase spans a literal boundary in the source.
    flat = re.sub(r'"\s*\\?\s*\n\s*\+\s*(f?)"', '', VIEW)
    assert 'name is also a DOI tag' in flat


def test_there_is_no_hard_delete():
    ''' doNotUse is already the retirement mechanism - 23 of 49 rows carry it -
        so a delete would be a second way to do the same thing with none of the
        reversibility.
    '''
    for forbidden in ('delete_one', 'delete_many', "'delete'"):
        assert forbidden not in HANDLER, forbidden


def test_retiring_is_reversible():
    assert "action in ('retire', 'restore')" in HANDLER
    assert '"$unset": {"doNotUse": ""}' in HANDLER


# --- who may write ---------------------------------------------------------

def test_the_write_endpoint_is_gated_and_the_page_is_not():
    ''' The mapping is a useful read for anyone, so /projects stays public and
        the writes get their own door - rather than per-section gating inside a
        public view.
    '''
    head = SOURCE[SOURCE.index("@app.route('/project_edit'"):SOURCE.index('def project_edit')]
    assert "@restricted('admin')" in head
    assert "methods=['POST']" in head
    page = SOURCE[SOURCE.index("@app.route('/projects')"):SOURCE.index('def show_projects')]
    assert '@restricted' not in page, '/projects must stay public'


def test_the_controls_are_hidden_from_non_admins():
    ''' Tidiness, not security: /project_edit enforces this independently. '''
    assert "may_edit = 'admin' in authorizations(current_user())" in VIEW
    assert 'if may_edit:' in VIEW


def test_every_handler_action_has_a_control():
    ''' An action with no control is dead code; a control with no action is a
        button that does nothing.
    '''
    literal = set(re.findall(r"name='action' value='(\w+)'", VIEW))
    dynamic = re.search(r"act, label = \('(\w+)', '\w+'\) if tag in retired \\\s*\n\s*"
                        r"else \('(\w+)'", VIEW)
    offered = literal | set(dynamic.groups() if dynamic else [])
    handled = set(re.findall(r"action == '(\w+)'", HANDLER))
    handled |= {'retire', 'restore'} if "action in ('retire', 'restore')" in HANDLER else set()
    assert handled <= offered, f'no control for: {handled - offered}'
    assert offered <= handled, f'no handler for: {offered - handled}'


# --- the cache -------------------------------------------------------------

def test_the_project_cache_is_refreshed_on_write():
    ''' PROJECT decides whether a name is a project on every request, so a
        stale cache misattributes authors.
    '''
    assert 'refresh_project()' in HANDLER


def test_the_project_cache_expires_like_the_vocabulary_one():
    ''' PROJECT had the same defect CVTERM did: loaded once, never reloaded,
        so an edit was invisible on every pod until a deploy.
    '''
    block = _block('def refresh_project', 'def refresh_cvterm')
    assert 'PROJECT.clear()' in block
    assert block.index('local = {}') < block.index('PROJECT.clear()')
    assert block.index('PROJECT.clear()') < block.index('PROJECT.update(local)')
    before = _block('def before_request', '\ndef ')
    assert 'refresh_project()' in before


def test_the_cache_leaves_out_retired_entries():
    ''' That is what makes doNotUse a retirement rather than a label. '''
    block = _block('def refresh_project', 'def refresh_cvterm')
    assert '{"doNotUse": {"$exists": False}}' in block


# --- details that bit during development -----------------------------------

def test_an_empty_collection_still_offers_the_add_form():
    ''' The empty branch used to replace html, discarding the form below it -
        so the first mapping could never be added.
    '''
    assert '"<p>No projects found</p>" + html' in VIEW


def test_a_write_redirects_rather_than_re_rendering():
    ''' A refresh would otherwise re-post it. The note travels in the session
        because it can contain the name the admin typed.
    '''
    assert "session['project_note']" in HANDLER
    assert 'return redirect(url_for(' in HANDLER
    assert "session.pop('project_note', '')" in VIEW


def test_a_name_is_mapped_only_once():
    assert 'find_one({"name": name})' in HANDLER
    assert 'is already mapped' in HANDLER


def test_synonyms_go_one_per_line_when_the_controls_are_shown():
    ''' Each synonym gains a "move to" box and two buttons for an admin, so a
        comma-joined list runs together. A reader with no controls keeps the
        compact form.
    '''
    assert 'joiner = "<br>" if may_edit else \', \'' in VIEW
    assert 'joiner.join(synonyms)' in VIEW
