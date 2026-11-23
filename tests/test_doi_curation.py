''' Curating a DOI record from the web UI: the Curate tab and /doi_edit.

    The first editable field is jrc_newsletter, chosen because it is the one
    judgment field on a DOI that no nightly program recomputes: update_dois
    writes around it, update_tags --auto reads it and skips, nothing else
    touches it. It is also the "reviewed" mark - setting it freezes jrc_author
    against the sync - so a change here is a change in behavior, not a label,
    and it is recorded as such.

    The functions are extracted with ast rather than imported, because importing
    dis_responder needs Flask, a config file and a database, none of which these
    rules depend on.
'''

import ast
import re
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / 'api' / 'dis_responder.py').read_text(encoding='utf-8')
TEMPLATE = (ROOT / 'api' / 'templates' / 'doi.html').read_text(encoding='utf-8')


def _block(start, end):
    seg = SOURCE[SOURCE.index(start):]
    return seg[:seg.index(end)]


HANDLER = _block('def doi_edit(doi)', "@app.route('/pmidui/")
PANEL = _block('def curate_panel(', 'def _save_newsletter(')
SAVE = _block('def _save_newsletter(', "@app.route('/doi_edit/")
TABS = _block('def doi_tabs(', 'def show_doi_ui(')
SHOW = _block('def show_doi_ui(', 'def _date_text(')
GATE = _block('def restricted(arg=None)', "@app.route('/login'")
ACKTAG = _block('def _acktag_section(', 'def _save_ack_tag(')

# Parsed once: ast.parse of a 27,000-line file per test was most of the run.
_FUNCS = {n.name: n for n in ast.parse(SOURCE).body if isinstance(n, ast.FunctionDef)}


class _Collection:
    ''' Enough of a collection to record what was asked of it. '''
    def __init__(self):
        self.calls = []
    def update_one(self, query, update, upsert=False):
        self.calls.append((query, update, upsert))


def _ns(dois=None, processing=None, explode=False):
    ''' _save_newsletter and record_doi_event, wired to fake collections. '''
    dois = dois or _Collection()
    processing = processing or _Collection()
    if explode:
        def _boom(*_a, **_k):
            raise RuntimeError("mongo is down")
        dois.update_one = _boom
    ns = {'datetime': datetime, 're': re, '__version__': '1.2.3',
          'ACK_MAX_CHARS': 20000,
          'DB': {'dis': type('D', (), {'dois': dois, 'processing': processing})()},
          'print': lambda *a, **k: None,
          'render_warning': lambda msg, severity='error': f"[{severity}] {msg}"}
    ns['DERIVED_DEFAULT_TAGS'] = ('AI Initiative', 'Janelia Experimental Technology (jET)',
                                  'Scientific Computing Software')
    ns['AUTHOR_MATCHES'] = {'asserted': 'Janelia affiliation', 'ORCID': 'ORCID match',
                            'name': 'Name match'}
    ns['IRIS_CURATOR'] = 'IRIS'
    for name in ('_date_text', 'record_doi_event', '_save_newsletter',
                 '_ack_text', '_save_acknowledgements', '_derived_tags', '_save_tag',
                 '_author_rows', '_first_last_change', '_save_author',
                 '_acktag_in_text', '_ack_tag_change', '_save_ack_tag'):
        exec(ast.get_source_segment(SOURCE, _FUNCS[name]), ns)    # pylint: disable=W0122
    ns['_dois'] = dois
    ns['_processing'] = processing
    return ns


ME = {'orcid': '0000-0002-1825-0097', 'userIdO365': 'Curator@hhmi.org'}


# --- who may write ---------------------------------------------------------

def test_the_write_endpoint_needs_the_curator_term_and_the_page_does_not():
    ''' /doiui is the public record of a DOI and stays open; the write gets its
        own door. The term is "curator", not "admin": reviewing papers is a
        different job from granting access, and "admin" implies it anyway.
    '''
    head = SOURCE[SOURCE.index("@app.route('/doi_edit/"):SOURCE.index('def doi_edit')]
    assert "@restricted(CURATE_TERMS)" in head
    assert "methods=['POST']" in head
    assert "CURATOR_TERM = 'curator'" in SOURCE
    assert "ACK_TAG_TERM = 'ack_tag'" in SOURCE
    assert "CURATE_TERMS = (CURATOR_TERM, ACK_TAG_TERM)" in SOURCE
    page = SOURCE[SOURCE.index("@app.route('/doiui/<path:doi>')"):SOURCE.index('def show_doi_ui')]
    assert '@restricted' not in page, '/doiui must stay public'


def test_the_tab_is_shown_to_curators_and_admins_only():
    ''' Tidiness, not security: /doi_edit enforces this independently. The set
        intersection, not a bare term test, is what lets "admin" through when
        the cv failed to load - the same shape @restricted uses.
    '''
    assert "held = authorizations(current_user())" in TABS
    assert "({'admin', *CURATE_TERMS} & held)" in TABS
    assert "content['curate'] = curate_panel(doi, row, authors, curate_note, held)" in TABS


def test_the_tab_looks_like_a_write_control():
    ''' Every other tab shows the record; this one changes it. It carries the
        site's amber write color (the same WRITE_COLOR the save buttons use)
        and a pen, so it reads as what it is from across the room.
    '''
    assert "' tab-write' if key == 'curate'" in TABS
    assert 'fa-pen-to-square' in TABS
    css = (ROOT / 'api' / 'static' / 'css' / 'main.css').read_text(encoding='utf-8')
    assert '.nav-tabs .nav-link.tab-write' in css
    assert '.nav-tabs .nav-link.tab-write.active' in css
    write_color = re.search(r"WRITE_COLOR = '(#[0-9a-fA-F]{6})'", SOURCE).group(1)
    assert write_color in css


def test_the_tab_is_offered_only_for_a_record_we_hold():
    ''' When the DOI is not in the collection, show_doi_ui passes the
        registrar's payload as `row`; it has no Mongo id, and there is nothing
        to curate.
    '''
    assert "row.get('_id')" in TABS


# --- what the panel offers -------------------------------------------------

def test_every_handler_action_has_a_control():
    ''' An action with no control is dead code; a control with no action is a
        button that does nothing.
    '''
    offered = set(re.findall(r"name='action' value='(\w+)'", PANEL))
    handled = set(re.findall(r"action in \(([^)]+)\)", HANDLER))
    handled = {a.strip("' ") for group in handled for a in group.split(',')}
    dynamic = set(re.findall(r"act = '(\w+)' if arow\['credited'\] else '(\w+)'", PANEL))
    offered |= {a for pair in dynamic for a in pair}
    assert offered == handled == {'newsletter_set', 'newsletter_clear',
                                  'ack_set', 'ack_clear', 'acktag_add', 'acktag_remove',
                                  'tag_add', 'tag_remove', 'author_add', 'author_remove'}


def test_clear_is_offered_only_when_there_is_something_to_clear():
    assert PANEL.index('if current:') < PANEL.index("value='newsletter_clear'")
    assert PANEL.index('if ack:') < PANEL.index("value='ack_clear'")


def test_the_textarea_holds_the_current_text_escaped():
    ''' Harvested text is third-party content from publishers' HTML and PDFs;
        a stray "</textarea>" in it would otherwise end the control.
    '''
    assert '{escape(ack)}</textarea>' in PANEL


def test_the_panel_says_what_the_date_really_does():
    ''' Setting it is not labelling: update_dois stops adding authors and
        update_tags --auto skips the record. A curator should read that beside
        the button, not discover it.
    '''
    assert 'reviewed' in PANEL
    assert 'stops adding' in PANEL
    assert 'Processing tab' in PANEL


# --- the save --------------------------------------------------------------

def test_set_writes_the_same_string_the_cli_writes():
    ''' update_tags.py and add_newsletter.py store "YYYY-MM-DD" text, and the
        nightly programs test the field's presence; a datetime here would be a
        second shape for one field.
    '''
    ns = _ns()
    note = ns['_save_newsletter']({'doi': '10.1/x'}, ME, 'newsletter_set', '2025-03-04')
    (query, update, _), = ns['_dois'].calls
    assert query == {'doi': '10.1/x'}
    assert update == {'$set': {'jrc_newsletter': '2025-03-04'}}
    assert note.startswith('[success]')


def test_clear_unsets_rather_than_blanking():
    ''' The nightly programs test for the field's existence
        ({"$exists": False}); an empty string would still count as reviewed.
    '''
    ns = _ns()
    ns['_save_newsletter']({'doi': '10.1/x', 'jrc_newsletter': '2025-03-04'}, ME,
                           'newsletter_clear', None)
    (_, update, _), = ns['_dois'].calls
    assert update == {'$unset': {'jrc_newsletter': ''}}


def test_a_malformed_date_writes_nothing():
    ns = _ns()
    for bad in ('', '04/03/2025', 'yesterday', '2025-02-30', '2025-03-04T00:00'):
        note = ns['_save_newsletter']({'doi': '10.1/x'}, ME, 'newsletter_set', bad)
        assert note.startswith('[warning]'), bad
    assert not ns['_dois'].calls
    assert not ns['_processing'].calls


def test_the_stored_date_is_always_zero_padded():
    ''' strptime accepts "2025-3-4". Stored as typed, the field would hold two
        shapes, and the string comparisons the nightly programs and this page
        make would treat the same day as two different dates.
    '''
    ns = _ns()
    ns['_save_newsletter']({'doi': '10.1/x'}, ME, 'newsletter_set', ' 2025-3-4 ')
    (_, update, _), = ns['_dois'].calls
    assert update == {'$set': {'jrc_newsletter': '2025-03-04'}}


def test_a_no_op_writes_nothing():
    ''' Setting the date it already has, or clearing one that is not there,
        would otherwise leave an audit event that records no change.
    '''
    ns = _ns()
    row = {'doi': '10.1/x', 'jrc_newsletter': '2025-03-04'}
    assert ns['_save_newsletter'](row, ME, 'newsletter_set', '2025-03-04').startswith('[warning]')
    assert ns['_save_newsletter']({'doi': '10.1/x'}, ME, 'newsletter_clear', None) \
        .startswith('[warning]')
    assert not ns['_dois'].calls
    assert not ns['_processing'].calls


def test_a_stored_datetime_compares_as_a_date():
    ''' A value written by hand could be a datetime; it must still read as the
        same day, or "set to today" on such a record would be refused as a
        no-op one moment and recorded as a change the next.
    '''
    ns = _ns()
    row = {'doi': '10.1/x', 'jrc_newsletter': datetime(2025, 3, 4, 13, 45)}
    assert ns['_save_newsletter'](row, ME, 'newsletter_set', '2025-03-04').startswith('[warning]')
    assert not ns['_dois'].calls


def test_only_the_one_field_changes():
    ''' jrc_updated means "re-read from the registrar", which this is not; and
        nothing else on the record is this change's business.
    '''
    ns = _ns()
    ns['_save_newsletter']({'doi': '10.1/x'}, ME, 'newsletter_set', '2025-03-04')
    (_, update, _), = ns['_dois'].calls
    touched = {k for op in update.values() for k in op}
    assert touched == {'jrc_newsletter'}


def test_a_failed_write_is_reported_and_leaves_no_audit_event():
    ns = _ns(explode=True)
    note = ns['_save_newsletter']({'doi': '10.1/x'}, ME, 'newsletter_set', '2025-03-04')
    assert note.startswith('[error]')
    assert not ns['_processing'].calls


# --- the audit trail -------------------------------------------------------

def test_the_event_lands_in_the_doi_s_own_processing_record():
    ''' Same key and shape as doi_common.add_doi_process, so the Processing tab
        shows a curator's change in the same table as the nightly runs.
    '''
    ns = _ns()
    ns['_save_newsletter']({'doi': '10.1/x', 'jrc_newsletter': '2025-01-02'}, ME,
                           'newsletter_set', '2025-03-04')
    (query, update, upsert), = ns['_processing'].calls
    assert query == {'type': 'doi', 'key': '10.1/x'}
    assert upsert is True
    event = update['$push']['processes']
    assert event['action'] == 'newsletter_set'
    assert event['program'] == 'dis_responder.py'
    assert event['version'] == '1.2.3'
    assert event['notes'] == '2025-01-02 → 2025-03-04'
    datetime.fromisoformat(event['timestamp'])


def test_the_event_names_the_person_the_way_the_tab_resolves_it():
    ''' The Processing tab translates "user" through user_display_name, which
        is keyed by the login id (local part of userIdO365, lowercased). The
        ORCID iD travels too, as "actor", for anyone reading the raw record.
    '''
    ns = _ns()
    ns['_save_newsletter']({'doi': '10.1/x'}, ME, 'newsletter_set', '2025-03-04')
    (_, update, _), = ns['_processing'].calls
    event = update['$push']['processes']
    assert event['user'] == 'curator'
    assert event['actor'] == ME['orcid']


def test_a_person_without_a_login_id_is_still_named():
    ns = _ns()
    ns['record_doi_event']('10.1/x', 'newsletter_set', {'orcid': 'X-1'}, '')
    (_, update, _), = ns['_processing'].calls
    assert update['$push']['processes']['user'] == 'X-1'


def test_clearing_records_what_was_cleared():
    ns = _ns()
    ns['_save_newsletter']({'doi': '10.1/x', 'jrc_newsletter': '2025-03-04'}, ME,
                           'newsletter_clear', None)
    (_, update, _), = ns['_processing'].calls
    event = update['$push']['processes']
    assert event['action'] == 'newsletter_cleared'
    assert event['notes'] == '2025-03-04 → not set'


def test_a_failed_audit_write_does_not_fail_the_change():
    ''' A lost event is bad; a save that reports failure after succeeding is
        worse, and the audit runs after the write it describes.
    '''
    processing = _Collection()
    def _boom(*_a, **_k):
        raise RuntimeError("mongo is down")
    processing.update_one = _boom
    ns = _ns(processing=processing)
    note = ns['_save_newsletter']({'doi': '10.1/x'}, ME, 'newsletter_set', '2025-03-04')
    assert note.startswith('[success]')
    assert len(ns['_dois'].calls) == 1


# --- acknowledgement text --------------------------------------------------

ACK = 'We thank the Janelia Light Microscopy facility.\n\nFunded by HHMI.'


def test_adding_text_marks_it_manual_and_clears_the_no_ack_marker():
    ''' add_acknowledgement.py sets the source only on external_dois, so until
        now hand-entered text on a dois record read as harvested. And a record
        that says both "here is the text" and "there is no text" (the
        harvester's jrc_no_acknowledgements) contradicts itself.
    '''
    ns = _ns()
    row = {'doi': '10.1/x', 'jrc_no_acknowledgements': True}
    note = ns['_save_acknowledgements'](row, ME, 'ack_set', ACK)
    (query, update, _), = ns['_dois'].calls
    assert query == {'doi': '10.1/x'}
    assert update == {'$set': {'jrc_acknowledgements': ACK, 'jrc_ack_source': 'Manual'},
                      '$unset': {'jrc_no_acknowledgements': ''}}
    assert note.startswith('[success]')


def test_clearing_drops_the_text_its_source_and_the_no_ack_marker():
    ''' A source with no text would label nothing. The harvester's queries
        require BOTH the text field and jrc_no_acknowledgements to be absent,
        so a clear that left the marker behind (a pull script can write text
        over it) would promise "may fill it again" and never deliver.
    '''
    ns = _ns()
    row = {'doi': '10.1/x', 'jrc_acknowledgements': ACK, 'jrc_ack_source': 'PMC',
           'jrc_no_acknowledgements': True}
    note = ns['_save_acknowledgements'](row, ME, 'ack_clear', None)
    (_, update, _), = ns['_dois'].calls
    assert update == {'$unset': {'jrc_acknowledgements': '', 'jrc_ack_source': '',
                                 'jrc_no_acknowledgements': ''}}
    assert 'harvester may fill it again' in note


def test_saving_an_untouched_textarea_is_a_no_op():
    ''' Harvested text often carries PDF spacing that _ack_text tidies, so a
        Save with nothing changed used to count as an edit and relabel the
        harvester's text as Manual.
    '''
    ns = _ns()
    row = {'doi': '10.1/x', 'jrc_acknowledgements': 'We  thank   X.\n\n\nFunded by  Y.',
           'jrc_ack_source': 'PMC'}
    note = ns['_save_acknowledgements'](row, ME, 'ack_set', row['jrc_acknowledgements'])
    assert note.startswith('[warning]') and not ns['_dois'].calls


def test_the_replaced_text_travels_with_the_event_in_full():
    ''' Restorable from the Processing record: the notes say how long it was
        and where it came from, the event itself holds every character.
    '''
    ns = _ns()
    row = {'doi': '10.1/x', 'jrc_acknowledgements': ACK, 'jrc_ack_source': 'PMC'}
    ns['_save_acknowledgements'](row, ME, 'ack_set', 'Thanks all.')
    (_, update, _), = ns['_processing'].calls
    event = update['$push']['processes']
    assert event['action'] == 'acknowledgements_set'
    assert event['previous'] == ACK
    assert event['previous_source'] == 'PMC'
    assert event['notes'] == f'replaced {len(ACK)} characters from PMC with 11 characters'
    ns = _ns()
    ns['_save_acknowledgements'](row, ME, 'ack_clear', None)
    (_, update, _), = ns['_processing'].calls
    event = update['$push']['processes']
    assert event['action'] == 'acknowledgements_cleared'
    assert event['previous'] == ACK
    assert event['notes'] == f'cleared {len(ACK)} characters from PMC'


def test_a_first_add_carries_no_previous_text():
    ns = _ns()
    ns['_save_acknowledgements']({'doi': '10.1/x'}, ME, 'ack_set', ACK)
    (_, update, _), = ns['_processing'].calls
    event = update['$push']['processes']
    assert 'previous' not in event
    assert event['notes'] == f'added {len(ACK)} characters'


def test_pasted_text_keeps_its_paragraphs_but_not_its_pdf_spacing():
    ''' Unlike add_acknowledgement.py, which flattens to one line: the
        Acknowledgements tab renders a newline as a line break, so paragraphs
        pasted here should survive. Runs of spaces and blank lines do not.
    '''
    tidy = _ns()['_ack_text']
    assert tidy('  We  thank   X. \r\n\r\n\r\n\n  Funded by   Y.  \n') \
        == 'We thank X.\n\nFunded by Y.'
    assert tidy('one\ntwo') == 'one\ntwo'
    assert tidy(None) == ''
    assert tidy('  \n \n ') == ''


def test_empty_oversized_and_unchanged_text_write_nothing():
    ns = _ns()
    row = {'doi': '10.1/x', 'jrc_acknowledgements': ACK, 'jrc_ack_source': 'Manual'}
    assert ns['_save_acknowledgements'](row, ME, 'ack_set', '   ').startswith('[warning]')
    assert ns['_save_acknowledgements'](row, ME, 'ack_set', 'x' * 20001).startswith('[warning]')
    assert ns['_save_acknowledgements'](row, ME, 'ack_set', f'  {ACK}  ').startswith('[warning]')
    assert ns['_save_acknowledgements']({'doi': '10.1/x'}, ME, 'ack_clear', None) \
        .startswith('[warning]')
    assert not ns['_dois'].calls
    assert not ns['_processing'].calls


def test_an_empty_box_does_not_clear():
    ''' Clearing is its own button. Submitting an emptied textarea is more
        often a slip than a decision, so it is refused with directions.
    '''
    ns = _ns()
    row = {'doi': '10.1/x', 'jrc_acknowledgements': ACK}
    note = ns['_save_acknowledgements'](row, ME, 'ack_set', '')
    assert 'Use Clear' in note
    assert not ns['_dois'].calls


# --- tags ------------------------------------------------------------------

SUPORGS = {'Cardona Lab': {'code': 'JRC-CL', 'active': True},
           'Scientific Computing Software': {'code': 'JRC-SCS', 'active': True},
           'Connectomics Team': {'code': 'JRC-CT', 'active': True},
           'Old Team': {'code': 'JRC-OLD'}}
# What a curator may ADD: live projects only.
PROJECTS = {'CellMap', 'MouseLight'}
# What the nightly programs tag BY: both sides of project_map, synonyms and
# retired rows included, as DL.get_project_map(inactive=True) returns it.
PROJECT_MAP = {'CellMap': 'CellMap', 'MouseLight': 'MouseLight',
               'MouseLight Project Team': 'MouseLight',
               'GENIE Project Team': 'GENIE Project', 'GENIE Project': 'GENIE Project'}
AUTHORS = [{'family': 'Cardona', 'given': 'Albert', 'janelian': True, 'group': 'Cardona Lab',
            'tags': ['Cardona Lab', 'Old Team', 'Group Leader/Lab Head'],
            'managedTeams': [{'supOrgName': 'Cardona Lab', 'supOrgSubType': 'Lab'},
                             {'supOrgName': 'Connectomics Team', 'supOrgSubType': 'Team'}]},
           {'family': 'Bock', 'given': 'Davi', 'janelian': True,
            'tags': ['AI Initiative']},
           {'name': 'CellMap'}]


def _tags(row, action, name, derived=None):
    ns = _ns()
    derived = ns['_derived_tags'](AUTHORS, PROJECT_MAP, SUPORGS) if derived is None else derived
    note = ns['_save_tag'](row, ME, action, name, derived, SUPORGS, PROJECTS)
    return ns, note


def test_the_default_tag_list_matches_update_dois():
    ''' update_dois.DEFAULT_TAGS is what the nightly sync applies from an
        author's affiliation list; the panel's copy decides which tags it calls
        fixed. If they drift, the panel either lets a curator remove a tag the
        sync restores, or freezes one it would not.
    '''
    def constant(source, name):
        # Module-level assignments include attribute and subscript targets
        # (app.config[...] = ...), which have no .id; getattr skips them.
        return next(ast.literal_eval(n.value) for n in ast.parse(source).body
                    if isinstance(n, ast.Assign)
                    and getattr(n.targets[0], 'id', None) == name)
    ud = (ROOT / 'sync' / 'bin' / 'update_dois.py').read_text(encoding='utf-8')
    assert set(constant(SOURCE, 'DERIVED_DEFAULT_TAGS')) == set(constant(ud, 'DEFAULT_TAGS'))


def test_derived_tags_are_what_the_nightly_programs_would_apply():
    ''' Group (the lab), non-lab managed teams, default tags, ACTIVE suporgs in
        the affiliation list, and the project an organizational author maps
        to. Not: labs an author merely lists, inactive orgs, or the
        "Group Leader/Lab Head" pseudo-tag.
    '''
    derived = _ns()['_derived_tags'](AUTHORS, PROJECT_MAP, SUPORGS)
    assert derived == {'Cardona Lab': ['Cardona'],
                       'Connectomics Team': ['Cardona'],
                       'AI Initiative': ['Bock'],
                       'CellMap': ['CellMap']}
    assert _ns()['_derived_tags'](None, {}, {}) == {}


def test_a_project_derived_through_a_synonym_counts_as_derived():
    ''' update_dois tags an organizational author "GENIE Project Team" as
        "GENIE Project" through project_map. The first version checked the
        author's name against the project names alone, so that tag read as
        "added by hand" with a Remove button - and the nightly sync put it
        back. Live data has 23 such synonyms on thousands of DOIs.
    '''
    derived = _ns()['_derived_tags']([{'name': 'GENIE Project Team'},
                                      {'name': 'MouseLight Project Team'}],
                                     PROJECT_MAP, SUPORGS)
    assert derived == {'GENIE Project': ['GENIE Project Team'],
                       'MouseLight': ['MouseLight Project Team']}


def test_the_write_rule_is_narrower_than_the_label_rule():
    ''' The label is generous (anything either nightly program COULD put
        back). The write made when crediting an author must not be: an active
        affiliation in the roster's cumulative list that update_dois would
        never add would otherwise be written and then called fixed.
    '''
    author = [{'family': 'Cardona', 'group': 'Cardona Lab',
               'tags': ['Connectomics Team', 'Scientific Computing Software'],
               'managedTeams': [{'supOrgName': 'Connectomics Team', 'supOrgSubType': 'Team'}]}]
    label = _ns()['_derived_tags'](author, PROJECT_MAP, SUPORGS)
    write = _ns()['_derived_tags'](author, PROJECT_MAP, SUPORGS, strict=True)
    assert set(label) == {'Cardona Lab', 'Connectomics Team', 'Scientific Computing Software'}
    assert set(write) == {'Cardona Lab', 'Scientific Computing Software'}


def test_a_derived_tag_cannot_be_removed_and_says_why():
    row = {'doi': '10.1/x', 'jrc_tag': [{'name': 'Cardona Lab', 'code': {}, 'type': 'suporg'}]}
    ns, note = _tags(row, 'tag_remove', 'Cardona Lab')
    assert note.startswith('[warning]')
    assert 'Cardona' in note and 'nightly sync' in note
    # The nightly programs derive from every MATCHED author, credited or not,
    # so the refusal must not send the curator to change a credit.
    assert 'credit' not in note
    assert not ns['_dois'].calls


def test_a_hand_added_tag_can_be_removed():
    row = {'doi': '10.1/x', 'jrc_tag': [{'name': 'Cardona Lab', 'code': {}, 'type': 'suporg'},
                                       {'name': 'MouseLight', 'code': None,
                                        'type': 'affiliation'}]}
    ns, note = _tags(row, 'tag_remove', 'MouseLight')
    assert note.startswith('[success]')
    (_, update, _), = ns['_dois'].calls
    assert update == {'$pull': {'jrc_tag': {'name': 'MouseLight'}}}
    (_, event, _), = ns['_processing'].calls
    assert event['$push']['processes']['action'] == 'tag_removed'


def test_removing_the_last_tag_unsets_the_field():
    ''' The nightly programs test for the field's presence; an empty list
        would read as "tagged, with nothing".
    '''
    row = {'doi': '10.1/x', 'jrc_tag': [{'name': 'MouseLight', 'code': None,
                                        'type': 'affiliation'}]}
    ns, _ = _tags(row, 'tag_remove', 'MouseLight')
    assert [u for _, u, _ in ns['_dois'].calls] == [
        {'$pull': {'jrc_tag': {'name': 'MouseLight'}}},
        {'$unset': {'jrc_tag': ''}}]


def test_adding_builds_the_entry_the_way_update_tags_does():
    ''' A supervisory organization gets type "suporg" and the {code, active}
        dict; anything else that is a project gets "affiliation" and no code.
    '''
    ns, note = _tags({'doi': '10.1/x'}, 'tag_add', 'Scientific Computing Software')
    assert note.startswith('[success]')
    (_, update, _), = ns['_dois'].calls
    assert update == {'$push': {'jrc_tag': {'name': 'Scientific Computing Software',
                                            'code': {'code': 'JRC-SCS', 'active': True},
                                            'type': 'suporg'}}}
    ns, _ = _tags({'doi': '10.1/x'}, 'tag_add', ' MouseLight ')
    (_, update, _), = ns['_dois'].calls
    assert update == {'$push': {'jrc_tag': {'name': 'MouseLight', 'code': None,
                                            'type': 'affiliation'}}}


def test_a_name_outside_the_vocabulary_is_refused():
    ''' A typo must not create a tag: the vocabulary is the suporg collection
        plus the projects in project_map, and nothing else.
    '''
    # 'Old Team' is in the suporg collection but not active: the pick-list does
    # not offer it, and a datalist is a suggestion, so the server has to apply
    # the same rule to a typed name.
    for bad in ('Cardonna Lab', '', '  ', 'Group Leader/Lab Head', 'Old Team'):
        ns, note = _tags({'doi': '10.1/x'}, 'tag_add', bad)
        assert note.startswith('[warning]'), bad
        assert not ns['_dois'].calls


def test_removing_a_duplicated_tag_unsets_the_field():
    ''' $pull takes every entry with the name; judging "last one" by list
        length alone left jrc_tag: [] when the name was in twice.
    '''
    row = {'doi': '10.1/x', 'jrc_tag': [{'name': 'MouseLight', 'code': None, 'type': 'affiliation'},
                                       {'name': 'MouseLight', 'code': None, 'type': 'affiliation'}]}
    ns, _ = _tags(row, 'tag_remove', 'MouseLight')
    assert [u for _, u, _ in ns['_dois'].calls][-1] == {'$unset': {'jrc_tag': ''}}


def test_adding_a_tag_does_not_need_the_author_lookup():
    ''' Author details come from OpenAlex and PubMed as well as the roster; an
        add has nothing to derive and must not fail because one is down.
    '''
    block = _block('def _save_tag_action(', 'ACK_MAX_CHARS')
    assert block.index("if action == 'tag_remove':") < block.index('DL.get_author_details')


def test_duplicates_and_absent_tags_are_no_ops():
    row = {'doi': '10.1/x', 'jrc_tag': [{'name': 'MouseLight', 'code': None,
                                        'type': 'affiliation'}]}
    ns, note = _tags(row, 'tag_add', 'MouseLight')
    assert 'already' in note and not ns['_dois'].calls
    ns, note = _tags(row, 'tag_remove', 'CellMap')
    assert 'not a tag on this DOI' in note and not ns['_dois'].calls


def test_the_derived_set_is_recomputed_on_the_server():
    ''' The "fixed" label on the page protects nothing on its own; a hand-made
        POST must meet the same rule, so the handler derives the set itself
        rather than trusting anything the form sends.
    '''
    block = _block('def _save_tag_action(', "@app.route('/doi_edit/")
    assert 'DL.get_author_details(row' in block
    assert '_derived_tags(authors, project_map, suporgs)' in block
    assert "request.form.get('name')" in block


def test_the_panel_offers_only_active_orgs_and_projects_to_add():
    assert "(v or {}).get('active')" in PANEL
    assert "list='tag-choices'" in PANEL


def test_the_vocabulary_reads_orgs_from_the_collection():
    ''' The same rule tests/test_tag_code_shape.py pins on the scripts: the
        collection form gives the {code, active} dict, the HQ-API form a bare
        string, and jrc_tag[].code is written straight from it. (That test
        cannot list dis_responder.py as a whole, which also uses the API form
        where it genuinely compares HQ against Mongo.)
    '''
    block = _block('def _tag_vocabulary(', 'def _derived_tags(')
    assert "DL.get_supervisory_orgs(coll=DB['dis'].suporg)" in block
    # The derivation map carries synonyms and retired rows, as update_dois
    # reads them; the addable set carries live projects only.
    assert "project_map[rec['name']] = rec['project']" in block
    assert "if not rec.get('doNotUse'):" in block


# --- author credits --------------------------------------------------------

PAPER = [
    # position 1, asserted affiliation, on the roster with an ID
    {'given': 'Albert', 'family': 'Cardona', 'employeeId': 'E1', 'match': 'asserted',
     'asserted': True, 'janelian': True, 'alumni': False, 'is_first': True,
     'group': 'Cardona Lab', 'orcid': '0000-0001-0000-0001'},
    # name match, roster knows them but has no employee ID
    {'given': 'Pat', 'family': 'Noid', 'match': 'name', 'janelian': True, 'alumni': False,
     'userIdO365': 'noidp@hhmi.org'},
    # no evidence at all
    {'given': 'Someone', 'family': 'Else', 'match': None, 'janelian': False, 'alumni': False},
    # ORCID match, former employee, last position
    {'given': 'Davi', 'family': 'Bock', 'employeeId': 'E4', 'match': 'ORCID', 'janelian': True,
     'alumni': True, 'is_last': True},
    # no evidence now, but already credited
    {'given': 'Ghost', 'family': 'Credit', 'employeeId': 'E5', 'match': None,
     'janelian': False, 'alumni': False},
]


def _authors(row, action, eid, lookup=None):
    ns = _ns()
    note = ns['_save_author'](row, ME, action, eid, PAPER, SUPORGS, PROJECT_MAP, lookup)
    return ns, note


def test_eligibility_needs_evidence_and_an_employee_id():
    ''' The two rules: a match (affiliation, ORCID or name) to be listed at all,
        and an employee ID to be creditable. Without the ID the row is shown
        but cannot be acted on, with the reason.
    '''
    rows, unlisted = _ns()['_author_rows'](PAPER, ['E5'])
    by = {r['who']: r for r in rows}
    assert unlisted == 1, 'Someone Else has no evidence and is not credited'
    assert by['Albert Cardona']['eligible'] and by['Albert Cardona']['evidence'] == 'Janelia affiliation'
    assert not by['Pat Noid']['eligible'] and 'No employee ID' in by['Pat Noid']['why']
    assert by['Pat Noid']['link'] == 'noidp@hhmi.org'
    assert by['Davi Bock']['eligible'] and by['Davi Bock']['alumni']
    # A credit with no remaining evidence is listed, and removable, because
    # that is exactly the row a curator needs to see.
    assert by['Ghost Credit']['credited'] and by['Ghost Credit']['eligible']
    assert by['Ghost Credit']['evidence'] == 'none'
    assert [r['who'] for r in rows] == ['Albert Cardona', 'Pat Noid', 'Davi Bock', 'Ghost Credit']


def test_a_credit_matching_no_author_gets_a_row_named_from_the_roster():
    ''' The author list changed with a new version, or a CLI credit used the
        wrong ID: the employee ID is in jrc_author but no author on the paper
        carries it. Without a row it could not be seen or uncredited here.
    '''
    roster = {'E9': {'given': ['Orphan', 'O.'], 'family': ['Credit'], 'orcid': '0000-9'}}
    rows, _ = _ns()['_author_rows'](PAPER, ['E1', 'E9'], roster.get)
    orphan = rows[-1]
    assert orphan == {'eid': 'E9', 'who': 'Orphan Credit', 'link': '0000-9',
                      'evidence': 'not on this paper’s author list', 'eligible': True,
                      'why': '', 'credited': True, 'alumni': False}
    # Unknown to the roster too: still a row, named by the ID.
    rows, _ = _ns()['_author_rows'](PAPER, ['E9'], lambda eid: None)
    assert rows[-1]['who'] == 'employee E9'
    # And it can be uncredited.
    ns, note = _authors({'doi': '10.1/x', 'jrc_author': ['E1', 'E9']}, 'author_remove', 'E9',
                        roster.get)
    assert note.startswith('[success]')
    (_, update, _), = ns['_dois'].calls
    assert update['$pull'] == {'jrc_author': 'E9'}


def test_a_person_listed_twice_gets_one_row():
    twice = PAPER[:1] + [dict(PAPER[0], is_first=False, is_last=True)]
    rows, _ = _ns()['_author_rows'](twice, [])
    assert [r['who'] for r in rows] == ['Albert Cardona']


def test_the_panel_disables_rather_than_hides_an_ineligible_author():
    assert "if not arow['eligible']:" in PANEL
    assert 'disabled' in PANEL and "title='{escape(arow['why'])}'" in PANEL


def test_crediting_adds_the_id_recomputes_first_last_and_adds_derived_tags():
    ''' After a credit the record should be what the nightly sync would make
        it, not inconsistent until tomorrow: first/last author from position
        intersected with the new list, and the author's lab tag present.
    '''
    ns, note = _authors({'doi': '10.1/x'}, 'author_add', 'E1')
    assert note.startswith('[success]')
    (_, update, _), = ns['_dois'].calls
    assert update['$addToSet'] == {'jrc_author': 'E1'}
    assert update['$set'] == {'jrc_first_id': ['E1'], 'jrc_first_author': 'Cardona, Albert'}
    assert update['$unset'] == {'jrc_last_id': '', 'jrc_last_author': ''}
    assert update['$push'] == {'jrc_tag': {'$each': [
        {'name': 'Cardona Lab', 'code': {'code': 'JRC-CL', 'active': True}, 'type': 'suporg'}]}}
    (_, event, _), = ns['_processing'].calls
    assert event['$push']['processes']['action'] == 'author_credited'
    assert event['$push']['processes']['notes'] == \
        'credited Albert Cardona (Janelia affiliation); added tags Cardona Lab'


def test_a_credit_never_duplicates_a_tag_the_record_has():
    row = {'doi': '10.1/x', 'jrc_tag': [{'name': 'Cardona Lab', 'code': {}, 'type': 'suporg'}]}
    ns, _ = _authors(row, 'author_add', 'E1')
    (_, update, _), = ns['_dois'].calls
    assert '$push' not in update


def test_the_audit_names_the_person_not_the_employee_id():
    ''' The Processing tab is public; the employee ID is the identifier the
        API redacts from unauthorized callers.
    '''
    ns, _ = _authors({'doi': '10.1/x'}, 'author_add', 'E1')
    (_, event, _), = ns['_processing'].calls
    assert 'E1' not in event['$push']['processes']['notes']


def test_credits_outside_the_rules_are_refused():
    for eid, why in (('E2', 'unknown id'), ('', 'empty'), ('E9', 'not on the paper')):
        ns, note = _authors({'doi': '10.1/x'}, 'author_add', eid)
        assert note.startswith('[warning]'), why
        assert not ns['_dois'].calls
    ns, note = _authors({'doi': '10.1/x', 'jrc_author': ['E1']}, 'author_add', 'E1')
    assert 'already credited' in note and not ns['_dois'].calls


def test_uncrediting_pulls_the_id_and_recomputes_first_last():
    row = {'doi': '10.1/x', 'jrc_author': ['E1', 'E4'], 'jrc_newsletter': '2025-01-01',
           'jrc_first_id': ['E1'], 'jrc_last_id': 'E4'}
    ns, note = _authors(row, 'author_remove', 'E4')
    assert note.startswith('[success]')
    (_, update, _), = ns['_dois'].calls
    assert update == {'$pull': {'jrc_author': 'E4'},
                      '$set': {'jrc_first_id': ['E1'], 'jrc_first_author': 'Cardona, Albert'},
                      '$unset': {'jrc_last_id': '', 'jrc_last_author': ''}}
    assert 'nightly sync' not in note, 'reviewed record: no re-add warning'


def test_uncrediting_on_an_unreviewed_record_warns_about_the_nightly_sync():
    ''' update_dois re-credits any Janelian it detects unless jrc_newsletter
        is set. The note has to say so, or the curator's change evaporates
        overnight with nothing to explain why.
    '''
    row = {'doi': '10.1/x', 'jrc_author': ['E1', 'E4']}
    _, note = _authors(row, 'author_remove', 'E4')
    assert note.startswith('[success]') and 'newsletter date' in note
    # A credit with no detector evidence will not be re-added, so no warning.
    _, note = _authors({'doi': '10.1/x', 'jrc_author': ['E1', 'E5']}, 'author_remove', 'E5')
    assert note.startswith('[success]') and 'newsletter date' not in note


def test_uncrediting_the_last_author_unsets_the_field():
    row = {'doi': '10.1/x', 'jrc_author': ['E1']}
    ns, _ = _authors(row, 'author_remove', 'E1')
    assert [u for _, u, _ in ns['_dois'].calls] == [
        {'$pull': {'jrc_author': 'E1'},
         '$unset': {'jrc_first_id': '', 'jrc_first_author': '',
                    'jrc_last_id': '', 'jrc_last_author': ''}},
        {'$unset': {'jrc_author': ''}}]


def test_first_last_follow_update_dois_rules():
    ''' Position in the registrar record intersected with the credit list;
        co-first authors make jrc_first_id a list; nothing behind a field means
        it is unset, not left stale.
    '''
    fl = _ns()['_first_last_change']
    co = [{'employeeId': 'A', 'given': 'X', 'family': 'Ay', 'is_first': True},
          {'employeeId': 'B', 'given': 'Y', 'family': 'Bee', 'is_first': True},
          {'employeeId': 'C', 'given': 'Z', 'family': 'Cee', 'is_last': True}]
    assert fl(co, ['A', 'B', 'C']) == ({'jrc_first_id': ['A', 'B'],
                                        'jrc_first_author': 'Ay, X, Bee, Y',
                                        'jrc_last_id': 'C', 'jrc_last_author': 'Cee, Z'}, {})
    assert fl(co, ['B']) == ({'jrc_first_id': ['B'], 'jrc_first_author': 'Bee, Y'},
                             {'jrc_last_id': '', 'jrc_last_author': ''})
    assert fl(co, []) == ({}, {'jrc_first_id': '', 'jrc_first_author': '',
                               'jrc_last_id': '', 'jrc_last_author': ''})


def test_the_token_api_merges_rather_than_rewriting_jrc_author():
    ''' POST /doi/jrc_author used to $set a from-scratch list, which would
        have wiped any credit added on /doiui. It now starts from what is
        stored and only appends, as update_dois.persist_author does.
    '''
    body = _block('def set_jrc_author(doi)', "@app.route('/raw/")
    assert "jrc_author = list(row.get('jrc_author') or [])" in body
    assert "jrc_author = []" not in body
    assert "auth['employeeId'] not in jrc_author" in body
    assert 'Never removes' in body


# --- the round trip --------------------------------------------------------

def test_a_write_redirects_back_to_the_curate_tab():
    ''' A refresh would otherwise re-post it. The note travels in the session,
        and the fragment reopens the tab so the note is seen rather than hidden
        behind the Authors tab - the template turns a fragment into a click.
    '''
    assert 'session[\'doi_note\'] = {"doi": doi, "html": str(note)' in HANDLER
    assert "url_for('show_doi_ui', doi=doi) + '#curate'" in HANDLER
    assert "getElementById(hash.slice(1) + '-tab')" in TEMPLATE


def test_a_note_never_outlives_the_page_load_that_follows_it():
    ''' The note used to be popped inside curate_panel, which runs only when
        the DOI is held, the registrar answered, and the viewer is still a
        curator. On any other path (a Crossref timeout is the common one) it
        stayed in the cookie and surfaced on the next DOI the curator opened -
        a success message about record A, shown on record B. Now show_doi_ui
        pops it first thing, on every path, and keeps it only for its own DOI.
    '''
    assert "session.pop('doi_note'" not in PANEL
    assert "pending = session.pop('doi_note', None)" in SHOW
    assert "pending.get('doi') != doi" in SHOW
    # It is popped before the first early return.
    assert SHOW.index("session.pop('doi_note'") < SHOW.index('return ')
    # A DOI deleted between the page load and the click: no Curate tab to hold
    # the note, so it is shown above the banner rather than dropped.
    assert "if pending and not row:" in SHOW
    assert "recsec = pending['html'] + recsec" in SHOW
    assert "doi_tabs(doi, row, rowext, data, authors, pending)" in SHOW


def test_the_section_acted_on_reopens_with_the_note_inside_it():
    ''' The sections fold to one status line each. After a change, the one the
        action belonged to is the one open, holding the note, so the round
        trip lands where the curator was. The key is the action's prefix, so
        every action maps to a section without a table to maintain.
    '''
    assert '"section": action.split(\'_\', 1)[0]' in HANDLER
    assert "opened = pending.get('section', '')" in PANEL
    keys = set(re.findall(r"html \+= section\('(\w+)'", PANEL))
    keys |= set(re.findall(r"return section\('(\w+)'", ACKTAG))
    assert keys == {'newsletter', 'ack', 'acktag', 'tag', 'author'}
    assert "CURATE_SECTIONS = ('newsletter', 'ack', 'acktag', 'tag', 'author')" in SOURCE
    actions = {'newsletter_set', 'newsletter_clear', 'ack_set', 'ack_clear',
               'acktag_add', 'acktag_remove',
               'tag_add', 'tag_remove', 'author_add', 'author_remove'}
    assert {a.split('_', 1)[0] for a in actions} == keys
    assert "{note if is_open else ''}" in PANEL
    # A note for no section (an unknown action) is shown above them, not lost.
    assert 'html = note if note and opened not in CURATE_SECTIONS else ""' in PANEL


def test_a_lapsed_session_on_a_save_button_goes_back_to_the_page():
    ''' @restricted sends a signed-out request to /login?next=<path>, and
        sign-in replays "next" with a GET. For a POST-only route that is a
        405. The POST's referrer - the page the form was on - is the right
        place to land; the change itself is lost either way.
    '''
    assert "if request.method != 'GET':" in GATE
    assert "ref = urlsplit(request.referrer or '')" in GATE
    assert "if ref.path.startswith('/') else '/'" in GATE


def test_today_is_the_curators_today():
    ''' Production runs in UTC, where a Virginia evening is already tomorrow;
        the newsletter date box must not default to a date that has not
        happened yet for the person setting it.
    '''
    assert "CURATION_TZ = 'America/New_York'" in SOURCE
    assert "datetime.now(ZoneInfo(CURATION_TZ))" in PANEL
    assert "datetime.now().strftime" not in PANEL


def test_each_summary_line_is_the_field_s_state():
    ''' Folded, the tab must still answer "what does this record say". '''
    assert "&middot; reviewed" in PANEL and "not set &middot; not reviewed" in PANEL
    assert "characters" in PANEL and "none recorded" in PANEL
    assert "', '.join(names) if names else 'none'" in PANEL
    assert 'f"{ncred} credited"' in PANEL
    assert 'window.location.hash' in TEMPLATE


def test_a_doi_we_do_not_hold_is_refused_before_any_action_runs():
    assert HANDLER.index('if not row:') < HANDLER.index("elif action in (")
    assert 'nothing to curate' in HANDLER


def test_the_doi_is_normalised_like_the_page_normalises_it():
    ''' /doiui lowercases and strips slashes; a write keyed any other way would
        miss the record the page just showed.
    '''
    assert "doi = doi.lstrip('/').rstrip('/').lower()" in HANDLER


# --- acknowledgement tags --------------------------------------------------

VOCAB = {'FlyLight': r'fly\s*light', 'Vivarium': r'vivarium', 'Rubin Lab': r'rubin'}
ACK_SUPORGS = {'Vivarium': {'code': 'VIV', 'active': True}}
TAGGER = {'orcid': '0000-0003-0000-0001', 'userIdO365': 'Tagger@hhmi.org'}


def _ack_row(tags=None, text='We thank the FlyLight team and the Vivarium.'):
    row = {'doi': '10.1/ack'}
    if text:
        row['jrc_acknowledgements'] = text
    if tags is not None:
        row['jrc_acknowledge'] = tags
    return row


def _iris(name):
    return {'name': name, 'code': None, 'type': 'acknowledgement', 'curator': 'IRIS',
            'updated': datetime(2026, 1, 1)}


def test_the_door_admits_either_term_and_the_view_sorts_them_out():
    ''' @restricted grew an any-of form for this: the acknowledgement tagger
        and the curator share /doi_edit. Inside, only the acktag actions are
        open to both; everything else still needs the curator term, checked
        on the POST and not only by which sections the tab drew.
    '''
    assert "elif isinstance(arg, str):\n        terms = {arg}" in GATE
    assert "terms = set(arg)" in GATE
    assert "if terms and not ({'admin'} | terms) & authorizations(person):" in GATE
    assert "' or '.join(" in GATE
    assert "curator = bool({'admin', CURATOR_TERM} & authorizations(me))" in HANDLER
    order = [HANDLER.index("elif action in ('acktag_add', 'acktag_remove'):"),
             HANDLER.index("elif not curator:"),
             HANDLER.index("elif action in ('newsletter_set', 'newsletter_clear'):")]
    assert order == sorted(order), 'acktag actions must be dispatched before the curator check'
    assert "covers acknowledgement tags only" in HANDLER


def test_a_tagger_without_the_curator_term_sees_only_their_section():
    ''' The text read-only (it is what they are tagging), the tag section, and
        the help for it; none of the other controls.
    '''
    assert "curator = held is None or bool({'admin', CURATOR_TERM} & held)" in PANEL
    tagger_view = PANEL[PANEL.index('if not curator:'):PANEL.index('# Newsletter date')]
    assert "_acktag_section(section, row, edoi, button, muted)" in tagger_view
    assert "white-space:pre-wrap" in tagger_view and "{escape(ack)}" in tagger_view
    assert "return html" in tagger_view
    assert "newsletter" not in tagger_view and "tag_add" not in tagger_view
    panel_fn = _block('def curate_panel(', 'IRIS_CURATOR =')
    assert panel_fn.count("_acktag_section(section, row, edoi, button, muted)") == 2
    assert panel_fn.count("{ACKTAG_HELP}") == 2


def test_an_automatic_tag_has_no_remove_control_and_a_hand_placed_one_does():
    assert "if tag.get('curator') == IRIS_CURATOR:" in ACKTAG
    assert ">automatic</span>" in ACKTAG
    assert "value='acktag_remove'" in ACKTAG
    assert "name='name' list='acktag-choices'" in ACKTAG
    # The pick list leaves out what is already tagged, and there is no form
    # without text to describe.
    assert "choices = sorted(n for n in vocabulary if n not in names)" in ACKTAG
    assert "if not ack:" in ACKTAG and "add the text in the Acknowledgements section first" in ACKTAG


def test_the_in_text_hint_runs_the_entity_s_pattern_over_the_stored_text():
    ns = _ns()
    hint = ns['_acktag_in_text']
    assert hint(r'fly\s*light', 'We thank FlyLight.') is True
    assert hint(r'fly\s*light', 'We thank the Vivarium.') is False
    assert hint('', 'anything') is None and hint(r'x', '') is None
    assert hint(r'(', 'unbalanced') is None, 'a bad pattern is a hint, not a crash'


def test_adding_an_acknowledgement_tag_writes_the_tagger_s_shape_with_a_human_curator():
    ns = _ns()
    note = ns['_save_ack_tag'](_ack_row([_iris('FlyLight')]), TAGGER, 'acktag_add',
                               ' Vivarium ', VOCAB, ACK_SUPORGS)
    assert note.startswith('[success]')
    (query, update, _), = ns['_dois'].calls
    assert query == {'doi': '10.1/ack'}
    entry = update['$push']['jrc_acknowledge']
    assert entry['name'] == 'Vivarium' and entry['type'] == 'suporg'
    assert entry['code'] == {'code': 'VIV', 'active': True}
    assert entry['curator'] == 'Tagger@hhmi.org', 'curator_display looks the person up by userIdO365'
    assert isinstance(entry['updated'], datetime)
    event = ns['_processing'].calls[0][1]['$push']['processes']
    assert event['action'] == 'acktag_added' and 'Vivarium' in event['notes']
    assert event['user'] == 'tagger'


def test_a_name_that_is_not_a_supervisory_org_is_an_acknowledgement_tag_with_no_code():
    ns = _ns()
    ns['_save_ack_tag'](_ack_row([]), TAGGER, 'acktag_add', 'Rubin Lab', VOCAB, ACK_SUPORGS)
    entry = ns['_dois'].calls[0][1]['$push']['jrc_acknowledge']
    assert entry['type'] == 'acknowledgement' and entry['code'] is None


def test_an_add_is_refused_without_text_for_an_unknown_entity_or_a_duplicate():
    ns = _ns()
    save = ns['_save_ack_tag']
    assert 'no acknowledgement text' in save(_ack_row([], text=''), TAGGER, 'acktag_add',
                                             'FlyLight', VOCAB, ACK_SUPORGS)
    assert 'not an entity' in save(_ack_row([]), TAGGER, 'acktag_add', 'Imaginary Lab',
                                   VOCAB, ACK_SUPORGS)
    assert 'already an acknowledgement tag' in save(_ack_row([_iris('FlyLight')]), TAGGER,
                                                    'acktag_add', 'FlyLight', VOCAB, ACK_SUPORGS)
    assert 'No entity name' in save(_ack_row([]), TAGGER, 'acktag_add', '  ', VOCAB, ACK_SUPORGS)
    assert ns['_dois'].calls == [] and ns['_processing'].calls == []


def test_an_automatic_tag_cannot_be_removed_but_a_hand_placed_one_can():
    ''' The tagger would match the text and put an IRIS tag back tonight, so
        refusing is more honest than a one-night removal. A tag a person
        placed is never touched by the tagger, so its removal sticks.
    '''
    ns = _ns()
    save = ns['_save_ack_tag']
    note = save(_ack_row([_iris('FlyLight')]), TAGGER, 'acktag_remove', 'FlyLight', VOCAB, {})
    assert 'placed by the nightly tagger' in note and ns['_dois'].calls == []
    assert 'not an acknowledgement tag' in save(_ack_row([]), TAGGER, 'acktag_remove',
                                                'FlyLight', VOCAB, {})
    human = {'name': 'Vivarium', 'code': None, 'type': 'acknowledgement',
             'curator': 'Someone@hhmi.org'}
    note = save(_ack_row([_iris('FlyLight'), human]), TAGGER, 'acktag_remove', 'Vivarium',
                VOCAB, {})
    assert note.startswith('[success]')
    assert ns['_dois'].calls == [({'doi': '10.1/ack'},
                                  {'$pull': {'jrc_acknowledge': {'name': 'Vivarium'}}}, False)]
    assert ns['_processing'].calls[0][1]['$push']['processes']['action'] == 'acktag_removed'


def test_removing_the_last_acknowledgement_tag_unsets_the_field():
    ''' The metrics and the untagged queue test jrc_acknowledge.0 / the
        field's presence; an empty list would count as tagged.
    '''
    ns = _ns()
    human = {'name': 'Vivarium', 'curator': 'Someone@hhmi.org'}
    ns['_save_ack_tag'](_ack_row([human]), TAGGER, 'acktag_remove', 'Vivarium', VOCAB, {})
    assert [u for _, u, _ in ns['_dois'].calls] == [
        {'$pull': {'jrc_acknowledge': {'name': 'Vivarium'}}},
        {'$unset': {'jrc_acknowledge': ''}}]


def test_a_failed_acknowledgement_tag_write_is_reported_and_not_audited():
    ns = _ns(explode=True)
    note = ns['_save_ack_tag'](_ack_row([]), TAGGER, 'acktag_add', 'FlyLight', VOCAB, {})
    assert note == '[error] Could not save that change.'
    assert ns['_processing'].calls == []
