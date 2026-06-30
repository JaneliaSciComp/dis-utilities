''' dis_state.py
    Shared runtime state, populated at startup from the database. It lives here
    (not in dis_responder.py) so helpers in other modules — e.g. dis_html.py —
    can read it without importing the Flask app.

    dis_responder.before_request fills these dicts IN PLACE (CVTERM.update(...) /
    PROJECT.update(...)); every importer reads them by reference and therefore
    sees the same, populated objects. Always mutate in place — never rebind
    (CVTERM = {...}), or importers would keep pointing at the empty originals.
'''

# Controlled-vocabulary terms from the cvterm collection, keyed
# CVTERM[cv][name] -> term record (e.g. CVTERM['jrc']['jrc_obtained_from']).
CVTERM = {}

# Lookup of project name/alias -> True, from the project_map collection.
PROJECT = {}
