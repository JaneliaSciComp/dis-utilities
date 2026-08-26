''' tag_acknowledgement.py

PURPOSE
-------
Manually add an acknowledgement tag to one or more DOIs, attributed to the human
curator running the program. For each supplied DOI the tag is merged into the
`jrc_acknowledge` list of the DOI's record in the `dois` collection (internal DOIs)
or, failing that, the `external_dois` collection (external DOIs) - `dois` is checked
first, then `external_dois`.

The tag object has the same shape tag_janelia_acks.py / update_tags.py write -
{name, code, type, curator, updated}:
  - name    : the supplied acknowledgement, which MUST be a `key` in the search_regex
              collection (the canonical entity vocabulary); the program aborts if it
              is not.
  - code    : the matching supervisory-organization record ({code, active} dict) from
              the `suporg` collection when the name is a formal org, else None.
  - type    : "suporg" when a suporg code was found, else "acknowledgement".
  - curator : the userIdO365 of the account running the program (see below).
  - updated : the current timestamp.

CURATOR
-------
The curator is resolved from the OS user running the program ($USER) by matching it
case-insensitively against the local part of the `userIdO365` field in the `orcid`
collection (userIdO365 is stored as e.g. SVIRSKASR@hhmi.org). The matched userIdO365
value is what is stored as `curator` - that is what the DIS DOI page's curator_display
looks up. If no orcid record matches, the program ABORTS before touching anything.

MERGE RULES (per DOI)
---------------------
  - If a tag with this `name` and a NON-IRIS (already human-curated) curator exists,
    the DOI is left untouched (skipped) - a human has already curated it.
  - Else if a tag with this `name` and curator "IRIS" (machine-generated) exists, that
    entry is upgraded in place to this human curator (curator + updated refreshed); no
    duplicate is added.
  - Else the new tag is appended.

INPUTS
------
- DIS MongoDB database (read by default; read/write with --write).
- Command-line flags:
    --doi DOI  A single DOI to tag.
    --file F   A file of DOIs (whitespace/newline separated). --doi and --file may be
               combined; at least one is required.
    --key K    An acknowledgement to add (must be a search_regex key). May be repeated
               (--key A --key B) to add several. If omitted, an interactive checkbox
               menu of the search_regex keys is shown (space to toggle, enter to confirm).
    --test     Send the summary email to the developer only, instead of the
               production receivers list (does NOT affect DB writes - use --write
               for that).
    --write    Actually update the database (default: dry run).
    --verbose  Increase logging verbosity.
    --debug    Maximum logging verbosity.

OUTPUT
------
- Prints a per-outcome summary (added / upgraded / skipped / not found).
- With --debug, prints the added/changed tag entry as JSON for each added/upgraded DOI.
- Writes tag_acknowledgement.json with one entry per processed DOI.
- On a --write run that added or upgraded a tag, sends an HTML summary email (house
  style) to the production receivers list - or, with --test, to the developer only.
  Dry runs never email.

DEPENDENCIES
------------
- jrc_common.jrc_common  (JRC): config, database connection, logging, email helpers.
- doi_common.doi_common  (DL): supervisory-organization lookup.
- jrc_email.jrc_email    (JE): house-style HTML email components.
'''

import argparse
import collections
from datetime import datetime
import getpass
import json
from operator import attrgetter
import os
import re
import sys
import inquirer
from inquirer.themes import BlueComposure
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL
import jrc_email.jrc_email as JE

__version__ = '1.1.0'

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Database
DB = {}
# Counters
COUNT = collections.defaultdict(lambda: 0, {})
# Global variables
ARG = DISCONFIG = LOGGER = None
# Resolved curator (orcid userIdO365 of the OS user); set in initialize_program.
CURATOR = None
# Supervisory-organization name -> {code, active} record, for code/type resolution.
SUPORG = {}
# Selected acknowledgement keys (list; from --key, repeatable, or the checkbox menu).
KEYS = []
# Machine-curator marker: an existing tag with this curator is upgraded to the human
# curator rather than left as-is (see MERGE RULES).
IRIS = 'IRIS'


def terminate_program(msg=None):
    ''' Terminate the program gracefully
        Keyword arguments:
          msg: error message or object
        Returns:
          None
    '''
    if msg:
        if not isinstance(msg, str):
            msg = f"An exception of type {type(msg).__name__} occurred. Arguments:\n{msg.args}"
        LOGGER.critical(msg)
    sys.exit(-1 if msg else 0)


def resolve_curator():
    ''' Resolve the OS user to an orcid userIdO365 (case-insensitive local-part match)
        and return that userIdO365. Aborts the program if there is no match.
        Keyword arguments:
          None
        Returns:
          The matched userIdO365 string (stored as the tag's `curator`)
    '''
    user = getpass.getuser()
    try:
        rec = DB['dis'].orcid.find_one(
            {"userIdO365": {"$regex": f"^{re.escape(user)}@", "$options": "i"}},
            {"userIdO365": 1, "given": 1, "family": 1})
    except Exception as err:
        terminate_program(err)
    if not rec:
        terminate_program(f"No orcid record has a userIdO365 matching OS user '{user}' - "
                          "cannot attribute curation. Aborting without changes.")
    name = f"{(rec.get('given') or [''])[0]} {(rec.get('family') or [''])[0]}".strip()
    LOGGER.info(f"Curator: {rec['userIdO365']} ({name or 'name unknown'})")
    return rec['userIdO365']


def select_keys():
    ''' Prompt the user to pick one or more acknowledgements from the search_regex
        vocabulary (used when --key was not supplied). Aborts if cancelled or nothing
        is chosen.
        Keyword arguments:
          None
        Returns:
          The selected keys (list of str)
    '''
    try:
        keys = sorted(DB['dis'].search_regex.distinct("key"))
    except Exception as err:
        terminate_program(err)
    if not keys:
        terminate_program("No keys found in search_regex")
    quest = [inquirer.Checkbox('keys',
                               message="Select acknowledgement(s) to add "
                                       "(↑↓ move, space to toggle, enter to confirm)",
                               choices=keys, carousel=True)]
    try:
        ans = inquirer.prompt(quest, theme=BlueComposure())
    except KeyboardInterrupt:
        terminate_program("User cancelled program")
    if not ans or not ans.get('keys'):
        terminate_program("No acknowledgement selected")
    return ans['keys']


def initialize_program():
    ''' Connect to the database, resolve the curator, prompt for the acknowledgement
        key(s) if not supplied, load the suporg lookup, and validate the keys.
        Aborts on any failure before processing.
        Keyword arguments:
          None
        Returns:
          None
    '''
    global CURATOR, SUPORG, DISCONFIG, KEYS   # pylint: disable=global-statement
    # Fail fast on missing DOI input - before the DB connect and the key menu, so the
    # user isn't made to resolve a curator and pick a key only to be told no DOIs were
    # supplied. (read_input_dois still catches an empty/whitespace-only --file later.)
    if not ARG.DOI and not ARG.FILE:
        terminate_program("No DOIs supplied - use --doi and/or --file")
    try:
        dbconfig = JRC.get_config("databases")
        DISCONFIG = JRC.simplenamespace_to_dict(JRC.get_config("dis"))
    except Exception as err:
        terminate_program(err)
    # Read role by default; write role only when --write (a write connection can read
    # too). --test does NOT change this - it only affects the email recipient.
    dbo = attrgetter(f"dis.prod.{'write' if ARG.WRITE else 'read'}")(dbconfig)
    LOGGER.info(f"Connecting to {dbo.name} prod on {dbo.host} as {dbo.user}")
    try:
        DB['dis'] = JRC.connect_database(dbo)
    except Exception as err:
        terminate_program(err)
    CURATOR = resolve_curator()   # aborts if unresolved
    keys = ARG.KEY if ARG.KEY else select_keys()   # checkbox menu when --key omitted
    KEYS = list(dict.fromkeys(keys))               # de-duplicate, preserve order
    try:
        SUPORG = DL.get_supervisory_orgs(coll=DB['dis'].suporg)
    except Exception as err:
        terminate_program(err)
    # Every acknowledgement is restricted to the search_regex vocabulary.
    try:
        valid = set(DB['dis'].search_regex.distinct("key"))
    except Exception as err:
        terminate_program(err)
    invalid = [k for k in KEYS if k not in valid]
    if invalid:
        terminate_program("Not a search_regex key: " + ", ".join(invalid)
                          + " - aborting. (Add to search_regex first.)")


def read_input_dois():
    ''' Collect, normalize (strip + lowercase) and de-duplicate the DOIs from --doi
        and/or --file, preserving order. Aborts if none were supplied.
        Keyword arguments:
          None
        Returns:
          List of DOI strings
    '''
    raw = []
    if ARG.DOI:
        raw.append(ARG.DOI)
    if ARG.FILE:
        try:
            with open(ARG.FILE, encoding='utf-8') as handle:
                raw.extend(handle.read().split())
        except Exception as err:
            terminate_program(err)
    seen, dois = set(), []
    for token in raw:
        doi = token.strip().lower()
        if doi and doi not in seen:
            seen.add(doi)
            dois.append(doi)
    if not dois:
        terminate_program("No DOIs supplied - use --doi and/or --file")
    return dois


def find_record(doi):
    ''' Locate a DOI in `dois` (internal) then `external_dois` (external).
        Keyword arguments:
          doi: DOI string (lower-case)
        Returns:
          (collection_name, record) or (None, None) if in neither collection
    '''
    for coll in ('dois', 'external_dois'):
        try:
            rec = DB['dis'][coll].find_one({"doi": doi}, {"doi": 1, "jrc_acknowledge": 1})
        except Exception as err:
            terminate_program(err)
        if rec:
            return coll, rec
    return None, None


def new_tag(key):
    ''' Build the tag object for a key, resolving code/type from the suporg lookup
        (the same way tag_janelia_acks.py / update_tags.py do).
        Keyword arguments:
          key: the acknowledgement (a search_regex key)
        Returns:
          Tag dict {name, code, type, curator, updated}
    '''
    code = SUPORG.get(key)
    return {"name": key, "code": code,
            "type": "suporg" if code else "acknowledgement",
            "curator": CURATOR, "updated": datetime.now()}


def merge_tags(existing):
    ''' Apply every selected key's merge rules against a DOI's jrc_acknowledge list.
        Per key: skip if a non-IRIS (human) tag with that name already exists; upgrade
        an IRIS-curated one in place; otherwise append a new tag.
        Keyword arguments:
          existing: current jrc_acknowledge list (list of dicts), may be empty/None
        Returns:
          (new_list, outcomes) - outcomes is a list of {key, outcome, entry} with
          outcome 'added' | 'upgraded' | 'skipped'; new_list is the value to $set, or
          None when nothing changed (every key skipped)
    '''
    tags = list(existing or [])
    outcomes = []
    changed_any = False
    for key in KEYS:
        matches = [tag for tag in tags if tag.get('name') == key]
        if any(tag.get('curator') != IRIS for tag in matches):
            outcomes.append({"key": key, "outcome": "skipped", "entry": None})
            continue                     # already human-curated - leave untouched
        if matches:                      # only IRIS match(es) - upgrade in place
            entry = None
            for tag in tags:
                if tag.get('name') == key and tag.get('curator') == IRIS:
                    tag['curator'] = CURATOR
                    tag['updated'] = datetime.now()
                    entry = tag
            outcomes.append({"key": key, "outcome": "upgraded", "entry": entry})
            changed_any = True
        else:                            # no match - append
            entry = new_tag(key)
            tags.append(entry)
            outcomes.append({"key": key, "outcome": "added", "entry": entry})
            changed_any = True
    return (tags if changed_any else None), outcomes


def process(dois):
    ''' Tag each DOI with every selected key, writing to the DB in --write mode.
        Keyword arguments:
          dois: list of DOI strings
        Returns:
          (results, errors) - results is a list of {doi, collection, key, outcome}
          (one per DOI x key); errors is a list of {doi, error} for DOIs in neither
          collection
    '''
    results, errors = [], []
    for doi in dois:
        coll, rec = find_record(doi)
        if not rec:
            errors.append({"doi": doi, "error": "not found in dois or external_dois"})
            COUNT['notfound'] += 1
            LOGGER.warning(f"{doi} not found in dois or external_dois")
            continue
        new_list, outcomes = merge_tags(rec.get('jrc_acknowledge', []))
        for out in outcomes:
            COUNT[out['outcome']] += 1
            results.append({"doi": doi, "collection": coll,
                            "key": out['key'], "outcome": out['outcome']})
            LOGGER.debug(f"{doi} [{coll}] {out['key']} -> {out['outcome']}")
            if ARG.DEBUG and out['entry'] is not None:
                print(f"{doi} [{coll}] {out['key']} {out['outcome']}:")
                print(json.dumps(out['entry'], indent=2, default=str))
        if new_list is not None and ARG.WRITE:
            try:
                res = DB['dis'][coll].update_one({"doi": doi},
                                                 {"$set": {"jrc_acknowledge": new_list}})
            except Exception as err:
                terminate_program(err)
            if res.modified_count:
                COUNT['written'] += 1
    return results, errors


def generate_email(results, errors):
    ''' Send the house-style HTML run summary. Called only on a --write run that added
        or upgraded at least one tag - to the production receivers list, or the
        developer only with --test.
        Keyword arguments:
          results: list of {doi, collection, outcome}
          errors: list of {doi, error}
        Returns:
          None
    '''
    run_data = JRC.get_run_data(__file__, __version__).strip()
    # Only reached on a --write run (see processing), so the mode is always WRITE.
    mode_label = 'WRITE' + (' · TEST' if ARG.TEST else '')
    mode_tone = 'good'
    kpis  = JE.kpi_card(f"{COUNT['added']:,}", "Added",
                        'good' if COUNT['added'] else 'neutral')
    kpis += JE.kpi_card(f"{COUNT['upgraded']:,}", "Upgraded from IRIS",
                        'good' if COUNT['upgraded'] else 'neutral')
    kpis += JE.kpi_card(f"{COUNT['skipped']:,}", "Skipped (already curated)", 'neutral')
    kpis += JE.kpi_card(f"{COUNT['notfound']:,}", "Not found",
                        'bad' if COUNT['notfound'] else 'neutral')
    keys_label = ', '.join(KEYS)
    changed = [r for r in results if r['outcome'] in ('added', 'upgraded')]
    body = JE.section_header(f"&#127991; Acknowledgement(s) \"{keys_label}\" &mdash; "
                             f"{len(changed):,} tag(s) applied")
    if changed:
        entries = [(r['doi'], f"{r['key']} &middot; {r['collection']} &middot; {r['outcome']}")
                   for r in changed]
        body += JE.doi_card(f"Curator: {CURATOR}", entries, 'good',
                            second_header='Tag &middot; collection &middot; action')
    body = JE.body_row(body)
    skipped = [r for r in results if r['outcome'] == 'skipped']
    if skipped:
        body += JE.body_row(
            JE.section_header(f"Skipped &mdash; already curated ({len(skipped):,})")
            + JE.doi_card("Skipped",
                          [(r['doi'], f"{r['key']} &middot; {r['collection']}") for r in skipped],
                          'neutral', second_header='Tag &middot; collection'))
    if errors:
        body += JE.body_row(
            JE.section_header(f"&#9888; Not found ({len(errors):,})")
            + JE.doi_card("Not found", [(e['doi'], e['error']) for e in errors],
                          'bad', second_header='Detail'))
    msg = JE.render(os.path.basename(__file__), __version__, run_data,
                    mode_label, mode_tone, kpis, body)
    subject = f"Acknowledgement tag(s) \"{keys_label}\" applied"
    if ARG.TEST:
        subject = "[TEST] " + subject
    # --test -> developer only; production run -> the configured receivers list.
    recipient = DISCONFIG['developer'] if ARG.TEST else DISCONFIG['receivers']
    JRC.send_email(msg, DISCONFIG['sender'], recipient, subject, mime='html')


def processing():
    ''' Read the DOIs, tag them, report, and email.
        Keyword arguments:
          None
        Returns:
          None
    '''
    dois = read_input_dois()
    LOGGER.info(f"Tagging {len(dois):,} DOI(s) with {len(KEYS)} acknowledgement(s) "
                f"[{', '.join(KEYS)}] ({'WRITE' if ARG.WRITE else 'dry run'})")
    results, errors = process(dois)
    rows = [("DOIs processed", len(dois)),
            ("  tags added", COUNT['added']),
            ("  tags upgraded (IRIS -> curator)", COUNT['upgraded']),
            ("  tags skipped (already curated)", COUNT['skipped']),
            ("  DOIs not found", COUNT['notfound'])]
    if ARG.WRITE:
        rows.append(("  DOIs written", COUNT['written']))
    width = max(len(label) for label, _ in rows) + 1   # + the colon
    for label, num in rows:
        print(f"{label + ':':<{width + 2}}{num:>5,}")
    if results or errors:
        with open('tag_acknowledgement.json', 'w', encoding='utf-8') as handle:
            json.dump({"keys": KEYS, "curator": CURATOR,
                       "results": results, "errors": errors}, handle, indent=4, default=str)
    # Email only on a --write run that actually changed the DB: dry runs never email,
    # so a preview can't spam the receivers list. A mail failure is logged, not raised,
    # so it can't mask the (already-committed) successful write.
    if ARG.WRITE and (COUNT['added'] or COUNT['upgraded']):
        try:
            generate_email(results, errors)
        except Exception as err:
            LOGGER.error(f"DB updated, but sending the summary email failed: {err}")


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Add an acknowledgement tag (jrc_acknowledge) to DOIs, attributed "
                    "to the running user as curator")
    PARSER.add_argument('--doi', dest='DOI', default=None, help='Single DOI to tag')
    PARSER.add_argument('--file', dest='FILE', default=None,
                        help='File of DOIs (whitespace/newline separated)')
    PARSER.add_argument('--key', dest='KEY', action='append', default=None,
                        help='Acknowledgement to add (must be a search_regex key); '
                             'repeat to add several. If omitted, choose from an '
                             'interactive checkbox menu')
    PARSER.add_argument('--test', dest='TEST', action='store_true', default=False,
                        help='Route the summary email to the developer only '
                             '(does NOT affect DB writes)')
    PARSER.add_argument('--write', dest='WRITE', action='store_true', default=False,
                        help='Actually update the database (default: dry run)')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true', default=False,
                        help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true', default=False,
                        help='Flag, Very chatty; also prints the added/changed tag '
                             'entry as JSON')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    processing()
    terminate_program()
