''' tag_janelia_acks.py

PURPOSE
-------
Tags DOIs with the specific Janelia entities named in their acknowledgement text.
For each DOI whose jrc_acknowledgements text references Janelia - by name or by the
JFRC (Janelia Farm Research Campus) abbreviation, matched case-insensitively - it
identifies the department, facility, project, or PI lab being acknowledged (e.g.
Advanced Imaging Center, FlyEM Project, Lavis Lab) and records them in the
jrc_acknowledge field.
It does NOT fetch acknowledgements - it tags text already collected by
pull_external_acks.py / pull_internal_acks.py.

Identification is purely lexical: the `search_regex` collection holds one
case-insensitive regex per entity (keyed by name), and a DOI is tagged with an entity
if that entity's regex matches the (flattened) acknowledgement text. A DOI may be
tagged with multiple entities. The regexes are loaded at runtime (see
initialize_program) and are maintained / seeded by seed_search_regex.py.

INPUTS
------
- DIS MongoDB database (read-only by default; read/write with --write). The
  acknowledgement text is pulled from the `dois` and `external_dois` collections
  (every record that has a jrc_acknowledgements field). The `suporg` collection is
  read to resolve supervisory-organization codes for the tag objects.
- Command-line flags:
    --doi DOI  Restrict processing to a single DOI (internal or external). The
               aggregate output files are not rewritten for a single-DOI run.
    --untagged Print to stderr the DOI of every record that references Janelia/JFRC
               but matched no entity (for tuning the search_regex patterns).
    --write    Update jrc_acknowledge in the database (default: dry run).
    --verbose  Increase logging verbosity.
    --debug    Maximum logging verbosity.

EMAIL RECIPIENT
----------------
The summary email always goes to the configured developer address, never a
full receivers list (same convention as pull_internal_acks.py/
pull_external_acks.py/update_ack_records.py), and is sent any time a DOI was
updated - --write or not. A --doi spot-check run sends no email, same as it
skips the aggregate output files.

HIGH-LEVEL FLOW
---------------
1. Initialization
   - Connect to the database (read-only unless --write) and load the
     supervisory-organization name->code map and the entity regexes from search_regex.
2. For each acknowledgement record from the database (dois and external_dois):
   - Flatten its acknowledgement value to a single string (ack_to_text).
   - Decide whether the record is even worth searching for entities:
     - External (external_dois) records: skip unless the text references
       "Janelia" or the "JFRC" abbreviation (case-insensitive) - a bare
       entity-name match there (e.g. "FlyLight") could plausibly refer to an
       unrelated, same-named thing at another institution (e.g. a "FlyLight"
       project at Harvard), so an explicit Janelia/JFRC mention is required.
     - Internal (dois, Janelia-authored) records: never skipped on this basis
       - a bare entity-name match is unambiguous when the authors are Janelia
       themselves, so every search_regex pattern is tried regardless of
       whether "Janelia"/"JFRC" is separately mentioned.
   - Run every search_regex pattern (find_acknowledged) for the matched
     entity names, and MERGE them into the DOI's existing jrc_acknowledge list:
     existing tags are preserved and never modified, removed, or overwritten.
     A detected entity that already has an IRIS-curated tag of the same name is
     NOT re-added (it was added by an earlier run of this program - just
     idempotent reprocessing, not reported). A detected entity that already has
     only a non-IRIS (human-curated) tag of the same name is counted per-entity
     for the "Human Curated" report column, but likewise gets NO new IRIS tag
     appended - once a human has curated a name, IRIS never adds a second,
     separate record for it. Each genuinely new tag (a name with no existing
     tag at all) is appended as a tag object
     (see TAG OBJECTS below).
3. Database update (--write only)
   - For each DOI that gained at least one new tag, set jrc_acknowledge on its
     source collection (dois or external_dois).
4. Output
   - Write each record that gained at least one new tag this run (doi, matched
     entities, and the merged jrc_acknowledge value - existing tags included)
     to one of two JSON files, split by source collection: dois ->
     internal_acks_tagged.json and external_dois -> external_acks_tagged.json.
     A record with no new tag this run is neither written to the database nor
     included in either file, even if it already carries older tags. (A
     single-DOI --doi run skips these aggregate files so a spot check cannot
     overwrite them.)
   - Print a summary, including both the total record count currently carrying
     acknowledgement tags (old or new - "Records tagged") and the count that
     actually gained a new tag this run ("Records written", the same records
     in the JSON files), and a per-entity count table (New / Human Curated
     columns: newly-tagged this run vs. matched but already carrying a
     human-curated tag - see build_tags). (Use --untagged to list, on stderr,
     the DOIs that reference Janelia/JFRC but matched no entity.)
   - Send a summary email (see EMAIL RECIPIENT above): a header banner (run
     data, DRY RUN/WRITE badge), KPI stat tiles (DOIs updated, records tagged,
     records written, human curated), and the same Entity/New/Human Curated
     table. Built
     entirely from inline styles/tables (no <style> block) for compatibility
     with older email clients, matching the convention used by
     sync_citations.py and the sibling ack scripts.

TAG OBJECTS (jrc_acknowledge)
-----------------------------
Each entry in jrc_acknowledge is a tag object {name, code, type, curator, updated} -
the same shape utility/bin/update_tags.py writes:
- name    : the canonical entity name (the search_regex key).
- type    : "suporg" if name EXACTLY matches a supervisory organization in the
            suporg collection (a formal Janelia org-chart unit); otherwise
            "acknowledgement" (a recognized facility/project/lab label that is not
            a formal org unit). The match is exact-string on the canonical name, so
            most entity names resolve to "acknowledgement".
- code    : for a "suporg" tag, the matched supervisory-organization record - a
            {code, active} dict as returned by DL.get_supervisory_orgs; None for an
            "acknowledgement" tag. The "suporg" linkage is what enables org-level
            rollups; "acknowledgement" tags are name-only labels.
- curator : "IRIS" on every tag this program adds, marking it machine-generated
            (human-curated tags carry a different curator, or none).
- updated : timestamp this tag was written, set only on tags newly added this
            run - existing tags (human-curated or already IRIS-tagged from an
            earlier run) are never modified, so they keep whatever (or no)
            updated value they already had.

DEPENDENCIES
------------
- jrc_common.jrc_common  (JRC): logging, config, and database connection helpers.
- doi_common.doi_common  (DL): supervisory-organization lookup.
'''

import argparse
import collections
from datetime import datetime
import html
import json
from operator import attrgetter
import os
import re
import sys
import traceback
from pymongo import UpdateOne
from tqdm import tqdm
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL

__version__ = '1.6.0'

# pylint: disable=broad-exception-caught,logging-fstring-interpolation,line-too-long

# Output files (always written): tagged records split by source collection. Only
# records that gained at least one new tag this run are included.
INTERNAL_OUTPUT_FILE = 'internal_acks_tagged.json'   # from the dois collection
EXTERNAL_OUTPUT_FILE = 'external_acks_tagged.json'   # from the external_dois collection

# Database handle
DB = {}
# Supervisory-organization name -> code (for jrc_acknowledge tag objects)
SUPORG = {}
# Counters
COUNT = collections.defaultdict(lambda: 0, {})
# Collections searched for acknowledgement text
COLLECTIONS = ('dois', 'external_dois')
# Attribution stamped on every tag this program adds to jrc_acknowledge
CURATOR = 'IRIS'
# Global variables
ARG = DISCONFIG = LOGGER = None
# HTML run-summary email palette/layout (generate_email and its html_* helpers).
# Mirrors sync_citations.py's email convention: inline styles only (no <style>
# block/classes), colors paired with an icon/label (not color alone) for
# colorblind accessibility, for reliable rendering across email clients
# including older Outlook.
EMAIL_NAVY = '#1f3a5f'
EMAIL_GREEN = '#1c7c3f'
EMAIL_GREEN_BG = '#eefaf1'
EMAIL_RED = '#c0392b'
EMAIL_RED_BG = '#fdecea'
EMAIL_AMBER = '#d68a1f'
EMAIL_GRAY = '#5b6b7c'
EMAIL_GRAY_BG = '#f2f4f6'
EMAIL_STRIPE_BG = '#f7f9fb'
EMAIL_BORDER = '#eef1f4'


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


def initialize_program():
    ''' Connect to the database and load supervisory-organization codes
        Keyword arguments:
          None
        Returns:
          None
    '''
    try:
        dbconfig = JRC.get_config("databases")
    except Exception as err:
        terminate_program(err)
    dbo = attrgetter(f"dis.prod.{'write' if ARG.WRITE else 'read'}")(dbconfig)
    LOGGER.info(f"Connecting to {dbo.name} prod on {dbo.host} as {dbo.user}")
    try:
        DB['dis'] = JRC.connect_database(dbo)
    except Exception as err:
        terminate_program(err)
    try:
        orgs = DL.get_supervisory_orgs(coll=DB['dis'].suporg)
    except Exception as err:
        terminate_program(err)
    for key, val in orgs.items():
        SUPORG[key] = val
    LOGGER.info(f"Loaded {len(SUPORG):,} supervisory-organization codes")
    # Load entity-matching regexes from search_regex (single source of truth, shared
    # with the UI). Each doc is {key, regex, description}; COMPILED becomes a list of
    # (key, compiled-regex). Same flags as the patterns were authored with (DOTALL so
    # ".*?" co-occurrence gaps may span newlines).
    try:
        rows = DB['dis'].search_regex.find({})
        COMPILED[:] = [(row['key'], re.compile(row['regex'], re.IGNORECASE | re.DOTALL))
                       for row in rows]
    except Exception as err:
        terminate_program(err)
    if not COMPILED:
        terminate_program("No entity regexes found in the search_regex collection")
    LOGGER.info(f"Loaded {len(COMPILED):,} entity regexes from search_regex")


# Entity-matching regexes live in the `search_regex` MongoDB collection - the single
# source of truth shared with the UI's /acksregexui - and are loaded into COMPILED at
# runtime by initialize_program(). Each entry is (key, compiled-regex). Maintain the
# patterns via seed_search_regex.py (which seeds search_regex).
COMPILED = []

# An acknowledgement is only considered for entity tagging if it references Janelia,
# either by name or by the "JFRC" (Janelia Farm Research Campus) abbreviation. Matched
# case-insensitively so neither a lowercase "janelia" nor a "jfrc" mention is missed.
# JFRC is word-boundary anchored so it does not fire on the common Drosophila plasmid
# names ("pJFRC7", "pJFRC26", ...), which are not campus references.
JANELIA_GATE = re.compile(r'Janelia|\bJFRC\b', re.IGNORECASE)


def ack_to_text(ack):
    ''' Flatten an acknowledgement value into a single string. jrc_acknowledgements
        is normally already a string; lists/dicts are coerced defensively.
    '''
    if isinstance(ack, str):
        return ack
    if not ack:
        return ''
    parts = []
    for item in ack:
        if isinstance(item, str):
            parts.append(item)
        elif isinstance(item, dict):
            for val in item.values():
                if isinstance(val, list):
                    parts.extend(str(x) for x in val)
                else:
                    parts.append(str(val))
    return ' '.join(parts)


def find_acknowledged(text, require_gate=True):
    ''' Return sorted list of Janelia entities identified in ack text.
        Keyword arguments:
          text: flattened acknowledgement text
          require_gate: if True, an explicit "Janelia"/"JFRC" mention
                        (JANELIA_GATE) is required before any entity regex is
                        even tried. Callers should pass False only for
                        Janelia-authored (dois collection) records: there, a
                        bare entity-name match (e.g. "FlyLight") is
                        unambiguous since the authors themselves are Janelia.
                        For external_dois records, keep this True - a bare
                        entity-name match could plausibly refer to an
                        unrelated, same-named thing at another institution
                        (e.g. a "FlyLight" project at Harvard).
        Returns:
          sorted list of matched entity keys
    '''
    if require_gate and not JANELIA_GATE.search(text):
        return []
    found = set()
    for key, regex in COMPILED:
        if regex.search(text):
            found.add(key)
    return sorted(found)


def get_suporg_code(name):
    ''' Return the supervisory-organization record for an entity name, or None
        Keyword arguments:
          name: entity name
        Returns:
          The suporg record ({code, active} dict) if the name is a supervisory
          organization, else None
    '''
    return SUPORG.get(name)


def build_tags(existing, names):
    ''' Merge detected entity names into a DOI's existing jrc_acknowledge tag list.
        Existing tags are never modified or removed. Each genuinely new tag is
        appended as a tag object {name, code, type, curator, updated}, the same
        shape update_tags.py writes: type is "suporg" if the name resolves to a
        supervisory organization else "acknowledgement"; code is the matching
        supervisory-organization record ({code, active} dict) for a "suporg" tag,
        else None; curator is CURATOR ("IRIS"), marking it machine-generated;
        updated is the current timestamp.
        A detected entity whose name already has ANY existing tag - IRIS-curated
        or human-curated - is left as-is and NOT re-added. An IRIS-curated match
        is just idempotent reprocessing from an earlier run (not newsworthy, not
        added to `already_tagged`). A human-curated match IS added to
        `already_tagged` for the caller to report, but still gets no new IRIS
        tag appended alongside it - once a human has curated a name, IRIS never
        adds a second, separate record for it.
        Keyword arguments:
          existing: current jrc_acknowledge list (list of dicts), may be empty
          names: detected entity names (list of str)
        Returns:
          (merged_tags, added_names, already_tagged) tuple
    '''
    merged = list(existing or [])
    added = []
    already_tagged = []
    for name in names:
        matches = [tag for tag in merged if tag.get('name') == name]
        if any(tag.get('curator') != CURATOR for tag in matches):
            already_tagged.append(name)
        if matches:
            continue
        code = get_suporg_code(name)
        candidate = {"name": name, "code": code,
                     "type": "suporg" if code else "acknowledgement",
                     "curator": CURATOR, "updated": datetime.now()}
        merged.append(candidate)
        added.append(name)
    return merged, added, already_tagged


def load_records():
    ''' Yield acknowledgement records to tag from the dois and external_dois
        collections. Each yielded record is normalised to a dict with:
          doi        : DOI string
          text       : flattened acknowledgement text
          existing   : current jrc_acknowledge list (may be empty)
          collection : source collection name ('dois' or 'external_dois')
        Keyword arguments:
          None
        Yields:
          normalised record dicts
    '''
    projection = {"_id": 0, "doi": 1, "jrc_acknowledgements": 1, "jrc_acknowledge": 1}
    for coll in COLLECTIONS:
        try:
            rows = DB['dis'][coll].find(query_payload(), projection)
        except Exception as err:
            terminate_program(err)
        for row in rows:
            yield {"doi": row.get('doi'),
                   "text": ack_to_text(row.get('jrc_acknowledgements', '')),
                   "existing": row.get('jrc_acknowledge', []),
                   "collection": coll}


def query_payload():
    ''' Build the MongoDB query for records that carry acknowledgement text,
        optionally restricted to a single DOI (--doi).
        Keyword arguments:
          None
        Returns:
          query payload dict
    '''
    payload = {"jrc_acknowledgements": {"$exists": True}}
    if ARG.DOI:
        payload["doi"] = ARG.DOI.lower()
    return payload


def record_totals():
    ''' Count records carrying acknowledgement text per collection (for the
        progress-bar total and the read stats).
        Keyword arguments:
          None
        Returns:
          dict of collection name -> document count
    '''
    totals = {}
    for coll in COLLECTIONS:
        try:
            totals[coll] = DB['dis'][coll].count_documents(query_payload())
        except Exception as err:
            terminate_program(err)
    return totals


def apply_updates(pending):
    ''' Apply (with --write) or count (dry run) the jrc_acknowledge updates. Writes are
        batched per collection with bulk_write, so a run touches the database in one
        round-trip per collection instead of one per DOI. Records without a DOI are
        skipped with a warning. COUNT['updated'] tallies the writable DOIs either way
        (the dry-run "DOIs to update" total).
        Keyword arguments:
          pending: list of (doi, collection, tags) tuples to write
        Returns:
          None
    '''
    ops = collections.defaultdict(list)
    for doi, collection, tags in pending:
        if not doi:
            LOGGER.warning("Cannot write jrc_acknowledge: record has no DOI")
            continue
        COUNT['updated'] += 1
        ops[collection].append(UpdateOne({"doi": doi}, {"$set": {"jrc_acknowledge": tags}}))
    if not ARG.WRITE:
        return
    for collection, batch in ops.items():
        try:
            DB['dis'][collection].bulk_write(batch, ordered=False)
        except Exception as err:
            terminate_program(err)


def write_output(internal, external):
    ''' Write the tagged records to two JSON files, split by source collection:
        internal (dois) and external (external_dois). Both files are always
        written (possibly empty). Only records that gained at least one new
        tag this run reach this point (see processing()); a record whose
        existing tags are unchanged this run is not included, even if it
        already carries older tags.
        Keyword arguments:
          internal: tagged records from the dois collection
          external: tagged records from the external_dois collection
        Returns:
          None
    '''
    for records, path in ((internal, INTERNAL_OUTPUT_FILE),
                          (external, EXTERNAL_OUTPUT_FILE)):
        with open(path, 'w', encoding='utf-8') as fileobj:
            json.dump(records, fileobj, indent=2, default=str)
        LOGGER.info(f"Wrote {len(records):,} tagged records to {path}")


def build_entity_rows(new_counts, already_tagged_counts):
    ''' Combine new_counts (genuinely added this run) and already_tagged_counts
        (matched but already carrying a non-IRIS, i.e. human-curated, tag) into
        (entity, new, already) rows, sorted by total descending. Tracked as two
        independent counters (not derived by subtracting one from a combined
        total) so a third, unreported bucket - a match against an entity that
        already carries an IRIS-curated tag from an earlier run, which is
        neither new nor human-curated - can't silently leak into either column.
        Shared by report() (console) and generate_email() (HTML) so both tables
        agree.
        Keyword arguments:
          new_counts: dict of entity name -> tags genuinely added this run
          already_tagged_counts: dict of entity name -> matches that already
                                 carried a human-curated tag
        Returns:
          list of (entity, new, already) tuples
    '''
    all_entities = set(new_counts) | set(already_tagged_counts)
    rows = [(entity, new_counts.get(entity, 0), already_tagged_counts.get(entity, 0))
            for entity in all_entities]
    return sorted(rows, key=lambda r: -(r[1] + r[2]))


def report(rows):
    ''' Print a run summary and a combined per-entity count table.
        Keyword arguments:
          rows: list of (entity, new, already) tuples from build_entity_rows
        Returns:
          None
    '''
    print(f"Records read (internal):       {COUNT['read_internal']:,}")
    print(f"Records read (external):       {COUNT['read_external']:,}")
    print(f"Records with Janelia/JFRC:     {COUNT['janelia']:,}")
    print(f"  Tagged with specific entity: {COUNT['tagged']:,}")
    print(f"  No entity identified:        {COUNT['untagged']:,}")
    if COUNT['entity_only']:
        print(f"Internal, entity match only "
              f"(no Janelia/JFRC mention): {COUNT['entity_only']:,}")
    if COUNT['already_tagged']:
        print(f"Human-curated matches:         {COUNT['already_tagged']:,}")
    action = "DOIs updated:" if ARG.WRITE else "DOIs to update (dry run):"
    print(f"{action:<31}{COUNT['updated']:,}")
    print(f"Records tagged (internal):     {COUNT['snapshot_internal']:,}")
    print(f"Records tagged (external):     {COUNT['snapshot_external']:,}")
    print(f"Records written (internal):    {COUNT['written_internal']:,}  ({INTERNAL_OUTPUT_FILE})")
    print(f"Records written (external):    {COUNT['written_external']:,}  ({EXTERNAL_OUTPUT_FILE})")
    if rows:
        print()
        print(f"{'Entity':<40} {'New':>6} {'Human Curated':>14}")
        print("-" * 62)
        for entity, new, already in rows:
            new_str = f"{new:,}" if new else ""
            already_str = f"{already:,}" if already else ""
            print(f"  {entity:<38} {new_str:>6} {already_str:>14}")


def html_kpi_card(value, label, tone='neutral', width='33%'):
    ''' Build one KPI stat tile for the run-summary email's header row.
        A single <td> carries the box look directly (bgcolor attribute +
        background-color, no nested table) - Outlook's Word rendering engine
        chokes on a percentage-width table nested inside a percentage-width <td>.
        Keyword arguments:
          value: display value (already formatted, e.g. "3")
          label: caption under the value
          tone: 'neutral', 'good', or 'bad' - selects the tile's color scheme
          width: tile width as a percentage string (tune to the tile count)
        Returns:
          HTML for one table cell
    '''
    bg, fg = {'good': (EMAIL_GREEN_BG, EMAIL_GREEN),
              'bad': (EMAIL_RED_BG, EMAIL_RED),
              'neutral': (EMAIL_GRAY_BG, EMAIL_GRAY)}[tone]
    return (f'<td width="{width}" align="center" valign="top" bgcolor="{bg}" '
            f'style="padding:14px 6px;background-color:{bg};border-radius:8px;">'
            f'<div style="font-size:24px;font-weight:700;color:{fg};">{value}</div>'
            f'<div style="font-size:10.5px;color:{EMAIL_GRAY};text-transform:uppercase;'
            f'letter-spacing:.04em;margin-top:2px;">{label}</div>'
            f'</td>')


def html_section_header(title):
    ''' Build a section header bar for the run-summary email
        Keyword arguments:
          title: section title (may include an HTML entity icon prefix)
        Returns:
          HTML div block
    '''
    return (f'<div style="font-size:14px;font-weight:700;color:{EMAIL_NAVY};'
            f'border-bottom:2px solid {EMAIL_BORDER};padding-bottom:7px;'
            f'margin-bottom:10px;">{title}</div>')


def html_entity_table(rows):
    ''' Build the combined Entity/New/Human Curated HTML table for the
        run-summary email, mirroring report()'s console table (zero counts
        render blank). No zebra striping (plain rows); vertical-align:top on
        every cell keeps the counts lined up with the first line of a long
        entity name that wraps to multiple lines (e.g. "Gene Targeting and
        Transgenic Facility"). The numeric columns pair the align attribute
        with text-align in the style (not align alone) - the same
        belt-and-suspenders convention this codebase's other email tables use
        for older Outlook.
        Keyword arguments:
          rows: list of (entity, new, already) tuples from build_entity_rows
        Returns:
          HTML table
    '''
    trs = []
    for entity, new, already in rows:
        new_str = f"{new:,}" if new else ''
        already_str = f"{already:,}" if already else ''
        trs.append(
            '<tr>'
            f'<td style="padding:6px 10px;vertical-align:top;">{html.escape(entity)}</td>'
            f'<td align="right" style="padding:6px 10px;text-align:right;'
            f'vertical-align:top;">{new_str}</td>'
            f'<td align="right" style="padding:6px 10px;text-align:right;'
            f'vertical-align:top;">{already_str}</td></tr>')
    return ('<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
            'style="border-collapse:collapse;font-size:12.5px;">'
            f'<tr style="color:{EMAIL_GRAY};font-size:10.5px;text-transform:uppercase;'
            'letter-spacing:.03em;"><td style="padding:6px 10px;">Entity</td>'
            '<td align="right" style="padding:6px 10px;text-align:right;">'
            'Newly Annotated</td>'
            '<td align="right" style="padding:6px 10px;text-align:right;">'
            'Human Curated</td></tr>'
            + "".join(trs) + '</table>')


def generate_email(rows):
    ''' Generate and send the HTML run-summary email. Always goes to the
        developer (same convention as the sibling ack scripts), sent whenever
        something was updated, --write or not.
        Keyword arguments:
          rows: list of (entity, new, already) tuples from build_entity_rows
        Returns:
          None
    '''
    run_data = JRC.get_run_data(__file__, __version__).strip()
    mode_badge_bg = EMAIL_GREEN if ARG.WRITE else EMAIL_AMBER
    mode_label = 'WRITE' if ARG.WRITE else 'DRY RUN'
    restrict = f" &middot; doi: {ARG.DOI}" if ARG.DOI else ""

    snapshot_total = COUNT['snapshot_internal'] + COUNT['snapshot_external']
    written_total = COUNT['written_internal'] + COUNT['written_external']
    kpis = ''.join([
        html_kpi_card(f"{COUNT['updated']:,}", "DOIs updated",
                      'good' if COUNT['updated'] else 'neutral', width='25%'),
        html_kpi_card(f"{snapshot_total:,}", "Records tagged", 'neutral', width='25%'),
        html_kpi_card(f"{written_total:,}", "Records written", 'neutral', width='25%'),
        html_kpi_card(f"{COUNT['already_tagged']:,}", "Human curated", 'neutral', width='25%'),
    ])

    entity_section = (
        html_section_header(f"&#127991; Acknowledgement Tagging ({len(rows):,})")
        + (html_entity_table(rows) if rows else f'<div style="color:{EMAIL_GRAY};'
                                                 'font-size:13px;">No entities matched.</div>'))

    msg = (
        f'<div style="font-family:-apple-system,\'Segoe UI\',Roboto,Helvetica,Arial,sans-serif;'
        f'background-color:#eef1f4;padding:8px 0;">'
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0"><tr>'
        '<td align="center" style="padding:8px 14px 32px 14px;">'
        '<table role="presentation" width="720" cellpadding="0" cellspacing="0" '
        'bgcolor="#ffffff" '
        f'style="max-width:720px;width:100%;background-color:#ffffff;border-radius:10px;'
        f'border:1px solid {EMAIL_BORDER};overflow:hidden;">'
        f'<tr><td bgcolor="{EMAIL_NAVY}" style="background-color:{EMAIL_NAVY};'
        f'padding:22px 28px;">'
        f'<div style="color:#ffffff;font-size:19px;font-weight:600;">'
        f'{os.path.basename(__file__)}&nbsp;'
        f'<span style="font-weight:400;opacity:.7;font-size:14px;">v{__version__}</span></div>'
        f'<div style="color:#c9d6e6;font-size:12.5px;margin-top:6px;">{run_data}{restrict}'
        f' &middot; <span style="background-color:{mode_badge_bg};color:#fff;'
        f'border-radius:10px;padding:1px 9px;font-size:11px;font-weight:600;'
        f'letter-spacing:.03em;">{mode_label}</span></div></td></tr>'
        f'<tr><td style="padding:22px 22px 6px 22px;">'
        # cellspacing (not CSS margin, which <td> mostly ignores) puts a real gap
        # between the KPI tiles; Outlook's Word engine honors this old-school
        # HTML attribute far more reliably than CSS spacing tricks.
        f'<table role="presentation" width="100%" cellpadding="0" cellspacing="6"><tr>'
        f'{kpis}</tr></table></td></tr>'
        f'<tr><td style="padding:18px 28px 4px 28px;">{entity_section}</td></tr>'
        f'<tr><td bgcolor="{EMAIL_STRIPE_BG}" style="padding:18px 28px;'
        f'background-color:{EMAIL_STRIPE_BG};color:{EMAIL_GRAY};'
        f'font-size:11px;text-align:center;border-top:1px solid {EMAIL_BORDER};">'
        'Generated by tag_janelia_acks.py &middot; Data and Information Services &middot; '
        'Janelia Research Campus</td></tr>'
        '</table></td></tr></table></div>')

    try:
        LOGGER.info(f"Sending email to {DISCONFIG['developer']}")
        JRC.send_email(msg, DISCONFIG['sender'], DISCONFIG['developer'],
                       "Janelia acknowledgement tagging summary", mime='html')
    except Exception as err:
        print(str(err))
        traceback.print_exc()
        terminate_program(err)


def processing():  # pylint: disable=too-many-locals,too-many-branches,too-many-statements
    ''' Tag acknowledgement records and optionally update the database
        Keyword arguments:
          None
        Returns:
          None
    '''
    internal = []
    external = []
    new_counts = {}
    already_tagged_counts = {}
    pending = []    # (doi, collection, merged) records to write in a second pass
    totals = record_totals()
    LOGGER.info(f"Processing {sum(totals.values()):,} records with acknowledgements "
                f"({totals.get('dois', 0):,} internal, {totals.get('external_dois', 0):,} external)")
    for rec in tqdm(load_records(), total=sum(totals.values()),
                    desc="Tagging acknowledgements"):
        collection = rec['collection']
        is_internal = collection == 'dois'
        COUNT['read_internal' if is_internal else 'read_external'] += 1
        gated = bool(JANELIA_GATE.search(rec['text']))
        # Internal (Janelia-authored) records don't need the explicit
        # Janelia/JFRC mention - a bare entity-name match is unambiguous
        # there. External records still require it (see find_acknowledged).
        names = find_acknowledged(rec['text'], require_gate=not is_internal)
        if not gated and not names:
            continue
        if gated:
            COUNT['janelia'] += 1
        else:
            COUNT['entity_only'] += 1
        doi = rec['doi']
        merged, added, already_tagged = build_tags(rec['existing'], names)
        for name in added:
            new_counts[name] = new_counts.get(name, 0) + 1
        for name in already_tagged:
            already_tagged_counts[name] = already_tagged_counts.get(name, 0) + 1
        COUNT['already_tagged'] += len(already_tagged)
        if names:
            COUNT['tagged'] += 1
        else:
            COUNT['untagged'] += 1
            if ARG.UNTAGGED:
                tqdm.write(doi or '?', file=sys.stderr)
        # Tally every record currently carrying acknowledgement tags (old or new) for
        # the "Records tagged" report line, independent of whether anything changed
        # this run.
        if merged:
            COUNT['snapshot_internal' if collection == 'dois' else 'snapshot_external'] += 1
        # Only emit and persist (JSON output + database) records that actually gained
        # a new tag this run.
        if not added:
            continue
        result = {"doi": doi, "acknowledged": names, "jrc_acknowledge": merged}
        # Split output by source collection.
        if collection == 'dois':
            internal.append(result)
            COUNT['written_internal'] += 1
        else:
            external.append(result)
            COUNT['written_external'] += 1
        pending.append((doi, collection, merged))
    apply_updates(pending)
    rows = build_entity_rows(new_counts, already_tagged_counts)
    # A single-DOI run only processes one record, so writing the aggregate output files
    # would clobber the full result set with one entry; skip them for a --doi spot check.
    if ARG.DOI:
        LOGGER.info(f"Single-DOI run (--doi): not rewriting {INTERNAL_OUTPUT_FILE} / "
                    f"{EXTERNAL_OUTPUT_FILE}")
    else:
        write_output(internal, external)
    report(rows)
    # Same single-DOI exemption as the aggregate output files - a spot check
    # shouldn't trigger a summary email. Otherwise send whenever something was
    # updated, regardless of --write.
    if ARG.DOI:
        LOGGER.info("Single-DOI run (--doi): not sending summary email")
    elif COUNT['updated']:
        generate_email(rows)


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Tag DOIs with the Janelia entities named in their acknowledgements")
    PARSER.add_argument('--doi', dest='DOI', default=None,
                        help='Restrict processing to a single DOI (internal or external)')
    PARSER.add_argument('--untagged', dest='UNTAGGED', action='store_true', default=False,
                        help='Print DOIs that reference Janelia/JFRC but match no entity')
    PARSER.add_argument('--write', dest='WRITE', action='store_true', default=False,
                        help='Flag, Update database')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true', default=False,
                        help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true', default=False,
                        help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    DISCONFIG = JRC.simplenamespace_to_dict(JRC.get_config("dis"))
    initialize_program()
    processing()
    terminate_program()
