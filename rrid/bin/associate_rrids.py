''' associate_rrids.py
    Associate SciCrunch RRIDs (rrids.org) with records in the suporg collection.

    Janelia's core facilities each hold an RRID, minted through ABRF
    CoreMarketplace. The registry stores the full official name ("Howard Hughes
    Medical Institute at Janelia Research Campus Advanced Imaging Center Core
    Facility") while suporg stores the local one ("Advanced Imaging Center"), so
    the two have to be matched rather than joined.

    Matching runs in three tiers, strongest first:
      exact     the registry's own item.synonyms contains a suporg name verbatim.
                Free and certain - it covers half the facilities outright.
      strong    the boilerplate-stripped registry name scores >= STRONG against a
                suporg name.
      candidate anything weaker, offered as a numbered shortlist.

    NOTHING is auto-accepted. Fuzzy scores for this data do not separate right
    from wrong: "MouseLight Project" -> "MouseLight" scores 71 and is correct,
    while "Light Microscopy" -> "Electron Microscopy" scores 74 and is wrong
    because no Light Microscopy suporg exists. Every association below the exact
    tier is confirmed by a human, and even exact hits are shown before writing.

    token_sort_ratio is used rather than WRatio or token_set_ratio, both of which
    score that same wrong pair far higher (86 and 100).

    The RRID is stored as a sub-document on the suporg record:

        rrid: {id, abrf, name, citation, type, retired, curator, checked}

    `id` is the bare identifier (SCR_017823) - render "RRID:" for display. `name`
    and `citation` are cached snapshots: the registry name is editable and drifts,
    so it is stored with a `checked` timestamp rather than trusted forever. Note
    that this field is invisible to DL.get_supervisory_orgs(), which whitelists
    only code and active. Do not widen that function's default return - the
    acknowledgement taggers store its whole dict into jrc_acknowledge[].code, so
    widening it would stamp rrid into the dois collection. Add a separate
    accessor instead.

    Records are read from the keyed Elasticsearch API rather than the public
    resolver because only Elastic returns provenance.lastSeenDate, which tells a
    refreshed record from a stale one. Requires SCICRUNCH_API_KEY.
'''

__version__ = '1.0.0'

import argparse
import collections
import html
from operator import attrgetter
import os
import re
import sys
from datetime import datetime
import requests
from rapidfuzz import fuzz, process, utils
import jrc_common.jrc_common as JRC
import jrc_email.jrc_email as JE

# pylint: disable=broad-exception-caught,logging-fstring-interpolation,logging-not-lazy

# Database
DB = {}
# Global variables
ARG = DIS = LOGGER = None
# Counters
COUNT = collections.defaultdict(lambda: 0, {})
SESSION = requests.Session()
ELASTIC = "https://api.scicrunch.io/elastic"
# Resource indices worth searching for an organizational unit. Antibodies,
# plasmids and organisms are resources a facility produces, not the facility.
INDICES = ("RIN_Tool_pr",)
# Score at or above which a fuzzy match is presented as the leading candidate.
# Not an auto-accept threshold - there is no safe one (see the module docstring).
STRONG = 90
# Below this a candidate is not worth showing at all.
FLOOR = 55
SHORTLIST = 5
# The registry's only machine-readable closure signal; there is no boolean and
# no superseded-by pointer.
RETIRED = 'NO LONGER IN SERVICE'


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
    ''' Initialize database connection and the SciCrunch session
        Keyword arguments:
          None
        Returns:
          None
    '''
    if "SCICRUNCH_API_KEY" not in os.environ:
        terminate_program("Missing key - set in SCICRUNCH_API_KEY environment variable")
    SESSION.headers['apikey'] = os.environ['SCICRUNCH_API_KEY']
    try:
        dbconfig = JRC.get_config("databases")
    except Exception as err:
        terminate_program(err)
    dbo = attrgetter(f"dis.{ARG.MANIFOLD}.{'write' if ARG.WRITE else 'read'}")(dbconfig)
    LOGGER.info(f"Connecting to {dbo.name} {ARG.MANIFOLD} on {dbo.host} as {dbo.user}")
    try:
        DB['dis'] = JRC.connect_database(dbo)
    except Exception as err:
        terminate_program(err)


def names_from(block, key='name'):
    ''' Pull the non-empty strings out of one of the registry's list-of-dicts
        fields, stripped. At least one Janelia record (SCR_026515) carries
        trailing whitespace in its official name, which breaks exact matching.
        Keyword arguments:
          block: list of dicts, or None
          key: dict key holding the string
        Returns:
          List of strings
    '''
    out = []
    for ent in block or []:
        val = ent.get(key) if isinstance(ent, dict) else ent
        if val and str(val).strip():
            out.append(str(val).strip())
    return out


def short_name(name):
    ''' Strip the institutional boilerplate the registry prepends and appends.
        Keyword arguments:
          name: official registry name
        Returns:
          Local-looking name
    '''
    out = re.sub(r'^Howard Hughes Medical Institute\s*(at)?\s*', '', name.strip(), flags=re.I)
    out = re.sub(r'^Janelia Research Campus\s*', '', out, flags=re.I)
    out = re.sub(r'\s*(Shared Resource\s*)?Core Facility\s*$', '', out, flags=re.I)
    return out.strip()


def fetch_resources():
    ''' Every Janelia resource of the requested type from the registry.
        Keyword arguments:
          None
        Returns:
          List of _source dicts
    '''
    body = {"size": 200, "query": {"query_string": {"query": f'"{ARG.TERM}"'}}}
    try:
        resp = SESSION.post(f"{ELASTIC}/{','.join(INDICES)}/_search", json=body, timeout=60)
    except Exception as err:
        terminate_program(err)
    if resp.status_code != 200:
        terminate_program(f"SciCrunch returned HTTP {resp.status_code}")
    out = []
    for hit in resp.json().get('hits', {}).get('hits', []):
        src = hit.get('_source') or {}
        types = names_from((src.get('item') or {}).get('types'))
        if ARG.TYPE and ARG.TYPE not in types:
            COUNT['wrong_type'] += 1
            continue
        out.append(src)
    LOGGER.info(f"Registry resources matching '{ARG.TERM}'"
                + (f" of type '{ARG.TYPE}'" if ARG.TYPE else "") + f": {len(out):,}")
    return out


def load_suporgs():
    ''' suporg records, keyed by name.
        Keyword arguments:
          None
        Returns:
          (dict of name -> record, sorted list of names)
    '''
    try:
        rows = list(DB['dis'].suporg.find({}))
    except Exception as err:
        terminate_program(err)
    byname = {}
    for row in rows:
        # Duplicate names exist (two "Molecular Genomics" rows share one code);
        # prefer the one flagged active so the association lands on the live row.
        cur = byname.get(row['name'])
        if cur is None or (row.get('active') and not cur.get('active')):
            byname[row['name']] = row
    LOGGER.info(f"Suporg records: {len(rows):,} ({len(byname):,} distinct names)")
    return byname, sorted(byname)


def build_entry(src):
    ''' The rrid sub-document for one registry record.
        Keyword arguments:
          src: registry _source dict
        Returns:
          dict
    '''
    item = src.get('item') or {}
    avail = names_from(item.get('availability'), 'keyword') \
            + names_from(item.get('availability'), 'description')
    types = names_from(item.get('types'))
    abrf = next((a for a in names_from(item.get('alternateIdentifiers'), 'identifier')
                 if a.startswith('ABRF_')), None)
    rid = str(item.get('identifier') or '').strip()
    name = str(item.get('name') or '').strip()
    entry = {"id": rid,
             "name": name,
             "citation": str((src.get('rrid') or {}).get('properCitation') or '').strip()
                         or f"{name} (RRID:{rid})",
             "type": ARG.TYPE if ARG.TYPE in types else (types[0] if types else None),
             "retired": any(RETIRED in a.upper() for a in avail),
             "curator": ARG.CURATOR,
             "checked": datetime.now()}
    if abrf:
        entry['abrf'] = abrf
    return entry


def choose(src, byname, names):
    ''' Decide which suporg a registry record belongs to.
        Keyword arguments:
          src: registry _source dict
          byname: dict of suporg name -> record
          names: sorted suporg names
        Returns:
          suporg name, or None to skip
    '''
    item = src.get('item') or {}
    official = str(item.get('name') or '').strip()
    rid = str(item.get('identifier') or '').strip()
    short = short_name(official)
    # Tier 1: the registry's own synonyms, verbatim.
    for syn in names_from(item.get('synonyms')) + [short]:
        if syn in byname:
            LOGGER.debug(f"{rid}: exact synonym match on '{syn}'")
            COUNT['exact'] += 1
            return syn if ARG.YES or confirm_exact(rid, official, syn) else None
    # Tier 2/3: fuzzy shortlist. token_sort_ratio only - see the module docstring.
    scored = process.extract(short, names, scorer=fuzz.token_sort_ratio,
                             processor=utils.default_process, limit=SHORTLIST)
    scored = [s for s in scored if s[1] >= FLOOR]
    if not scored:
        LOGGER.info(f"{rid}: no candidate for '{short}'")
        COUNT['no_candidate'] += 1
        return None
    COUNT['strong' if scored[0][1] >= STRONG else 'weak'] += 1
    if ARG.YES:
        # Unattended runs take only the exact tier; a fuzzy hit always needs eyes.
        LOGGER.info(f"{rid}: '{short}' needs review (best {scored[0][0]} @ {scored[0][1]:.0f})")
        COUNT['needs_review'] += 1
        return None
    return prompt(rid, official, short, scored)


def confirm_exact(rid, official, syn):
    ''' Confirm an exact-synonym association.
        Keyword arguments:
          rid: RRID
          official: official registry name
          syn: matched suporg name
        Returns:
          True to accept
    '''
    print(f"\n  RRID:{rid}\n    registry : {official}\n    suporg   : {syn}   (exact synonym)")
    return input("    accept? [Y/n] ").strip().lower() in ('', 'y', 'yes')


def prompt(rid, official, short, scored):
    ''' Offer a shortlist and let the user pick.
        The whole list is shown rather than just the best match because the score
        does not rank truth reliably at this end of the range.
        Keyword arguments:
          rid: RRID
          official: official registry name
          short: boilerplate-stripped name
          scored: list of (name, score, index)
        Returns:
          Chosen suporg name, or None
    '''
    print(f"\n  RRID:{rid}\n    registry : {official}\n    stripped : {short}")
    for idx, (name, score, _) in enumerate(scored, 1):
        print(f"      {idx}) {name:44} {score:5.0f}")
    print("      s) skip      q) quit")
    while True:
        ans = input("    choose: ").strip().lower()
        if ans in ('s', ''):
            COUNT['skipped'] += 1
            return None
        if ans == 'q':
            raise KeyboardInterrupt
        if ans.isdigit() and 1 <= int(ans) <= len(scored):
            return scored[int(ans) - 1][0]
        print("      enter a number, s, or q")


def html_table(entries):
    ''' Zebra-striped table of associations for the run-summary email.
        No per-cell border-radius: plain background-color striping is what
        survives Outlook's Word rendering engine.
        Keyword arguments:
          entries: list of (suporg name, rrid entry) tuples
        Returns:
          HTML table
    '''
    rows = []
    for i, (sup, ent) in enumerate(entries):
        striped = i % 2 == 0
        bgattr = f' bgcolor="{JE.STRIPE_BG}"' if striped else ''
        bgs = f'background-color:{JE.STRIPE_BG};' if striped else ''
        badge = JE.pill(JE.RED_BG, JE.RED, 'Retired') if ent.get('retired') \
                else JE.pill(JE.GREEN_BG, JE.GREEN, '&#10003; Active')
        rows.append(
            f'<tr{bgattr} style="{bgs}">'
            f'<td style="padding:8px 10px;">{html.escape(sup)}</td>'
            f'<td style="padding:8px 10px;color:{JE.GRAY};">RRID:{html.escape(ent["id"])}</td>'
            f'<td style="padding:8px 10px;color:{JE.GRAY};">'
            f'{html.escape(str(ent.get("abrf") or ""))}</td>'
            f'<td style="padding:8px 10px;" align="right">{badge}</td></tr>')
    rows.append('<tr><td colspan="4" style="height:1px;line-height:1px;font-size:1px;">'
                '&nbsp;</td></tr>')
    return ('<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
            'style="border-collapse:collapse;font-size:12.5px;">' + "".join(rows) + '</table>')


def generate_email(entries, resources):
    ''' Send the HTML run-summary email.
        Keyword arguments:
          entries: list of (suporg name, rrid entry) tuples
          resources: number of registry resources considered
        Returns:
          None
    '''
    run_data = (JRC.get_run_data(__file__, __version__).strip()
                + f" &middot; manifold: {ARG.MANIFOLD}")
    mode_label = 'WRITE' if ARG.WRITE else 'DRY RUN'
    mode_tone = 'good' if ARG.WRITE else 'warn'
    retired = sum(1 for _, e in entries if e.get('retired'))
    kpis = ''.join([
        JE.kpi_card(f"{resources:,}", "Registry resources", width='25%'),
        JE.kpi_card(f"{COUNT['exact']:,}", "Exact matches", width='25%'),
        JE.kpi_card(f"{len(entries):,}", "Associated", 'good', width='25%'),
        JE.kpi_card(f"{retired:,}", "Retired", 'warn' if retired else 'neutral', width='25%'),
    ])
    body = JE.body_row(JE.section_header(f"&#128279; RRIDs Associated ({len(entries):,})")
                       + html_table(entries))
    msg = JE.render(os.path.basename(__file__), __version__, run_data,
                    mode_label, mode_tone, kpis, body)
    email = DIS['developer'] if ARG.TEST else DIS['receivers']
    try:
        LOGGER.info(f"Sending email to {email}")
        JRC.send_email(msg, DIS['sender'], email, 'RRIDs associated with suporgs', mime='html')
    except Exception as err:
        LOGGER.error(err)


def persist(entries, byname):
    ''' Store the chosen associations, or print them when this is a dry run.
        Matched on name AND code so a duplicate-named suporg cannot take the
        write intended for its twin.
        Keyword arguments:
          entries: list of (suporg name, rrid entry) tuples
          byname: dict of suporg name -> record
        Returns:
          None
    '''
    if not ARG.WRITE:
        for sup, ent in entries:
            print(f"  {sup}  <-  RRID:{ent['id']}  {ent.get('abrf') or ''}"
                  + ("  [RETIRED]" if ent['retired'] else ""))
        if entries:
            print(f"\nDry run: nothing written. Re-run with --write to store "
                  f"{len(entries):,} association(s).", file=sys.stderr)
        return
    for sup, ent in entries:
        try:
            DB['dis'].suporg.update_one({"name": sup, "code": byname[sup].get('code')},
                                        {"$set": {"rrid": ent}})
            COUNT['written'] += 1
        except Exception as err:
            terminate_program(err)


def processing():
    ''' Main routine
        Keyword arguments:
          None
        Returns:
          None
    '''
    byname, names = load_suporgs()
    resources = fetch_resources()
    entries = []
    try:
        for src in resources:
            rid = str((src.get('item') or {}).get('identifier') or '').strip()
            if not rid:
                COUNT['no_identifier'] += 1
                continue
            existing = DB['dis'].suporg.find_one({"rrid.id": rid})
            if existing and not ARG.REFRESH:
                LOGGER.debug(f"{rid}: already on '{existing['name']}', leaving it alone")
                COUNT['already_present'] += 1
                continue
            sup = choose(src, byname, names)
            if not sup:
                continue
            entries.append((sup, build_entry(src)))
            COUNT['associated'] += 1
    except KeyboardInterrupt:
        LOGGER.warning("Stopped by user; keeping what was chosen so far")
    persist(entries, byname)
    if entries and (ARG.TEST or ARG.WRITE):
        generate_email(entries, len(resources))
    print(file=sys.stderr)
    for key in sorted(COUNT):
        print(f"{key + ':':<20} {COUNT[key]:,}", file=sys.stderr)


# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Associate SciCrunch RRIDs with suporg records")
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, prod)')
    PARSER.add_argument('--term', dest='TERM', action='store', default='Janelia',
                        help='Registry search phrase (default: Janelia)')
    PARSER.add_argument('--type', dest='TYPE', action='store', default='core facility',
                        help='Registry item type to keep ("" for all)')
    PARSER.add_argument('--curator', dest='CURATOR', action='store',
                        default=os.environ.get('USER', 'unknown'),
                        help='Curator recorded on each association')
    PARSER.add_argument('--refresh', dest='REFRESH', action='store_true', default=False,
                        help='Re-examine RRIDs already stored')
    PARSER.add_argument('--yes', dest='YES', action='store_true', default=False,
                        help='Unattended: take exact synonym matches only, never fuzzy')
    PARSER.add_argument('--test', dest='TEST', action='store_true',
                        default=False, help='Send email to developer')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Write to database')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    LOGGER.info(f"Started run (version {__version__})")
    try:
        DIS = JRC.simplenamespace_to_dict(JRC.get_config("dis"))
    except Exception as err:
        terminate_program(err)
    initialize_program()
    processing()
    terminate_program()
