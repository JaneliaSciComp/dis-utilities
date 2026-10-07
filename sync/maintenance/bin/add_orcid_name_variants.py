''' add_orcid_name_variants.py
    Enrich the given/family name lists in the orcid collection with name
    variants published on the author's own ORCID record.

    Every non-alumni orcid-collection record that carries an ORCID is fetched
    from the ORCID public API. Four fields are mined - name/given-names,
    name/family-name, name/credit-name and other-names - and anything not
    already held is proposed as a new variant.

    A proposal carrying a middle initial is offered in both spellings -
    "Abigail R." and "Abigail R" - since publishers are inconsistent about the
    period and holding one form loses matches on the other. The two are one
    decision, accepted or skipped together.

    Candidates are split into two tiers, because the data is not uniformly
    trustworthy:

      APPLY   A given-name variant whose trailing family name matches one we
              already hold. The split is unambiguous, so --write stores it.
      REVIEW  Everything else: family-name changes (marriage, hyphenation,
              transliteration), names that cannot be split against a known
              family name, and initials-only forms. These are reported for a
              human to decide and are never written.

    Names in non-Latin scripts are skipped, since a reviewer cannot judge a
    script they do not read; --non-latin brings them in as review candidates.
    Accented Latin names - "Lösche", "Núñez", "Nguyễn" - are Latin and are
    never affected by that flag.

    Every run writes a TSV carrying the full name context for each proposal,
    not just the proposal itself. --review then walks that list one screen at a
    time - what we hold, what ORCID holds, what is proposed and why, and how
    many DOIs the person has - and takes a decision for each:

      y  apply as shown        e  edit the value, choose the field
      g  put it in given       n  skip
      f  put it in family      b  back, undoing the previous decision
                               q  stop here

    Nothing is stored until the walk ends, and then only with --write and a
    final confirmation. Pointing --review at an existing --file replays that
    scan instead of re-fetching, so a session can be finished later.

    This complements fix_middle_names.py, which derives variants
    mechanically from names already present (middle-initial punctuation, bare
    first names) without consulting ORCID. Run this first to bring in the
    variants ORCID knows about, then fix_middle_names.py to permute them.

    An HTML summary email is sent when --write stores at least one variant, or
    whenever --test is supplied; the full candidate list is attached as a TSV.
'''

__version__ = '1.0.1'

import argparse
import collections
import csv
import html
import os
from operator import attrgetter
import sys
import tempfile
import time
import traceback
import requests
from tqdm import tqdm
import jrc_common.jrc_common as JRC
import jrc_email.jrc_email as JE
from dis_review_lib import (apply_decisions, candidate_forms, review_candidates,
                            summarize)
from dis_name_lib import (exact_key, initial_variants, is_initials_only,
                          is_latin, normalize, split_against_family,
                          split_against_given, strip_suffixes, uninvert)

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Database
DB = {}
# Counters
COUNT = collections.defaultdict(lambda: 0, {})
# Global variables
ARG = DIS = LOGGER = None
# Candidates: lists of dicts, keyed by tier
APPLY = []
REVIEW = []
# DOI counts, cached per ORCID
DOIS = {}
# TSV columns, in order. The file is a complete record of a scan: --review
# replays it without re-fetching 437 ORCID records.
TSV_FIELDS = ['tier', 'orcid', 'name', 'userId', 'field', 'value', 'variants', 'source', 'note',
              'cur_given', 'cur_family', 'o_given', 'o_family', 'o_credit', 'o_other',
              'dois']
# ORCID rate-limit handling (~24 req/s, bursts -> 503, plus daily quotas)
ORCID_MAX_RETRIES = 4
ORCID_BACKOFF = 2
ORCID_PAUSE = 0.08
TIMEOUT = (requests.exceptions.ConnectTimeout, requests.exceptions.ReadTimeout,
           requests.exceptions.Timeout)
# Palette for the bespoke candidate tables that jrc_email does not cover.
EMAIL_NAVY = '#1f3a5f'
EMAIL_GRAY = '#5b6b7c'
EMAIL_STRIPE_BG = '#f7f9fb'


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
    ''' Initialize database connection
        Keyword arguments:
          None
        Returns:
          None
    '''
    try:
        dbconfig = JRC.get_config("databases")
    except Exception as err:
        terminate_program(err)
    for source in ['dis']:
        dbo = attrgetter(f"{source}.{ARG.MANIFOLD}.write")(dbconfig)
        LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, ARG.MANIFOLD, dbo.host, dbo.user)
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)


def orcid_get(oid, path='person'):
    ''' GET a section of an ORCID record from the public API, retrying with
        backoff on rate-limit responses (429/503) and transient timeouts.
        Keyword arguments:
          oid: ORCID iD
          path: record section to fetch
        Returns:
          Parsed JSON dict on HTTP 200, else None. Never raises on HTTP status
          or JSON-decode errors.
    '''
    url = f"https://pub.orcid.org/v3.0/{oid}/{path}"
    for attempt in range(1, ORCID_MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers={"Accept": "application/json"}, timeout=15)
        except TIMEOUT:
            LOGGER.warning(f"ORCID request timed out ({attempt}/{ORCID_MAX_RETRIES}): {oid}")
            if attempt < ORCID_MAX_RETRIES:
                time.sleep(ORCID_BACKOFF * attempt)
            continue
        except requests.exceptions.RequestException as err:
            LOGGER.warning(f"ORCID request failed for {oid}: {err}")
            return None
        if resp.status_code == 200:
            try:
                return resp.json()
            except ValueError:
                LOGGER.warning(f"ORCID returned non-JSON for {oid}")
                return None
        if resp.status_code in (429, 503):
            retry_after = resp.headers.get('Retry-After', '')
            wait = int(retry_after) if retry_after.isdigit() else ORCID_BACKOFF * attempt
            LOGGER.warning(f"ORCID rate-limited ({resp.status_code}); waiting {wait}s "
                           f"({attempt}/{ORCID_MAX_RETRIES})")
            if attempt < ORCID_MAX_RETRIES:
                time.sleep(wait)
            continue
        # Deprecated (301), not found (404), deactivated/locked (409), etc.
        LOGGER.debug(f"ORCID {resp.status_code} for {oid}")
        COUNT['unavailable'] += 1
        return None
    LOGGER.warning(f"ORCID gave up after {ORCID_MAX_RETRIES} attempts: {oid}")
    COUNT['unavailable'] += 1
    return None











def doi_count(rec):
    ''' Count the DOIs credited to a person, as a measure of how much a name
        variant is worth. Only called for records that produced a candidate,
        so the cost is a few dozen queries rather than one per employee.
        Keyword arguments:
          rec: orcid collection record
        Returns:
          Number of DOIs, or 0 when the person has no employee ID
    '''
    if not rec.get('employeeId'):
        return 0
    if rec['orcid'] in DOIS:
        return DOIS[rec['orcid']]
    try:
        DOIS[rec['orcid']] = DB['dis']['dois'].count_documents(
            {'jrc_author': rec['employeeId']})
    except Exception as err:
        LOGGER.warning(f"Could not count DOIs for {rec['orcid']}: {err}")
        DOIS[rec['orcid']] = 0
    return DOIS[rec['orcid']]


def add_candidate(rec, value, source, tier, note='', person=None, variants=None):
    ''' Record a proposed variant, with enough context for a reviewer to judge
        it without going back to ORCID.
        Keyword arguments:
          rec: orcid collection record
          value: proposed name
          source: ORCID field it came from
          tier: 'apply' or 'review'
          note: reason, for review candidates
          person: the ORCID person record, for the name context columns
        Returns:
          None
    '''
    name = (person or {}).get('name') or {}
    others = [entry.get('content') for entry
              in ((person or {}).get('other-names') or {}).get('other-name', [])
              if entry.get('content')]
    cand = {'tier': tier.upper(),
            'orcid': rec['orcid'],
            'name': f"{(rec.get('given') or ['?'])[0]} {(rec.get('family') or ['?'])[0]}",
            'userId': rec.get('userIdO365', ''),
            'field': 'given' if tier == 'apply' else note.split(':')[0],
            'value': value,
            'variants': ' | '.join(variants or [value]),
            'source': source,
            'note': note,
            'cur_given': ', '.join(rec.get('given') or []),
            'cur_family': ', '.join(rec.get('family') or []),
            'o_given': (name.get('given-names') or {}).get('value') or '',
            'o_family': (name.get('family-name') or {}).get('value') or '',
            'o_credit': (name.get('credit-name') or {}).get('value') or '',
            'o_other': ' | '.join(others),
            'dois': doi_count(rec)}
    (APPLY if tier == 'apply' else REVIEW).append(cand)
    COUNT[f"{tier}_{source}"] += 1


def accept_given(rec, value, source, person, held):
    ''' Queue a given-name variant for the APPLY tier, expanded into both
        middle-initial spellings, and remember what was queued.
        Keyword arguments:
          rec: orcid collection record
          value: given name
          source: ORCID field it came from
          person: ORCID person record
          held: name sets for this record
        Returns:
          None
    '''
    forms = [f for f in initial_variants(value) if exact_key(f) not in held['given_exact']]
    add_candidate(rec, value, source, 'apply', person=person, variants=forms)
    held['given'].add(normalize(value))
    held['given_exact'].update(exact_key(f) for f in forms)


def process_given_names(rec, name, person, held):
    ''' Mine the ORCID name/given-names field. It is usually a bare given name,
        but plenty of ORCID users put their whole name in it ("Kari Close",
        where Close is the family name). Splitting first keeps the family name
        out of the given list; when no known family name is present the field
        is taken at face value.
        Keyword arguments:
          rec: orcid collection record
          name: the ORCID name block
          person: ORCID person record
          held: name sets for this record
        Returns:
          None
    '''
    value = (name.get('given-names') or {}).get('value')
    if value:
        trimmed, _ = split_against_family(value, held['families'])
        if trimmed is not None:
            value = trimmed
    if not value or normalize(value) in held['given']:
        return
    if not is_latin(value):
        if not ARG.NONLATIN:
            COUNT['non_latin'] += 1
            return
        add_candidate(rec, value, 'given-names', 'review',
                      'given: non-Latin script', person=person)
    elif is_initials_only(value):
        add_candidate(rec, value, 'given-names', 'review',
                      'given: initials only', person=person)
    else:
        accept_given(rec, value, 'given-names', person, held)


def process_unsplit_name(rec, source, raw, full, person, held):
    ''' Handle a name carrying no family name we hold. Either it is a bare
        given name ("Jefferson"), or the person publishes under a family name
        we do not have - a maiden name, a hyphenation, a restored diacritic.
        None of these are safe to apply unseen.
        Keyword arguments:
          rec: orcid collection record
          source: ORCID field it came from
          raw: the name as ORCID holds it
          full: the name after suffix and inversion cleanup
          person: ORCID person record
          held: name sets for this record
        Returns:
          None
    '''
    if normalize(full) in held['given']:
        COUNT['already_held'] += 1
        return
    # "Allison Sowell", when we hold the given name Allison, is a family name
    # of Sowell - not a family name of "Allison Sowell". Matching a given name
    # against the front and proposing what follows is what makes these usable:
    # the reviewer accepts a surname instead of retyping one.
    surname = split_against_given(full, rec.get('given') or [])
    if surname:
        if normalize(surname) in held['family']:
            COUNT['already_held'] += 1
            return
        add_candidate(rec, surname, source, 'review',
                      'family: former or alternate surname', person=person)
        held['family'].add(normalize(surname))
        return
    note = 'given: bare name, no family name to split on' \
           if ' ' not in full.strip() else 'family: no known family name in it'
    add_candidate(rec, raw, source, 'review', note, person=person)


def process_alternate_name(rec, source, raw, person, held):
    ''' Mine one credit-name or other-name. These are full names, so each must
        be split before either part can be used.
        Keyword arguments:
          rec: orcid collection record
          source: 'credit-name' or 'other-name'
          raw: the name as ORCID holds it
          person: ORCID person record
          held: name sets for this record
        Returns:
          None
    '''
    if not raw or not raw.strip():
        return
    if not is_latin(raw):
        # Reviewers cannot judge a script they do not read, so these are
        # dropped unless asked for. The count is still reported.
        if not ARG.NONLATIN:
            COUNT['non_latin'] += 1
            return
        add_candidate(rec, raw, source, 'review', 'given: non-Latin script', person=person)
        return
    # Suffixes come off first: "David E. Clapham MD, PhD" has a comma that
    # uninvert would otherwise read as a Family, Given separator.
    full = uninvert(strip_suffixes(raw))
    if not full:
        return
    given, _ = split_against_family(full, held['families'])
    if given is None:
        process_unsplit_name(rec, source, raw, full, person, held)
        return
    if not given:
        COUNT['family_only'] += 1
        return
    if is_initials_only(given):
        # Compared against the exact spellings rather than the normalized ones:
        # "M.A." and "M A" are different spellings, publishers use both, and
        # holding both is the point - so only a literal duplicate is skipped.
        # Without this the same initials were proposed on every run, however
        # many times they had already been accepted.
        if exact_key(given) in held['given_exact']:
            COUNT['already_held'] += 1
            return
        add_candidate(rec, given, source, 'review', 'given: initials only', person=person)
        return
    if normalize(given) in held['given']:
        COUNT['already_held'] += 1
        return
    accept_given(rec, given, source, person, held)


def process_record(rec):
    ''' Compare one orcid-collection record against its ORCID record and
        collect every name variant ORCID holds that we do not.
        Keyword arguments:
          rec: orcid collection record
        Returns:
          None
    '''
    COUNT['read'] += 1
    person = orcid_get(rec['orcid'])
    if person is None:
        return
    COUNT['fetched'] += 1
    # given_exact is a period-sensitive twin of given: normalize() cannot tell
    # the two spellings of a middle initial apart, and both are wanted.
    held = {'given': {normalize(g) for g in (rec.get('given') or [])},
            'given_exact': {exact_key(g) for g in (rec.get('given') or [])},
            'family': {normalize(f) for f in (rec.get('family') or [])},
            'families': rec.get('family') or []}
    name = person.get('name') or {}
    process_given_names(rec, name, person, held)
    # A family name we do not hold is a real finding - marriage, hyphenation,
    # a restored diacritic - but it is also where ORCID data is wrongest, so it
    # never auto-applies.
    o_family = (name.get('family-name') or {}).get('value')
    if o_family and normalize(o_family) not in held['family']:
        add_candidate(rec, o_family, 'family-name', 'review',
                      'family: not currently held', person=person)
    # credit-name and other-names are full names and must be split.
    alternates = [('credit-name', (name.get('credit-name') or {}).get('value'))]
    alternates += [('other-name', entry.get('content'))
                   for entry in (person.get('other-names') or {}).get('other-name', [])]
    for source, raw in alternates:
        process_alternate_name(rec, source, raw, person, held)


def apply_variants():
    ''' Write accepted given-name variants back to the orcid collection. Each
        record is updated once with every variant found for it, using $addToSet
        so a concurrent run cannot duplicate an entry.
        Keyword arguments:
          None
        Returns:
          None
    '''
    coll = DB['dis']['orcid']
    by_orcid = collections.defaultdict(list)
    for cand in APPLY:
        by_orcid[cand['orcid']].extend(candidate_forms(cand))
    for oid, values in by_orcid.items():
        try:
            result = coll.update_one({'orcid': oid},
                                     {'$addToSet': {'given': {'$each': values}}})
        except Exception as err:
            terminate_program(err)
        if result.modified_count:
            COUNT['written'] += len(values)
        else:
            LOGGER.warning(f"{oid}: no record updated")


def write_tsv(path):
    ''' Write every candidate to a TSV. The file carries the full name context,
        not just the proposal, so --review can drive an interactive session
        from it without going back to ORCID.
        Keyword arguments:
          path: output file path
        Returns:
          None
    '''
    with open(path, 'w', encoding='utf-8', newline='') as handle:
        writer = csv.DictWriter(handle, delimiter='\t', extrasaction='ignore',
                                fieldnames=TSV_FIELDS)
        writer.writeheader()
        for cand in APPLY + REVIEW:
            writer.writerow(cand)


def read_tsv(path):
    ''' Reload candidates written by an earlier run
        Keyword arguments:
          path: TSV path
        Returns:
          List of candidate dicts
    '''
    try:
        with open(path, encoding='utf-8', newline='') as handle:
            rows = list(csv.DictReader(handle, delimiter='\t'))
    except OSError as err:
        terminate_program(err)
    missing = set(TSV_FIELDS) - set(rows[0].keys() if rows else TSV_FIELDS)
    if missing:
        terminate_program(f"{path} is missing columns: {', '.join(sorted(missing))}")
    for row in rows:
        row['dois'] = int(row.get('dois') or 0)
    return rows







def current_names(cand):
    ''' The list a proposal would join, as held today. Derived here rather than
        stored on the candidate: a --review --file replay builds candidates
        straight from the TSV and never passes through add_candidate, so any
        key set there is absent on a replayed row.
        Keyword arguments:
          cand: candidate dict
        Returns:
          Comma-joined names
    '''
    return cand['cur_given'] if cand.get('field') == 'given' else cand['cur_family']


def html_candidate_table(rows, empty):
    ''' Build a candidate table for the summary email
        Keyword arguments:
          rows: candidate dicts
          empty: text to show when there are none
        Returns:
          HTML string
    '''
    if not rows:
        return f'<div style="font-size:13px;color:{EMAIL_GRAY};padding:4px 10px">{empty}</div>'
    head = ''.join(f'<td style="padding:6px 10px">{col}</td>'
                   for col in ('Author', 'Proposed', 'From', 'Currently held'))
    body = ''
    for idx, cand in enumerate(rows):
        stripe = f' style="background-color:{EMAIL_STRIPE_BG}"' if idx % 2 else ''
        if cand['userId']:
            author = (f'<a href="https://dis.int.janelia.org/userui/{html.escape(cand["userId"])}"'
                      f' style="color:{EMAIL_NAVY};text-decoration:none;font-weight:600">'
                      f'{html.escape(cand["name"])}</a>')
        else:
            author = html.escape(cand['name'])
        note = f' <span style="color:{EMAIL_GRAY}">({html.escape(cand["note"])})</span>' \
               if cand['note'] else ''
        body += (f'<tr{stripe}><td style="padding:8px 10px;white-space:nowrap">{author}</td>'
                 f'<td style="padding:8px 10px"><b>{html.escape(cand["value"])}</b>{note}</td>'
                 f'<td style="padding:8px 10px">{html.escape(cand["source"])}</td>'
                 f'<td style="padding:8px 10px;color:{EMAIL_GRAY}">'
                 f'{html.escape(current_names(cand))}</td></tr>')
    return ('<table style="border-collapse:collapse;font-size:12.5px;width:100%">'
            f'<tbody><tr style="color:{EMAIL_GRAY};text-transform:uppercase;'
            f'letter-spacing:.03em">{head}</tr>{body}</tbody></table>')


def generate_email(fname):
    ''' Generate and send the HTML run-summary email
        Keyword arguments:
          fname: path to the candidate TSV
        Returns:
          None
    '''
    run_data = JRC.get_run_data(__file__, __version__).strip()
    run_data += f" &middot; manifold: {ARG.MANIFOLD}"
    mode_label = 'WRITE' if ARG.WRITE else 'DRY RUN'
    mode_tone = 'good' if ARG.WRITE else 'warn'
    kpis = ''.join([
        JE.kpi_card(f"{COUNT['read']:,}", "Records read", width='25%'),
        JE.kpi_card(f"{len(APPLY):,}", "Variants found",
                    'good' if APPLY else 'neutral', '25%'),
        JE.kpi_card(f"{COUNT['written']:,}", "Applied",
                    'good' if COUNT['written'] else 'neutral', '25%'),
        JE.kpi_card(f"{len(REVIEW):,}", "Need review",
                    'warn' if REVIEW else 'neutral', '25%'),
    ])
    body = JE.body_row(
        JE.section_header(f"&#9989; Given-name variants ({len(APPLY):,})")
        + html_candidate_table(APPLY, "No new given-name variants were found."))
    body += JE.body_row(
        JE.section_header(f"&#9888; Needs review ({len(REVIEW):,})")
        + html_candidate_table(REVIEW, "Nothing needs review."))
    msg = JE.render(os.path.basename(__file__), __version__, run_data,
                    mode_label, mode_tone, kpis, body)
    subject = "ORCID name variants for the orcid collection"
    email = DIS['developer'] if ARG.TEST else DIS['receivers']
    attach = fname if os.path.exists(fname) else None
    try:
        LOGGER.info(f"Sending email to {email}")
        JRC.send_email(msg, DIS['sender'], email, subject, attachment=attach, mime='html')
    except Exception as err:
        print(str(err))
        traceback.print_exc()
        terminate_program(err)


def processing():
    ''' Fetch every candidate record, compare names, and report
        Keyword arguments:
          None
        Returns:
          None
    '''
    fname = ARG.FILE or os.path.join(tempfile.gettempdir(), 'orcid_name_variants.tsv')
    if ARG.REVIEW and ARG.FILE:
        # Replay a previous scan. Nothing is fetched, so a review session can
        # be picked up later without hammering ORCID again.
        cands = read_tsv(fname)
        for cand in cands:
            (APPLY if cand['tier'] == 'APPLY' else REVIEW).append(cand)
        LOGGER.info(f"Loaded {len(cands):,} candidates from {fname}")
    else:
        # Alumni are excluded deliberately: their names no longer change, and
        # the collection carries more alumni than current staff.
        payload = {'orcid': {'$exists': True}, 'alumni': {'$exists': False}}
        try:
            recs = list(DB['dis']['orcid'].find(payload))
        except Exception as err:
            terminate_program(err)
        LOGGER.info(f"Found {len(recs):,} non-alumni records with an ORCID")
        for rec in tqdm(recs, desc='ORCID records', disable=not ARG.VERBOSE):
            process_record(rec)
            time.sleep(ORCID_PAUSE)
        write_tsv(fname)
        print(f"Records read:            {COUNT['read']:,}")
        print(f"ORCID records fetched:   {COUNT['fetched']:,}")
        print(f"Records unavailable:     {COUNT['unavailable']:,}")
        print(f"Variants already held:   {COUNT['already_held']:,}")
        if COUNT['non_latin']:
            label = 'included' if ARG.NONLATIN else 'skipped (--non-latin to include)'
            print(f"Non-Latin names:         {COUNT['non_latin']:,} {label}")
    print(f"Given-name variants:     {len(APPLY):,}")
    print(f"Needing review:          {len(REVIEW):,}")
    print(f"Candidate list:          {fname}")
    if ARG.REVIEW:
        interactive_session()
    elif ARG.WRITE and APPLY:
        apply_variants()
        print(f"Variants written:        {COUNT['written']:,}")
    if ARG.TEST or (ARG.WRITE and COUNT['written']):
        generate_email(fname)


def orcid_context(cand):
    """ The ORCID side of a candidate, as review-block rows. Built here rather
        than in add_candidate because a --review --file replay loads candidates
        straight from the TSV and never passes through it.
        Keyword arguments:
          cand: candidate dict
        Returns:
          List of (label, value) pairs
    """
    # The label column is padded by the renderer, so a continuation row carries
    # no indent of its own - adding one would double it.
    rows = [('ORCID has', f"given    {cand.get('o_given') or '-'}"),
            ('', f"family   {cand.get('o_family') or '-'}")]
    if cand.get('o_credit'):
        rows.append(('', f"credit   {cand['o_credit']}"))
    if cand.get('o_other'):
        rows.append(('', f"other    {cand['o_other']}"))
    return rows


def interactive_session():
    ''' Run the reviewer over the selected tiers and store what was accepted
        Keyword arguments:
          None
        Returns:
          None
    '''
    if not sys.stdin.isatty():
        terminate_program("--review needs a terminal; run it without a pipe or redirect")
    pool = {'apply': list(APPLY), 'review': list(REVIEW), 'all': APPLY + REVIEW}[ARG.TIER]
    if not pool:
        print(f"\nNothing to review in the '{ARG.TIER}' tier.")
        return
    # Grouped by person, so someone with several proposals is decided in one
    # pass rather than turning up again forty screens later.
    pool.sort(key=lambda c: (c['name'].lower(), c['tier'] != 'APPLY'))
    for cand in pool:
        cand['extra'] = orcid_context(cand)
    accepted = review_candidates(pool, no_color=ARG.NOCOLOR)
    summarize(accepted, no_color=ARG.NOCOLOR)
    if not accepted:
        return
    if not ARG.WRITE:
        print("\nDry run - rerun with --write to store these.")
        return
    try:
        confirm = input(f"\nApply {len(accepted):,} change(s) to the orcid collection? [y/N] ")
    except (EOFError, KeyboardInterrupt):
        print("\nCancelled.")
        return
    if confirm.strip().lower() not in ('y', 'yes'):
        print("Cancelled - nothing written.")
        return
    try:
        COUNT['written'] = apply_decisions(DB['dis']['orcid'], accepted, LOGGER)
    except Exception as err:
        terminate_program(err)
    print(f"Variants written:        {COUNT['written']:,}")


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Add name variants from ORCID to the orcid collection")
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, prod)')
    PARSER.add_argument('--review', dest='REVIEW', action='store_true',
                        default=False,
                        help='Step through each proposal interactively')
    PARSER.add_argument('--tier', dest='TIER', action='store',
                        default='all', choices=['all', 'apply', 'review'],
                        help='Which proposals to review ([all], apply, review)')
    PARSER.add_argument('--file', dest='FILE', action='store',
                        default=None,
                        help='TSV path; with --review, replay it instead of rescanning')
    PARSER.add_argument('--non-latin', dest='NONLATIN', action='store_true',
                        default=False,
                        help='Include names in non-Latin scripts (skipped by default)')
    PARSER.add_argument('--no-color', dest='NOCOLOR', action='store_true',
                        default=False, help='Flag, plain output')
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
