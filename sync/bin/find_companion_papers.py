''' find_companion_papers.py
    Find companion papers - the same work published twice by one group, usually a
    method and its protocol, or an article and its resource paper.

    The motivating case: Nature Protocols 10.1038/s41596-026-01427-w and Nature
    Methods 10.1038/s41592-025-02664-9 are the protocol and method for TEMI, and
    nature.com links them under "Associated content". That link is editorial and
    page-only - it is not in Crossref's relation field, the Springer Meta API,
    OpenAlex related_works, or Europe PMC. The only machine-readable trace is
    that one cites the other, which is far too weak on its own: that paper cites
    29 works and only one is its companion.

    So the pair is inferred from three signals that hold together:

      journal   both in the same publisher family (--publisher), but not
                necessarily the same journal - Nature Methods and Nature
                Protocols are the archetype
      authors   a substantial shared author set, measured against the smaller
                author list so a long-author paper does not dilute the score
      title     near-identical title stem. token_set_ratio is used because a
                companion title is usually a subset of the other's words ("TEMI:
                tissue-expansion mass-spectrometry imaging" inside "Tissue
                expansion mass spectrometry imaging (TEMI) for..."), and
                hyphenation differs between the two, so titles are normalised
                before comparison.

    token_set_ratio alone is unsafe - it scores 100 whenever one title's tokens
    are a subset of the other's, which is true of many unrelated papers from one
    group. It is only trusted in combination with the author overlap.

    Preprint pairs are deliberately not reported. That relationship belongs to
    jrc_preprint, which update_preprints.py owns; a second program writing the
    same link by a different route would let the two diverge. They are still
    classified, so the run counts how many were dropped.

    Read-only by default: the run reports candidate pairs. --write stores them
    as jrc_companion on both DOIs.
'''

__version__ = '1.13.0'

import argparse
import collections
import html
import itertools
import json
import os
import re
import sys
from datetime import datetime
from operator import attrgetter
import jrc_common.jrc_common as JRC
import jrc_email.jrc_email as JE
import doi_common.doi_common as DL
from rapidfuzz import fuzz

# pylint: disable=broad-exception-caught,logging-fstring-interpolation,logging-not-lazy

# Database
DB = {}
# Global variables
ARG = DIS = LOGGER = None
# Counters
COUNT = collections.defaultdict(lambda: 0, {})
# Titles differ in hyphenation between a method and its protocol
# ("tissue-expansion" vs "tissue expansion"), so every dash becomes a space.
DASHES = re.compile(r'[‐-―−-]')
NONWORD = re.compile(r'[^a-z0-9 ]')
# A matched pair is not always two companion works. These classify what kind of
# relationship it is, so the caller can ask for the one it wants.
CORRECTION_RE = re.compile(r'^\s*(correction|erratum|retraction|addendum|'
                           r'publisher correction|author correction|comment on)\b', re.I)
# Preprint servers and SSRN: the same paper in two venues, not a companion work.
# 10.1101 is deliberately absent - it belongs to Cold Spring Harbor Laboratory
# Press, which registers bioRxiv AND its own journals (Cold Spring Harbor
# Protocols, Genes & Development, CSH Perspectives, Learning & Memory, Genome
# Research). Treating the whole prefix as preprint miscalled 44 CSH journal
# articles, including a Current Opinion review and its CSH Protocols protocol -
# a companion pair. bioRxiv is identified by its suffix instead.
PREPRINT_PREFIXES = ('10.2139', '10.21203', '10.31234', '10.26434', '10.20944')
# bioRxiv and medRxiv suffixes begin with a date (2025.02.22.639343) or a bare
# accession number; CSH journal suffixes begin with a journal code (pdb., gad.).
BIORXIV_SUFFIX = re.compile(r'^\d')
# Venues whose records are largely conference abstracts of a full paper published
# elsewhere. Page shape is not a reliable test - Microscopy and Microanalysis
# abstracts carry ranges and empty values alike - so the venue is the signal.
ABSTRACT_VENUES = ('biophysical journal', 'microscopy and microanalysis',
                   'alzheimer s dementia', 'the faseb journal', 'faseb journal',
                   'acta crystallographica section a')
# Venue names that mark an abstract supplement or meeting proceedings whatever
# the journal: "Archives of Cardiovascular Diseases Supplements", "ECS Meeting
# Abstracts". Cheaper and more durable than naming every such venue.
# "proceedings" is deliberately absent: it matches "Proceedings of the National
# Academy of Sciences", which would class every PNAS pair as an abstract.
ABSTRACT_WORDS = ('meeting abstract', 'supplements', 'abstracts')
# Some supplement abstracts give no hint in the journal name, only in the issue:
# Journal of Hypertension carries issue "Suppl 1". Pagination was tried as a
# second marker and withdrawn - an S- or e-prefixed page means "supplement" in
# cardiology journals but not elsewhere, and it swept up genuine pairs including
# a PNAS paper and its Current Protocols protocol. The issue field is
# unambiguous; pagination conventions are not.
SUPPL_ISSUE = re.compile(r'suppl', re.I)
# A bare S- or e-prefixed page with no range ("S47") is a supplement abstract;
# Appetite volume 57 carries the SSIB proceedings that way with no issue at all.
# Deliberately not applied to page ranges, which are ordinary articles.
SUPPL_PAGE = re.compile(r'^\s*[SeE]\d+\s*$')
# Cover-picture and frontispiece entries: a separate record for the artwork, not
# a companion work. Angewandte publishes these in both editions.
COVER_RE = re.compile(r'^\s*(inside (front|back) cover|cover picture|frontispiece|'
                      r'back cover|front cover|innen(r[üu]ck)?titelbild|titelbild|'
                      r'r[üu]cktitelbild|graphical abstract)\b', re.I)
# Angewandte Chemie publishes every paper twice, German and International Edition,
# under DOIs that differ only in the alpha token of the suffix (ange./anie.).
# Compared on every digit in the suffix, not the first run: Elsevier DOIs begin
# with the year, so matching the first run made any two 2014 papers look like one
# article in two editions. A minimum length rules out short numeric suffixes, and
# the registrant prefix must match - two editions come from one publisher.
DOI_DIGITS = re.compile(r'\d+')
DOI_TAIL_MIN = 6
# Words that carry no distinguishing weight in a title comparison.
STOP = frozenset(('a', 'an', 'and', 'for', 'of', 'the', 'to', 'in', 'on', 'with',
                  'using', 'via', 'by', 'from', 'at'))


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
    dbo = attrgetter(f"dis.{ARG.MANIFOLD}.{'write' if ARG.WRITE else 'read'}")(dbconfig)
    LOGGER.info(f"Connecting to {dbo.name} {ARG.MANIFOLD} on {dbo.host} as {dbo.user}")
    try:
        DB['dis'] = JRC.connect_database(dbo)
    except Exception as err:
        terminate_program(err)


def normalize(title):
    ''' Reduce a title to comparable words.
        Keyword arguments:
          title: raw title
        Returns:
          Normalised string
    '''
    text = DASHES.sub(' ', str(title or '').lower())
    text = NONWORD.sub(' ', text)
    words = [w for w in text.split() if w not in STOP]
    return ' '.join(words)


def surnames(rec):
    ''' Author surnames, lower-cased.
        Keyword arguments:
          rec: DOI record
        Returns:
          Set of surnames
    '''
    out = set()
    for auth in rec.get('author') or rec.get('creators') or []:
        fam = auth.get('family') or auth.get('familyName') or ''
        if fam:
            out.add(str(fam).lower().strip())
    return out


def venue(rec):
    """ Journal name, flattened for comparison.
        Keyword arguments:
          rec: DOI record
        Returns:
          Lower-cased journal with punctuation removed
    """
    # Collapse whitespace: stripping punctuation from "Alzheimer's &amp; Dementia"
    # leaves runs of spaces, which stopped it matching the venue list.
    flat = NONWORD.sub(' ', str(rec.get('jrc_journal') or '').replace('&amp;', ' ').lower())
    return re.sub(r'\s+', ' ', flat).strip()


def doi_tail(doi):
    """ Every digit in a DOI's suffix, for spotting the same article issued
        under two DOIs that differ only in an alpha token (ange./anie.).
        Keyword arguments:
          doi: DOI
        Returns:
          Digit string, or '' when too short to be distinctive
    """
    digits = ''.join(DOI_DIGITS.findall(str(doi or '').partition('/')[2]))
    return digits if len(digits) >= DOI_TAIL_MIN else ''


def known_links():
    """ Every DOI pair that already has a declared relationship.
        A pair the registrars (or our own linkers) already connect is not a
        discovery, so it is not worth reporting: Crossref's relation and
        DataCite's relatedIdentifiers cover what the publisher deposited,
        jrc_preprint what update_preprints.py matched,
        jrc_dataset_supplement what link_dataset_supplements.py derived, and
        jrc_companion what this program stored on an earlier run.
        Keyword arguments:
          None
        Returns:
          Set of frozenset DOI pairs, lower-cased
    """
    pairs = set()

    def add(one, two):
        one, two = str(one or '').lower().strip(), str(two or '').lower().strip()
        if one and two and one != two:
            pairs.add(frozenset((one, two)))
    payload = {"$or": [{"relation": {"$exists": True}},
                       {"relatedIdentifiers": {"$exists": True}},
                       {"jrc_preprint": {"$exists": True}},
                       {"jrc_dataset_supplement": {"$exists": True}},
                       {"jrc_companion": {"$exists": True}}]}
    try:
        rows = DB['dis'].dois.find(payload, {"doi": 1, "relation": 1, "relatedIdentifiers": 1,
                                             "jrc_preprint": 1,
                                             "jrc_dataset_supplement": 1,
                                             "jrc_companion": 1})
    except Exception as err:
        terminate_program(err)
    for row in rows:
        doi = row.get('doi')
        for vals in (row.get('relation') or {}).values():
            for val in vals:
                if str(val.get('id-type') or '').lower() == 'doi':
                    add(doi, val.get('id'))
        for rel in row.get('relatedIdentifiers') or []:
            if str(rel.get('relatedIdentifierType') or '').upper() == 'DOI':
                add(doi, rel.get('relatedIdentifier'))
        for pre in row.get('jrc_preprint') or []:
            add(doi, pre)
        for ent in row.get('jrc_dataset_supplement') or []:
            add(doi, ent.get('doi'))
        # Pairs this program has already stored, so a re-run does not re-propose
        # what it linked last time - the dry-run output stays honest, not just
        # the write path.
        for ent in row.get('jrc_companion') or []:
            add(doi, ent.get('doi'))
    LOGGER.info(f"Existing declared relationships: {len(pairs):,} pairs")
    return pairs


def is_preprint(doi):
    """ Is this DOI from a preprint server?
        Keyword arguments:
          doi: DOI
        Returns:
          True for a preprint
    """
    prefix, _, suffix = str(doi or '').partition('/')
    if prefix in PREPRINT_PREFIXES:
        return True
    return prefix == '10.1101' and bool(BIORXIV_SUFFIX.match(suffix))


def is_supplement(rec):
    """ Is this record a supplement abstract?
        Judged from the issue and a bare S-prefixed page rather than the journal
        name, which often says nothing.
        Keyword arguments:
          rec: DOI record
        Returns:
          True when it looks like a supplement abstract
    """
    if SUPPL_ISSUE.search(str(rec.get('issue') or '')):
        return True
    return bool(SUPPL_PAGE.match(str(rec.get('page') or '')))


def classify(rec_a, rec_b):
    """ What kind of pair is this?
        Keyword arguments:
          rec_a: first record
          rec_b: second record
        Returns:
          One of correction, preprint, translation, abstract, companion
    """
    titles = [str(DL.get_title(r)) for r in (rec_a, rec_b)]
    if any(CORRECTION_RE.match(t) for t in titles):
        return 'correction'
    if any(COVER_RE.match(t) for t in titles):
        return 'cover'
    if any(is_preprint(r['doi']) for r in (rec_a, rec_b)):
        return 'preprint'
    tails = [doi_tail(r['doi']) for r in (rec_a, rec_b)]
    same_registrant = rec_a['doi'].partition('/')[0] == rec_b['doi'].partition('/')[0]
    if same_registrant and tails[0] and tails[0] == tails[1]:
        return 'translation'
    venues = [venue(r) for r in (rec_a, rec_b)]
    if any(v in ABSTRACT_VENUES for v in venues) \
       or any(w in v for v in venues for w in ABSTRACT_WORDS) \
       or any(is_supplement(r) for r in (rec_a, rec_b)):
        return 'abstract'
    return 'companion'


def candidates():
    ''' Records in scope, keyed for pairing.
        --publisher is matched against both the publisher and the journal,
        because the string a person would type differs between the two: Nature
        journals carry publisher "Springer Science and Business Media LLC" and
        journal "Nature Methods", so "Nature" only ever matches the journal.
        Keyword arguments:
          None
        Returns:
          List of DOI records
    '''
    payload = {"type": "journal-article", "title": {"$exists": True},
               "author": {"$exists": True}}
    try:
        rows = list(DB['dis'].dois.find(payload,
                                        {"doi": 1, "title": 1, "author": 1, "publisher": 1,
                                         "jrc_journal": 1, "jrc_publishing_date": 1,
                                         "DOI": 1, "container-title": 1,
                                         "issue": 1, "page": 1}))
    except Exception as err:
        terminate_program(err)
    if not ARG.PUBLISHER:
        LOGGER.info(f"Records in scope (all publishers): {len(rows):,}")
        return rows
    want = ARG.PUBLISHER.lower()
    kept = [r for r in rows
            if want in str(r.get('publisher') or '').lower()
            or want in str(r.get('jrc_journal') or '').lower()]
    LOGGER.info(f"Records in scope for '{ARG.PUBLISHER}': {len(kept):,} of {len(rows):,}")
    return kept


def score_pair(rec_a, rec_b):
    ''' Score one candidate pair.
        Keyword arguments:
          rec_a: first record
          rec_b: second record
        Returns:
          (title score, author overlap, shared surnames) or None when disqualified
    '''
    aut_a, aut_b = surnames(rec_a), surnames(rec_b)
    if not aut_a or not aut_b:
        return None
    shared = aut_a & aut_b
    # An absolute floor as well as a fraction: measuring overlap against the
    # smaller author list makes a single-author paper score 100% on one shared
    # surname, which matched a one-page Nature Reviews research highlight to the
    # paper it was highlighting.
    if len(shared) < ARG.MIN_SHARED:
        return None
    overlap = len(shared) / min(len(aut_a), len(aut_b))
    if overlap < ARG.AUTHORS:
        return None
    norm_a, norm_b = normalize(DL.get_title(rec_a)), normalize(DL.get_title(rec_b))
    # token_set_ratio scores 100 whenever one title's words are a subset of the
    # other's, so a two-word title ("The claustrum" -> "claustrum") matches any
    # paper mentioning it. Below this length the score carries no information.
    if min(len(norm_a.split()), len(norm_b.split())) < ARG.MIN_WORDS:
        return None
    title = fuzz.token_set_ratio(norm_a, norm_b)
    if title < ARG.TITLE:
        return None
    return title, overlap, shared


def find_pairs(rows):
    ''' Compare every pair within a journal family.
        Records are bucketed by their first shared author surname before
        pairing: comparing all n^2 combinations is wasteful when a companion
        pair always shares authors, and the bucket makes the run linear in
        practice.
        Keyword arguments:
          rows: candidate records
        Returns:
          List of (score, overlap, shared, rec_a, rec_b)
    '''
    linked = known_links()
    bucket = collections.defaultdict(list)
    for rec in rows:
        for sur in surnames(rec):
            bucket[sur].append(rec)
    seen = set()
    found = []
    for sur, group in bucket.items():
        if len(group) < 2:
            continue
        COUNT['buckets_examined'] += 1
        for rec_a, rec_b in itertools.combinations(group, 2):
            key = tuple(sorted((rec_a['doi'], rec_b['doi'])))
            if key in seen:
                continue
            seen.add(key)
            COUNT['pairs_compared'] += 1
            # Same journal is usually a duplicate record or an erratum rather
            # than a companion; the archetype spans two journals in one family.
            if not ARG.SAME_JOURNAL and \
               str(rec_a.get('jrc_journal') or '') == str(rec_b.get('jrc_journal') or ''):
                COUNT['skipped_same_journal'] += 1
                continue
            if frozenset((rec_a['doi'].lower(), rec_b['doi'].lower())) in linked:
                COUNT['skipped_already_linked'] += 1
                continue
            hit = score_pair(rec_a, rec_b)
            if not hit:
                continue
            kind = classify(rec_a, rec_b)
            COUNT[f'kind_{kind}'] += 1
            if kind == 'preprint':
                # jrc_preprint owns this relationship and update_preprints.py owns
                # that field. Reporting it here would invite a second program to
                # write the same link by a different route, so it is detected only
                # to be counted and dropped.
                COUNT['skipped_preprint'] += 1
                continue
            if ARG.KIND not in ('all', kind):
                continue
            found.append((hit[0], hit[1], hit[2], rec_a, rec_b, kind))
    found.sort(key=lambda x: (-x[0], -x[1]))
    return found


def html_table(pairs):
    ''' Zebra-striped pair table for the run-summary email.
        Keyword arguments:
          pairs: list of scored pairs
        Returns:
          HTML table
    '''
    rows = []
    for idx, (score, _, shared, rec_a, rec_b, kind) in enumerate(pairs[:40]):
        striped = idx % 2 == 0
        bgattr = f' bgcolor="{JE.STRIPE_BG}"' if striped else ''
        bgs = f'background-color:{JE.STRIPE_BG};' if striped else ''
        rows.append(
            f'<tr{bgattr} style="{bgs}">'
            f'<td style="padding:8px 10px;">{html.escape(rec_a["doi"])}<br>'
            f'<span style="color:{JE.GRAY};">{html.escape(str(rec_a.get("jrc_journal") or ""))}'
            f'</span></td>'
            f'<td style="padding:8px 10px;">{html.escape(rec_b["doi"])}<br>'
            f'<span style="color:{JE.GRAY};">{html.escape(str(rec_b.get("jrc_journal") or ""))}'
            f'</span></td>'
            f'<td style="padding:8px 10px;">{html.escape(kind)}</td>'
            f'<td style="padding:8px 10px;" align="right">{score:.0f}</td>'
            f'<td style="padding:8px 10px;" align="right">{len(shared)}</td></tr>')
    rows.append('<tr><td colspan="5" style="height:1px;line-height:1px;font-size:1px;">'
                '&nbsp;</td></tr>')
    # Five columns, two of them bare numbers, so a header row is needed here -
    # the two- and three-column tables in the sibling scripts get away without
    # one. Plain bottom border rather than a filled block: a background on a
    # header cell is one of the things Outlook's Word engine renders badly.
    thl = (f'style="padding:6px 10px;text-align:left;font-weight:700;'
           f'border-bottom:1px solid {JE.BORDER};"')
    thr = (f'style="padding:6px 10px;text-align:right;font-weight:700;'
           f'border-bottom:1px solid {JE.BORDER};"')
    head = (f'<tr><th {thl}>DOI</th><th {thl}>Companion</th><th {thl}>Kind</th>'
            f'<th {thr}>Title score</th><th {thr}>Shared authors</th></tr>')
    return ('<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
            'style="border-collapse:collapse;font-size:12.5px;">'
            + head + "".join(rows) + '</table>')


def generate_email(pairs):
    ''' Send the HTML run-summary email.
        Keyword arguments:
          pairs: list of scored pairs
        Returns:
          None
    '''
    run_data = (JRC.get_run_data(__file__, __version__).strip()
                + f" &middot; manifold: {ARG.MANIFOLD}")
    mode_label = 'WRITE' if ARG.WRITE else 'DRY RUN'
    mode_tone = 'good' if ARG.WRITE else 'warn'
    kpis = ''.join([
        JE.kpi_card(f"{COUNT['pairs_compared']:,}", "Pairs compared", width='33%'),
        JE.kpi_card(f"{len(pairs):,}", "Companion pairs", 'good', width='33%'),
        JE.kpi_card(ARG.PUBLISHER or 'all', "Publisher", width='33%'),
    ])
    body = JE.body_row(JE.section_header(f"&#128279; Companion Papers ({len(pairs):,})")
                       + html_table(pairs))
    msg = JE.render(os.path.basename(__file__), __version__, run_data,
                    mode_label, mode_tone, kpis, body)
    email = DIS['developer'] if ARG.TEST else DIS['receivers']
    try:
        LOGGER.info(f"Sending email to {email}")
        JRC.send_email(msg, DIS['sender'], email, 'Companion papers found', mime='html')
    except Exception as err:
        LOGGER.error(err)


def write_output(pairs):
    ''' Write the candidate pairs to JSON for review.
        Written on every run, --write or not: the whole point of the file is to
        be read before anything is stored, the same way tag_janelia_acks.py
        writes its snapshot. Each pair appears once, with both records and the
        evidence that matched them, rather than as the two mirrored entries the
        database gets.
        Keyword arguments:
          pairs: list of scored pairs
        Returns:
          None
    '''
    out = []
    for score, overlap, shared, rec_a, rec_b, kind in pairs:
        out.append({"kind": kind,
                    "title_score": round(score, 1),
                    "author_overlap": round(overlap, 2),
                    "shared_authors": sorted(shared),
                    "a": {"doi": rec_a['doi'], "journal": rec_a.get('jrc_journal'),
                          "title": str(DL.get_title(rec_a)),
                          "published": rec_a.get('jrc_publishing_date')},
                    "b": {"doi": rec_b['doi'], "journal": rec_b.get('jrc_journal'),
                          "title": str(DL.get_title(rec_b)),
                          "published": rec_b.get('jrc_publishing_date')}})
    try:
        with open(ARG.OUTPUT, 'w', encoding='utf-8') as handle:
            json.dump(out, handle, indent=2, default=str)
    except Exception as err:
        terminate_program(err)
    LOGGER.info(f"Wrote {len(out):,} pair(s) to {ARG.OUTPUT}")


def persist(pairs):
    ''' Store jrc_companion on both DOIs of each pair, or print the pairs.
        Keyword arguments:
          pairs: list of scored pairs
        Returns:
          None
    '''
    if not ARG.WRITE:
        for score, overlap, shared, rec_a, rec_b, kind in pairs:
            print(f"  {score:5.1f}  authors {len(shared):2} ({overlap:.0%})  [{kind}]")
            print(f"     {rec_a['doi']:34} {str(rec_a.get('jrc_journal') or '')[:28]:28} "
                  f"{str(DL.get_title(rec_a))[:52]}")
            print(f"     {rec_b['doi']:34} {str(rec_b.get('jrc_journal') or '')[:28]:28} "
                  f"{str(DL.get_title(rec_b))[:52]}")
        if pairs:
            print(f"\nDry run: nothing written. Re-run with --write to store "
                  f"{len(pairs):,} pair(s).", file=sys.stderr)
        return
    now = datetime.now()
    for score, overlap, shared, rec_a, rec_b, kind in pairs:
        for this, other in ((rec_a, rec_b), (rec_b, rec_a)):
            # $addToSet compares the whole sub-document, and "updated" differs on
            # every run, so it would append a duplicate entry and push another
            # processing event each time. Test for the companion DOI instead, and
            # skip when it is already recorded.
            try:
                already = DB['dis'].dois.find_one(
                    {"doi": this['doi'], "jrc_companion.doi": other['doi']}, {"_id": 1})
            except Exception as err:
                terminate_program(err)
            if already:
                COUNT['already_recorded'] += 1
                continue
            # No journal here: the display reads it from the target record, so a
            # copy stored alongside would only be a second thing to keep current.
            entry = {"doi": other['doi'], "kind": kind,
                     "title_score": round(score, 1), "author_overlap": round(overlap, 2),
                     "shared_authors": len(shared), "curator": "IRIS", "updated": now}
            try:
                DB['dis'].dois.update_one({"doi": this['doi']},
                                          {"$push": {"jrc_companion": entry}})
                COUNT['written'] += 1
            except Exception as err:
                terminate_program(err)
            try:
                DL.add_doi_process(this['doi'], action='link_companion',
                                   coll=DB['dis'].processing,
                                   notes=f"companion {other['doi']} "
                                         f"(title {score:.0f}, {len(shared)} shared authors)")
                COUNT['processing_logged'] += 1
            except Exception as err:
                LOGGER.error(f"Could not log a processing event for {this['doi']}: {err}")
                COUNT['processing_error'] += 1


def processing():
    ''' Main routine
        Keyword arguments:
          None
        Returns:
          None
    '''
    rows = candidates()
    pairs = find_pairs(rows)
    COUNT['pairs_found'] = len(pairs)
    write_output(pairs)
    persist(pairs)
    if pairs and (ARG.TEST or ARG.WRITE):
        generate_email(pairs)
    print(file=sys.stderr)
    for key in sorted(COUNT):
        print(f"{key + ':':<28} {COUNT[key]:,}", file=sys.stderr)


# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Find companion papers (a method and its protocol, etc.)")
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, prod)')
    PARSER.add_argument('--publisher', dest='PUBLISHER', action='store', default='',
                        help='Publisher or journal family, matched as a case-insensitive '
                             'substring of either (default: all)')
    PARSER.add_argument('--title', dest='TITLE', action='store', type=float, default=90.0,
                        help='Minimum token_set_ratio between titles (default 90)')
    PARSER.add_argument('--authors', dest='AUTHORS', action='store', type=float, default=0.5,
                        help='Minimum shared-author fraction of the smaller list (default 0.5)')
    PARSER.add_argument('--min-words', dest='MIN_WORDS', action='store', type=int, default=4,
                        help='Minimum significant words in the shorter title (default 4)')
    PARSER.add_argument('--min-shared', dest='MIN_SHARED', action='store', type=int, default=2,
                        help='Minimum number of shared author surnames (default 2)')
    PARSER.add_argument('--output', dest='OUTPUT', action='store',
                        default='companion_papers.json',
                        help='JSON output file, written every run '
                             '(default: companion_papers.json)')
    PARSER.add_argument('--kind', dest='KIND', action='store', default='companion',
                        choices=['all', 'companion', 'abstract', 'correction',
                                 'translation', 'cover'],
                        help='Kind of pair to report (default: companion)')
    PARSER.add_argument('--same-journal', dest='SAME_JOURNAL', action='store_true',
                        default=False, help='Also pair records from the same journal')
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
