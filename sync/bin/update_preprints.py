""" update_preprints.py
    Update the jrc_preprint field in the dois collection for all locally-stored DOIs.

    Relationships come from three independent sources:
      - Explicit metadata: DOI records that directly declare the relationship, via
        Crossref's "relation" field (has-preprint / is-preprint-of, checked on both
        primaries and preprints) or DataCite's "relatedIdentifiers" field (checked
        only on preprints, via the IsPreprintOf relationType - PRIMARY only ever
        loads Crossref journal-articles, so a DataCite HasPreprint declaration on a
        primary could never be reached). These are treated as ground truth and are
        not subject to any scoring.
      - Fuzzy matching: every preprint (from DataCite and Crossref) is compared to
        every "primary" DOI (from Crossref). A relationship requires author
        confirmation - one of:
          a. the first and last author each score >= --author-threshold, or
          b. the two records share at least one author ORCID (immune to name
             formatting differences and author-list changes - added, removed, or
             reordered co-authors - that would fail check (a)), or
          c. the first or last author of either record fuzzy-matches ANY author in
             the other record's full author list at >= --author-threshold (recovers
             cases where the specific first/last positions changed but a stable
             author moved elsewhere in the list), or
          d. the two records share a Janelia identity via the internal roster (the
             "orcid" collection, maintained independently of DOI metadata by
             update_orcid.py/apply_orcids.py/add_people_to_orcid.py from HHMI's
             People system and the public ORCID API) - resolving an author by their
             own embedded ORCID first, falling back to a (given, family) name
             lookup against the roster otherwise. This recovers matches where a
             Janelia author's ORCID is present on one deposit but omitted from the
             other, which check (b)'s direct ORCID-intersection can't catch
        - plus a title match, which is one of:
          a. RapidFuzz title score >= --title-threshold (token_sort_ratio, boosted by
             token_set_ratio when the two titles differ by only a few words - handles
             a subtitle added/removed during peer review - but only when the length
             difference is small, since token_set_ratio alone would wrongly score a
             short generic title as a perfect match against any longer title that
             happens to contain its words), or
          b. title score within TITLE_DATE_GRACE points of --title-threshold, if the
             preprint's and primary's publishing dates are both known and consistent
             (preprint not meaningfully after the primary) - two independent weak
             signals (a near-miss title plus sane date ordering) corroborating each
             other is more defensible than loosening either check alone.
        The author threshold defaults lower than the title threshold because short
        name strings are far more sensitive to benign formatting differences
        (initials vs. full given names, hyphenation, diacritics) than long title
        strings are - the same numeric cutoff is not equally strict for both.
      - Version propagation: preprints are sometimes deposited under more than one DOI
        for different versions (e.g. v1/v2/v3 on a preprint server). If one version is
        related to a primary (via either path above), that relation is propagated to
        its sibling versions - identified via the same version-relation vocabulary
        used by utility/bin/apply_version_tags.py (Crossref's relation.is-version-of/
        has-version/is-same-as, DataCite's relatedIdentifiers IsVersionOf/HasVersion/
        IsIdenticalTo) - even though a sibling's own title may never independently
        match anything (e.g. an early draft with a different working title).
    For each pair with a title/author match, a relationship will be created between the
    DOIs. When all DOIs have been processed, the relationships will be written to the
    jrc_preprint field in the dois collection.

    An HTML summary of newly-created relations (see generate_email) is sent when
    --test (developer only) or --write (real recipients) is given; sender/recipients
    come from the "dis" config. Any DOI named on either side of a relation but never
    loaded locally (not in the dois collection, and not on the to_ignore list) is
    tracked separately (see MISSING) - written to preprints_missing_dois.txt and
    attached to the email, and also summarized inline in the email body, grouped by
    the other DOI each was referenced from (see missing_doi_groups_html), so a
    curator can see at a glance which record a missing DOI probably belongs to. An
    associated DOI that's on the to_ignore list is annotated with its recorded
    reason when one is available, in place of a title.
"""

__version__ = '2.0.0'

import argparse
import collections
from datetime import datetime
import html
from operator import attrgetter
import sys
import pandas as pd
from pymongo.collation import Collation, CollationStrength
from rapidfuzz import fuzz, utils
from tqdm import tqdm
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# DOI casing isn't guaranteed consistent in the dois collection (some are stored
# mixed-case) - this codebase's convention elsewhere (e.g. api/dis_responder.py,
# utility/bin/assign_authors.py) is a case-insensitive collation on doi queries/writes.
INSENSITIVE = Collation(locale='en', strength=CollationStrength.PRIMARY)

# DataCite relatedIdentifiers.relationType value that declares a preprint link on the
# preprint side, mirroring Crossref's relation.is-preprint-of. NOTE: verify this
# against real relatedIdentifiers.relationType values in the dois collection (e.g. an
# aggregation like dis_responder.py's "$group" on that field) before relying on it -
# DataCite's relation vocabulary isn't exercised elsewhere in this codebase for
# preprint linkage. There is no primary-side (HasPreprint) equivalent here: PRIMARY
# only loads Crossref journal-articles (see initialize_program), so a DataCite record
# can never be a primrec and a HasPreprint check on primaries could never fire.
DATACITE_IS_PREPRINT_OF = "IsPreprintOf"

# Version-relation vocabulary linking different DOIs for the same underlying preprint
# (e.g. v1/v2/v3 on a preprint server). Confirmed real values for this DB via
# utility/bin/apply_version_tags.py, which already tags DOIs using these same lists.
CROSSREF_VERSION_RELATIONS = ('is-version-of', 'has-version', 'is-same-as')
DATACITE_VERSION_RELATIONS = ('IsVersionOf', 'HasVersion', 'IsIdenticalTo')

# How many points a near-miss title score may fall short of --title-threshold and
# still be accepted, if the preprint/primary publishing dates are known and consistent
# and author confirmation otherwise succeeds. See title_score()/dates_consistent().
TITLE_DATE_GRACE = 5

EMAIL_SUBJECT = "New preprint/primary matches"
BADGE_PREPRINT = "background-color:#2e86de; color:#fff; padding:2px 8px; " \
                 "border-radius:10px; font-size:11px; font-weight:bold; white-space:nowrap;"
BADGE_PRIMARY = "background-color:#27ae60; color:#fff; padding:2px 8px; " \
                "border-radius:10px; font-size:11px; font-weight:bold; white-space:nowrap;"
BADGE_UNKNOWN = "background-color:#7f8c8d; color:#fff; padding:2px 8px; " \
                "border-radius:10px; font-size:11px; font-weight:bold; white-space:nowrap;"
ROLE_BADGE = {"Preprint": BADGE_PREPRINT, "Primary": BADGE_PRIMARY, "DOI": BADGE_UNKNOWN}

# Database
DB = {}
# DIS config (sender/receivers/developer for the new-matches email)
DISCONFIG = {}
# Counters
COUNT = collections.defaultdict(lambda: 0, {})
# DOIs
PRIMARY = {}
PREPRINT = {}
PRIMARYREL = {}
PREPRINTREL = {}
# (predoi, primdoi) -> {"source": ..., and for "fuzzy" edges also "title_score",
# "first_author_score", "last_author_score"}. Set once per genuinely new edge (see
# make_doi_relationships) - used by new_pair_card() to show how a relation was
# made: score-based confirmation, or "explicit"/"version" for the two source types
# that aren't scored at all.
EDGE_INFO = {}
AUTHOR_CACHE = {}
ORCID_CACHE = {}
EMPLOYEE_CACHE = {}
IGNORE = set()
# DOI -> to_ignore's "reason" field, for display only (e.g. in the missing-DOIs
# email section, when an associated DOI has no local title because it was never
# loaded - only ever referenced via to_ignore - see missing_group_card)
IGNORE_REASON = {}
# Internal Janelia roster (the "orcid" collection, maintained by update_orcid.py/
# apply_orcids.py/add_people_to_orcid.py from HHMI's People system and the public
# ORCID API - NOT derived from DOI metadata). Preloaded once in initialize_program,
# same pattern as PRIMARY/PREPRINT/IGNORE, and used by get_employee_identities() to
# resolve an author to a Janelia identity even when a DOI's own embedded ORCID
# field is missing. ROSTER_BY_ORCID maps a roster ORCID to a person's identity
# token (employeeId, or that ORCID itself if no employeeId is on file);
# ROSTER_BY_NAME maps every (given, family) name-variant combination for a person
# to the same token, mirroring doi_common.single_orcid_lookup_name's array-field
# semantics against the collection's given/family list fields.
ROSTER_BY_ORCID = {}
ROSTER_BY_NAME = {}
# Output data
AUDIT = []
MATCH = {"DOI": [], "Type": [], "Title": [], "Score": [], "First author": [],
         "First author score": [], "Last author": [], "Last author score": [],
         "Author Overlap Match": [], "ORCID Match": [], "Roster Match": [],
         "Publishing date": [], "Relation": [], "Decision": []}
CURATE = {"Preprint DOI": [], "Preprint Title": [], "Primary DOI": [], "Primary Title": [],
          "Score": [], "First author score": [], "Last author score": [],
          "Preprint publishing date": [], "Primary publishing date": []}
NEARMISS = []
# DOI -> set of DOI(s) it was related to that triggered the missing flag (see
# make_doi_relationships) - the associated DOI(s) are what a curator should
# probably relate/ingest the missing DOI against.
MISSING = {}
# Preprint DOIs that cleared the title-matching grace window against at least one
# primary this run - used by zero_candidate_preprints() to report preprints where
# NOTHING came close on title (distinct from NEARMISS, where title matched but
# author confirmation failed).
TITLE_CANDIDATE_SEEN = set()
# (doi, newly_added_related_doi) pairs discovered in write_to_database(); each true
# new edge is recorded twice (once from each side) and deduped by dedupe_new_pairs()
NEW_PAIRS = []
# doi -> True if that DOI's jrc_preprint value was empty before this run (only set
# for DOIs with a known current value - see get_current_relation). Used by
# dedupe_new_pairs() to tell a brand-new relation (neither DOI had any relation
# before) from an addition (at least one side already had a relation to something).
EMPTY_BEFORE = {}

def terminate_program(msg=None):
    ''' Terminate the program gracefully
        Keyword arguments:
          msg: error message
        Returns:
          None
    '''
    if msg:
        if not isinstance(msg, str):
            msg = f"An exception of type {type(msg).__name__} occurred. Arguments:\n{msg.args}"
        LOGGER.critical(msg)
    sys.exit(-1 if msg else 0)


def initialize_program():
    ''' Intialize the program
        Keyword arguments:
          None
        Returns:
          None
    '''
    # Database
    try:
        dbconfig = JRC.get_config("databases")
    except Exception as err:
        terminate_program(err)
    dbs = ['dis']
    for source in dbs:
        dbo = attrgetter(f"{source}.{ARG.MANIFOLD}.write")(dbconfig)
        LOGGER.info(f"Connecting to {dbo.name} {ARG.MANIFOLD} on {dbo.host} as {dbo.user}")
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)
    LOGGER.info("Getting DOIs")
    projection = {"_id": 0, "DOI": 1, "doi": 1, "title": 1, "titles": 1,
                  "author": 1, "creators": 1, "relation": 1, "relatedIdentifiers": 1,
                  "type": 1, "types": 1, "subtype": 1,
                  "published": 1, "published-print": 1, "published-online": 1,
                  "posted": 1, "created": 1, "registered": 1, "dates": 1,
                  "jrc_preprint": 1}
    try:
        # Primary DOIs will all be from Crossref
        rows = DB['dis'].dois.find({"type": "journal-article"},
                                   projection)
    except Exception as err:
        terminate_program(err)
    for row in rows:
        # DOI casing isn't guaranteed consistent in the collection - normalize once
        # here so every dict/set keyed by DOI downstream (PREPRINTREL, PRIMARYREL,
        # MISSING, IGNORE, version siblings) agrees on the same string.
        row['doi'] = row['doi'].lower()
        PRIMARY[row['doi']] = row
    LOGGER.info(f"Primary DOIs: {len(PRIMARY):,}")
    try:
        # Preprints can come from Crossref (type=posted-content, subtype=preprint) or
        # DataCite (types.resourceTypeGeneral=Preprint). The subtype filter excludes
        # other posted-content subtypes (letter/retraction/correction/editorial/other),
        # matching doi_common.is_preprint()'s convention.
        rows = DB['dis'].dois.find({"$or": [{"type": "posted-content",
                                             "subtype": "preprint"},
                                            {"types.resourceTypeGeneral": "Preprint"}],
                                    "doi": {"$not": {"$regex": r"^10\.25378/janelia\."}}},
                                   projection)
    except Exception as err:
        terminate_program(err)
    for row in rows:
        row['doi'] = row['doi'].lower()
        PREPRINT[row['doi']] = row
    LOGGER.info(f"Preprint DOIs: {len(PREPRINT):,}")
    try:
        for rec in DB['dis'].to_ignore.find({"type": "doi"}, {"key": 1, "reason": 1}):
            if rec.get('key'):
                key = rec['key'].lower()
                IGNORE.add(key)
                if rec.get('reason'):
                    IGNORE_REASON[key] = rec['reason']
    except Exception as err:
        terminate_program(err)
    LOGGER.info(f"Ignored DOIs: {len(IGNORE):,}")
    try:
        # Internal Janelia roster, for get_employee_identities()'s name-fallback
        # ORCID resolution. Every (given, family) name variant is indexed - a
        # roster row's given/family fields are lists (see get_name_combinations in
        # doi_common.py), and doi_common.single_orcid_lookup_name's Mongo query
        # matches on array-containment per field, not a specific given/family
        # pairing, so we replicate that by cross-producting each row's variants.
        for row in DB['dis'].orcid.find({}, {"_id": 0, "employeeId": 1, "orcid": 1,
                                             "given": 1, "family": 1}):
            person_id = row.get('employeeId') or row.get('orcid')
            if not person_id:
                continue
            if row.get('orcid'):
                ROSTER_BY_ORCID[row['orcid'].upper()] = person_id
            for given in row.get('given') or []:
                for family in row.get('family') or []:
                    ROSTER_BY_NAME[(given.strip().lower(), family.strip().lower())] = person_id
    except Exception as err:
        terminate_program(err)
    LOGGER.info(f"Roster: {len(ROSTER_BY_ORCID):,} ORCIDs, "
                f"{len(ROSTER_BY_NAME):,} name combinations")
    overlap = set(PRIMARY) & set(PREPRINT)
    if overlap:
        sample = ', '.join(sorted(overlap)[:10])
        if len(overlap) > 10:
            sample += ', ...'
        LOGGER.warning(f"{len(overlap):,} DOIs are classified as both primary and preprint "
                        f"and will not be self-matched: {sample}")


def process_explicit_relations():
    ''' Make relationships declared directly in DOI metadata. Crossref records use the
        "relation" field (has-preprint/is-preprint-of); DataCite preprint records
        declare the reverse relation in a differently-shaped "relatedIdentifiers"
        field instead, with relationType value DATACITE_IS_PREPRINT_OF (there is no
        DataCite check on the primary side - see DATACITE_IS_PREPRINT_OF's comment).
        This is independent of the fuzzy title/author matching pass, so it only needs
        to visit each record once. Every indexed key is guarded with .get() so a
        relation entry missing an expected field is skipped rather than raising.
        Keyword arguments:
          None
        Returns:
          None
    '''
    for primrec in PRIMARY.values():
        if "relation" in primrec and "has-preprint" in primrec["relation"]:
            for rec in primrec["relation"]["has-preprint"]:
                if rec.get("id-type") == "doi" and rec.get("id"):
                    make_doi_relationships(rec["id"], primrec["doi"], source="explicit")
    for prerec in PREPRINT.values():
        if "relation" in prerec and "is-preprint-of" in prerec["relation"]:
            for rec in prerec["relation"]["is-preprint-of"]:
                if rec.get("id-type") == "doi" and rec.get("id"):
                    make_doi_relationships(prerec["doi"], rec["id"], source="explicit")
        for rec in prerec.get("relatedIdentifiers") or []:
            if rec.get("relatedIdentifierType") == "DOI" \
               and rec.get("relationType") == DATACITE_IS_PREPRINT_OF \
               and rec.get("relatedIdentifier"):
                make_doi_relationships(prerec["doi"], rec["relatedIdentifier"],
                                       source="explicit")


def make_doi_relationships(predoi, primdoi, source, scores=None):
    ''' Make relationships between two DOIs. This is the single function all three
        relation-creation paths (explicit metadata, fuzzy matching, version
        propagation) funnel through, so shared invariants - self-relation
        prevention, missing-DOI tracking - are enforced here rather than by each
        caller individually.
        Keyword arguments:
          predoi: preprint DOI
          primdoi: primary DOI
          source: where this relation came from ("explicit" metadata, "fuzzy" match,
                  or "version" propagation)
          scores: optional dict of title_score/first_author_score/last_author_score,
                  for "fuzzy" edges only - recorded in EDGE_INFO for display in the
                  new-matches email (see new_pair_card)
        Returns:
          None
    '''
    predoi = predoi.lower()
    primdoi = primdoi.lower()
    if predoi == primdoi:
        return
    # Find DOIs missing from dois collection entirely (excluding known-ignorable
    # DOIs, and checking both buckets since a DOI can legitimately be loaded under
    # the "other" role - see the primary/preprint overlap warning in initialize_program).
    # The other side of the relation is recorded too, so the missing-DOIs email
    # section can group each missing DOI under the record it should probably be
    # related to.
    if predoi not in PREPRINT and predoi not in PRIMARY and predoi not in IGNORE:
        MISSING.setdefault(predoi, set()).add(primdoi)
    if primdoi not in PRIMARY and primdoi not in PREPRINT and primdoi not in IGNORE:
        MISSING.setdefault(primdoi, set()).add(predoi)
    PREPRINTREL.setdefault(predoi, [])
    PRIMARYREL.setdefault(primdoi, [])
    if primdoi in PREPRINTREL[predoi]:
        return
    LOGGER.debug(f"Adding relation {predoi} -> {primdoi} ({source})")
    PREPRINTREL[predoi].append(primdoi)
    PRIMARYREL[primdoi].append(predoi)
    EDGE_INFO[(predoi, primdoi)] = {"source": source, **(scores or {})}
    COUNT['relations_created'] += 1
    COUNT[f'relations_from_{source}'] += 1


def existing_relation(rec):
    ''' Format a DOI record's pre-existing jrc_preprint relation for display
        Keyword arguments:
          rec: DOI record
        Returns:
          Comma-separated string of already-related DOIs, or an empty string if none
    '''
    return ", ".join(rec.get('jrc_preprint') or [])


def get_author_names(rec, doi):
    ''' Get the full formatted author-name list for a DOI record, memoized by DOI so
        each record's author list is only computed once across the whole comparison
        pass. Used both for the first/last-author check and for the broader
        author-set overlap check (see best_author_match). A real exception from
        DL.get_author_list (e.g. an unexpected creators/author shape) is counted
        separately (COUNT['author_parse_errors']) from a record that genuinely has
        no authors, so a systemic parsing problem is visible in the summary instead
        of just inflating the generic skipped-no-author count - but both cases end
        up returning/caching an empty list, since either way there's nothing to
        compare against.
        Keyword arguments:
          rec: DOI record
          doi: DOI (used as the cache key and for logging)
        Returns:
          List of formatted author name strings (empty list if none could be determined)
    '''
    if doi in AUTHOR_CACHE:
        return AUTHOR_CACHE[doi]
    try:
        authors = DL.get_author_list(rec, returntype="list")
    except Exception as err:
        LOGGER.warning(f"Could not determine authors for {doi}: {err}")
        COUNT['author_parse_errors'] += 1
        authors = None
    if not authors:
        LOGGER.debug(f"No authors found for {doi}")
        authors = None
    AUTHOR_CACHE[doi] = authors or []
    return AUTHOR_CACHE[doi]


def get_first_last_author(rec, doi):
    ''' Get the first and last author for a DOI record
        Keyword arguments:
          rec: DOI record
          doi: DOI (used as the cache key and for logging)
        Returns:
          (first author, last author) tuple, or None if authors could not be determined
    '''
    authors = get_author_names(rec, doi)
    return (authors[0], authors[-1]) if authors else None


def best_author_match(name, others):
    ''' Find the best-scoring fuzzy match for a name among a list of candidates, at
        or above the author threshold. Used to recover matches where a specific
        first/last position changed between preprint and publication, but the same
        person is still present elsewhere in the other record's author list
        (reordering, new corresponding author, etc.) - and to report exactly which
        name matched and at what score, for display in the new-matches email
        (see edge_detail), rather than just a yes/no.
        Keyword arguments:
          name: name to check
          others: list of candidate names to check against
        Returns:
          (matched_name, score) for the highest-scoring candidate >= ARG.AUTHOR_THRESHOLD,
          or None if no candidate clears the threshold
    '''
    best = None
    for other in others:
        candidate_score = fuzz.token_sort_ratio(name, other, processor=utils.default_process)
        if candidate_score >= ARG.AUTHOR_THRESHOLD and (best is None or candidate_score > best[1]):
            best = (other, candidate_score)
    return best


def get_orcids(rec, doi):
    ''' Get the set of bare ORCID identifiers attached to a DOI record's author list,
        memoized by DOI. Crossref authors carry an ORCID URL directly on the author
        dict ("ORCID"); DataCite creators carry it in a "nameIdentifiers" sub-list
        tagged with nameIdentifierScheme "ORCID" instead. A shared ORCID between two
        records' author lists is a much stronger match signal than fuzzy name
        similarity - it's immune to name formatting differences, name changes, and
        author-list reordering/edits made between preprint and publication. IDs are
        uppercased since the trailing ORCID checksum character can be reported as
        either case ("...009x" vs "...009X") depending on the source.
        Keyword arguments:
          rec: DOI record
          doi: DOI (used as the cache key)
        Returns:
          Set of ORCID identifiers (e.g. "0000-0002-1825-0097")
    '''
    if doi in ORCID_CACHE:
        return ORCID_CACHE[doi]
    authors = rec.get('author') if 'DOI' in rec else rec.get('creators')
    orcids = set()
    for auth in authors or []:
        if 'nameIdentifiers' in auth:
            for nid in auth['nameIdentifiers']:
                if nid.get('nameIdentifierScheme') == 'ORCID' and nid.get('nameIdentifier'):
                    orcids.add(nid['nameIdentifier'].rstrip('/').split('/')[-1].upper())
        elif auth.get('ORCID'):
            orcids.add(auth['ORCID'].rstrip('/').split('/')[-1].upper())
    ORCID_CACHE[doi] = orcids
    return orcids


def get_employee_identities(rec, doi):
    ''' Resolve each author on a DOI record to a Janelia identity token, using the
        internal roster (ROSTER_BY_ORCID/ROSTER_BY_NAME, preloaded in
        initialize_program from the "orcid" collection - populated independently
        of DOI metadata from HHMI's People system and the public ORCID API). For
        an author with an embedded ORCID, resolves by ORCID; otherwise falls back
        to a (given, family) name lookup - the same two-step resolution
        doi_common._add_single_author_jrc already uses elsewhere in this codebase.
        This recovers matches where a Janelia author's ORCID is present on one
        deposit but omitted from the other (not every source requires ORCID at
        submission), which the plain get_orcids() intersection would miss. Most
        authors aren't Janelia-affiliated and resolve to nothing - that's expected,
        not an error. Memoized by DOI like get_orcids/get_author_names.
        Keyword arguments:
          rec: DOI record
          doi: DOI (used as the cache key)
        Returns:
          Set of resolved identity tokens (employeeId, or that person's ORCID if
          no employeeId is on file for them in the roster)
    '''
    if doi in EMPLOYEE_CACHE:
        return EMPLOYEE_CACHE[doi]
    datacite = 'DOI' not in rec
    authors = rec.get('creators') if datacite else rec.get('author')
    identities = set()
    for auth in authors or []:
        orcid = None
        given = auth.get('givenName') if datacite else auth.get('given')
        family = auth.get('familyName') if datacite else auth.get('family')
        if datacite:
            for nid in auth.get('nameIdentifiers') or []:
                if nid.get('nameIdentifierScheme') == 'ORCID' and nid.get('nameIdentifier'):
                    orcid = nid['nameIdentifier'].rstrip('/').split('/')[-1].upper()
                    break
        elif auth.get('ORCID'):
            orcid = auth['ORCID'].rstrip('/').split('/')[-1].upper()
        person_id = ROSTER_BY_ORCID.get(orcid) if orcid else None
        if person_id is None and given and family:
            person_id = ROSTER_BY_NAME.get((given.strip().lower(), family.strip().lower()))
        if person_id:
            identities.add(person_id)
    EMPLOYEE_CACHE[doi] = identities
    return identities


def title_score(pretitle, primtitle):
    ''' Compute a title similarity score. token_sort_ratio is the primary metric.
        token_set_ratio (which ignores extra/duplicate tokens) is used to boost titles
        that differ by only a short added/removed subtitle or clause - but ONLY when
        the two titles are close in length. token_set_ratio scores a short, generic
        title as a perfect match against any longer title that happens to contain its
        words (e.g. "Cancer" vs. "Cancer risk factors in aging populations: a
        systematic review" scores 100), so it's unsafe to use unconditionally.
        Keyword arguments:
          pretitle: first title
          primtitle: second title
        Returns:
          Similarity score (0-100)
    '''
    sort_score = fuzz.token_sort_ratio(pretitle, primtitle, processor=utils.default_process)
    pre_words = pretitle.split()
    prim_words = primtitle.split()
    shorter_len = min(len(pre_words), len(prim_words))
    word_diff = abs(len(pre_words) - len(prim_words))
    # floor=2 (not 4): a flat 4-word floor let short titles (e.g. 3 words) pass with
    # up to 4 extra words - empirically, "Cancer risk factors" vs "Cancer risk
    # factors in aging populations review" scored a false 100.0 under that floor.
    if shorter_len and word_diff <= max(2, shorter_len * 0.3):
        set_score = fuzz.token_set_ratio(pretitle, primtitle, processor=utils.default_process)
        return max(sort_score, set_score)
    return sort_score


def dates_consistent(predate, primdate):
    ''' Check whether a preprint's publishing date is sane relative to its candidate
        primary's, as corroborating evidence for a near-miss title score (see
        TITLE_DATE_GRACE): the preprint should not have been posted meaningfully after
        the primary was published. Returns False (no corroboration, not the benefit of
        the doubt) if either date is unknown/unparseable, since missing data can't
        corroborate anything.
        Keyword arguments:
          predate: preprint publishing date (YYYY-MM-DD or "unknown")
          primdate: primary publishing date (YYYY-MM-DD or "unknown")
        Returns:
          True if the preprint date is on or before the primary date (with a small
          grace window for recording noise), else False
    '''
    try:
        pre = datetime.strptime(predate, "%Y-%m-%d")
        prim = datetime.strptime(primdate, "%Y-%m-%d")
    except (ValueError, TypeError):
        return False
    return (pre - prim).days <= 30


def process_pair(prerec, primrec):
    ''' Compare one preprint/primary pair and, if confirmed, create a fuzzy-match
        relation between them. A title match is required first (title_score() >=
        --title-threshold, or within TITLE_DATE_GRACE points of it - see date_ok
        below), then author confirmation via any one of, checked in this priority
        order: strict first+last author score >= --author-threshold; first/last
        author of either record fuzzy-matching any author in the other's full list
        (best_author_match); or a shared author ORCID. This same priority order
        determines which evidence gets recorded for the new-matches email (see the
        author_scores selection below) - not always the strict first/last scores,
        since those are often below threshold precisely when the fallback checks
        were needed. If the title is a near-miss (below --title-threshold but
        within the grace window) rather than a full match, author confirmation
        alone isn't enough - the preprint/primary publishing dates must also be
        known and consistent (dates_consistent). Every preprint that clears the
        initial title gate against at least one primary is recorded in
        TITLE_CANDIDATE_SEEN (see zero_candidate_preprints) - independent of whether
        author confirmation later succeeds - to separate "title never came close to
        anything" from "title matched but authors didn't." Every pair that clears
        the title gate also gets a row recorded in MATCH regardless of the final
        decision; pairs that fail confirmation but have no pre-existing relation on
        either side are queued in NEARMISS for build_curation_list() to reconsider
        once the whole run (including version propagation) has completed.
        Keyword arguments:
          prerec: preprint DOI record
          primrec: primary DOI record
        Returns:
          None
    '''
    predoi = prerec['doi']
    primdoi = primrec['doi']
    if predoi == primdoi:
        return
    pretitle = DL.get_title(prerec)
    primtitle = DL.get_title(primrec)
    COUNT['comparisons'] += 1
    if pretitle == "No title" or primtitle == "No title":
        COUNT['skipped_no_title'] += 1
        return
    score = title_score(pretitle, primtitle)
    if score < ARG.TITLE_THRESHOLD - TITLE_DATE_GRACE:
        return
    TITLE_CANDIDATE_SEEN.add(predoi)
    pre_author = get_first_last_author(prerec, predoi)
    prim_author = get_first_last_author(primrec, primdoi)
    if not pre_author or not prim_author:
        COUNT['skipped_no_author'] += 1
        return
    prefirst, prelast = pre_author
    primfirst, primlast = prim_author
    pre_authors = get_author_names(prerec, predoi)
    prim_authors = get_author_names(primrec, primdoi)
    MATCH['DOI'].extend([predoi, primdoi])
    MATCH['Type'].extend(["Preprint", "Primary"])
    MATCH['Title'].extend([pretitle, primtitle])
    MATCH['Score'].extend([score, score])
    MATCH['First author'].extend([prefirst, primfirst])
    first_score = fuzz.token_sort_ratio(prefirst, primfirst, processor=utils.default_process)
    MATCH['First author score'].extend([first_score, first_score])
    MATCH['Last author'].extend([prelast, primlast])
    last_score = fuzz.token_sort_ratio(prelast, primlast, processor=utils.default_process)
    MATCH['Last author score'].extend([last_score, last_score])
    strict_name_confirmed = (first_score >= ARG.AUTHOR_THRESHOLD) \
        and (last_score >= ARG.AUTHOR_THRESHOLD)
    overlap_match = None
    if not strict_name_confirmed:
        overlap_match = (best_author_match(prefirst, prim_authors)
                          or best_author_match(prelast, prim_authors)
                          or best_author_match(primfirst, pre_authors)
                          or best_author_match(primlast, pre_authors))
    overlap_confirmed = overlap_match is not None
    MATCH['Author Overlap Match'].extend(["Yes" if overlap_confirmed else "",
                                          "Yes" if overlap_confirmed else ""])
    shared_orcids = get_orcids(prerec, predoi) & get_orcids(primrec, primdoi)
    shared_orcid = bool(shared_orcids)
    MATCH['ORCID Match'].extend(["Yes" if shared_orcid else "", "Yes" if shared_orcid else ""])
    shared_identities = get_employee_identities(prerec, predoi) \
        & get_employee_identities(primrec, primdoi)
    shared_roster = bool(shared_identities)
    MATCH['Roster Match'].extend(["Yes" if shared_roster else "", "Yes" if shared_roster else ""])
    predate = DL.get_publishing_date(prerec)
    primdate = DL.get_publishing_date(primrec)
    MATCH['Publishing date'].extend([predate, primdate])
    prerel = existing_relation(prerec)
    primrel = existing_relation(primrec)
    MATCH['Relation'].extend([prerel, primrel])
    title_ok = score >= ARG.TITLE_THRESHOLD
    if title_ok:
        COUNT['title_match'] += 1
    author_ok = strict_name_confirmed or overlap_confirmed or shared_orcid or shared_roster
    date_ok = (not title_ok) and dates_consistent(predate, primdate)
    if author_ok and (title_ok or date_ok):
        # Record whichever evidence actually confirmed authorship (same priority
        # order as the COUNT attribution below), not always the strict first/last
        # scores - those are frequently below threshold precisely when overlap,
        # ORCID, or the roster cross-reference was needed as a fallback, which
        # would otherwise misrepresent a legitimately-confirmed match as a weak
        # one in the new-matches email.
        if strict_name_confirmed:
            author_scores = {"first_author_score": first_score,
                             "last_author_score": last_score}
        elif overlap_confirmed:
            matched_name, matched_score = overlap_match
            author_scores = {"author_overlap_match": matched_name,
                             "author_overlap_score": matched_score}
        elif shared_orcid:
            author_scores = {"shared_orcid": next(iter(shared_orcids))}
        else:
            author_scores = {"shared_roster_id": next(iter(shared_identities))}
        make_doi_relationships(predoi, primdoi, source="fuzzy",
                               scores={"title_score": score, **author_scores})
        MATCH['Decision'].extend(["Relate", "Relate"])
        COUNT['title_author_match'] += 1
        if date_ok:
            COUNT['relations_via_date_assist'] += 1
        if not strict_name_confirmed:
            if overlap_confirmed:
                COUNT['relations_via_author_overlap'] += 1
            elif shared_orcid:
                COUNT['relations_via_orcid'] += 1
            elif shared_roster:
                COUNT['relations_via_roster'] += 1
    else:
        MATCH['Decision'].extend(["", ""])
        if not prerel and not primrel:
            NEARMISS.append({"predoi": predoi, "pretitle": pretitle,
                              "primdoi": primdoi, "primtitle": primtitle,
                              "score": score, "first_score": first_score,
                              "last_score": last_score, "predate": predate,
                              "primdate": primdate})


def build_version_siblings():
    ''' Build a DOI -> set(sibling DOIs) map for preprint version chains, using the
        same version-relation vocabulary as utility/bin/apply_version_tags.py (Crossref's
        "relation" field and DataCite's "relatedIdentifiers"). Siblings are different
        DOIs for what is fundamentally the same preprint (e.g. v1/v2/v3 on a preprint
        server) - relating one to a primary should relate all of them.
        Keyword arguments:
          None
        Returns:
          Dict of preprint DOI -> set of sibling preprint DOIs
    '''
    siblings = collections.defaultdict(set)
    def link(doi_a, doi_b):
        siblings[doi_a].add(doi_b)
        siblings[doi_b].add(doi_a)
    for prerec in PREPRINT.values():
        doi = prerec['doi']
        relation = prerec.get('relation') or {}
        for reltype in CROSSREF_VERSION_RELATIONS:
            for rec in relation.get(reltype) or []:
                if rec.get("id-type") == "doi" and rec.get("id"):
                    link(doi, rec["id"].lower())
        for rec in prerec.get('relatedIdentifiers') or []:
            if rec.get('relatedIdentifierType') == 'DOI' \
               and rec.get('relationType') in DATACITE_VERSION_RELATIONS \
               and rec.get('relatedIdentifier'):
                link(doi, rec['relatedIdentifier'].lower())
    return siblings


def propagate_version_relations():
    ''' Propagate confirmed preprint relations across version siblings (see
        build_version_siblings): if one version of a preprint has a confirmed relation
        to a primary, every other known version of that same preprint should get the
        same relation, even if that sibling's own title/authors never independently
        matched anything (e.g. an early draft posted under a different working title).
        Runs to a fixed point since a version chain can be more than one hop
        (v1 -> v2 -> v3), and propagating to v2 may newly qualify v1 too. Must run
        after explicit relations and the entire fuzzy comparison pass, since it acts
        on the final PREPRINTREL state.
        Keyword arguments:
          None
        Returns:
          None
    '''
    siblings = build_version_siblings()
    if not siblings:
        return
    changed = True
    while changed:
        changed = False
        for predoi, primdois in list(PREPRINTREL.items()):
            for sibling in siblings.get(predoi, ()):
                for primdoi in list(primdois):
                    before = COUNT['relations_created']
                    make_doi_relationships(sibling, primdoi, source="version")
                    if COUNT['relations_created'] != before:
                        changed = True


def build_curation_list():
    ''' Filter near-miss title matches (title matched, author check failed, neither
        DOI had a pre-existing relation) down to pairs that are still unresolved
        after the full comparison pass. This must run after the explicit-relation
        pass, the entire fuzzy comparison loop, AND version-sibling propagation,
        since a near-miss candidate's preprint or primary DOI may have been resolved
        by a different match found later in the run, or by inheriting a relation
        from a version sibling rather than any match of its own.
        Keyword arguments:
          None
        Returns:
          None
    '''
    for cand in NEARMISS:
        if cand['predoi'] in PREPRINTREL or cand['primdoi'] in PRIMARYREL:
            continue
        CURATE['Preprint DOI'].append(cand['predoi'])
        CURATE['Preprint Title'].append(cand['pretitle'])
        CURATE['Primary DOI'].append(cand['primdoi'])
        CURATE['Primary Title'].append(cand['primtitle'])
        CURATE['Score'].append(cand['score'])
        CURATE['First author score'].append(cand['first_score'])
        CURATE['Last author score'].append(cand['last_score'])
        CURATE['Preprint publishing date'].append(cand['predate'])
        CURATE['Primary publishing date'].append(cand['primdate'])
        COUNT['needs_curation'] += 1


def zero_candidate_preprints():
    ''' Preprint DOIs with a real title that never scored within the title-matching
        grace window against ANY primary this run - i.e. nothing came close on
        title alone. Distinct from NEARMISS/CURATE (title matched, author didn't)
        and from skipped_no_title (title field itself was missing): this is the
        "no candidate at all" case, useful for telling "not published yet" apart
        from "something's wrong with matching" without inspecting every preprint.
        Keyword arguments:
          None
        Returns:
          Sorted list of preprint DOIs
    '''
    return sorted(doi for doi, rec in PREPRINT.items()
                  if DL.get_title(rec) != "No title" and doi not in TITLE_CANDIDATE_SEEN)


def get_record(doi):
    ''' Look up a DOI's already-loaded record, regardless of role
        Keyword arguments:
          doi: DOI
        Returns:
          The DOI record from PREPRINT or PRIMARY, or None if never loaded locally
    '''
    return PREPRINT.get(doi) or PRIMARY.get(doi)


def doi_role(doi):
    ''' Determine which role a DOI was loaded under, for email display
        Keyword arguments:
          doi: DOI
        Returns:
          "Preprint", "Primary", or "DOI" if never loaded locally
    '''
    if doi in PREPRINT:
        return "Preprint"
    if doi in PRIMARY:
        return "Primary"
    return "DOI"


def get_current_relation(doi):
    ''' Look up a DOI's current jrc_preprint value from the already-loaded PRIMARY/
        PREPRINT records, with no additional database read - jrc_preprint is already
        in the projection used at load time (see existing_relation). Used two ways by
        write_to_database(): as the dry-run would-change/already-correct prediction,
        and as the base value the newly-discovered relations are unioned with before
        writing (so an existing relation - manually curated or from a prior run -
        that this run doesn't happen to rediscover is preserved, not overwritten).
        Keyword arguments:
          doi: DOI
        Returns:
          Sorted list of currently-related DOIs, or None if this DOI was never loaded
          locally (only referenced via a relation - see MISSING) and so is unknown
          without a database read
    '''
    rec = get_record(doi)
    if rec is None:
        return None
    return sorted(rec.get('jrc_preprint') or [])


def write_to_database():
    ''' Write relationships to the database. A DOI may accumulate relations from
        both the preprint side and the primary side (e.g. a record that plays both
        roles) - merge them per DOI instead of issuing two independent overwrites.
        The relations discovered this run are UNIONED with the DOI's existing
        jrc_preprint value rather than replacing it outright: utility/bin/
        add_preprint.py (the manual-curation tool fed by preprints_needs_curation.xlsx)
        is itself additive, and a plain overwrite would silently erase a human's
        manually-curated relation - or any prior relation this run simply doesn't
        happen to rediscover - the next time this script runs.
        DOIs known to be in the to_ignore collection are never in the local dois
        collection, so both writing to one directly and leaving one embedded inside
        another DOI's relation set would reference something that doesn't exist -
        both are scrubbed.
        Every DOI's final (merged) relation list is diffed against its current value
        (see get_current_relation) regardless of --write, so dry runs get a real
        would-change/already-correct prediction. When --write is set, the actual
        update_one result (matched_count/modified_count) is also recorded, which
        should match the dry-run prediction exactly as a sanity check. The write
        uses a case-insensitive collation since DOI casing isn't guaranteed
        consistent in the dois collection.
        A failure writing one DOI is logged and counted rather than aborting the
        remaining writes. Every element added by the union (i.e. not present in the
        DOI's pre-existing value) is recorded in NEW_PAIRS for generate_email(), along
        with whether the DOI's own value was empty before this run (EMPTY_BEFORE) so
        the email can distinguish a brand-new relation from an addition to an
        already-established one. A DOI with no pre-existing value at all (current is
        None, never loaded locally) is excluded from both, since there's nothing
        reliable to diff against.
        Keyword arguments:
          None
        Returns:
          None
    '''
    combined = {}
    for predoi, primdois in PREPRINTREL.items():
        combined.setdefault(predoi, set()).update(primdois)
    for primdoi, predois in PRIMARYREL.items():
        combined.setdefault(primdoi, set()).update(predois)
    ignored = [doi for doi in combined if doi in IGNORE]
    for doi in ignored:
        del combined[doi]
    for related in combined.values():
        related -= IGNORE
    COUNT['dois_ignored_for_update'] = len(ignored)
    COUNT['dois_flagged_for_update'] = len(combined)
    for doi, related in tqdm(combined.items(), desc="Write relations"):
        current = get_current_relation(doi)
        if current is None:
            COUNT['dois_unknown_current_value'] += 1
            merged = sorted(related)
        else:
            merged = sorted(set(related) | set(current))
            if merged == current:
                COUNT['dois_would_be_unchanged'] += 1
            else:
                COUNT['dois_would_change'] += 1
                EMPTY_BEFORE[doi] = not current
                for added in set(merged) - set(current):
                    NEW_PAIRS.append((doi, added))
        AUDIT.append(f"{doi} -> {merged}")
        if not ARG.WRITE:
            continue
        try:
            result = DB['dis'].dois.update_one({"doi": doi},
                                               {"$set": {"jrc_preprint": merged}},
                                               collation=INSENSITIVE)
        except Exception as err:
            COUNT['write_errors'] += 1
            LOGGER.error(f"Could not update {doi}: {err}")
            continue
        if not result.matched_count:
            COUNT['dois_not_found'] += 1
            LOGGER.warning(f"{doi} was not found in the dois collection")
        elif result.modified_count:
            COUNT['dois_written'] += 1
        else:
            COUNT['dois_unchanged'] += 1


def write_output_files():
    ''' Write audit, title-match, needs-curation, missing-DOI, and zero-candidate
        output files
        Keyword arguments:
          None
        Returns:
          None
    '''
    if AUDIT:
        file_name = "preprints_audit.txt"
        with open(file_name, 'w', encoding='utf-8') as ostream:
            for line in AUDIT:
                ostream.write(f"{line}\n")
        LOGGER.warning(f"Audit written to {file_name}")
    if MATCH['DOI']:
        file_name = "preprints_title_matches.xlsx"
        df = pd.DataFrame.from_dict(MATCH)
        df.to_excel(file_name, index=False)
        LOGGER.warning(f"Title matches written to {file_name}")
    if CURATE['Preprint DOI']:
        file_name = "preprints_needs_curation.xlsx"
        df = pd.DataFrame.from_dict(CURATE)
        df.to_excel(file_name, index=False)
        LOGGER.warning(f"Pairs needing curation written to {file_name}")
    if MISSING:
        file_name = "preprints_missing_dois.txt"
        with open(file_name, 'w', encoding='utf-8') as ostream:
            for line in MISSING:
                ostream.write(f"{line}\n")
        LOGGER.warning(f"Missing DOIs written to {file_name}")
    zero_candidates = zero_candidate_preprints()
    if zero_candidates:
        file_name = "preprints_zero_candidates.txt"
        with open(file_name, 'w', encoding='utf-8') as ostream:
            for doi in zero_candidates:
                ostream.write(f"{doi}\n")
        LOGGER.warning(f"Zero-candidate preprints written to {file_name}")


def doiurl(doi, color=None):
    ''' Format a DOI as a DIS UI link
        Keyword arguments:
          doi: DOI to format
          color: optional link text color (e.g. for legibility on a dark card background)
        Returns:
          HTML anchor
    '''
    style = f" style='color:{color};'" if color else ""
    return f"<a href='https://dis.int.janelia.org/doiui/{doi}'{style}>{doi}</a>"


def dedupe_new_pairs():
    ''' Deduplicate NEW_PAIRS and classify each edge as brand-new (neither DOI had
        any relation before this run) or an addition (at least one side already had
        a relation to something else). Each true new edge is recorded twice in
        NEW_PAIRS (once from each DOI's perspective in write_to_database) unless one
        side of the pair was never loaded locally, in which case it's only recorded
        once (from the loaded side) - either way, a canonical unordered key collapses
        it to one. When only one side's "was empty" state is known (the other side
        was never loaded locally), the edge is conservatively classified as an
        addition rather than assumed brand-new.
        Keyword arguments:
          None
        Returns:
          List of (doi_a, doi_b, brand_new) tuples, one per newly-created edge
    '''
    seen = set()
    pairs = []
    for doi_a, doi_b in NEW_PAIRS:
        key = frozenset((doi_a, doi_b))
        if key in seen:
            continue
        seen.add(key)
        empty_a = EMPTY_BEFORE.get(doi_a)
        empty_b = EMPTY_BEFORE.get(doi_b)
        brand_new = bool(empty_a) and bool(empty_b) \
            if empty_a is not None and empty_b is not None else False
        pairs.append((doi_a, doi_b, brand_new))
    return pairs


def edge_detail(doi_a, doi_b):
    ''' Describe how an edge was created, for display under the connector badge in
        new_pair_card(). Looks up EDGE_INFO under either DOI ordering, since
        predoi/primdoi order isn't preserved once a pair is deduped for display.
        For a fuzzy edge, shows whichever evidence actually confirmed authorship -
        strict first/last scores, the specific overlap match and its score, the
        shared ORCID, or the shared Janelia roster identity - matching the
        priority order process_pair() itself uses, so e.g. an overlap-confirmed
        match never shows a misleadingly low strict score.
        Keyword arguments:
          doi_a: one DOI in the pair
          doi_b: the other DOI in the pair
        Returns:
          Short HTML string describing the source/scores, or "" if unknown
    '''
    info = EDGE_INFO.get((doi_a, doi_b)) or EDGE_INFO.get((doi_b, doi_a)) or {}
    if "first_author_score" in info:
        author_line = f"First author: {info['first_author_score']:.0f} &nbsp;|&nbsp; " \
                      + f"Last author: {info['last_author_score']:.0f}"
    elif "author_overlap_score" in info:
        author_line = "Author overlap match: " \
                      + f"\"{info['author_overlap_match']}\" " \
                      + f"({info['author_overlap_score']:.0f})"
    elif "shared_orcid" in info:
        author_line = f"Shared ORCID: {info['shared_orcid']}"
    elif "shared_roster_id" in info:
        author_line = f"Shared Janelia identity (roster): {info['shared_roster_id']}"
    elif info.get("source") == "explicit":
        return "Source: explicit metadata"
    elif info.get("source") == "version":
        return "Source: version propagation"
    else:
        return ""
    return f"Title score: {info['title_score']:.0f} &nbsp;|&nbsp; {author_line}"


def new_pair_card(doi_a, doi_b, brand_new):
    ''' Build one HTML card for a newly-created preprint/primary relation, showing
        each DOI's role (Preprint/Primary/DOI, via doi_role), link, and title
        (when the record was loaded locally), connected by a label distinguishing
        a brand-new relation from an addition to an already-established one, plus
        the title/author scores (fuzzy matches) or source (explicit/version) that
        produced the edge (see edge_detail).
        Keyword arguments:
          doi_a: one DOI in the pair
          doi_b: the other DOI in the pair
          brand_new: True if neither DOI had any relation before this run
        Returns:
          HTML string
    '''
    def side(doi):
        rec = get_record(doi)
        title = DL.get_title(rec) if rec else None
        title_html = f"<div style='margin:3px 0 0 4px; color:#555; font-size:13px;'>" \
                     + f"{title}</div>" if title and title != "No title" else ""
        return f"<span style='{ROLE_BADGE[doi_role(doi)]}'>{doi_role(doi)}</span> " \
               + f"{doiurl(doi)}{title_html}"
    if brand_new:
        connector_style = "background-color:#8e44ad; color:#fff; padding:2px 10px; " \
                           "border-radius:10px; font-size:11px; font-weight:bold;"
        connector = "&#10024; NEW RELATION"
    else:
        connector_style = "background-color:#e67e22; color:#fff; padding:2px 10px; " \
                           "border-radius:10px; font-size:11px; font-weight:bold;"
        connector = "&#43; ADDED TO EXISTING RELATION"
    detail = edge_detail(doi_a, doi_b)
    detail_html = f"<div style='color:#888; font-size:11px; margin:2px 0 0 0;'>" \
                  + f"{detail}</div>" if detail else ""
    return "<div style='border:1px solid #e0e0e0; border-radius:8px; padding:10px 14px; " \
           + "margin:0 0 10px 0; background-color:#fafafa;'>" \
           + side(doi_a) \
           + "<div style='text-align:center; margin:6px 0;'>" \
           + f"<span style='{connector_style}'>{connector}</span>{detail_html}</div>" \
           + side(doi_b) + "</div>"


def missing_group_card(associated_doi, missing_dois):
    ''' Build one HTML card for a group of missing DOIs (see MISSING) that all
        share the same associated DOI from this run's relations - typically a
        known primary with one or more preprint DOIs (e.g. version siblings)
        that were referenced but never loaded into the dois collection.
        The title/ignore-reason annotation is only ever attempted for
        associated_doi, not for the items in missing_dois: every DOI in
        missing_dois is - by construction in make_doi_relationships - guaranteed
        absent from PREPRINT, PRIMARY, and IGNORE for the whole run, so it could
        never have a local title or an IGNORE_REASON entry to show.
        Keyword arguments:
          associated_doi: the DOI the missing DOIs were related to
          missing_dois: sorted list of missing DOIs associated with associated_doi
        Returns:
          HTML string
    '''
    link_color = "#8ecbff"
    rec = get_record(associated_doi)
    title = DL.get_title(rec) if rec else None
    if title and title != "No title":
        title_html = f"<div style='margin:3px 0 0 4px; color:#c7d0dc; font-size:13px;'>" \
                      + f"{html.escape(title)}</div>"
    elif associated_doi in IGNORE_REASON:
        reason = html.escape(IGNORE_REASON[associated_doi])
        title_html = f"<div style='margin:3px 0 0 4px; color:#e8a33d; font-size:13px; " \
                      + f"font-style:italic;'>Ignored: {reason}</div>"
    else:
        title_html = ""
    role = doi_role(associated_doi)
    header = f"<span style='{ROLE_BADGE[role]}'>{role}</span> " \
             + f"{doiurl(associated_doi, color=link_color)}{title_html}"
    items = "".join(f"<li style='margin:2px 0;'>{doiurl(doi, color=link_color)} " \
                     + f"<span style='{ROLE_BADGE[doi_role(doi)]}'>{doi_role(doi)}</span></li>"
                     for doi in missing_dois)
    return "<div style='border:1px solid #c0392b; border-radius:8px; padding:10px 14px; " \
           + "margin:0 0 10px 0; background-color:#101020; color:#e8ecf1;'>" \
           + header \
           + "<div style='margin:6px 0 2px 4px; color:#a9b4c4; font-size:12px;'>" \
           + f"Missing DOI(s) referenced ({len(missing_dois)}):</div>" \
           + f"<ul style='margin:0 0 0 14px; padding:0;'>{items}</ul></div>"


def missing_doi_groups_html():
    ''' Build an HTML section grouping missing DOIs (referenced by a relation this
        run but never loaded locally - see MISSING) by the other DOI each was
        associated with, so a curator can see at a glance which known record each
        missing DOI probably belongs to, without cross-referencing the attached
        preprints_missing_dois.txt by hand. A missing DOI referenced from more than
        one associated DOI (rare) appears in more than one group, so a card's own
        count can't be summed across cards to reconstruct len(MISSING).
        When the "associated DOI" is itself missing too (both sides of a relation
        unresolved - e.g. two never-loaded version siblings related to each other),
        that pair would otherwise produce two mirror-image cards, one headed by
        each side. Canonicalized to a single card, headed by whichever of the two
        DOIs sorts first, by only processing such a pair once.
        Keyword arguments:
          None
        Returns:
          HTML string, or "" if MISSING is empty
    '''
    if not MISSING:
        return ""
    groups = collections.defaultdict(set)
    for missing_doi, associated in MISSING.items():
        for assoc_doi in associated:
            if assoc_doi in MISSING:
                if missing_doi > assoc_doi:
                    continue
                header, item = missing_doi, assoc_doi
            else:
                header, item = assoc_doi, missing_doi
            groups[header].add(item)
    cards = "".join(missing_group_card(assoc_doi, sorted(missing_dois))
                     for assoc_doi, missing_dois in sorted(groups.items()))
    return "<p style='margin:14px 0 6px 0;'><strong>Missing DOIs by associated record:</strong> " \
           + f"{len(MISSING):,} distinct DOI(s) referenced but not in the database, across " \
           + f"{len(groups):,} associated record(s) below (a DOI tied to more than one " \
           + "record appears in more than one card, so card counts won't sum to the " \
           + "total above).</p>" + cards


def generate_email():
    ''' Build and send an HTML summary email highlighting newly-created preprint/
        primary relations from this run - either a brand-new pairing, or an
        additional relation added to a DOI that already had at least one (see
        NEW_PAIRS, populated in write_to_database). Brand-new relations are listed
        before additions. Also calls out DOIs that were referenced in a relation
        but never loaded locally (COUNT['dois_unknown_current_value']) - those are
        silently excluded from the matches list above, since there's no local
        record to diff/label against, and that exclusion is otherwise easy to miss.
        preprints_missing_dois.txt is attached whenever it was written (MISSING is
        non-empty), so the referenced DOIs are one click away rather than requiring
        server access - the missing_doi_groups_html section below also lists them
        inline, grouped by the record each was referenced from. Follows the same
        convention as sync_citations.py/pull_arxiv.py: sent only when --test
        (developer only) or --write (real recipients) is set, and skipped entirely
        if there's nothing new to report (no new pairs and no missing DOIs) -
        including on a plain dry run with neither flag.
        Keyword arguments:
          None
        Returns:
          None
    '''
    pairs = dedupe_new_pairs()
    if not pairs and not MISSING:
        LOGGER.info("No new preprint/primary matches or missing DOIs - skipping email")
        return
    pairs.sort(key=lambda pair: not pair[2])  # brand-new relations (True) first
    cards = "".join(new_pair_card(doi_a, doi_b, brand_new) for doi_a, doi_b, brand_new in pairs)
    brand_new_count = sum(1 for *_, brand_new in pairs if brand_new)
    summary = "<p><strong>New matches:</strong> " + f"{len(pairs):,}" \
              + f" ({brand_new_count:,} new relations, " \
              + f"{len(pairs) - brand_new_count:,} additions)" \
              + " &nbsp;|&nbsp; <strong>DOIs flagged for update:</strong> " \
              + f"{COUNT['dois_flagged_for_update']:,}" \
              + " &nbsp;|&nbsp; <strong>DOIs changed:</strong> " \
              + f"{COUNT['dois_would_change']:,}</p>"
    unknown_note = ""
    if COUNT['dois_unknown_current_value']:
        unknown_note = "<p style='color:#c0392b; font-size:13px; margin:0 0 12px 0;'>" \
                       + f"&#9888; {COUNT['dois_unknown_current_value']:,} DOI(s) " \
                       + "referenced in this run's relations were never loaded locally, " \
                       + "so their current value is unknown and they're excluded from " \
                       + "the matches below - see the attached preprints_missing_dois.txt.</p>"
    missing_html = missing_doi_groups_html()
    separator = "<div style='margin:24px 0 10px 0; font-size:20px; font-weight:bold;'>" \
                + "New/updated relations</div>" if missing_html and cards else ""
    email_html = "<div style='font-family:Arial,Helvetica,sans-serif; color:#222;'>" \
                 + JRC.get_run_data(__file__, __version__) + "<br><br>" \
                 + summary + unknown_note + missing_html + separator + cards + "</div>"
    try:
        email = DISCONFIG['developer'] if ARG.TEST else DISCONFIG['receivers']
        LOGGER.info(f"Sending email to {email}")
        # write_output_files() runs before generate_email() in add_jrc_preprint(),
        # so preprints_missing_dois.txt already exists on disk whenever MISSING is
        # non-empty - same condition write_output_files() itself uses to write it.
        attachment = "preprints_missing_dois.txt" if MISSING else None
        JRC.send_email(email_html, DISCONFIG['sender'], email, EMAIL_SUBJECT,
                       attachment=attachment, mime='html')
    except Exception as err:
        LOGGER.error(f"Could not send email: {err}")


def print_summary():
    ''' Print a run summary
        Keyword arguments:
          None
        Returns:
          None
    '''
    rows = [("Primary DOIs", len(PRIMARY)),
            ("Preprint DOIs", len(PREPRINT))]
    rows.extend([("Comparisons", COUNT['comparisons']),
                 ("Skipped (no title)", COUNT['skipped_no_title']),
                 ("Skipped (no author)", COUNT['skipped_no_author']),
                 ("  ...of which author-list parsing errors", COUNT['author_parse_errors']),
                 ("Preprints with zero title candidates", len(zero_candidate_preprints())),
                 ("Title matches", COUNT['title_match']),
                 ("Title/author matches", COUNT['title_author_match']),
                 ("  ...confirmed via author overlap", COUNT['relations_via_author_overlap']),
                 ("  ...confirmed via shared ORCID", COUNT['relations_via_orcid']),
                 ("  ...confirmed via Janelia roster", COUNT['relations_via_roster']),
                 ("  ...confirmed via date-assisted near-miss title",
                  COUNT['relations_via_date_assist']),
                 ("Pairs needing curation", COUNT['needs_curation']),
                 ("Preprint DOIs with relations", len(PREPRINTREL)),
                 ("Primary DOIs with relations", len(PRIMARYREL)),
                 ("Relations from explicit metadata", COUNT['relations_from_explicit']),
                 ("Relations from fuzzy matching", COUNT['relations_from_fuzzy']),
                 ("Relations from version propagation", COUNT['relations_from_version']),
                 ("Total relations created", COUNT['relations_created']),
                 ("Missing DOIs referenced", len(MISSING)),
                 ("DOIs ignored (in to_ignore, skipped)", COUNT['dois_ignored_for_update']),
                 ("DOIs flagged for jrc_preprint update", COUNT['dois_flagged_for_update']),
                 ("DOIs predicted to change (dry-run diff)", COUNT['dois_would_change']),
                 ("DOIs predicted already correct (dry-run diff)",
                  COUNT['dois_would_be_unchanged']),
                 ("DOIs with unknown current value", COUNT['dois_unknown_current_value'])])
    if ARG.WRITE:
        rows.extend([("DOIs actually written (value changed)", COUNT['dois_written']),
                     ("DOIs actually already up to date", COUNT['dois_unchanged']),
                     ("DOIs not found in collection", COUNT['dois_not_found']),
                     ("Write failures", COUNT['write_errors'])])
    width = max(len(label) for label, _ in rows) + 2
    for label, value in rows:
        print(f"{(label + ':'):<{width}}{value:,}")
    if not ARG.WRITE:
        LOGGER.warning(f"Dry run: {COUNT['dois_flagged_for_update']:,} DOIs flagged, "
                       f"{COUNT['dois_would_change']:,} would actually change "
                       "(use --write to apply)")


def add_jrc_preprint():
    ''' Update the jrc_preprint field in the dois collection. All three
        relation-building phases (explicit metadata, fuzzy comparison, version
        propagation) run inside one try/except so that a malformed record anywhere
        still leaves build_curation_list/write_to_database/write_output_files/
        print_summary to run against whatever was accumulated - rather than a raw
        unhandled exception before any of them ever execute. The new-matches email
        (see generate_email) is sent even if a partial error occurred, since the
        accumulated results up to that point are still meaningful - matching
        --test/--write's "send even on other bad news" semantics elsewhere in this
        codebase (e.g. sync_citations.py).
        Keyword arguments:
          None
        Returns:
          None
    '''
    error = None
    try:
        process_explicit_relations()
        for prerec in tqdm(PREPRINT.values(), desc="Preprints"):
            for primrec in PRIMARY.values():
                process_pair(prerec, primrec)
        # Propagate confirmed relations across preprint version siblings
        propagate_version_relations()
    except Exception as err:
        error = err
        LOGGER.error(f"Relation-building aborted early, writing partial results: {err}")
    # Resolve which near-misses are still unrelated after the full pass
    build_curation_list()
    # Write to dois collection
    write_to_database()
    if COUNT['write_errors'] and not error:
        error = RuntimeError(f"{COUNT['write_errors']} DOI(s) failed to update")
    # Output files
    write_output_files()
    # Summary
    print_summary()
    if ARG.TEST or ARG.WRITE:
        generate_email()
    if error:
        terminate_program(error)

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Update jrc_preprint in the dois collection")
    PARSER.add_argument('--title-threshold', dest='TITLE_THRESHOLD', action='store',
                        default=90, type=int, help='Fuzzy matching threshold for titles')
    PARSER.add_argument('--author-threshold', dest='AUTHOR_THRESHOLD', action='store',
                        default=85, type=int,
                        help='Fuzzy matching threshold for author names')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, prod)')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False,
                        help='Write jrc_preprint updates to the dois collection '
                             '(default is a dry run)')
    PARSER.add_argument('--test', dest='TEST', action='store_true', default=False,
                        help='Send the new-matches summary email to the developer only')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    try:
        DISCONFIG = JRC.simplenamespace_to_dict(JRC.get_config("dis"))
    except Exception as err:
        terminate_program(err)
    initialize_program()
    add_jrc_preprint()
    terminate_program()
