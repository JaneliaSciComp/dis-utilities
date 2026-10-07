''' dis_funder_lib.py
    Parse funder identifiers out of registrar metadata and roll them up the
    Crossref Funder Registry hierarchy.

    The two registrars do not agree on how to name a funder. Crossref deposits
    a Funder Registry DOI (10.13039/100000011); DataCite deposits whatever the
    submitter chose, most often a ROR. Both are normalized here to the bare
    Funder Registry ID, which is what the Crossref funders API takes and what
    the rollup below is keyed on. A ROR is crosswalked through ROR's own
    external_ids.fundref.

    Rollup exists because the registry is a tree: NINDS (100000065) sits under
    NIH (100000002), which sits under HHS (100000016). A paper funded by NINDS
    is NIH-funded, and asking "which papers did NIH fund" has to find it. Each
    record therefore stores every funder's own ID *and* its ancestors in one
    flat array, so a single indexed term answers the question - no $graphLookup
    at query time and no client-side expansion.

    Registry lookups are cached in the "funder" collection, since the hierarchy
    changes on the order of never and the API is rate-limited.
'''

__version__ = '1.0.0'

import re
import time
from datetime import datetime
import requests

# pylint: disable=broad-exception-caught

# Crossref Funder Registry. A funder DOI is always the 10.13039 prefix plus a
# numeric suffix; the suffix alone is the API's key and what we store.
FUNDER_PREFIX = '10.13039/'
FUNDER_DOI = re.compile(r'^(?:https?://(?:dx\.)?doi\.org/)?10\.13039/(\d+)$', re.I)
FUNDER_BARE = re.compile(r'^\d+$')
CROSSREF_FUNDERS = 'https://api.crossref.org/funders/'
# ROR -> Funder Registry crosswalk. ROR records carry a fundref external id.
ROR_API = 'https://api.ror.org/v2/organizations/'
ROR_ID = re.compile(r'^(?:https?://ror\.org/)?(0[0-9a-z]{8})$', re.I)
TIMEOUT = 15
RETRIES = 3
BACKOFF = 2


def normalize_funder_id(value):
    ''' Reduce a funder identifier to its bare Funder Registry ID.
        Keyword arguments:
          value: a funder DOI, DOI URL, or bare ID
        Returns:
          Bare numeric ID string, or None if it is not a Funder Registry ID
    '''
    if not value:
        return None
    text = str(value).strip()
    match = FUNDER_DOI.match(text)
    if match:
        return match.group(1)
    return text if FUNDER_BARE.match(text) else None


def normalize_ror(value):
    ''' Reduce a ROR identifier to its bare form
        Keyword arguments:
          value: a ROR URL or bare ROR ID
        Returns:
          Bare ROR ID, or None
    '''
    if not value:
        return None
    match = ROR_ID.match(str(value).strip())
    return match.group(1).lower() if match else None


def _get_json(url):
    ''' GET JSON with a short retry, returning None rather than raising. These
        are enrichment lookups: a funder we cannot resolve costs us its
        ancestors, not the record.
        Keyword arguments:
          url: full request URL
        Returns:
          Parsed JSON dict, or None
    '''
    for attempt in range(1, RETRIES + 1):
        try:
            resp = requests.get(url, timeout=TIMEOUT)
        except requests.exceptions.RequestException:
            if attempt < RETRIES:
                time.sleep(BACKOFF * attempt)
            continue
        if resp.status_code == 200:
            try:
                return resp.json()
            except ValueError:
                return None
        if resp.status_code in (429, 503):
            if attempt < RETRIES:
                time.sleep(BACKOFF * attempt)
            continue
        return None
    return None


def _ancestors_from_tree(tree, target):
    ''' Walk the Crossref hierarchy tree to the target, collecting the path.

        The tree is rooted at the topmost ancestor, not at the funder asked
        for, so the funder's own ID has to be located inside it. Subtrees the
        API truncates carry a "more" key, which is a marker rather than a node.
        Keyword arguments:
          tree: the "hierarchy" object from a funders API response
          target: bare funder ID being located
        Returns:
          List of ancestor IDs, outermost first; empty if not found or top-level
    '''
    path = []

    def walk(node, trail):
        for key, val in node.items():
            if key == 'more':
                continue
            if key == target:
                path.extend(trail)
                return True
            if isinstance(val, dict) and walk(val, trail + [key]):
                return True
        return False

    walk(tree or {}, [])
    return path


def funder_record(fid, coll=None, write=True):
    ''' Resolve a funder to its registry name and ancestor chain, caching the
        result. The hierarchy effectively never changes, and the API is
        rate-limited, so a cache miss is the only time we call out.
        Keyword arguments:
          fid: bare Funder Registry ID
          coll: funder collection for the cache (optional)
          write: store a freshly fetched record in the cache
        Returns:
          dict with id, name, ancestors - or None if unresolvable
    '''
    fid = normalize_funder_id(fid)
    if not fid:
        return None
    if coll is not None:
        cached = coll.find_one({"id": fid})
        # An entry with no name is incomplete rather than cached: ror_to_funder
        # upserts {id, ror} without resolving the funder. Treating it as a hit
        # made the gap permanent - the short-circuit returned the empty record
        # and nothing ever went back for the name.
        if cached and cached.get('name'):
            return {"id": fid, "name": cached['name'],
                    "ancestors": cached.get('ancestors') or []}
    data = _get_json(f"{CROSSREF_FUNDERS}{fid}")
    if not data or 'message' not in data:
        return None
    msg = data['message']
    rec = {"id": fid, "name": msg.get('name'),
           "ancestors": _ancestors_from_tree(msg.get('hierarchy'), fid)}
    if coll is not None and write:
        try:
            coll.update_one({"id": fid},
                            {"$set": {**rec, "updated": datetime.now()}},
                            upsert=True)
        except Exception:
            pass
    return rec


def ror_to_funder(ror, coll=None):
    ''' Crosswalk a ROR to a Funder Registry ID through ROR's external_ids.
        DataCite submitters most often give a ROR, and without this the
        DataCite half of the collection cannot join to the Crossref half.
        Keyword arguments:
          ror: ROR ID or URL
          coll: funder collection, used to cache the crosswalk
        Returns:
          Bare Funder Registry ID, or None
    '''
    ror = normalize_ror(ror)
    if not ror:
        return None
    if coll is not None:
        cached = coll.find_one({"ror": ror})
        if cached and cached.get('id'):
            return cached['id']
    data = _get_json(f"{ROR_API}{ror}")
    if not data:
        return None
    for ext in data.get('external_ids') or []:
        if str(ext.get('type', '')).lower() != 'fundref':
            continue
        fid = normalize_funder_id(ext.get('preferred') or (ext.get('all') or [None])[0])
        if fid and coll is not None:
            try:
                coll.update_one({"id": fid}, {"$set": {"ror": ror}}, upsert=True)
            except Exception:
                pass
            # That upsert alone leaves a cache entry holding nothing but an id
            # and a ror, so the funder renders as "Funder 501100001671" on its
            # own page. Resolve it properly while we are here.
            funder_record(fid, coll)
        return fid
    return None


def _salvage_funder_doi(value):
    ''' Recover a funder DOI from a deposit that ran other content into the
        field, e.g. "10.13039/100000002,%22national%20institutes...". The part
        before the first comma is unambiguous; anything after it is not a DOI.
        Keyword arguments:
          value: the raw DOI field
        Returns:
          Bare funder ID, or None
    '''
    if not value or ',' not in str(value):
        return None
    return normalize_funder_id(str(value).split(',', 1)[0])


def _crossref_funders(rec, coll=None):
    ''' Parse the Crossref funder array.

        A funder's ID can arrive three ways. Most deposits put a Funder
        Registry DOI in the DOI field. Some carry it only in the parallel id[]
        array, and a growing number put a *ROR* there instead - Crossref now
        accepts both, so looking only for id-type DOI silently loses them.
        Keyword arguments:
          rec: DOI record
          coll: funder collection, for the ROR crosswalk cache
        Returns:
          List of {id, name, awards, ror} dicts, id possibly None
    '''
    out = []
    for entry in rec.get('funder') or []:
        if not isinstance(entry, dict):
            continue
        fid = normalize_funder_id(entry.get('DOI')) or _salvage_funder_doi(entry.get('DOI'))
        ror = None
        for sub in entry.get('id') or []:
            if not isinstance(sub, dict):
                continue
            subtype = str(sub.get('id-type', '')).upper()
            if not fid and subtype == 'DOI':
                fid = normalize_funder_id(sub.get('id'))
            elif subtype == 'ROR' and not ror:
                ror = normalize_ror(sub.get('id'))
        if not fid and ror:
            fid = ror_to_funder(ror, coll)
        awards = [str(a) for a in (entry.get('award') or []) if a]
        out.append({"id": fid, "name": entry.get('name'), "awards": awards, "ror": ror})
    return out


def _datacite_funders(rec, coll=None):
    ''' Parse the DataCite fundingReferences array. funderIdentifierType is
        whatever the submitter chose - ROR, Crossref Funder ID, GRID, or
        nothing - so each is normalized separately and a ROR is crosswalked.
        Keyword arguments:
          rec: DOI record
          coll: funder collection, for the ROR crosswalk cache
        Returns:
          List of {id, name, awards, ror} dicts
    '''
    out = []
    for entry in rec.get('fundingReferences') or []:
        if not isinstance(entry, dict):
            continue
        ident = entry.get('funderIdentifier')
        itype = str(entry.get('funderIdentifierType') or '').lower()
        fid = ror = None
        if itype == 'ror' or normalize_ror(ident):
            ror = normalize_ror(ident)
            fid = ror_to_funder(ror, coll) if ror else None
        else:
            fid = normalize_funder_id(ident)
        awards = [str(entry[k]) for k in ('awardNumber',) if entry.get(k)]
        out.append({"id": fid, "name": entry.get('funderName'),
                    "awards": awards, "ror": ror})
    return out


def parse_funders(rec, coll=None):
    ''' Collect every funder named by a DOI record, from whichever registrar
        deposited it, merged by funder so a funder listed twice (Crossref
        repeats it in funder[] and funder[].id[]) appears once with its awards
        pooled.
        Keyword arguments:
          rec: DOI record
          coll: funder collection, for the ROR crosswalk cache
        Returns:
          List of {id, name, awards, ror} dicts, ordered as first seen
    '''
    merged = {}
    order = []
    for entry in _crossref_funders(rec, coll) + _datacite_funders(rec, coll):
        # Funders with no resolvable ID are kept and keyed on their name: they
        # are real funding, just not joinable. Dropping them would understate
        # the funding we hold; guessing an ID would misattribute it.
        key = entry.get('id') or f"name:{(entry.get('name') or '').strip().lower()}"
        if not entry.get('id') and not (entry.get('name') or '').strip():
            continue
        if key not in merged:
            merged[key] = {"id": entry.get('id'), "name": entry.get('name'),
                           "awards": [], "ror": entry.get('ror')}
            order.append(key)
        tgt = merged[key]
        if not tgt.get('name') and entry.get('name'):
            tgt['name'] = entry['name']
        if not tgt.get('ror') and entry.get('ror'):
            tgt['ror'] = entry['ror']
        for award in entry.get('awards') or []:
            if award not in tgt['awards']:
                tgt['awards'].append(award)
    out = []
    for key in order:
        entry = merged[key]
        if not entry.get('ror'):
            entry.pop('ror', None)
        out.append(entry)
    return out


def rollup_ids(funders, coll=None):
    ''' Every funder ID on a record plus all of their ancestors, deduplicated.

        This is the field a query actually uses: one indexed term finds a
        funder and everything beneath it, so "NIH-funded" matches a paper that
        names only NINDS.
        Keyword arguments:
          funders: list from parse_funders()
          coll: funder collection for the hierarchy cache
        Returns:
          Sorted list of bare funder IDs
    '''
    ids = set()
    for entry in funders or []:
        fid = entry.get('id')
        if not fid:
            continue
        ids.add(fid)
        rec = funder_record(fid, coll)
        if not rec:
            continue
        for ancestor in rec.get('ancestors') or []:
            ids.add(ancestor)
            # Resolve the ancestor too, purely to cache its name. It lands in
            # jrc_funder_ids and so becomes a /funder/<id> page of its own, but
            # nothing else would ever look it up: ancestors are never named
            # directly on a record. Without this, HHS is rolled up onto 803
            # DOIs and renders as "Funder 100000016".
            funder_record(ancestor, coll)
    return sorted(ids)
