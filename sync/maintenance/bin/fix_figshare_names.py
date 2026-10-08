''' fix_figshare_names.py
    Correct misspelled Janelia author names on figshare items.

    An author is credited on a DOI only when their name resolves against the
    orcid collection, so a name figshare holds slightly wrong - "Crystal Lopez"
    for "Crystall Lopez" - leaves real work uncredited, and no amount of
    affiliation checking recovers it. doi_common.name_mismatches already finds
    these; this program acts on the figshare subset of them.

    What figshare does and does not allow, which shapes everything here:

    There is no endpoint that renames an author record. The authors API is
    read-only (search and fetch). The only thing an item can be made to do is
    point at a DIFFERENT author record, so a correction is a swap, and the
    misspelled record survives, orphaned.

    Adding, editing or removing an author is on figshare's versioning trigger
    list, so publishing a corrected item mints a new version and a new .vN DOI.
    That is not a side effect this program can avoid; figshare can disable
    automatic versioning per institution, but only by support request. Nothing
    is written without --write and an explicit confirmation that names the
    consequence.

    Three situations turn up and only the first is actionable:
      ghost author   - is_active false and url_name "_", an unregistered name
                       entry. Swappable.
      registered     - a real figshare account whose own profile name is wrong.
                       Correcting it means editing that person's profile, which
                       changes their name on every item they have ever posted.
                       Reported, never touched.
      already fixed  - the name is absent from the item's current version. Our
                       DOI is a version DOI (.v1) whose frozen metadata still
                       carries the typo. Old versions cannot be modified by any
                       means. Reported, never touched.
'''

__version__ = '1.0.0'

import argparse
import collections
import json
from operator import attrgetter
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Database
DB = {}
# Counters
COUNT = collections.defaultdict(lambda: 0, {})
# Global variables
ARG = LOGGER = None
# figshare
API = 'https://api.figshare.com/v2'
# figshare asks for no more than one request a second and documents no 429
# contract, so the pause is ours to keep rather than something to back off from.
PAUSE = 1.0
# DOI prefixes figshare mints for Janelia. 10.6084 is figshare's own prefix,
# used before the institutional one.
FIGSHARE_PREFIXES = ('10.25378/', '10.6084/')
# An unregistered author has no profile, and figshare renders that as this
# url_name rather than an empty string.
GHOST_URL_NAME = '_'


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
    ''' Connect to the database and check the figshare token
        Keyword arguments:
          None
        Returns:
          None
    '''
    try:
        dbconfig = JRC.get_config("databases")
    except Exception as err:
        terminate_program(err)
    # Read-only: the corrections go to figshare, not to our copy. The next
    # DataCite sync is what brings the corrected name back into the collection.
    dbo = attrgetter(f"dis.{ARG.MANIFOLD}.read")(dbconfig)
    LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, ARG.MANIFOLD, dbo.host, dbo.user)
    try:
        DB['dis'] = JRC.connect_database(dbo)
    except Exception as err:
        terminate_program(err)
    if not ARG.TOKEN:
        terminate_program("No figshare token. Set FIGSHARE_JWT or pass --token.")
    # Fail here rather than three hundred requests in: a bad token reads as a
    # 403 on every call, which looks like a permissions problem with the data.
    try:
        call_figshare('GET', '/account')
    except Exception as err:
        terminate_program(f"figshare rejected the token: {err}")


def call_figshare(method, path, payload=None):
    ''' Call the figshare API
        Keyword arguments:
          method: HTTP method
          path: path below the API root
          payload: dict to send as JSON (optional)
        Returns:
          Decoded response, or None for an empty body
    '''
    url = f"{API}{path}"
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={'Authorization': f"token {ARG.TOKEN}",
                                          'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(req, timeout=60) as response:
            body = response.read()
            return json.loads(body) if body else None
    except urllib.error.HTTPError as err:
        detail = err.read().decode('utf-8', 'replace')[:300]
        raise RuntimeError(f"{method} {path} -> HTTP {err.code}: {detail}") from err
    finally:
        time.sleep(PAUSE)


def article_id(doi):
    ''' The figshare article ID a DOI refers to.

        Taken from the DOI rather than the stored url: both carry it, but the
        DOI is the thing we were asked about. A version DOI ("....v1") names
        the same article as its base, so the version is dropped here and
        handled by looking at what the current version actually holds.
        Keyword arguments:
          doi: figshare DOI
        Returns:
          Article ID string, or None
    '''
    tail = str(doi).rsplit('/', maxsplit=1)[-1]
    tail = tail.split('.v', maxsplit=1)[0]
    digits = tail.rsplit('.', maxsplit=1)[-1]
    return digits if digits.isdigit() else None


def roster_orcid(name):
    ''' The ORCID held for a roster name, used to tell apart several figshare
        author records sharing a name.
        Keyword arguments:
          name: "Given Family" as name_mismatches reports it
        Returns:
          ORCID string, or None
    '''
    parts = name.rsplit(' ', maxsplit=1)
    if len(parts) != 2:
        return None
    given, family = parts
    try:
        row = DB['dis'].orcid.find_one({"given": given, "family": family,
                                        "orcid": {"$exists": True}}, {"orcid": 1})
    except Exception:
        return None
    return (row or {}).get('orcid')


def find_author(authors, name):
    ''' The entry in an item's author list carrying a given name
        Keyword arguments:
          authors: author list from figshare
          name: name to look for
        Returns:
          (index, author) or (None, None)
    '''
    want = ' '.join(str(name).split()).lower()
    for idx, author in enumerate(authors):
        if ' '.join(str(author.get('full_name') or '').split()).lower() == want:
            return idx, author
    return None, None


def is_ghost(author):
    ''' Whether an author record is an unregistered name entry rather than an
        account. figshare exposes no flag for this; the url_name is the tell.
        Keyword arguments:
          author: figshare author record
        Returns:
          True when the record has no profile
    '''
    return str(author.get('url_name') or '').strip() in (GHOST_URL_NAME, '')


def replacement_for(name, orcid):
    ''' Candidate figshare author records for a correct name.

        Never falls back to sending the name as a string. figshare creates a
        fresh record for every name it is handed and does not deduplicate them,
        so doing that would swap one orphan for another and lose the ORCID.
        Keyword arguments:
          name: the roster spelling
          orcid: the roster ORCID, or None
        Returns:
          (chosen, candidates) - chosen is the unambiguous pick, or None
    '''
    try:
        found = call_figshare('POST', '/account/authors/search', {"search_for": name}) or []
    except Exception as err:
        LOGGER.warning(f"author search for {name}: {err}")
        return None, []
    want = ' '.join(name.split()).lower()
    exact = [a for a in found
             if ' '.join(str(a.get('full_name') or '').split()).lower() == want]
    if not exact:
        return None, []
    if orcid:
        # An ORCID settles it outright, and several of these names have two or
        # three records where only one carries one.
        matched = [a for a in exact if str(a.get('orcid_id') or '').strip() == orcid]
        if len(matched) == 1:
            return matched[0], exact
    if len(exact) == 1:
        return exact[0], exact
    return None, exact


def classify(row):
    ''' Work out what, if anything, can be done about one mismatched name
        Keyword arguments:
          row: a name_mismatches row restricted to figshare DOIs
        Returns:
          dict of findings for this name
    '''
    out = {'name': row['name'], 'roster': row['roster'], 'kind': row['kind'],
           'score': row['score'], 'alumni': row['alumni'], 'items': [],
           'skipped': []}
    # A base DOI and its version DOIs name one article, and the collection
    # holds both - 10.25378/janelia.23411009 alongside ...23411009.v2. Acting
    # on each in turn would read, swap and publish the same item twice, and
    # since publishing is what mints a version, the second pass would create
    # one for no reason. One article, one correction.
    byarticle = collections.OrderedDict()
    for doi in sorted(row['dois']):
        aid = article_id(doi)
        if not aid:
            out['skipped'].append((doi, 'no article ID in the DOI'))
            continue
        byarticle.setdefault(aid, []).append(doi)
    for aid, dois in byarticle.items():
        doi = dois[0]
        try:
            # The whole item rather than its authors sub-resource: this one call
            # carries both the author list and account_id, the owner whose
            # identity the correction has to be made under.
            item = call_figshare('GET', f"/account/articles/{aid}") or {}
        except Exception as err:
            out['skipped'].append((doi, f"could not read the item: {err}"))
            continue
        authors = item.get('authors') or []
        owner = item.get('account_id')
        if not owner:
            out['skipped'].append((doi, 'figshare did not say who owns the item'))
            continue
        idx, author = find_author(authors, row['name'])
        if author is None:
            # The name is not on the current version. Our DOI is a version DOI
            # whose frozen metadata still carries it, and old versions cannot
            # be modified from the website, the API or anywhere else.
            out['skipped'].append((doi, 'already corrected in a later version'))
            continue
        if not is_ghost(author):
            out['skipped'].append(
                (doi, f"registered figshare account (id {author['id']}, "
                      f"{author.get('url_name')}) - the name belongs to that "
                      "person's profile, not to this item"))
            continue
        out['items'].append({'doi': doi, 'dois': dois, 'article': aid,
                             'authors': authors, 'index': idx, 'author': author,
                             'owner': owner})
    return out


def report(findings):
    ''' Print what was found
        Keyword arguments:
          findings: list of classify() results
        Returns:
          None
    '''
    print(f"\n{'Name on figshare':<24} {'Roster name':<24} {'Fixable':>7} {'Skipped':>7}")
    print('-' * 66)
    for find in findings:
        print(f"{find['name'][:23]:<24} {find['roster'][:23]:<24} "
              f"{len(find['items']):>7} {len(find['skipped']):>7}")
    print()
    for find in findings:
        if not find['skipped']:
            continue
        print(f"{find['name']}:")
        for doi, why in find['skipped']:
            print(f"    {doi:<34} {why}")


def confirm(findings):
    ''' Walk the proposals and collect the ones to apply
        Keyword arguments:
          findings: list of classify() results
        Returns:
          List of (finding, replacement author) pairs
    '''
    accepted = []
    for find in findings:
        if not find['items']:
            continue
        orcid = roster_orcid(find['roster'])
        chosen, candidates = replacement_for(find['roster'], orcid)
        print(f"\n{'=' * 68}")
        print(f"  on figshare : {find['name']}")
        print(f"  roster says : {find['roster']}"
              f"{'  (alumni)' if find['alumni'] else ''}")
        print(f"  similarity  : {find['score']:.1f}  ({find['kind']})")
        print(f"  ORCID held  : {orcid or 'none'}")
        print(f"  items       : {len(find['items'])}")
        for item in find['items']:
            extra = f"  (+{len(item['dois']) - 1} version DOI)" if len(item['dois']) > 1 else ''
            print(f"      article {item['article']:<10} {item['doi']:<32}"
                  f" author id {item['author']['id']}  owner {item['owner']}{extra}")
        if not candidates:
            print("  No figshare author record carries the roster spelling. Skipped:"
                  "\n    sending a bare name would create another unregistered record"
                  "\n    rather than correcting this one.")
            COUNT['no_candidate'] += 1
            continue
        if not chosen:
            print("  Several figshare records carry that name and nothing"
                  " distinguishes them:")
            for cand in candidates:
                print(f"      id {cand['id']:<10} orcid "
                      f"{cand.get('orcid_id') or '-':<22} {cand.get('url_name')}")
            if not sys.stdin.isatty():
                COUNT['ambiguous'] += 1
                continue
            reply = input("  Enter the author id to use, or blank to skip: ").strip()
            chosen = next((c for c in candidates if str(c['id']) == reply), None)
            if not chosen:
                print("  Skipped.")
                COUNT['ambiguous'] += 1
                continue
        print(f"  replacement : id {chosen['id']}  {chosen['full_name']}"
              f"  orcid {chosen.get('orcid_id') or '-'}")
        if not ARG.WRITE:
            continue
        if not sys.stdin.isatty():
            terminate_program("--write needs a terminal to confirm each change")
        reply = input(f"  Apply to {len(find['items'])} item(s)? "
                      "Each becomes a NEW VERSION with a new .vN DOI. [y/N] ").strip()
        if reply.lower() in ('y', 'yes'):
            accepted.append((find, chosen))
        else:
            print("  Skipped.")
    return accepted


def apply_changes(accepted):
    ''' Swap the author on each accepted item and publish it
        Keyword arguments:
          accepted: list of (finding, replacement author)
        Returns:
          None
    '''
    for find, chosen in accepted:
        for item in find['items']:
            # The whole list goes back, with one element replaced in place.
            # figshare's PUT removes every author already associated, and the
            # array is positional - there is no order field - so rebuilding it
            # from anything less than the full read would drop authors and
            # scramble the rest.
            ids = [{"id": a['id']} for a in item['authors']]
            ids[item['index']] = {"id": chosen['id']}
            # Reads succeed against any item in the institution, writes do not:
            # without impersonation the PUT is refused outright. The owner's
            # account id goes in the body for PUT and POST, which is where
            # figshare documents it for those verbs.
            try:
                call_figshare('PUT', f"/account/articles/{item['article']}/authors",
                              {"authors": ids, "impersonate": item['owner']})
                call_figshare('POST', f"/account/articles/{item['article']}/publish",
                              {"impersonate": item['owner']})
            except Exception as err:
                LOGGER.error(f"{item['doi']}: {err}")
                COUNT['failed'] += 1
                continue
            LOGGER.info(f"{item['doi']}: {find['name']} -> {chosen['full_name']}")
            COUNT['written'] += 1


def process_names():
    ''' Find mismatched names on figshare DOIs and act on them
        Keyword arguments:
          None
        Returns:
          None
    '''
    try:
        rows = DL.name_mismatches(DB['dis'].dois, DB['dis'].orcid, cutoff=ARG.CUTOFF)
    except Exception as err:
        terminate_program(err)
    COUNT['mismatches'] = len(rows)
    findings = []
    for row in rows:
        dois = [d for d in row['dois'] if str(d).startswith(FIGSHARE_PREFIXES)]
        if not dois:
            continue
        COUNT['figshare_names'] += 1
        findings.append(classify({**row, 'dois': dois}))
    for find in findings:
        COUNT['fixable_items'] += len(find['items'])
        COUNT['skipped_items'] += len(find['skipped'])
    report(findings)
    accepted = confirm(findings)
    if accepted and ARG.WRITE:
        apply_changes(accepted)
    print(f"\nMismatched names overall:  {COUNT['mismatches']:,}")
    print(f"On figshare DOIs:          {COUNT['figshare_names']:,}")
    print(f"Items that can be fixed:   {COUNT['fixable_items']:,}")
    print(f"Items left alone:          {COUNT['skipped_items']:,}")
    if COUNT['no_candidate']:
        print(f"No correctly-named record: {COUNT['no_candidate']:,}")
    if COUNT['ambiguous']:
        print(f"Ambiguous replacement:     {COUNT['ambiguous']:,}")
    if ARG.WRITE:
        print(f"Items updated:             {COUNT['written']:,}")
        if COUNT['failed']:
            print(f"Items that failed:         {COUNT['failed']:,}")
    else:
        LOGGER.warning("Dry run, figshare was not modified")


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(description="Correct misspelled author names on figshare")
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, prod)')
    PARSER.add_argument('--cutoff', dest='CUTOFF', action='store', type=int, default=95,
                        help='Minimum similarity for a spelling candidate [95]')
    PARSER.add_argument('--token', dest='TOKEN', action='store',
                        help='figshare token [$FIGSHARE_JWT]')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Write to figshare')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    if not ARG.TOKEN:
        import os
        ARG.TOKEN = os.environ.get('FIGSHARE_JWT')
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    process_names()
    terminate_program()
