''' harvest_rrids.py
    Survey the RRIDs (Research Resource Identifiers, rrids.org) cited in Janelia
    papers.

    Seeded from the dois collection's jrc_pmid rather than a Europe PMC
    affiliation search: AFF:"Janelia Research Campus" returns far fewer papers
    than we hold, because Europe PMC only indexes affiliation for some records.
    Each PMID is resolved to a PMCID, and every open-access full text is scanned
    for RRID tokens.

    Two passes, so an interrupted run is cheap to resume:
      1. PMID -> PMCID/open-access, batched 40 per query. Cached in the file named
         by --cache; delete it to force a re-resolve.
      2. Full-text fetch and RRID extraction, threaded.

    Read-only: it writes nothing to MongoDB, so there is no --write flag. Output
    is a TSV of RRID/paper-count/PMIDs plus the per-paper JSON.

    NOTE ON SCOPE: this finds RRIDs *cited by* Janelia papers. Resources
    *registered by* Janelia are a different question and need a registry-wide
    SciCrunch query - their Elastic API needs a (free) key, and the HTML search
    sits behind Cloudflare.
'''

__version__ = '1.0.0'

import argparse
import collections
import html
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
import requests
import jrc_common.jrc_common as JRC

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

ARG = LOGGER = None
DB = {}
COUNT = collections.defaultdict(lambda: 0, {})
EPMC = "https://www.ebi.ac.uk/europepmc/webservices/rest"
SESSION = requests.Session()
TAG = re.compile(r'<[^>]+>')
# The token after "RRID:" varies by authority: AB_123, SCR_123, CVCL_0063,
# IMSR_JAX:000664, Addgene_12345.
TOKEN = re.compile(r'RRID[:\s]+([A-Za-z][A-Za-z0-9]*(?:_[A-Za-z0-9]+)?'
                   r'(?::[A-Za-z0-9_\-.]+)?)')
# Bare authority tokens, for the papers that put "RRID:" in a neighbouring table
# cell or only give the registry URL.
BARE = re.compile(r'\b((?:SCR|AB|CVCL|Addgene|BDSC|MMRRC|RGD|ZFIN|ZIRC|NXR|DGRC|'
                  r'IMSR_JAX|IMSR_TAC|IMSR_EM|MGI|SAMN)_[A-Za-z0-9\-.]*\d[A-Za-z0-9\-.]*)')


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
    ''' Initialize program
        Keyword arguments:
          None
        Returns:
          None
    '''
    try:
        dbconfig = JRC.get_config("databases")
    except Exception as err:
        terminate_program(err)
    dbo = getattr(getattr(dbconfig.dis, ARG.MANIFOLD), 'read')
    LOGGER.info(f"Connecting to {dbo.name} {ARG.MANIFOLD} on {dbo.host} as {dbo.user}")
    try:
        DB['dis'] = JRC.connect_database(dbo)
    except Exception as err:
        terminate_program(err)
    SESSION.headers['User-Agent'] = f"Janelia-DIS-RRID-survey/{__version__}"


def get(url, **kwargs):
    ''' GET with a bounded retry on the transient statuses Europe PMC returns
        under load. Returns None rather than raising so one bad article cannot
        end a run of thousands.
        Keyword arguments:
          url: URL to fetch
          kwargs: passed through to requests.get
        Returns:
          Response, or None
    '''
    for attempt in range(4):
        try:
            resp = SESSION.get(url, timeout=60, **kwargs)
            if resp.status_code == 200:
                return resp
            if resp.status_code in (429, 500, 502, 503):
                time.sleep(2 * (attempt + 1))
                continue
            return None
        except requests.RequestException:
            time.sleep(2 * (attempt + 1))
    return None


def rrids_from_xml(xml):
    ''' Pull RRIDs out of a JATS full text.
        Publishers wrap the identifier in an <ext-link>, so "RRID:" and its token
        are separated by markup, not whitespace - a naive RRID:\\s*(\\w+) match
        finds nothing. Tags are replaced by a space before matching.
        Keyword arguments:
          xml: JATS full-text XML
        Returns:
          Set of RRID token strings
    '''
    text = re.sub(r'\s+', ' ', html.unescape(TAG.sub(' ', xml)))
    found = set()
    for mat in TOKEN.finditer(text):
        tok = mat.group(1).rstrip('.,;')
        if re.search(r'\d', tok):
            found.add(tok)
    found.update(mat.group(1).rstrip('.,;') for mat in BARE.finditer(text))
    return found


def seed_pmids():
    ''' Every PMID we hold for a Janelia DOI.
        Keyword arguments:
          None
        Returns:
          Sorted list of PMID strings
    '''
    try:
        rows = DB['dis'].dois.find({"jrc_pmid": {"$exists": True}}, {"jrc_pmid": 1})
    except Exception as err:
        terminate_program(err)
    pmids = sorted({str(row['jrc_pmid']) for row in rows})
    LOGGER.info(f"Seed PMIDs from the dois collection: {len(pmids):,}")
    return pmids


def resolve_pmcids(pmids):
    ''' Map PMIDs to PMCIDs, reusing the cache when one is present.
        Keyword arguments:
          pmids: list of PMID strings
        Returns:
          dict of PMID -> {pmcid, oa, title, doi}
    '''
    if os.path.exists(ARG.CACHE):
        with open(ARG.CACHE, encoding='utf-8') as handle:
            meta = json.load(handle)
        LOGGER.info(f"Reusing {ARG.CACHE}: {len(meta):,} resolved PMIDs "
                    "(delete it to force a re-resolve)")
        return meta
    meta = {}
    for idx in range(0, len(pmids), 40):
        chunk = pmids[idx:idx + 40]
        query = " OR ".join(f"EXT_ID:{pmid}" for pmid in chunk)
        resp = get(f"{EPMC}/search", params={'query': query, 'format': 'json',
                                             'pageSize': 40, 'resultType': 'lite'})
        if not resp:
            COUNT['resolve_errors'] += 1
            continue
        for res in resp.json().get('resultList', {}).get('result', []):
            if res.get('pmid'):
                meta[res['pmid']] = {'pmcid': res.get('pmcid'),
                                     'oa': res.get('isOpenAccess') == 'Y',
                                     'title': (res.get('title') or '')[:140],
                                     'doi': res.get('doi')}
        if ARG.VERBOSE and not idx % 400:
            LOGGER.info(f"  resolved {idx + len(chunk):,}/{len(pmids):,}")
    with open(ARG.CACHE, 'w', encoding='utf-8') as handle:
        json.dump(meta, handle)
    LOGGER.info(f"Wrote {ARG.CACHE}")
    return meta


def processing():  # pylint: disable=too-many-locals
    ''' Main routine
        Keyword arguments:
          None
        Returns:
          None
    '''
    pmids = seed_pmids()
    meta = resolve_pmcids(pmids)
    oal = {pmid: rec for pmid, rec in meta.items() if rec.get('pmcid') and rec.get('oa')}
    LOGGER.info(f"Matched in Europe PMC: {len(meta):,}; with open-access full text: {len(oal):,}")
    if ARG.LIMIT:
        oal = dict(list(oal.items())[:ARG.LIMIT])
        LOGGER.info(f"--limit in force: scanning {len(oal):,}")
    results = {}
    done = [0]

    def work(item):
        pmid, rec = item
        resp = get(f"{EPMC}/{rec['pmcid']}/fullTextXML")
        done[0] += 1
        if ARG.VERBOSE and not done[0] % 200:
            LOGGER.info(f"  fetched {done[0]:,}/{len(oal):,}")
        if not resp:
            COUNT['fulltext_errors'] += 1
            return pmid, None
        return pmid, sorted(rrids_from_xml(resp.text))

    with ThreadPoolExecutor(max_workers=ARG.WORKERS) as pool:
        for pmid, found in pool.map(work, oal.items()):
            if found:
                results[pmid] = found
    counts = collections.Counter()
    papers = collections.defaultdict(set)
    for pmid, lst in results.items():
        for rrid in lst:
            counts[rrid] += 1
            papers[rrid].add(pmid)
    with open('rrid_by_paper.json', 'w', encoding='utf-8') as handle:
        json.dump({pmid: {'rrids': lst, 'doi': meta[pmid].get('doi'),
                          'title': meta[pmid].get('title')}
                   for pmid, lst in results.items()}, handle, indent=1)
    with open('rrids_janelia.tsv', 'w', encoding='utf-8') as handle:
        handle.write("rrid\tpapers\tpmids\n")
        for rrid, cnt in counts.most_common():
            handle.write(f"{rrid}\t{cnt}\t{','.join(sorted(papers[rrid]))}\n")
    COUNT['papers_scanned'] = len(oal)
    COUNT['papers_citing_rrids'] = sum(1 for v in results.values() if v)
    COUNT['distinct_rrids'] = len(counts)
    print()
    for key in sorted(COUNT):
        print(f"{key + ':':<26} {COUNT[key]:,}")
    authority = collections.Counter(rrid.split('_')[0] for rrid in counts)
    print("\nDistinct RRIDs by authority:")
    for auth, cnt in authority.most_common():
        print(f"  {auth + ':':<14} {cnt:,}")
    print("\nWrote rrids_janelia.tsv and rrid_by_paper.json")


# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Survey the RRIDs cited in Janelia papers")
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, prod)')
    PARSER.add_argument('--cache', dest='CACHE', action='store',
                        default='rrid_pmc_cache.json',
                        help='PMID->PMCID cache file (reused if present)')
    PARSER.add_argument('--workers', dest='WORKERS', action='store', type=int,
                        default=6, help='Concurrent full-text fetches')
    PARSER.add_argument('--limit', dest='LIMIT', action='store', type=int,
                        default=0, help='Scan only the first N papers (testing)')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    LOGGER.info(f"Started run (version {__version__})")
    initialize_program()
    processing()
    terminate_program()
