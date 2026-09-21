''' janelia_rrids.py
    List the RRIDs (rrids.org) associated with Janelia, from the SciCrunch registry.

    Companion to harvest_rrids.py, which answers the other half of the question:
    that one finds RRIDs *cited by* Janelia papers, this one finds resources the
    registry itself ties to Janelia.

    SciCrunch splits the registry across per-kind Elasticsearch indices
    (RIN_Tool_pr for software/facilities, RIN_Organism_pr for stock-centre
    organisms, RIN_Antibody_pr, and so on). Each is searched for "Janelia
    Research Campus" and every hit is classified by WHERE the name matched,
    because the three big pools mean very different things:

      donor      a stock centre record naming Janelia as the donor of the line -
                 a Janelia-created resource held elsewhere (the BDSC fly lines)
      name       Janelia is in the resource's own name or synonyms - Janelia's
                 own software, facilities and databases
      dye        a commercial product built on Janelia Fluor(R), the dye chemistry
                 invented at Janelia and licensed to vendors. Associated with
                 Janelia, but not a Janelia-registered resource
      described  Janelia appears in the description only
      incidental matched only through a relationship to another record

    --category keeps only the named categories (comma-delimited, case-insensitive);
    omit it for all of them. The filter runs after classification, so it narrows
    the output, not the time the run takes.

    Requires SCICRUNCH_API_KEY. Read-only; writes TSV and JSON, never MongoDB.
'''

__version__ = '1.1.0'

import argparse
import collections
import os
import re
import sys
import time
import requests

# pylint: disable=broad-exception-caught

ARG = None
BASE = "https://api.scicrunch.io/elastic"
# The resource indices; RIN_Mentions_pr is deliberately absent - it holds paper
# mentions of resources, not resources, so it would double-count.
INDICES = ("RIN_Tool_pr", "RIN_Organism_pr", "RIN_Antibody_pr", "RIN_CellLine_pr",
           "RIN_Plasmid_pr", "RIN_Addgene_pr", "RIN_BioSample_pr",
           "RIN_DGRC_Clones_pr", "RIN_DGRC_Vectors_pr", "RIN_Protocols_pr")
SOURCE_FIELDS = ["rrid.curie", "item.name", "item.synonyms", "item.description",
                 "item.notes", "item.types", "item.identifier", "graph.parent",
                 "organization"]
# Everything classify() can return; --category is validated against this, so a
# typo fails at startup rather than silently producing an empty file.
CATEGORIES = ("donor", "dye", "name", "described", "incidental")
SESSION = requests.Session()


def _tsv(val):
    ''' Flatten a value for one TSV cell - tabs and newlines would break the row.
        Keyword arguments:
          val: any value
        Returns:
          Single-line string
    '''
    return str(val).replace('\t', ' ').replace('\n', ' ')


def terminate_program(msg=None):
    ''' Terminate the program gracefully
        Keyword arguments:
          msg: error message
        Returns:
          None
    '''
    if msg:
        print(msg, file=sys.stderr)
    sys.exit(-1 if msg else 0)


def search(index, body):
    ''' One _search call, with a bounded retry.
        Keyword arguments:
          index: index name
          body: Elasticsearch query body
        Returns:
          Parsed response, or None
    '''
    for attempt in range(4):
        try:
            resp = SESSION.post(f"{BASE}/{index}/_search", json=body, timeout=90)
            if resp.status_code == 200:
                return resp.json()
            # The gateway 403s/500s in waves when SciCrunch is under load; those
            # are transient and clear on a retry rather than meaning a bad key.
            if resp.status_code in (403, 429, 500, 502, 503):
                time.sleep(3 * (attempt + 1))
                continue
            return None
        except requests.RequestException:
            time.sleep(3 * (attempt + 1))
    return None


def classify(src):
    ''' Where in the record does Janelia appear?
        Keyword arguments:
          src: the _source dict
        Returns:
          (category, evidence string)
    '''
    item = src.get('item') or {}
    name = str(item.get('name') or '')
    syns = ' '.join(str(s.get('name') or '') for s in (item.get('synonyms') or []))
    notes = ' '.join(str(n.get('description') or '') for n in (item.get('notes') or []))
    desc = str(item.get('description') or '')
    if re.search(r'janelia\s+fluor', name + ' ' + syns + ' ' + desc, re.I):
        return 'dye', name[:120]
    if re.search(r'janelia', name + ' ' + syns, re.I):
        return 'name', name[:120]
    mat = re.search(r'Donor[^;]*Janelia[^;]*', notes, re.I)
    if mat:
        return 'donor', mat.group(0)[:120]
    if re.search(r'janelia', desc, re.I):
        return 'described', desc[:120]
    return 'incidental', name[:120]


def harvest(index, wanted):
    ''' Page an index with search_after, which is stateless and has no 10k cap.
        A category filter cannot be pushed into the query: the category is decided
        by classify() from the record's own fields, not by anything the index
        stores, so every hit is still fetched and scanned and only the output
        shrinks. --category narrows the answer, not the runtime.
        Keyword arguments:
          index: index name
          wanted: set of categories to keep, or None for all
        Returns:
          List of (rrid, name, category, evidence)
    '''
    rows = []
    scanned = 0
    after = None
    while True:
        body = {"size": ARG.PAGE, "sort": [{"_id": "asc"}],
                "_source": SOURCE_FIELDS,
                "query": {"query_string": {"query": f'"{ARG.TERM}"'}}}
        if after:
            body['search_after'] = after
        res = search(index, body)
        if not res or not res.get('hits', {}).get('hits'):
            break
        for hit in res['hits']['hits']:
            src = hit['_source']
            rrid = (src.get('rrid') or {}).get('curie') or hit['_id']
            cat, why = classify(src)
            scanned += 1
            if wanted is None or cat in wanted:
                rows.append((rrid, str((src.get('item') or {}).get('name') or '')[:150],
                             cat, why))
            after = hit.get('sort')
        if ARG.VERBOSE:
            print(f"  {index}: kept {len(rows):,} of {scanned:,} scanned", file=sys.stderr)
        if ARG.LIMIT and len(rows) >= ARG.LIMIT:
            break
    return rows


def processing():
    ''' Main routine
        Keyword arguments:
          None
        Returns:
          None
    '''
    key = os.environ.get('SCICRUNCH_API_KEY')
    if not key:
        terminate_program("SCICRUNCH_API_KEY is not set")
    SESSION.headers['apikey'] = key
    wanted = None
    if ARG.CATEGORY:
        wanted = {cat.strip().lower() for cat in ARG.CATEGORY.split(',') if cat.strip()}
        bad = wanted - set(CATEGORIES)
        if bad:
            terminate_program(f"Unknown category: {', '.join(sorted(bad))}. "
                              f"Choose from {', '.join(CATEGORIES)}")
        print(f"Keeping categories: {', '.join(sorted(wanted))}", file=sys.stderr)
    allrows = {}
    per_index = collections.Counter()
    for index in INDICES:
        rows = harvest(index, wanted)
        per_index[index] = len(rows)
        print(f"{index:<22} {len(rows):>8,}", file=sys.stderr)
        for rrid, name, cat, why in rows:
            allrows[rrid] = (name, cat, why, index)
    cats = collections.Counter(v[1] for v in allrows.values())
    with open(ARG.OUTPUT, 'w', encoding='utf-8') as handle:
        handle.write("rrid\tcategory\tindex\tname\tevidence\n")
        for rrid in sorted(allrows, key=lambda r: (allrows[r][1], r)):
            name, cat, why, index = allrows[rrid]
            handle.write(f"{rrid}\t{cat}\t{index}\t{_tsv(name)}\t{_tsv(why)}\n")
    print(f"\nDistinct RRIDs: {len(allrows):,}", file=sys.stderr)
    for cat, cnt in cats.most_common():
        print(f"  {cat + ':':<12} {cnt:>8,}", file=sys.stderr)
    print(f"\nWrote {ARG.OUTPUT}", file=sys.stderr)


# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="List Janelia-associated RRIDs from the SciCrunch registry")
    PARSER.add_argument('--term', dest='TERM', action='store',
                        default='Janelia Research Campus',
                        help='Phrase to search for')
    PARSER.add_argument('--category', dest='CATEGORY', action='store', default='',
                        help='Comma-delimited categories to keep, case-insensitive '
                             f"({', '.join(CATEGORIES)}); default is all")
    PARSER.add_argument('--output', dest='OUTPUT', action='store',
                        default='janelia_rrids.tsv', help='Output TSV')
    PARSER.add_argument('--page', dest='PAGE', action='store', type=int,
                        default=1000, help='Page size')
    PARSER.add_argument('--limit', dest='LIMIT', action='store', type=int,
                        default=0, help='Stop after N rows per index (testing)')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    ARG = PARSER.parse_args()
    processing()
