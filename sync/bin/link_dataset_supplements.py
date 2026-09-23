''' link_dataset_supplements.py
    Record the dataset/article supplement relationships already declared in DOI
    metadata, as jrc_dataset_supplement on the dois collection.

    Answers "where is the data for this paper?" and its inverse. The two
    registrars declare the relationship in opposite directions and in different
    shapes:

      DataCite  relatedIdentifiers[] with relationType "IsSupplementTo",
                pointing dataset -> article
      Crossref  relation["is-supplemented-by"], an object keyed by relation type
                in kebab-case, pointing article -> dataset

    Both ends of every pair are written, so a record carries the link whichever
    side declared it. No DOI currently appears on both sides, so the graph is
    built in one pass with no reconciliation.

    Reads only fields already stored on our own records - this makes no network
    calls. It is therefore downstream of update_dois.py, which refreshes the
    registrar payload, but independent of update_preprints.py: that program reads
    the same two raw fields for a different relation type (IsPreprintOf /
    has-preprint) and writes a different field, so neither needs the other.

    Preprint relationships are deliberately out of scope - jrc_preprint owns
    those, and duplicating them here would let the two diverge. Versioning
    relations (IsIdenticalTo, IsPartOf) are figshare bookkeeping with no reader
    value.

    A DOI stores what its own metadata declares, so a versioned figshare record
    and its base DOI both carry the link they each assert - the run summary
    reports distinct relationships separately from records written, because most
    figshare links are restated on every version.
'''

__version__ = '1.3.0'

import argparse
import collections
import html
from operator import attrgetter
import os
import re
import sys
from datetime import datetime
import jrc_common.jrc_common as JRC
import jrc_email.jrc_email as JE
import doi_common.doi_common as DL

# pylint: disable=broad-exception-caught,logging-fstring-interpolation,logging-not-lazy

# Database
DB = {}
# Global variables
ARG = DIS = LOGGER = None
# Counters
COUNT = collections.defaultdict(lambda: 0, {})
# DOI prefixes that identify a data repository. A dataset -> dataset link is
# figshare collection bookkeeping, not a supplement relationship, so the target
# side is filtered on this.
DATA_PREFIXES = ('10.25378',      # Janelia figshare
                 '10.6084',       # figshare
                 '10.5281',       # Zenodo
                 '10.5061',       # Dryad
                 '10.17632',      # Mendeley Data
                 '10.7910',       # Harvard Dataverse
                 '10.48324')      # BossDB
DATA_RE = re.compile(r'^(?:' + '|'.join(re.escape(p) for p in DATA_PREFIXES) + r')/', re.I)
VERSION_RE = re.compile(r'\.v\d+$', re.I)


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


def is_dataset(doi):
    ''' Is this DOI from a data repository?
        Keyword arguments:
          doi: DOI string
        Returns:
          True if the prefix belongs to a known data repository
    '''
    return bool(DATA_RE.match(doi or ''))


def collect():
    ''' Build the dataset/article graph from stored registrar metadata.
        Keyword arguments:
          None
        Returns:
          dict of doi -> list of entry dicts
    '''
    graph = collections.defaultdict(dict)

    def add(source_doi, target_doi, relation, registrar):
        ''' Record one direction of a pair, keyed on target so a link restated
            by several relation entries is stored once. '''
        graph[source_doi][target_doi] = {"doi": target_doi, "relation": relation,
                                         "source": registrar}
    payload = {"$or": [{"relatedIdentifiers.relationType": "IsSupplementTo"},
                       {"relation.is-supplemented-by": {"$exists": True}}]}
    try:
        rows = DB['dis'].dois.find(payload, {"doi": 1, "relatedIdentifiers": 1, "relation": 1})
    except Exception as err:
        terminate_program(err)
    for row in rows:
        doi = str(row.get('doi') or '').lower().strip()
        if not doi:
            COUNT['no_doi'] += 1
            continue
        # DataCite: this record supplements the target.
        for rel in row.get('relatedIdentifiers') or []:
            if rel.get('relationType') != 'IsSupplementTo' \
               or rel.get('relatedIdentifierType') != 'DOI':
                continue
            tgt = str(rel.get('relatedIdentifier') or '').lower().strip()
            if not tgt or tgt == doi:
                continue
            if is_dataset(tgt):
                # dataset -> dataset is collection membership, not a supplement
                COUNT['skipped_dataset_to_dataset'] += 1
                continue
            add(doi, tgt, 'supplements', 'DataCite')
            add(tgt, doi, 'supplemented_by', 'DataCite')
            COUNT['datacite_links'] += 1
        # Crossref: the target supplements this record.
        for val in ((row.get('relation') or {}).get('is-supplemented-by') or []):
            if val.get('id-type') != 'doi':
                # accession numbers (PDB, EMDB) and bare URIs are not joinable
                COUNT['skipped_non_doi'] += 1
                continue
            tgt = str(val.get('id') or '').lower().strip()
            if not tgt or tgt == doi:
                continue
            if not is_dataset(tgt):
                COUNT['skipped_not_a_dataset'] += 1
                continue
            add(doi, tgt, 'supplemented_by', 'Crossref')
            add(tgt, doi, 'supplements', 'Crossref')
            COUNT['crossref_links'] += 1
    return graph


def collapse(graph, held):
    ''' Collapse versioned targets to their base DOI.
        A figshare deposit restates the same supplement link on every version, so
        an article ends up pointing at both janelia.12106749 and .v2 - 24 entries
        where 12 deposits exist. The base DOI is substituted when we hold it; when
        we do not, the versioned DOI is kept rather than replaced by something
        that resolves to nothing here.
        Keyword arguments:
          graph: dict of doi -> {target: entry}
          held: set of DOIs we hold
        Returns:
          dict of doi -> sorted entry list
    '''
    out = {}
    for doi, entries in graph.items():
        merged = {}
        for tgt, ent in entries.items():
            base = stem(tgt)
            key = base if (base != tgt and base in held) else tgt
            if key != tgt:
                COUNT['versions_collapsed'] += 1
            merged.setdefault(key, dict(ent, doi=key))
        out[doi] = sorted(merged.values(), key=lambda e: e['doi'])
    return out


def stem(doi):
    ''' A figshare DOI without its version suffix, for counting distinct
        relationships: janelia.12106749 and janelia.12106749.v2 assert the same
        link and would otherwise be counted twice.
        Keyword arguments:
          doi: DOI string
        Returns:
          DOI with any trailing .vN removed
    '''
    return VERSION_RE.sub('', doi or '')


def unchanged(doi, entries):
    ''' Does the record already hold exactly these links?
        These relations derive from metadata that rarely changes, so on a
        scheduled re-run almost every record is identical. Rewriting anyway would
        bump the timestamp and push a duplicate processing event every night -
        hundreds of events saying nothing happened. The stored timestamp is
        excluded from the comparison; only the links decide whether it changed.
        Keyword arguments:
          doi: DOI
          entries: the entry list this run computed
        Returns:
          True when the stored value already matches
    '''
    try:
        rec = DB['dis'].dois.find_one({"doi": doi}, {"jrc_dataset_supplement": 1})
    except Exception as err:
        terminate_program(err)
    stored = (rec or {}).get('jrc_dataset_supplement')
    if not stored:
        return False
    shape = [(e.get('doi'), e.get('relation'), e.get('source')) for e in entries]
    return [(e.get('doi'), e.get('relation'), e.get('source')) for e in stored] == shape


def persist(graph, held):
    ''' Store the field, or print what would be stored.
        Keyword arguments:
          graph: dict of doi -> entry list
          held: set of DOIs we actually hold
        Returns:
          None
    '''
    for doi in sorted(graph):
        if doi not in held:
            continue
        if unchanged(doi, graph[doi]):
            COUNT['unchanged'] += 1
            continue
        COUNT['changed'] += 1
        if not ARG.WRITE:
            for ent in graph[doi]:
                mark = '' if ent['doi'] in held else '   (target not held)'
                print(f"  {doi:42} {ent['relation']:16} {ent['doi']}{mark}")
            continue
        try:
            DB['dis'].dois.update_one({"doi": doi},
                                      {"$set": {"jrc_dataset_supplement": graph[doi],
                                                "jrc_dataset_supplement_updated":
                                                    datetime.now()}})
            COUNT['written'] += 1
        except Exception as err:
            terminate_program(err)
        log_process(doi, graph[doi])
    if not ARG.WRITE:
        print(f"\nDry run: nothing written. Re-run with --write to store "
              f"{COUNT['changed']:,} changed record(s) "
              f"({COUNT['unchanged']:,} already current).", file=sys.stderr)


def log_process(doi, entries):
    ''' Record a processing event for a DOI whose links were stored.
        Only called under --write: add_doi_process has no write gate of its own,
        so a dry run has to be stopped before the call rather than inside it.
        Keyword arguments:
          doi: DOI
          entries: the jrc_dataset_supplement list stored
        Returns:
          None
    '''
    counts = collections.Counter(e['relation'] for e in entries)
    notes = "; ".join(f"{rel.replace('_', ' ')}: {num}" for rel, num in sorted(counts.items()))
    try:
        DL.add_doi_process(doi, action='link_dataset_supplement',
                           coll=DB['dis'].processing, notes=notes)
    except Exception as err:
        LOGGER.error(f"Could not log a processing event for {doi}: {err}")
        COUNT['processing_error'] += 1
        return
    COUNT['processing_logged'] += 1


def html_table(graph, held):
    ''' Zebra-striped sample table for the run-summary email.
        No per-cell border-radius - plain background striping is what survives
        Outlook's Word rendering engine.
        Keyword arguments:
          graph: dict of doi -> entry list
          held: set of DOIs we hold
        Returns:
          HTML table
    '''
    rows = []
    shown = [d for d in sorted(graph) if d in held][:25]
    for idx, doi in enumerate(shown):
        striped = idx % 2 == 0
        bgattr = f' bgcolor="{JE.STRIPE_BG}"' if striped else ''
        bgs = f'background-color:{JE.STRIPE_BG};' if striped else ''
        ent = graph[doi]
        # Green for "this record supplements something", gray for the inverse -
        # JE has no blue background constant, only a blue foreground.
        tone = (JE.GREEN_BG, JE.GREEN) if ent[0]['relation'] == 'supplements' \
               else (JE.GRAY_BG, JE.NAVY)
        rows.append(
            f'<tr{bgattr} style="{bgs}">'
            f'<td style="padding:8px 10px;">{html.escape(doi)}</td>'
            f'<td style="padding:8px 10px;">{JE.pill(tone[0], tone[1], ent[0]["relation"])}</td>'
            f'<td style="padding:8px 10px;color:{JE.GRAY};">'
            f'{html.escape(", ".join(e["doi"] for e in ent)[:90])}</td></tr>')
    rows.append('<tr><td colspan="3" style="height:1px;line-height:1px;font-size:1px;">'
                '&nbsp;</td></tr>')
    return ('<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
            'style="border-collapse:collapse;font-size:12.5px;">' + "".join(rows) + '</table>')


def generate_email(graph, held, relationships):
    ''' Send the HTML run-summary email.
        Keyword arguments:
          graph: dict of doi -> entry list
          held: set of DOIs we hold
          relationships: count of distinct version-collapsed relationships
        Returns:
          None
    '''
    run_data = (JRC.get_run_data(__file__, __version__).strip()
                + f" &middot; manifold: {ARG.MANIFOLD}")
    mode_label = 'WRITE' if ARG.WRITE else 'DRY RUN'
    mode_tone = 'good' if ARG.WRITE else 'warn'
    kpis = ''.join([
        JE.kpi_card(f"{relationships:,}", "Distinct relationships", width='25%'),
        JE.kpi_card(f"{COUNT['records']:,}", "Records with the field", 'good', width='25%'),
        JE.kpi_card(f"{COUNT['datacite_links']:,}", "From DataCite", width='25%'),
        JE.kpi_card(f"{COUNT['crossref_links']:,}", "From Crossref", width='25%'),
    ])
    body = JE.body_row(JE.section_header("&#128202; Dataset Supplements")
                       + html_table(graph, held))
    msg = JE.render(os.path.basename(__file__), __version__, run_data,
                    mode_label, mode_tone, kpis, body)
    email = DIS['developer'] if ARG.TEST else DIS['receivers']
    try:
        LOGGER.info(f"Sending email to {email}")
        JRC.send_email(msg, DIS['sender'], email, 'Dataset supplements linked', mime='html')
    except Exception as err:
        LOGGER.error(err)


def processing():
    ''' Main routine
        Keyword arguments:
          None
        Returns:
          None
    '''
    graph = collect()
    try:
        held = {str(d['doi']).lower() for d in DB['dis'].dois.find({}, {"doi": 1})
                if d.get('doi')}
    except Exception as err:
        terminate_program(err)
    graph = collapse(graph, held)
    ours = {doi: ents for doi, ents in graph.items() if doi in held}
    COUNT['records'] = len(ours)
    COUNT['targets_not_held'] = len({e['doi'] for ents in ours.values() for e in ents} - held)
    # Distinct relationships, with figshare versions of the same deposit collapsed.
    pairs = {tuple(sorted((stem(doi), stem(e['doi']))))
             for doi, ents in graph.items() for e in ents}
    LOGGER.info(f"Records that would carry the field: {len(ours):,}")
    persist(ours, held)
    if ARG.TEST or ARG.WRITE:
        generate_email(ours, held, len(pairs))
    print(file=sys.stderr)
    for key in sorted(COUNT):
        print(f"{key + ':':<32} {COUNT[key]:,}", file=sys.stderr)
    print(f"{'distinct relationships:':<32} {len(pairs):,}", file=sys.stderr)


# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Link datasets to the articles they supplement")
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, prod)')
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
