"""
Query the Elsevier API for Janelia-affiliated publications and write candidate
DOIs to a local file for downstream ingestion.

Usage:
    python pull_elsevier.py [--test] [--write] [--verbose] [--debug]

Searches the Elsevier Metadata API for articles with a Janelia affiliation,
filtering to records on or after the minimum publishing date configured in the
dis config. Results are paged in batches of 200.

DOIs already present in the MongoDB dois collection, or listed in the
external_dois or to_ignore collections, are excluded from output.

Records whose prism:coverDate is in the future are treated as not-yet-published
(Elsevier assigns an ahead-of-print article to a forthcoming issue with a future
cover date): they are held back rather than flagged as ready, excluded from
elsevier_ready.txt, and reported in their own email section. No state is kept -
once the cover date has passed such a DOI flows into the ready set on a later run.

Output files (written to the current working directory):
    elsevier_ready.txt   DOIs not yet in the database, ready for processing
                         (future-cover-date DOIs are excluded). When a run has no
                         ready DOIs, any stale elsevier_ready.txt is deleted so a
                         downstream consumer never re-reads an earlier run's output.

An HTML summary email is sent when --test or --write is supplied.
"""

__version__ = '2.4.1'

import argparse
import collections
import datetime
import os
from operator import attrgetter
import sys
import traceback
from tqdm import tqdm
import jrc_common.jrc_common as JRC
import jrc_email.jrc_email as JE

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Database
DB = {}
DOI_CACHE = {}  # doi -> source collection name
# Counters
COUNT = collections.defaultdict(lambda: 0, {})
# Global variables
ARG = DISCONFIG = LOGGER = None

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
    dbs = ['dis']
    for source in dbs:
        dbo = attrgetter(f"{source}.prod.read")(dbconfig)
        LOGGER.info(f"Connecting to {dbo.name} prod on {dbo.host} as {dbo.user}")
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)
    build_doi_cache()


def build_doi_cache():
    ''' Pre-load known DOIs from dois, external_dois, and to_ignore collections
        Keyword arguments:
          None
        Returns:
          None
    '''
    try:
        for rec in DB['dis']['dois'].find({}, {"doi": 1}):
            if rec.get('doi'):
                DOI_CACHE[rec['doi'].lower()] = 'dois'
        for rec in DB['dis']['external_dois'].find({}, {"doi": 1}):
            if rec.get('doi'):
                DOI_CACHE[rec['doi'].lower()] = 'external_dois'
        for rec in DB['dis']['to_ignore'].find({"type": "doi"}, {"key": 1}):
            if rec.get('key'):
                DOI_CACHE[rec['key'].lower()] = 'to_ignore'
    except Exception as err:
        terminate_program(err)
    LOGGER.info(f"Loaded {len(DOI_CACHE):,} known DOIs into cache")


def get_janelia_works():
    ''' Get author works
        Keyword arguments:
          None
        Returns:
          dict mapping DOI (lower-case) -> Elsevier cover date (prism:coverDate)
    '''
    suffix = "metadata/article?query=aff%28janelia%29&httpAccept=application/json&count=200"
    rows = {}
    total = None
    seen = 0
    part = 1
    while True:
        try:
            resp = JRC.call_elsevier(suffix)
        except Exception as err:
            terminate_program(err)
        if 'search-results' not in resp:
            terminate_program(f"Unexpected Elsevier response: {resp}")
        results = resp['search-results']
        if total is None:
            try:
                total = int(results.get('opensearch:totalResults'))
            except (TypeError, ValueError):
                total = None
        entries = results.get('entry', [])
        seen += len(entries)
        for row in entries:
            if 'prism:coverDate' in row and 'prism:doi' in row \
               and row['prism:coverDate'] >= DISCONFIG['min_publishing_date']:
                # Retain the cover date so processing() can hold back articles with
                # a future (not-yet-published) cover date.
                rows[row['prism:doi'].lower()] = row['prism:coverDate']
        print(f"Got part {part}: found {len(rows)} works")
        part += 1
        suffix = None
        for link in results.get('link', []):
            if link['@ref'] == 'next':
                suffix = link['@href'].replace('https://api.elsevier.com/content/', '')
        if not suffix:
            break
    # Elsevier stops issuing a 'next' link once its deep-paging cap is reached; if
    # we paged fewer entries than the reported total, results beyond the cap were
    # silently dropped - surface that rather than let it read as full coverage.
    if total is not None and seen < total:
        LOGGER.warning(f"Elsevier returned {seen:,} of {total:,} results before paging "
                       f"stopped - {total - seen:,} were not retrieved (deep-paging cap). "
                       "Narrow the query (e.g. a later min_publishing_date) to capture them.")
    LOGGER.debug(f"Found {len(rows)} works")
    return rows


def generate_email(to_process, not_published):
    ''' Generate and send the HTML run-summary email (built with jrc_email): a
        header banner, a KPI stat-tile row, a "Ready to Process" card, and (when
        any) a highlighted "Not Yet Published" card for DOIs held back on a future
        cover date. Nothing is sent when there are neither ready nor held-back DOIs.
        Keyword arguments:
          to_process: list of DOIs ready to be added
          not_published: list of (DOI, cover date) held back as not yet published
        Returns:
          None
    '''
    if not to_process and not not_published:
        return
    run_data = JRC.get_run_data(__file__, __version__).strip()
    # Receivers (the full list) are notified only on a real --write run that has
    # something ready to add; --test runs, and held-back-only runs (nothing ready,
    # just not-yet-published), go to the developer. The mode badge follows the
    # actual recipient so it never overstates who was notified.
    to_receivers = ARG.WRITE and not ARG.TEST and bool(to_process)
    mode_label = 'LIVE' if to_receivers else 'TEST'
    mode_tone = 'good' if to_receivers else 'warn'
    kpis = ''.join([
        JE.kpi_card(f"{COUNT['read']:,}", "Read from Elsevier"),
        JE.kpi_card(f"{COUNT['in_dois']:,}", "Already in DB"),
        JE.kpi_card(f"{COUNT['ignored']:,}", "To ignore"),
        JE.kpi_card(f"{COUNT['not_published']:,}", "Not yet published",
                    'warn' if not_published else 'neutral'),
        JE.kpi_card(f"{len(to_process):,}", "Ready to process",
                    'good' if to_process else 'neutral'),
    ])
    ready_body = (JE.doi_card("Ready to Process", [(doi, None) for doi in to_process], 'good')
                  if to_process else
                  f'<div style="color:{JE.GRAY};font-size:13px;">'
                  'No new DOIs are ready to process.</div>')
    body = JE.body_row(JE.section_header(f"&#10003; Ready to Process ({len(to_process):,})")
                       + ready_body)
    if not_published:
        held = (JE.section_header(f"&#128197; Not Yet Published ({len(not_published):,})")
                + f'<div style="color:{JE.GRAY};font-size:12px;margin:-4px 0 10px 0;">'
                'Elsevier assigned these to a forthcoming issue with a future cover date; '
                'they were held back and NOT added, and will be picked up automatically once '
                'the cover date has passed.</div>'
                + JE.doi_card("Not Yet Published",
                              [(doi, cover) for doi, cover in not_published],
                              'warn', second_header='Cover date'))
        body += JE.body_row(held, '6px 28px 4px 28px')
    msg = JE.render(os.path.basename(__file__), __version__, run_data,
                    mode_label, mode_tone, kpis, body)
    try:
        email = DISCONFIG['receivers'] if to_receivers else DISCONFIG['developer']
        LOGGER.info(f"Sending email to {email}")
        JRC.send_email(msg, DISCONFIG['sender'], email, "Elsevier DOI sync", mime='html')
    except Exception as err:
        print(str(err))
        traceback.print_exc()
        terminate_program(err)


def remove_output(path):
    ''' Delete a stale output file when the current run produced nothing for it, so
        a downstream consumer never re-reads a previous run's results. A missing
        file is fine; a real removal error is logged but not fatal.
        Keyword arguments:
          path: output file path
        Returns:
          None
    '''
    try:
        os.remove(path)
        LOGGER.info(f"No output for {path}; removed stale file")
    except FileNotFoundError:
        pass
    except OSError as err:
        LOGGER.warning(f"Could not remove stale output file {path}: {err}")


def processing():
    ''' Processing
        Keyword arguments:
          None
        Returns:
          None
    '''
    works = get_janelia_works()
    COUNT['read'] = len(works)
    today = datetime.date.today().isoformat()
    to_process = []
    not_published = []
    for doi, cover in tqdm(works.items(), desc="Processing DOIs"):
        if doi in DOI_CACHE:
            if DOI_CACHE[doi] == 'dois':
                COUNT['in_dois'] += 1
            else:
                COUNT['ignored'] += 1
            continue
        # A cover date in the future means Elsevier has assigned the article to a
        # forthcoming issue but it is not actually published yet - hold it back
        # rather than flag it as ready. It will flow into the ready set on a later
        # run once the cover date has passed (it stays absent from the DB until then).
        if cover and cover > today:
            not_published.append((doi, cover))
            COUNT['not_published'] += 1
            continue
        to_process.append(doi)
    not_published.sort(key=lambda item: item[1])
    if to_process:
        with open('elsevier_ready.txt', 'w', encoding='utf-8') as fileout:
            for doi in to_process:
                fileout.write(doi + '\n')
    else:
        remove_output('elsevier_ready.txt')
    summary = (
        f"DOIs read from Elsevier:   {COUNT['read']:,}\n"
        f"DOIs already in database:  {COUNT['in_dois']:,}\n"
        f"DOIs to ignore:            {COUNT['ignored']:,}\n"
        f"DOIs not yet published:    {COUNT['not_published']:,}\n"
        f"DOIs ready for processing: {len(to_process):,}"
    )
    print(summary)
    if ARG.TEST or ARG.WRITE:
        generate_email(to_process, not_published)
# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Find new works from Elsevier")
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Flag, send emails')
    PARSER.add_argument('--test', dest='TEST', action='store_true',
                        default=False, help='Flag, Test mode')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    DISCONFIG = JRC.simplenamespace_to_dict(JRC.get_config("dis"))
    initialize_program()
    processing()
    terminate_program()
