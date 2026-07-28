''' pull_pubmed.py

PURPOSE
-------
Find new works authored at Janelia by searching PubMed for the "Janelia"
affiliation, and write the not-yet-known DOIs to a local file for downstream
ingestion. This is a discovery/candidate step: it is read-only against the DIS
MongoDB database and never writes DOI records itself.

INPUTS
------
- NCBI_API_KEY environment variable (required): API key for the NCBI E-utilities
  API (raises the rate limit to ~10 requests/second).
- DIS MongoDB database (read-only): the `dois` collection (to skip DOIs already
  held) and the `to_ignore` collection (type="doi", DOIs to never add).
- Command-line flags:
    --test     Send the run-summary email to the developer only.
    --write    Send the run-summary email to the full receivers list (only when
               there are DOIs ready to add; otherwise it falls back to the
               developer).
    --verbose  Increase logging verbosity.
    --debug    Maximum logging verbosity.
  Neither flag writes to the database; both only control the email. The output
  files are always written (or cleared) regardless of the flags.

HIGH-LEVEL FLOW
---------------
1. Initialization (initialize_program)
   - Connects to the DIS MongoDB database (read-only).
   - Loads the to_ignore DOIs (type="doi") and every DOI already in the `dois`
     collection into in-memory, lower-cased sets for fast lookup.
2. PubMed search (search_janelia_dois)
   - esearch for "Janelia[Affiliation]" using the NCBI history server, then pages
     the FULL result set off it via efetch in batches of 200. (A bare esearch
     idlist is capped at retmax and would silently drop the remainder.)
   - Parses each PubmedArticle for its PMID, DOI (from PubmedData/ArticleIdList
     only - not a DOI buried in the reference list), title, first author, year,
     and the Janelia authors (those whose <Affiliation> text contains "Janelia").
3. Classification (processing)
   - Skips records with no DOI, DOIs on the ignore list, DOIs already in the
     database, and duplicate DOIs (two PMIDs can share one DOI).
   - A remaining new DOI with at least one Janelia author is "ready to add"; one
     with none is set aside as "no Janelia author found" for manual review (the
     affiliation search matched, but no author affiliation parsed as Janelia).
4. Output
   - Prints a per-bucket summary of counts.
   - Writes the output files below, and sends the run-summary email when --test
     or --write is supplied.

OUTPUT FILES (written to the current working directory)
-------------------------------------------------------
- pubmed_ready.txt             : new, Janelia-authored DOIs, one per line - the
                                 candidate list for downstream ingestion.
- pubmed_details.json          : the same DOIs with their PMID and Janelia authors.
- pubmed_no_janelians.json     : PMID -> raw author XML for records that matched
                                 the affiliation search but had no parseable
                                 Janelia author.
- pubmed_no_janelians_dois.txt : the DOIs of those no-author records.
Each file is deleted when a run produces nothing for it, so a downstream consumer
never re-reads an earlier run's results.

EMAIL
-----
An HTML run-summary email (shared jrc_email house style: header banner, KPI stat
tiles, DOI cards) is sent when --test or --write is supplied and there is
something to report. The full receivers list is used only on a --write run that
has DOIs ready to add; --test runs and nothing-ready runs go to the developer,
and the mode badge follows the actual recipient.

DEPENDENCIES
------------
- jrc_common.jrc_common (JRC): logging, config, database connection, run metadata,
  and email sending.
- jrc_email.jrc_email  (JE) : the shared HTML run-summary email building blocks.
- requests, xmltodict, xml.etree.ElementTree: NCBI HTTP calls and XML parsing.
- tqdm: progress bar for the efetch batches.

NOTES
-----
- NCBI rate limit: 0.1 s between efetch batches with an API key (~10/s), 0.34 s
  without (~3/s).
- The affiliation search is deliberately broad ("Janelia[Affiliation]"); the
  per-author affiliation check is what actually attributes a paper to Janelia,
  hence the "no Janelia author found" review bucket for search hits that do not
  confirm.
- search_janelia_dois was originally drafted by Claude Code v2.0.76 (Sonnet 4.5)
  on 2025-12-24 from the prompt "use the NCBI E-utilities API to find DOIs that
  contain the word 'Janelia' in the author affiliation"; it was close but botched
  the DOI extraction and omitted request timeouts, and has since been rewritten to
  page the full result set off the history server and to guard malformed records.
'''

import argparse
import collections
import json
from operator import attrgetter
import os
import sys
import time
import traceback
from typing import List, Dict, Optional
import xml.etree.ElementTree as ET
import xmltodict
import requests
from tqdm import tqdm
import jrc_common.jrc_common as JRC
import jrc_email.jrc_email as JE

__version__ = '1.0.0'

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Database
DB = {}
# Counters
COUNT = collections.defaultdict(lambda: 0, {})
# Global variables
ARG = DISCONFIG = LOGGER = None
IGNORE = set()
PRESENT = set()


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
    if "NCBI_API_KEY" not in os.environ:
        terminate_program("Missing NCBI API key - set in NCBI_API_KEY environment variable")
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
    try:
        rows = DB['dis']['to_ignore'].find({"type": "doi"})
    except Exception as err:
        terminate_program(err)
    # DOIs are stored/compared lower-case; a set keeps the membership test O(1).
    for row in rows:
        if row.get('key'):
            IGNORE.add(row['key'].lower())
    LOGGER.info(f"Found {len(IGNORE):,} DOIs to ignore")
    try:
        rows = DB['dis'].dois.find({}, {"doi": 1})
    except Exception as err:
        terminate_program(err)
    for row in rows:
        if row.get('doi'):
            PRESENT.add(row['doi'].lower())
    LOGGER.info(f"Found {len(PRESENT):,} DOIs in dois collection")


def get_janelia_authors(auth):
    ''' Get Janelia authors from an XML element
        Keyword arguments:
          auth: XML element
        Returns:
          List of dictionaries containing author information with keys:
          - family: Family name
          - given: Given name
        '''
    authors = []
    raw = []
    for author in auth.findall(".//Author"):
        raw.append(xmltodict.parse(ET.tostring(author)))
        if author.find("AffiliationInfo") is None:
            continue
        for aff in author.findall(".//Affiliation"):
            if not aff.text or 'Janelia' not in aff.text:
                continue
            family = author.findtext("LastName", default="")
            given = author.findtext("ForeName", default="")
            if not given:
                given = author.findtext("Initials", default="")
            LOGGER.debug(f"Janelia author: {family} {given}")
            authors.append({
                "family": family,
                "given": given,
            })
    return raw, authors


def search_janelia_dois(
    max_results: Optional[int] = None,
    email: Optional[str] = None,
    api_key: Optional[str] = None
) -> List[Dict[str, str]]:
    """
    Search PubMed for articles with 'Janelia' in author affiliation and return DOIs.
    The full result set is paged off the NCBI history server (a plain esearch idlist
    is capped at retmax and would silently drop everything beyond it).
    Args:
        max_results: Optional cap on records retrieved (default: None = all)
        email: Your email address (recommended by NCBI)
        api_key: NCBI API key for higher rate limits (optional)
    Returns:
        List of dictionaries with keys: pmid, doi, title, first_author, year,
        authors (Janelia authors), raw (raw XML author data)
    """
    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
    common = {"db": "pubmed"}
    if email:
        common["email"] = email
    if api_key:
        common["api_key"] = api_key
    # Step 1: esearch with the history server so the full set can be paged. retmax=0
    # returns just the count plus the WebEnv/query_key handle to page against.
    search_params = {**common, "term": "Janelia[Affiliation]",
                     "usehistory": "y", "retmax": 0, "retmode": "json"}
    print("Searching PubMed for articles with 'Janelia' in affiliation...")
    search_response = requests.get(f"{base_url}esearch.fcgi", params=search_params, timeout=10)
    search_response.raise_for_status()
    result = search_response.json().get("esearchresult", {})
    try:
        total = int(result.get("count", 0))
    except (TypeError, ValueError):
        total = 0
    webenv = result.get("webenv")
    query_key = result.get("querykey")
    if not total or not webenv or not query_key:
        print("No articles found.")
        return []
    want = total if max_results is None else min(total, max_results)
    print(f"Found {total} articles, retrieving {want} records...")
    # Step 2: page the results off the history server via efetch (batches of 200).
    results = []
    batch_size = 200
    for start in tqdm(range(0, want, batch_size)):
        fetch_params = {**common, "WebEnv": webenv, "query_key": query_key,
                        "retstart": start, "retmax": min(batch_size, want - start),
                        "retmode": "xml"}
        if start > 0:
            time.sleep(0.1 if api_key else 0.34)  # 10/s with a key, ~3/s without
        fetch_response = requests.get(f"{base_url}efetch.fcgi", params=fetch_params, timeout=30)
        fetch_response.raise_for_status()
        root = ET.fromstring(fetch_response.content)
        for article in root.findall(".//PubmedArticle"):
            pmid = article.findtext(".//PMID")
            # The DOI must come from PubmedData/ArticleIdList (not a DOI buried in a
            # ReferenceList); guard against records that lack either element.
            doi = None
            pubmed_data = article.find(".//PubmedData")
            id_list = pubmed_data.find("ArticleIdList") if pubmed_data is not None else None
            if id_list is not None:
                for article_id in id_list.findall("ArticleId"):
                    if article_id.get("IdType") == "doi":
                        doi = article_id.text
                        LOGGER.debug(f"PMID: {pmid} DOI: {doi}")
                        break
            title = article.findtext(".//ArticleTitle", default="N/A")
            first_author = "N/A"
            author_elem = article.find(".//Author")
            if author_elem is not None:
                lastname = author_elem.findtext("LastName", default="")
                initials = author_elem.findtext("Initials", default="")
                if lastname:
                    first_author = f"{lastname} {initials}".strip()
            year = article.findtext(".//PubDate/Year", default="N/A")
            if year == "N/A":
                year = article.findtext(".//PubDate/MedlineDate", default="N/A")
                if year != "N/A" and len(year) >= 4:
                    year = year[:4]
            raw, janelians = get_janelia_authors(article)
            results.append({
                "pmid": pmid,
                "doi": doi if doi else "N/A",
                "title": title,
                "first_author": first_author,
                "year": year,
                "authors": janelians,
                "raw": raw,
            })
    return results


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


def generate_email(details, noauthors_doi):
    ''' Generate and send the HTML run-summary email (jrc_email house style): a
        header banner, a KPI stat-tile row, a "Ready to Add" card (new
        Janelia-authored DOIs with their first Janelia author), and - when any - a
        "No Janelia Author Found" review card. The full receivers list is notified
        only on a real --write run that has DOIs ready to add; --test runs and
        nothing-ready runs go to the developer, and the mode badge follows the
        actual recipient. Nothing is sent when there is neither.
        Keyword arguments:
          details: list of {pmid, doi, authors} for DOIs ready to add
          noauthors_doi: list of DOIs that matched the affiliation search but had
                         no parseable Janelia author
        Returns:
          None
    '''
    if not details and not noauthors_doi:
        return
    run_data = JRC.get_run_data(__file__, __version__).strip()
    to_receivers = ARG.WRITE and not ARG.TEST and bool(details)
    mode_label = 'LIVE' if to_receivers else 'TEST'
    mode_tone = 'good' if to_receivers else 'warn'
    kpis = ''.join([
        JE.kpi_card(f"{COUNT['found']:,}", "Found in PubMed"),
        JE.kpi_card(f"{COUNT['in_database']:,}", "Already in DB"),
        JE.kpi_card(f"{COUNT['ignored']:,}", "Ignored"),
        JE.kpi_card(f"{len(noauthors_doi):,}", "No Janelia author",
                    'warn' if noauthors_doi else 'neutral'),
        JE.kpi_card(f"{len(details):,}", "Ready to add",
                    'good' if details else 'neutral'),
    ])
    ready_entries = []
    for rec in details:
        authors = rec.get('authors') or []
        if authors:
            first = ', '.join(p for p in (authors[0].get('family', '').strip(),
                                          authors[0].get('given', '').strip()) if p)
            if len(authors) > 1:
                first += f" (+{len(authors) - 1})"
        else:
            first = ''
        ready_entries.append((rec['doi'], first))
    ready_body = (JE.doi_card("Ready to Add", ready_entries, 'good',
                              second_header='Janelia author')
                  if ready_entries else
                  f'<div style="color:{JE.GRAY};font-size:13px;">'
                  'No new DOIs are ready to add.</div>')
    body = JE.body_row(JE.section_header(f"&#10003; Ready to Add ({len(details):,})")
                       + ready_body)
    if noauthors_doi:
        review = (JE.section_header(f"&#9888; No Janelia Author Found "
                                    f"({len(noauthors_doi):,})")
                  + f'<div style="color:{JE.GRAY};font-size:12px;margin:-4px 0 10px 0;">'
                  'Matched "Janelia" in a PubMed affiliation search, but no author had a '
                  'Janelia affiliation the parser could read - review before ingesting.</div>'
                  + JE.doi_card("No Janelia Author", [(d, None) for d in noauthors_doi],
                                'warn', icon='&#9888;'))
        body += JE.body_row(review, '6px 28px 4px 28px')
    msg = JE.render(os.path.basename(__file__), __version__, run_data,
                    mode_label, mode_tone, kpis, body)
    try:
        email = DISCONFIG['receivers'] if to_receivers else DISCONFIG['developer']
        LOGGER.info(f"Sending email to {email}")
        JRC.send_email(msg, DISCONFIG['sender'], email, "PubMed DOI sync", mime='html')
    except Exception as err:
        print(str(err))
        traceback.print_exc()
        terminate_program(err)


def processing():
    ''' Find unprocessed DOIs with Janelia authors in PubMed.
        Keyword arguments:
          None
        Returns:
          None
    '''
    articles = search_janelia_dois(api_key=os.environ["NCBI_API_KEY"])
    COUNT['found'] = len(articles)
    to_process = []
    details = []
    noauthors = {}
    noauthors_doi = []
    queued = set()
    for row in articles:
        doi = row['doi'].lower()
        if not doi or doi == 'n/a':
            LOGGER.warning(f"No DOI found for {row['pmid']}")
            COUNT['no_doi'] += 1
            continue
        if doi in IGNORE:
            COUNT['ignored'] += 1
            continue
        if doi in PRESENT:
            COUNT['in_database'] += 1
            continue
        if doi in queued:
            # Two PMIDs can carry the same DOI (e.g. a correction); keep it once.
            COUNT['duplicate'] += 1
            continue
        COUNT['to_check'] += 1
        if row['authors']:
            queued.add(doi)
            to_process.append(doi)
            details.append({"pmid": row['pmid'], "doi": row['doi'], "authors": row['authors']})
        else:
            noauthors[row['pmid']] = row['raw']
            noauthors_doi.append(row['doi'])
            LOGGER.warning(f"No Janelia authors found for {row['pmid']} {row['doi']}")
    print(f"DOIs returned from PubMed:    {COUNT['found']:,}")
    print(f"DOIs ignored:                 {COUNT['ignored']:,}")
    print(f"DOIs in database:             {COUNT['in_database']:,}")
    print(f"Duplicate DOIs skipped:       {COUNT['duplicate']:,}")
    print(f"PMIDs with no DOI:            {COUNT['no_doi']:,}")
    print(f"DOIs to check for Janelians:  {COUNT['to_check']:,}")
    print(f"DOIs to add:                  {len(to_process):,}")
    print(f"DOIs with no Janelia authors: {len(noauthors):,}")
    if to_process:
        with open('pubmed_ready.txt', 'w', encoding='utf-8') as fileout:
            for doi in to_process:
                fileout.write(doi + '\n')
        with open('pubmed_details.json', 'w', encoding='utf-8') as fileout:
            json.dump(details, fileout, indent=4)
    else:
        remove_output('pubmed_ready.txt')
        remove_output('pubmed_details.json')
    if noauthors:
        with open('pubmed_no_janelians.json', 'w', encoding='utf-8') as fileout:
            json.dump(noauthors, fileout, indent=4)
        with open('pubmed_no_janelians_dois.txt', 'w', encoding='utf-8') as fileout:
            for doi in noauthors_doi:
                fileout.write(doi + '\n')
    else:
        remove_output('pubmed_no_janelians.json')
        remove_output('pubmed_no_janelians_dois.txt')
    if ARG.TEST or ARG.WRITE:
        generate_email(details, noauthors_doi)

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Find new works from PubMed")
    PARSER.add_argument('--test', dest='TEST', action='store_true',
                        default=False, help='Send summary email to the developer only')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Send summary email to the receivers list')
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
