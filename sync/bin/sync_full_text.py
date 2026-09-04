''' sync_full_text.py
    Update links to full-text files for DOIs. PDFs are preferred.
'''

__version__ = '2.2.0'

import argparse
import collections
import configparser
from operator import attrgetter
import os
import sys
import urllib.request
from metapub import FindIt
from tqdm import tqdm
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL
import jrc_email.jrc_email as JE

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Database
DB = {}
# Counters
COUNT = collections.defaultdict(lambda: 0, {})
# Global variables
ARG = CONFIG = DISCONFIG = LOGGER = None

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
        dbo = attrgetter(f"{source}.{ARG.MANIFOLD}.write")(dbconfig)
        LOGGER.info(f"Connecting to {dbo.name} {ARG.MANIFOLD} on {dbo.host} as {dbo.user}")
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)


def find_full_text(doi, row):
    ''' Find full text for a DOI
        Keyword arguments:
          doi: DOI
          row: row from the dois table
        Returns:
          URL for full text
    '''
    # bioRxiv (PDF)
    jour = DL.get_journal(row)
    if jour and 'bioRxiv' in jour:
        COUNT['biorxiv'] += 1
        return f"{CONFIG['journals']['biorxiv']}{doi}.full.pdf", None
    # Links from DOI record
    if 'link' in row and row['link']:
        for link in row['link']:
            if link['content-type'] == 'application/pdf' or 'pdf' in link['URL']:
                COUNT['link'] += 1
                return link['URL'], None
    # eLife (no PDF)
    if jour and 'eLife' in jour:
        try:
            num = doi.split('/')[-1].replace('elife.', '').split('.')[0]
            COUNT['elife'] += 1
            return f"{CONFIG['journals']['elife']}{num}", None
        except Exception as _:
            pass
    # OpenAlex (PDF)
    oresp = {}
    try:
        oresp = DL.get_doi_record(doi, coll=None, source='openalex')
    except Exception:
        pass
    if oresp:
        # 'best_oa_location_url'
        for field in ['publisher_url_for_pdf', 'best_oa_location_url_for_pdf']:
            if field in oresp and oresp[field]:
                COUNT['openalex'] += 1
                return oresp[field], None
        return "", None
    # OA.Report (PDF)
    oresp = {}
    try:
        oresp = JRC.call_oa(doi)
    except Exception:
        pass
    if oresp:
        # 'best_oa_location_url'
        for field in ['publisher_url_for_pdf', 'best_oa_location_url_for_pdf']:
            if field in oresp and oresp[field]:
                COUNT['oa'] += 1
                return oresp[field], None
    # PubMed Central
    if 'jrc_pmid' in row and row['jrc_pmid']:
        COUNT['pmc'] += 1
        try:
            src = FindIt(row['jrc_pmid'])
            if src.url:
                return src.url, None
            return "", src.reason
        except Exception as _:
            pass
    return "", None


def log_processing(doi, url):
    ''' Record a processing event for a DOI whose full-text URL was set. Called
        after the write and only when the record actually changed, so an event never
        describes a no-op. add_doi_process supplies the program, version, and
        user - none for an unattended run, the operator for a hand-run one.
        A failure is logged and counted rather than fatal: the DOI is already
        stored, and losing its event is not worth ending the run.
        Keyword arguments:
          doi: DOI just written
          url: full-text URL stored for it
        Returns:
          None
    '''
    try:
        DL.add_doi_process(doi, action='add_fulltext', coll=DB['dis'].processing,
                           notes=f"Full text at {url}")
    except Exception as err:
        LOGGER.error(f"Could not log a processing event for {doi}: {err}")
        COUNT['processing_error'] += 1
        return
    COUNT['processing_logged'] += 1


def download_file(doi, url):
    ''' Download a file
        Keyword arguments:
          doi: DOI
          url: URL of the file
        Returns:
          None
    '''
    try:
        #fname = f"{COUNT['downloaded']:04}_{doi.replace('/', '_')}.pdf"
        fname = f"{doi.replace('/', '_')}.pdf"
        urllib.request.urlretrieve(url, f"{ARG.DIR}/{fname}")
        COUNT['downloaded'] += 1
    except Exception as err:
        LOGGER.error(f"Error downloading {url}: {err}")


def generate_email():
    ''' Send the house-style HTML run-summary email (jrc_email): KPI tiles for the
        headline counts plus a per-source full-text breakdown table. Sent to the
        developer only with --test, otherwise to the configured receivers.
        Keyword arguments:
          None
        Returns:
          None
    '''
    run_data = JRC.get_run_data(__file__, __version__).strip()
    mode_label = 'WRITE' if ARG.WRITE else 'DRY RUN'
    mode_tone = 'good' if ARG.WRITE else 'warn'
    kpis = ''.join([
        JE.kpi_card(f"{COUNT['read']:,}", "DOIs checked", width='25%'),
        JE.kpi_card(f"{COUNT['updated']:,}", "Full text found",
                    'good' if COUNT['updated'] else 'neutral', '25%'),
        JE.kpi_card(f"{COUNT['not_found']:,}", "Not found",
                    'bad' if COUNT['not_found'] else 'neutral', '25%'),
        JE.kpi_card(f"{COUNT['downloaded']:,}", "PDFs downloaded", width='25%'),
    ])
    sources = [("bioRxiv", COUNT['biorxiv']), ("Crossref", COUNT['link']),
               ("eLife", COUNT['elife']), ("OpenAlex", COUNT['openalex']),
               ("OA.Report", COUNT['oa']), ("PMC", COUNT['pmc'])]
    rows = ""
    for idx, (label, val) in enumerate(sources):
        bgc = "#f4f6f8" if idx % 2 == 0 else "#ffffff"
        rows += (f'<tr bgcolor="{bgc}" style="background-color:{bgc};">'
                 f'<td style="padding:6px 12px;">{label}</td>'
                 f'<td style="padding:6px 12px;" align="right">{val:,}</td></tr>')
    table = (
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
        'style="border-collapse:collapse;font-size:13px;">'
        '<tr style="color:#5b6b7c;font-size:10.5px;text-transform:uppercase;'
        'letter-spacing:.03em;"><td style="padding:6px 12px;">Source</td>'
        '<td style="padding:6px 12px;" align="right">Full text found</td></tr>'
        + rows + '</table>')
    body = JE.body_row(JE.section_header("&#128196; Full-text sources") + table)
    email_html = JE.render(os.path.basename(__file__), __version__, run_data,
                           mode_label, mode_tone, kpis, body)
    recipient = DISCONFIG['developer'] if ARG.TEST else DISCONFIG['receivers']
    JRC.send_email(email_html, DISCONFIG['sender'], recipient, "Full-text sync", mime='html')


def processing():
    ''' Update full-text links for DOIs
        Keyword arguments:
          None
        Returns:
          None
    '''
    if ARG.FILE:
        if ARG.DOWNLOAD:
            if not os.access(ARG.DIR, os.W_OK):
                terminate_program(f"Directory {ARG.DIR} is not writeable")
        with open(ARG.FILE, 'r', encoding='utf-8') as f:
            dois = [line.strip() for line in f.readlines()]
        rows = []
        for doi in tqdm(dois, desc="Getting DOIs from Crossref"):
            mdata = JRC.call_crossref(doi)
            if mdata:
                mdata['message']['doi'] = mdata['message']['DOI'].lower()
                rows.append(mdata['message'])
        cnt = len(rows)
    else:
        payload = {"jrc_fulltext_url": {"$exists": False},
                   "jrc_obtained_from": "Crossref"}
        try:
            cnt = DB['dis'].dois.count_documents(payload)
            rows = DB['dis'].dois.find(payload)
        except Exception as err:
            terminate_program(err)
    print(f"Found {cnt:,} DOIs to update")
    audit = []
    notfound = []
    for row in tqdm(rows, total=cnt, unit="DOI", desc="Checking DOIs"):
        doi = row['doi']
        fulltext, reason = find_full_text(doi, row)
        if fulltext:
            if ARG.DOWNLOAD and 'pdf' in fulltext:
                download_file(doi, fulltext)
            if not ARG.WRITE or ARG.FILE:
                audit.append(f"{doi} {fulltext}")
            if ARG.FILE:
                continue
            if not ARG.WRITE:
                COUNT['updated'] += 1
                continue
            result = DB['dis'].dois.update_one({"doi": doi},
                                               {"$set": {"jrc_fulltext_url": fulltext}})
            if result.modified_count:
                COUNT['updated'] += result.modified_count
                log_processing(doi, fulltext)
        else:
            notfound.append(f"{doi}\t{row['type']}\t{reason if reason else ''}")
            COUNT['not_found'] += 1
    if audit:
        with open('dois_with_fulltext.txt', 'w', encoding='utf-8') as f:
            for line in audit:
                f.write(line + '\n')
    if notfound:
        with open('dois_missing_fulltext.txt', 'w', encoding='utf-8') as f:
            for line in notfound:
                f.write(line + '\n')
    COUNT['read'] = cnt
    print(f"DOIs checked:    {cnt:,}")
    print("Sources:")
    print(f"  bioRxiv:       {COUNT['biorxiv']:,}")
    print(f"  Crossref:      {COUNT['link']:,}")
    print(f"  eLife:         {COUNT['elife']:,}")
    print(f"  OpenAlex:      {COUNT['openalex']:,}")
    print(f"  OA.Report:     {COUNT['oa']:,}")
    print(f"  PMC:           {COUNT['pmc']:,}")
    print(f"DOIs updated:    {COUNT['updated']:,}")
    if COUNT['processing_logged']:
        print(f"Events logged:   {COUNT['processing_logged']:,}")
    if COUNT['processing_error']:
        print(f"Events failed:   {COUNT['processing_error']:,}")
    print(f"PDFs downloaded: {COUNT['downloaded']:,}")
    print(f"DOIs not found:  {COUNT['not_found']:,}")
    if ARG.TEST or (ARG.WRITE and COUNT['updated']):
        generate_email()

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Apply ORCIDS to orcid collection")
    PARSER.add_argument('--file', dest='FILE', action='store',
                        help='File of DOIs to download')
    PARSER.add_argument('--dir', dest='DIR', action='store',
                        default='pdfs', help='Output directory')
    PARSER.add_argument('--download', dest='DOWNLOAD', action='store_true',
                        default=False, help='Download full-text files')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, prod)')
    PARSER.add_argument('--test', dest='TEST', action='store_true',
                        default=False, help='Send email to developer only')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Write to database/config system')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    DISCONFIG = JRC.simplenamespace_to_dict(JRC.get_config("dis"))
    CONFIG = configparser.ConfigParser()
    CONFIG.read('config.ini')
    processing()
    terminate_program()
