''' sync_full_text.py
    Update links to full-text files for DOIs. PDFs are preferred.
'''

__version__ = '1.0.0'

import argparse
import collections
import configparser
from operator import attrgetter
import sys
import urllib.request
from metapub import FindIt
from tqdm import tqdm
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Database
DB = {}
# Counters
COUNT = collections.defaultdict(lambda: 0, {})
# Global variables
ARG = CONFIG = LOGGER = None

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
    # OA (PDF)
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
    # eLife (no PDF)
    if jour and 'eLife' in jour:
        try:
            num = doi.split('/')[-1].replace('elife.', '').split('.')[0]
            COUNT['elife'] += 1
            return f"{CONFIG['journals']['elife']}{num}", None
        except Exception as _:
            pass
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


def download_file(url):
    ''' Download a file
        Keyword arguments:
          url: URL of the file
        Returns:
          None
    '''
    urllib.request.urlretrieve(url, f"pdfs/{url.split('/')[-1]}")


def processing():
    ''' Update full-text links for DOIs
        Keyword arguments:
          None
        Returns:
          None
    '''
    payload = {"jrc_fulltext_url": {"$exists": False},
               "jrc_obtained_from": "Crossref"}
    try:
        cnt = DB['dis'].dois.count_documents(payload)
        print(f"Found {cnt:,} DOIs to update")
        rows = DB['dis'].dois.find(payload)
    except Exception as err:
        terminate_program(err)
    audit = []
    notfound = []
    for row in tqdm(rows, total=cnt, unit="DOI"):
        doi = row['doi']
        fulltext, reason = find_full_text(doi, row)
        if fulltext:
            if ARG.DOWNLOAD and '.pdf' in fulltext:
                download_file(fulltext)
            if not ARG.WRITE:
                audit.append(f"{doi} {fulltext}")
                COUNT['updated'] += 1
                continue
            result = DB['dis'].dois.update_one({"doi": doi},
                                               {"$set": {"jrc_fulltext_url": fulltext}})
            if result.modified_count:
                COUNT['updated'] += result.modified_count
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
    print(f"DOIs checked:   {cnt:,}")
    print("Sources:")
    print(f"  bioRxiv:      {COUNT['biorxiv']:,}")
    print(f"  Crossref:     {COUNT['link']:,}")
    print(f"  OA:           {COUNT['oa']:,}")
    print(f"  eLife:        {COUNT['elife']:,}")
    print(f"  PMC:          {COUNT['pmc']:,}")
    print(f"DOIs updated:   {COUNT['updated']:,}")
    print(f"DOIs not found: {COUNT['not_found']:,}")

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Apply ORCIDS to orcid collection")
    PARSER.add_argument('--institution', dest='INSTITUTION', action='store',
                        default='janelia', choices=['hhmi', 'janelia'],
                        help='Institution (hhmi, [janelia])')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, prod)')
    PARSER.add_argument('--download', dest='DOWNLOAD', action='store_true',
                        default=False, help='Download full-text files')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Write to database/config system')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    CONFIG = configparser.ConfigParser()
    CONFIG.read('config.ini')
    processing()
    terminate_program()
