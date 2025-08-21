''' find_citing_works.py
    Find citing works for a given DOI (or list of DOIs)
    Usage:
        python find_citing_works.py --doi 10.1038/s41586-020-2649-2
        python find_citing_works.py --file dois.txt
    Output:
        citing_works.json
    Example:
        python find_citing_works.py --doi 10.1038/s41586-020-2649-2
'''

import argparse
import collections
import json
from operator import attrgetter
import sys
import time
import pyalex
import requests
import tqdm
import xmltodict
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL

#pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Parms
ARG = LOGGER = None
# Database
DB = {}
# API endpoints
PMC_CITING_WORKS = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi?dbfrom=pubmed" \
                   + "&linkname=pubmed_pmc_refs&email=svirskasr@hhmi.org&id="
# Counters
COUNT = collections.defaultdict(int)
# Missing DOIs
MISSING = []


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
        dbo = attrgetter(f"{source}.prod.read")(dbconfig)
        LOGGER.info(f"Connecting to {dbo.name} {dbo.host} as {dbo.user}")
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)


def parse_openalex_dois(works):
    ''' Get citing DOIs from the works
        Keyword arguments:
          doi: DOI of the cited work
          works: list of works
        Returns:
          list of DOIs
    '''
    dois = []
    for itm in works:
        dois = []
        for itm in works:
            if 'doi' in itm and itm['doi']:
                dois.append(itm['doi'])
    return dois


def get_openalex_citations(doi):
    ''' Get citing DOIs from OpenAlex
        Keyword arguments:
          doi: DOI of the cited work
        Returns:
          list of DOIs
    '''
    try:
        work = DL.get_doi_record(doi, coll=None, source='openalex')
    except Exception as err:
        LOGGER.warning(f"Error getting OpenAlex record for {doi}: {err}")
        COUNT['error'] += 1
        MISSING.append(doi)
        return []
    if not work or 'id' not in work:
        if ARG.VERBOSE:
            LOGGER.warning(f"No record found for {doi}")
        COUNT['not_found'] += 1
        MISSING.append(doi)
        return []
    COUNT['found'] += 1
    oaid = work['id'].split('/')[-1]
    try:
        pager = pyalex.Works().filter(cites=oaid).paginate(per_page=100)
    except Exception as err:
        LOGGER.warning(f"Error getting citing works for {doi}: {err}")
        COUNT['error'] += 1
        return []
    dois = []
    for page in pager:
        dois.extend(parse_openalex_dois(page))
    if dois:
        COUNT['cited'] += 1
    else:
        COUNT['not_cited'] += 1
    return dois


def single_pmc_call(pmcids):
    ''' Get DOIs from PMC
        Keyword arguments:
          pmcids: list of PMC IDs
        Returns:
          list of DOIs
    '''
    outlist = []
    try:
        recs = DL.convert_pubmed(','.join(pmcids), 'pmcid')
        if not recs:
            return outlist
        for rec in recs:
            if rec and 'doi' in rec:
                outlist.append(rec['doi'])
    except Exception as err:
        LOGGER.warning(f"Error converting PMCIDs to DOIs: {err}")
        COUNT['error'] += 1
        return []
    return outlist


def get_pmc_citations(doi):
    ''' Get citing DOIs from PMC
        Keyword arguments:
          doi: DOI of the cited work
        Returns:
          list of DOIs
    '''
    try:
        rec = DL.get_doi_record(doi, coll=DB['dis']['dois'])
    except Exception as err:
        LOGGER.warning(f"Error getting DOI record for {doi}: {err}")
        COUNT['error'] += 1
        MISSING.append(doi)
        return []
    if not rec or 'jrc_pmid' not in rec or 'jrc_pmc' not in rec:
        if ARG.VERBOSE:
            LOGGER.warning(f"No PubMed IDs found for {doi}")
        COUNT['not_found'] += 1
        MISSING.append(doi)
        return []
    COUNT['found'] += 1
    try:
        resp = requests.get(f"{PMC_CITING_WORKS}{rec['jrc_pmid']}", timeout=10)
    except Exception as err:
        LOGGER.warning(f"Error getting citing works for {doi}: {err}")
        COUNT['error'] += 1
        return []
    xmld = xmltodict.parse(resp.text)
    if not xmld:
        LOGGER.warning(f"Error getting citing works for {doi}: {err}")
        COUNT['error'] += 1
        return []
    if 'eLinkResult' not in xmld or 'LinkSet' not in xmld['eLinkResult'] \
        or 'LinkSetDb' not in xmld['eLinkResult']['LinkSet'] \
        or 'Link' not in xmld['eLinkResult']['LinkSet']['LinkSetDb']:
        if ARG.VERBOSE:
            LOGGER.warning(f"No citing works found for {doi}")
        COUNT['not_found'] += 1
        MISSING.append(doi)
        return []
    dois = []
    pmcids = []
    limit = len(xmld['eLinkResult']['LinkSet']['LinkSetDb']['Link'])
    for works in xmld['eLinkResult']['LinkSet']['LinkSetDb']['Link']:
        pmcids.append(works['Id'])
        if len(pmcids) < limit and len(pmcids) < 20:
            continue
        resp = single_pmc_call(pmcids)
        if resp:
            dois.extend(resp)
        pmcids = []
    if pmcids:
        resp = single_pmc_call(pmcids)
        if resp:
            dois.extend(resp)
    if dois:
        COUNT['cited'] += 1
    else:
        COUNT['not_cited'] += 1
    return dois


def process_dois():
    ''' Process the DOIs
        Keyword arguments:
          None
        Returns:
          None
    '''
    cdict = {}
    pyalex.config.email = "svirskasr@hhmi.org"
    if ARG.DOI:
        dois = [ARG.DOI]
    else:
        with open(ARG.FILE, 'r', encoding='utf-8') as file:
            dois = [line.strip().lower() for line in file if line.strip()]
    for doi in tqdm.tqdm(dois, desc="Processing DOIs"):
        if len(dois) > 1:
            time.sleep(.11)
        COUNT['read'] += 1
        if ARG.SOURCE == 'openalex':
            dois = get_openalex_citations(doi)
        else:
            dois = get_pmc_citations(doi)
        if ARG.COUNT:
            cdict[doi] = len(dois)
        else:
            cdict[doi] = dois
    # Write results to JSON file
    output_file = 'citing_works.json'
    LOGGER.info(f"Writing results to {output_file}")
    try:
        with open(output_file, 'w', encoding='utf-8') as outfile:
            json.dump(cdict, outfile, indent=2)
    except Exception as err:
        terminate_program(err)
    # Write missing DOIs to JSON file
    if MISSING:
        output_file = 'missing_dois.json'
        LOGGER.info(f"Writing missing DOIs to {output_file}")
        try:
            with open(output_file, 'w', encoding='utf-8') as outfile:
                json.dump(MISSING, outfile, indent=2)
        except Exception as err:
            terminate_program(err)
    if ARG.DOI and ARG.COUNT and cdict:
        print(cdict)
    print(f"DOIs read:                 {COUNT['read']}")
    if COUNT['not_found']:
        print(f"DOIs not found:            {COUNT['not_found']}")
    print(f"DOIs found:                {COUNT['found']}")
    print(f"DOIs with citing works:    {COUNT['cited']}")
    print(f"DOIs without citing works: {COUNT['not_cited']}")
    if COUNT['error']:
        print(f"Errors:                    {COUNT['error']}")


if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(
        description="Sync DOIs")
    PARSER.add_argument('--source', dest='SOURCE', action='store',
                        default='openalex', choices=['openalex', 'pubmed'],
                        help='Source of DOIs')
    group = PARSER.add_mutually_exclusive_group(required=True)
    group.add_argument('--doi', dest='DOI', action='store',
                        help='DOI')
    group.add_argument('--file', dest='FILE', action='store',
                        help='File containing DOIs')
    PARSER.add_argument('--count', dest='COUNT', action='store_true',
                        default=False, help='Provide citing DOI counts only')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    process_dois()
    terminate_program()
