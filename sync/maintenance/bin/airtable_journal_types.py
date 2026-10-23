""" airtable_journal_types.py
    Report the journal type of every Publisher/Journal pair in the Airtable
    Publications table.

    Each Publisher/Journal pair is expected to have exactly one Journal Type
    (hybrid, open access, subscription, ...). Pairs that appear with more than
    one type are an error condition: they are reported in a separate table and
    the program exits with a non-zero status. Publications with a blank type are
    ignored (they are only counted in the summary line).

    In the Publications table the publisher and journal fields are links, so
    they are resolved to names through the Publishers and Journals tables, and
    Journal Type is a lookup of the linked journal's Type. A publication linked
    to more than one journal or publisher is keyed on all of them (joined with
    "; "), and publications with no journal are counted and skipped, since the
    type comes from the journal.

    The types are also applied to the MongoDB subscription collection: each
    subscription record of type "Journal" whose title matches an Airtable journal
    gets that journal's Journal Type in a journal_type field
    (added, or updated if it differs). Titles are compared case-, accent- and
    punctuation-insensitively, with "&" equal to "and" and a leading "The"
    ignored. Publisher names are not compared: they rarely agree between the two
    systems (Airtable "Wiley" vs. subscription "John Wiley & Sons, Ltd"). A title
    that Airtable lists under more than one type is ambiguous and is skipped.
    Without --write nothing is changed: the records that would be changed are
    listed, using read-only database credentials.

    Credentials come from the environment and are never printed or logged:
      AIRTABLE_BASEID  ID of the base (appXXXXXXXXXXXXXX)
      AIRTABLE_TOKEN   personal access token (data.records:read scope)

    Usage:
        python airtable_journal_types.py [--tsv] [--write] [--manifold dev|prod]
                                         [--verbose] [--debug]
"""

__version__ = '1.0.0'

import argparse
import collections
from operator import attrgetter
import os
import re
import sys
import time
import unicodedata
from urllib.parse import quote
from pymongo import UpdateOne
import requests
from tqdm import tqdm
import jrc_common.jrc_common as JRC

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

API = "https://api.airtable.com/v0"
PAGE_SIZE = 100
# Airtable allows 5 requests/second per base
PAUSE = 0.21
TIMEOUT = 30
# A 429 means wait 30 seconds before trying again
RATE_LIMIT_WAIT = 30
MAX_TRIES = 5
# Seconds to wait before retrying a server error or a dropped connection (doubles each try)
RETRY_WAIT = 5
NONE = "(none)"
# Database
DB = {}
# Counters
COUNT = collections.defaultdict(lambda: 0, {})
# Globals
ARG = LOGGER = None
BASEID = TOKEN = None


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


class AirtableError(Exception):
    ''' An Airtable request failed. The message is safe to print: it never contains
        the token or the request headers.
    '''


def get_credentials():
    ''' Get the base ID and token from the environment
        Keyword arguments:
          None
        Returns:
          Tuple of (base ID, token)
    '''
    baseid = os.environ.get('AIRTABLE_BASEID')
    token = os.environ.get('AIRTABLE_TOKEN')
    missing = [name for name, val in (('AIRTABLE_BASEID', baseid),
                                      ('AIRTABLE_TOKEN', token)) if not val]
    if missing:
        raise AirtableError(f"Environment variable(s) not set: {', '.join(missing)}")
    return baseid, token


def api_get(url, token, params=None):
    ''' GET a JSON document from the Airtable API. Rate limiting (429), server errors
        (5xx), timeouts and dropped connections are retried, up to MAX_TRIES attempts.
        Keyword arguments:
          url: API URL
          token: personal access token
          params: query parameters
        Returns:
          Decoded JSON response
    '''
    for attempt in range(1, MAX_TRIES + 1):
        last = attempt == MAX_TRIES
        try:
            # Only the response is ever reported; the request (and its Authorization
            # header) is deliberately kept out of every message.
            resp = requests.get(url, params=params, timeout=TIMEOUT,
                                headers={"Authorization": f"Bearer {token}"})
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as err:
            if last:
                raise AirtableError(f"Could not reach Airtable: {type(err).__name__}") from None
            time.sleep(RETRY_WAIT * 2 ** (attempt - 1))
            continue
        except requests.exceptions.RequestException as err:
            raise AirtableError(f"Could not reach Airtable: {type(err).__name__}") from None
        if resp.status_code == 429 and not last:
            time.sleep(RATE_LIMIT_WAIT)
            continue
        if resp.status_code >= 500 and not last:
            time.sleep(RETRY_WAIT * 2 ** (attempt - 1))
            continue
        break
    if resp.status_code != 200:
        try:
            body = resp.json()
            detail = body.get('error') if isinstance(body, dict) else None
        except ValueError:
            detail = None
        raise AirtableError(f"Airtable returned HTTP {resp.status_code}: "
                            f"{detail or resp.reason}")
    try:
        return resp.json()
    except ValueError:
        raise AirtableError("Airtable returned a response that is not JSON") from None


def get_records(table, fields, desc):
    ''' Get every record of a table, following Airtable's pagination
        Keyword arguments:
          table: table name
          fields: list of field names to return
          desc: progress bar description
        Returns:
          List of records
    '''
    url = f"{API}/{BASEID}/{quote(table, safe='')}"
    params = [('fields[]', fld) for fld in fields] + [('pageSize', PAGE_SIZE)]
    records = []
    offset = None
    with tqdm(desc=desc, unit=' records') as pbar:
        while True:
            page = api_get(url, TOKEN, params + ([('offset', offset)] if offset else []))
            records.extend(page.get('records', []))
            pbar.update(len(page.get('records', [])))
            offset = page.get('offset')
            if not offset:
                return records
            time.sleep(PAUSE)


def get_names(table, field):
    ''' Map the record IDs of a table to the value of one field (its primary field)
        Keyword arguments:
          table: table name
          field: name of the field holding the name
        Returns:
          Dict of record ID -> name
    '''
    return {rec['id']: rec['fields'].get(field, '').strip()
            for rec in get_records(table, [field], table)}


def label(ids, names):
    ''' Build the display name for a (possibly multi-valued) link field
        Keyword arguments:
          ids: list of linked record IDs
          names: dict of record ID -> name
        Returns:
          Name(s) joined with "; ", or NONE
    '''
    found = sorted({names.get(rid) or rid for rid in ids or []})
    return '; '.join(found) if found else NONE


def collect_types(publications, publishers, journals):
    ''' Count the Journal Types seen for each Publisher/Journal pair
        Keyword arguments:
          publications: Publications records
          publishers: dict of publisher record ID -> name
          journals: dict of journal record ID -> name
        Returns:
          Dict of (publisher, journal) -> Counter of type -> number of publications
    '''
    pairs = collections.defaultdict(collections.Counter)
    for rec in publications:
        fields = rec['fields']
        if not fields.get('journal'):
            COUNT['no_journal'] += 1
            continue
        key = (label(fields.get('publisher'), publishers), label(fields['journal'], journals))
        # A lookup comes back as a list, one entry per linked journal
        types = set(fields.get('Journal Type') or [])
        if not types:
            COUNT['blank_type'] += 1
            continue
        for jtype in types:
            pairs[key][jtype] += 1
    return pairs


def print_table(headers, rows):
    ''' Print a table, aligned for reading or tab-separated with --tsv
        Keyword arguments:
          headers: list of column headings
          rows: list of row lists
        Returns:
          None
    '''
    rows = [[str(col) for col in row] for row in rows]
    if ARG.TSV:
        for row in [headers] + rows:
            print('\t'.join(row))
        return
    widths = [max(len(row[idx]) for row in [headers] + rows) for idx in range(len(headers))]
    for row in [headers, ['-' * wid for wid in widths]] + rows:
        print('  '.join(col.ljust(wid) for col, wid in zip(row, widths)).rstrip())


def report(pairs):
    ''' Print the consistent pairs and, separately, the pairs in error
        Keyword arguments:
          pairs: dict of (publisher, journal) -> Counter of type -> publications
        Returns:
          Number of pairs in error
    '''
    good = []
    bad = []
    for (publisher, journal), counts in sorted(pairs.items(), key=lambda kv: (
            kv[0][0].lower(), kv[0][1].lower())):
        if len(counts) == 1:
            jtype, count = next(iter(counts.items()))
            good.append([publisher, journal, jtype, count])
        else:
            detail = ', '.join(f"{jtype} ({count:,})" for jtype, count in counts.most_common())
            bad.append([publisher, journal, detail])
    print(f"\nPublisher / Journal / Type ({len(good):,} pairs)\n")
    print_table(['Publisher', 'Journal', 'Type', 'Publications'], good)
    if bad:
        print(f"\nERROR: Publisher / Journal pairs without exactly one type ({len(bad):,})\n")
        print_table(['Publisher', 'Journal', 'Types (publications)'], bad)
    return len(bad)


def normalize(text):
    ''' Reduce a journal title to a form that compares equal across systems
        Keyword arguments:
          text: journal title
        Returns:
          Lower-case ASCII string of space-separated words, without a leading "the"
          (empty if the title has no Latin letters or digits)
    '''
    text = unicodedata.normalize('NFKD', text or '').encode('ascii', 'ignore').decode()
    text = re.sub(r'[^a-z0-9]+', ' ', text.lower().replace('&', ' and ')).strip()
    return re.sub(r'^the ', '', text)


def types_by_title(pairs):
    ''' Collapse the Publisher/Journal pairs to one Journal Type per journal title
        Keyword arguments:
          pairs: dict of (publisher, journal) -> Counter of type -> publications
        Returns:
          Tuple of (dict of normalized title -> type, set of ambiguous normalized titles)
    '''
    found = collections.defaultdict(set)
    for (_, journal), counts in pairs.items():
        title = normalize(journal)
        # A title with no Latin letters or digits normalizes to '' and would "match"
        # every other such title, so it can't be used as a key
        if title:
            found[title].update(counts)
    types = {title: next(iter(jtypes)) for title, jtypes in found.items() if len(jtypes) == 1}
    return types, {title for title, jtypes in found.items() if len(jtypes) > 1}


def connect():
    ''' Connect to the dis database (write credentials only when --write is given)
        Keyword arguments:
          None
        Returns:
          None
    '''
    try:
        dbconfig = JRC.get_config("databases")
        dbo = attrgetter(f"dis.{ARG.MANIFOLD}.{'write' if ARG.WRITE else 'read'}")(dbconfig)
        LOGGER.info(f"Connecting to {dbo.name} {ARG.MANIFOLD} on {dbo.host} as {dbo.user}")
        DB['dis'] = JRC.connect_database(dbo)
    except Exception as err:
        terminate_program(err)


def update_subscriptions(pairs):
    ''' Add or update journal_type on matching subscription records
        Keyword arguments:
          pairs: dict of (publisher, journal) -> Counter of type -> publications
        Returns:
          None
    '''
    connect()
    types, ambiguous = types_by_title(pairs)
    for title in sorted(ambiguous):
        LOGGER.warning(f"Skipping '{title}': Airtable lists it under more than one type")
    ops = []
    try:
        rows = list(DB['dis'].subscription.find({"type": "Journal"},
                                                {"title": 1, "provider": 1, "journal_type": 1}))
    except Exception as err:
        terminate_program(err)
    for rec in rows:
        COUNT['journals'] += 1
        title = normalize(rec['title'])
        if not title:
            COUNT['no_title'] += 1
            continue
        if title in ambiguous:
            COUNT['ambiguous'] += 1
            continue
        jtype = types.get(title)
        if not jtype:
            COUNT['not_in_airtable'] += 1
            continue
        existing = rec.get('journal_type')
        if existing == jtype:
            COUNT['unchanged'] += 1
            continue
        COUNT['updated' if existing else 'inserted'] += 1
        action = f"Updating (was '{existing}')" if existing else "Inserting"
        print(f"{'' if ARG.WRITE else 'Would be: '}{action} journal_type='{jtype}' for "
              f"{rec['title']} [{rec.get('provider')}]")
        ops.append(UpdateOne({'_id': rec['_id']}, {'$set': {'journal_type': jtype}}))
    if ARG.WRITE and ops:
        try:
            result = DB['dis'].subscription.bulk_write(ops, ordered=False)
        except Exception as err:
            terminate_program(err)
        COUNT['written'] = result.modified_count
    print(f"\nSubscription journals:            {COUNT['journals']:,}\n"
          f"Not in Airtable:                  {COUNT['not_in_airtable']:,}\n"
          f"Skipped, ambiguous type:          {COUNT['ambiguous']:,}\n"
          f"Skipped, no Latin title:          {COUNT['no_title']:,}\n"
          f"journal_type unchanged:           {COUNT['unchanged']:,}\n"
          f"journal_type inserted:            {COUNT['inserted']:,}\n"
          f"journal_type updated:             {COUNT['updated']:,}\n"
          f"Records written:                  {COUNT['written']:,}")


def run():
    ''' Fetch the Airtable data, report on it, and update the subscription collection
        Keyword arguments:
          None
        Returns:
          None
    '''
    global BASEID, TOKEN  # pylint: disable=global-statement
    try:
        BASEID, TOKEN = get_credentials()
        publishers = get_names('Publishers', 'Publisher Name')
        journals = get_names('Journals', 'Journal Name')
        publications = get_records('Publications', ['publisher', 'journal', 'Journal Type'],
                                   'Publications')
    except AirtableError as err:
        terminate_program(str(err))
    pairs = collect_types(publications, publishers, journals)
    bad = report(pairs)
    print(f"\n{len(publications):,} publications read, {COUNT['no_journal']:,} with no journal "
          f"and {COUNT['blank_type']:,} with a blank type ignored, {len(pairs):,} "
          f"publisher/journal pairs, {bad:,} in error")
    update_subscriptions(pairs)
    if bad:
        terminate_program(f"{bad:,} publisher/journal pair(s) do not have exactly one type")

# -----------------------------------------------------------------------------

if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(
        description="Report the journal type of each Publisher/Journal pair in Airtable")
    PARSER.add_argument('--tsv', dest='TSV', action='store_true',
                        default=False, help='Flag, print tab-separated tables')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Flag, write journal_type to the subscription '
                        'collection (otherwise only show what would change)')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, prod)')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    run()
    terminate_program()
