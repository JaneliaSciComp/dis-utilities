''' remove_misattributed_credits.py
    Remove a jrc_author credit from DOIs whose author list never names that person.

    A Janelia credit is an assertion that a particular employee wrote a particular
    paper. When the roster holds two people with the same surname and only one of
    them has an employee ID, that assertion can land on the wrong one: every DOI
    crediting Laurel Royer (62941) lists Loic Royer, whose roster record carries an
    ORCID but no employee ID, so a credit on his work had nowhere correct to go.

    This tool takes an employee ID, finds every DOI crediting it, and reports the
    ones where no deposited author carries that person's name - checking the linked
    preprint/published record too, since authors are routinely added between a
    preprint and its published version and that credit is legitimate. It refuses to
    touch anything that matches.

    A name is compared case- and accent-folded, allowing initials ("J. N. Etheredge"
    for Jack Etheredge), the "Family, Given" and "Given Family" forms that arrive in
    an unsplit DataCite name field, and any given-name variant on the roster record.
    Deposits authored by a group ("CellMap Project Team") name no individual at all,
    so a credit on one of those is left alone rather than guessed at.

    Dry run by default; --write removes the credits and logs a processing event on
    each DOI so the removal is traceable.
'''

__version__ = '1.0.0'

import argparse
import collections
from operator import attrgetter
import sys
import unicodedata
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

ARG = LOGGER = None
# Database
DB = {}
# Counters
COUNT = collections.defaultdict(lambda: 0, {})


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
    dbo = attrgetter(f"dis.{ARG.MANIFOLD}.{'write' if ARG.WRITE else 'read'}")(dbconfig)
    LOGGER.info(f"Connecting to {dbo.name} {ARG.MANIFOLD} on {dbo.host} as {dbo.user}")
    try:
        DB['dis'] = JRC.connect_database(dbo)
    except Exception as err:
        terminate_program(err)


def fold(text):
    ''' Reduce a name to a comparable form: tidied, lowercased, accents stripped.
        Keyword arguments:
          text: a name, or part of one
        Returns:
          Folded string
    '''
    text = unicodedata.normalize('NFKD', DL.tidy_name(str(text or '')).lower())
    return ''.join(c for c in text if not unicodedata.combining(c))


def person_keys(rec):
    ''' Build the name keys a deposited author would have to carry to be this person.
        Keyword arguments:
          rec: record from the orcid collection
        Returns:
          dict of family names, given names, given initials, and ORCIDs
    '''
    given = {fold(g) for g in rec.get('given') or [] if fold(g)}
    return {'family': {fold(f) for f in rec.get('family') or [] if fold(f)},
            'given': given,
            'initial': {g[0] for g in given if g},
            'orcid': {rec['orcid']} if rec.get('orcid') else set()}


def names_on(row):
    ''' Every author on a DOI record, reduced to comparable parts.
        Keyword arguments:
          row: row from the dois collection
        Returns:
          List of (family, given, whole name, ORCID) tuples
    '''
    out = []
    for auth in row.get('creators') or row.get('author') or []:
        out.append((fold(auth.get('family') or auth.get('familyName') or ''),
                    fold(auth.get('given') or auth.get('givenName') or ''),
                    fold(auth.get('name') or ''), author_orcid(auth)))
    return out


def author_orcid(auth):
    ''' Pull an ORCID off an author entry, Crossref or DataCite.
        Keyword arguments:
          auth: author/creator entry
        Returns:
          Bare ORCID string, or ''
    '''
    if auth.get('ORCID'):
        return str(auth['ORCID']).rsplit('/', maxsplit=1)[-1]
    for nid in auth.get('nameIdentifiers') or []:
        if nid.get('nameIdentifierScheme') == 'ORCID' and nid.get('nameIdentifier'):
            return str(nid['nameIdentifier']).rsplit('/', maxsplit=1)[-1]
    return ''


def given_matches(deposited, keys):
    ''' Does a deposited given name denote this person? Accepts a full variant, or an
        initial standing in for one ("J. N." for Jack), which is how a great many
        journals deposit names.
        Keyword arguments:
          deposited: folded given name from the record
          keys: person_keys dict
        Returns:
          True or False
    '''
    if not deposited:
        return False
    if deposited in keys['given']:
        return True
    first = deposited.split(' ')[0].rstrip('.')
    if first in keys['given']:
        return True
    # An initial matches only if that is all the publisher gave us
    return len(first) == 1 and first in keys['initial']


def credits_person(row, keys):
    ''' Does any author on this record carry this person's name (or ORCID)?
        Keyword arguments:
          row: row from the dois collection
          keys: person_keys dict
        Returns:
          The matching author string, or None
    '''
    for auth in row.get('creators') or row.get('author') or []:
        oid = author_orcid(auth)
        if oid and oid in keys['orcid']:
            return f"ORCID {oid}"
        family = fold(auth.get('family') or auth.get('familyName') or '')
        given = fold(auth.get('given') or auth.get('givenName') or '')
        if family in keys['family'] and given_matches(given, keys):
            return f"{given} {family}".strip()
        # Unsplit deposits: one string holding the whole name, either order
        whole = fold(auth.get('name') or '')
        for candidate in (whole, family):
            if not candidate:
                continue
            for fam in keys['family']:
                for giv in keys['given']:
                    if candidate in (f"{giv} {fam}", f"{fam}, {giv}"):
                        return candidate
    return None


def group_authored(row):
    ''' Is this deposit credited to a team rather than to named people? Janelia's
        figshare datasets are routinely authored by "CellMap Project Team" and the
        like, and a person credited on one of those cannot be matched by name - that
        is not evidence the credit is wrong.
        Keyword arguments:
          row: row from the dois collection
        Returns:
          True or False
    '''
    markers = ('team', 'group', 'consortium', 'project', 'laboratory', 'collaboration')
    for auth in row.get('creators') or row.get('author') or []:
        whole = fold(auth.get('name') or '') or fold(auth.get('family')
                                                    or auth.get('familyName') or '')
        if any(marker in whole for marker in markers):
            return True
    return False


def report_and_remove(eid, rec):
    ''' Find, report and optionally remove this person's unsupported credits.
        Keyword arguments:
          eid: employee ID
          rec: that person's record from the orcid collection
        Returns:
          None
    '''
    keys = person_keys(rec)
    name = f"{(rec.get('given') or [''])[0]} {(rec.get('family') or [''])[0]}".strip()
    try:
        rows = list(DB['dis'].dois.find({"jrc_author": eid}))
    except Exception as err:
        terminate_program(err)
    LOGGER.info(f"{name} ({eid}) is credited on {len(rows)} DOIs")
    doomed = []
    for row in rows:
        COUNT['examined'] += 1
        found = credits_person(row, keys)
        if found:
            COUNT['named'] += 1
            if ARG.VERBOSE:
                LOGGER.info(f"  {row['doi']}: keeping, deposit names {found}")
            continue
        if group_authored(row):
            COUNT['group'] += 1
            LOGGER.info(f"  {row['doi']}: keeping, deposit is group-authored")
            continue
        # Authors are added between a preprint and its published version, so the
        # partner record is the other place a good credit can come from.
        partner = None
        for pdoi in row.get('jrc_preprint') or []:
            prow = DB['dis'].dois.find_one({"doi": pdoi})
            if prow and credits_person(prow, keys):
                partner = pdoi
                break
        if partner:
            COUNT['via_partner'] += 1
            LOGGER.info(f"  {row['doi']}: keeping, named on linked {partner}")
            continue
        COUNT['unsupported'] += 1
        doomed.append(row)
    if not doomed:
        LOGGER.info("No unsupported credits found")
        return
    remove(eid, name, doomed)


def remove(eid, name, doomed):
    ''' Report the unsupported credits and, with --write, remove them.
        Keyword arguments:
          eid: employee ID
          name: that person's name, for the report and the processing note
          doomed: rows whose credit is unsupported
        Returns:
          None
    '''
    print(f"\n{len(doomed)} DOIs credit {name} ({eid}) but name no such author:")
    for row in doomed:
        authors = ', '.join(f"{g} {f}".strip() for f, g, _, _ in names_on(row)) or '(none)'
        print(f"  {row['doi']:<36} {str(DL.get_publishing_date(row))[:10]}  {authors[:92]}")
    if not ARG.WRITE:
        print(f"\nDry run: nothing changed. Re-run with --write to remove {len(doomed)} "
              "credits.")
        return
    for row in doomed:
        try:
            result = DB['dis'].dois.update_one({"doi": row['doi']},
                                               {"$pull": {"jrc_author": eid}})
            if not result.modified_count:
                LOGGER.warning(f"{row['doi']}: no change written")
                COUNT['not_written'] += 1
                continue
            COUNT['removed'] += 1
            # A DOI with no Janelia author normally has no jrc_author field at all
            # (965 of them), not an empty array (5). Removing the last credit should
            # leave the record looking like the former.
            if len(row.get('jrc_author') or []) == 1:
                DB['dis'].dois.update_one({"doi": row['doi']},
                                          {"$unset": {"jrc_author": ""}})
                COUNT['emptied'] += 1
        except Exception as err:
            LOGGER.error(f"Could not update {row['doi']}: {err}")
            COUNT['write_error'] += 1
            continue
        try:
            DL.add_doi_process(row['doi'], action='remove_credit',
                               coll=DB['dis'].processing,
                               notes=f"Removed credit for {name}; no author on this "
                                     "record carries that name")
        except Exception as err:
            LOGGER.error(f"Could not log a processing event for {row['doi']}: {err}")
            COUNT['processing_error'] += 1


def processing():
    ''' Main routine
        Keyword arguments:
          None
        Returns:
          None
    '''
    try:
        rec = DB['dis'].orcid.find_one({"employeeId": ARG.EMPLOYEE})
    except Exception as err:
        terminate_program(err)
    if not rec:
        terminate_program(f"No orcid record has employeeId {ARG.EMPLOYEE}")
    report_and_remove(ARG.EMPLOYEE, rec)
    print()
    for key in sorted(COUNT):
        print(f"{key + ':':<20} {COUNT[key]:,}")


# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Remove jrc_author credits that a DOI's author list does not support")
    PARSER.add_argument('--employee', dest='EMPLOYEE', action='store', required=True,
                        help='Employee ID whose credits are to be checked')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, prod)')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Flag, Update database')
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
