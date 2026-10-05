""" find_unloaded_relations.py
    Find referenced DOIs that have not been loaded.

    Registrar metadata records relations between DOIs. Where one end names a
    DOI the collection does not hold, that DOI is a candidate for loading, so
    the list is written to unloaded_relations.txt for update_dois.py --file.

    This is the only place such a gap is visible. The derived relation fields
    (jrc_preprint, jrc_dataset_supplement) are built only when both ends are
    already held, so /relation_integrity cannot report a DOI that was never
    loaded - it reports the complementary case, a derived relation pointing at
    something absent.

    A DOI already settled elsewhere is not a gap, so targets on the ignore
    list (to_ignore, type "doi") and those held in external_dois are skipped
    and counted rather than reported.

    An HTML summary email is sent with --test (developer only) or --write
    (the receivers list). The text file is written either way, since the
    scheduled run feeds update_dois.py from it.
"""

__version__ = '1.2.0'

import argparse
import collections
import html
from operator import attrgetter
import os
import re
import sys
import traceback
import jrc_common.jrc_common as JRC
import jrc_email.jrc_email as JE

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Database
DB = {}
# Counters
COUNT = collections.defaultdict(lambda: 0, {})
# Global variables
ARG = DISCONFIG = LOGGER = None
# References
REFERENCES = ("has-preprint", "is-preprint-of", "is-supplement-to", "is-supplemented-by")
# Output file, consumed by update_dois.py --file
OUTPUT_FILE = "unloaded_relations.txt"
# A DOI is a "10." prefix, a slash, and a non-empty suffix. Registrars do
# mislabel other identifiers as id-type "doi" - 10.1107/s2052252518017621
# offers "10.1107/S2052252518017621/fq5004sup1.pdf", a supplementary file
# path - and an entry like that can never resolve, so it must not reach the
# loader's input file.
DOI_SHAPE = re.compile(r"^10\.\d{4,9}/\S+$")
# Suffixes that mark a deposited file rather than a registered DOI.
FILE_SUFFIXES = ('.pdf', '.doc', '.docx', '.xls', '.xlsx', '.csv', '.tsv', '.txt',
                 '.zip', '.gz', '.tar', '.jpg', '.jpeg', '.png', '.tif', '.tiff',
                 '.mov', '.mp4', '.avi', '.xml', '.json')


def terminate_program(msg=None):
    ''' Terminate the program gracefully
        Keyword arguments:
          msg: error message
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
        dbo = attrgetter(f"{source}.{ARG.MANIFOLD}.read")(dbconfig)
        LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, ARG.MANIFOLD, dbo.host, dbo.user)
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)


def is_doi(identifier):
    ''' Test whether a registrar-supplied identifier is really a DOI.

        id-type is taken from the deposit and is not always right: a
        supplementary file path carrying the article's DOI as a prefix is
        still deposited as id-type "doi". Loading one is guaranteed to fail,
        so the shape is checked rather than the label.
        Keyword arguments:
          identifier: the id value from a relation entry
        Returns:
          True if it looks like a DOI
    '''
    if not identifier or not DOI_SHAPE.match(identifier.strip()):
        return False
    return not identifier.strip().lower().endswith(FILE_SUFFIXES)


def generate_email(unloaded, rejected):
    ''' Generate and send the HTML run-summary email (jrc_email house style): a
        header banner, a KPI stat-tile row, an "Unloaded Relations" card, and -
        when any - a card for identifiers that were not DOIs. Recipient is the
        developer for --test and the receivers list otherwise.
        Keyword arguments:
          unloaded: list of referenced DOIs not in the collection
          rejected: list of (referring DOI, identifier) that were not DOIs
        Returns:
          None
    '''
    if not unloaded and not rejected:
        return
    run_data = JRC.get_run_data(__file__, __version__).strip()
    mode_label = 'TEST' if ARG.TEST else 'LIVE'
    mode_tone = 'warn' if ARG.TEST else 'good'
    kpis = ''.join([
        JE.kpi_card(f"{COUNT['loaded']:,}", "DOIs in database"),
        JE.kpi_card(f"{COUNT['with_relations']:,}", "With relations"),
        JE.kpi_card(f"{COUNT['ignored'] + COUNT['external']:,}", "Already settled"),
        JE.kpi_card(f"{len(rejected):,}", "Not a DOI",
                    'warn' if rejected else 'neutral'),
        JE.kpi_card(f"{len(unloaded):,}", "Unloaded",
                    'good' if unloaded else 'neutral'),
    ])
    unloaded_body = (JE.doi_card("Unloaded Relations", [(d, None) for d in unloaded], 'good')
                     if unloaded else
                     f'<div style="color:{JE.GRAY};font-size:13px;">'
                     'No referenced DOIs are missing from the collection.</div>')
    body = JE.body_row(JE.section_header(f"&#10003; Unloaded Relations ({len(unloaded):,})")
                       + unloaded_body)
    if rejected:
        # Escaped because this is the one list built from identifiers the
        # registrar got wrong - a malformed id is exactly where an angle
        # bracket turns up, and it lands in an HTML email unaltered.
        rows = ''.join(
            f'<div style="font-size:12px;margin:2px 0;">'
            f'<code>{html.escape(str(ident))}</code>'
            f'<span style="color:{JE.GRAY};"> &mdash; referenced by '
            f'{html.escape(str(src))}</span></div>'
            for src, ident in rejected)
        body += JE.body_row(
            JE.section_header(f"&#9888; Deposited as a DOI, but is not one ({len(rejected):,})")
            + f'<div style="color:{JE.GRAY};font-size:12px;margin:-4px 0 10px 0;">'
            'The registrar labelled these id-type "doi". They cannot resolve and are '
            'kept out of the loader\'s input file. Worth reporting to the publisher.'
            '</div>' + rows, '6px 28px 4px 28px')
    msg = JE.render(os.path.basename(__file__), __version__, run_data,
                    mode_label, mode_tone, kpis, body)
    try:
        email = DISCONFIG['developer'] if ARG.TEST else DISCONFIG['receivers']
        LOGGER.info(f"Sending email to {email}")
        JRC.send_email(msg, DISCONFIG['sender'], email, "Unloaded DOI relations",
                       mime='html')
    except Exception as err:
        print(str(err))
        traceback.print_exc()
        terminate_program(err)


def settled_dois():
    ''' DOIs that are not gaps, whatever a relation says about them.

        to_ignore (type "doi") records a curator's decision not to load one;
        external_dois holds those tracked without being curated. Reporting
        either would hand update_dois.py something it should not load, and
        bury the real candidates in the noise.
        Keyword arguments:
          None
        Returns:
          (ignored, external) sets of lowercased DOIs
    '''
    try:
        ignored = {row['key'].lower() for row
                   in DB['dis'].to_ignore.find({"type": "doi"}, {"key": 1})
                   if row.get('key')}
        external = {row['doi'].lower() for row
                    in DB['dis'].external_dois.find({}, {"doi": 1})
                    if row.get('doi')}
    except Exception as err:
        terminate_program(err)
    LOGGER.info(f"Ignored DOIs: {len(ignored):,}   External DOIs: {len(external):,}")
    return ignored, external


def scan_relations(coll, loaded, ignored, external):
    ''' Walk every record carrying registrar relations and collect the DOIs
        they name that the collection does not hold.
        Keyword arguments:
          coll: dois collection
          loaded: DOIs held in the collection
          ignored: DOIs on the ignore list
          external: DOIs held in external_dois
        Returns:
          (unloaded, rejected) - a dict of DOIs to load, and a list of
          (referring DOI, identifier) pairs that were not DOIs at all
    '''
    unloaded, rejected = {}, []
    try:
        rows = coll.find({"relation": {"$exists": True}}, {"doi": 1, "relation": 1})
    except Exception as err:
        terminate_program(err)
    for row in rows:
        COUNT['with_relations'] += 1
        for rel, items in row['relation'].items():
            if rel not in REFERENCES:
                continue
            for itm in items:
                if itm.get('id-type') != 'doi':
                    continue
                if not is_doi(itm.get('id')):
                    LOGGER.warning(f"{row['doi']} references a non-DOI: {itm.get('id')}")
                    rejected.append((row['doi'], itm.get('id')))
                    COUNT['rejected'] += 1
                    continue
                # Stripped as well as lowercased: is_doi() validates the
                # stripped form, so a padded id would otherwise pass the shape
                # check and then match nothing, reaching the loader's file with
                # its whitespace intact.
                target = itm['id'].strip().lower()
                if target in loaded:
                    continue
                if target in ignored:
                    COUNT['ignored'] += 1
                    continue
                if target in external:
                    COUNT['external'] += 1
                    continue
                print(f"{row['doi']} {itm['id']}")
                unloaded[target] = True
    return unloaded, rejected


def processing():
    ''' Main processing routine
        Keyword arguments:
          None
        Returns:
          None
    '''
    coll = DB['dis'].dois
    loaded_dois = {}
    LOGGER.info("Finding DOIs")
    try:
        rows = coll.find({}, {"doi": 1})
    except Exception as err:
        terminate_program(err)
    for row in rows:
        loaded_dois[row['doi']] = True
    COUNT['loaded'] = len(loaded_dois)
    LOGGER.info(f"Loaded DOIs: {len(loaded_dois):,}")
    ignored, external = settled_dois()
    LOGGER.info("Finding unloaded supplements")
    unloaded, rejected = scan_relations(coll, loaded_dois, ignored, external)
    # Written on every run, including when there is nothing to load, and never
    # gated behind --write. The scheduled run feeds update_dois.py from this
    # file: skipping the write on an empty result leaves the previous run's
    # list in place to be loaded a second time, and gating it behind a flag
    # would break the chain with no error to notice.
    LOGGER.info(f"Writing {len(unloaded):,} DOI(s) to {OUTPUT_FILE}")
    try:
        with open(OUTPUT_FILE, "w", encoding="ascii") as file:
            for doi in unloaded:
                file.write(f"{doi}\n")
    except OSError as err:
        terminate_program(err)
    summary = (
        f"DOIs in database:          {COUNT['loaded']:,}\n"
        f"DOIs with relations:       {COUNT['with_relations']:,}\n"
        f"Identifiers not a DOI:     {COUNT['rejected']:,}\n"
        f"Skipped (on ignore list):  {COUNT['ignored']:,}\n"
        f"Skipped (external DOI):    {COUNT['external']:,}\n"
        f"Unloaded relations:        {len(unloaded):,}"
    )
    print(summary)
    if ARG.TEST or ARG.WRITE:
        generate_email(sorted(unloaded), rejected)


# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Find referenced DOIs that have not been loaded")
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, prod)')
    PARSER.add_argument('--test', dest='TEST', action='store_true',
                        default=False, help='Send email to developer only')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Send email to receivers')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    LOGGER.info(f"Started run (version {__version__})")
    try:
        DISCONFIG = JRC.simplenamespace_to_dict(JRC.get_config("dis"))
    except Exception as err:
        terminate_program(err)
    initialize_program()
    processing()
    terminate_program()
