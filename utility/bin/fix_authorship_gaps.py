''' fix_authorship_gaps.py
    Credit authors that a DOI's linked record already credits.

    Two records can describe the same work: a preprint and its published version
    (jrc_preprint), or two versions of one figshare deposit. When one credits an
    author the other does not, the difference is a contradiction rather than a
    guess - the partner record already establishes the person is a Janelian - so
    the fix is to copy the employee ID across.

    The gaps come from doi_common.authorship_gaps, which is also what the
    /dois_authorship_mismatch report displays, so this tool and that page cannot
    disagree about what needs fixing.

    A run summary is emailed to the configured receivers, or to the developer
    alone with --test. A dry run mails the developer whatever the flags, badged
    as a dry run, so the list can be reviewed without troubling anyone else; a
    --doi spot check mails nothing.
'''

__version__ = '1.1.0'

import argparse
import collections
from operator import attrgetter
import os
import sys
from tqdm import tqdm
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL
import jrc_email.jrc_email as JE

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Database
DB = {}
# Counters
COUNT = collections.defaultdict(lambda: 0, {})
# Employee ID -> name, for the summary email
NAMES = {}
# (DOI, names) credited this run, for the summary email
CLOSED = []
# Global variables
ARG = DISCONFIG = LOGGER = None
# DOIs listed individually in the summary email before it defers to the report.
# A full run can close hundreds of gaps, and a mail that long is skimmed, not read.
EMAIL_DOI_LIMIT = 50
# The report showing the same gaps, linked from the email.
REPORT_URL = "https://dis.int.janelia.org/dois_authorship_mismatch"


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
    # authorship_gaps deals in employee IDs, being about which records disagree
    # rather than about people; the email needs names.
    try:
        for row in DB['dis'].orcid.find({"employeeId": {"$exists": True}},
                                        {"employeeId": 1, "given": 1, "family": 1}):
            NAMES[row['employeeId']] = \
                f"{(row.get('given') or [''])[0]} {(row.get('family') or [''])[0]}".strip() \
                or row['employeeId']
    except Exception as err:
        terminate_program(err)


def generate_email(closed):
    ''' Send the run summary. Goes to the configured receivers, or to the
        developer alone with --test. Sent for a dry run too, badged as one, so
        the list can be reviewed before anything is written - a --doi spot check
        sends nothing, matching the other tools.
        Keyword arguments:
          closed: list of (doi, names) actually credited (or that would be)
        Returns:
          None
    '''
    run_data = JRC.get_run_data(__file__, __version__).strip()
    if ARG.RELATION != 'both':
        run_data += f" &middot; relation: {ARG.RELATION}"
    mode_label = 'WRITE' if ARG.WRITE else 'DRY RUN'
    mode_tone = 'good' if ARG.WRITE else 'warn'
    kpis = ''.join([
        JE.kpi_card(f"{COUNT['gaps']:,}", "Gaps found",
                    'good' if COUNT['gaps'] else 'neutral', '25%'),
        JE.kpi_card(f"{COUNT['authors_added']:,}",
                    "Authors credited" if ARG.WRITE else "Authors to credit",
                    'neutral', '25%'),
        JE.kpi_card(f"{COUNT['relation_preprint']:,}", "By preprint", 'neutral', '25%'),
        JE.kpi_card(f"{COUNT['relation_version']:,}", "By version", 'neutral', '25%'),
    ])
    shown = closed[:EMAIL_DOI_LIMIT]
    body = JE.section_header(f"&#128100; Authors credited ({len(closed):,})") \
           + JE.doi_card("Credited from a linked record", shown, 'good',
                         second_header="Authors")
    if len(closed) > len(shown):
        body += f'<div style="font-size:13px;padding-top:6px;">' \
                + f'{len(closed) - len(shown):,} more not listed &middot; ' \
                + f'<a href="{REPORT_URL}">see the full report</a></div>'
    msg = JE.render(os.path.basename(__file__), __version__, run_data,
                    mode_label, mode_tone, kpis, JE.body_row(body))
    # A dry run goes to the developer whatever the flags: it is exploratory, and
    # nothing has changed for the receivers to hear about. --test then matters
    # for a real run, where it holds the mail back to the developer as well.
    email = DISCONFIG['developer'] if (ARG.TEST or not ARG.WRITE) \
            else DISCONFIG['receivers']
    try:
        LOGGER.info(f"Sending email to {email}")
        JRC.send_email(msg, DISCONFIG['sender'], email,
                       "Authorship gaps closed", mime='html')
    except Exception as err:
        LOGGER.error(f"Could not send email: {err}")


def update_first_last(doi):
    ''' Refresh a DOI's first/last author fields after crediting someone new.
        Adding an author can change who the first or last Janelia author is, so
        the fields are recomputed rather than left stale.
        The payload is only applied when it sets something. get_first_last_author_payload
        returns an $unset when it finds no in-database authors, which would clear
        fields this program has just given a reason to be populated - that can only
        mean the lookup failed, so it is reported and skipped.
        Keyword arguments:
          doi: DOI
        Returns:
          None
    '''
    try:
        payload = DL.get_first_last_author_payload(doi)
    except Exception as err:
        LOGGER.warning(f"Could not get first/last authors for {doi}: {err}")
        COUNT['firstlast_error'] += 1
        return
    if '$set' not in payload:
        LOGGER.warning(f"Skipping first/last update for {doi}: lookup returned no authors")
        COUNT['firstlast_empty'] += 1
        return
    try:
        DB['dis'].dois.update_one({"doi": doi}, payload)
    except Exception as err:
        LOGGER.error(f"Could not update first/last authors for {doi}: {err}")
        COUNT['firstlast_error'] += 1
        return
    COUNT['firstlast_updated'] += 1


def apply_gap(gap):
    ''' Credit the authors a DOI's linked record already credits.
        $addToSet, never a recomputed list: doi_common's update_jrc_author_from_doi
        derives jrc_author from the DOI's own record and replaces it wholesale, which
        is exactly wrong here - that record is what fails to name the person - and
        would drop anyone its matching could not confirm.
        Keyword arguments:
          gap: entry from DL.authorship_gaps
        Returns:
          None
    '''
    doi = gap['doi']
    LOGGER.info(f"{doi}: adding {', '.join(gap['missing'])} "
                f"(credited by {', '.join(gap['partners'])})")
    COUNT['authors_added'] += len(gap['missing'])
    names = ', '.join(NAMES.get(eid, eid) for eid in gap['missing'])
    if not ARG.WRITE:
        CLOSED.append((doi, names))
        return
    try:
        resp = DB['dis'].dois.update_one({"doi": doi},
                                         {"$addToSet": {"jrc_author":
                                                        {"$each": gap['missing']}}})
    except Exception as err:
        LOGGER.error(f"Could not update {doi}: {err}")
        COUNT['write_error'] += 1
        return
    if not resp.modified_count:
        # Nothing changed, so the DOI already had them - the gap was closed
        # between the scan and now. Do not log a processing event for a no-op.
        COUNT['already_current'] += 1
        return
    COUNT['dois_updated'] += 1
    CLOSED.append((doi, names))
    update_first_last(doi)
    try:
        DL.add_doi_process(doi, action='credit_author', coll=DB['dis'].processing,
                           notes=f"Credited {', '.join(gap['missing'])} from "
                                 f"{gap['relation']} {', '.join(gap['partners'])}")
    except Exception as err:
        LOGGER.error(f"Could not log a processing event for {doi}: {err}")
        COUNT['processing_error'] += 1


def processing():
    ''' Find and optionally close authorship gaps
        Keyword arguments:
          None
        Returns:
          None
    '''
    try:
        gaps = DL.authorship_gaps(DB['dis'].dois, ARG.RELATION)
    except Exception as err:
        terminate_program(err)
    if ARG.DOI:
        doi = ARG.DOI.lower().strip()
        gaps = [g for g in gaps if g['doi'] == doi]
        if not gaps:
            terminate_program(f"{doi} has no authorship gap to close")
    COUNT['gaps'] = len(gaps)
    for gap in tqdm(gaps, desc="Closing authorship gaps"):
        COUNT[f"relation_{gap['relation']}"] += 1
        apply_gap(gap)
    print(f"Gaps found:                {COUNT['gaps']:,}")
    for rel in ('preprint', 'version'):
        if COUNT[f"relation_{rel}"]:
            print(f"  by {rel + ' relation:':<22}{COUNT[f'relation_{rel}']:,}")
    # Kept inside the 27-column field the other lines use; the dry-run warning
    # below already says nothing was written.
    label = "Authors credited:" if ARG.WRITE else "Authors to credit:"
    print(f"{label:<27}{COUNT['authors_added']:,}")
    if ARG.WRITE:
        print(f"DOIs updated:              {COUNT['dois_updated']:,}")
        print(f"DOIs already current:      {COUNT['already_current']:,}")
        print(f"First/last authors reset:  {COUNT['firstlast_updated']:,}")
        for key, text in (('firstlast_empty', 'First/last lookup empty:'),
                          ('firstlast_error', 'First/last errors:'),
                          ('write_error', 'Write errors:'),
                          ('processing_error', 'Processing events failed:')):
            if COUNT[key]:
                print(f"{text:<27}{COUNT[key]:,}")
    else:
        LOGGER.warning("Dry run successful, no updates were made")
    # A --doi run is a spot check, so it reports to the terminal only - the same
    # exemption the acknowledgement tools make.
    if ARG.DOI:
        LOGGER.info("Single-DOI run (--doi): not sending summary email")
    elif CLOSED:
        generate_email(CLOSED)


# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Credit authors that a DOI's linked record already credits")
    PARSER.add_argument('--doi', dest='DOI', action='store',
                        help='Single DOI to fix (default: every gap found)')
    PARSER.add_argument('--relation', dest='RELATION', action='store', default='both',
                        choices=['preprint', 'version', 'both'],
                        help='Which linkage to act on (default: both)')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, prod)')
    PARSER.add_argument('--test', dest='TEST', action='store_true',
                        default=False, help='Flag, Send the summary email to the '
                                            'developer only')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Flag, Update database')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    DISCONFIG = JRC.simplenamespace_to_dict(JRC.get_config("dis"))
    LOGGER.info(f"Started run (version {__version__})")
    initialize_program()
    processing()
    terminate_program()
