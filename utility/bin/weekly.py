""" weekly.py

    PURPOSE
    -------
    Weekly ingestion-and-enrichment pipeline for Crossref DOIs. Given one DOI or a
    file of DOIs, it adds any that are new to the DIS database, runs the standard
    enrichment steps over the batch (Janelia author assignment, affiliation
    tagging, newsletter dating), generates citations, and emails them to the
    operator. weekly.py is a thin orchestrator: each step is an existing
    utility/sync script run as a subprocess, so this program only decides what to
    run and on which DOIs.

    NEW vs. EXISTING DOIs
    ---------------------
    Every input DOI is looked up in the `dois` collection:
      - not found -> "new": written to new_dois_<timestamp>.txt and, only with
                     --write, also included in the batch that gets enriched.
      - found     -> already in the database: included in the batch as-is.
    So without --write, new DOIs are reported but neither loaded nor enriched;
    with --write they are loaded (update_dois then update_preprints) and then
    enriched alongside the existing ones.

    INPUTS
    ------
    - DIS configuration (JRC.get_config): "databases" (the dis MongoDB manifold,
      opened for write) and "dis" (for the email sender address).
    - Command-line flags:
        --doi DOI    A single DOI to process.
        --file FILE  A text file of DOIs, one per line.
                     (--doi and --file are mutually exclusive; exactly one is
                     required.)
        --manifold   MongoDB manifold, dev or prod [prod].
        --write      Actually write. Passed through to every sub-step except
                     get_citation.py; without it the whole run is a dry run and
                     new DOIs are excluded from enrichment.
        --verbose / --debug  Logging verbosity (also passed to the sub-steps).

    Input DOIs are normalized first: lowercased (DOIs are case-insensitive),
    stripped of blank lines, and de-duplicated while preserving order.

    WORKING DIRECTORY
    -----------------
    Run this from utility/bin: the sub-steps are invoked via relative paths
    (../../sync/bin, ../../utility/bin) and the timestamped output files are
    written to the current directory.

    HIGH-LEVEL FLOW
    ---------------
    1. Read and normalize the input DOIs; split into "new" (absent from `dois`)
       and the batch to process; abort if there is nothing to process.
    2. Write new_dois_<timestamp>.txt (if any new) and all_dois_<timestamp>.txt.
    3. If there are new DOIs, load them: sync/bin/update_dois.py --file
       new_dois_*, then sync/bin/update_preprints.py.
    4. Enrich the batch (all_dois_*), in order:
         - utility/bin/assign_authors.py --auto    (assign Janelia authors)
         - utility/bin/update_tags.py    --auto    (update affiliation tags)
         - utility/bin/add_newsletter.py --ignore  (add the newsletter date)
         - utility/bin/get_citation.py             (generate citations)
    5. If citations were produced, email them (HTML) to the operator
       (<login>@janelia.hhmi.org) and regesters@janelia.hhmi.org.

    OUTPUT
    ------
    - new_dois_<timestamp>.txt and all_dois_<timestamp>.txt in the working
      directory, plus each sub-step's console output.
    - A citations email to the operator and the registration list.

    EXAMPLES
    --------
      # Dry run against prod for one DOI (reports new/existing and runs the
      # sub-steps in dry-run mode; new DOIs are not added)
      weekly.py --doi 10.1234/example

      # Full weekly run from a file, writing to the database
      weekly.py --file this_week.txt --write
"""

__version__ = '3.0.2'

import argparse
from datetime import datetime
from operator import attrgetter
import os
import subprocess
import sys
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Database
DB = {}
# Global
ARG = DIS = LOGGER = None

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
        dbo = attrgetter(f"{source}.{ARG.MANIFOLD}.write")(dbconfig)
        LOGGER.info(f"Connecting to {dbo.name} {ARG.MANIFOLD} on {dbo.host} as {dbo.user}")
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)


def run_subprocess(cmd, file):
    ''' Run a subprocess
        Keyword arguments:
          cmd: list of command and arguments
          file: name of file to process
        Returns:
          None
    '''
    cmd.insert(0, sys.executable)
    if 'get_citation' not in cmd[1]:
        # get_citation.py has none of these parms
        cmd.append("--verbose")
        if ARG.WRITE:
            cmd.append("--write")
        if ARG.DEBUG:
            cmd.append("--debug")
    print(f"{'-'*80}\nRunning {cmd[1]} on {file}\n")
    try:
        proc = subprocess.run(cmd,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT,
                              text=True,
                              check=False)
        exit_code = proc.returncode
        stdout, stderr = proc.stdout, proc.stderr
    except Exception as err:
        terminate_program(err)
    if stdout:
        print(stdout)
    if exit_code:
        terminate_program(f"Error from {cmd[1]}: {stderr}")
    return stdout if stdout else ''


def load_new_dois(file):
    ''' Load new DOIs
        Keyword arguments:
          file: name of file containing new DOIs
        Returns:
          None
    '''
    cmd = ["../../sync/bin/update_dois.py",
            "--file", file,
            "--manifold", ARG.MANIFOLD]
    _ = run_subprocess(cmd, file)
    cmd = ["../../sync/bin/update_preprints.py",
           "--manifold", ARG.MANIFOLD]
    _ = run_subprocess(cmd, '')


def generate_email(citations):
    ''' Generate and send an email to the user that ran the program
        Keyword arguments:
          citations: list of citations
        Returns:
          None
    '''
    msg = JRC.get_run_data(__file__, __version__)
    user = os.getlogin()
    email = [f"{user}@janelia.hhmi.org", "regesters@janelia.hhmi.org"]
    msg += "<br><br>"
    msg += f"<pre>{citations}</pre>"
    try:
        LOGGER.info(f"Sending email to {email}")
        JRC.send_email(msg, DIS['sender'], email, "Citations for DOIs",
                       mime='html')
    except Exception as err:
        LOGGER.error(err)


def doi_processing(file):
    ''' Additional DOI processing
        Keyword arguments:
          file: name of file contining DOIs
          new: list of new DOIs
        Returns:
          None
    '''
    # Assign Janelia authors
    cmd = ["../../utility/bin/assign_authors.py",
            "--file", file, "--auto",
            "--manifold", ARG.MANIFOLD]
    _ = run_subprocess(cmd, file)
    # Update affiliation tags
    cmd = ["../../utility/bin/update_tags.py",
            "--file", file, "--auto",
            "--manifold", ARG.MANIFOLD]
    _ = run_subprocess(cmd, file)
    # Add newsletter date
    cmd = ["../../utility/bin/add_newsletter.py",
            "--file", file,
            "--ignore",
            "--manifold", ARG.MANIFOLD]
    _ = run_subprocess(cmd, file)
    # Generate citations
    cmd = ["../../utility/bin/get_citation.py",
            "--file", file]
    citations = run_subprocess(cmd, file)
    if citations:
        generate_email(citations)


def processing():
    ''' Main processing routine
        Keyword arguments:
          None
        Returns:
          None
    '''
    dois = []
    input_dois = []
    new = []
    if ARG.DOI:
        input_dois = [ARG.DOI]
    elif ARG.FILE:
        with open(ARG.FILE, 'r', encoding='ascii') as stream:
            input_dois = [line.strip() for line in stream.readlines()]
    # DOIs are case-insensitive: lowercase, drop blanks, and de-duplicate
    # (preserving order) so the same DOI isn't checked or written twice.
    input_dois = list(dict.fromkeys(
        d for d in (doi.strip().lower() for doi in input_dois) if d))
    for doi in input_dois:
        if not DL.get_doi_record(doi, DB['dis']['dois']):
            new.append(doi)
            if ARG.WRITE:
                dois.append(doi)
        else:
            dois.append(doi)
    if not dois:
        terminate_program("No DOIs to process")
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    if new:
        new_file = f"new_dois_{timestamp}.txt"
        with open(new_file, 'w', encoding='ascii') as output:
            for doi in new:
                output.write(f"{doi}\n")
    all_file = f"all_dois_{timestamp}.txt"
    with open(all_file, 'w', encoding='ascii') as output:
        for doi in dois:
            output.write(f"{doi}\n")
    print(f"New DOIs:      {len(new)}")
    print(f"Existing DOIs: {len(dois)}")
    if new:
        load_new_dois(new_file)
    doi_processing(all_file)

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Weekly Crossref DOI processing")
    GROUP_A = PARSER.add_mutually_exclusive_group(required=True)
    GROUP_A.add_argument('--doi', dest='DOI', action='store',
                         help='Single DOI to process')
    GROUP_A.add_argument('--file', dest='FILE', action='store',
                         help='Text file of DOIs to process')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, prod)')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Write to database/config system')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    DIS = JRC.simplenamespace_to_dict(JRC.get_config("dis"))
    processing()
    terminate_program()
