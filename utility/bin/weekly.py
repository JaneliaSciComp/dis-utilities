""" weekly.py
    Weekly processing for Crossref DOIs.
    This program:
    - Adds DOI(s) to the database (new DOIs only)
    - Auto-assigns Janelia authors to DOIs
    - Auto-updates affiliation tags
    - Adds newsletter date (for DOIs that don't already have one)
    - Generates citations
    - Sends citations email to the user that ran the program
"""

__version__ = '2.0.0'

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


def generate_email(citations):
    ''' Generate and send an email to the user that ran the program
        Keyword arguments:
          citations: list of citations
        Returns:
          None
    '''
    msg = JRC.get_run_data(__file__, __version__)
    user = os.getlogin()
    email = f"{user}@janelia.hhmi.org"
    msg += "<br><br>"
    msg += f"<pre>{citations}</pre>"
    try:
        LOGGER.info(f"Sending email to {email}")
        JRC.send_email(msg, DIS['sender'], email, "Citations for DOIs", mime='html')
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
