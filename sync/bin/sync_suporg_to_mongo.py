''' sync_suporg_to_org_group.py
    Update the MongoDB suporg collection with data from the People system.
'''

__version__ = '1.0.1'

import argparse
import collections
import html
from operator import attrgetter
import os
import sys
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL
import jrc_email.jrc_email as JE

# pylint: disable=broad-exception-caught,logging-fstring-interpolation,logging-not-lazy

# Database
DB = {}
# Global variables
ARG = DIS = LOGGER = None
IGNORE = {}
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
    ''' Initialize database connection
        Keyword arguments:
          None
        Returns:
          None
    '''
    if "PEOPLE_API_KEY" not in os.environ:
        terminate_program("Missing token - set in PEOPLE_API_KEY environment variable")
    try:
        dbconfig = JRC.get_config("databases")
    except Exception as err:
        terminate_program(err)
    dbs = ['dis']
    for source in dbs:
        dbo = attrgetter(f"{source}.{ARG.MANIFOLD}.write")(dbconfig)
        LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, ARG.MANIFOLD, dbo.host, dbo.user)
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)
    try:
        rows = DB['dis']['to_ignore'].find({"type": "group"})
        for row in rows:
            IGNORE[row['key']] = True
    except Exception as err:
        terminate_program(err)


def html_suporg_table(entries):
    ''' Build a zebra-striped Name/Code/Active table for added suporgs.
        Deliberately no per-cell border-radius - plain background-color
        striping is the pattern that survives Outlook's Word rendering engine
        (see the equivalent list tables in add_people_to_orcid.py).
        Keyword arguments:
          entries: list of {name, code, active} dicts
        Returns:
          HTML table
    '''
    rows = []
    for i, entry in enumerate(entries):
        striped = i % 2 == 0
        bgattr = f' bgcolor="{JE.STRIPE_BG}"' if striped else ''
        bg = f'background-color:{JE.STRIPE_BG};' if striped else ''
        name = html.escape(entry['name'])
        code = html.escape(str(entry['code']))
        badge = JE.pill(JE.GREEN_BG, JE.GREEN, '&#10003; Active') if entry['active'] else ''
        rows.append(
            f'<tr{bgattr} style="{bg}">'
            f'<td style="padding:8px 10px;">{name}</td>'
            f'<td style="padding:8px 10px;color:{JE.GRAY};">{code}</td>'
            f'<td style="padding:8px 10px;" align="right">{badge}</td></tr>')
    rows.append('<tr><td colspan="3" style="height:1px;line-height:1px;font-size:1px;">'
                '&nbsp;</td></tr>')
    return ('<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
            'style="border-collapse:collapse;font-size:12.5px;">'
            + "".join(rows) + '</table>')


def generate_email(people_count, mongo_count, added):
    ''' Generate and send the HTML run-summary email for suporgs added to
        MongoDB from the People system.
        Keyword arguments:
          people_count: number of suporgs found in People
          mongo_count: number of suporgs already in MongoDB
          added: list of {name, code, active} dicts for suporgs added
        Returns:
          None
    '''
    run_data = (JRC.get_run_data(__file__, __version__).strip()
                + f" &middot; manifold: {ARG.MANIFOLD}")
    mode_label = 'WRITE' if ARG.WRITE else 'DRY RUN'
    mode_tone = 'good' if ARG.WRITE else 'warn'
    kpis = ''.join([
        JE.kpi_card(f"{people_count:,}", "Suporgs in People", width='33%'),
        JE.kpi_card(f"{mongo_count:,}", "Suporgs in MongoDB", width='33%'),
        JE.kpi_card(f"{len(added):,}", "Suporgs added", 'good', width='33%'),
    ])
    body = JE.body_row(JE.section_header(f"&#127970; Suporgs Added ({len(added):,})")
                       + html_suporg_table(added))
    msg = JE.render(os.path.basename(__file__), __version__, run_data,
                    mode_label, mode_tone, kpis, body)
    email = DIS['developer'] if ARG.TEST else DIS['receivers']
    try:
        LOGGER.info(f"Sending email to {email}")
        JRC.send_email(msg, DIS['sender'], email, 'Suporgs added', mime='html')
    except Exception as err:
        LOGGER.error(err)


def update_suporgs():
    ''' Update supervisory organizations
        Keyword arguments:
          None
        Returns:
          None
    '''
    people = DL.get_supervisory_orgs(full=True)
    mongo = DL.get_supervisory_orgs(coll=DB['dis'].suporg)
    added = []
    for suporg, val in people.items():
        if suporg not in mongo and suporg not in IGNORE:
            LOGGER.info(f"Adding {suporg} with code {val['SUPORGCODE']}")
            active = bool(val.get('active'))
            payload = {'name': suporg, 'code': val['SUPORGCODE']}
            if active:
                payload['active'] = True
            if ARG.WRITE:
                DB['dis'].suporg.insert_one(payload)
            added.append({'name': suporg, 'code': val['SUPORGCODE'], 'active': active})
            COUNT['added'] += 1
    print(f"Suporgs in People:  {len(people):,}")
    print(f"Suporgs in MongoDB: {len(mongo):,}")
    print(f"Suporgs added:      {COUNT['added']:,}")
    if (not added) or (not (ARG.TEST or ARG.WRITE)):
        return
    generate_email(len(people), len(mongo), added)

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Sync People to MongoDB:suporg")
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                      default='prod', choices=['dev', 'prod'],
                      help='MongoDB manifold (dev, prod)')
    PARSER.add_argument('--test', dest='TEST', action='store_true',
                        default=False, help='Send email to developer')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Write to database')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    try:
        DIS = JRC.simplenamespace_to_dict(JRC.get_config("dis"))
    except Exception as err:
        terminate_program(err)
    initialize_program()
    update_suporgs()
    terminate_program()
