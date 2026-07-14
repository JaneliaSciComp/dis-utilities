''' email_authors.py
    Email information on newly-added DOIs to authors
'''

__version__ = '1.7.0'

import argparse
from datetime import datetime, timedelta
from operator import attrgetter
import os
import sys
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL

# pylint: disable=broad-exception-caught,logging-fstring-interpolation,logging-not-lazy

ARG = DISCONFIG = LOGGER = None
# Database
DB = {}
# DOI-level data
AUTHORLIST = {}
TAGLIST = {}
# Per-run caches for external lookups, keyed by employeeId
PEOPLE_CACHE = {}
ORCID_CACHE = {}
# doi_common author-detail match type -> display label/color for the staff email
MATCH_LABEL = {'asserted': 'Affiliation', 'ORCID': 'ORCID', 'name': 'Name',
               'jrc_author': 'Author list'}
MATCH_COLOR = {'asserted': 'limegreen', 'ORCID': 'limegreen', 'name': 'crimson',
               'jrc_author': 'dodgerblue'}
# Inline CSS for the staff-email author match table
TH_L = "style='text-align:left; padding:4px 8px; border-bottom:2px solid #888;'"
TH_C = "style='text-align:center; padding:4px 8px; border-bottom:2px solid #888;'"
TD_L = "style='padding:4px 8px; border-bottom:1px solid #ddd; vertical-align:top;'"
TD_C = "style='padding:4px 8px; border-bottom:1px solid #ddd; vertical-align:top; " \
       "text-align:center;'"
BADGE_RED = "background-color:#c0392b; color:#fff; padding:2px 8px; " \
            "border-radius:10px; font-size:12px; white-space:nowrap;"
BADGE_ORANGE = "background-color:orange; color:#fff; padding:2px 8px; " \
               "border-radius:10px; font-size:12px; white-space:nowrap;"
# Subject line for the DIS staff summary email
STAFF_SUBJECT = "Emails have been sent to authors for recent publications"
# F/L author-role circle badge for the staff-email author table
AUTHOR_CIRCLE = "display:inline-block; width:16px; height:16px; line-height:16px; " \
                "border-radius:50%; background-color:#5b9bd5; color:#fff; " \
                "text-align:center; font-size:11px; font-weight:bold; margin-left:4px;"

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
    dbs = ['dis']
    for source in dbs:
        dbo = attrgetter(f"{source}.{ARG.MANIFOLD}.write")(dbconfig)
        LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, ARG.MANIFOLD, dbo.host, dbo.user)
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)


def get_citation(row):
    ''' Create a citation for a DOI
        Keyword arguments:
          row: row from the dois collection
        Returns:
          DIS-style citation
    '''

    authors = DL.get_author_list(row)
    title = DL.get_title(row)
    citation = f"{authors} {title}."
    year = str(row['jrc_publishing_date'])[:4] if row.get('jrc_publishing_date') else ''
    meta = ", ".join(filter(None, [row.get('jrc_journal'), year]))
    if meta:
        citation += f" {meta}."
    return citation + f" https://doi.org/{row['doi']}."


def doi_type_display(row):
    ''' Build a human-readable type/subtype string for a DOI
        Keyword arguments:
          row: row from the dois collection
        Returns:
          "type / subtype" style string, or "" if no type is present
    '''
    parts = []
    if row.get('jrc_obtained_from') == 'DataCite':
        types = row.get('types', {})
        general = types.get('resourceTypeGeneral')
        specific = types.get('resourceType')
        if general:
            parts.append(general)
        if specific and specific != general:
            parts.append(specific)
    else:
        # Crossref
        if row.get('type'):
            parts.append(row['type'])
        if row.get('subtype'):
            parts.append(row['subtype'])
    return " / ".join(parts)


def related_publication(row):
    ''' Preprint/published counterpart DOIs for a row, from jrc_preprint
        Keyword arguments:
          row: row from the dois collection
        Returns:
          Tuple (label, [dois]) for the related work, or None. label is
          "Published version" when this row is itself a preprint, else "Preprint".
    '''
    related = row.get('jrc_preprint')
    if not related:
        return None
    dois = related if isinstance(related, list) else [related]
    dois = [doi for doi in dois if doi]
    if not dois:
        return None
    label = "Published version" if DL.is_preprint(row) else "Preprint"
    return label, dois


def create_doilists(row):
    ''' Create an authorlist for a DOI
        Keyword arguments:
          row: row from the dois collection
        Returns:
          None
    '''
    if 'jrc_tag' in row:
        rtags = []
        for tag in row['jrc_tag']:
            rtags.append(tag['name'])
        TAGLIST[row['doi']] = ", ".join(rtags)
    if 'jrc_author' not in row:
        return
    names = []
    for auth in row['jrc_author']:
        valid, _orcid = valid_author(auth)
        if not valid:
            continue
        resp = people_by_id(auth)
        if not resp or 'employeeId' not in resp or not resp['employeeId']:
            LOGGER.error(f"No People information found for {auth}")
            continue
        first = resp.get('nameFirstPreferred')
        last = resp.get('nameLastPreferred')
        if first and last:
            names.append(' '.join([first, last]))
        else:
            LOGGER.warning(f"No preferred name found for {auth}")
    AUTHORLIST[row['doi']] = ", ".join(names)


def orcid_record(authid):
    ''' Cached lookup of an employee's record in the orcid collection
        Keyword arguments:
          authid: employeeId
        Returns:
          The orcid-collection record, or None if there is none
    '''
    if authid not in ORCID_CACHE:
        ORCID_CACHE[authid] = DL.single_orcid_lookup(authid, DB['dis'].orcid, 'employeeId')
    return ORCID_CACHE[authid]


def people_by_id(authid):
    ''' Cached lookup of an employee's People record
        Keyword arguments:
          authid: employeeId
        Returns:
          The People record, or None if unavailable (errors are logged once)
    '''
    if authid not in PEOPLE_CACHE:
        try:
            PEOPLE_CACHE[authid] = JRC.call_people_by_id(authid)
        except Exception as err:
            LOGGER.error(f"Error calling People by ID for {authid}: {err}")
            PEOPLE_CACHE[authid] = None
    return PEOPLE_CACHE[authid]


def valid_author(authid):
    ''' Check if an author is valid
        Keyword arguments:
          authid: author ID
        Returns:
          Tuple of (is_valid, orcid). is_valid is False for unknown or alumni
          authors; orcid is the author's ORCID, or None if they have none.
    '''
    orc = orcid_record(authid)
    if not orc or 'alumni' in orc:
        return False, None
    return True, orc.get('orcid') or None


def update_processing(doi):
    ''' Update the processing status for a DOI
        Keyword arguments:
          doi: DOI to update
        Returns:
          None
    '''
    proc = {'action': 'notify_author',
            'program': os.path.basename(__file__),
            'version': __version__,
            'timestamp': datetime.now().isoformat()}
    try:
        DB['dis'].processing.update_one({'type': 'doi', 'key': doi},
                                        {'$push': {'processes': proc}}, upsert=True)
    except Exception as err:
        terminate_program(err)


def matched_authors(row):
    ''' Get the Janelia-matched authors for a DOI, with match details
        Keyword arguments:
          row: row from the dois collection
        Returns:
          List of author detail dicts whose match type is affiliation, ORCID,
          name, or jrc_author (on this DOI's authoritative Janelia author list,
          even if get_author_details() couldn't independently confirm it --
          e.g. after a transient lookup failure). Each dict is tagged with
          'name_green': True only when the author's employeeId is in the
          DOI's jrc_author list.
    '''
    try:
        details = DL.get_author_details(row, DB['dis'].orcid)
    except Exception as err:
        LOGGER.error(f"Could not get author details for {row['doi']}: {err}")
        details = []
    jrc_authors = set(row.get('jrc_author', []))
    matched = []
    seen = set()
    for auth in details or []:
        if not auth.get('match'):
            continue
        auth['name_green'] = bool(auth.get('employeeId')) \
                             and auth['employeeId'] in jrc_authors
        matched.append(auth)
        if auth.get('employeeId'):
            seen.add(auth['employeeId'])
    # jrc_author is this DOI's authoritative, already-curated Janelia author
    # list -- it's what individual notification emails are actually driven
    # by. get_author_details() re-derives matches independently (asserted
    # affiliation, ORCID, or an exact name match) and can miss or fail on
    # someone jrc_author already confirms (e.g. a transient OpenAlex lookup
    # failure can wipe out an entire DOI's details). Anyone on jrc_author not
    # already covered above still needs to show up here, since they ARE
    # being emailed.
    for employee_id in jrc_authors - seen:
        resp = people_by_id(employee_id)
        if not resp:
            continue
        first = resp.get('nameFirstPreferred')
        last = resp.get('nameLastPreferred')
        if not (first and last):
            continue
        orc = orcid_record(employee_id) or {}
        matched.append({
            'name': f"{first} {last}",
            'employeeId': employee_id,
            'name_green': True,
            'orcid': orc.get('orcid'),
            'alumni': 'alumni' in orc,
            'workerType': orc.get('workerType'),
            'in_database': True,
            'match': 'jrc_author',
            'match_notes': "On this DOI's Janelia author list",
        })
    return matched


def author_match_table(authors):
    ''' Build an HTML table of matched Janelia authors for a publication
        Keyword arguments:
          authors: list of author detail dicts (from matched_authors)
        Returns:
          HTML string (a <table>, or a short note if there are no matches)
    '''
    rows_html = ""
    for auth in authors:
        name = auth.get('name') \
               or ' '.join([auth.get('given', ''), auth.get('family', '')]).strip()
        if auth.get('name_green'):
            name = f"<span style='color:forestgreen;'>{name}</span>"
        if auth.get('is_first'):
            name += f"<span style='{AUTHOR_CIRCLE}'>F</span>"
        if auth.get('is_last'):
            name += f"<span style='{AUTHOR_CIRCLE}'>L</span>"
        orcid = auth.get('orcid')
        orcid_cell = f"<a href='https://dis.int.janelia.org/orcidui/{orcid}'>{orcid}</a>" \
                     if orcid else ""
        match_type = auth.get('match')
        label = MATCH_LABEL.get(match_type, match_type or "")
        color = MATCH_COLOR.get(match_type)
        match_cell = f"<span style='color:{color};'>{label}</span>" if color else label
        worker_type = auth.get('workerType')
        if auth.get('alumni'):
            status_cell = f"<span style='{BADGE_RED}'>Former employee</span>"
        elif not auth.get('in_database'):
            status_cell = f"<span style='{BADGE_RED}'>Not in database</span>"
        elif worker_type and worker_type != 'Employee':
            status_cell = f"<span style='{BADGE_ORANGE}'>{worker_type}</span>"
        else:
            status_cell = "<span style='color:forestgreen; font-size:18px;'>&#10004;</span>"
        notes = auth.get('match_notes', "")
        rows_html += f"<tr><td {TD_L}>{name}</td>" \
                     + f"<td {TD_L}>{orcid_cell}</td>" \
                     + f"<td {TD_C}>{match_cell}</td>" \
                     + f"<td {TD_C}>{status_cell}</td>" \
                     + f"<td {TD_L}>{notes}</td></tr>"
    if not rows_html:
        return "<p style='margin:4px 0 0 0; color:#888;'><em>No matched Janelia authors.</em></p>"
    return "<table style='border-collapse:collapse; margin:8px 0 0 0; font-size:14px;'>" \
           + f"<tr><th {TH_L}>Author</th><th {TH_L}>ORCID</th>" \
           + f"<th {TH_C}>Match type</th><th {TH_C}>Current employee</th>" \
           + f"<th {TH_L}>Match notes</th></tr>" \
           + rows_html + "</table>"


def legend_section():
    ''' Build the "how to read this email" legend for the staff summary
        Returns:
          HTML string
    '''
    circle_f = f"<span style='{AUTHOR_CIRCLE}'>F</span>"
    circle_l = f"<span style='{AUTHOR_CIRCLE}'>L</span>"
    check = "<span style='color:forestgreen; font-size:16px;'>&#10004;</span>"
    items = [
        "<span style='color:forestgreen; font-weight:bold;'>Green author name</span>"
        " &ndash; the author's employee ID is on this paper's Janelia author list.",
        f"{circle_f} / {circle_l} &ndash; first / last author of the paper.",
        "<strong>Match type</strong> &ndash; how the author was matched: "
        f"<span style='color:{MATCH_COLOR['asserted']};'>Affiliation</span>, "
        f"<span style='color:{MATCH_COLOR['ORCID']};'>ORCID</span>, "
        f"<span style='color:{MATCH_COLOR['name']};'>Name</span>, or "
        f"<span style='color:{MATCH_COLOR['jrc_author']};'>Author list</span> "
        "(already on this DOI's Janelia author list, but not independently "
        "re-confirmed here).",
        f"<strong>Current employee</strong> &ndash; {check} current employee; "
        f"<span style='{BADGE_ORANGE}'>worker type</span> non-standard worker type; "
        f"<span style='{BADGE_RED}'>Former employee</span> or "
        f"<span style='{BADGE_RED}'>Not in database</span>.",
    ]
    lis = "".join(f"<li style='margin:2px 0;'>{item}</li>" for item in items)
    box = "background-color:#f5f5f5; border:1px solid #ddd; border-radius:6px; " \
          "padding:10px 14px; margin:8px 0; font-size:13px;"
    return f"<div style='{box}'><strong>How to read this email:</strong>" \
           + f"<ul style='margin:6px 0 0 0; padding-left:20px;'>{lis}</ul></div>"


def pub_section(pub):
    ''' Build the HTML block for one publication in the staff summary
        Keyword arguments:
          pub: staff-summary publication dict
        Returns:
          HTML string
    '''
    doi = pub['doi']
    doi_link = f"<a href='https://dis.int.janelia.org/doiui/{doi}'>{doi}</a>"
    meta = f"<strong>DOI:</strong> {doi_link}"
    for label, key in (('Source', 'source'), ('Type', 'doi_type'),
                       ('Published', 'published'), ('Added', 'inserted')):
        if pub.get(key):
            meta += f" &nbsp;|&nbsp; <strong>{label}:</strong> {pub[key]}"
    if pub.get('total_count'):
        meta += " &nbsp;|&nbsp; <strong>Janelia authors:</strong> " \
                + f"{pub['jan_count']} of {pub['total_count']}"
    html = "<div style='margin-bottom:40px;'>"
    html += f"<p style='margin:0 0 4px 0; font-size:14px;'>{meta}</p>"
    html += f"<p style='margin:0 0 4px 0;'>{pub['citation']}</p>"
    if pub.get('related'):
        rlabel, rdois = pub['related']
        links = ", ".join(f"<a href='https://dis.int.janelia.org/doiui/{d}'>{d}</a>"
                          for d in rdois)
        html += "<p style='margin:0 0 4px 0; font-size:14px;'>" \
                + f"<strong>{rlabel}:</strong> {links}</p>"
    if pub.get('tags'):
        tag_style = "background-color:#4b0082; color:#fff; padding:2px 8px; " \
                    "border-radius:10px; font-size:12px; white-space:nowrap; " \
                    "display:inline-block; margin:0 4px 4px 0;"
        badges = "".join(f"<span style='{tag_style}'>{tag}</span>"
                         for tag in pub['tags'].split(", "))
        html += "<p style='margin:0;'><span style='font-weight:bold;'>Tags:</span> " \
                + badges + "</p>"
    html += author_match_table(pub['authors'])
    return html + "</div>"


def skipped_section(skipped):
    ''' Build the HTML block listing authors who were not notified
        Keyword arguments:
          skipped: list of {'skip', 'label'} dicts
        Returns:
          HTML string
    '''
    items = "".join(f"<li>{item['label']} &ndash; {item['skip']}</li>"
                    for item in skipped)
    return "<hr style='border:none; border-top:1px solid #ccc; margin:16px 0;'>" \
           + "<p style='margin:0 0 4px 0;'><strong>Authors not notified:</strong></p>" \
           + f"<ul style='margin:0;'>{items}</ul>"


def build_staff_email(staff_pubs, cnt, num_authors, skipped):
    ''' Build the HTML body for the DIS staff summary email
        Keyword arguments:
          staff_pubs: list of {doi, citation, source, doi_type, published, tags,
                      authors} dicts
          cnt: DOI count
          num_authors: number of authors emailed
          skipped: list of {'skip', 'label'} dicts for authors not notified
        Returns:
          HTML string
    '''
    html = "<div style='font-family:Arial,Helvetica,sans-serif; color:#222;'>"
    html += f"<p>{STAFF_SUBJECT}.</p>"
    summary = "<p><strong>DOIs:</strong> " + f"{cnt}" \
              + " &nbsp;|&nbsp; <strong>Authors:</strong> " + f"{num_authors}"
    if skipped:
        summary += " &nbsp;|&nbsp; <strong>Not notified:</strong> " + f"{len(skipped)}"
    summary += " &nbsp;|&nbsp; <strong>Window:</strong> last " \
               + f"{ARG.DAYS} day{'' if ARG.DAYS == 1 else 's'}</p>"
    html += summary
    html += legend_section()
    html += "<hr style='border:none; border-top:1px solid #ccc; margin:16px 0;'>"
    for pub in staff_pubs:
        html += pub_section(pub)
    if skipped:
        html += skipped_section(skipped)
    return html + "</div>"


def resolve_author(auth):
    ''' Resolve a Janelia author to their notification details
        Keyword arguments:
          auth: author employeeId
        Returns:
          On success, a dict with 'first', 'name', 'email', and 'orcid'. When
          the author cannot be notified, a dict with 'skip' (the reason) and
          'label' (name or ID); the reason is also logged.
    '''
    valid, orcid = valid_author(auth)
    if not valid:
        arec = orcid_record(auth)
        if arec and arec.get('given') and arec.get('family'):
            name = ' '.join([arec['given'][0], arec['family'][0]])
            LOGGER.warning(f"Skipping alumnus {name} ({auth})")
            return {'skip': 'Former employee', 'label': name}
        LOGGER.warning(f"Skipping unknown author {auth}")
        return {'skip': 'Unknown author', 'label': auth}
    resp = people_by_id(auth)
    if not resp or 'employeeId' not in resp or not resp['employeeId']:
        LOGGER.warning(f"No People information found for {auth}")
        return {'skip': 'No People record', 'label': auth}
    first = resp.get('nameFirstPreferred')
    last = resp.get('nameLastPreferred')
    if not (first and last):
        LOGGER.warning(f"No preferred name found for {auth}")
        return {'skip': 'No preferred name', 'label': auth}
    name = ' '.join([first, last])
    if not resp.get('email'):
        LOGGER.warning(f"No email found for {name}")
        return {'skip': 'No email address', 'label': name}
    return {'first': first, 'name': name, 'email': resp['email'], 'orcid': orcid}


def author_email_body(info, val):
    ''' Build the HTML body of one author's DOI-notification email
        Keyword arguments:
          info: author dict from resolve_author (first, name, email, orcid)
          val: {'citations': [...], 'dois': [...]} for this author
        Returns:
          HTML string
    '''
    first = info['first']
    text1 = "publication" if len(val['citations']) == 1 else "publications"
    text = f'''\
Hello {first},<br><br>
Janelia’s Data and Information Services department (DIS) has added your recent
{text1} (DOIs listed below) to our database:<br>
        '''
    for doi in val['dois']:
        text += f"<a href='https://dis.int.janelia.org/doiui/{doi}'>{doi}</a><br>"
    text += '''\
<br><br>
To ensure accuracy, review the metadata below and
<em>let us know if they are incorrect by responding to this email</em>.
<strong><em>There is no action required from you, unless you see an error</em></strong>.
<br><br>
<h3>FYI</h3>
<strong>Tags</strong>:
In the DIS publication database, there may be multiple redundant tags for the same lab,
project team or support team. This is normal, but
<em>please let us know if a lab/team is missing or if a lab/team doesn’t belong on the publication</em>.
<br><br>
<strong>Janelia authors</strong>:
The Janelia author names listed below may not perfectly correspond to author names listed on the paper
(e.g., Jane Doe / Janet P. Doe), which is normal. If we have missed any authors or included authors
not affiliated with Janelia, please let us know.
<br><br>
        '''
    if not info['orcid']:
        LOGGER.warning(f"Author {info['name']} has no ORCID")
        text += "<strong>Note:</strong> We could not find " \
                + "an ORCID for you. We ask that you please create one with " \
                + "your Janelia affiliation. To create one, please visit " \
                + "<a href='https://orcid.org/register'>ORCID</a>.<br><br>"
    text += "Thank you and have a great day,<br><br>Lauren Acquarole<br>Mary Lay<br><br>"
    text += "<h3>Citations</h3>"
    for res, doi in zip(val['citations'], val['dois']):
        text += f"{res}"
        if doi in TAGLIST:
            text += f"<br><span style='font-weight: bold'>Tags:</span> {TAGLIST[doi]}"
        if doi in AUTHORLIST:
            text += "<br><span style='font-weight: bold'>Janelia authors:</span> " \
                    + f"{AUTHORLIST[doi]}"
        text += "<br><br>"
    return text


def send_staff_summary(cnt, staff_pubs, num_authors, skipped):
    ''' Send the DIS staff summary email (write and test modes only)
        Keyword arguments:
          cnt: DOI count
          staff_pubs: list of {doi, citation, source, doi_type, published, tags,
                      authors} dicts for the summary
          num_authors: number of authors emailed
          skipped: list of {'skip', 'label'} dicts for authors not notified
        Returns:
          None
    '''
    if not (ARG.WRITE or ARG.TEST) or not cnt:
        return
    body = build_staff_email(staff_pubs, cnt, num_authors, skipped)
    email = [DISCONFIG['developer']] if ARG.TEST else DISCONFIG['receivers']
    try:
        JRC.send_email(body, DISCONFIG['sender'], email, STAFF_SUBJECT, mime='html')
    except Exception as err:
        LOGGER.error(f"Failed to send DIS staff summary email: {err}")
        return
    LOGGER.info(f"DIS staff summary email sent to {', '.join(email)}")


def send_one_email(info, subject, text, example_sent):
    ''' Send (or, per run mode, simulate) one author's notification email
        Keyword arguments:
          info: resolved author dict (name, email, ...)
          subject: email subject
          text: HTML email body
          example_sent: whether a test-mode example has already been sent
        Returns:
          Tuple (delivered, example_sent). delivered is True only when a real
          email was sent to the author (write mode); example_sent is the
          possibly-updated flag.
    '''
    name = info['name']
    email = DISCONFIG['developer'] if ARG.TEST else info['email']
    if ARG.WRITE:
        try:
            JRC.send_email(text, DISCONFIG['sender'], [email], subject, mime='html')
        except Exception as err:
            LOGGER.error(f"Failed to send email to {name} ({email}): {err}")
            return False, example_sent
        LOGGER.info(f"Email sent to {name} ({email})")
        return True, example_sent
    if ARG.TEST:
        if example_sent:
            LOGGER.info(f"Skipping individual email for {name} " \
                        + "(test mode example already sent)")
            return False, example_sent
        try:
            JRC.send_email(text, DISCONFIG['sender'], [email], subject, mime='html')
        except Exception as err:
            LOGGER.error(f"Failed to send example email to {email}: {err}")
            return False, example_sent
        LOGGER.info(f"Example author email sent to developer ({email}) for {name}")
        return False, True
    LOGGER.info(f"Would send email to {name} ({email})")
    return False, example_sent


def process_authors(authors, cnt, staff_pubs):
    ''' Create and send emails to each author with their resources
        Keyword arguments:
          authors: dictionary of authors and their citations
          cnt: DOI count
          staff_pubs: list of {doi, citation, source, doi_type, published, tags,
                      authors} dicts for the DIS staff summary email
        Returns:
          None
    '''
    notified = []
    skipped = []
    emailed = 0
    # Individual author emails. In test mode we send a single example to the
    # developer rather than one email per author.
    example_sent = False
    for auth, val in authors.items():
        info = resolve_author(auth)
        if info.get('skip'):
            skipped.append(info)
            continue
        subject = "Your recent publication" if len(val['citations']) == 1 \
                  else "Your recent publications"
        text = author_email_body(info, val)
        delivered, example_sent = send_one_email(info, subject, text, example_sent)
        if ARG.WRITE and not delivered:
            continue
        emailed += 1
        if ARG.WRITE:
            for doi in val['dois']:
                if doi not in notified:
                    notified.append(doi)
                    update_processing(doi)
    send_staff_summary(cnt, staff_pubs, emailed, skipped)


def process_dois():
    ''' Find and process DOIs
        Keyword arguments:
          None
        Returns:
          None
    '''
    week_ago = (datetime.today() - timedelta(days=ARG.DAYS)).strftime("%Y-%m-%d")
    LOGGER.info(f"Finding DOIs from the last {ARG.DAYS} day{'' if ARG.DAYS == 1 else 's'} " \
                + f"({week_ago})")
    payload = {"jrc_newsletter": {"$gte": week_ago}, "jrc_author": {"$exists": True},
               "$or": [{"jrc_obtained_from": "Crossref"},
                       {"jrc_obtained_from": "DataCite",
                        "types.resourceTypeGeneral": {"$ne": "Dataset"}}]}
    try:
        cnt = DB['dis'].dois.count_documents(payload)
        rows = DB['dis'].dois.find(payload)
    except Exception as err:
        terminate_program(err)
    LOGGER.info(f"DOIs found: {cnt}")
    authors = {}
    staff_pubs = []
    for row in rows:
        citation = get_citation(row)
        for auth in row['jrc_author']:
            if auth not in authors:
                authors[auth] = {"citations": [], "dois": []}
            authors[auth]['citations'].append(citation)
            authors[auth]['dois'].append(row['doi'])
        if row['doi'] not in AUTHORLIST:
            create_doilists(row)
        staff_pubs.append({'doi': row['doi'], 'citation': citation,
                           'source': row.get('jrc_obtained_from', ''),
                           'doi_type': doi_type_display(row),
                           'published': row.get('jrc_publishing_date', ''),
                           'inserted': str(row.get('jrc_inserted', '')).split(' ', maxsplit=1)[0],
                           'jan_count': len(row.get('jrc_author', [])),
                           'total_count': len(row.get('author') or row.get('creators') or []),
                           'related': related_publication(row),
                           'tags': TAGLIST.get(row['doi']),
                           'authors': matched_authors(row)})
    LOGGER.info(f"Authors found: {len(authors)}")
    process_authors(authors, cnt, staff_pubs)

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Email information on newly-added DOIs to author")
    PARSER.add_argument('--days', dest='DAYS', action='store', type=int,
                        default=5, help='Number of days to go back for DOIs')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, prod)')
    PARSER.add_argument('--test', dest='TEST', action='store_true',
                        default=False, help='Send emails to developer')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Actually send emails')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    DISCONFIG = JRC.simplenamespace_to_dict(JRC.get_config("dis"))
    initialize_program()
    process_dois()
    terminate_program()
