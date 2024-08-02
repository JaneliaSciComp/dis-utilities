''' dis_responder.py
    UI and REST API for Data and Information Services
'''

from datetime import date, datetime, timedelta
import inspect
from json import JSONEncoder
from operator import attrgetter
import os
import random
import re
import string
import sys
from time import time
import bson
from flask import (Flask, make_response, render_template, request, jsonify, send_file)
from flask_cors import CORS
from flask_swagger import swagger
import requests
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL
import dis_plots as DP

# pylint: disable=broad-exception-caught,too-many-lines

__version__ = "10.4.0"
# Database
DB = {}
# Custom queries
CUSTOM_REGEX = {"publishing_year": {"field": "jrc_publishing_date",
                                    "value": "^!REPLACE!"}
               }

# Navigation
NAV = {"Home": "",
       "DOIs": {"DOIs by authorship": "dois_author",
                "DOIs by insertion date": "dois_insertpicker",
                "DOIs by preprint status": "dois_preprint",
                "DOIs by publisher": "dois_publisher",
                "DOIs by tag": "dois_tag",
                "DOIs by source": "dois_source",
                "DOIs by year": "dois_year",
                "Top tags by year": "dois_top"
            },
       "ORCID": {"Groups": "groups",
                 "Affiliations": "orcid_tag",
                 "Entries": "orcid_entry"
                },
       "Stats" : {"Database": "stats_database"
                 },
      }
# Sources
SOURCES = ["Crossref", "DataCite"]

# ******************************************************************************
# * Classes                                                                    *
# ******************************************************************************

class CustomJSONEncoder(JSONEncoder):
    ''' Define a custom JSON encoder
    '''
    def default(self, o):
        try:
            if isinstance(o, bson.objectid.ObjectId):
                return str(o)
            if isinstance(o, datetime):
                return o.strftime("%a, %-d %b %Y %H:%M:%S")
            if isinstance(o, timedelta):
                seconds = o.total_seconds()
                hours = seconds // 3600
                minutes = (seconds % 3600) // 60
                seconds = seconds % 60
                return f"{hours:02d}:{minutes:02d}:{seconds:.02f}"
            iterable = iter(o)
        except TypeError:
            pass
        else:
            return list(iterable)
        return JSONEncoder.default(self, o)


class InvalidUsage(Exception):
    ''' Class to populate error return for JSON.
    '''
    def __init__(self, message, status_code=400, payload=None):
        Exception.__init__(self)
        self.message = message
        self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        ''' Build error response
        '''
        retval = dict(self.payload or ())
        retval['rest'] = {'status_code': self.status_code,
                          'error': True,
                          'error_text': f"{self.message}\n" \
                                        + f"An exception of type {type(self).__name__} occurred. " \
                                        + f"Arguments:\n{self.args}"}
        return retval


class CustomException(Exception):
    ''' Class to populate error return for HTML.
    '''
    def __init__(self,message, preface=""):
        super().__init__(message)
        self.original = type(message).__name__
        self.args = message.args
        cfunc = inspect.stack()[1][3]
        self.preface = f"In {cfunc}, {preface}" if preface else f"Error in {cfunc}."


# ******************************************************************************
# * Flask                                                                      *
# ******************************************************************************

app = Flask(__name__, template_folder="templates")
app.json_encoder = CustomJSONEncoder
app.config.from_pyfile("config.cfg")
CORS(app, supports_credentials=True)
app.json_encoder = CustomJSONEncoder
app.config["STARTDT"] = datetime.now()
app.config["LAST_TRANSACTION"] = time()


@app.before_request
def before_request():
    ''' Set transaction start time and increment counters.
        If needed, initilize global variables.
    '''
    if not DB:
        try:
            dbconfig = JRC.get_config("databases")
        except Exception as err:
            return render_template('warning.html', urlroot=request.url_root,
                                   title=render_warning("Config error"), message=err)
        dbo = attrgetter("dis.prod.write")(dbconfig)
        print(f"Connecting to {dbo.name} prod on {dbo.host} as {dbo.user}")
        try:
            DB['dis'] = JRC.connect_database(dbo)
        except Exception as err:
            return render_template('warning.html', urlroot=request.url_root,
                                   title=render_warning("Database connect error"), message=err)
    app.config["START_TIME"] = time()
    app.config["COUNTER"] += 1
    endpoint = request.endpoint if request.endpoint else "(Unknown)"
    app.config["ENDPOINTS"][endpoint] = app.config["ENDPOINTS"].get(endpoint, 0) + 1
    if request.method == "OPTIONS":
        result = initialize_result()
        return generate_response(result)
    return None

# ******************************************************************************
# * Error utility functions                                                    *
# ******************************************************************************

@app.errorhandler(InvalidUsage)
def handle_invalid_usage(error):
    ''' Error handler
        Keyword arguments:
          error: error object
    '''
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response


def error_message(err):
    ''' Create an error message from an exception
        Keyword arguments:
          err: exception
        Returns:
          Error message
    '''
    if isinstance(err, CustomException):
        msg = f"{err.preface}\n" if err.preface else ""
        msg += f"An exception of type {err.original} occurred. Arguments:\n{err.args}"
    else:
        msg = f"An exception of type {type(err).__name__} occurred. Arguments:\n{err.args}"
    return msg


def inspect_error(err, errtype):
    ''' Render an error with inspection
        Keyword arguments:
          err: exception
        Returns:
          Error screen
    '''
    mess = f"In {inspect.stack()[1][3]}, An exception of type {type(err).__name__} occurred. " \
           + f"Arguments:\n{err.args}"
    return render_template('error.html', urlroot=request.url_root,
                           title=render_warning(errtype), message=mess)


def render_warning(msg, severity='error', size='lg'):
    ''' Render warning HTML
        Keyword arguments:
          msg: message
          severity: severity (warning, error, or success)
          size: glyph size
        Returns:
          HTML rendered warning
    '''
    icon = 'exclamation-triangle'
    color = 'goldenrod'
    if severity == 'error':
        color = 'red'
    elif severity == 'success':
        icon = 'check-circle'
        color = 'lime'
    elif severity == 'na':
        icon = 'minus-circle'
        color = 'gray'
    elif severity == 'missing':
        icon = 'minus-circle'
    elif severity == 'no':
        icon = 'times-circle'
        color = 'red'
    elif severity == 'warning':
        icon = 'exclamation-circle'
    return f"<span class='fas fa-{icon} fa-{size}' style='color:{color}'></span>" \
           + f"&nbsp;{msg}"

# ******************************************************************************
# * Navigation utility functions                                               *
# ******************************************************************************

def generate_navbar(active):
    ''' Generate the web navigation bar
        Keyword arguments:
          Navigation bar
    '''
    nav = '''
    <nav class="navbar navbar-expand-lg navbar-dark bg-dark">
      <div class="collapse navbar-collapse" id="navbarSupportedContent">
        <ul class="navbar-nav mr-auto">
    '''
    for heading, subhead in NAV.items():
        basic = '<li class="nav-item active">' if heading == active else '<li class="nav-item">'
        drop = '<li class="nav-item dropdown active">' if heading == active \
               else '<li class="nav-item dropdown">'
        menuhead = '<a class="nav-link dropdown-toggle" href="#" id="navbarDropdown" ' \
                   + 'role="button" data-toggle="dropdown" aria-haspopup="true" ' \
                   + f"aria-expanded=\"false\">{heading}</a><div class=\"dropdown-menu\" "\
                   + 'aria-labelledby="navbarDropdown">'
        if subhead:
            nav += drop + menuhead
            for itm, val in subhead.items():
                link = f"/{val}" if val else ('/' + itm.replace(" ", "_")).lower()
                nav += f"<a class='dropdown-item' href='{link}'>{itm}</a>"
            nav += '</div></li>'
        else:
            nav += basic
            link = ('/' + heading.replace(" ", "_")).lower()
            nav += f"<a class='nav-link' href='{link}'>{heading}</a></li>"
    nav += '</ul></div></nav>'
    return nav

# ******************************************************************************
# * Payload utility functions                                                  *
# ******************************************************************************

def receive_payload():
    ''' Get a request payload (form or JSON).
        Keyword arguments:
          None
        Returns:
          payload dictionary
    '''
    pay = {}
    if not request.get_data():
        return pay
    try:
        if request.form:
            for itm in request.form:
                pay[itm] = request.form[itm]
        elif request.json:
            pay = request.json
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    return pay


def initialize_result():
    ''' Initialize the result dictionary
        An auth header with a JWT token is required for all POST and DELETE requests
        Returns:
          decoded partially populated result dictionary
    '''
    result = {"rest": {"requester": request.remote_addr,
                       "url": request.url,
                       "endpoint": request.endpoint,
                       "error": False,
                       "elapsed_time": "",
                       "row_count": 0,
                       "pid": os.getpid()}}
    if app.config["LAST_TRANSACTION"]:
        print(f"Seconds since last transaction: {time() - app.config['LAST_TRANSACTION']}")
    app.config["LAST_TRANSACTION"] = time()
    return result


def generate_response(result):
    ''' Generate a response to a request
        Keyword arguments:
          result: result dictionary
        Returns:
          JSON response
    '''
    result["rest"]["elapsed_time"] = str(timedelta(seconds=time() - app.config["START_TIME"]))
    return jsonify(**result)


def get_custom_payload(ipd, display_value):
    ''' Get custom payload
        Keyword arguments:
          ipd: input payload dictionary
          display_value: display value
        Returns:
          payload: payload for MongoDB find
          ptitle: page titleq
    '''
    if ipd['field'] in CUSTOM_REGEX:
        rex = CUSTOM_REGEX[ipd['field']]['value']
        ipd['value'] = {"$regex": rex.replace("!REPLACE!", ipd['value'])}
        ipd['field'] = CUSTOM_REGEX[ipd['field']]['field']
    ptitle = f"DOIs for {ipd['field']} {display_value}"
    payload = {ipd['field']: ipd['value']}
    if 'jrc_obtained_from' in ipd and ipd['jrc_obtained_from']:
        payload['jrc_obtained_from'] = ipd['jrc_obtained_from']
        ptitle += f" from {ipd['jrc_obtained_from']}"
    return payload, ptitle

# ******************************************************************************
# * ORCID utility functions                                                    *
# ******************************************************************************

def get_work_publication_date(wsumm):
    ''' Get a publication date from an ORCID work summary
        Keyword arguments:
          wsumm: ORCID work summary
        Returns:
          Publication date
    '''
    pdate = ''
    if 'publication-date' in wsumm and wsumm['publication-date']:
        ppd = wsumm['publication-date']
        if 'year' in ppd and ppd['year']['value']:
            pdate = ppd['year']['value']
        if 'month' in ppd and ppd['month'] and ppd['month']['value']:
            pdate += f"-{ppd['month']['value']}"
        if 'day' in ppd and ppd['day'] and ppd['day']['value']:
            pdate += f"-{ppd['day']['value']}"
    return pdate


def get_work_doi(work):
    ''' Get a DOI from an ORCID work
        Keyword arguments:
          work: ORCID work
        Returns:
          DOI
    '''
    if not work['external-ids']['external-id']:
        return ''
    for eid in work['external-ids']['external-id']:
        if eid['external-id-type'] != 'doi':
            continue
        if 'external-id-normalized' in eid:
            return eid['external-id-normalized']['value']
        if 'external-id-value' in eid:
            return eid['external-id-url']['value']
    return ''


def orcid_payload(oid, orc, eid=None):
    ''' Generate a payload for searching the dois collection by ORCID or employeeId
        Keyword arguments:
          oid: ORCID or employeeId
          orc: orcid record
          eid: employeeId boolean
        Returns:
          Payload
    '''
    # Name only search
    payload = {"$and": [{"$or": [{"author.given": {"$in": orc['given']}},
                                 {"creators.givenName": {"$in": orc['given']}}]},
                        {"$or": [{"author.family": {"$in": orc['family']}},
                                 {"creators.familyName": {"$in": orc['family']}}]}]
              }
    if eid and not oid:
        # Employee ID only search
        payload = {"$or": [{"jrc_author": eid}, {"$and": payload["$and"]}]}
    elif oid and eid:
        # Search by either name or employee ID
        payload = {"$or": [{"orcid": oid}, {"jrc_author": eid}, {"$and": payload["$and"]}]}
    return payload


def get_dois_for_orcid(oid, orc, use_eid, both):
    ''' Generate DOIs for a single user
        Keyword arguments:
          oid: ORCID or employeeId
          orc: orcid record
          use_eid: use employeeId boolean
          both: search by both ORCID and employeeId
        Returns:
          HTML and a list of DOIs
    '''
    try:
        if both:
            eid = orc['employeeId'] if 'employeeId' in orc else None
            rows = DB['dis'].dois.find(orcid_payload(oid, orc, eid))
        elif use_eid:
            rows = DB['dis'].dois.find(orcid_payload(None, orc, oid))
        else:
            rows = DB['dis'].dois.find(orcid_payload(oid, orc))
    except Exception as err:
        raise CustomException(err, "Could not find in dois collection by name.") from err
    return rows


def generate_works_table(rows, name=None):
    ''' Generate table HTML for a person's works
        Keyword arguments:
          rows: rows from dois collection
        Returns:
          HTML and a list of DOIs
    '''
    works = []
    dois = []
    authors = {}
    html = ""
    for row in rows:
        if row['doi']:
            doi = f"<a href='/doiui/{row['doi']}'>{row['doi']}</a>"
        else:
            doi = "&nbsp;"
        title = DL.get_title(row)
        dois.append(row['doi'])
        payload = {"date":  DL.get_publishing_date(row),
                   "doi": doi,
                   "title": title
                  }
        works.append(payload)
        if name:
            alist = DL.get_author_details(row)
            for auth in alist:
                if "family" in auth and "given" in auth and auth["family"].lower() == name.lower():
                    authors[f"{auth['given']} {auth['family']}"] = True
    if not works:
        return html, []
    html += f"Publications: {len(works)}<br><table id='pubs' class='tablesorter standard'>" \
            + '<thead><tr><th>Published</th><th>DOI</th><th>Title</th></tr></thead><tbody>'

    for work in sorted(works, key=lambda row: row['date'], reverse=True):
        html += f"<tr><td>{work['date']}</td><td>{work['doi'] if work['doi'] else '&nbsp;'}</td>" \
                + f"<td>{work['title']}</td></tr>"
    if dois:
        html += "</tbody></table>"
    if authors:
        html = f"Authors found: {', '.join(sorted(authors.keys()))}<br>" \
               + f"This may include non-Janelia authors<br>{html}"
    return html, dois


def get_orcid_from_db(oid, use_eid=False, both=False, bare=False):
    ''' Generate HTML for an ORCID or employeeId that is in the orcid collection
        Keyword arguments:
          oid: ORCID or employeeId
          use_eid: use employeeId boolean
          both: search by both ORCID and employeeId
          bare: entry has no ORCID or employeeId
        Returns:
          HTML and a list of DOIs
    '''
    try:
        if bare:
            orc = DB['dis'].orcid.find_one({"_id": bson.ObjectId(oid)})
        else:
            orc = DL.single_orcid_lookup(oid, DB['dis'].orcid, 'employeeId' if use_eid else 'orcid')
    except Exception as err:
        raise CustomException(err, "Could not find_one in orcid collection by ORCID ID.") from err
    if not orc:
        return "", []
    html = "<br><table class='borderless'>"
    if use_eid and 'orcid' in orc:
        html += f"<tr><td>ORCID:</td><td><a href='https://orcid.org/{orc['orcid']}'>" \
                + f"{orc['orcid']}</a></td></tr>"
    html += f"<tr><td>Given name:</td><td>{', '.join(sorted(orc['given']))}</td></tr>"
    html += f"<tr><td>Family name:</td><td>{', '.join(sorted(orc['family']))}</td></tr>"
    if 'employeeId' in orc:
        link = "<a href='" + f"{app.config['WORKDAY']}{orc['userIdO365']}" \
               + f"' target='_blank'>{orc['employeeId']}</a>"
        html += f"<tr><td>Employee ID:</td><td>{link}</td></tr>"
    if 'affiliations' in orc:
        html += f"<tr><td>Affiliations:</td><td>{', '.join(orc['affiliations'])}</td></tr>"
    html += "</table><br>"
    try:
        rows = get_dois_for_orcid(oid, orc, use_eid, both)
    except Exception as err:
        raise err
    tablehtml, dois = generate_works_table(rows)
    if tablehtml:
        html = f"{' '.join(add_orcid_badges(orc))}{html}{tablehtml}"
    else:
        html = f"{' '.join(add_orcid_badges(orc))}{html}<br>No works found in dois collection."
    return html, dois


def add_orcid_works(data, dois):
    ''' Generate HTML for a list of works from ORCID
        Keyword arguments:
          data: ORCID data
          dois: list of DOIs from dois collection
        Returns:
          HTML for a list of works from ORCID
    '''
    html = inner = ""
    works = 0
    for work in data['activities-summary']['works']['group']:
        wsumm = work['work-summary'][0]
        pdate = get_work_publication_date(wsumm)
        doi = get_work_doi(work)
        if (not doi) or (doi in dois):
            continue
        works += 1
        if not doi:
            inner += f"<tr><td>{pdate}</td><td>&nbsp;</td>" \
                     + f"<td>{wsumm['title']['title']['value']}</td></tr>"
            continue
        if work['external-ids']['external-id'][0]['external-id-url']:
            if work['external-ids']['external-id'][0]['external-id-url']:
                link = "<a href='" \
                       + work['external-ids']['external-id'][0]['external-id-url']['value'] \
                       + f"' target='_blank'>{doi}</a>"
        else:
            link = f"<a href='/doiui/{doi}'>{doi}</a>"
        inner += f"<tr><td>{pdate}</td><td>{link}</td>" \
                 + f"<td>{wsumm['title']['title']['value']}</td></tr>"
    if inner:
        title = "title is" if works == 1 else f"{works} titles are"
        html += f"<hr>The additional {title} from ORCID. Note that titles below may " \
                + "be self-reported, may not have DOIs available, or may be from the author's " \
                + "employment outside of Janelia.</br>"
        html += '<table id="works" class="tablesorter standard"><thead><tr>' \
                + '<th>Published</th><th>DOI</th><th>Title</th>' \
                + f"</tr></thead><tbody>{inner}</tbody></table>"
    return html


def generate_user_table(rows):
    ''' Generate HTML for a list of users
        Keyword arguments:
          rows: rows from orcid collection
        Returns:
          HTML for a list of authors with a count
    '''
    count = 0
    html = '<table id="ops" class="tablesorter standard"><thead><tr>' \
           + '<th>ORCID</th><th>Given name</th><th>Family name</th>' \
           + '<th>Status</th></tr></thead><tbody>'
    for row in rows:
        count += 1
        if 'orcid' in row:
            link = f"<a href='/orcidui/{row['orcid']}'>{row['orcid']}</a>"
        elif 'employeeId' in row:
            link = f"<a href='/userui/{row['employeeId']}'>No ORCID found</a>"
        else:
            link = f"<a href='/unvaluserui/{row['_id']}'>No ORCID found</a>"
        auth = DL.get_single_author_details(row, DB['dis'].orcid)
        badges = get_badges(auth)
        html += f"<tr><td>{link}</td><td>{', '.join(row['given'])}</td>" \
                + f"<td>{', '.join(row['family'])}</td><td>{' '.join(badges)}</td></tr>"
    html += '</tbody></table>'
    return html, count

# ******************************************************************************
# * DOI utility functions                                                      *
# ******************************************************************************

def get_doi(doi):
    ''' Add a table of custom JRC fields
        Keyword arguments:
          doi: DOI
        Returns:
          source: data source
          data: data from response
    '''
    if DL.is_datacite(doi):
        resp = JRC.call_datacite(doi)
        source = 'datacite'
        data = resp['data']['attributes'] if 'data' in resp else {}
    else:
        resp = JRC.call_crossref(doi)
        source = 'crossref'
        data = resp['message'] if 'message' in resp else {}
    return source, data


def add_jrc_fields(row):
    ''' Add a table of custom JRC fields
        Keyword arguments:
          row: DOI record
        Returns:
          HTML
    '''
    jrc = {}
    prog = re.compile("^jrc_")
    for key, val in row.items():
        if not re.match(prog, key):
            continue
        if isinstance(val, list):
            val = ", ".join(sorted(val))
        jrc[key] = val
    if not jrc:
        return ""
    html = '<table class="standard">'
    for key in sorted(jrc):
        val = jrc[key]
        if key == 'jrc_author':
            link = []
            for auth in val.split(", "):
                link.append(f"<a href='/userui/{auth}'>{auth}</a>")
            val = ", ".join(link)
        if key == 'jrc_preprint':
            link = []
            for auth in val.split(", "):
                link.append(f"<a href='/doiui/{auth}'>{auth}</a>")
            val = ", ".join(link)
        elif key == 'jrc_tag':
            link = []
            for aff in val.split(", "):
                link.append(f"<a href='/affiliation/{aff}'>{aff}</a>")
            val = ", ".join(link)
        html += f"<tr><td>{key}</td><td>{val}</td></tr>"
    html += "</table><br>"
    return html


def add_relations(row):
    ''' Create a list of relations
        Keyword arguments:
          row: DOI record
        Returns:
          HTML
    '''
    html = ""
    if ("relation" not in row) or (not row['relation']):
        return html
    for rel in row['relation']:
        used = []
        for itm in row['relation'][rel]:
            if itm['id'] in used:
                continue
            link = f"<a href='/doiui/{itm['id']}'>{itm['id']}</a>"
            html += f"This DOI {rel.replace('-', ' ')} {link}<br>"
            used.append(itm['id'])
    return html


def get_migration_data(row, orgs):
    ''' Create a migration record for a single DOI
        Keyword arguments:
          doi: doi record
          orgs: dictionary of organizations/codes
        Returns:
          migration dictionary
    '''
    rec = {}
    # Author
    tags = []
    #tagname = []
    #try:
    #    authors = DL.get_author_details(row, DB['dis'].orcid)
    #except Exception as err:
    #    raise InvalidUsage("Could not get author details: " + str(err), 500) from err
    #if 'jrc_tag' in row:
    #    for atag in row['jrc_tag']:
    #        if atag not in tagname:
    #            code = orgs[atag] if atag in orgs else None
    #            tagname.append(atag)
    #            tags.append({"name": atag, "code": code})
    #rec['authors'] = authors
    if 'jrc_tag' in row:
        for atag in row['jrc_tag']:
            code = orgs[atag] if atag in orgs else None
            tags.append({"name": atag, "code": code})
    if 'jrc_author' in row:
        rec['jrc_author'] = row['jrc_author']
    if tags:
        rec['tags'] = tags
    # Additional data
    if row['jrc_obtained_from'] == 'Crossref' and 'abstract' in row:
        rec['abstract'] = row['abstract']
    rec['journal'] = DL.get_journal(row)
    if 'jrc_publishing_date' in row:
        rec['jrc_publishing_date'] = row['jrc_publishing_date']
    if 'publisher' in row:
        rec['publisher'] = row['publisher']
    rec['title'] = DL.get_title(row)
    if 'URL' in row:
        rec['url'] = row['URL']
    return rec

# ******************************************************************************
# * Badge utility functions                                                    *
# ******************************************************************************

def tiny_badge(btype, msg, link=None):
    ''' Create HTML for a [very] small badge
        Keyword arguments:
          btype: badge type (success, danger, etc.)
          msg: message to show on badge
          link: link to other web page
        Returns:
          HTML
    '''
    html = f"<span class='badge badge-{btype}' style='font-size: 8pt'>{msg}</span>"
    if link:
        html = f"<a href='{link}' target='_blank'>{html}</a>"
    return html


def get_badges(auth):
    ''' Create a list of badges for an author
        Keyword arguments:
          auth: detailed author record
        Returns:
          List of HTML badges
    '''
    badges = []
    if 'in_database' in auth and auth['in_database']:
        badges.append(f"{tiny_badge('success', 'In database')}")
        if auth['alumni']:
            badges.append(f"{tiny_badge('danger', 'Alumni')}")
        elif 'validated' not in auth or not auth['validated']:
            badges.append(f"{tiny_badge('warning', 'Not validated')}")
        if 'orcid' not in auth or not auth['orcid']:
            badges.append(f"{tiny_badge('urgent', 'No ORCID')}")
        if auth['asserted']:
            badges.append(f"{tiny_badge('info', 'Janelia affiliation')}")
    else:
        badges.append(f"{tiny_badge('danger', 'Not in database')}")
        if 'asserted' in auth and auth['asserted']:
            badges.append(f"{tiny_badge('info', 'Janelia affiliation')}")
    return badges


def show_tagged_authors(authors):
    ''' Create a list of Janelian authors (with badges and tags)
        Keyword arguments:
          authors: list of detailed authors from a publication
        Returns:
          List of HTML authors
    '''
    alist = []
    count = 0
    for auth in authors:
        if (not auth['janelian']) and (not auth['asserted']):
            continue
        if auth['janelian'] or auth['asserted']:
            count += 1
        who = f"{auth['given']} {auth['family']}"
        if 'orcid' in auth and auth['orcid']:
            who = f"<a href='/orcidui/{auth['orcid']}'>{who}</a>"
        elif 'employeeId' in auth and auth['employeeId']:
            who = f"<a href='/userui/{auth['employeeId']}'>{who}</a>"
        badges = get_badges(auth)
        tags = []
        if 'group' in auth:
            tags.append(auth['group'])
        if 'tags' in auth:
            for tag in auth['tags']:
                if tag not in tags:
                    tags.append(tag)
        tags.sort()
        row = f"<td>{who}</td><td>{' '.join(badges)}</td><td>{', '.join(tags)}</td>"
        alist.append(row)
    return f"<table class='borderless'><tr>{'</tr><tr>'.join(alist)}</tr></table>", count


def add_orcid_badges(orc):
    ''' Generate badges for an ORCID ID that is in the orcid collection
        Keyword arguments:
          orc: row from orcid collection
        Returns:
          List of badges
    '''
    badges = []
    badges.append(tiny_badge('success', 'In database'))
    if 'orcid' not in orc or not orc['orcid']:
        badges.append(f"{tiny_badge('urgent', 'No ORCID')}")
    if 'alumni' in orc:
        badges.append(tiny_badge('danger', 'Alumni'))
    if 'employeeId' not in orc:
        badges.append(tiny_badge('warning', 'Not validated'))
    return badges

# ******************************************************************************
# * General utility functions                                                  *
# ******************************************************************************

def random_string(strlen=8):
    ''' Generate a random string of letters and digits
        Keyword arguments:
          strlen: length of generated string
    '''
    cmps = string.ascii_letters + string.digits
    return ''.join(random.choice(cmps) for i in range(strlen))


def create_downloadable(name, header, content):
    ''' Generate a downloadable content file
        Keyword arguments:
          name: base file name
          header: table header
          content: table content
        Returns:
          File name
    '''
    fname = f"{name}_{random_string()}_{datetime.today().strftime('%Y%m%d%H%M%S')}.tsv"
    with open(f"/tmp/{fname}", "w", encoding="utf8") as text_file:
        if header:
            content = "\t".join(header) + "\n" + content
        text_file.write(content)
    return f'<a class="btn btn-outline-success" href="/download/{fname}" ' \
                + 'role="button">Download tab-delimited file</a>'


def humansize(num, suffix='B'):
    ''' Return a human-readable storage size
        Keyword arguments:
          num: size
          suffix: default suffix
        Returns:
          string
    '''
    for unit in ['', 'K', 'M', 'G', 'T']:
        if abs(num) < 1024.0:
            return f"{num:.1f}{unit}{suffix}"
        num /= 1024.0
    return "{num:.1f}P{suffix}"


def dloop(row, keys, sep="\t"):
    ''' Generate a string of joined velues from a dictionary
        Keyword arguments:
          row: dictionary
          keys: list of keys
          sep: separator
        Returns:
          Joined values from a dictionary
    '''
    return sep.join([str(row[fld]) for fld in keys])


def last_thursday():
    ''' Calculate the date of the most recent Thursday
        Returns:
          Date of the most recent Thursday
    '''
    today = date.today()
    offset = (today.weekday() - 3) % 7
    if offset:
        offset = 7
    return today - timedelta(days=offset)

# *****************************************************************************
# * Documentation                                                             *
# *****************************************************************************

@app.route('/doc')
def get_doc_json():
    ''' Show documentation
    '''
    try:
        swag = swagger(app)
    except Exception as err:
        return inspect_error(err, 'Could not parse swag')
    swag['info']['version'] = __version__
    swag['info']['title'] = "Data and Information Services"
    return jsonify(swag)


@app.route('/help')
def show_swagger():
    ''' Show Swagger docs
    '''
    return render_template('swagger_ui.html')

# *****************************************************************************
# * Admin endpoints                                                           *
# *****************************************************************************

@app.route("/stats")
def stats():
    '''
    Show stats
    Show uptime/requests statistics
    ---
    tags:
      - Diagnostics
    responses:
      200:
        description: Stats
      400:
        description: Stats could not be calculated
    '''
    tbt = time() - app.config['LAST_TRANSACTION']
    result = initialize_result()
    start = datetime.fromtimestamp(app.config['START_TIME']).strftime('%Y-%m-%d %H:%M:%S')
    up_time = datetime.now() - app.config['STARTDT']
    result['stats'] = {"version": __version__,
                       "requests": app.config['COUNTER'],
                       "start_time": start,
                       "uptime": str(up_time),
                       "python": sys.version,
                       "pid": os.getpid(),
                       "endpoint_counts": app.config['ENDPOINTS'],
                       "time_since_last_transaction": tbt,
                      }
    return generate_response(result)


# ******************************************************************************
# * API endpoints (DOI)                                                        *
# ******************************************************************************
@app.route('/doi/authors/<path:doi>')
def show_doi_authors(doi):
    '''
    Return a DOI's authors
    Return information on authors for a given DOI.
    ---
    tags:
      - DOI
    parameters:
      - in: path
        name: doi
        schema:
          type: path
        required: true
        description: DOI
    responses:
      200:
        description: DOI data
      500:
        description: MongoDB error
    '''
    doi = doi.lstrip('/').rstrip('/').lower()
    result = initialize_result()
    try:
        row = DB['dis'].dois.find_one({"doi": doi}, {'_id': 0})
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    if not row:
        result['data'] = []
        return generate_response(result)
    try:
        authors = DL.get_author_details(row, DB['dis'].orcid)
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    tagname = []
    tags = []
    try:
        orgs = DL.get_supervisory_orgs()
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    if 'jrc_tag' in row:
        for atag in row['jrc_tag']:
            if atag not in tagname:
                code = orgs[atag] if atag in orgs else None
                tagname.append(atag)
                tags.append({"name": atag, "code": code})
        if tags:
            result['tags'] = tags
    result['data'] = authors
    return generate_response(result)


@app.route('/doi/janelians/<path:doi>')
def show_doi_janelians(doi):
    '''
    Return a DOI's Janelia authors
    Return information on Janelia authors for a given DOI.
    ---
    tags:
      - DOI
    parameters:
      - in: path
        name: doi
        schema:
          type: path
        required: true
        description: DOI
    responses:
      200:
        description: DOI data
      500:
        description: MongoDB error
    '''
    result = initialize_result()
    resp = show_doi_authors(doi)
    data = resp.json
    result['data'] = []
    tags = []
    for auth in data['data']:
        if auth['janelian']:
            result['data'].append(auth)
            if 'tags' in auth:
                for atag in auth['tags']:
                    if atag not in tags:
                        tags.append(atag)
    if tags:
        tags.sort()
        result['tags'] = tags
    return generate_response(result)


@app.route('/doi/migration/<path:doi>')
def show_doi_migration(doi):
    '''
    Return a DOI's migration record
    Return migration information for a given DOI.
    ---
    tags:
      - DOI
    parameters:
      - in: path
        name: doi
        schema:
          type: path
        required: true
        description: DOI
    responses:
      200:
        description: DOI data
      500:
        description: MongoDB error
    '''
    doi = doi.lstrip('/').rstrip('/').lower()
    result = initialize_result()
    try:
        row = DB['dis'].dois.find_one({"doi": doi}, {'_id': 0})
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    if not row:
        rec = []
    else:
        try:
            orgs = DL.get_supervisory_orgs()
        except Exception as err:
            raise InvalidUsage("Could not get suporgs: " + str(err), 500) from err
        try:
            rec = get_migration_data(row, orgs)
        except Exception as err:
            raise InvalidUsage(str(err), 500) from err
        rec['doi'] = doi
    result['data'] = rec
    result['rest']['source'] = 'mongo'
    result['rest']['row_count'] = len(result['data'])
    return generate_response(result)


@app.route('/doi/migrations/<string:idate>')
def show_doi_migrations(idate):
    '''
    Return migration records for DOIs inserted since a specified date
    Return migration records for DOIs inserted since a specified date.
    ---
    tags:
      - DOI
    parameters:
      - in: path
        name: idate
        schema:
          type: string
        required: true
        description: Earliest insertion date in ISO format (YYYY-MM-DD)
    responses:
      200:
        description: DOI data
      500:
        description: MongoDB error
    '''
    result = initialize_result()
    try:
        isodate = datetime.strptime(idate,'%Y-%m-%d')
    except Exception as err:
        raise InvalidUsage(str(err), 400) from err
    try:
        rows = DB['dis'].dois.find({"jrc_author": {"$exists": True},
                                    "jrc_inserted": {"$gte" : isodate}}, {'_id': 0})
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    result['rest']['row_count'] = 0
    result['rest']['source'] = 'mongo'
    result['data'] = []
    try:
        orgs = DL.get_supervisory_orgs()
    except Exception as err:
        raise InvalidUsage("Could not get suporgs: " + str(err), 500) from err
    for row in rows:
        try:
            doi = row['doi']
            rec = get_migration_data(row, orgs)
            rec['doi'] = doi
            result['data'].append(rec)
        except Exception as err:
            raise InvalidUsage(str(err), 500) from err
    result['rest']['row_count'] = len(result['data'])
    return generate_response(result)


@app.route('/doi/<path:doi>')
def show_doi(doi):
    '''
    Return a DOI
    Return Crossref or DataCite information for a given DOI.
    If it's not in the dois collection, it will be retrieved from Crossref or Datacite.
    ---
    tags:
      - DOI
    parameters:
      - in: path
        name: doi
        schema:
          type: path
        required: true
        description: DOI
    responses:
      200:
        description: DOI data
      500:
        description: MongoDB error
    '''
    doi = doi.lstrip('/').rstrip('/').lower()
    result = initialize_result()
    try:
        row = DB['dis'].dois.find_one({"doi": doi}, {'_id': 0})
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    if row:
        result['rest']['row_count'] = 1
        result['rest']['source'] = 'mongo'
        result['data'] = row
        return generate_response(result)
    result['rest']['source'], result['data'] = get_doi(doi)
    if result['data']:
        result['rest']['row_count'] = 1
    return generate_response(result)


@app.route('/doi/inserted/<string:idate>')
def show_inserted(idate):
    '''
    Return DOIs inserted since a specified date
    Return all DOIs that have been inserted since midnight on a specified date.
    ---
    tags:
      - DOI
    parameters:
      - in: path
        name: idate
        schema:
          type: string
        required: true
        description: Earliest insertion date in ISO format (YYYY-MM-DD)
    responses:
      200:
        description: DOI data
      400:
        description: bad input data
      500:
        description: MongoDB error
    '''
    result = initialize_result()
    try:
        isodate = datetime.strptime(idate,'%Y-%m-%d')
    except Exception as err:
        raise InvalidUsage(str(err), 400) from err
    try:
        rows = DB['dis'].dois.find({"jrc_inserted": {"$gte" : isodate}}, {'_id': 0})
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    result['rest']['row_count'] = 0
    result['rest']['source'] = 'mongo'
    result['data'] = []
    for row in rows:
        result['data'].append(row)
        result['rest']['row_count'] += 1
    return generate_response(result)


@app.route('/citation/<path:doi>')
@app.route('/citation/dis/<path:doi>')
def show_citation(doi):
    '''
    Return a DIS-style citation
    Return a DIS-style citation for a given DOI.
    ---
    tags:
      - DOI
    parameters:
      - in: path
        name: doi
        schema:
          type: path
        required: true
        description: DOI
    responses:
      200:
        description: DOI data
      404:
        description: DOI not found
      500:
        description: MongoDB or formatting error
    '''
    doi = doi.lstrip('/').rstrip('/').lower()
    result = initialize_result()
    try:
        row = DB['dis'].dois.find_one({"doi": doi}, {'_id': 0})
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    if not row:
        raise InvalidUsage(f"DOI {doi} is not in the database", 404)
    result['rest']['row_count'] = 1
    result['rest']['source'] = 'mongo'
    authors = DL.get_author_list(row)
    title = DL.get_title(row)
    result['data'] = f"{authors} {title}. https://doi.org/{doi}."
    if 'jrc_preprint' in row:
        result['jrc_preprint'] = row['jrc_preprint']
    return generate_response(result)


@app.route('/citations', defaults={'ctype': 'dis'}, methods=['OPTIONS', 'POST'])
@app.route('/citations/<string:ctype>', methods=['OPTIONS', 'POST'])
def show_multiple_citations(ctype='dis'):
    '''
    Return DIS-style citations
    Return a dictionary of DIS-style citations for a list of given DOIs.
    ---
    tags:
      - DOI
    parameters:
      - in: path
        name: ctype
        schema:
          type: string
        required: false
        description: Citation type (dis or flylight)
      - in: query
        name: dois
        schema:
          type: list
        required: true
        description: List of DOIs
    responses:
      200:
        description: DOI data
      500:
        description: MongoDB or formatting error
    '''
    result = initialize_result()
    ipd = receive_payload()
    if "dois" not in ipd or not (ipd['dois']) or not isinstance(ipd['dois'], list):
        raise InvalidUsage("You must specify a list of DOIs")
    result['rest']['source'] = 'mongo'
    result['data'] = {}
    for doi in ipd['dois']:
        try:
            row = DB['dis'].dois.find_one({"doi": doi.tolower()}, {'_id': 0})
        except Exception as err:
            raise InvalidUsage(str(err), 500) from err
        if not row:
            result['data'][doi] = ''
            continue
        result['rest']['row_count'] += 1
        authors = DL.get_author_list(row, style=ctype)
        title = DL.get_title(row)
        if ctype == 'dis':
            result['data'][doi] = f"{authors} {title}. https://doi.org/{doi}."
        else:
            journal = DL.get_journal(row)
            result['data'][doi] = f"{authors} {title}. {journal}."
    return generate_response(result)


@app.route('/citation/flylight/<path:doi>')
def show_flylight_citation(doi):
    '''
    Return a FlyLight-style citation
    Return a FlyLight-style citation for a given DOI.
    ---
    tags:
      - DOI
    parameters:
      - in: path
        name: doi
        schema:
          type: path
        required: true
        description: DOI
    responses:
      200:
        description: DOI data
      404:
        description: DOI not found
      500:
        description: MongoDB or formatting error
    '''
    doi = doi.lstrip('/').rstrip('/').lower()
    result = initialize_result()
    try:
        row = DB['dis'].dois.find_one({"doi": doi}, {'_id': 0})
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    if not row:
        raise InvalidUsage(f"DOI {doi} is not in the database", 404)
    result['rest']['row_count'] = 1
    result['rest']['source'] = 'mongo'
    authors = DL.get_author_list(row, style='flylight')
    title = DL.get_title(row)
    journal = DL.get_journal(row)
    result['data'] = f"{authors} {title}. {journal}."
    if 'jrc_preprint' in row:
        result['jrc_preprint'] = row['jrc_preprint']
    return generate_response(result)


@app.route('/components/<path:doi>')
def show_components(doi):
    '''
    Return components of a DIS-style citation
    Return components of a DIS-style citation for a given DOI.
    ---
    tags:
      - DOI
    parameters:
      - in: path
        name: doi
        schema:
          type: path
        required: true
        description: DOI
    responses:
      200:
        description: DOI data
      404:
        description: DOI not found
      500:
        description: MongoDB or formatting error
    '''
    doi = doi.lstrip('/').rstrip('/').lower()
    result = initialize_result()
    try:
        row = DB['dis'].dois.find_one({"doi": doi}, {'_id': 0})
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    if not row:
        raise InvalidUsage(f"DOI {doi} is not in the database", 404)
    result['rest']['row_count'] = 1
    result['rest']['source'] = 'mongo'
    result['data'] = {"authors": DL.get_author_list(row, returntype="list"),
                      "journal": DL.get_journal(row),
                      "publishing_date": DL.get_publishing_date(row),
                      "title": DL.get_title(row)
                     }
    if row['jrc_obtained_from'] == 'Crossref' and 'abstract' in row:
        result['data']['abstract'] = row['abstract']
    return generate_response(result)


@app.route('/doi/custom', methods=['OPTIONS', 'POST'])
def show_dois_custom():
    '''
    Return DOIs for a given find query
    Return a list of DOI records for a given query.
    ---
    tags:
      - DOI
    parameters:
      - in: query
        name: query
        schema:
          type: string
        required: true
        description: MongoDB query
    responses:
      200:
        description: DOI data
      500:
        description: MongoDB or formatting error
    '''
    result = initialize_result()
    ipd = receive_payload()
    if "query" not in ipd or not ipd['query']:
        raise InvalidUsage("You must specify a custom query")
    result['rest']['source'] = 'mongo'
    result['rest']['query'] = ipd['query']
    result['data'] = []
    try:
        rows = DB['dis'].dois.find(ipd['query'], {'_id': 0})
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    if not rows:
        generate_response(result)
    for row in rows:
        result['data'].append(row)
        result['rest']['row_count'] += 1
    return generate_response(result)


@app.route('/components', defaults={'ctype': 'dis'}, methods=['OPTIONS', 'POST'])
@app.route('/components/<string:ctype>', methods=['OPTIONS', 'POST'])
def show_multiple_components(ctype='dis'):
    '''
    Return DOI components for a given tag
    Return a list of citation components for a given tag.
    ---
    tags:
      - DOI
    parameters:
      - in: path
        name: ctype
        schema:
          type: string
        required: false
        description: Citation type (dis or flylight)
      - in: query
        name: tag
        schema:
          type: string
        required: true
        description: Group tag
    responses:
      200:
        description: Component data
      500:
        description: MongoDB or formatting error
    '''
    result = initialize_result()
    ipd = receive_payload()
    if "tag" not in ipd or not (ipd['tag']) or not isinstance(ipd['tag'], str):
        raise InvalidUsage("You must specify a tag")
    result['rest']['source'] = 'mongo'
    result['data'] = []
    try:
        rows = DB['dis'].dois.find({"jrc_tag": ipd['tag']}, {'_id': 0})
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    if not rows:
        generate_response(result)
    for row in rows:
        record = {"doi": row['doi'],
                  "authors": DL.get_author_list(row, style=ctype, returntype="list"),
                  "title": DL.get_title(row),
                  "journal": DL.get_journal(row),
                  "publishing_date": DL.get_publishing_date(row)
                 }
        if row['jrc_obtained_from'] == 'Crossref' and 'abstract' in row:
            record['abstract'] = row['abstract']
        result['data'].append(record)
        result['rest']['row_count'] += 1
    return generate_response(result)


@app.route('/types')
def show_types():
    '''
    Show data types
    Return DOI data types, subtypes, and counts
    ---
    tags:
      - DOI
    responses:
      200:
        description: types
      500:
        description: MongoDB error
    '''
    result = initialize_result()
    payload = [{"$group": {"_id": {"type": "$type", "subtype": "$subtype"},"count": {"$sum": 1}}}]
    try:
        rows = DB['dis'].dois.aggregate(payload)
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    result['rest']['source'] = 'mongo'
    result['data'] = {}
    for row in rows:
        if 'type' not in row['_id']:
            result['data']['datacite'] = {"count": row['count'], "subtype": None}
        else:
            typ = row['_id']['type']
            result['data'][typ] = {"count": row['count']}
            result['data'][typ]['subtype'] = row['_id']['subtype'] if 'subtype' in row['_id'] \
                                             else None
    result['rest']['row_count'] = len(result['data'])
    return generate_response(result)


@app.route('/doi/jrc_author/<path:doi>', methods=['OPTIONS', 'POST'])
def set_jrc_author(doi):
    '''
    Update Janelia authors for a given DOI
    Update Janelia authors (as employee IDs) in "jrc_author" for a given DOI.
    ---
    tags:
      - DOI
    parameters:
      - in: path
        name: doi
        schema:
          type: path
        required: true
        description: DOI
    responses:
      200:
        description: Success
      500:
        description: MongoDB or formatting error
    '''
    doi = doi.lstrip('/').rstrip('/').lower()
    result = initialize_result()
    result['data'] = []
    try:
        row = DB['dis'].dois.find_one({"doi": doi}, {'_id': 0})
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    if not row:
        raise InvalidUsage(f"Could not find DOI {doi}", 400)
    result['rest']['row_count'] = 1
    try:
        authors = DL.get_author_details(row, DB['dis'].orcid)
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    jrc_author = []
    for auth in authors:
        if auth['janelian'] and 'employeeId' in auth and auth['employeeId']:
            jrc_author.append(auth['employeeId'])
    if not jrc_author:
        return generate_response(result)
    payload = {"$set": {"jrc_author": jrc_author}}
    try:
        res = DB['dis'].dois.update_one({"doi": doi}, payload)
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    if hasattr(res, 'matched_count') and res.matched_count:
        if hasattr(res, 'modified_count') and res.modified_count:
            result['rest']['rows_updated'] = res.modified_count
        result['data'] = jrc_author
    return generate_response(result)

# ******************************************************************************
# * API endpoints (ORCID)                                                      *
# ******************************************************************************

@app.route('/orcid')
def show_oids():
    '''
    Show saved ORCID IDs
    Return information for saved ORCID IDs
    ---
    tags:
      - ORCID
    responses:
      200:
        description: ORCID data
      500:
        description: MongoDB error
    '''
    result = initialize_result()
    try:
        rows = DB['dis'].orcid.find({}, {'_id': 0}).collation({"locale": "en"}).sort("family", 1)
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    result['rest']['source'] = 'mongo'
    result['data'] = []
    for row in rows:
        result['data'].append(row)
    result['rest']['row_count'] = len(result['data'])
    return generate_response(result)


@app.route('/orcid/<string:oid>')
def show_oid(oid):
    '''
    Show an ORCID ID
    Return information for an ORCID ID or name
    ---
    tags:
      - ORCID
    parameters:
      - in: path
        name: oid
        schema:
          type: string
        required: true
        description: ORCID ID, given name, or family name
    responses:
      200:
        description: ORCID data
      500:
        description: MongoDB error
    '''
    result = initialize_result()
    if re.match(r'([0-9A-Z]{4}-){3}[0-9A-Z]+', oid):
        payload = {"orcid": oid}
    else:
        payload = {"$or": [{"family": {"$regex": oid, "$options" : "i"}},
                           {"given": {"$regex": oid, "$options" : "i"}}]
                  }
    try:
        rows = DB['dis'].orcid.find(payload, {'_id': 0})
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    result['rest']['source'] = 'mongo'
    result['data'] = []
    for row in rows:
        result['data'].append(row)
    return generate_response(result)


@app.route('/orcidapi/<string:oid>')
def show_oidapi(oid):
    '''
    Show an ORCID ID (using the ORCID API)
    Return information for an ORCID ID (using the ORCID API)
    ---
    tags:
      - ORCID
    parameters:
      - in: path
        name: oid
        schema:
          type: string
        required: true
        description: ORCID ID
    responses:
      200:
        description: ORCID data
    '''
    result = initialize_result()
    url = f"https://pub.orcid.org/v3.0/{oid}"
    try:
        resp = requests.get(url, headers={"Accept": "application/json"}, timeout=10)
        result['data'] = resp.json()
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    if 'error-code' not in result['data']:
        result['rest']['source'] = 'orcid'
        result['rest']['row_count'] = 1
    return generate_response(result)


# ******************************************************************************
# * UI endpoints (general)                                                     *
# ******************************************************************************
@app.route('/download/<string:fname>')
def download(fname):
    ''' Downloadable content
    '''
    try:
        return send_file('/tmp/' + fname, download_name=fname)  # pylint: disable=E1123
    except Exception as err:
        return render_template("error.html", urlroot=request.url_root,
                               title='Download error', message=err)


@app.route('/')
@app.route('/home')
def show_home():
    ''' Home
    '''
    response = make_response(render_template('home.html', urlroot=request.url_root,
                                             navbar=generate_navbar('Home')))
    return response

# ******************************************************************************
# * UI endpoints (DOI)                                                         *
# ******************************************************************************
@app.route('/doiui/<path:doi>')
def show_doi_ui(doi):
    ''' Show DOI
    '''
    # pylint: disable=too-many-return-statements
    doi = doi.lstrip('/').rstrip('/').lower()
    try:
        row = DB['dis'].dois.find_one({"doi": doi})
    except Exception as err:
        return inspect_error(err, 'Could not get DOI')
    if row:
        html = '<h5 style="color:lime">This DOI is saved locally in the Janelia database</h5>'
        html += add_jrc_fields(row)
    else:
        html = '<h5 style="color:red">This DOI is not saved locally in the ' \
               + 'Janelia database</h5><br>'
    _, data = get_doi(doi)
    if not data:
        return render_template('warning.html', urlroot=request.url_root,
                                title=render_warning("Could not find DOI", 'warning'),
                                message=f"Could not find DOI {doi}")
    authors = DL.get_author_list(data, orcid=True)
    if not authors:
        return render_template('error.html', urlroot=request.url_root,
                                title=render_warning("Could not generate author list"),
                                message=f"Could not generate author list for {doi}")
    title = DL.get_title(data)
    if not title:
        return render_template('error.html', urlroot=request.url_root,
                                title=render_warning("Could not find title"),
                                message=f"Could not find title for {doi}")
    citation = f"{authors} {title}."
    journal = DL.get_journal(data)
    if not journal:
        return render_template('error.html', urlroot=request.url_root,
                                title=render_warning("Could not find journal"),
                                message=f"Could not find journal for {doi}")
    link = f"<a href='https://dx.doi.org/{doi}' target='_blank'>{doi}</a>"
    rlink = f"/doi/{doi}"
    chead = 'Citation'
    if 'type' in data:
        chead += f" for {data['type'].replace('-', ' ')}"
        if 'subtype' in data:
            chead += f" {data['subtype'].replace('-', ' ')}"
    html += f"<h4>{chead}</h4><span class='citation'>{citation} {journal}." \
            + f"<br>DOI: {link}</span> {tiny_badge('primary', 'Raw data', rlink)}<br><br>"
    html += add_relations(data)
    if row:
        try:
            authors = DL.get_author_details(row, DB['dis'].orcid)
        except Exception as err:
            return inspect_error(err, 'Could not get author list details')
        alist, count = show_tagged_authors(authors)
        if alist:
            html += f"<br><h4>Janelia authors ({count})</h4>" \
                    + f"<div class='scroll'>{''.join(alist)}</div>"
    response = make_response(render_template('general.html', urlroot=request.url_root,
                                             title=doi, html=html,
                                             navbar=generate_navbar('DOIs')))
    return response


@app.route('/doisui/<string:name>')
def show_doi_by_name_ui(name):
    ''' Show DOIs for a name
    '''
    payload = {'$or': [{"author.family": {"$regex": f"^{name}$", "$options" : "i"}},
                       {"creators.familyName": {"$regex": f"^{name}$", "$options" : "i"}},
                       {"creators.name": {"$regex": f"{name}$", "$options" : "i"}},
                      ]}
    try:
        rows = DB['dis'].dois.find(payload).collation({"locale": "en"}).sort("doi", 1)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get DOIs from dois collection"),
                               message=error_message(err))
    html, dois = generate_works_table(rows, name)
    if not html:
        return render_template('warning.html', urlroot=request.url_root,
                               title=render_warning("Could not find DOIs", 'warning'),
                               message=f"Could not find any DOIs with author name matching {name}")
    response = make_response(render_template('general.html', urlroot=request.url_root,
                                             title=f"DOIs for {name} ({len(dois):,})", html=html,
                                             navbar=generate_navbar('DOIs')))
    return response


@app.route('/dois_author')
def dois_author():
    ''' Show first/last authors
    '''
    source = {}
    for src in ('Crossref', 'DataCite', 'Crossref-none', 'DataCite-none'):
        payload = {"jrc_obtained_from": src,
                   "$or": [{"jrc_first_author": {"$exists": True}},
                           {"jrc_last_author": {"$exists": True}}]}
        if '-none' in src:
            payload = {"jrc_obtained_from": src.replace('-none', '')}
        try:
            cnt = DB['dis'].dois.count_documents(payload)
            source[src] = cnt
        except Exception as err:
            return render_template('error.html', urlroot=request.url_root,
                                   title=render_warning("Could not get authorship " \
                                                        + "from dois collection"),
                                   message=error_message(err))
    html = '<table id="authors" class="tablesorter numbers"><thead><tr>' \
           + '<th>Authorship</th><th>Crossref</th><th>DataCite</th>' \
           + '</tr></thead><tbody>'
    data = {}
    for src in SOURCES:
        data[src] = source[src]
    html += f"<tr><td>First and/or last</td><td>{source['Crossref']:,}</td>" \
            + f"<td>{source['DataCite']:,}</td></tr>"
    html += f"<tr><td>Additional only</td><td>{source['Crossref-none']-source['Crossref']:,}</td>" \
            + f"<td>{source['DataCite-none']-source['DataCite']:,}</td></tr>"
    html += '</tbody></table>'
    data['Additional'] = source['Crossref-none'] + source['DataCite-none'] - source['Crossref'] \
                         - source['DataCite']
    chartscript, chartdiv = DP.pie_chart(data, "DOIs by authorship", "source",
                                         colors=DP.SOURCE3_PALETTE)
    response = make_response(render_template('bokeh.html', urlroot=request.url_root,
                                             title="DOI authorship", html=html,
                                             chartscript=chartscript, chartdiv=chartdiv,
                                             navbar=generate_navbar('DOIs')))
    return response


@app.route('/dois_source')
def dois_source():
    ''' Show data sources
    '''
    payload = [{"$group": {"_id": {"source": "$jrc_obtained_from", "type": "$type",
                                   "subtype": "$subtype"},
                           "count": {"$sum": 1}}},
               {"$sort" : {"count": -1}}]
    try:
        rows = DB['dis'].dois.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get types from dois collection"),
                               message=error_message(err))
    html = '<table id="types" class="tablesorter numberlast"><thead><tr>' \
           + '<th>Source</th><th>Type</th><th>Subtype</th><th>Count</th>' \
           + '</tr></thead><tbody>'
    data = {}
    for row in rows:
        if row['_id']['source'] not in data:
            data[row['_id']['source']] = 0
        for field in ('source', 'type', 'subtype'):
            if field not in row['_id']:
                row['_id'][field] = ''
        data[row['_id']['source']] += row['count']
        html += f"<tr><td>{row['_id']['source']}</td><td>{row['_id']['type']}</td>" \
                + f"<td>{row['_id']['subtype']}</td><td>{row['count']:,}</td></tr>"
    html += '</tbody></table>'
    chartscript, chartdiv = DP.pie_chart(data, "DOIs by source", "source",
                                         colors=DP.SOURCE_PALETTE)
    response = make_response(render_template('bokeh.html', urlroot=request.url_root,
                                             title="DOI sources", html=html,
                                             chartscript=chartscript, chartdiv=chartdiv,
                                             navbar=generate_navbar('DOIs')))
    return response


@app.route('/dois_preprint')
def dois_preprint():
    ''' Show preprints
    '''
    source = {}
    for src in SOURCES:
        payload = {"jrc_obtained_from": src, "jrc_preprint": {"$exists": False}}
        if src == 'Crossref':
            payload['type'] = {"$in": ["journal-article", "posted-content"]}
        else:
            payload['doi'] = {"$not": {"$regex": "janelia|zenodo"}}
        try:
            cnt = DB['dis'].dois.count_documents(payload)
            source[src] = cnt
        except Exception as err:
            return render_template('error.html', urlroot=request.url_root,
                                   title=render_warning("Could not get source counts " \
                                                        + "from dois collection"),
                                   message=error_message(err))
    payload = [{"$match": {"jrc_preprint": {"$exists": True}}},
               {"$group": {"_id": {"type": "$type", "preprint": "$preprint"},"count": {"$sum": 1}}}]
    try:
        rows = DB['dis'].dois.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get preprint counts " \
                                                    + "from dois collection"),
                               message=error_message(err))
    data = {'Has preprint relation': 0}
    preprint = {}
    for row in rows:
        if 'type' in row['_id']:
            preprint[row['_id']['type']] = row['count']
            data['Has preprint relation'] += row['count']
        else:
            preprint['DataCite'] = row['count']
            data['Has preprint relation'] += row['count']
    html = '<table id="preprints" class="tablesorter numbers"><thead><tr>' \
           + '<th>Status</th><th>Crossref</th><th>DataCite</th>' \
           + '</tr></thead><tbody>'
    html += "<tr><td>Preprints with journal articles</td>" \
            + f"<td>{preprint['journal-article']:,}</td><td>{preprint['DataCite']}</td></tr>"
    html += f"<tr><td>Journal articles with preprints</td><td>{preprint['posted-content']:,}</td>" \
            + "<td>0</td></tr>"
    html += f"<tr><td>No preprint relation</td><td>{source['Crossref']:,}</td>" \
            + f"<td>{source['DataCite']:,}</td></tr>"
    html += '</tbody></table>'
    data['No preprint relation'] = source['Crossref'] + source['DataCite']
    chartscript, chartdiv = DP.pie_chart(data, "DOIs by preprint status", "source",
                                         colors=DP.SOURCE_PALETTE, width=500)
    # Preprint types
    try:
        chartscript2, chartdiv2 = DP.preprint_type_piechart(DB['dis'].dois)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get preprint counts " \
                                                    + "from dois collection"),
                               message=error_message(err))
    # Preprint capture
    try:
        chartscript3, chartdiv3 = DP.preprint_capture_piechart(DB['dis'].dois)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get preprint counts " \
                                                    + "from dois collection"),
                               message=error_message(err))
    response = make_response(render_template('bokeh.html', urlroot=request.url_root,
                                             title="DOI preprint status", html=html,
                                             chartscript=chartscript+chartscript2+chartscript3,
                                             chartdiv=chartdiv+chartdiv2+chartdiv3,
                                             navbar=generate_navbar('DOIs')))
    return response


@app.route('/dois_publisher')
def dois_publisher():
    ''' Show publishers with counts
    '''
    payload = [{"$group": {"_id": {"publisher": "$publisher", "source": "$jrc_obtained_from"},
                           "count":{"$sum": 1}}},
               {"$sort": {"_id.publisher": 1}}
              ]
    try:
        rows = DB['dis'].dois.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get publishers " \
                                                    + "from dois collection"),
                               message=error_message(err))
    html = '<table id="types" class="tablesorter numbers"><thead><tr>' \
           + '<th>Publisher</th><th>Crossref</th><th>DataCite</th>' \
           + '</tr></thead><tbody>'
    pubs = {}
    for row in rows:
        if row['_id']['publisher'] not in pubs:
            pubs[row['_id']['publisher']] = {}
        if row['_id']['source'] not in pubs[row['_id']['publisher']]:
            pubs[row['_id']['publisher']][row['_id']['source']] = row['count']
    for pub, val in pubs.items():
        onclick = "onclick='nav_post(\"publisher\",\"" + pub + "\")'"
        link = f"<a href='#' {onclick}>{pub}</a>"
        html += f"<tr><td>{link}</td>"
        for source in SOURCES:
            if source in val:
                onclick = "onclick='nav_post(\"publisher\",\"" + pub \
                          + "\",\"" + source + "\")'"
                link = f"<a href='#' {onclick}>{val[source]:,}</a>"
            else:
                link = ""
            html += f"<td>{link}</td>"
        html += "</tr>"
    html += '</tbody></table>'
    response = make_response(render_template('general.html', urlroot=request.url_root,
                                             title=f"DOI publishers ({len(pubs):,})", html=html,
                                             navbar=generate_navbar('DOIs')))
    return response


@app.route('/dois_tag')
def dois_tag():
    ''' Show tags with counts
    '''
    payload = [{"$unwind" : "$jrc_tag"},
               {"$project": {"_id": 0, "jrc_tag": 1, "jrc_obtained_from": 1}},
               {"$group": {"_id": {"tag": "$jrc_tag", "source": "$jrc_obtained_from"},
                           "count":{"$sum": 1}}},
               {"$sort": {"_id.tag": 1}}
              ]
    try:
        rows = DB['dis'].dois.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get tags from dois collection"),
                               message=error_message(err))
    html = '<table id="types" class="tablesorter numbers"><thead><tr>' \
           + '<th>Tag</th><th>Crossref</th><th>DataCite</th>' \
           + '</tr></thead><tbody>'
    tags = {}
    for row in rows:
        if row['_id']['tag'] not in tags:
            tags[row['_id']['tag']] = {}
        if row['_id']['source'] not in tags[row['_id']['tag']]:
            tags[row['_id']['tag']][row['_id']['source']] = row['count']
    for tag, val in tags.items():
        onclick = "onclick='nav_post(\"jrc_tag\",\"" + tag + "\")'"
        link = f"<a href='#' {onclick}>{tag}</a>"
        html += f"<tr><td>{link}</td>"
        for source in SOURCES:
            if source in val:
                onclick = "onclick='nav_post(\"jrc_tag\",\"" + tag \
                          + "\",\"" + source + "\")'"
                link = f"<a href='#' {onclick}>{val[source]:,}</a>"
            else:
                link = ""
            html += f"<td>{link}</td>"
        html += "</tr>"
    html += '</tbody></table>'
    response = make_response(render_template('general.html', urlroot=request.url_root,
                                             title=f"DOI tags ({len(tags):,})", html=html,
                                             navbar=generate_navbar('DOIs')))
    return response


@app.route('/dois_top', defaults={'num': 10})
@app.route('/dois_top/<int:num>')
def dois_top(num):
    ''' Show a chart of DOIs by top tags
    '''
    payload = [{"$unwind" : "$jrc_tag"},
               {"$project": {"_id": 0, "jrc_tag": 1, "jrc_publishing_date": 1}},
               {"$group": {"_id": {"tag": "$jrc_tag",
                                   "year": {"$substrBytes": ["$jrc_publishing_date", 0, 4]}},
                           "count": {"$sum": 1}},
                },
               {"$sort": {"_id.year": 1, "_id.tag": 1}}
              ]
    try:
        rows = DB['dis'].dois.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get tags from dois collection"),
                               message=error_message(err))
    html = ""
    ytags = {}
    tags = {}
    data = {"years": []}
    for row in rows:
        if row['_id']['tag'] not in tags:
            tags[row['_id']['tag']] = 0
        tags[row['_id']['tag']] += row['count']
        if row['_id']['year'] not in ytags:
            ytags[row['_id']['year']] = {}
            data['years'].append(row['_id']['year'])
        if row['_id']['tag'] not in ytags[row['_id']['year']]:
            ytags[row['_id']['year']][row['_id']['tag']] = row['count']
    top = sorted(tags, key=tags.get, reverse=True)[:num]
    for year in data['years']:
        for tag in sorted(tags):
            if tag not in top:
                continue
            if tag not in data:
                data[tag] = []
            if tag in ytags[year]:
                data[tag].append(ytags[year][tag])
            else:
                data[tag].append(0)
    chartscript, chartdiv = DP.stacked_bar_chart(data, f"DOIs published by year for top {num} tags",
                                                 xaxis="years", yaxis=top)
    response = make_response(render_template('bokeh.html', urlroot=request.url_root,
                                             title="DOI tags by year/tag", html=html,
                                             chartscript=chartscript, chartdiv=chartdiv,
                                             navbar=generate_navbar('DOIs')))
    return response


@app.route('/dois_report/<string:year>')
@app.route('/dois_report')
def dois_report(year=str(datetime.now().year)):
    ''' Show publishers with counts
    '''
    pmap = {"journal-article": "Journal articles", "posted-content": "Preprints",
            "proceedings-article": "Proceedings articles", "book-chapter": "Book chapters",
            "datasets": "Datasets", "peer-review": "Peer reviews", "grant": "Grants",
            "other": "Other"}
    payload = [{"$match": {"jrc_publishing_date": {"$regex": "^"+ year}}},
               {"$group": {"_id": {"type": "$type", "subtype": "$subtype"}, "count": {"$sum": 1}}}
              ]
    try:
        rows = DB['dis'].dois.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get yearly metrics " \
                                                    + "from dois collection"),
                               message=error_message(err))
    typed = {}
    for row in rows:
        typ = row['_id']['type'] if 'type' in row['_id'] else "DataCite"
        sub = row['_id']['subtype'] if 'subtype' in row['_id'] else ""
        if sub == 'preprint':
            typ = 'posted-content'
        if typ not in typed:
            typed[typ] = 0
        typed[typ] += row['count']
    payload = [{"$match": {"jrc_publishing_date": {"$regex": "^"+ year},
                           "jrc_first_author": {"$exists": True}}},
               {"$group": {"_id": {"type": "$type", "subtype": "$subtype"}, "count": {"$sum": 1}}}
              ]
    first = {}
    try:
        rows = DB['dis'].dois.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get yearly metrics " \
                                                    + "from dois collection"),
                               message=error_message(err))
    for row in rows:
        typ = row['_id']['type'] if 'type' in row['_id'] else "DataCite"
        sub = row['_id']['subtype'] if 'subtype' in row['_id'] else ""
        if sub == 'preprint':
            typ = 'posted-content'
        if typ not in first:
            first[typ] = 0
        first[typ] += row['count']
    html = ""
    for key, val in pmap.items():
        if key in typed:
            additional = ""
            if key in first:
                additional = f" ({first[key]:,} with first author)"
            html += f"<h4>{val}: {typed[key]:,}{additional}</h4>"
    if 'DataCite' in typed:
        html += f"<h4>DataCite entries: {typed['DataCite']:,}</h4>"
    response = make_response(render_template('general.html', urlroot=request.url_root,
                                             title=f"{year}", html=html,
                                             navbar=generate_navbar('DOIs')))
    return response


@app.route('/dois_year')
def dois_year():
    ''' Show publishing years with counts
    '''
    payload = [{"$group": {"_id": {"year": {"$substrBytes": ["$jrc_publishing_date", 0, 4]},
                                   "source": "$jrc_obtained_from"
                                  },
                           "count": {"$sum": 1}}},
               {"$sort": {"_id.pdate": -1}}
              ]
    try:
        rows = DB['dis'].dois.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get tags from dois collection"),
                               message=error_message(err))
    html = '<table id="years" class="tablesorter numbers"><thead><tr>' \
           + '<th>Year</th><th>Crossref</th><th>DataCite</th>' \
           + '</tr></thead><tbody>'
    years = {}
    for row in rows:
        if row['_id']['year'] not in years:
            years[row['_id']['year']] = {}
        if row['_id']['source'] not in years[row['_id']['year']]:
            years[row['_id']['year']][row['_id']['source']] = row['count']
    data = {"years": [], "Crossref": [], "DataCite": []}
    for year in sorted(years, reverse=True):
        data['years'].insert(0, str(year))
        onclick = "onclick='nav_post(\"publishing_year\",\"" + year + "\")'"
        link = f"<a href='#' {onclick}>{year}</a>"
        html += f"<tr><td>{link}</td>"
        for source in SOURCES:
            if source in years[year]:
                data[source].insert(0, years[year][source])
                onclick = "onclick='nav_post(\"publishing_year\",\"" + year \
                          + "\",\"" + source + "\")'"
                link = f"<a href='#' {onclick}>{years[year][source]:,}</a>"
            else:
                data[source].insert(0, 0)
                link = ""
            html += f"<td>{link}</td>"
        html += "</tr>"
    html += '</tbody></table>'
    chartscript, chartdiv = DP.stacked_bar_chart(data, "DOIs published by year/source",
                                                 xaxis="years", yaxis=SOURCES,
                                                 colors=DP.SOURCE_PALETTE)
    response = make_response(render_template('bokeh.html', urlroot=request.url_root,
                                             title="DOIs published by year", html=html,
                                             chartscript=chartscript, chartdiv=chartdiv,
                                             navbar=generate_navbar('DOIs')))
    return response


@app.route('/dois_insertpicker')
def show_insert_picker():
    '''
    Show a datepicker for selecting DOIs inserted since a specified date
    '''
    before = "Select a minimum DOI insertion date"
    start = last_thursday()
    after = '<a class="btn btn-success" role="button" onclick="startdate(); return False;">' \
            + 'Look up DOIs</a>'
    return make_response(render_template('picker.html', urlroot=request.url_root,
                                         title="DOI lookup by insertion date", before=before,
                                         start=start, stop=str(date.today()),
                                         after=after, navbar=generate_navbar('DOIs')))


@app.route('/doiui/insert/<string:idate>')
def show_insert(idate):
    '''
    Return DOIs that have been inserted since a specified date
    '''
    try:
        isodate = datetime.strptime(idate,'%Y-%m-%d')
    except Exception as err:
        raise InvalidUsage(str(err), 400) from err
    try:
        rows = DB['dis'].dois.find({"jrc_inserted": {"$gte" : isodate}},
                                   {'_id': 0}).sort("jrc_inserted", 1)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get DOIs"),
                               message=error_message(err))
    if not rows:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("DOIs not found"),
                               message=f"No DOIs were inserted on or after {idate}")
    html = '<table id="dois" class="tablesorter numbers"><thead><tr>' \
           + '<th>DOI</th><th>Source</th><th>Type</th><th>Published</th><th>Load source</th>' \
           + '<th>Inserted</th><th>Is version of</th><th>Newsletter</th></tr></thead><tbody>'
    fileoutput = ""
    for row in rows:
        source = row['jrc_load_source'] if row['jrc_load_source'] else ""
        typ = row['type'] if 'type' in row else ""
        if 'subtype' in row:
            typ += f" {row['subtype']}"
        version = []
        if 'relation' in row and 'is-version-of' in row['relation']:
            for ver in row['relation']['is-version-of']:
                if ver['id-type'] == 'doi':
                    version.append(ver['id'])
        version = ", ".join(version) if version else ""
        link = f"<a href='/doiui/{row['doi']}'>{row['doi']}</a>"
        news = row['jrc_newsletter'] if 'jrc_newsletter' in row else ""
        html += "<tr><td>" + "</td><td>".join([link, row['jrc_obtained_from'], typ,
                                              row['jrc_publishing_date'], source,
                                              str(row['jrc_inserted']), version,
                                              news]) + "</td></tr>"
        frow = "\t".join([row['doi'], row['jrc_obtained_from'], typ, row['jrc_publishing_date'],
                          source, str(row['jrc_inserted']), version, news])
        fileoutput += f"{frow}\n"
    html += '</tbody></table>'
    html = create_downloadable("jrc_inserted", None, fileoutput) + html
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=f"DOIs inserted on or after {idate}", html=html,
                                         navbar=generate_navbar('DOIs')))


@app.route('/doiui/custom', methods=['OPTIONS', 'POST'])
def show_doiui_custom():
    '''
    Return DOIs for a given find query
    Return a list of DOI records for a given query.
    ---
    tags:
      - DOI
    parameters:
      - in: query
        name: field
        schema:
          type: string
        required: true
        description: MongoDB field
      - in: query
        name: value
        schema:
          type: string
        required: true
        description: field value
    responses:
      200:
        description: DOI data
      500:
        description: MongoDB or formatting error
    '''
    ipd = receive_payload()
    for row in ('field', 'value'):
        if row not in ipd or not ipd[row]:
            return render_template('error.html', urlroot=request.url_root,
                                   title=render_warning(f"Missing {row}"),
                                   message=f"You must specify a {row}")
    display_value = ipd['value']
    payload, ptitle = get_custom_payload(ipd, display_value)
    print(f"Custom payload: {payload}")
    try:
        rows = DB['dis'].dois.find(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get DOIs"),
                               message=error_message(err))
    if not rows:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("DOIs not found"),
                               message=f"No DOIs were found for {ipd['field']}={display_value}")
    header = ['Published', 'DOI', 'Title']
    html = "<table id='dois' class='tablesorter standard'><thead><tr>" \
           + ''.join([f"<th>{itm}</th>" for itm in header]) + "</tr></thead><tbody>"
    works = []
    for row in rows:
        published = DL.get_publishing_date(row)
        title = DL.get_title(row)
        if not title:
            title = ""
        link = f"<a href='/doiui/{row['doi']}'>{row['doi']}</a>"
        works.append({"published": published, "link": link, "title": title, "doi": row['doi']})
    fileoutput = ""
    for row in sorted(works, key=lambda row: row['published'], reverse=True):
        html += "<tr><td>" + dloop(row, ['published', 'link', 'title'], "</td><td>") + "</td></tr>"
        row['title'] = row['title'].replace("\n", " ")
        fileoutput += dloop(row, ['published', 'doi', 'title']) + "\n"
    html += '</tbody></table>'
    html = create_downloadable(ipd['field'], header, fileoutput) + html
    response = make_response(render_template('general.html', urlroot=request.url_root,
                                             title=ptitle, html=html,
                                             navbar=generate_navbar('DOIs')))
    return response


# ******************************************************************************
# * UI endpoints (ORCID)                                                       *
# ******************************************************************************
@app.route('/orcidui/<string:oid>')
def show_oid_ui(oid):
    ''' Show ORCID user
    '''
    try:
        resp = requests.get(f"https://pub.orcid.org/v3.0/{oid}",
                            headers={"Accept": "application/json"}, timeout=10)
        data = resp.json()
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not retrieve ORCID ID"),
                               message=error_message(err))
    if 'person' not in data:
        return render_template('warning.html', urlroot=request.url_root,
                               title=render_warning(f"Could not find ORCID ID {oid}", 'warning'),
                               message=data['user-message'])
    name = data['person']['name']
    if name['credit-name']:
        who = f"{name['credit-name']['value']}"
    elif 'family-name' not in name or not name['family-name']:
        who = f"{name['given-names']['value']} <span style='color: red'>" \
              + "(Family name is missing in ORCID)</span>"
    else:
        who = f"{name['given-names']['value']} {name['family-name']['value']}"
    try:
        orciddata, dois = get_orcid_from_db(oid, both=True)
    except CustomException as err:
        return render_template('error.html', urlroot=request.url_root,
                                title=render_warning(f"Could not find ORCID ID {oid}", 'error'),
                                message=error_message(err))
    if not orciddata:
        return render_template('warning.html', urlroot=request.url_root,
                               title=render_warning(f"Could not find ORCID ID {oid}", 'warning'),
                               message="Could not find any information for this ORCID ID")
    html = f"<h3>{who}</h3>{orciddata}"
    # Works
    if 'works' in data['activities-summary'] and data['activities-summary']['works']['group']:
        html += add_orcid_works(data, dois)
    response = make_response(render_template('general.html', urlroot=request.url_root,
                                             title=f"<a href='https://orcid.org/{oid}' " \
                                                   + f"target='_blank'>{oid}</a>", html=html,
                                             navbar=generate_navbar('ORCID')))
    return response


@app.route('/userui/<string:eid>')
def show_user_ui(eid):
    ''' Show user record by employeeId
    '''
    try:
        orciddata, _ = get_orcid_from_db(eid, use_eid=True)
    except CustomException as err:
        return render_template('error.html', urlroot=request.url_root,
                                title=render_warning(f"Could not find employee ID {eid}",
                                                     'warning'),
                                message=error_message(err))
    if not orciddata:
        return render_template('warning.html', urlroot=request.url_root,
                               title=render_warning(f"Could not find employee ID {eid}", 'warning'),
                               message="Could not find any information for this employee ID")
    response = make_response(render_template('general.html', urlroot=request.url_root,
                                             title=f"Employee ID {eid}", html=orciddata,
                                             navbar=generate_navbar('ORCID')))
    return response


@app.route('/unvaluserui/<string:iid>')
def show_unvaluser_ui(iid):
    ''' Show user record by orcid collection ID
    '''
    try:
        orciddata, _ = get_orcid_from_db(iid, bare=True)
    except CustomException as err:
        return render_template('error.html', urlroot=request.url_root,
                                title=render_warning(f"Could not find orcid collection ID {iid}",
                                                     'warning'),
                                message=error_message(err))
    if not orciddata:
        return render_template('warning.html', urlroot=request.url_root,
                               title=render_warning(f"Could not find ID {iid}", 'warning'),
                               message="Could not find any information for this orcid " \
                                       + "collection ID")
    response = make_response(render_template('general.html', urlroot=request.url_root,
                                             title="User has no ORCID or employee ID",
                                             html=orciddata,
                                             navbar=generate_navbar('ORCID')))
    return response


@app.route('/namesui/<string:name>')
def show_names_ui(name):
    ''' Show user names
    '''
    payload = {"$or": [{"family": {"$regex": name, "$options" : "i"}},
                       {"given": {"$regex": name, "$options" : "i"}},
                      ]}
    try:
        if not DB['dis'].orcid.count_documents(payload):
            return render_template('warning.html', urlroot=request.url_root,
                                   title=render_warning("Could not find name", 'warning'),
                                    message=f"Could not find any names matching {name}")
        rows = DB['dis'].orcid.find(payload).collation({"locale": "en"}).sort("family", 1)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not count names in dois collection"),
                               message=error_message(err))
    html, count = generate_user_table(rows)
    html = f"Search term: {name}<br>" + html
    response = make_response(render_template('general.html', urlroot=request.url_root,
                                             title=f"Users: {count:,}", html=html,
                                             navbar=generate_navbar('ORCID')))
    return response


@app.route('/orcid_tag')
def orcid_tag():
    ''' Show ORCID tags (affiliations) with counts
    '''
    payload = [{"$unwind" : "$affiliations"},
               {"$project": {"_id": 0, "affiliations": 1}},
               {"$group": {"_id": "$affiliations", "count":{"$sum": 1}}},
               {"$sort": {"_id": 1}}
              ]
    try:
        rows = DB['dis'].orcid.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get affiliations " \
                                                    + "from orcid collection"),
                               message=error_message(err))
    html = '<table id="types" class="tablesorter numberlast"><thead><tr>' \
           + '<th>Affiliation</th><th>Count</th>' \
           + '</tr></thead><tbody>'
    count = 0
    for row in rows:
        count += 1
        link = f"<a href='/affiliation/{row['_id']}'>{row['_id']}</a>"
        html += f"<tr><td>{link}</td><td>{row['count']:,}</td></tr>"
    html += '</tbody></table>'
    response = make_response(render_template('general.html', urlroot=request.url_root,
                                             title=f"ORCID affiliations ({count:,})", html=html,
                                             navbar=generate_navbar('ORCID')))
    return response


@app.route('/orcid_entry')
def orcid_entry():
    ''' Show ORCID users with counts
    '''
    payload = {"$and": [{"orcid": {"$exists": True}}, {"employeeId": {"$exists": True}},
                        {"alumni": {"$exists": False}}]}
    try:
        cntb = DB['dis'].orcid.count_documents(payload)
        payload["$and"][1]["employeeId"]["$exists"] = False
        cnto = DB['dis'].orcid.count_documents(payload)
        payload["$and"][0]["orcid"]["$exists"] = False
        payload["$and"][1]["employeeId"]["$exists"] = True
        cnte = DB['dis'].orcid.count_documents(payload)
        cntj = DB['dis'].orcid.count_documents({"alumni": {"$exists": False}})
        cnta = DB['dis'].orcid.count_documents({"alumni": {"$exists": True}})
        payload = {"$and": [{"affiliations": {"$exists": False}}, {"group": {"$exists": False}},
                            {"alumni": {"$exists": False}}]}
        cntf = DB['dis'].orcid.count_documents(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get affiliations " \
                                                    + "from orcid collection"),
                               message=error_message(err))
    total = cntj + cnta
    data = {}
    html = '<table id="types" class="tablesorter standard"><tbody>'
    html += f"<tr><td>Entries in collection</td><td>{total:,}</td></tr>"
    html += f"<tr><td>Current Janelians</td><td>{cntj:,} ({cntj/total*100:.2f}%)</td></tr>"
    html += f"<tr><td>&nbsp;&nbsp;Janelians with ORCID and employee ID</td><td>{cntb:,}" \
            + f" ({cntb/cntj*100:.2f}%)</td></tr>"
    data['Janelians with ORCID and employee ID'] = cntb
    html += f"<tr><td>&nbsp;&nbsp;Janelians with ORCID only</td><td>{cnto:,}" \
            + f" ({cnto/cntj*100:.2f}%)</td></tr>"
    data['Janelians with ORCID only'] = cnto
    html += f"<tr><td>&nbsp;&nbsp;Janelians with employee ID only</td><td>{cnte:,}" \
            + f" ({cnte/cntj*100:.2f}%)</td></tr>"
    data['Janelians with employee ID only'] = cnte
    html += f"<tr><td>&nbsp;&nbsp;Janelians without affiliations/groups</td><td>{cntf:,}</td></tr>"
    html += f"<tr><td>Alumni</td><td>{cnta:,} ({cnta/total*100:.2f}%)</td></tr>"
    data['Alumni'] = cnta
    html += '</tbody></table>'
    chartscript, chartdiv = DP.pie_chart(data, "ORCID entries", "type", height=500, width=600,
                                         colors=DP.TYPE_PALETTE, location="top_right")
    response = make_response(render_template('bokeh.html', urlroot=request.url_root,
                                             title="ORCID entries", html=html,
                                             chartscript=chartscript, chartdiv=chartdiv,
                                             navbar=generate_navbar('ORCID')))
    return response


@app.route('/affiliation/<string:aff>')
def orcid_affiliation(aff):
    ''' Show ORCID tags (affiliations) with counts
    '''
    payload = {"jrc_tag": aff}
    try:
        cnt = DB['dis'].dois.count_documents(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not count affiliations " \
                                                    + "in dois collection"),
                               message=error_message(err))
    html = f"<p>Number of tagged DOIs: {cnt:,}</p>"
    payload = {"affiliations": aff}
    try:
        rows = DB['dis'].orcid.find(payload).collation({"locale": "en"}).sort("family", 1)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get affiliations from " \
                                                    + "orcid collection"),
                               message=error_message(err))
    additional, count = generate_user_table(rows)
    response = make_response(render_template('general.html', urlroot=request.url_root,
                                             title=f"{aff} affiliation ({count:,})",
                                             html=html + additional,
                                             navbar=generate_navbar('ORCID')))
    return response

# ******************************************************************************
# * UI endpoints (stats)                                                       *
# ******************************************************************************
@app.route('/stats_database')
def stats_database():
    ''' Show database stats
    '''
    collection = {}
    try:
        cnames = DB['dis'].list_collection_names()
        for cname in cnames:
            stat = DB['dis'].command('collStats', cname)
            indices = []
            for key, val in stat['indexSizes'].items():
                indices.append(f"{key} ({humansize(val)})")
            free = stat['freeStorageSize'] / stat['storageSize'] * 100
            collection[cname] = {"docs": f"{stat['count']:,}",
                                 "size": humansize(stat['size']),
                                 "free": f"{free:.2f}%",
                                 "idx": ", ".join(indices)
                                }
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get collection stats"),
                               message=error_message(err))
    html = '<table id="collections" class="tablesorter numbercenter"><thead><tr>' \
           + '<th>Collection</th><th>Documents</th><th>Size</th><th>Free space</th>' \
           + '<th>Indices</th></tr></thead><tbody>'
    for coll, val in sorted(collection.items()):
        html += f"<tr><td>{coll}</td><td>" + dloop(val, ['docs', 'size', 'free', 'idx'],
                                                   "</td><td>") + "</td></tr>"
    html += '</tbody></table>'
    response = make_response(render_template('general.html', urlroot=request.url_root,
                                             title="Database statistics", html=html,
                                             navbar=generate_navbar('Stats')))
    return response

# ******************************************************************************
# * Multi-role endpoints (ORCID)                                               *
# ******************************************************************************

@app.route('/groups')
def show_groups():
    '''
    Show group owners from ORCID
    Return records whose ORCIDs have a group
    ---
    tags:
      - ORCID
    responses:
      200:
        description: groups
      500:
        description: MongoDB error
    '''
    result = initialize_result()
    expected = 'html' if 'Accept' in request.headers \
                         and 'html' in request.headers['Accept'] else 'json'
    payload = {"group": {"$exists": True}}
    try:
        rows = DB['dis'].orcid.find(payload, {'_id': 0}).sort("group", 1)
    except Exception as err:
        if expected == 'html':
            return render_template('error.html', urlroot=request.url_root,
                                   title=render_warning("Could not get groups from MongoDB"),
                                   message=error_message(err))
        raise InvalidUsage(str(err), 500) from err
    if expected == 'json':
        result['rest']['source'] = 'mongo'
        result['data'] = []
        for row in rows:
            result['data'].append(row)
        result['rest']['row_count'] = len(result['data'])
        return generate_response(result)
    html = '<table class="standard"><thead><tr><th>Name</th><th>ORCID</th><th>Group</th>' \
           + '<th>Affiliations</th></tr></thead><tbody>'
    count = 0
    for row in rows:
        count += 1
        if 'affiliations' not in row:
            row['affiliations'] = ''
        link = f"<a href='/orcidui/{row['orcid']}'>{row['orcid']}</a>" if 'orcid' in row else ''
        html += f"<tr><td>{row['given'][0]} {row['family'][0]}</td>" \
                + f"<td style='width: 180px'>{link}</td><td>{row['group']}</td>" \
                + f"<td>{', '.join(row['affiliations'])}</td></tr>"
    html += '</tbody></table>'
    return render_template('general.html', urlroot=request.url_root, title=f"Groups ({count:,})",
                           html=html, navbar=generate_navbar('ORCID'))

# *****************************************************************************

if __name__ == '__main__':
    if app.config["RUN_MODE"] == 'dev':
        app.run(debug=app.config["DEBUG"])
    else:
        app.run(debug=app.config["DEBUG"])
