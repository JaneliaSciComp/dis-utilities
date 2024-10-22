''' dis_responder.py
    UI and REST API for Data and Information Services
'''

from datetime import date, datetime, timedelta
from html import escape
import inspect
import json
from json import JSONEncoder
from operator import attrgetter, itemgetter
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

# pylint: disable=broad-exception-caught,broad-exception-raised,too-many-lines

__version__ = "20.5.0"
# Database
DB = {}
# Custom queries
CUSTOM_REGEX = {"publishing_year": {"field": "jrc_publishing_date",
                                    "value": "^!REPLACE!"}
               }

# Navigation
NAV = {"Home": "",
       "DOIs": {"DOIs by insertion date": "dois_insertpicker",
                "DOIs awaiting processing": "dois_pending",
                "DOIs by journal": "dois_journal",
                "DOIs by publisher": "dois_publisher",
                "DOIs by source": "dois_source",
                "DOIs by year": "dois_year",
                "DOIs by month": "dois_month",
                "DOI yearly report": "dois_report"
            },
       "Authorship": {"DOIs by authorship": "dois_author",
                      "DOIs with lab head first/last authors": "doiui_group"},
       "Preprints": {"DOIs by preprint status": "dois_preprint",
                     "DOIs by preprint status by year": "dois_preprint_year"},
       "ORCID": {"Groups": "groups",
                 "Entries": "orcid_entry",
                 "Duplicates": "orcid_duplicates",
                },
       "Tag/affiliation": {"DOIs by tag": "dois_tag",
                           "Top DOI tags by year": "dois_top",
                           "Author affiliations": "orcid_tag",
                          },
       "Stats" : {"Database": "stats_database"
                 },
       "External systems": {"Search People system": "people",
                            "Supervisory Organizations": "orgs",
                           }
      }
# Sources

# Dates
OPSTART = datetime.strptime('2024-05-16','%Y-%m-%d')

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
                if itm == 'divider':
                    nav += "<div class='dropdown-divider'></div>"
                    continue
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
          ptitle: page title
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
        if use_eid:
            payload = {"jrc_author": oid}
        elif both:
            eid = orc['employeeId'] if 'employeeId' in orc else None
            payload = orcid_payload(oid, orc, eid)
        else:
            payload = orcid_payload(oid, orc)
        rows = DB['dis'].dois.find(payload)
    except Exception as err:
        raise CustomException(err, "Could not find in dois collection by name.") from err
    return rows


def generate_works_table(rows, name=None):
    ''' Generate table HTML for a person's works
        Keyword arguments:
          rows: rows from dois collection
          name: search key [optional]
        Returns:
          HTML and a list of DOIs
    '''
    works = []
    dois = []
    authors = {}
    html = ""
    fileoutput = ""
    for row in rows:
        doi = doi_link(row['doi']) if row['doi'] else "&nbsp;"
        if 'title' in row and isinstance(row['title'], str):
            title = row['title']
        else:
            title = DL.get_title(row)
        dois.append(row['doi'])
        payload = {"date":  DL.get_publishing_date(row),
                   "doi": doi,
                   "title": title
                  }
        works.append(payload)
        fileoutput += f"{payload['date']}\t{row['doi']}\t{payload['title']}\n"
        if name:
            alist = DL.get_author_details(row)
            if alist:
                for auth in alist:
                    if "family" in auth and "given" in auth \
                       and auth["family"].lower() == name.lower():
                        authors[f"{auth['given']} {auth['family']}"] = True
            else:
                print(f"Could not get author details for {row['doi']}")
    if not works:
        return html, []
    html += "<table id='pubs' class='tablesorter standard'>" \
            + '<thead><tr><th>Published</th><th>DOI</th><th>Title</th></tr></thead><tbody>'
    for work in sorted(works, key=lambda row: row['date'], reverse=True):
        html += f"<tr><td>{work['date']}</td><td>{work['doi'] if work['doi'] else '&nbsp;'}</td>" \
                + f"<td>{work['title']}</td></tr>"
    if dois:
        html += "</tbody></table>"
    if authors:
        html = f"<br>Authors found: {', '.join(sorted(authors.keys()))}<br>" \
               + f"This may include non-Janelia authors<br>{html}"
    html = create_downloadable('works', ['Published', 'DOI', 'Title'], fileoutput) + html
    html = f"DOIs: {len(works)}<br>" + html
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
            payload = {'userIdO365' if use_eid else 'orcid': oid}
            orc = DB['dis'].orcid.find_one(payload)
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
    if 'userIdO365' in orc:
        link = "<a href='" + f"{app.config['WORKDAY']}{orc['userIdO365']}" \
               + f"' target='_blank'>{orc['userIdO365']}</a>"
        html += f"<tr><td>User ID:</td><td>{link}</td></tr>"
    if 'affiliations' in orc:
        html += f"<tr><td>Affiliations:</td><td>{', '.join(orc['affiliations'])}</td></tr>"
    html += "</table><br>"
    try:
        if use_eid:
            oid = orc['employeeId']
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
        link = ""
        if work['external-ids']['external-id'][0]['external-id-url']:
            if work['external-ids']['external-id'][0]['external-id-url']:
                link = "<a href='" \
                       + work['external-ids']['external-id'][0]['external-id-url']['value'] \
                       + f"' target='_blank'>{doi}</a>"
        else:
            link = doi_link(doi)
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
        elif 'userIdO365' in row:
            link = f"<a href='/userui/{row['userIdO365']}'>No ORCID found</a>"
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

def doi_link(doi):
    ''' Return a link to a DOI or DOIs
        Keyword arguments:
          doi: DOI
        Returns:
          newdoi: HTML link(s) to DOI(s) as a string
    '''
    if not doi:
        return ""
    doilist = [doi] if isinstance(doi, str) else doi
    newdoi = []
    for item in doilist:
        newdoi.append(f"<a href='/doiui/{item}'>{item}</a>")
    if isinstance(doi, str):
        newdoi = newdoi[0]
    else:
        newdoi = ", ".join(newdoi)
    return newdoi


def get_doi(doi):
    ''' Get a single DOI record
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
        if not re.match(prog, key) or key in app.config['DO_NOT_DISPLAY']:
            continue
        if isinstance(val, list) and key not in ('jrc_preprint'):
            try:
                if isinstance(val[0], dict):
                    val = ", ".join(sorted(elem['name'] for elem in val))
                else:
                    val = ", ".join(sorted(val))
            except TypeError:
                val = json.dumps(val)
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
            val = doi_link(val)
        elif 'jrc_tag' in key:
            link = []
            for aff in val.split(", "):
                link.append(f"<a href='/affiliation/{escape(aff)}'>{aff}</a>")
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
            html += f"This DOI {rel.replace('-', ' ')} {doi_link(itm['id'])}<br>"
            used.append(itm['id'])
    return html


def get_migration_data(row):
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
    if 'jrc_tag' in row and row['jrc_tag']:
        if isinstance(row['jrc_tag'][0], dict):
            for atag in row['jrc_tag']:
                tags.append(atag)
        #else:
        #    #TAG Old style - can delete after cutover
        #    for atag in row['jrc_tag']:
        #        code = orgs[atag] if atag in orgs else None
        #        tags.append({"name": atag, "code": code})
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


def compute_preprint_data(rows):
    ''' Create a dictionaries of preprint data
        Keyword arguments:
          rows: preprint types
        Returns:
          data: preprint data dictionary
          preprint: preprint types dictionary
    '''
    data = {'Has preprint relation': 0}
    preprint = {}
    for row in rows:
        if 'type' in row['_id']:
            preprint[row['_id']['type']] = row['count']
            data['Has preprint relation'] += row['count']
        else:
            preprint['DataCite'] = row['count']
            data['Has preprint relation'] += row['count']
    for key in ('journal-article', 'posted-content', 'DataCite'):
        if key not in preprint:
            preprint[key] = 0
    return data, preprint


def counts_by_type(rows):
    ''' Count DOIs by type
        Keyword arguments:
          rows: aggregate rows from dois collection
        Returns:
          Dictionary of type counts
    '''
    typed = {}
    for row in rows:
        typ = row['_id']['type'] if 'type' in row['_id'] else "DataCite"
        sub = row['_id']['subtype'] if 'subtype' in row['_id'] else ""
        if sub == 'preprint':
            typ = 'posted-content'
        if typ not in typed:
            typed[typ] = 0
        typed[typ] += row['count']
    return typed


def get_first_last_authors(year):
    ''' Get first and last author counts
        Keyword arguments:
          year: year to get counts for
        Returns:
          First and last author counts
    '''
    stat = {'first': {}, 'last': {}}
    for which in ("first", "last"):
        payload = [{"$match": {"jrc_publishing_date": {"$regex": "^"+ year},
                               f"jrc_{which}_author": {"$exists": True}}},
                   {"$group": {"_id": {"type": "$type", "subtype": "$subtype"},
                               "count": {"$sum": 1}}}
                  ]
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
            if typ not in stat[which]:
                stat[which][typ] = 0
            stat[which][typ] += row['count']
    return stat['first'], stat['last']


def get_no_relation():
    ''' Get DOIs with no relation
        Keyword arguments:
          None
        Returns:
          Dictionary of types/subtypes with no relation
    '''
    no_relation = {"Crossref": {}, "DataCite": {}}
    payload = {"Crossref_journal": {"type": "journal-article", "subtype": {"$ne": "preprint"},
                                    "jrc_preprint": {"$exists": False}},
               "Crossref_preprint": {"subtype": "preprint", "jrc_preprint": {"$exists": False}},
               "DataCite_journal": {"jrc_obtained_from": "DataCite",
                                    "types.resourceTypeGeneral": {"$ne": "Preprint"},
                                    "jrc_preprint": {"$exists": False}},
               "DataCite_preprint": {"types.resourceTypeGeneral": "Preprint",
                                     "jrc_preprint": {"$exists": False}}
              }
    for key, val in payload.items():
        try:
            cnt = DB['dis'].dois.count_documents(val)
        except Exception as err:
            raise err
        src, typ = key.split('_')
        no_relation[src][typ] = cnt
    return no_relation


def get_preprint_stats(rows):
    ''' Create a dictionary of preprint statistics
        Keyword arguments:
          rows: types/subtypes over years
        Returns:
          Preprint statistics dictionary
    '''
    stat = {}
    for row in rows:
        if 'type' not in row['_id']:
            continue
        if 'sub' in row['_id'] and row['_id']['sub'] == 'preprint':
            if row['_id']['year'] not in stat:
                stat[row['_id']['year']] = {}
            for sub in ('journal', 'preprint'):
                if sub not in stat[row['_id']['year']]:
                    stat[row['_id']['year']][sub] = 0
            stat[row['_id']['year']]['preprint'] += row['count']
        elif row['_id']['type'] == 'journal-article':
            if row['_id']['year'] not in stat:
                stat[row['_id']['year']] = {}
            for sub in ('journal', 'preprint'):
                if sub not in stat[row['_id']['year']]:
                    stat[row['_id']['year']][sub] = 0
            stat[row['_id']['year']]['journal'] += row['count']
    return stat


def get_source_data(year):
    ''' Get DOI data by source and type/subtype or resourceTypeGeneral
        Keyword arguments:
          year: year to get data for
        Returns:
          Data dictionary and html dictionary
    '''
    # Crossref
    if year != 'All':
        match = {"jrc_obtained_from": "Crossref",
                 "jrc_publishing_date": {"$regex": "^"+ year}}
    else:
        match = {"jrc_obtained_from": "Crossref"}
    payload = [{"$match": match},
               {"$group": {"_id": {"source": "$jrc_obtained_from", "type": "$type",
                                   "subtype": "$subtype"},
                           "count": {"$sum": 1}}},
              ]
    try:
        rows = DB['dis'].dois.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get Crossref types from dois"),
                               message=error_message(err))
    data = {"Crossref": 0, "DataCite": 0}
    hdict = {}
    for row in rows:
        for field in ('type', 'subtype'):
            if field not in row['_id']:
                row['_id'][field] = ''
        data['Crossref'] += row['count']
        hdict["_".join([row['_id']['source'], row['_id']['type'],
                        row['_id']['subtype']])] = row['count']
    # DataCite
    match['jrc_obtained_from'] = "DataCite"
    payload = [{"$match": match},
               {"$group": {"_id": "$types.resourceTypeGeneral","count": {"$sum": 1}}}
              ]
    try:
        rows = DB['dis'].dois.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get DataCite types from dois"),
                               message=error_message(err))
    for row in rows:
        data['DataCite'] += row['count']
        hdict["_".join(['DataCite', row['_id'], ""])] = row['count']
    return data, hdict


def s2_citation_count(doi, fmt='plain'):
    ''' Get citation count from Semantic Scholar
        Keyword arguments:
          doi: DOI
          fmt: format (plain or html)
        Returns:
          Citation count
    '''
    url = f"{app.config['S2_GRAPH']}paper/DOI:{doi}?fields=citationCount"
    headers = {'x-api-key': app.config['S2_API_KEY']}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 429:
            raise Exception("Rate limit exceeded")
        if resp.status_code != 200:
            return 0
        data = resp.json()
        if fmt == 'html':
            cnt = f"<a href='{app.config['S2']}{data['paperId']}' target='_blank'>" \
                  + f"{data['citationCount']}</a>"
        else:
            cnt = data['citationCount']
        return cnt
    except Exception:
        return 0


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
        if 'duplicate_name' in auth:
            badges.append(f"{tiny_badge('warning', 'Duplicate name')}")
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
        if (not auth['janelian']) and (not auth['asserted']) and (not auth['alumni']):
            continue
        if auth['janelian'] or auth['asserted']:
            count += 1
        who = f"{auth['given']} {auth['family']}"
        if 'orcid' in auth and auth['orcid']:
            who = f"<a href='/orcidui/{auth['orcid']}'>{who}</a>"
        elif 'userIdO365' in auth and auth['userIdO365']:
            who = f"<a href='/userui/{auth['userIdO365']}'>{who}</a>"
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
    if 'duplicate_name' in orc:
        badges.append(tiny_badge('warning', 'Duplicate name'))
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


def humansize(num, suffix='B', places=2, space='disk'):
    ''' Return a human-readable storage size
        Keyword arguments:
          num: size
          suffix: default suffix
          space: "disk" or "mem"
        Returns:
          string
    '''
    limit = 1024.0 if space == 'disk' else 1000.0
    for unit in ['', 'K', 'M', 'G', 'T']:
        if abs(num) < limit:
            return f"{num:.{places}f}{unit}{suffix}"
        num /= limit
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
        Keyword arguments:
          None
        Returns:
          Date of the most recent Thursday
    '''
    today = date.today()
    offset = (today.weekday() - 3) % 7
    if offset:
        offset = 7
    return today - timedelta(days=offset)


def weeks_ago(weeks):
    ''' Calculate the date of a number of weeks ago
        Keyword arguments:
          weeks: number of weeks
        Returns:
          Date of a number of weeks ago
    '''
    today = date.today()
    return today - timedelta(weeks=weeks)


def year_pulldown(prefix, all_years=True):
    ''' Generate a year pulldown
        Keyword arguments:
          prefic: navigation prefix
        Returns:
          Pulldown HTML
    '''
    years = ['All'] if all_years else []
    for year in range(datetime.now().year, 2005, -1):
        years.append(str(year))
    html = "<div class='btn-group'><button type='button' class='btn btn-info dropdown-toggle' " \
           + "data-toggle='dropdown' aria-haspopup='true' aria-expanded='false'>" \
           + "Select publishing year</button><div class='dropdown-menu'>"
    for year in years:
        html += f"<a class='dropdown-item' href='/{prefix}/{year}'>{year}</a>"
    html += "</div></div>"
    return html


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
        orgs = DL.get_supervisory_orgs(coll=DB['dis'].suporg)
    except Exception as err:
        raise InvalidUsage("Could not get supervisory orgs: " + str(err), 500) from err
    if 'jrc_tag' in row:
        for atag in row['jrc_tag']:
            if atag['name'] not in tagname:
                if atag['name'] in orgs:
                    code = atag['code']
                    tagtype = atag['type']
                else:
                    code = None
                    tagtype = None
                tagname.append(atag['name'])
                tags.append({"name": atag['name'], "code": code, "type": tagtype})
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
            rec = get_migration_data(row)
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
    for row in rows:
        try:
            doi = row['doi']
            rec = get_migration_data(row)
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
    Return citations
    Return a dictionary of citations for a list of given DOIs.
    ---
    tags:
      - DOI
    parameters:
      - in: path
        name: ctype
        schema:
          type: string
        required: false
        description: Citation type (dis, flylight, or full)
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
        journal = DL.get_journal(row)
        result['data'][doi] = f"{authors} {title}."
        if ctype == 'dis':
            result['data'][doi] = f"{result['data'][doi]}. https://doi.org/{doi}."
        else:
            result['data'][doi] = f"{result['data'][doi]}. {journal}."
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


@app.route('/citation/full/<path:doi>')
def show_full_citation(doi):
    '''
    Return a full citation
    Return a full citation (DIS+journal) for a given DOI.
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
    print(ipd['query'])
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
        rows = DB['dis'].dois.find({"jrc_tag.name": ipd['tag']}, {'_id': 0})
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
    url = f"{app.config['ORCID']}{oid}"
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
    return make_response(render_template('home.html', urlroot=request.url_root,
                                         navbar=generate_navbar('Home')))

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
    authors = DL.get_author_list(data, orcid=True, project_map=DB['dis'].project_map)
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
    mlink = f"/doi/migration/{doi}"
    oresp = JRC.call_oa(doi)
    obutton = ""
    if oresp:
        olink = f"{app.config['OA']}{doi}"
        obutton = f" {tiny_badge('primary', 'OA data', olink)}"
    chead = 'Citation'
    if 'type' in data:
        chead += f" for {data['type'].replace('-', ' ')}"
        if 'subtype' in data:
            chead += f" {data['subtype'].replace('-', ' ')}"
    elif 'types' in data and 'resourceTypeGeneral' in data['types']:
        chead += f" for {data['types']['resourceTypeGeneral']}"
    html += f"<h4>{chead}</h4><span class='citation'>{citation} {journal}.</span><br><br>"
    html += f"<span class='paperdata'>DOI: {link} {tiny_badge('primary', 'Raw data', rlink)}" \
            + f" {tiny_badge('primary', 'HQ migration', mlink)} {obutton}</span><br>"
    if row:
        citations = s2_citation_count(doi, fmt='html')
        if citations:
            html += f"<span class='paperdata'>Citations: {citations}</span><br>"
    html += "<br>"
    html += add_relations(data)
    if row:
        try:
            authors = DL.get_author_details(row, DB['dis'].orcid)
        except Exception as err:
            return inspect_error(err, 'Could not get author list details')
        if authors:
            alist, count = show_tagged_authors(authors)
            if alist:
                html += f"<br><h4>Potential Janelia authors ({count})</h4>" \
                        + f"<div class='scroll'>{''.join(alist)}</div>"
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=doi, html=html,
                                         navbar=generate_navbar('DOIs')))


@app.route('/doisui_name/<string:name>')
def show_doi_by_name_ui(name):
    ''' Show DOIs for a family name
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
    html, _ = generate_works_table(rows, name)
    if not html:
        return render_template('warning.html', urlroot=request.url_root,
                               title=render_warning("Could not find DOIs", 'warning'),
                               message=f"Could not find any DOIs with author name matching {name}")
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=f"DOIs for {name}", html=html,
                                         navbar=generate_navbar('DOIs')))


@app.route('/doisui_type/<string:src>/<string:typ>/<string:sub>', defaults={'year': 'All'})
@app.route('/doisui_type/<string:src>/<string:typ>/<string:sub>/<string:year>')
def show_doi_by_type_ui(src, typ, sub, year):
    ''' Show DOIs for a given type/subtype
    '''
    payload = {"jrc_obtained_from": src,
               ("type" if src == 'Crossref' else 'types.resourceTypeGeneral'): typ}
    if sub != 'None':
        payload["subtype"] = sub
    if year != 'All':
        payload['jrc_publishing_date'] = {"$regex": "^" + year}
    try:
        rows = DB['dis'].dois.find(payload).collation({"locale": "en"}).sort("doi", 1)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get DOIs from dois collection"),
                               message=error_message(err))
    html, _ = generate_works_table(rows)
    desc = f"{src} {typ}"
    if sub != 'None':
        desc += f"/{sub}"
    if year != 'All':
        desc += f" ({year})"
    if not html:
        return render_template('warning.html', urlroot=request.url_root,
                               title=render_warning("Could not find DOIs", 'warning'),
                               message="Could not find any DOIs with type/subtype matching " \
                                       + desc)
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=f"DOIs for {desc}", html=html,
                                         navbar=generate_navbar('DOIs')))


@app.route('/titlesui/<string:title>')
def show_doi_by_title_ui(title):
    ''' Show DOIs for a given title
    '''
    payload = ([{"$unwind" : "$title"},
                {"$match": {"title": {"$regex": title, "$options" : "i"},
                        }}
               ])
    try:
        rows = DB['dis'].dois.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get DOIs from dois collection"),
                               message=error_message(err))
    union = []
    for row in rows:
        union.append(row)
    payload = {"titles.title": {"$regex": title, "$options" : "i"}}
    try:
        rows = DB['dis'].dois.find(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get DOIs from dois collection"),
                               message=error_message(err))
    for row in rows:
        union.append(row)
    html, _ = generate_works_table(union, title)
    if not html:
        return render_template('warning.html', urlroot=request.url_root,
                               title=render_warning("Could not find DOIs", 'warning'),
                               message=f"Could not find any DOIs with title matching {title}")
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=f"DOIs for {title}", html=html,
                                         navbar=generate_navbar('DOIs')))


@app.route('/dois_author/<string:year>')
@app.route('/dois_author')
def dois_author(year='All'):
    ''' Show first/last authors
    '''
    source = {}
    for src in ('Crossref', 'DataCite', 'Crossref-all', 'DataCite-all', 'Crossref-jrc',
                'DataCite-jrc'):
        payload = {"jrc_obtained_from": src,
                   "$or": [{"jrc_first_author": {"$exists": True}},
                           {"jrc_last_author": {"$exists": True}}]}
        if '-all' in src:
            payload = {"jrc_obtained_from": src.replace('-all', '')}
        elif '-jrc' in src:
            payload = {"jrc_obtained_from": src.replace('-jrc', ''),
                       "$or": [{"jrc_first_author": {"$exists": True}},
                               {"jrc_last_author": {"$exists": True}},
                               {"jrc_author": {"$exists": True}}]}
        if year != 'All':
            payload['jrc_publishing_date'] = {"$regex": "^"+ year}
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
    for src in app.config['SOURCES']:
        data[src] = source[src]
    html += f"<tr><td>All authors</td><td>{source['Crossref-all']:,}</td>" \
            + f"<td>{source['DataCite-all']:,}</td></tr>"
    html += f"<tr><td>Any Janelia author</td><td>{source['Crossref-jrc']:,}</td>" \
            + f"<td>{source['DataCite-jrc']:,}</td></tr>"
    html += f"<tr><td>First and/or last</td><td>{source['Crossref']:,}</td>" \
            + f"<td>{source['DataCite']:,}</td></tr>"
    html += f"<tr><td>Additional only</td><td>{source['Crossref-jrc']-source['Crossref']:,}</td>" \
            + f"<td>{source['DataCite-jrc']-source['DataCite']:,}</td></tr>"
    html += '</tbody></table><br>' + year_pulldown('dois_author')
    data = {"Crossref": source['Crossref-jrc'],
            "DataCite": source['DataCite-jrc']}
    title = "DOIs by authorship, any Janelia author"
    if year != 'All':
        title += f" ({year})"
    chartscript, chartdiv = DP.pie_chart(data, title, "source",
                                         colors=DP.SOURCE_PALETTE)
    data = {"First and/or last": source['Crossref'],
            "Additional": source['Crossref-jrc']-source['Crossref']}
    title = "Crossref DOIs by authorship"
    if year != 'All':
        title += f" ({year})"
    script2, div2 = DP.pie_chart(data, title, "source",
                                 colors=DP.SOURCE_PALETTE)
    chartscript += script2
    chartdiv += div2
    if source['DataCite'] or source['DataCite-jrc']:
        data = {"First and/or last": source['DataCite'],
                "Additional": source['DataCite-jrc']-source['DataCite']}
        title = "DataCite DOIs by authorship"
        if year != 'All':
            title += f" ({year})"
        script2, div2 = DP.pie_chart(data, title, "source",
                                     colors=DP.SOURCE_PALETTE)
        chartscript += script2
        chartdiv += div2
    title = "DOI authorship"
    if year != 'All':
        title += f" ({year})"
    return make_response(render_template('bokeh.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         chartscript=chartscript, chartdiv=chartdiv,
                                         navbar=generate_navbar('Authorship')))


@app.route('/doiui_group/<string:year>')
@app.route('/doiui_group')
def doiui_group(year='All'):
    ''' Show group leader first/last authorship
    '''
    payload = {"group_code": {"$exists": True}}
    try:
        rows = DB['dis'].orcid.find(payload, {"employeeId": 1})
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get group leads " \
                                                    + "from dois collection"),
                               message=error_message(err))
    leads = []
    for row in rows:
        leads.append(row['employeeId'])
    payload = {"jrc_first_id": {"$in": leads}}
    if year != 'All':
        payload['jrc_publishing_date'] = {"$regex": "^"+ year}
    cnt = {}
    try:
        cnt['first'] = DB['dis'].dois.count_documents(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get first authors " \
                                                    + "from dois collection"),
                               message=error_message(err))
    payload = {"jrc_last_id": {"$in": leads}}
    if year != 'All':
        payload['jrc_publishing_date'] = {"$regex": "^"+ year}
    try:
        cnt['last'] = DB['dis'].dois.count_documents(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get last authors " \
                                                    + "from dois collection"),
                               message=error_message(err))
    payload = {"jrc_author": {"$exists": True}}
    if year != 'All':
        payload['jrc_publishing_date'] = {"$regex": "^"+ year}
    try:
        cnt['total'] = DB['dis'].dois.count_documents(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get last authors " \
                                                    + "from dois collection"),
                               message=error_message(err))
    html = "<table id='group' class='tablesorter numbers'><thead></thead><tbody>"
    html += f"<tr><td>Lab head first author</td><td>{cnt['first']:,}</td></tr>"
    html += f"<tr><td>Lab head last author</td><td>{cnt['last']:,}</td></tr>"
    html += "</tbody></table><br>" + year_pulldown('doiui_group')
    data = {'Lab head first author': cnt['first'],
            'Non-lab head first author': cnt['total'] - cnt['first']}
    title = "DOIs with lab head first author"
    if year != 'All':
        title += f" ({year})"
    chartscript, chartdiv = DP.pie_chart(data, title, "source",
                                         width=520, height=350,
                                         colors=DP.SOURCE_PALETTE)
    data = {'Lab head last author': cnt['last'],
            'Non-lab head last author': cnt['total'] - cnt['last']}
    title = "DOIs with lab head last author"
    if year != 'All':
        title += f" ({year})"
    script2, div2 = DP.pie_chart(data, title, "source",
                                         width=520, height=350,
                                         colors=DP.SOURCE_PALETTE)
    chartscript += script2
    chartdiv += div2
    title = "DOIs with lab head first/last authors"
    if year != 'All':
        title += f" ({year})"
    return make_response(render_template('bokeh.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         chartscript=chartscript, chartdiv=chartdiv,
                                         navbar=generate_navbar('Authorship')))


@app.route('/dois_journal/<string:year>/<int:top>')
@app.route('/dois_journal/<string:year>')
@app.route('/dois_journal')
def dois_journal(year='All', top=10):
    ''' Show journals
    '''
    top = min(top, 20)
    match = {"container-title": {"$exists": True, "$ne" : ""}}
    if year != 'All':
        match["jrc_publishing_date"] = {"$regex": "^"+ year}
    payload = [{"$unwind" : "$container-title"},
               {"$match": match},
               {"$group": {"_id": "$container-title", "count":{"$sum": 1}}},
              ]
    try:
        rows = DB['dis'].dois.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get Crossref journals"),
                               message=error_message(err))
    journal = {}
    for row in rows:
        journal[row['_id']] = row['count']
    payload = [{"$unwind" : "$institution"},
               {"$match": match},
               {"$group": {"_id": "$institution.name", "count":{"$sum": 1}}},
              ]
    try:
        rows = DB['dis'].dois.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get Crossref journals"),
                               message=error_message(err))
    html = '<table id="journals" class="tablesorter numberlast"><thead><tr>' \
           + '<th>Journal</th><th>Count</th></tr></thead><tbody>'
    for row in rows:
        journal[row['_id']] = row['count']
    data = {}
    for key in sorted(journal, key=journal.get, reverse=True):
        val = journal[key]
        if len(data) >= top:
            continue
        data[key] = val
        html += f"<tr><td>{key}</td><td>{val:,}</td></tr>"
    html += '</tbody></table><br>' + year_pulldown('dois_journal')
    title = "DOIs by journal"
    if year != 'All':
        title += f" ({year})"
    chartscript, chartdiv = DP.pie_chart(data, title, "source", width=875, height=550,
                                         colors='Category20')
    title = f"Top {top} DOI journals"
    if year != 'All':
        title += f" ({year})"
    return make_response(render_template('bokeh.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         chartscript=chartscript, chartdiv=chartdiv,
                                         navbar=generate_navbar('DOIs')))


@app.route('/dois_source/<string:year>')
@app.route('/dois_source')
def dois_source(year='All'):
    ''' Show data sources
    '''
    try:
        data, hdict = get_source_data(year)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get source data from dois"),
                               message=error_message(err))
    # HTML and charts
    html = '<table id="types" class="tablesorter numberlast"><thead><tr>' \
           + '<th>Source</th><th>Type</th><th>Subtype</th><th>Count</th>' \
           + '</tr></thead><tbody>'
    for key, val in sorted(hdict.items(), key=itemgetter(1), reverse=True):
        src, typ, sub = key.split('_')
        if not sub:
            sub = 'None'
        if year == 'All':
            val = f"<a href='/doisui_type/{src}/{typ}/{sub}'>{val}</a>"
        else:
            val = f"<a href='/doisui_type/{src}/{typ}/{sub}/{year}'>{val}</a>"
        html += f"<tr><td>{src}</td><td>{typ}</td><td>{sub if sub != 'None' else ''}</td>" \
                + f"<td>{val}</td></tr>"
    html += '</tbody></table><br>' + year_pulldown('dois_source')
    title = "DOIs by source"
    if year != 'All':
        title += f" ({year})"
    chartscript, chartdiv = DP.pie_chart(data, title, "source", width=500,
                                         colors=DP.SOURCE_PALETTE)
    if year == 'All' or year >= '2024':
        payload = [{"$match": {"jrc_inserted" : { "$gte" : OPSTART}}},
                   {"$group": {"_id": "$jrc_load_source", "count": {"$sum": 1}}},
                   {"$sort" : {"count": -1}}
                  ]
        try:
            rows = DB['dis'].dois.aggregate(payload)
        except Exception as err:
            return render_template('error.html', urlroot=request.url_root,
                                   title=render_warning("Could not get load methods " \
                                                        + "from dois collection"),
                                   message=error_message(err))
        data = {}
        for row in rows:
            data[row['_id']] = row['count']
        title = "DOIs by load method"
        if year != 'All':
            title += f" ({year})"
        script2, div2 = DP.pie_chart(data, title, "source", width=500,
                                     colors=DP.SOURCE_PALETTE)
        chartscript += script2
        chartdiv += div2
    title = "DOI sources"
    if year != 'All':
        title += f" ({year})"
    return make_response(render_template('bokeh.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         chartscript=chartscript, chartdiv=chartdiv,
                                         navbar=generate_navbar('DOIs')))


@app.route('/dois_preprint/<string:year>')
@app.route('/dois_preprint')
def dois_preprint(year='All'):
    ''' Show preprints
    '''
    source = {}
    for src in app.config['SOURCES']:
        payload = {"jrc_obtained_from": src, "jrc_preprint": {"$exists": False}}
        if year != 'All':
            payload['jrc_publishing_date'] = {"$regex": "^"+ year}
        if src == 'Crossref':
            payload['type'] = {"$in": ["journal-article", "posted-content"]}
        else:
            payload['type'] = {"types.resourceTypeGeneral": "Preprint"}
        try:
            cnt = DB['dis'].dois.count_documents(payload)
            source[src] = cnt
        except Exception as err:
            return render_template('error.html', urlroot=request.url_root,
                                   title=render_warning("Could not get source counts " \
                                                        + "from dois collection"),
                                   message=error_message(err))
    match = {"jrc_preprint": {"$exists": True}}
    if year != 'All':
        match['jrc_publishing_date'] = {"$regex": "^"+ year}
    payload = [{"$match": match},
               {"$group": {"_id": {"type": "$type", "preprint": "$preprint"},"count": {"$sum": 1}}}]
    try:
        rows = DB['dis'].dois.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get preprint counts " \
                                                    + "from dois collection"),
                               message=error_message(err))
    data, preprint = compute_preprint_data(rows)
    no_relation = get_no_relation()
    html = '<table id="preprints" class="tablesorter numbers"><thead><tr>' \
           + '<th>Status</th><th>Crossref</th><th>DataCite</th>' \
           + '</tr></thead><tbody>'
    html += "<tr><td>Preprints with journal articles</td>" \
            + f"<td>{preprint['journal-article']:,}</td><td>{preprint['DataCite']}</td></tr>"
    html += f"<tr><td>Journal articles with preprints</td><td>{preprint['posted-content']:,}</td>" \
            + "<td>0</td></tr>"
    html += "<tr><td>Journals without preprints</td>" \
            f"<td>{no_relation['Crossref']['journal']:,}</td>" \
            + f"<td>{no_relation['DataCite']['journal']:,}</td></tr>"
    html += "<tr><td>Preprints without journals</td>" \
            f"<td>{no_relation['Crossref']['preprint']:,}</td>" \
            + f"<td>{no_relation['DataCite']['preprint']:,}</td></tr>"
    html += '</tbody></table><br>' + year_pulldown('dois_preprint')
    data['No preprint relation'] = source['Crossref'] + source['DataCite']
    try:
        chartscript, chartdiv = DP.preprint_pie_charts(data, year, DB['dis'].dois)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not generate preprint pie charts"),
                               message=error_message(err))
    title = "DOI preprint status"
    if year != 'All':
        title += f" ({year})"
    return make_response(render_template('bokeh.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         chartscript=chartscript, chartdiv=chartdiv,
                                         navbar=generate_navbar('Preprints')))


@app.route('/dois_preprint_year')
def dois_preprint_year():
    ''' Show preprints by year
    '''
    payload = [{"$group": {"_id": {"year": {"$substrBytes": ["$jrc_publishing_date", 0, 4]},
                                   "type": "$type", "sub": "$subtype",
                                  },
                           "count": {"$sum": 1}}},
               {"$sort": {"_id.year": 1}}
              ]
    try:
        rows = DB['dis'].dois.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get preprint year counts " \
                                                    + "from dois collection"),
                               message=error_message(err))
    stat = get_preprint_stats(rows)
    data = {'years': [], 'Journal article': [], 'Preprint': []}
    for key, val in stat.items():
        if key < '2006':
            continue
        data['years'].append(key)
        data['Journal article'].append(val['journal'])
        data['Preprint'].append(val['preprint'])
    payload = {"doi": {"$regex": "arxiv", "$options": "i"}}
    try:
        rows = DB['dis'].dois.find(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get arXiv DOIs"),
                               message=error_message(err))
    for row in rows:
        year = row['jrc_publishing_date'][:4]
        data['Preprint'][data['years'].index(year)] += 1
    html = '<table id="years" class="tablesorter numbers"><thead><tr>' \
           + '<th>Year</th><th>Journal articles</th><th>Preprints</th></thead><tbody>'
    for idx in range(len(data['years'])):
        html += f"<tr><td>{data['years'][idx]}</td><td>{data['Journal article'][idx]:,}</td>" \
                + f"<td>{data['Preprint'][idx]:,}</td></tr>"
    html += '</tbody></table>'
    chartscript, chartdiv = DP.stacked_bar_chart(data, "DOIs published by year/preprint status",
                                                 xaxis="years",
                                                 yaxis=('Journal article', 'Preprint'),
                                                 colors=DP.SOURCE_PALETTE)
    return make_response(render_template('bokeh.html', urlroot=request.url_root,
                                         title="DOIs preprint status by year", html=html,
                                         chartscript=chartscript, chartdiv=chartdiv,
                                         navbar=generate_navbar('Preprints')))


@app.route('/dois_month/<string:year>')
@app.route('/dois_month')
def dois_month(year=str(datetime.now().year)):
    ''' Show DOIs by month
    '''
    payload = [{"$match": {"jrc_publishing_date": {"$regex": "^"+ year}}},
               {"$group": {"_id": {"month": {"$substrBytes": ["$jrc_publishing_date", 0, 7]},
                                   "obtained": "$jrc_obtained_from"
                                  },
                           "count": {"$sum": 1}}},
               {"$sort": {"_id.month": 1}}
              ]
    try:
        rows = DB['dis'].dois.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get month counts " \
                                                    + "from dois collection"),
                               message=error_message(err))
    data = {'months': [f"{mon:02}" for mon in range(1, 13)], 'Crossref': [0] * 12,
            'DataCite': [0] * 12}
    for row in rows:
        data[row['_id']['obtained']][int(row['_id']['month'][-2:])-1] = row['count']
    title = f"DOIs published by month for {year}"
    html = '<table id="years" class="tablesorter numbers"><thead><tr>' \
           + '<th>Month</th><th>Crossref</th><th>DataCite</th>' \
           + '</tr></thead><tbody>'
    for mon in data['months']:
        mname = date(1900, int(mon), 1).strftime('%B')
        html += f"<tr><td>{mname}</td>"
        for source in app.config['SOURCES']:
            if data[source][int(mon)-1]:
                onclick = "onclick='nav_post(\"publishing_year\",\"" \
                          + f"{year}-{mon}" + "\",\"" + source + "\")'"
                link = f"<a href='#' {onclick}>{data[source][int(mon)-1]:,}</a>"
                html += f"<td>{link}</td>"
            else:
                html += "<td></td>"
        html += "</tr>"
    html += '</tbody></table><br>' + year_pulldown('dois_month', all_years=False)
    chartscript, chartdiv = DP.stacked_bar_chart(data, title,
                                                 xaxis="months",
                                                 yaxis=('Crossref', 'DataCite'),
                                                 colors=DP.SOURCE_PALETTE)
    return make_response(render_template('bokeh.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         chartscript=chartscript, chartdiv=chartdiv,
                                         navbar=generate_navbar('DOIs')))


@app.route('/dois_pending')
def dois_pending():
    ''' Show DOIs awaiting processing
    '''
    try:
        cnt = DB['dis'].dois_to_process.count_documents({})
        rows = DB['dis'].dois_to_process.find({})
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get DOIs " \
                                                    + "from dois_to_process collection"),
                               message=error_message(err))
    html = '<table id="types" class="tablesorter numbers"><thead><tr>' \
           + '<th>DOI</th><th>Inserted</th><th>Time waiting</th>' \
           + '</tr></thead><tbody>'
    if not cnt:
        return render_template('warning.html', urlroot=request.url_root,
                               title=render_warning("No DOIs found", 'warning'),
                               message="No DOIs are awaiting processing")
    for row in rows:
        elapsed = datetime. now() - row['inserted']
        if elapsed.days:
            etime = f"{elapsed.days} day{'s' if elapsed.days > 1 else ''}, " \
                    + f"{elapsed.seconds // 3600:02}:{elapsed.seconds // 60 % 60:02}:" \
                    + f"{elapsed.seconds % 60:02}"
        else:
            etime = f"{elapsed.seconds // 3600:02}:{elapsed.seconds // 60 % 60:02}:" \
                    + f"{elapsed.seconds % 60:02}"
        html += f"<tr><td>{doi_link(row['doi'])}</td><td>{row['inserted']}</td><td>{etime}</td>"
    html += '</tbody></table>'
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title="DOIs awaiting processing", html=html,
                                         navbar=generate_navbar('DOIs')))


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
        for source in app.config['SOURCES']:
            if source in val:
                onclick = "onclick='nav_post(\"publisher\",\"" + pub \
                          + "\",\"" + source + "\")'"
                link = f"<a href='#' {onclick}>{val[source]:,}</a>"
            else:
                link = ""
            html += f"<td>{link}</td>"
        html += "</tr>"
    html += '</tbody></table>'
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=f"DOI publishers ({len(pubs):,})", html=html,
                                         navbar=generate_navbar('DOIs')))


@app.route('/dois_tag')
def dois_tag():
    ''' Show tags with counts
    '''
    payload = [{"$unwind" : "$jrc_tag"},
               {"$project": {"_id": 0, "jrc_tag.name": 1, "jrc_obtained_from": 1}},
               {"$group": {"_id": {"tag": "$jrc_tag.name", "source": "$jrc_obtained_from"},
                           "count":{"$sum": 1}}},
               {"$sort": {"_id.tag": 1}}
              ]
    try:
        orgs = DL.get_supervisory_orgs(coll=DB['dis'].suporg)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get supervisory orgs"),
                               message=error_message(err))
    try:
        rows = DB['dis'].dois.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get tags from dois collection"),
                               message=error_message(err))
    html = '<table id="types" class="tablesorter numbers"><thead><tr>' \
           + '<th>Tag</th><th>SupOrg</th><th>Crossref</th><th>DataCite</th>' \
           + '</tr></thead><tbody>'
    tags = {}
    for row in rows:
        if row['_id']['tag'] not in tags:
            tags[row['_id']['tag']] = {}
        if row['_id']['source'] not in tags[row['_id']['tag']]:
            tags[row['_id']['tag']][row['_id']['source']] = row['count']
    for tag, val in tags.items():
        link = f"<a href='tag/{tag}'>{tag}</a>"
        rclass = 'other'
        if tag in orgs:
            if 'active' in orgs[tag]:
                org = "<span style='color: lime;'>Yes</span>"
                rclass = 'active'
            else:
                org = "<span style='color: yellow;'>Inactive</span>"
        else:
            org = "<span style='color: red;'>No</span>"
        html += f"<tr class={rclass}><td>{link}</td><td>{org}</td>"
        for source in app.config['SOURCES']:
            if source in val:
                onclick = "onclick='nav_post(\"jrc_tag.name\",\"" + tag \
                          + "\",\"" + source + "\")'"
                link = f"<a href='#' {onclick}>{val[source]:,}</a>"
            else:
                link = ""
            html += f"<td>{link}</td>"
        html += "</tr>"
    html += '</tbody></table>'
    cbutton = "<button class=\"btn btn-outline-warning\" " \
              + "onclick=\"$('.other').toggle();\">Filter for active SupOrgs</button>"
    html = cbutton + html
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=f"DOI tags ({len(tags):,})", html=html,
                                         navbar=generate_navbar('Tag/affiliation')))


@app.route('/dois_top', defaults={'num': 10})
@app.route('/dois_top/<int:num>')
def dois_top(num):
    ''' Show a chart of DOIs by top tags
    '''
    payload = [{"$unwind" : "$jrc_tag"},
               {"$project": {"_id": 0, "jrc_tag.name": 1, "jrc_publishing_date": 1}},
               {"$group": {"_id": {"tag": "$jrc_tag.name",
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
    height = 600
    if num > 23:
        height += 22 * (num - 23)
    from bokeh.palettes import all_palettes, plasma
    colors = plasma(len(top))
    print(len(top))
    if len(top) <= 10:
        colors = all_palettes['Category10'][len(top)]
    elif len(top) <= 20:
        colors = all_palettes['Category20'][len(top)]
    chartscript, chartdiv = DP.stacked_bar_chart(data, f"DOIs published by year for top {num} tags",
                                                 xaxis="years", yaxis=top, width=900, height=height,
                                                 colors=colors)
    return make_response(render_template('bokeh.html', urlroot=request.url_root,
                                         title="DOI tags by year/tag", html=html,
                                         chartscript=chartscript, chartdiv=chartdiv,
                                         navbar=generate_navbar('Tag/affiliation')))


@app.route('/dois_report/<string:year>')
@app.route('/dois_report')
def dois_report(year=str(datetime.now().year)):
    ''' Show year in review
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
    typed = counts_by_type(rows)
    first, last = get_first_last_authors(year)
    # arXiv count
    try:
        cnt = DB['dis'].dois.count_documents({"doi": {"$regex": "arxiv", "$options": "i"},
                                              "jrc_publishing_date": {"$regex": "^"+ year}})
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get arXiv metrics " \
                                                    + "from dois collection"),
                               message=error_message(err))
    if cnt:
        typed['posted-content'] += cnt
    stat = {}
    # Journal count
    payload = [{"$unwind" : "$container-title"},
               {"$match": {"container-title": {"$exists": True}, "type": "journal-article",
                           "jrc_publishing_date": {"$regex": "^"+ year}}},
               {"$group": {"_id": "$container-title", "count":{"$sum": 1}}}
              ]
    try:
        rows = DB['dis'].dois.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get journal metrics " \
                                                    + "from dois collection"),
                               message=error_message(err))
    cnt = 0
    for row in rows:
        if row['_id']:
            cnt += 1
    for key, val in pmap.items():
        if key in typed:
            additional = []
            if key in first:
                additional.append(f"{first[key]:,} with Janelian first author")
            if key in last:
                additional.append(f"{last[key]:,} with Janelian last author")
            additional = f" ({', '.join(additional)})" if additional else ""
            stat[val] = f"<span style='font-weight: bold'>{typed[key]:,}</span> {val.lower()}"
            if val == 'Journal articles':
                stat[val] += f" in {cnt:,} journals"
            stat[val] += additional
    if 'DataCite' in typed:
        stat['DataCite'] = f"<span style='font-weight: bold'>{typed['DataCite']:,}" \
                           + "</span> DataCite entries"
    if 'Journal articles' not in stat:
        stat['Journal articles'] = "<span style='font-weight: bold'>0</span> journal articles"
    if 'Preprints' not in stat:
        stat['Preprints'] = "<span style='font-weight: bold'>0</span> preprints"
    if 'DataCite' not in stat:
        stat['DataCite'] = "<span style='font-weight: bold'>0</span> DataCite entries"
    # Citations
    try:
        rows = DB['dis'].dois.find({"jrc_publishing_date": {"$regex": "^"+ year}})
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get journal metrics " \
                                                    + "from dois collection"),
                               message=error_message(err))
    html = f"{stat['Journal articles']}<br>{stat['Preprints']}<br>{stat['DataCite']}<br>"
    #cnt = 0
    #for row in rows:
    #    if row['jrc_obtained_from'] != 'Crossref':
    #        continue
    #    try:
    #        cnt += s2_citation_count(row['doi'])
    #    except Exception as err:
    #        return render_template('error.html', urlroot=request.url_root,
    #                               title=render_warning("Could not get citation count"),
    #                               message=error_message(err))
    #    sleep(.1)
    # html += f"Citations: {cnt:,}"
    html = f"<div class='titlestat'>{year} YEAR IN REVIEW</div><div class='yearstat'>{html}</div>"
    html += '<br>' + year_pulldown('dois_report', all_years=False)
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=f"{year}", html=html,
                                         navbar=generate_navbar('DOIs')))


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
        if year < '2006':
            continue
        data['years'].insert(0, str(year))
        onclick = "onclick='nav_post(\"publishing_year\",\"" + year + "\")'"
        link = f"<a href='#' {onclick}>{year}</a>"
        html += f"<tr><td>{link}</td>"
        for source in app.config['SOURCES']:
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
                                                 xaxis="years", yaxis=app.config['SOURCES'],
                                                 colors=DP.SOURCE_PALETTE)
    return make_response(render_template('bokeh.html', urlroot=request.url_root,
                                         title="DOIs published by year", html=html,
                                         chartscript=chartscript, chartdiv=chartdiv,
                                         navbar=generate_navbar('DOIs')))


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
                                   {'_id': 0}).sort([("jrc_obtained_from", 1), ("jrc_inserted", 1)])
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
    limit = weeks_ago(2)
    for row in rows:
        source = row['jrc_load_source'] if row['jrc_load_source'] else ""
        typ = ""
        if 'type' in row:
            typ = row['type']
            if 'subtype' in row:
                typ += f" {row['subtype']}"
        elif 'types' in row and 'resourceTypeGeneral' in row['types']:
            typ = row['types']['resourceTypeGeneral']
        version = []
        if 'relation' in row and 'is-version-of' in row['relation']:
            for ver in row['relation']['is-version-of']:
                if ver['id-type'] == 'doi':
                    version.append(ver['id'])
        version = doi_link(version) if version else ""
        news = row['jrc_newsletter'] if 'jrc_newsletter' in row else ""
        if (not news) and (row['jrc_obtained_from'] == 'Crossref') and \
           (row['jrc_publishing_date'] >= str(limit)):
            rclass = 'candidate'
        else:
            rclass = 'other'
        html += f"<tr class='{rclass}'><td>" \
                + "</td><td>".join([doi_link(row['doi']), row['jrc_obtained_from'], typ,
                                    row['jrc_publishing_date'], source,
                                    str(row['jrc_inserted']), version,
                                    news]) + "</td></tr>"
        frow = "\t".join([row['doi'], row['jrc_obtained_from'], typ, row['jrc_publishing_date'],
                          source, str(row['jrc_inserted']), version, news])
        fileoutput += f"{frow}\n"
    html += '</tbody></table>'
    cbutton = "<button class=\"btn btn-outline-warning\" " \
              + "onclick=\"$('.other').toggle();\">Filter for candidate DOIs</button>"
    html = create_downloadable("jrc_inserted", None, fileoutput) + f" &nbsp;{cbutton}{html}"
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
    if request.form:
        for row in ('field', 'value'):
            if row not in ipd or not ipd[row]:
                return render_template('error.html', urlroot=request.url_root,
                                       title=render_warning(f"Missing {row}"),
                                       message=f"You must specify a {row}")
        display_value = ipd['value']
        payload, ptitle = get_custom_payload(ipd, display_value)
    else:
        payload = ipd['query']
        ipd['field'] = "_".join(list(payload.keys()))
        ptitle = ''
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
        works.append({"published": published, "link": doi_link(row['doi']), "title": title,
                      "doi": row['doi']})
    fileoutput = ""
    for row in sorted(works, key=lambda row: row['published'], reverse=True):
        html += "<tr><td>" + dloop(row, ['published', 'link', 'title'], "</td><td>") + "</td></tr>"
        row['title'] = row['title'].replace("\n", " ")
        fileoutput += dloop(row, ['published', 'doi', 'title']) + "\n"
    html += '</tbody></table>'
    html = create_downloadable(ipd['field'], header, fileoutput) + html
    html = f"DOIs: {len(works)}<br>" + html
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=ptitle, html=html,
                                         navbar=generate_navbar('DOIs')))


# ******************************************************************************
# * UI endpoints (ORCID)                                                       *
# ******************************************************************************
@app.route('/orcidui/<string:oid>')
def show_oid_ui(oid):
    ''' Show ORCID user
    '''
    try:
        resp = requests.get(f"{app.config['ORCID']}{oid}",
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
        orciddata, dois = get_orcid_from_db(oid, use_eid=bool('userIdO365' in oid), both=True)
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
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=f"<a href='https://orcid.org/{oid}' " \
                                               + f"target='_blank'>{oid}</a>", html=html,
                                         navbar=generate_navbar('ORCID')))


@app.route('/userui/<string:eid>')
def show_user_ui(eid):
    ''' Show user record by employeeId
    '''
    try:
        orciddata, _ = get_orcid_from_db(eid, use_eid=True)
    except CustomException as err:
        return render_template('error.html', urlroot=request.url_root,
                                title=render_warning(f"Could not find user ID {eid}",
                                                     'warning'),
                                message=error_message(err))
    if not orciddata:
        return render_template('warning.html', urlroot=request.url_root,
                               title=render_warning(f"Could not find user ID {eid}", 'warning'),
                               message="Could not find any information for this employee ID")
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=f"User ID {eid}", html=orciddata,
                                         navbar=generate_navbar('ORCID')))


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
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title="User has no ORCID or employee ID",
                                         html=orciddata, navbar=generate_navbar('ORCID')))


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
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=f"Users: {count:,}", html=html,
                                         navbar=generate_navbar('ORCID')))


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
        orgs = DL.get_supervisory_orgs(coll=DB['dis'].suporg)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get supervisory orgs"),
                               message=error_message(err))
    try:
        rows = DB['dis'].orcid.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get affiliations " \
                                                    + "from orcid collection"),
                               message=error_message(err))
    html = '<table id="types" class="tablesorter numbers"><thead><tr>' \
           + '<th>Affiliation</th><th>SupOrg</th><th>Authors</th>' \
           + '</tr></thead><tbody>'
    count = 0
    for row in rows:
        count += 1
        link = f"<a href='tag/{escape(row['_id'])}'>{row['_id']}</a>"
        link2 = f"<a href='/affiliation/{escape(row['_id'])}'>{row['count']:,}</a>"
        if row['_id'] in orgs:
            if orgs[row['_id']]:
                if 'active' in orgs[row['_id']]:
                    org = "<span style='color: lime;'>Yes</span>"
                else:
                    org = "<span style='color: yellow;'>Inactive</span>"
            else:
                org = "<span style='color: yellow;'>No code</span>"
        else:
            org = "<span style='color: red;'>No</span>"
        html += f"<tr><td>{link}</td><td>{org}</td><td>{link2}</td></tr>"
    html += '</tbody></table>'
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=f"Author affiliations ({count:,})", html=html,
                                         navbar=generate_navbar('Tag/affiliation')))


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
    return make_response(render_template('bokeh.html', urlroot=request.url_root,
                                         title="ORCID entries", html=html,
                                         chartscript=chartscript, chartdiv=chartdiv,
                                         navbar=generate_navbar('ORCID')))


@app.route('/affiliation/<string:aff>')
def orcid_affiliation(aff):
    ''' Show ORCID tags (affiliations) with counts
    '''
    payload = {"jrc_tag.name": aff}
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
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=f"{aff} affiliation ({count:,})",
                                         html=html + additional,
                                         navbar=generate_navbar('ORCID')))


@app.route('/orcid_duplicates')
def orcid_duplicates():
    ''' Show ORCID duplicate records
    '''
    html = ""
    for check in ("employeeId", "orcid"):
        payload = [{"$sortByCount": f"${check}"},
                   {"$match": {"_id": {"$ne": None}, "count": {"$gt": 1}}}
                  ]
        try:
            rowsobj = DB['dis'].orcid.aggregate(payload)
        except Exception as err:
            return render_template('error.html', urlroot=request.url_root,
                                   title=render_warning(f"Could not get duplicate {check}s " \
                                                        + "from orcid collection"),
                                   message=error_message(err))
        rows = []
        for row in rowsobj:
            rows.append(row)
        if rows:
            if check == 'employeeId':
                html += f"{check}<table id='duplicates' class='tablesorter standard'><thead><tr>" \
                        + "<th>Name</th><th>ORCIDs</th></tr></thead><tbody>"
            else:
                html += f"{check}<table id='duplicates' class='tablesorter standard'><thead><tr>" \
                        + "<th>Name</th><th>User IDs</th></tr></thead><tbody>"
            for row in rows:
                try:
                    recs = DB['dis'].orcid.find({"employeeId": row['_id']})
                except Exception as err:
                    return render_template('error.html', urlroot=request.url_root,
                                           title=render_warning("Could not get ORCID data for " \
                                                                + row['_id']),
                                           message=error_message(err))
                names = []
                other = []
                for rec in recs:
                    names.append(f"{rec['given'][0]} {rec['family'][0]}")
                    other.append(f"<a href=\"https://orcid.org/{rec['orcid']}\">{rec['orcid']}</a>")
                html += f"<tr><td>{', '.join(names)}</td><td>{', '.join(other)}</td></tr>"
            html += '</tbody></table>'
        if not html:
            html = "<p>No duplicates found</p>"
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title="ORCID duplicates", html=html,
                                         navbar=generate_navbar('ORCID')))


# ******************************************************************************
# * UI endpoints (People)                                                      *
# ******************************************************************************
@app.route('/orgs')
def peoporgsle():
    ''' Show information on supervisory orgs
    '''
    payload = [{"$unwind": "$affiliations"},
               {"$project": {"_id": 0, "affiliations": 1}},
               {"$group": {"_id": "$affiliations", "count": {"$sum": 1}}}]
    try:
        rows = DB['dis'].orcid.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get affiliations from " \
                                                    + "orcid collection"),
                               message=error_message(err))
    aff = {}
    for row in rows:
        aff[row['_id']] = row['count']
    try:
        orgs = DL.get_supervisory_orgs()
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get supervisory orgs"),
                               message=error_message(err))
    payload = [{"$unwind": "$jrc_tag"},
               {"$project": {"_id": 0, "jrc_tag.name": 1}},
               {"$group": {"_id": "$jrc_tag.name", "count": {"$sum": 1}}}]
    try:
        rows = DB['dis'].dois.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get affiliations from " \
                                                    + "orcid collection"),
                               message=error_message(err))
    tag = {}
    for row in rows:
        if isinstance(row['_id'], dict):
            continue
        tag[row['_id']] = row['count']
    html = "<table id='orgs' class='tablesorter numbers'><thead><tr><th>Name</th><th>Code</th>" \
           + "<th>Authors</th><th>DOI tags</th></tr></thead><tbody>"
    for key, val in sorted(orgs.items()):
        alink = f"<a href='/affiliation/{escape(key)}'>{aff[key]}</a>" if key in aff else ''
        tlink = ""
        if key in tag:
            onclick = "onclick='nav_post(\"jrc_tag.name\",\"" + key + "\")'"
            tlink = f"<a href='#' {onclick}>{tag[key]}</a>"
        html += f"<tr><td>{key}</td><td>{val}</td><td>{alink}</td><td>{tlink}</td></tr>"
    html += "</tbody></table>"
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=f"Supervisory organizations ({len(orgs):,})",
                                         html=html, navbar=generate_navbar('External systems')))


# ******************************************************************************
# * UI endpoints (People)                                                      *
# ******************************************************************************
@app.route('/people/<string:name>')
@app.route('/people')
def people(name=None):
    ''' Show information from the People system
    '''
    if not name:
        return make_response(render_template('people.html', urlroot=request.url_root,
                                             title="Search People system", content="",
                                             navbar=generate_navbar('ORCID')))
    try:
        response = JRC.call_people_by_name(name)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning(f"Could not get People data for {name}"),
                               message=error_message(err))
    if not response:
        return make_response(render_template('people.html', urlroot=request.url_root,
                                             title="Search People system",
                                             content="<br><h3>No names found containing " \
                                                     + f"\"{name}\"</h3>",
                                             navbar=generate_navbar('ORCID')))
    html = "<br><br><h3>Select a name for details:</h3>"
    html += "<table id='people' class='tablesorter standard'><thead><tr><th>Name</th>" \
            + "<th>Title</th><th>Location</th></tr></thead><tbody>"
    for rec in response:
        pname = f"{rec['nameFirstPreferred']} {rec['nameLastPreferred']}"
        link = f"<a href='/peoplerec/{rec['userIdO365']}'>{pname}</a>"
        loc = rec['locationName'] if 'locationName' in rec else ""
        if "Janelia" in loc:
            loc = f"<span style='color:lime'>{loc}</span>"
        html += f"<tr><td>{link}</td><td>{rec['businessTitle']}</td><td>{loc}</td></tr>"
    html += "</tbody></table>"
    return make_response(render_template('people.html', urlroot=request.url_root,
                                         title="Search People system", content=html,
                                         navbar=generate_navbar('External systems')))


@app.route('/peoplerec/<string:eid>')
def peoplerec(eid):
    ''' Show a single People record
    '''
    try:
        rec = JRC.call_people_by_id(eid)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning(f"Could not get People data for {eid}"),
                               message=error_message(err))
    if not rec:
        return render_template('warning.html', urlroot=request.url_root,
                               title=render_warning(f"Could not find People record for {eid}"),
                               message="No record found")
    title = f"{rec['nameFirstPreferred']} {rec['nameLastPreferred']}"
    for field in ['employeeId', 'managerId']:
        if field in rec:
            del rec[field]
    if 'photoURL' in rec:
        title += f"&nbsp;<img src='{rec['photoURL']}' width=100 height=100 " \
                 + f"alt='Photo of {rec['nameFirstPreferred']}'>"
    html = f"<div class='scroll' style='height:750px'><pre>{json.dumps(rec, indent=2)}</pre></div>"
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         navbar=generate_navbar('External systems')))

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
                indices.append(f"{key} ({humansize(val, space='mem')})")
            free = stat['freeStorageSize'] / stat['storageSize'] * 100
            if 'avgObjSize' not in stat:
                stat['avgObjSize'] = 0
            collection[cname] = {"docs": f"{stat['count']:,}",
                                 "docsize": humansize(stat['avgObjSize'], space='mem'),
                                 "size": humansize(stat['storageSize'], space='mem'),
                                 "free": f"{free:.2f}%",
                                 "idx": ", ".join(indices)
                                }
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get collection stats"),
                               message=error_message(err))
    html = '<table id="collections" class="tablesorter numbercenter"><thead><tr>' \
           + '<th>Collection</th><th>Documents</th><th>Avg. document size</th><th>Size</th>' \
            + '<th>Free space</th><th>Indices</th></tr></thead><tbody>'
    for coll, val in sorted(collection.items()):
        html += f"<tr><td>{coll}</td><td>" + dloop(val, ['docs', 'docsize', 'size', 'free', 'idx'],
                                                   "</td><td>") + "</td></tr>"
    html += '</tbody>'
    stat = DB['dis'].command('dbStats')
    val = {"objects": f"{stat['objects']:,}",
              "avgObjSize": humansize(stat['avgObjSize'], space='mem'),
              "storageSize": humansize(stat['storageSize'], space='mem'),
              "blank": "",
              "indexSize": f"{stat['indexes']} indices " \
                           + f"({humansize(stat['indexSize'], space='mem')})"}
    html += '<tfoot>'
    html += "<tr><th style='text-align:right'>TOTAL</th><th style='text-align:center'>" \
            + dloop(val, ['objects', 'avgObjSize', 'storageSize', 'blank', 'indexSize'],
                    "</th><th style='text-align:center'>") + "</th></tr>"
    html += '</tfoot>'
    html += '</table>'
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title="Database statistics", html=html,
                                         navbar=generate_navbar('Stats')))

# ******************************************************************************
# * UI endpoints (tags)                                                        *
# ******************************************************************************
@app.route('/tag/<string:tag>')
def tagrec(tag):
    ''' Show a single tag
    '''
    payload = {"affiliations": tag}
    try:
        acnt = DB['dis'].orcid.count_documents(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get users for tag"),
                               message=error_message(err))
    tagtype = "Affiliation" if acnt else ""
    try:
        orgs = DL.get_supervisory_orgs(coll=DB['dis'].suporg)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get supervisory orgs"),
                               message=error_message(err))
    payload = [{"$match": {"jrc_tag.name": tag}},
               {"$unwind": "$jrc_tag"},
               {"$match": {"jrc_tag.name": tag}},
               {"$group": {"_id": "$jrc_tag.type", "count": {"$sum": 1}}},
               {"$sort": {"_id": 1}}
              ]
    try:
        rows = DB['dis'].dois.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get DOIs for tag"),
                               message=error_message(err))
    html = "<table id='tagprops' class='proplist'><thead></thead><tbody>"
    pdict = {}
    for row in rows:
        pdict[row['_id']] = row['count']
    if not pdict and not acnt:
        return render_template('warning.html', urlroot=request.url_root,
                               title=render_warning(f"Could not find tag {tag}", 'warning'),
                               message="No DOI tags or user affiliations found")
    parr = []
    for key, val in pdict.items():
        parr.append(f"{key}: {val}")
    if tag in orgs:
        tagtype = 'Supervisory org'
        html += f"<tr><td>Tag type</td><td>{tagtype}</td></tr>"
        html += f"<tr><td>Code</td><td>{orgs[tag]['code']}</td></tr>"
        html += f"<tr><td>Status</td><td>"
        if 'active' in orgs[tag]:
            html += f"<span style='color: lime;'>Active</span></td></tr>"
        else:
            html += f"<span style='color: yellow;'>Inactive</span></td></tr>"
    else:
        html += f"<tr><td>Tag type</td><td>{tagtype}</td></tr>"
    if pdict:
        onclick = "onclick='nav_post(\"jrc_tag.name\",\"" + tag + "\")'"
        link = f"<a href='#' {onclick}>Show DOIs</a>"
        html += f"<tr><td>Appears in DOI tags</td><td>{'<br>'.join(parr)}<br>{link}</td></tr>"
    if acnt:
        link = f"<a href='/affiliation/{escape(tag)}'>Show authors</a>"
        html += f"<tr><td>Authors with affiliation</td><td>{acnt}<br>{link}</td></tr>"
    html += "</tbody></table>"
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=f"Tag {tag}", html=html,
                                         navbar=generate_navbar('Tag/affiliation')))

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
