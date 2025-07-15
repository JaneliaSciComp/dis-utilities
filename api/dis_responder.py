''' dis_responder.py
    UI and REST API for Data and Information Services
'''

import collections
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
from urllib.parse import unquote
from bokeh.palettes import all_palettes, plasma
import bson
from flask import (Flask, make_response, render_template, request, jsonify, redirect, send_file)
from flask_cors import CORS
from flask_swagger import swagger
import requests
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL
import dis_plots as DP

# pylint: disable=broad-exception-caught,broad-exception-raised,too-many-lines,too-many-locals

__version__ = "62.0.0"
# Database
DB = {}
CVTERM = {}
PROJECT = {}
# Custom queries
CUSTOM_REGEX = {"publishing_year": {"field": "jrc_publishing_date",
                                    "value": "^!REPLACE!"}
               }

# Navigation
NAV = {"Home": "",
       "DOIs": {"DOIs by insertion date": "dois_insertpicker",
                "DOI stats": "dois_source",
                "DOIs awaiting processing": "dois_pending",
                "DOIs by publisher": "dois_publisher",
                "DOIs by subject": "dois_subjectpicker",
                "DOIs by year": "dois_year",
                "DOIs by month": "dois_month",
                "DOI yearly report": "dois_report"},
       "DataCite": {"DataCite DOI stats": "datacite_dois",
                    "DataCite DOI downloads": "datacite_downloads",
                    "DataCite subjects": "datacite_subject"},
       "Authorship": {"DOIs by authorship": "dois_author",
                      "DOIs with lab head first/last authors": "doiui_group",
                      "Top first and last authors": "dois_top_author",
                      "DOIs without Janelia authors": "dois_no_janelia"},
       "Preprints": {"DOIs by preprint status": "dois_preprint",
                     "DOIs by preprint status by year": "dois_preprint_year",
                     "Preprints with journal publications": "preprint_with_pub",
                     "Preprints without journal publications": "preprint_no_pub",
                     "Journal publications without preprints": "pub_no_preprint"},
       "Journals": {"DOIs by journal": "journals_dois",
                    "Top journals": "top_journals",
                    "DOIs missing journals": "dois_nojournal",
                    "Journals referenced": "journals_referenced"},
       "Subscriptions": {"Summary": "subscriptions",
                         "Journals": "subscriptions/Journal",
                         "Books": "subscriptions/Book",
                         "Book series": "subscriptions/Book series",
                         "Monographs": "subscriptions/Monograph"},
       "ORCID": {"Entries": "orcid_entry",
                 "Labs": "labs",
                 "Latest hires": "orcid_datepicker",
                 "Authors with multiple ORCIDs": "orcid_duplicates",
                 "Duplicate authors": "duplicate_authors"},
       "Tag/affiliation": {"DOIs by tag": "dois_tag",
                           "DOIs by acknowledgement": "dois_ack",
                           "Top DOI tags by year": "dois_top",
                           "Author affiliations: P&C": "orcid_tag",
                           "Author affiliations: Janelia": "janelia_affiliations",
                           "Projects": "projects"},
       "System" : {"Database stats": "stats_database",
                   "Controlled vocabularies": "cv",
                   "DOI relationships": "doi_relationships",
                   "Endpoints": "stats_endpoints"},
       "External systems": {"Search HHMI People system": "people",
                            "HHMI Supervisory Organizations": "orgs/full",
                            "ROR": "ror"}
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
        try:
            rows = DB['dis'].cvterm.find({})
            for row in rows:
                if row['cv'] not in CVTERM:
                    CVTERM[row['cv']] = {}
                CVTERM[row['cv']][row['name']] = row
            rows = DB['dis'].project_map.find({"doNotUse": {"$exists": False}})
            for row in rows:
                PROJECT[row['name']] = True
                PROJECT[row['project']] = True
        except Exception as err:
            return render_template('warning.html', urlroot=request.url_root,
                                   title=render_warning("Database error"), message=err)
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
          severity: severity (warning, error, info, or success)
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
    elif severity == 'info':
        icon = 'circle-info'
        color = 'blue'
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
                       "authorized": False,
                       "url": request.url,
                       "endpoint": request.endpoint,
                       "error": False,
                       "elapsed_time": "",
                       "row_count": 0,
                       "pid": os.getpid()}}
    if app.config["LAST_TRANSACTION"]:
        print(f"Seconds since last transaction: {time() - app.config['LAST_TRANSACTION']}")
    app.config["LAST_TRANSACTION"] = time()
    if "Authorization" in request.headers:
        token = re.sub(r'Bearer\s+', "", request.headers["Authorization"])
        result['rest']['authorized'] = bool(token in app.config['KEYS'].values())
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
    elif ipd['value'] == "!EXISTS!":
        ipd['value'] = {"$exists": 1}
    fdisplay = CVTERM['jrc'][ipd['field']]['display'] if ipd['field'] in CVTERM['jrc'] \
               else ipd['field']
    ptitle = f"DOIs for {fdisplay} {display_value}"
    payload = {ipd['field']: ipd['value']}
    if 'jrc_obtained_from' in ipd and ipd['jrc_obtained_from']:
        payload['jrc_obtained_from'] = ipd['jrc_obtained_from']
        ptitle += f" from {ipd['jrc_obtained_from']}"
    return payload, ptitle

# ******************************************************************************
# * ORCID utility functions                                                    *
# ******************************************************************************

def get_leads_and_org_members(org):
    ''' Get lab head employee IDs and organization members
        Keyword arguments:
          org: organization name
        Returns:
          leads: list of lab head employee IDs
          shared: list of organization members
    '''
    # Get lab head employee IDs
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
    # Get Shared Resources employee IDs
    payload = {"group": org}
    try:
        row = DB['dis'].org_group.find_one(payload)
    except Exception as err:
        raise err
    shared = []
    if row:
        for member in row['members']:
            shared.append(member)
    return leads, shared


def get_org_authorship(year, leads, shared):
    ''' Get organization authorship
        Keyword arguments:
          year: year
          leads: list of lab head employee IDs
          shared: list of organization members
        Returns:
          finds: dictionary of journals with lab head last authors for Janelia and the specified org
    '''
    finds = {"janelia": [], "org": []}
    payload = {"jrc_last_id": {"$in": leads}}
    if year != 'All':
        payload['jrc_publishing_date'] = {"$regex": "^"+ year}
    try:
        rows = DB['dis'].dois.find(payload)
        for row in rows:
            if DL.is_journal(row) and not DL.is_version(row):
                finds['janelia'].append(row['jrc_publishing_date'])
        payload['jrc_tag.name'] = {"$in": shared}
        rows = DB['dis'].dois.find(payload)
        for row in rows:
            if DL.is_journal(row) and not DL.is_version(row):
                finds['org'].append(row['jrc_publishing_date'])
    except Exception as err:
        raise err
    return finds


def get_dup_color(occur):
    ''' Get a background color for a duplicate author
        Keyword arguments:
          occur: list of occurrences of an author name
        Returns:
          Background color  
    '''
    orcid = []
    affiliation = []
    email = []
    for occ in occur:
        if 'User ID' in occ:
            match = re.search(r"User ID: (\S*)", occ)
            if match and match.group(0) not in email:
                email.append(match.group(0))
        if 'Affiliations' in occ:
            match = re.search(r"Affiliations: (\S*)", occ)
            if match and match.group(0) not in affiliation:
                affiliation.append(match.group(0))
        if 'ORCID' in occ:
            match = re.search(r"ORCID: (\S*)", occ)
            if match and match.group(0) not in orcid:
                orcid.append(match.group(0))
    if len(affiliation) == len(occur):
        return "gold"
    if len(orcid) == len(occur):
        return "lightsalmon"
    print(email)
    if len(email) == 1:
        return "orange"
    return "red"


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
        else:
            pdate += "-01"
        if 'day' in ppd and ppd['day'] and ppd['day']['value']:
            pdate += f"-{ppd['day']['value']}"
        else:
            pdate += "-01"
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


def add_to_name(given, name, grow):
    ''' Add a name to the given dictionary
        Keyword arguments:
          given: dictionary of names
          name: name to add
          grow: single orcid record
        Returns:
          None
    '''
    if name not in given:
        given[name] = []
    comp = []
    if 'userIdO365' in grow:
        comp.append(f"User ID: <a href='/peoplerec/{grow['userIdO365']}'>{grow['userIdO365']}</a>")
    if 'orcid' in grow:
        comp.append(f"ORCID: {grow['orcid']}")
    if 'affiliations' in grow:
        comp.append("Affiliations: " + ", ".join(grow['affiliations']))
    if 'alumni' in grow:
        comp.append(f"{tiny_badge('alumni', 'Former employee')}")
    given[name].append(' '.join(comp))



def name_search_payload(given, family):
    ''' Generate a payload for searching the orcid collection by names
        Keyword arguments:
          given: list of given names
          family: list of family names
        Returns:
          Payload
    '''
    cross = {"author": {"$elemMatch": {"given": {'$in': given},
                                       "family": {'$in': family}}}}
    data = {"creators": {"$elemMatch": {"givenName": {'$in': given},
                                        "familyName": {'$in': family}}}}
    return {"$or": [cross, data]}


def single_name_search_payload(given, family):
    ''' Generate a payload for searching the orcid collection by a single name
        Keyword arguments:
          given: given name
          family: family name
        Returns:
          Payload
    '''
    cross = {"author": {"$elemMatch": {"given": {"$regex": f"^{given}$", "$options" : "i"},
                                       "family": {"$regex": f"^{family}$", "$options" : "i"}}}}
    data = {"creators": {"$elemMatch": {"givenName": {"$regex": f"^{given}$", "$options" : "i"},
                                        "familyName": {"$regex": f"^{family}$", "$options" : "i"}}}}
    return {"$or": [cross, data,
                    {"$or": [{"creators.name": {"$regex": f"^{given}$", "$options" : "i"}},
                             {"creators.name": {"$regex": f"^{family}$", "$options" : "i"}}]},
                   ]}



def orcid_payload(oid, orc, eid=None):
    ''' Generate a payload for searching the dois collection by ORCID or employeeId
        Keyword arguments:
          oid: ORCID or employeeId
          orc: orcid record
          eid: employeeId boolean
        Returns:
          Payload
    '''
    payload = {}
    # Name only search
    npayload = name_search_payload(orc['given'], orc['family'])
    if eid and not oid:
        # Employee ID only search
        payload = {"$or": [{"jrc_author": eid}, npayload]}
    elif oid and eid:
        # Search by either name or employee ID
        payload = {"$or": [{"orcid": oid}, {"jrc_author": eid}, npayload]}
    elif oid and not eid:
        # Search by either name or ORCID
        payload = {"$or": [{"orcid": oid}, npayload]}
    payload = payload or npayload
    return payload


def author_doi_count(given, family):
    ''' Get the number of DOIs for a given author
        Keyword arguments:
          given: list of given names
          family: list of family names
        Returns:
          Number of DOIs
    '''
    payload = name_search_payload(given, family)
    rows = DB['dis'].dois.find(payload, {"doi": 1, "author": 1, "creators": 1})
    dois = 0
    for row in rows:
        field = 'author' if 'author' in row else 'creators'
        for aut in row[field]:
            if field == 'author' and 'given' in aut:
                if aut['given'] in given and aut['family'] in family:
                    dois += 1
                    break
            elif 'givenName' in aut and aut['givenName'] in given and aut['familyName'] in family:
                dois += 1
                break
    if dois:
        dois = f"{dois:,} DOI{'' if dois == 1 else 's'}"
        return f" {tiny_badge('info', dois)}"
    return ""


def get_dois_for_orcid(oid, orc):
    ''' Generate DOIs for a single user
        Keyword arguments:
          oid: ORCID or employeeId
          orc: orcid record
        Returns:
          HTML and a list of DOIs
    '''
    try:
        eid = orc['employeeId'] if 'employeeId' in orc else None
        payload = orcid_payload(oid, orc, eid)
        rows = DB['dis'].dois.find(payload)
    except Exception as err:
        raise CustomException(err, "Could not find in dois collection by name.") from err
    return rows


def get_work_title(row):
    ''' Get a work title
        Keyword arguments:
          row: row from dois collection
        Returns:
          title
    '''
    if 'title' in row and isinstance(row['title'], str):
        return row['title']
    return DL.get_title(row)


def generate_works_table(rows, name=None, show="full"):
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
        if show == "journal" and not (("type" in row and row['type'] == "journal-article") \
                                  or ("types" in row and "resourceTypeGeneral" in row["types"] \
                                      and row["types"]["resourceTypeGeneral"] == "Preprint") \
                                  or ("subtype" in row and row['subtype'] == "preprint")):
            continue
        doi = doi_link(row['doi']) if row['doi'] else "&nbsp;"
        dois.append(row['doi'])
        payload = {"date":  DL.get_publishing_date(row),
                   "doi": doi,
                   "title": get_work_title(row),
                   "raw": row
                  }
        works.append(payload)
        fileoutput += f"{payload['date']}\t{row['doi']}\t{payload['title']}\n"
        if name:
            alist = DL.get_author_details(row)
            if alist:
                for auth in alist:
                    if "family" in auth and "given" in auth \
                       and auth["family"].lower() == name.lower():
                        aname = f"{auth['given']} {auth['family']}"
                        authors[aname] = f"<a href=/doisui_name/{auth['family']}/" \
                                         + f"{auth['given'].replace(' ', '%20')}>{aname}</a>"
            else:
                print(f"Could not get author details for {row['doi']}")
    if not works:
        return html, []
    html += "<table id='pubs' class='tablesorter standard'>" \
            + '<thead><tr><th>Published</th><th>DOI</th><th>Title</th></tr></thead><tbody>'
    for work in sorted(works, key=lambda row: row['date'], reverse=True):
        version = DL.is_version(work['raw'])
        cls = []
        if version:
            cls.append('ver')
        html += f"<tr class=\'{' '.join(cls)}\'><td>{work['date']}</td>" \
                + f"<td>{work['doi'] if work['doi'] else '&nbsp;'}</td>" \
                + f"<td>{work['title']}</td></tr>"
    if dois:
        html += "</tbody></table>"
    if authors:
        html = f"<br>Authors found: {', '.join(sorted(authors.values()))}<br>" \
               + f"This may include non-Janelia authors<br>{html}"
    cbutton = "<button class=\"btn btn-outline-warning\" " \
              + "onclick=\"toggler('pubs', 'ver', 'totalrows');\">" \
              + "Filter for versioned DOIs</button>"
    html = cbutton + create_downloadable('works', ['Published', 'DOI', 'Title'], fileoutput) + html
    html = f"Number of DOIs: <span id='totalrows'>{len(works)}</span><br>" + html
    return html, dois


def get_orcid_from_db(oid, use_eid=False, bare=False, show="full"):
    ''' Generate HTML for an ORCID or employeeId that is in the orcid collection
        Keyword arguments:
          oid: ORCID or employeeId
          use_eid: use employeeId boolean
          bare: entry has no ORCID or employeeId
        Returns:
          HTML, a list of DOIs, and full name
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
        return "", [], ""
    full_name = " ".join([orc['given'][0], orc['family'][0]])
    html = "<br><table class='borderless'>"
    if use_eid and 'orcid' in orc:
        html += f"<tr><td>ORCID:</td><td><a href='https://orcid.org/{orc['orcid']}'>" \
                + f"{orc['orcid']}</a></td></tr>"
    html += f"<tr><td>Given name:</td><td>{', '.join(sorted(orc['given']))}</td></tr>"
    html += f"<tr><td>Family name:</td><td>{', '.join(sorted(orc['family']))}</td></tr>"
    if 'orcid' in orc:
        html += f"<tr><td>ORCID:</td><td><a href='https://orcid.org/{orc['orcid']}'>" \
                + f"{orc['orcid']}</a></td></tr>"
    if 'userIdO365' in orc:
        link = "<a href='" + f"{app.config['WORKDAY']}{orc['userIdO365']}" \
               + f"' target='_blank'>{orc['userIdO365']}</a>"
        html += f"<tr><td>User ID:</td><td>{link}</td></tr>"
    if 'affiliations' in orc:
        html += f"<tr><td>Affiliations:</td><td>{', '.join(orc['affiliations'])}</td></tr>"
    html += "</table><br>"
    if 'orcid' in orc:
        olink = f"/orcidapi/{orc['orcid']}"
        html += f" {tiny_badge('info', 'Show ORCID data', olink)}"
    if 'userIdO365' in orc:
        olink = f"/peoplerec/{orc['userIdO365']}"
        html += f" {tiny_badge('info', 'Show People data', olink)}"
    html += "<br>"
    try:
        if use_eid:
            oid = orc['employeeId']
        rows = get_dois_for_orcid(oid, orc)
    except Exception as err:
        raise err
    tablehtml, dois = generate_works_table(rows, name=None, show=show)
    sad = DL.get_single_author_details(orc, DB['dis'].orcid)
    if tablehtml:
        html = f"{' '.join(get_badges(sad, True))}{html}{tablehtml}"
    else:
        html = f"{' '.join(get_badges(sad, True))}{html}<br>No works found in dois collection."
    return html, dois, full_name


def add_orcid_works(data, dois, return_html=True):
    ''' Generate HTML or JSON for a list of works from ORCID
        Keyword arguments:
          data: ORCID data
          dois: list of DOIs from dois collection
          return_html: return results as HTML
        Returns:
          HTML or JSON for a list of works from ORCID
    '''
    html = inner = ""
    results = []
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
        results.append({"date": pdate, "doi": doi, "title": wsumm['title']['title']['value']})
    if inner:
        title = "title is" if works == 1 else f"{works} titles are"
        html += f"<hr>The additional {title} from ORCID. Note that titles below may " \
                + "be self-reported, may not have DOIs available, or may be from the author's " \
                + "employment outside of Janelia.</br>"
        html += '<table id="works" class="tablesorter standard"><thead><tr>' \
                + '<th>Published</th><th>DOI</th><th>Title</th>' \
                + f"</tr></thead><tbody>{inner}</tbody></table>"
    return html if return_html else results


def endpoint_access():
    ''' Increment an endpoint counter
        Keyword arguments:
          None
        Returns:
          None
    '''
    endpoint = str(request.url_rule).split('/')[1]
    coll = DB['dis'].api_endpoint
    try:
        row = coll.find_one({"endpoint": endpoint})
        if row:
            coll.update_one({"endpoint": endpoint}, {"$inc": {"count": 1}})
        else:
            coll.insert_one({"endpoint": endpoint, "count": 1})
    except Exception:
        pass
    endpoint = unquote(request.url.replace(request.url_root, ""))
    coll = DB['dis'].api_endpoint_details
    try:
        row = coll.find_one({"endpoint": endpoint})
        if row:
            coll.update_one({"endpoint": endpoint}, {"$inc": {"count": 1}})
        else:
            coll.insert_one({"endpoint": endpoint, "count": 1})
    except Exception:
        pass


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
        badges = get_badges(auth, True)
        rclass = 'other' if (auth and auth['alumni']) else 'active'
        html += f"<tr class={rclass}><td>{link}</td><td>{', '.join(sorted(row['given']))}</td>" \
                + f"<td>{', '.join(sorted(row['family']))}</td><td>{' '.join(badges)}</td></tr>"
    html += '</tbody></table>'
    cbutton = "<button class=\"btn btn-outline-warning\" " \
              + "onclick=\"toggler('ops', 'other', 'totalrowsa');\">" \
              + "Filter for current authors</button>"
    html = cbutton + html
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
    try:
        if DL.is_datacite(doi):
            resp = JRC.call_datacite(doi)
            source = 'datacite'
            data = resp['data']['attributes'] if 'data' in resp else {}
        else:
            resp = JRC.call_crossref(doi)
            source = 'crossref'
            data = resp['message'] if 'message' in resp else {}
    except Exception as err:
        raise err
    return source, data


def get_separator(last, this):
    ''' Get a separator between dates, with a badge if the delta a day or more
        Keyword arguments:
          last: last date
          this: this date
        Returns:
          HTML
    '''
    delta = (datetime.strptime(this, '%Y-%m-%d') - datetime.strptime(last, '%Y-%m-%d')).days
    if delta and delta >= 0:
        delta =  f"{delta:,} day{'s' if delta > 1 else ''}"
        return f" &rarr; {tiny_badge('delta', delta, size=10)} &rarr; "
    return "&nbsp;&nbsp;&rarr;&nbsp;&nbsp;"


def add_update_times(row):
    ''' Produce a horizontal list of important record times
        Keyword arguments:
          row: DOI record
        Returns:
          HTML
    '''
    date_list = []
    last = None
    if 'jrc_updated' in row:
        updated = str(row['jrc_updated']).split(' ', maxsplit=1)[0]
    else:
        updated = None
    if 'jrc_publishing_date' in row:
        last = str(row['jrc_publishing_date']).split(' ', maxsplit=1)[0]
        date_list.append(f"Published {last}")
    if 'jrc_inserted' in row:
        this = str(row['jrc_inserted']).split(' ', maxsplit=1)[0]
        if last:
            date_list.append(get_separator(last, this))
        last = this
        date_list.append(f"Inserted {this}")
    if 'jrc_newsletter' in row:
        this = str(row['jrc_newsletter']).split(' ', maxsplit=1)[0]
        if last:
            if updated and updated < this:
                date_list.append(get_separator(last, updated))
                date_list.append(f"Updated {updated}")
                last = updated
                updated = None
            date_list.append(get_separator(last, this))
        last = this
        date_list.append(f"Added to newsletter {this}")
    if updated:
        this = updated
        if last:
            date_list.append(get_separator(last, this))
        last = this
        date_list.append(f"Updated {this}")
    if date_list:
        return f"<span class='paperdata'>{''.join(date_list)}</span>"
    return ""


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
            if not val:
                continue
            try:
                if isinstance(val[0], dict):
                    val = ", ".join(sorted(elem['name'] for elem in val))
                else:
                    val = ", ".join(sorted(val))
            except TypeError:
                val = json.dumps(val)
            except Exception as err:
                print(key, val)
                print(f"Error in add_jrc_fields for {row['doi']}: {err}")
        jrc[key] = val
    if not jrc:
        return ""
    html = '<table class="standard">'
    for key in sorted(jrc):
        if key in ['jrc_pmid']:
            continue
        val = jrc[key]
        if key == 'jrc_author':
            link = []
            for auth in val.split(", "):
                link.append(f"<a href='/userui/{auth}'>{auth}</a>")
            val = ", ".join(link)
        if key == 'jrc_preprint':
            val = doi_link(val)
        elif key in ['jrc_tag', 'jrc_acknowledge']:
            link = []
            for aff in val.split(", "):
                link.append(f"<a href='/tag/{escape(aff)}'>{aff}</a>")
            val = ", ".join(link)
        html += f"<tr><td>{CVTERM['jrc'][key]['display'] if key in CVTERM['jrc'] else key}</td>" \
                + f"<td>{val}</td></tr>"
    html += "</table><br>"
    return html


def make_link(url):
    ''' Create a link from a URL
        Keyword arguments:
          url: URL
        Returns:
          HTML link
    '''
    return f"<a href='{url}' target='_blank'>{url}</a>"


def get_relations_from_row(row):
    ''' Get relations from a row
        Keyword arguments:
          row: DOI record
        Returns:
          relations
    '''
    relations = {}
    if "relation" in row and row['relation']:
        # Crossref relations
        for rel in row['relation']:
            used = []
            for itm in row['relation'][rel]:
                if itm['id'] in used:
                    continue
                if rel not in relations:
                    relations[rel] = []
                if itm['id-type'] == 'uri':
                    relations[rel].append(f"<a href='{itm['id']}'>(Other resource)</a>")
                elif itm['id-type'] == 'doi':
                    relations[rel].append(doi_link(itm['id']))
                else:
                    relations[rel].append(itm['id'])
                used.append(itm['id'])
    elif 'relatedIdentifiers' in row and row['relatedIdentifiers']:
        # DataCite relations
        for rel in row['relatedIdentifiers']:
            if 'relatedIdentifierType' in rel and rel['relatedIdentifierType'] == 'DOI':
                if rel['relationType'] not in relations:
                    relations[rel['relationType']] = []
                relations[rel['relationType']].append(doi_link(rel['relatedIdentifier']))
            elif 'relatedIdentifierType' in rel and rel['relatedIdentifierType'] == 'URL':
                if rel['relationType'] not in relations:
                    relations[rel['relationType']] = []
                relations[rel['relationType']].append(make_link(rel['relatedIdentifier']))
    return relations


def add_relations(row):
    ''' Create a list of relations
        Keyword arguments:
          row: DOI record
        Returns:
          HTML
    '''
    relations = get_relations_from_row(row)
    html = ""
    for rel, val in relations.items():
        if '-' not in rel:
            words = re.split('(?<=.)(?=[A-Z])', rel)
            rel = ' '.join(wrd.lower() for wrd in words)
        html += f"This DOI {rel.replace('-', ' ')} " + ", ".join(val) + "<br>"
    return html


def get_top_authors(atype, year):
    ''' Get the top authors for a given year for articles and DataCite entries
        Keyword arguments:
          atype: author type
          year: year
        Returns:
          Top authors as a MongoDB object
    '''
    payload = [{"$match": {f"jrc_{atype}_author": {"$exists": 1},
                           "type": {"$in": app.config['ARTICLES']}}},
               {"$unwind": f"$jrc_{atype}_author"},
               {"$group": {"_id": f"$jrc_{atype}_author", "count": {"$sum": 1}}},
               {"$sort" : {"count": -1}},
               {"$limit": 10}
              ]
    if year != 'All':
        payload[0]['$match']['jrc_publishing_date'] = {"$regex": "^"+ year}
    return DB['dis'].dois.aggregate(payload)


def get_migration_data(row):
    ''' Create a migration record for a single DOI
        Keyword arguments:
          doi: doi record
          orgs: dictionary of organizations/codes
        Returns:
          migration dictionary
    '''
    rec = {}
    # Authors and tags
    if 'jrc_author' in row:
        rec['jrc_author'] = row['jrc_author']
    tags = []
    if 'jrc_tag' in row and row['jrc_tag']:
        if isinstance(row['jrc_tag'][0], dict):
            for atag in row['jrc_tag']:
                tags.append(atag)
    if tags:
        rec['tags'] = tags
    # Additional data
    for key in ['jrc_publishing_date', 'publisher', 'type', 'subtype']:
        if key in row:
            rec[key] = row[key]
    if row['jrc_obtained_from'] == 'DataCite' and 'types' in row:
        if 'resourceTypeGeneral' in row['types']:
            rec['type'] = row['types']['resourceTypeGeneral']
    if row['jrc_obtained_from'] == 'Crossref' and 'abstract' in row:
        rec['abstract'] = row['abstract']
    rec['journal'] = DL.get_journal(row)
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
    for key in app.config['ARTICLES']:
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
    preprints = 0
    for row in rows:
        typ = row['_id']['type'] if 'type' in row['_id'] else "DataCite"
        sub = row['_id']['subtype'] if 'subtype' in row['_id'] else ""
        if sub == 'preprint':
            preprints += row['count']
            typ = 'posted-content'
        elif (typ == 'DataCite' and row['_id']['DataCite'] == 'Preprint'):
            preprints += row['count']
        if typ not in typed:
            typed[typ] = 0
        typed[typ] += row['count']
    typed['preprints'] = preprints
    return typed


def get_first_last_authors(year):
    ''' Get first and last author counts
        Keyword arguments:
          year: year to get counts for
        Returns:
          First and last author counts
    '''
    stat = {'first': {}, 'last': {}, 'any': {}}
    for which in ("first", "last", "any"):
        if which == 'any':
            payload = [{"$match": {"jrc_publishing_date": {"$regex": "^"+ year},
                                   "jrc_author": {"$exists": True}}},
                       {"$group": {"_id": {"type": "$type", "subtype": "$subtype",
                                           "DataCite": "$types.resourceTypeGeneral"},
                                   "count": {"$sum": 1}}}
                      ]
        else:
            payload = [{"$match": {"jrc_publishing_date": {"$regex": "^"+ year},
                                   f"jrc_{which}_author": {"$exists": True}}},
                       {"$group": {"_id": {"type": "$type", "subtype": "$subtype",
                                           "DataCite": "$types.resourceTypeGeneral"},
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
            if sub == 'preprint' or (type == 'DataCite' and row['_id']['DataCite'] == 'Preprint'):
                if 'preprints' not in stat[which]:
                    stat[which]['preprints'] = 0
                stat[which]['preprints'] += row['count']
    print(json.dumps(stat, indent=2))
    return stat['first'], stat['last'], stat['any']


def get_no_relation(year=None):
    ''' Get DOIs with no relation
        Keyword arguments:
          year: year (optional)
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
    if year:
        for pay in payload.values():
            pay["jrc_publishing_date"] = {"$regex": "^"+ year}
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
        if fmt == 'html' and data['citationCount']:
            cnt = f"<a href='{app.config['S2']}{data['paperId']}' target='_blank'>" \
                  + f"{data['citationCount']}</a>"
        else:
            cnt = data['citationCount']
        return cnt
    except Exception:
        return 0


def wos_citation_count(doi):
    ''' Get citation count from Web of Science
        Keyword arguments:
          doi: DOI
        Returns:
          Citation count
    '''
    url = f"{app.config['WOS_DOI']}{doi}"
    headers = {'X-ApiKey': app.config['WOS_API_KEY']}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 429:
            raise Exception("Rate limit exceeded")
        if resp.status_code != 200:
            return 0
        data = resp.json()
        if 'hits' in data and len(data['hits']) > 0 and 'citations' in data['hits'][0]:
            for citation in data['hits'][0]['citations']:
                if citation['db'] == 'WOS':
                    return citation['count']
        else:
            return 0
    except Exception:
        return 0


def standard_doi_table(rows, prefix=None):
    ''' Create a standard table of DOIs
        Keyword arguments:
          rows: rows from dois collection
          year: prefix for year pulldown
        Returns:
          HTML
    '''
    header = ['Published', 'DOI', 'Title']
    html = "<table id='dois' class='tablesorter standard'><thead><tr>" \
           + ''.join([f"<th>{itm}</th>" for itm in header]) + "</tr></thead><tbody>"
    fileoutput = ""
    cnt = 0
    for row in rows:
        version = DL.is_version(row)
        row['published'] = DL.get_publishing_date(row)
        row['link'] = doi_link(row['doi'])
        row['title'] = DL.get_title(row)
        cls = []
        if version:
            cls.append('ver')
        # payload["$or"] = [{"type": "journal-article"}, {"types.resourceTypeGeneral": "Preprint"},
        #                   {"subtype": "preprint"}]
        if not (('type' in row and row['type'] == 'journal-article') \
           or ('subtype' in row and row['subtype'] == 'preprint') \
           or ('types' in row and 'resourceTypeGeneral' in row['types'] \
           and row['types']['resourceTypeGeneral'] == 'Preprint')):
            cls.append('notjournal')
        html += f"<tr class=\'{' '.join(cls)}\'><td>" \
            + dloop(row, ['published', 'link', 'title'], "</td><td>") + "</td></tr>"
        if row['title']:
            row['title'] = row['title'].replace("\n", " ")
        cnt += 1
        fileoutput += dloop(row, ['published', 'doi', 'title']) + "\n"
    html += '</tbody></table>'
    counter = f"<p>Number of DOIs: <span id='totalrows'>{cnt}</span></p>"
    cbutton = "<button id='verbtn' class=\"btn btn-outline-warning\" " \
              + "onclick=\"toggler('dois', 'ver', 'totalrows');\">" \
              + "Filter versioned DOIs</button>&nbsp;"
    if prefix:
        html = counter + year_pulldown(prefix) + "&nbsp;"*5 \
               + cbutton + create_downloadable('standard', header, fileoutput) + html
    else:
        html = counter + cbutton + create_downloadable('standard', header, fileoutput) + html
    return html, cnt

# ******************************************************************************
# * Badge utility functions                                                    *
# ******************************************************************************

def tiny_badge(btype, msg, link=None, size=8):
    ''' Create HTML for a [very] small badge
        Keyword arguments:
          btype: badge type (success, danger, etc.)
          msg: message to show on badge
          link: link to other web page
          size: size of badge (default 8)
        Returns:
          HTML
    '''
    html = f"<span class='badge badge-{btype}' style='font-size: {size}pt'>{msg}</span>"
    if link:
        html = f"<a href='{link}' target='_blank'>{html}</a>"
    return html


def get_badges(auth, ignore_match=False, who=None):
    ''' Create a list of badges for an author
        Keyword arguments:
          auth: detailed author record
          ignore_match: ignore match status
        Returns:
          List of HTML badges
    '''
    badges = []
    if 'in_database' in auth and auth['in_database']:
        #badges.append(f"{tiny_badge('database', 'In database')}")
        if auth['alumni']:
            badges.append(f"{tiny_badge('alumni', 'Former employee')}")
        elif 'validated' not in auth or not auth['validated']:
            badges.append(f"{tiny_badge('warning', 'Not validated')}")
        if 'orcid' not in auth or not auth['orcid']:
            badges.append(f"{tiny_badge('noorcid', 'No ORCID')}")
        if auth['asserted']:
            badges.append(f"{tiny_badge('asserted', 'Janelia affiliation')}")
        elif 'match' in auth and auth['match'] == 'ORCID':
            badges.append(f"{tiny_badge('orcid', 'ORCID' if ignore_match else 'ORCID match')}")
        elif 'match' in auth and auth['match'] == 'name' and not ignore_match:
            badges.append(f"{tiny_badge('name', 'Name match')}")
        if 'workerType' in auth and auth['workerType'] and auth['workerType'] != 'Employee':
            badges.append(f"{tiny_badge('contingent', auth['workerType'])}")
        if 'group' in auth:
            badges.append(f"{tiny_badge('lab', auth['group'])}")
        if 'managed' in auth and auth['managed']:
            for key in auth['managed']:
                badges.append(f"{tiny_badge('managed', key)}")
        if 'duplicate_name' in auth:
            badges.append(f"{tiny_badge('warning', 'Duplicate name')}")
    else:
        if who in PROJECT:
            badges.append(f"{tiny_badge('projecttag', 'Project tag')}")
        else:
            badges.append(f"{tiny_badge('danger', 'Not in database')}")
        if 'asserted' in auth and auth['asserted']:
            badges.append(f"{tiny_badge('asserted', 'Janelia affiliation')}")
        if 'match' in auth and auth['match'] == 'ORCID':
            badges.append(f"{tiny_badge('orcid', 'ORCID match')}")
    return badges


def show_tagged_authors(authors, confirmed):
    ''' Create a list of Janelian authors (with badges and tags)
        Keyword arguments:
          authors: list of detailed authors from a publication
          confirmed: list of confirmed authors
        Returns:
          List of HTML authors
    '''
    alist = []
    count = 0
    for auth in authors:
        is_project = False
        if 'name' in auth and auth['name'] in PROJECT:
            # Normally, a project would come back as not being a Janelian.
            auth['janelian'] = True
            is_project = True
        if (not auth['janelian']) and (not auth['asserted']) and (not auth['alumni']):
            continue
        if auth['janelian'] or auth['asserted']:
            count += 1
        if 'family' in auth:
            who = f"{auth['given']} {auth['family']}"
        # The next four lines are brought to you by authors that don't use their full names...
        elif 'name' in auth:
            who = auth['name']
        else:
            who = auth['given']
        if 'orcid' in auth and auth['orcid']:
            who = f"<a href='/userui/{auth['orcid']}'>{who}</a>"
        elif 'userIdO365' in auth and auth['userIdO365']:
            who = f"<a href='/userui/{auth['userIdO365']}'>{who}</a>"
        badges = get_badges(auth, who=who)
        if 'employeeId' in auth and auth['employeeId'] in confirmed:
            badges.insert(0, tiny_badge('author', 'Janelia author'))
        if is_project:
            who = f"<a href='/tag/{auth['name']}'>{who}</a>"
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
    #badges.append(tiny_badge('database', 'In database'))
    if 'duplicate_name' in orc:
        badges.append(tiny_badge('warning', 'Duplicate name'))
    if 'orcid' not in orc or not orc['orcid']:
        badges.append(f"{tiny_badge('noorcid', 'No ORCID')}")
    if 'alumni' in orc:
        badges.append(tiny_badge('alumni', 'Former employee'))
    if 'employeeId' not in orc:
        badges.append(tiny_badge('warning', 'Not validated'))
    return badges

# ******************************************************************************
# * Journal utility functions                                                  *
# ******************************************************************************

def get_subscriptions(stype='Journal'):
    ''' Get subscriptions
        Keyword arguments:
          stype: type to get data for
        Returns:
          Subscription data
    '''
    try:
        rows = DB['dis'].subscription.find({"type": stype})
    except Exception as err:
        raise err
    sub = {}
    for row in rows:
        sub[row['title']] = True
    return sub


def get_top_journals(year, maxpub=False, janelia=True):
    ''' Get top journals
        Keyword arguments:
          year: year to get data for
          maxpub: if True, get max publishing date
        Returns:
          Journal data
    '''
    if janelia:
        match = {"jrc_journal": {"$exists": True}}
    else:
        match = {"$and": [{"jrc_journal": {"$exists": True}},
                          {"jrc_journal": {"$ne": "Janelia Research Campus (non-publication)"}}]}
    if year != 'All':
        match["jrc_publishing_date"] = {"$regex": "^"+ year}
    payload = [{"$match": match},
               {"$group": {"_id": "$jrc_journal", "count":{"$sum": 1},
                           "maxpub": {"$max": "$jrc_publishing_date"}}}
               ]
    try:
        rows = DB['dis'].dois.aggregate(payload)
    except Exception as err:
        raise err
    journal = {}
    for row in rows:
        if maxpub:
            journal[row['_id']] = {"count": row['count'], "maxpub": row['maxpub']}
        else:
            journal[row['_id']] = row['count']
    if not journal:
        return {}
    return journal

# ******************************************************************************
# * Suporg utility functions                                                   *
# ******************************************************************************

def get_suporgs():
    ''' Get supervisory orgs
        Keyword arguments:
          None
        Returns:
          hqorgs: mapping of HQ suporg name to code
          suporgs: mapping of all suporgs to code and active status
    '''
    try:
        hqorgs = DL.get_supervisory_orgs()
    except Exception as err:
        raise err
    try:
        rows = DB['dis'].suporg.find({})
    except Exception as err:
        raise err
    suporgs = {}
    for row in rows:
        suporgs[row['name']] = {"code": row['code'],
                                "active": bool(row['name'] in hqorgs)}
    return hqorgs, suporgs

# ******************************************************************************
# * Tag utility functions                                                      *
# ******************************************************************************

def get_tag_details(tag):
    ''' Generate details on a tag from the orcid and suporg collections
        Keyword arguments:
          tag: tsg to get details for
        Returns:
          HTML
    '''
    payload = {"managed": tag}
    try:
        mgmt = DB['dis'].orcid.count_documents(payload)
        if mgmt:
            mgmt = DB['dis'].orcid.find_one(payload)
    except Exception as err:
        raise err
    payload = {"affiliations": tag}
    try:
        acnt = DB['dis'].orcid.count_documents(payload)
    except Exception as err:
        raise err
    tagtype = "Affiliation" if acnt else ""
    try:
        orgs = DL.get_supervisory_orgs(DB['dis'].suporg)
    except Exception as err:
        raise err
    payload = [{"$match": {"jrc_tag.name": tag}},
               {"$unwind": "$jrc_tag"},
               {"$match": {"jrc_tag.name": tag}},
               {"$group": {"_id": "$jrc_tag.type", "count": {"$sum": 1}}},
               {"$sort": {"_id": 1}}
              ]
    try:
        rows = DB['dis'].dois.aggregate(payload)
    except Exception as err:
        raise err
    html = "<table id='tagprops' class='proplist'><thead></thead><tbody>"
    if mgmt:
        html += f"<tr><td>Managed by</td><td>{mgmt['given'][0]} {mgmt['family'][0]}</td></tr>"
    pdict = {}
    for row in rows:
        pdict[row['_id']] = row['count']
    if not pdict and not acnt:
        return ''
    parr = []
    pcnt = 0
    for key, val in pdict.items():
        parr.append(f"{key}: {val:,}")
        pcnt += val
    if tag in orgs:
        tagtype = 'Supervisory org'
        html += f"<tr><td>Tag type</td><td>{tagtype}</td></tr>"
        html += f"<tr><td>Code</td><td>{orgs[tag]['code']}</td></tr>"
        html += "<tr><td>Status</td><td>"
        if 'active' in orgs[tag]:
            html += "<span style='color: lime;'>Active</span></td></tr>"
        else:
            html += "<span style='color: yellow;'>Inactive</span></td></tr>"
    else:
        html += f"<tr><td>Tag type</td><td>{tagtype}</td></tr>"
    if acnt:
        html += f"<tr><td>Authors with affiliation</td><td>{acnt}</td></tr>"
    if pdict:
        html += f"<tr><td>Appears in DOI tags</td><td>{'<br>'.join(parr)}</td></tr>"
    html += "</tbody></table>"
    return html

# ******************************************************************************
# * General utility functions                                                  *
# ******************************************************************************

def add_subjects(row, html=None):
    ''' Add subjects to the HTML
        Keyword arguments:
          row: row from dois collection
          html: HTML to add subjects to
        Returns:
          HTML with subjects added
    '''
    if row['jrc_obtained_from'] == 'DataCite':
    # Subjects (DataCite categories)
        try:
            if row and row['jrc_obtained_from'] == 'DataCite' and 'subjects' in row \
               and row['subjects']:
                if html:
                    html += "<h4>DataCite subjects</h4>" \
                            + f"{', '.join(sub['subject'] for sub in row['subjects'])}"
                else:
                    return f"{', '.join(sub['subject'] for sub in row['subjects'])}"
        except Exception as err:
            raise err
    elif 'jrc_mesh' in row:
        # MeSH subjects (Crossref)
        subjects = []
        for mesh in row['jrc_mesh']:
            if 'descriptor_name' in mesh:
                if 'major_topic' in mesh and mesh['major_topic']:
                    subj = mesh['descriptor_name']
                else:
                    subj = f"<span style='color: #88a'>{mesh['descriptor_name']}</span>"
                if 'key' in mesh and mesh['key']:
                    subj = f"<a href='https://www.ncbi.nlm.nih.gov/mesh/{mesh['key']}' " \
                           + f"target='_blank'>{subj}</a>"
                subjects.append(subj)
        if subjects:
            if html:
                html += f"<h4>MeSH subjects</h4>{', '.join(subjects)}"
            else:
                return f"{', '.join(subjects)}"
    return html


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


def journal_buttons(show, prefix):
    ''' Generate journal display buttons
        Keyword arguments:
          show: display type
          prefix: navigation prefix
        Returns:
          Button HTML
    '''
    if show == 'journal':
        full = f"window.location.href='{prefix}/full'"
        html = '<div><button id="toggle-to-all" type="button" class="btn btn-success btn-tiny"' \
               + f'onclick="{full}">Show all resource types</button></div>'
    else:
        jour = f"window.location.href='{prefix}/journal'"
        html = '<div><button id="toggle-to-journal" type="button" ' \
               + 'class="btn btn-success btn-tiny"' \
               + f'onclick="{jour}">Show journals/preprints only</button></div>'
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
    # Do not display
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
        orgs = DL.get_supervisory_orgs(DB['dis'].suporg)
    except Exception as err:
        raise InvalidUsage("Could not get supervisory orgs: " + str(err), 500) from err
    if not result['rest']['authorized'] and 'jrc_author' in row:
        del row['jrc_author']
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
    # Do not display
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
    # Do not display
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
    if not result['rest']['authorized']:
        for fld in app.config['DO_NOT_DISPLAY']:
            if fld in rec:
                del rec[fld]
    result['data'] = rec
    result['rest']['source'] = 'mongo'
    result['rest']['row_count'] = len(result['data'])
    return generate_response(result)


@app.route('/doi/migrations/<string:idate>', methods=['GET'])
def show_doi_migrations(idate):
    '''
    Return migration records for DOIs inserted since a specified date
    Return migration records for DOIs inserted since a specified date.
    # Do not display
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
    payload = {"jrc_author": {"$exists": True},
               "jrc_inserted": {"$gte" : isodate},
               "subtype": {"$ne": "other"},
               "$or": [{"types.resourceTypeGeneral": "Preprint"},
                       {"type": {"$in": ["journal-article", "peer-review"]}}
                      ]
              }
    try:
        rows = DB['dis'].dois.find(payload, {'_id': 0})
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
            #if not result['rest']['authorized'] and 'jrc_author' in rec: #PLUG
            #    del rec['jrc_author']
            result['data'].append(rec)
        except Exception as err:
            raise InvalidUsage(str(err), 500) from err
    result['rest']['row_count'] = len(result['data'])
    return generate_response(result)


@app.route('/doi/published/<string:start>/<string:end>', methods=['GET'])
def show_published_dois(start, end):
    '''
    Return DOI records for DOIs by date range
    Return DOI records for DOIs with a publishing date in a specified date range.
    # Do not display
    tags:
      - DOI
    parameters:
      - in: path
        name: start
        schema:
          type: string
        required: true
        description: Earliest publishing date in ISO format (YYYY-MM-DD)
      - in: path
        name: end
        schema:
          type: string
        required: true
        description: Latest publishing date in ISO format (YYYY-MM-DD)
    responses:
      200:
        description: DOI data
      500:
        description: MongoDB error
    '''
    result = initialize_result()
    try:
        _ = datetime.strptime(start,'%Y-%m-%d')
        _ = datetime.strptime(end,'%Y-%m-%d')
    except Exception as err:
        raise InvalidUsage(str(err), 400) from err
    payload = {"jrc_publishing_date": {"$gte" : start, "$lte" : end}}
    try:
        rows = DB['dis'].dois.find(payload, {'_id': 0})
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    result['rest']['row_count'] = 0
    result['rest']['source'] = 'mongo'
    result['data'] = []
    for row in rows:
        try:
            rec = row
            if not result['rest']['authorized'] and 'jrc_author' in rec:
                del rec['jrc_author']
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
    try:
        result['rest']['source'], result['data'] = get_doi(doi)
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
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
    # Do not display
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
    # Do not display
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
    try:
        result['data'] = JRC.call_orcid(oid)
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    if 'error-code' not in result['data']:
        result['rest']['source'] = 'orcid'
        result['rest']['row_count'] = 1
    return generate_response(result)


@app.route('/orcidworks/<string:oid>')
def show_orcidworks(oid):
    '''
    Return works for an ORCID ID
    Return works information for an ORCID ID (using the ORCID API)
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
    try:
        _, dois, _ = get_orcid_from_db(oid)
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    janelia_dois = []
    last_janelia_doi = {"doi": None, "jrc_publishing_date": '0000-00-00'}
    if dois:
        for doi in dois:
            try:
                rec = DL.get_doi_record(doi, DB['dis'].dois)
            except Exception as err:
                raise InvalidUsage(str(err), 500) from err
            janelia_dois.append({"doi": doi, "jrc_publishing_date": rec['jrc_publishing_date']})
            if rec['jrc_publishing_date'] > last_janelia_doi['jrc_publishing_date']:
                last_janelia_doi = {"doi": doi, "jrc_publishing_date": rec['jrc_publishing_date']}
    result['janelia_dois'] = janelia_dois
    result['last_janelia_doi'] = last_janelia_doi
    try:
        data = JRC.call_orcid(oid)
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    result['orcid'] = data
    other = add_orcid_works(data, dois, return_html=False)
    result['other_dois'] = []
    for doi in other:
        result['other_dois'].append(doi)
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
@app.route('/<path:doi>')
def show_home(doi=None):
    ''' Home
    '''
    if doi and doi != 'home':
        return show_doi_ui(doi)
    jlist = get_top_journals('All').keys()
    journals = '<option>'
    journals += '</option><option>'.join(sorted(jlist))
    journals += '</option>'
    try:
        rows = DB['dis']['org_group'].find({}).collation({"locale": "en"}).sort("group", 1)
    except Exception as err:
        return render_template('warning.html', urlroot=request.url_root,
                                title=render_warning("Could not find organization names", 'error'),
                                message=error_message(err))
    olist = []
    for row in rows:
        olist.append(row['group'])
    orgs = '<option>'
    orgs += '</option><option>'.join(sorted(olist))
    orgs += '</option>'
    try:
        rows = DB['dis']['project_map'].distinct("project")
    except Exception as err:
        return render_template('warning.html', urlroot=request.url_root,
                                title=render_warning("Could not find projects", 'error'),
                                message=error_message(err))
    plist = []
    for row in rows:
        plist.append(row)
    projects = '<option>'
    projects += '</option><option>'.join(sorted(plist))
    projects += '</option>'
    endpoint_access()
    return make_response(render_template('home.html', urlroot=request.url_root,
                                         journals=journals, orgs=orgs, projects=projects,
                                         navbar=generate_navbar('Home')))


def get_display_badges(doi, row, data, local):
    ''' Get badges for a DOI, formatted for display
        Keyword arguments:
          doi: DOI
          row: row from dois collection
          data: data from Crossref/DataCite API
          local: True if the DOI is local, False otherwise
        Returns:
          Badges as a string
    '''
    badges = "<span class='paperdata'>"
    if local:
        if 'jrc_pmid' in row:
            plink = f"{app.config['PMID']}{row['jrc_pmid']}/"
            badges += f" {tiny_badge('primary', 'PMID', plink)}"
    if '/protocols.io.' in doi:
        badges += f" {tiny_badge('source', 'protocols.io', f'/raw/protocols.io/{doi}')}"
    rlink = f"/doi/{doi}"
    if local:
        jour = DL.get_journal(data)
        if jour:
            if 'bioRxiv' in jour:
                badges += f" {tiny_badge('source', 'bioRxiv', f'/raw/bioRxiv/{doi}')}"
        if '/janelia.' in doi:
            badges += f" {tiny_badge('source', 'figshare', f'/raw/figshare/{doi}')}"
        badges += f" {tiny_badge('source', row['jrc_obtained_from'], rlink)}"
    else:
        badges += f" {tiny_badge('source', 'Raw data', rlink)}"
    oresp = JRC.call_oa(doi)
    if oresp:
        olink = f"{app.config['OA']}{doi}"
        badges += f" {tiny_badge('source', 'OA data', olink)}"
    if local and 'jrc_fulltext_url' in row:
        badges += f" {tiny_badge('pdf', 'Full text', row['jrc_fulltext_url'])}"
    #badges += f" {tiny_badge('info', 'HQ migration', f'/doi/migration/{doi}')}"
    badges += "</span>"
    return badges

# ******************************************************************************
# * UI endpoints (DOI)                                                         *
# ******************************************************************************

@app.route('/doiui/<path:doi>')
def show_doi_ui(doi):
    ''' Show DOI
    '''
    # pylint: disable=too-many-return-statements
    doi = doi.lstrip('/').rstrip('/').lower()
    if doi.isdigit():
        pmid = doi.lstrip('/').rstrip('/').lower()
        try:
            row = DB['dis'].dois.find_one({"jrc_pmid": pmid})
        except Exception as err:
            return inspect_error(err, 'Could not get PMID')
        if not row:
            return render_template('warning.html', urlroot=request.url_root,
                                   title=render_warning("Could not find DOI", 'warning'),
                                   message=f"Could not find DOI from PMID {pmid}")
        doi = row['doi']
    try:
        row = DB['dis'].dois.find_one({"doi": doi})
    except Exception as err:
        return inspect_error(err, 'Could not get DOI')
    local = False
    recsec = html = ""
    if row:
        recsec += '<h5 style="color:lime">This DOI is saved locally in the Janelia database</h5>'
        recsec += add_update_times(row)
        recsec += add_jrc_fields(row)
        local = True
    else:
        recsec = '<h5 style="color:red">This DOI is not saved locally in the ' \
                 + 'Janelia database</h5><br>'
    try:
        _, data = get_doi(doi)
    except Exception as err:
        return render_template('warning.html', urlroot=request.url_root,
                                title=render_warning("Could not get DOI", 'error'),
                                message=str(err))
    if not data:
        return render_template('warning.html', urlroot=request.url_root,
                                title=render_warning("Could not find DOI", 'warning'),
                                message=f"Could not find DOI {doi}")
    authors = DL.get_author_list(data, orcid=True, project_map=DB['dis'].project_map)
    #if not authors:
    #    return render_template('error.html', urlroot=request.url_root,
    #                            title=render_warning("Could not generate author list"),
    #                            message=f"Could not generate author list for {doi}")
    title = DL.get_title(data)
    if not title:
        return render_template('error.html', urlroot=request.url_root,
                                title=render_warning("Could not find title"),
                                message=f"Could not find title for {doi}")
    journal = DL.get_journal(data)
    if not journal:
        journal = ''
        #return render_template('error.html', urlroot=request.url_root,
        #                        title=render_warning("Could not find journal"),
        #                        message=f"Could not find journal for {doi}")
    citationf = f"{authors} {title}."
    try:
        citations = DL.short_citation(doi, True)
    except Exception as err:
        citations = f"Could not generate short citation for {doi} ({err})"
    doisec = ""
    # Citations (DataCite, Dimensions, OpenAlex, S2, Web of Science)
    if row:
        tblrow = []
        # DataCite
        if row['jrc_obtained_from'] == 'DataCite' and 'citationCount' in row \
            and row['citationCount']:
            tblrow.append(f"<td>Dimensions: {row['citationCount']:,}</td>")
        # Dimensions
        try:
            citcnt = DL.get_citation_count(doi)
        except Exception as err:
            citcnt = 0
        if citcnt:
            tblrow.append(f"<td>Dimensions: {citcnt:,}</td>")
        # OpenAlex
        try:
            citcnt = DL.get_citation_count(doi, 'openalex')
        except Exception as err:
            citcnt = 0
        if citcnt:
            tblrow.append(f"<td>OpenAlex: {citcnt:,}</td>")
        # Semantic Scholar
        citcnt = s2_citation_count(doi, fmt='html')
        if citcnt:
            tblrow.append(f"<td>Semantic Scholar: {citcnt}</td>")
        # Web of Science
        citcnt = wos_citation_count(doi)
        if citcnt:
            tblrow.append(f"<td>Web of Science: {citcnt:,}</td>")
        if tblrow:
            doisec += "<table id='citations' class='citations'><thead>" \
                      + f"<tr><th colspan={len(tblrow)}>Citation counts</th></tr>" \
                      + "</thead><tbody>" + ''.join(tblrow) + "</tbody></table>"
        # DataCite downloads
        if row['jrc_obtained_from'] == 'DataCite':
            if 'downloadCount' in row and row['downloadCount']:
                doisec += f"<span class='paperdata'>Downloads: {row['downloadCount']:,}</span><br>"
    doisec += "<br>"
    # Citations
    citsec = cittype = ""
    if 'type' in data:
        cittype += data['type'].replace('-', ' ')
        if 'subtype' in data:
            cittype += f" {data['subtype'].replace('-', ' ')}"
    elif 'types' in data and 'resourceTypeGeneral' in data['types']:
        cittype += data['types']['resourceTypeGeneral']
    citsec += f"<div id='div-full' class='citation'>{citationf} {journal}.</div>"
    citsec += f"<div id='div-short' class='citation'>{citations}</div>"
    citsec += "<br>"
    # Abstract
    abstract = ""
    if 'type' in data and data['type'] == 'grant':
        if 'project' in data and data['project']:
            if all(['project-description' in data['project'][0],
                    data['project'][0]['project-description'],
                    'description' in data['project'][0]['project-description'][0]]):
                abstract = data['project'][0]['project-description'][0]['description']
                ptitle = ""
                if 'project-title' in data['project'][0] and data['project'][0]['project-title']:
                    ptitle = f" ({data['project'][0]['project-title'][0]['title']})"
                html += f"<h4>Grant{ptitle}</h4><div class='abstract'>{abstract}</div><br>"
    else:
        abstract = DL.get_abstract(data)
        if abstract:
            html += f"<h4>Abstract</h4><div class='abstract'>{abstract}</div><br>"
    if row:
        try:
            html = add_subjects(row, html)
            if 'span' in html:
                html += "<br><i class='fa-solid fa-circle-info'></i> Subjects in " \
                        + "<span style='color: #88a'>gray-blue</span> are considered minor in MeSH"
            html += "<br><br>"
        except Exception as err:
            return inspect_error(err, f"Could not get subjects for DOI {row['doi']}")
    # Relations
    html += add_relations(data)
    # Author details
    if row:
        try:
            authors = DL.get_author_details(row, DB['dis'].orcid)
        except Exception as err:
            return inspect_error(err, 'Could not get author list details')
        if authors:
            alist, count = show_tagged_authors(authors, row['jrc_author'] \
                if 'jrc_author' in row else [])
            if alist:
                html += f"<br><h4>Potential Janelia authors ({count})</h4>" \
                        + f"<div class='scroll'>{''.join(alist)}</div>"
    # Title
    doilink = f"<a href='https://doi.org/{doi}' target='_blank'>{doi}</a>"
    badges = get_display_badges(doi, row, data, local)
    doititle = f"{doilink} (PMID: {row['jrc_pmid']})" if row and 'jrc_pmid' in row else doilink
    doititle += badges
    endpoint_access()
    return make_response(render_template('doi.html', urlroot=request.url_root, pagetitle=doi,
                                         title=doititle, recsec=recsec, doisec=doisec,
                                         cittype=cittype, citsec=citsec, html=html,
                                         navbar=generate_navbar('DOIs')))


@app.route('/pmidui/<string:pmid>')
def show_pmid_ui(pmid):
    ''' Show PMID
    '''
    pmid = pmid.lstrip('/').rstrip('/').lower()
    try:
        row = DB['dis'].dois.find_one({"jrc_pmid": pmid})
    except Exception as err:
        return inspect_error(err, 'Could not get DOI')
    if not row:
        return render_template('warning.html', urlroot=request.url_root,
                               title=render_warning("Could not find PMID", 'warning'),
                               message=f"Could not find PMID {pmid}")
    return redirect(f"/doiui/{row['doi']}")


@app.route('/doisui_name/<string:family>')
@app.route('/doisui_name/<string:family>/<string:given>')
def show_doi_by_name_ui(family, given=None):
    ''' Show DOIs for a family name
    '''
    if given:
        payload = single_name_search_payload(given, family)
        print(payload)
    else:
        payload = {'$or': [{"author.family": {"$regex": f"^{family}$", "$options" : "i"}},
                           {"creators.familyName": {"$regex": f"^{family}$", "$options" : "i"}},
                           {"creators.name": {"$regex": f"^{family}$", "$options" : "i"}},
                          ]}
    try:
        coll = DB['dis'].dois
        rows = coll.find(payload).collation({"locale": "en"}).sort("jrc_publishing_date", -1)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get DOIs from dois collection"),
                               message=error_message(err))
    html, _ = generate_works_table(rows, family)
    if not html:
        return render_template('warning.html', urlroot=request.url_root,
                               title=render_warning("Could not find DOIs", 'warning'),
                               message="Could not find any DOIs with author name matching " \
                                       + f"{family} {given}")
    name = f"{given} {family}" if given else family
    endpoint_access()
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
        coll = DB['dis'].dois
        rows = coll.find(payload).collation({"locale": "en"}).sort("jrc_publishing_date", -1)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get DOIs from dois collection"),
                               message=error_message(err))
    html, _ = standard_doi_table(rows)
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
    endpoint_access()
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
        if isinstance(row['title'], str):
            row['title'] = [row['title']]
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
    html, _ = standard_doi_table(union)
    if not html:
        return render_template('warning.html', urlroot=request.url_root,
                               title=render_warning("Could not find DOIs", 'warning'),
                               message=f"Could not find any DOIs with title matching {title}")
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=f"DOIs for {title}", html=html,
                                         navbar=generate_navbar('DOIs')))


@app.route('/dois_source/<string:year>')
@app.route('/dois_source')
def dois_source(year='All'):
    ''' Show data sources and other statistics
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
    total = 0
    for key, val in sorted(hdict.items(), key=itemgetter(1), reverse=True):
        src, typ, sub = key.split('_')
        if not sub:
            sub = 'None'
        total += val
        if year == 'All':
            val = f"<a href='/doisui_type/{src}/{typ}/{sub}'>{val}</a>"
        else:
            val = f"<a href='/doisui_type/{src}/{typ}/{sub}/{year}'>{val}</a>"
        html += f"<tr><td>{src}</td><td>{typ}</td><td>{sub if sub != 'None' else ''}</td>" \
                + f"<td>{val}</td></tr>"
    html += f"</tbody><tfoot><tr><td colspan='3'>TOTAL</td><td>{total:,}</td>" \
            + "</tr></tfoot></table><br>"
    html += year_pulldown('dois_source')
    title = "DOIs by source"
    if year != 'All':
        title += f" ({year})"
    chartscript, chartdiv = DP.pie_chart(data, title, "source", width=500,
                                         colors=DP.SOURCE_PALETTE)
    if year == 'All' or year >= '2024':
        payload = [{"$group": {"_id": "$jrc_load_source", "count": {"$sum": 1}}},
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
    # DOIs with PMIDs
    data = {}
    payload = {"jrc_obtained_from": "Crossref",
               "jrc_pmid": {"$exists": True}}
    if year != 'All':
        payload["jrc_publishing_date"] = {"$regex": "^"+ year}
    try:
        total = DB['dis'].dois.count_documents({"jrc_obtained_from": "Crossref"})
        cnt = DB['dis'].dois.count_documents(payload)
        data['PMIDs'] = cnt
        data['No PMIDs'] = total - cnt
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                                title=render_warning("Could not get load methods " \
                                                     + "from dois collection"),
                                message=error_message(err))
    title = "Crossref DOIs with PMIDs"
    if year != 'All':
        title += f" ({year})"
    chartscript2, chartdiv2 = DP.pie_chart(data, title, "source", width=500,
                                           colors=DP.SOURCE_PALETTE)
    # DOIs with PMIDs
    data = {}
    payload = {"jrc_obtained_from": "Crossref",
               "jrc_fulltext_url": {"$exists": True}}
    if year != 'All':
        payload["jrc_publishing_date"] = {"$regex": "^"+ year}
    try:
        cnt = DB['dis'].dois.count_documents(payload)
        data['Available'] = cnt
        data['Not available'] = total - cnt
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                                title=render_warning("Could not get load methods " \
                                                     + "from dois collection"),
                                message=error_message(err))
    title = "Crossref DOIs with full text available"
    if year != 'All':
        title += f" ({year})"
    script2, div2 = DP.pie_chart(data, title, "source", width=500,
                                 colors=DP.SOURCE_PALETTE)
    chartscript2 += script2
    chartdiv2 += div2
    title = "DOI sources"
    if year != 'All':
        title += f" ({year})"
    endpoint_access()
    return make_response(render_template('bokeh.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         chartscript=chartscript, chartdiv=chartdiv,
                                         chartscript2=chartscript2, chartdiv2=chartdiv2,
                                         navbar=generate_navbar('DOIs')))


@app.route('/dois_report/<string:year>')
@app.route('/dois_report')
def dois_report(year=str(datetime.now().year)):
    ''' Show year in review
    '''
    pmap = {"journal-article": "Journal articles", "posted-content": "Posted content",
            "preprints": "Preprints", "proceedings-article": "Proceedings articles",
            "book-chapter": "Book chapters", "datasets": "Datasets",
            "peer-review": "Peer reviews", "grant": "Grants", "other": "Other"}
    payload = [{"$match": {"jrc_publishing_date": {"$regex": "^"+ year}}},
               {"$group": {"_id": {"type": "$type", "subtype": "$subtype",
                                   "DataCite": "$types.resourceTypeGeneral"}, "count": {"$sum": 1}}}
              ]
    coll = DB['dis'].dois
    try:
        rows = coll.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get yearly metrics " \
                                                    + "from dois collection"),
                               message=error_message(err))
    typed = counts_by_type(rows)
    first, last, anyauth = get_first_last_authors(year)
    stat = {}
    sheet = []
    # Journal count
    payload = [{"$unwind" : "$container-title"},
               {"$match": {"container-title": {"$exists": True}, "type": "journal-article",
                           "jrc_publishing_date": {"$regex": "^"+ year}}},
               {"$group": {"_id": "$container-title", "count":{"$sum": 1}}}
              ]
    try:
        rows = coll.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get journal metrics " \
                                                    + "from dois collection"),
                               message=error_message(err))
    cnt = 0
    for row in rows:
        if row['_id']:
            cnt += 1
    typed['Crossref'] = 0
    for key, val in pmap.items():
        if key in typed:
            if key not in ('DataCite', 'preprints'):
                typed['Crossref'] += typed[key]
            additional = []
            if key in first:
                additional.append(f"{first[key]:,} with Janelian first author")
            if key in last:
                additional.append(f"{last[key]:,} with Janelian last author")
            if key in anyauth:
                additional.append(f"{anyauth[key]:,} with any Janelian author")
            additional = f" ({', '.join(additional)})" if additional else ""
            stat[val] = f"<span style='font-weight: bold'>{typed[key]:,}</span> {val.lower()}"
            if val in ('Journal articles', 'Preprints'):
                sheet.append(f"{val}\t{typed[key]}")
                if val == 'Journal articles':
                    stat[val] += f" in <span style='font-weight: bold'>{cnt:,}</span> journals"
                    sheet.append(f"\tJournals\t{cnt}")
                if key in first:
                    sheet.append(f"\tFirst authors\t{first[key]}")
                if key in last:
                    sheet.append(f"\tLast authors\t{last[key]}")
                if key in anyauth:
                    sheet.append(f"\tAny Janelian author\t{anyauth[key]:}")
            stat[val] += additional
            stat[val] += "<br>"
    # figshare (unversioned only)
    payload = [{"$match": {"doi": {"$regex": "janelia.[0-9]+$"},
                          "jrc_publishing_date": {"$regex": "^"+ year}}},
               {"$unwind": "$jrc_author"},
               {"$group": {"_id": "$jrc_author", "count": {"$sum": 1}}}]
    try:
        cnt = coll.count_documents(payload[0]['$match'])
        stat['figshare'] = f"<span style='font-weight: bold'>{cnt:,}</span> " \
                           + "figshare (unversioned) articles"
        sheet.append(f"figshare (unversioned) articles\t{cnt}")
        rows = coll.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get journal figshare stats"),
                               message=error_message(err))
    if cnt:
        cnt = 0
        for row in rows:
            cnt += 1
        stat['figshare'] += f" with <span style='font-weight: bold'>{cnt:,}</span> " \
                            + "Janelia authors<br>"
        sheet.append(f"\tJanelia authors\t{cnt}")
    # ORCID stats
    orcs = {}
    try:
        ocoll = DB['dis'].orcid
        rows = ocoll.find({})
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get orcid collection entries"),
                               message=error_message(err))
    for row in rows:
        if 'employeeId' in row and 'orcid' in row:
            orcs[row['employeeId']] = True
    payload = [{"$match": {"jrc_publishing_date": {"$regex": "^"+ year}}},
               {"$unwind": "$jrc_author"},
               {"$group": {"_id": "$jrc_author", "count": {"$sum": 1}}}
              ]
    try:
        rows = coll.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get jrc_author"),
                               message=error_message(err))
    cnt = orc = 0
    for row in rows:
        cnt += 1
        if row['_id'] in orcs:
            orc += 1
    stat['ORCID'] = f"<span style='font-weight: bold'>{cnt:,}</span> " \
                    + "distinct Janelia authors for all entries, " \
                    + f"<span style='font-weight: bold'>{orc:,}</span> " \
                    + f"({orc/cnt*100:.2f}%) with ORCIDs"
    sheet.extend([f"Distinct Janelia authors\t{cnt}", f"Janelia authors with ORCIDs\t{orc}"])
    # Entries
    if 'DataCite' not in typed:
        typed['DataCite'] = 0
    for key in ('DataCite', 'Crossref'):
        sheet.insert(0, f"{key} entries\t{typed[key]}")
    stat['Entries'] = f"<span style='font-weight: bold'>{typed['Crossref']:,}" \
                      + "</span> Crossref entries<br>" \
                      + f"<span style='font-weight: bold'>{typed['DataCite']:,}" \
                      + "</span> DataCite entries"
    if 'Journal articles' not in stat:
        stat['Journal articles'] = "<span style='font-weight: bold'>0</span> journal articles<br>"
    if 'Preprints' not in stat:
        stat['Preprints'] = "<span style='font-weight: bold'>0</span> preprints<br>"
    # Authors
    try:
        rows = coll.find({"jrc_publishing_date": {"$regex": "^"+ year}})
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get frc_author metrics " \
                                                    + "from dois collection"),
                               message=error_message(err))
    total = cnt = middle = 0
    for row in rows:
        total += 1
        field = 'creators' if 'creators' in row else 'author'
        if 'jrc_author' in row and len(row['jrc_author']) == len(row[field]):
            cnt += 1
        elif 'jrc_author' not in row:
            middle += 1
    stat['Author'] = f"<span style='font-weight: bold'>{cnt:,}</span> " \
                     + "entries with all Janelia authors<br>"
    stat['Author'] += f"<span style='font-weight: bold'>{total-cnt:,}</span> " \
                      + "entries with at least one external collaborator<br>"
    stat['Author'] += f"<span style='font-weight: bold'>{middle:,}</span> " \
                      + "entries with no Janelia first or last authors<br>"
    sheet.append(f"Entries with all Janelia authors\t{cnt}")
    sheet.append(f"Entries with external collaborators\t{total-cnt}")
    sheet.append(f"Entries with no Janelia first or last authors\t{middle}")
    # Preprints
    no_relation = get_no_relation(year)
    cnt = {'journal': 0, 'preprint': 0}
    for atype in ['journal', 'preprint']:
        for src in ['Crossref', 'DataCite']:
            if src in no_relation and atype in no_relation[src]:
                cnt[atype] += no_relation[src][atype]
    stat['Preprints'] += f"<span style='font-weight: bold'>{cnt['journal']:,}" \
                         + "</span> journal articles without preprints<br>"
    stat['Preprints'] += f"<span style='font-weight: bold'>{cnt['preprint']:,}" \
                         + "</span> preprints without journal articles<br>"
    # Journals
    journal = get_top_journals(year)
    cnt = 0
    stat['Topjournals'] = ""
    sheet.append("Top journals")
    for key in sorted(journal, key=journal.get, reverse=True):
        stat['Topjournals'] += f"&nbsp;&nbsp;&nbsp;&nbsp;{key}: {journal[key]}<br>"
        sheet.append(f"\t{key}\t{journal[key]}")
        cnt += 1
        if cnt >= 10:
            break
    # Tags
    payload = [{"$match": {"jrc_tag": {"$exists": True}, "jrc_obtained_from": "Crossref",
                           "jrc_publishing_date": {"$regex": "^"+ year}}},
               {"$project": {"doi": 1, "type": "$type", "numtags": {"$size": "$jrc_tag"}}}
              ]
    try:
        rows = coll.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get frc_author metrics " \
                                                    + "from dois collection"),
                               message=error_message(err))
    cnt = total = 0
    for row in rows:
        if 'type' not in row or row['type'] not in ('journal-article', 'posted-content'):
            continue
        cnt += 1
        total += row['numtags']
    stat['Tags'] = f"<span style='font-weight: bold'>{total/cnt:.1f}</span> " \
                   + "average tags per tagged entry"
    sheet.append(f"Average tags per tagged entry\t{total/cnt:.1f}")
    sheet = create_downloadable(f"{year}_in_review", None, "\n".join(sheet))
    html = f"<h2 class='dark'>Entries</h2>{stat['Entries']}<br>" \
           + f"<h2 class='dark'>Articles</h2>{stat['Journal articles']}" \
           + f"{stat['figshare']}" \
           + f"<h2 class='dark'>Preprints</h2>{stat['Preprints']}" \
           + f"<h2 class='dark'>Authors</h2>{stat['Author']}" \
           + f"{stat['figshare']}{stat['ORCID']}" \
           + f"<h2 class='dark'>Tags</h2>{stat['Tags']}" \
           + "<h2 class='dark'>Top journals</h2>" \
           + f"<p style='font-size: 14pt;line-height:90%;'>{stat['Topjournals']}</p>"
    html = f"<div class='titlestat'>{year} YEAR IN REVIEW</div>{sheet}<br>" \
           + f"<div class='yearstat'>{html}</div>"
    html += '<br>' + year_pulldown('dois_report', all_years=False)
    endpoint_access()
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
    endpoint_access()
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
        typ = subtype = ""
        if 'type' in row:
            typ = row['type']
            if 'subtype' in row:
                subtype = row['subtype']
                typ += f" {subtype}"
        elif 'types' in row and 'resourceTypeGeneral' in row['types']:
            typ = row['types']['resourceTypeGeneral']
        version = []
        if 'relation' in row and 'is-version-of' in row['relation']:
            for ver in row['relation']['is-version-of']:
                if ver['id-type'] == 'doi' and ver['id'] not in version:
                    version.append(ver['id'])
        version = doi_link(version) if version else ""
        news = row['jrc_newsletter'] if 'jrc_newsletter' in row else ""
        if (not news) and (row['jrc_obtained_from'] == 'Crossref') and \
           (row['jrc_publishing_date'] >= str(limit)) \
           and (typ == 'journal-article' or subtype == 'preprint'):
            rclass = 'candidate'
        else:
            rclass = 'other'
        jpd = row['jrc_publishing_date'] if row['jrc_publishing_date'] >= str(limit) else \
              f"<span style='color: gray'>{row['jrc_publishing_date']}</span>"
        html += f"<tr class='{rclass}'><td>" \
                + "</td><td>".join([doi_link(row['doi']), row['jrc_obtained_from'], typ,
                                    jpd, source, str(row['jrc_inserted']), version,
                                    news]) + "</td></tr>"
        frow = "\t".join([row['doi'], row['jrc_obtained_from'], typ, row['jrc_publishing_date'],
                          source, str(row['jrc_inserted']), version, news])
        fileoutput += f"{frow}\n"
    html += '</tbody></table>'
    cbutton = "<button class=\"btn btn-outline-warning\" " \
              + "onclick=\"$('.other').toggle();\">Filter for candidate DOIs</button>"
    html = create_downloadable("jrc_inserted", None, fileoutput) + f" &nbsp;{cbutton}{html}"
    endpoint_access()
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
        display_value = '' if ipd['value'] == "!EXISTS!" else ipd['value']
        payload, ptitle = get_custom_payload(ipd, display_value)
    else:
        payload = ipd['query']
        ipd['field'] = "_".join(list(payload.keys()))
        ptitle = ''
    try:
        cnt = DB['dis'].dois.count_documents(payload)
        print(f"Custom payload: {payload}     Results: {cnt}")
        rows = DB['dis'].dois.find(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get DOIs"),
                               message=error_message(err))
    if not rows:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("DOIs not found"),
                               message=f"No DOIs were found for {ipd['field']}={display_value}")
    header = ['Published', 'DOI', 'Title', 'Newsletter']
    html = "<table id='dois' class='tablesorter standard'><thead><tr>" \
           + ''.join([f"<th>{itm}</th>" for itm in header]) + "</tr></thead><tbody>"
    works = []
    jorp = newsletter = 0
    for row in rows:
        published = DL.get_publishing_date(row)
        title = DL.get_title(row)
        if not title:
            title = ""
        works.append({"published": published, "link": doi_link(row['doi']), "title": title,
                      "doi": row['doi'], \
                      "newsletter": row['jrc_newsletter'] if 'jrc_newsletter' in row else ""})
        if 'jrc_newsletter' in row and row['jrc_newsletter']:
            newsletter += 1
        if DL.is_journal(row) or DL.is_preprint(row):
            jorp += 1
    fileoutput = ""
    for row in sorted(works, key=lambda row: row['published'], reverse=True):
        html += "<tr><td>" + dloop(row, ['published', 'link', 'title', 'newsletter'], "</td><td>") \
            + "</td></tr>"
        row['title'] = row['title'].replace("\n", " ")
        fileoutput += dloop(row, ['published', 'doi', 'title', 'newsletter']) + "\n"
    html += '</tbody></table>'
    html = create_downloadable(ipd['field'], header, fileoutput) + html
    html = f"DOIs: {len(works):,}<br>Journals/preprints: {jorp:,}<br>" \
           + f"DOIs in newsletter: {newsletter:,}<br>" + html
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=ptitle, html=html,
                                         navbar=generate_navbar('DOIs')))


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
    endpoint_access()
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
                               title=render_warning("No DOIs found", 'info'),
                               message="No DOIs are awaiting processing. This isn't an error," \
                                       + " it just means that we're all caught up on " \
                                       + "DOI processing.")
    for row in rows:
        elapsed = datetime. now() - row['inserted']
        if elapsed.days:
            etime = f"{elapsed.days} day{'s' if elapsed.days > 1 else ''}, " \
                    + f"{elapsed.seconds // 3600:02}:{elapsed.seconds // 60 % 60:02}:" \
                    + f"{elapsed.seconds % 60:02}"
        else:
            etime = f"{elapsed.seconds // 3600:02}:{elapsed.seconds // 60 % 60:02}:" \
                    + f"{elapsed.seconds % 60:02}"
        url = f"<a href='{row['url']}' target='_blank'>{row['doi']}</a>" \
              if 'url' in row and row['url'] else doi_link(row['doi'])
        html += f"<tr><td>{url}</td><td>{row['inserted']}</td><td>{etime}</td>"
    html += '</tbody></table>'
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title="DOIs awaiting processing", html=html,
                                         navbar=generate_navbar('DOIs')))

@app.route('/dois_publisher/<string:year>')
@app.route('/dois_publisher')
def dois_publisher(year='All'):
    ''' Show publishers with counts
    '''
    if year == 'All':
        match = {}
    else:
        match = {"jrc_publishing_date": {"$regex": "^"+ year}}
    payload = [{"$match": match},
               {"$group": {"_id": {"publisher": "$publisher", "source": "$jrc_obtained_from"},
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
    html = year_pulldown('dois_publisher') + html
    title = "DOI publishers"
    if year != 'All':
        title += f" for {year}"
    title += f" ({len(pubs):,})"
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         navbar=generate_navbar('DOIs')))


@app.route('/dois_subjectpicker')
def show_doi_subjectpicker():
    ''' Show DOI subjects
    '''
    try:
        payload = [{"$match": {"subjects": {"$exists": True}}},
                   {"$unwind": "$subjects"},
                   {"$group": {"_id": {"subject": "$subjects.subject",
                                       "scheme": "$subjects.subjectScheme"},
                               "count": {"$sum": 1}}},
                   {"$sort": {"count": -1}}]
        rows = DB['dis'].dois.aggregate(payload)
    except Exception as err:
        return inspect_error(err, 'Could not get Crossref DOI subjects')
    if not rows:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not find subjects", 'warning'),
                               message="Could not find Crossref DOI subjects")
    subdict = {}
    for row in rows:
        if 'scheme' not in row['_id']:
            row['_id']['scheme'] = "DataCite unspecified"
        if row['_id']['subject'] not in subdict:
            subdict[row['_id']['subject']] = [{"count": row['count'],
                                               "schema": row['_id']['scheme']}]
        else:
            subdict[row['_id']['subject']].append({"count": row['count'],
                                                   "schema": row['_id']['scheme']})
    try:
        payload = [{"$match": {"jrc_mesh": {"$exists": 1}}},
                   {"$unwind": "$jrc_mesh"},
                   {"$group": {"_id": "$jrc_mesh.descriptor_name", "count": {"$sum": 1}}}]
        rows = DB['dis'].dois.aggregate(payload)
    except Exception as err:
        return inspect_error(err, 'Could not get DataCite DOI subjects')
    if not rows:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not find subjects", 'warning'),
                               message="Could not find DataCite DOI subjects")
    for row in rows:
        if row['_id'] not in subdict:
            subdict[row['_id']] = [{"count": row['count'], "schema": "MeSH"}]
        else:
            subdict[row['_id']].append({"count": row['count'], "schema": "MeSH"})
    sublist = '<option>'
    outlist = ""
    for subj, val in sorted(subdict.items()):
        outlist += f"{subj}\t"
        schlist = []
        for sch in val:
            schlist.append(f"{sch['schema']}: {sch['count']}")
        outlist += ", ".join(schlist) + "\n"
        sublist += f"<option value='{subj}'>{subj}</option>"
    sublist += '</option>'
    html = f"Found {len(subdict):,} unique subjects<br>" \
           + create_downloadable("subjects", ["Subject", "Schemas"], outlist) + "<br><br>"
    endpoint_access()
    return make_response(render_template('subject.html', urlroot=request.url_root,
                                         title="DOI subjects", html=html, subjects=sublist,
                                         navbar=generate_navbar('DOIs')))


@app.route('/dois_subject/<string:subject>/<string:partial>')
@app.route('/dois_subject/<string:subject>')
def show_doi_subject(subject, partial=None):
    ''' Show DOIs for a subject
    '''
    if partial:
        payload = {"$or": [{"subjects.subject": {"$regex": subject, "$options": "i"}},
                           {"jrc_mesh.descriptor_name": {"$regex": subject, "$options": "i"}}]}
    else:
        payload = {"$or": [{"subjects.subject": subject},
                           {"jrc_mesh.descriptor_name": subject}]}
    try:
        rows = DB['dis'].dois.find(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get DOI subjects"),
                               message=error_message(err))
    header = ['Published', 'DOI', 'Source', 'Title']
    if partial:
        header.insert(-1, "Subjects")
    html = "<table id='dois' class='tablesorter standard'><thead><tr>" \
           + ''.join([f"<th>{itm}</th>" for itm in header]) + "</tr></thead><tbody>"
    fileoutput = ""
    cnt = 0
    crossref = False
    for row in rows:
        row['published'] = DL.get_publishing_date(row)
        row['link'] = doi_link(row['doi'])
        row['title'] = DL.get_title(row)
        row['source'] = row['jrc_obtained_from'] if 'jrc_obtained_from' in row else 'DataCite'
        if row['source'] == 'Crossref':
            crossref = True
        html += "<tr><td>" \
            + dloop(row, ['published', 'link', 'source'], "</td><td>") + "</td>"
        if partial:
            subj = add_subjects(row)
            html += f"<td>{subj}</td>"
        html += f"<td>{row['title']}</td></tr>"
        if row['title']:
            row['title'] = row['title'].replace("\n", " ")
        cnt += 1
        fileoutput += dloop(row, ['published', 'doi', 'source', 'title']) + "\n"
    html += '</tbody></table>'
    counter = f"<p>Number of DOIs: <span id='totalrows'>{cnt:,}</span></p>"
    if partial and crossref:
        counter += "<p><i class='fa-solid fa-circle-info'></i> Subjects in " \
                   + "<span style='color: #777'>dark gray</span> are considered minor in MeSH</p>"
    html = counter + html
    title = f"DOIs with partial subject {subject}" if partial else f"DOIs for subject {subject}"
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         navbar=generate_navbar('DOIs')))

# ******************************************************************************
# * UI endpoints (DataCite)                                                    *
# ******************************************************************************

@app.route('/datacite_subject/<string:subject>/<string:year>')
@app.route('/datacite_subject/<string:subject>')
@app.route('/datacite_subject')
def datacite_subject(subject=None, year='All'):
    ''' Show DOI subjects
    '''
    if subject:
        payload = {"subjects.subject": subject}
    else:
        payload = [{"$match": {"subjects": {"$exists": True}}},
                   {"$unwind": "$subjects"},
                   {"$group": {"_id": {"subject": "$subjects.subject",
                                       "scheme": "$subjects.subjectScheme"},
                               "count": {"$sum": 1}}},
                   {"$sort": {"count": -1}}]
    try:
        if subject:
            if year != 'All':
                payload['jrc_publishing_date'] = {"$regex": "^"+ year}
            rows = DB['dis'].dois.find(payload)
        else:
            rows = DB['dis'].dois.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get DOI subjects"),
                               message=error_message(err))
    if subject:
        html, _ = standard_doi_table(rows, prefix=f"datacite_subject/{subject}")
        title = f"DOIs for {subject}"
        if year != 'All':
            title += f" (year={year})"
    else:
        html = "<table id='subjects' class='tablesorter numberlast'><thead><tr>" \
               + "<th>Subject</th><th>Scheme</th><th>Count</th></tr></thead><tbody>"
        for row in rows:
            scheme = row['_id']['scheme'] if 'scheme' in row['_id'] else ''
            html += f"<tr><td>{row['_id']['subject']}</td><td>{scheme}</td><td>" \
                    + f"<a href='/datacite_subject/{row['_id']['subject']}'>{row['count']}</a>" \
                    + "</td></tr>"
        html += "</tbody></table>"
        title = "Subjects"
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         navbar=generate_navbar('DOIs')))


@app.route('/datacite_dois')
def datacite_dois():
    ''' Show DataCite DOIs
    '''
    payload = [{"$match": {"jrc_obtained_from": "DataCite",
                           "types.resourceTypeGeneral": {"$nin": ["Preprint"]}}},
               {"$group": {"_id": {"type": "$types.resourceTypeGeneral",
                                   "detail": "$types.resourceType",
                                   "pub": "$publisher"}, "count": {"$sum": 1}}},
               {"$sort": {"count": -1}},
               {"$unionWith": {
                   "coll": "dois",
                   "pipeline": [
                       {"$match": {"jrc_obtained_from": "Crossref",
                                   "doi": {"$regex": "/protocols.io"}}},
                       {"$group": {"_id": {"type": "$subtype", "pub": "protocols.io"},
                                   "count": {"$sum": 1}}},
                       {"$sort": {"count": -1}}
                   ]
               }}
              ]
    coll = DB['dis'].dois
    try:
        rows = coll.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get data DOIs"),
                               message=error_message(err))
    types = {}
    dois = []
    for row in rows:
        if row['_id']['type'] not in types:
            types[row['_id']['type']] = 0
        types[row['_id']['type']] += row['count']
        if 'detail' not in row['_id']:
            row['_id']['detail'] = ""
        dois.append(row)
    # Summary
    inner = '<table id="types" class="tablesorter numberlast"><thead><tr>' \
            + '<th>Type</th><th>Count</th>' \
            + '</tr></thead><tbody>'
    for key, val in sorted(types.items(), key=itemgetter(1), reverse=True):
        link = f"/doisui_type/DataCite/{key}/None"
        inner += f"<td>{key}</td><td><a href='{link}'>{val}</a></td></tr>"
    inner += "</tbody><tfoot></tfoot></table>"
    html = f"<div class='flexrow'><div class='flexcol'>{inner}</div>" \
           + "<div class='flexcol' style='margin-left: 50px'>"
    # Details
    inner = '<table id="details" class="tablesorter numberlast"><thead><tr>' \
            + '<th>Type</th><th>Subtype</th><th>Publisher</th><th>Count</th>' \
            + '</tr></thead><tbody>'
    total = 0
    for row in sorted(dois, key=lambda x: x['count'], reverse=True):
        total += row['count']
        link = f"/datacite_dois/{row['_id']['type']}/{row['_id']['pub']}"
        inner += f"<td>{row['_id']['type']}</td><td>{row['_id']['detail']}</td>" \
                 + f"<td>{row['_id']['pub']}</td>" \
                 + f"<td><a href='{link}'>{row['count']}</a></td></tr>"
    inner += "</tbody><tfoot><tr><td colspan='3'>TOTAL</td>" \
             + f"<td>{total:,}</td></tr></tfoot></table>"
    html += f"{inner}</div></div>"
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title="DataCite DOI stats", html=html,
                                         navbar=generate_navbar('DOIs')))


@app.route('/datacite_dois/<string:dtype>/<string:pub>/<string:year>')
@app.route('/datacite_dois/<string:dtype>/<string:pub>')
def datacite_doisd(dtype=None, pub=None, year='All'):
    ''' Show data DOIs
    '''
    if pub == 'protocols.io':
        payload = {"jrc_obtained_from": "Crossref",
                   "doi": {"$regex": "/protocols.io"}}
    else:
        payload = {"jrc_obtained_from": "DataCite",
                   "types.resourceTypeGeneral": dtype, "publisher": pub}
    if year != 'All':
        payload['jrc_publishing_date'] = {"$regex": "^"+ year}
    coll = DB['dis'].dois
    try:
        rows = coll.find(payload).sort("jrc_publishing_date", -1)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get data DOIs"),
                               message=error_message(err))
    html, cnt = standard_doi_table(rows, prefix=f"datacite_dois/{dtype}/{pub}")
    title = f"DOIs for {pub} {dtype} ({cnt:,})"
    if year != 'All':
        title += f" (year={year})"
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         navbar=generate_navbar('DOIs')))


@app.route('/datacite_downloads')
def datacite_downloads():
    ''' Show DataCite DOI download counts
    '''
    payload = {"jrc_obtained_from": "DataCite", "downloadCount": {"$ne": 0}}
    coll = DB['dis'].dois
    try:
        rows = coll.find(payload).sort("downloadCount", -1)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get data DOIs"),
                               message=error_message(err))
    html = '<table id="data" class="tablesorter numberlast"><thead><tr>' \
           + '<th>DOI</th><th>Title</th><th>Downloads</th>' \
           + '</tr></thead><tbody>'
    total = 0
    for row in rows:
        total += row['downloadCount']
        link = doi_link(row['doi'])
        print(row['doi'])
        html += f"<td>{link}</td><td>{DL.get_title(row)}</td>" \
                + f"<td>{row['downloadCount']}</td></tr>"
    html += "</tbody><tfoot><tr><td colspan='2' style='text-align:right'>TOTAL</td>" \
            + f"<td>{total:,}</td></tr></tfoot></table><br>"
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title="DataCite DOI downloads", html=html,
                                         navbar=generate_navbar('DOIs')))

# ******************************************************************************
# * UI endpoints (Authorship)                                                  *
# ******************************************************************************

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
    endpoint_access()
    return make_response(render_template('bokeh.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         chartscript=chartscript, chartdiv=chartdiv,
                                         navbar=generate_navbar('Authorship')))


@app.route('/dois_top_author/<string:year>')
@app.route('/dois_top_author')
def dois_top_author(year='All'):
    ''' Show top first and last authors
    '''
    rows = get_top_authors('first', year)
    first = "<h2>Top first authors</h2><table id='topauthors' class='tablesorter numbers'>" \
            + "<thead></thead><tbody><tr><th>Author</th><th>DOIs</th></tr>"
    for row in rows:
        first += f"<tr><td>{row['_id']}</td><td>{row['count']}</td></tr>"
    first += "</tbody></table>"
    rows = get_top_authors('last', year)
    last = "<h2>Top last authors</h2><table id='topauthors' class='tablesorter numbers'>" \
           + "<thead></thead><tbody><tr><th>Author</th><th>DOIs</th></tr>"
    for row in rows:
        last += f"<tr><td>{row['_id']}</td><td>{row['count']}</td></tr>"
    last += "</tbody></table>"
    html = "<div class='flexrow'><div class='flexcol'>" + first + "</div>" \
           + "<div class='flexcol' style='margin-left: 40px;'>" + last + "</div></div>" \
           + "<br>" + year_pulldown('dois_top_author')
    title = "Top first authors"
    if year != 'All':
        title += f" ({year})"
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         html=html,
                                         navbar=generate_navbar('Authorship')))


@app.route('/doiui_group/<string:year>/<string:which>')
@app.route('/doiui_group/<string:year>')
@app.route('/doiui_group')
def doiui_group(year='All', which=None):
    ''' Show group leader first/last authorship
    '''
    # Get lab head employee IDs
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
    # Get first authors
    payload = {"jrc_first_id": {"$in": leads}}
    if year != 'All':
        payload['jrc_publishing_date'] = {"$regex": "^"+ year}
    cnt = {}
    try:
        if which == 'first':
            display_rows = DB['dis'].dois.find(payload)
        else:
            cnt['first'] = DB['dis'].dois.count_documents(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get first authors " \
                                                    + "from dois collection"),
                               message=error_message(err))
    # Get last authors
    payload = {"jrc_last_id": {"$in": leads}}
    if year != 'All':
        payload['jrc_publishing_date'] = {"$regex": "^"+ year}
    display_rows = []
    try:
        if which == 'last':
            display_rows = DB['dis'].dois.find(payload)
        else:
            cnt['last'] = DB['dis'].dois.count_documents(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get last authors " \
                                                    + "from dois collection"),
                               message=error_message(err))
    if which:
        html, _ = standard_doi_table(display_rows)
        title = f"DOIs with lab head {which} author"
        if year != 'All':
            title += f" ({year})"
        endpoint_access()
        return make_response(render_template('general.html', urlroot=request.url_root,
                                             title=title, html=html,
                                             navbar=generate_navbar('Authorship')))
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
    html += "<tr><td>Lab head first author</td><td>" \
            + f"<a href='/doiui_group/{year}/first'>{cnt['first']:,}</a></td></tr>"
    html += "<tr><td>Lab head last author</td><td>" \
            + f"<a href='/doiui_group/{year}/last'>{cnt['last']:,}</a></td></tr>"
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
    endpoint_access()
    return make_response(render_template('bokeh.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         chartscript=chartscript, chartdiv=chartdiv,
                                         navbar=generate_navbar('Authorship')))


@app.route('/dois_no_janelia/<string:year>')
@app.route('/dois_no_janelia')
def dois_no_janelia(year='All'):
    ''' Show DOIs without Janelia authors
    '''
    payload = {"jrc_author": {"$exists": False},
               "subtype": {"$ne": "other"},
               "$or": [{"types.resourceTypeGeneral": "Preprint"},
                       {"type": {"$in": ["journal-article", "peer-review"]}}
                      ]
              }
    if year != 'All':
        payload['jrc_publishing_date'] = {"$regex": "^"+ year}
    try:
        cnt = DB['dis'].dois.count_documents(payload)
        if cnt:
            rows = DB['dis'].dois.find(payload).sort([("jrc_publishing_date", -1)])
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not dois with no Janelia authors " \
                                                    + "from dois collection"),
                               message=error_message(err))
    html = "These are journal articles/preprints with no Janelia authors<br>" \
           + year_pulldown('dois_no_janelia')
    if cnt:
        html += "<table id='nojanelia' class='tablesorter standard'>" \
                + "<thead><tr><th>DOI</th><th>Title</th><th>Published</th></tr></thead>" \
                + "<tbody>"
        for row in rows:
            title = DL.get_title(row)
            html += f"<tr><td>{doi_link(row['doi'])}</td><td>{title}</td>" \
                    + f"<td>{row['jrc_publishing_date']}</td></tr>"
        html += "</tbody></table>"
    title = "DOIs without Janelia authors"
    if year != 'All':
        title += f" for {year}"
    title += f" ({cnt:,})"
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         navbar=generate_navbar('Authorship')))


@app.route('/raw/<string:resource>/<path:doi>')
def show_raw(resource=None, doi=None):
    ''' Raw resource metadata for a DOI
    '''
    result = initialize_result()
    response = None
    if resource == 'bioRxiv':
        try:
            response = JRC.call_biorxiv(doi)
        except Exception as err:
            raise InvalidUsage(str(err), 500) from err
    elif resource == 'figshare':
        try:
            response = JRC.call_figshare(doi)
            if response and 'url' in response[0] and response[0]['url']:
                try:
                    response2 = requests.get(response[0]['url'], timeout=10)
                    if response2.status_code == 200:
                        response = response2.json()
                except Exception:
                    pass
        except Exception as err:
            raise InvalidUsage(str(err), 500) from err
    elif resource == 'protocols.io':
        suffix = f"protocols/{doi}"
        try:
            response = JRC.call_protocolsio(suffix)
        except Exception as err:
            raise InvalidUsage(str(err), 500) from err
    if response:
        result['data'] = response
    return generate_response(result)

# ******************************************************************************
# * UI endpoints (Organizations)                                               *
# ******************************************************************************

@app.route('/org_detail/<string:org_in>/<string:year>/<string:show>')
@app.route('/org_detail/<string:org_in>/<string:year>')
@app.route('/org_detail/<string:org_in>')
def show_organization(org_in, year=str(datetime.now().year), show="full"):
    '''
    Return DOIs for an organization
    '''
    ptitle = f"DOIs for {org_in}"
    if year != 'All':
        ptitle += f" in {year}"
    try:
        row = DB['dis'].org_group.find_one({"group": org_in})
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get organization groups"),
                               message=error_message(err))
    orgcount = {}
    subtitle = ""
    if row:
        orgs = row['members']
        for org in orgs:
            orgcount[org] = 0
    else:
        orgs = [org_in]
    payload = {"jrc_tag.name": {"$in": orgs}}
    jrc_payload = {}
    jrc_journal_payload = {}
    if year != 'All':
        payload['jrc_publishing_date'] = {"$regex": "^"+ year}
        jrc_payload['jrc_publishing_date'] = {"$regex": "^"+ year}
        jrc_journal_payload['jrc_publishing_date'] = {"$regex": "^"+ year}
    if show == 'journal':
        payload["$or"] = [{"type": "journal-article"},
                          {"types.resourceTypeGeneral": "Preprint"},
                          {"types.resourceTypeGeneral": "DataPaper"}, {"subtype": "preprint"}]
        jrc_payload["$or"] = [{"type": "journal-article"},
                              {"types.resourceTypeGeneral": "Preprint"},
                              {"types.resourceTypeGeneral": "DataPaper"}, {"subtype": "preprint"}]
    jrc_journal_payload["$or"] = [{"type": 'journal-article', "subtype": {"$ne": ""}},
                                  {"types.resourceTypeGeneral": "DataPaper"}]
    try:
        rows = DB['dis'].dois.find(payload).sort("jrc_publishing_date", -1)
        jrc_items = DB['dis'].dois.count_documents(jrc_payload)
        jrc_journal_items = DB['dis'].dois.count_documents(jrc_journal_payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get DOIs"),
                               message=error_message(err))
    html = '<table id="dois" class="tablesorter standard"><thead><tr>' \
           + '<th>Published</th><th>DOI</th><th>Tags</th><th>Title</th></tr></thead><tbody>'
    dcnt = org_journal_cnt = 0
    content = ""
    for row in rows:
        #print(row['doi'], DL.is_journal(row))
        if DL.is_journal(row) and not DL.is_version(row):
            org_journal_cnt += 1
        dcnt += 1
        published = DL.get_publishing_date(row)
        title = DL.get_title(row)
        if not title:
            title = ""
        tags = []
        for tag in row['jrc_tag']:
            if tag['name'] in orgs:
                tags.append(tag['name'])
        html += f"<tr><td>{published}</td><td>{doi_link(row['doi'])}</td>" \
                + f"<td>{', '.join(sorted(tags))}</td><td>{title}</td></tr>"
        authors = DL.get_author_list(row)
        content += f"{published}\t{row['doi']}\t{', '.join(sorted(tags))}\t{title}\t{authors}\n"
        for org in [tag['name'] for tag in row['jrc_tag']]:
            if org in orgs and len(orgs) > 1:
                orgcount[org] += 1
    if len(orgs) > 1:
        subtitle += "<br><p style='line-height:1.1'>"
        for org in orgs:
            if not orgcount[org]:
                continue
            link = f"<a href='/tag/{org}/{year}'>{org}</a>"
            subtitle += f"<br><span style='color: white'>{link}: {orgcount[org]:,}</span>"
            count = DL.get_author_counts(org, year, show, DB['dis'].dois, DB['dis'].orcid)
            for auth, cnt in sorted(count.items()):
                subtitle += f"<br>&nbsp;&nbsp;{auth}: {cnt:,}"
        subtitle += "</p>"
        #subtitle += "<br><p style='line-height:1.1'>" \
        #            + "".join(["<br><span style='color: " \
        #                       + f"{'white' if orgcount[org] else 'darkgray'}'>{org}: " \
        #                       + f"{orgcount[org]:,}</span>" for org in orgs]) + "</p>"
    html += '</tbody></table>'
    if not dcnt:
        html = year_pulldown(f"org_detail/{org_in}") + subtitle \
               + f"<br>No DOIs found for {org_in}" \
               + journal_buttons(show, f"/org_detail/{org_in}/{year}")
    else:
        header = ['Published', 'DOI', 'Tags', 'Title', 'Authors']
        buttons = "<div class='flexrow'><div class='flexcol'>" \
                  + journal_buttons(show, f"/org_detail/{org_in}/{year}") \
                  + f"</div><div class='flexcol'>{'&nbsp;'*5}</div><div class='flexcol'>" \
                  + create_downloadable(f"{org_in.replace(' ', '_')}_{year}", header, content) \
                  + "</div></div>"
        html = year_pulldown(f"org_detail/{org_in}") + subtitle \
               + f"{'Journal/preprint ' if show == 'journal' else ''}" \
               + f"DOIs found for {org_in}: {dcnt:,} ({org_journal_cnt:,} " \
               + "journal publications)<br>" \
               + f"{'Journal/preprint ' if show == 'journal' else ''}" \
               + f"DOIs found for Janelia Research Campus: {jrc_items:,} " \
               + f"({jrc_journal_items:,} journal publications)<br>" \
               + buttons + html
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=ptitle, html=html,
                                         navbar=generate_navbar('DOIs')))


@app.route('/org_summary/<string:org>/<string:year>/<string:which>/')
@app.route('/org_summary/<string:org>/<string:year>')
@app.route('/org_summary/<string:org>')
@app.route('/org_summary')
def org_summary(org='Shared Resources',year='All', which=None):
    ''' Show organization authorship summary
    '''
    # Get lab head employee IDs
    leads, shared = get_leads_and_org_members(org)
    # Get first authors
    payload = {"jrc_first_id": {"$in": leads}}
    if year != 'All':
        payload['jrc_publishing_date'] = {"$regex": "^"+ year}
    finds = {"first": [], "firstsr": [], "last": [], "lastsr": []}
    try:
        rows = DB['dis'].dois.find(payload)
        for row in rows:
            if DL.is_journal(row) and not DL.is_version(row):
                finds['first'].append(row)
        payload['jrc_tag.name'] = {"$in": shared}
        rows = DB['dis'].dois.find(payload)
        for row in rows:
            if DL.is_journal(row) and not DL.is_version(row):
                finds['firstsr'].append(row)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get first authors " \
                                                    + "from dois collection"),
                               message=error_message(err))
    # Get last authors
    payload = {"jrc_last_id": {"$in": leads}}
    if year != 'All':
        payload['jrc_publishing_date'] = {"$regex": "^"+ year}
    try:
        rows = DB['dis'].dois.find(payload)
        for row in rows:
            if DL.is_journal(row) and not DL.is_version(row):
                finds['last'].append(row)
        payload['jrc_tag.name'] = {"$in": shared}
        rows = DB['dis'].dois.find(payload)
        for row in rows:
            if DL.is_journal(row) and not DL.is_version(row):
                finds['lastsr'].append(row)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get last authors " \
                                                    + "from dois collection"),
                               message=error_message(err))
    if which:
        if org == 'all':
            display_rows = finds['first'] if which == 'first' else finds['last']
        else:
            display_rows = finds['firstsr'] if which == 'first' else finds['lastsr']
        html, _ = standard_doi_table(display_rows)
        if org == 'all':
            title = f"Journal publications with lab head {which} author"
        else:
            title = f"Journal publications for {org} with lab head {which} author"
        if year != 'All':
            title += f" ({year})"
        endpoint_access()
        return make_response(render_template('general.html', urlroot=request.url_root,
                                             title=title, html=html,
                                             navbar=generate_navbar('Authorship')))
    title = f"Journal publications for {org}"
    if year != 'All':
        title += f" ({year})"
    html = "<table id='org' class='tablesorter numbers'><thead><tr><th></th><th>All</th>" \
           + f"<th>{org}</th></tr></thead><tbody>"
    c1 = f"<a href='/org_summary/all/{year}/first'>{len(finds['first']):,}</a>" \
        if finds['first'] else ""
    c2 = f"<a href='/org_summary/{org}/{year}/first'>{len(finds['firstsr']):,}</a>" \
         if finds['firstsr'] else ""
    html += f"<tr><td>Lab head first author</td><td>{c1}</td><td>{c2}</td></tr>"
    c1 = f"<a href='/org_summary/all/{year}/last'>{len(finds['last']):,}</a>" \
         if finds['last'] else ""
    c2 = f"<a href='/org_summary/{org}/{year}/last'>{len(finds['lastsr']):,}</a>" \
         if finds['lastsr'] else ""
    html += f"<tr><td>Lab head last author</td><td>{c1}</td><td>{c2}</td></tr>"
    html += "</tbody></table><br>" + year_pulldown(f"org_summary/{org}")
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         navbar=generate_navbar('Authorship')))


@app.route('/org_year/<string:org>')
@app.route('/org_year')
def org_year(org="Shared Resources"):
    ''' Plot organization journal publications by year
    '''
    leads, shared = get_leads_and_org_members(org)
    finds = get_org_authorship('All', leads, shared)
    years = {'years': {}, 'Janelia': {}, org: {}}
    for pdate in finds['janelia']:
        if pdate[:4] not in years['Janelia']:
            years['Janelia'][pdate[:4]] = 0
        years['Janelia'][pdate[:4]] += 1
    for pdate in finds['org']:
        years['years'][pdate[:4]] = True
        if pdate[:4] not in years[org]:
            years[org][pdate[:4]] = 0
        years[org][pdate[:4]] += 1
    data = {'years': sorted(years['years'].keys()), 'Janelia': [], org: []}
    for yr in data['years']:
        if yr in years[org]:
            data['Janelia'].append(years['Janelia'][yr] - years[org][yr])
            data[org].append(years[org][yr])
        else:
            data['Janelia'].append(years['Janelia'][yr])
            data[org].append(0)
    title = f"Journal publications by year for {org} with lab head last author"
    html = '<table id="years" class="tablesorter numbers"><thead><tr>' \
           + f"<th>Year</th><th>All</th><th>{org}</th>" \
           + '</tr></thead><tbody>'
    total = {'Janelia': 0, org: 0}
    for yr in data['years']:
        total['Janelia'] += years['Janelia'][yr]
        total[org] += years[org][yr]
        c1 = f"<a href='/org_summary/all/{yr}/last'>{years['Janelia'][yr]}</a>"
        c2 = f"<a href='/org_summary/{org}/{yr}/last'>{years[org][yr]}</a>"
        html += f"<tr><td>{yr}</td><td>{c1}</td><td>{c2}</td></tr>"
    c1 = f"<a href='/org_summary/all/All/last'>{total['Janelia']}</a>"
    c2 = f"<a href='/org_summary/{org}/All/last'>{total[org]}</a>"
    html += f"</tbody><tfoot><tr><td>TOTAL</td><td>{c1}</td><td>{c2}</td></tr>"
    html += '</tfoot></table><br>'
    print(json.dumps(data, indent=2))
    data[f"With {org} authors"] = data.pop(org)
    data[f"No {org} authors"] = data.pop("Janelia")
    print(json.dumps(data, indent=2))
    chartscript, chartdiv = DP.stacked_bar_chart(data, title,
                                                 xaxis="years",
                                                 yaxis=(f"No {org} authors", f"With {org} authors"),
                                                 colors=DP.SOURCE_PALETTE)
    endpoint_access()
    return make_response(render_template('bokeh.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         chartscript=chartscript, chartdiv=chartdiv,
                                         navbar=generate_navbar('DOIs')))

# ******************************************************************************
# * UI endpoints (Preprints)                                                  *
# ******************************************************************************

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
    endpoint_access()
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
    endpoint_access()
    return make_response(render_template('bokeh.html', urlroot=request.url_root,
                                         title="DOIs preprint status by year", html=html,
                                         chartscript=chartscript, chartdiv=chartdiv,
                                         navbar=generate_navbar('Preprints')))


@app.route('/preprint_with_pub')
def preprint_with_pub():
    ''' Show preprints with publications
    '''
    payload = {"subtype": "preprint", "jrc_preprint": {"$exists": 1}}
    coll = DB['dis'].dois
    try:
        rows = coll.find(payload).sort([("jrc_publishing_date", -1)])
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get preprint data from dois"),
                               message=error_message(err))
    day_count = []
    day_pub = {}
    fileoutput = ""
    header = ['Published', 'DOI', 'Title', 'Journal']
    html = "<table id='preprint_with_pub' class='tablesorter numbers'><thead><tr><th>" \
           + "</th><th>".join(header) + "</th></tr></thead><tbody>"
    for row in rows:
        if len(row['jrc_preprint']) == 1:
            prep = row['jrc_preprint'][0]
        else:
            prep = None
            for pdoi in row['jrc_preprint']:
                prow = DL.get_doi_record(pdoi, coll=coll)
                if not DL.is_version(prow):
                    prep = pdoi
                    break
            if not prep:
                prep = row['jrc_preprint'][0]
        jour = DL.get_doi_record(prep, coll=coll)
        if not jour or 'jrc_publishing_date' not in jour:
            continue
        # Get dates
        preprint_date = datetime.strptime(row['jrc_publishing_date'], '%Y-%m-%d')
        journal_date = datetime.strptime(jour['jrc_publishing_date'], '%Y-%m-%d')
        # Calculate days between preprint and journal publication
        days = (journal_date - preprint_date).days
        day_count.append(days)
        if row['jrc_journal'] not in day_pub:
            day_pub[row['jrc_journal']] = []
        day_pub[row['jrc_journal']].append(days)
        fileoutput+= "\t".join([row['jrc_publishing_date'], row['doi'], DL.get_title(row),
                                row['jrc_journal']]) + "\n"
        html += f"<tr><td>{row['jrc_publishing_date']}</td><td>{doi_link(row['doi'])}</td>" \
                + f"<td>{DL.get_title(row)}</td><td>{row['jrc_journal']}</td></tr>"
    html += '</tbody></table>'
    avg_days = sum(day_count) / len(day_count) if day_count else 0
    pre = f"Preprints with journal publications: {len(day_count):,}<br>" \
           + f"Average days to publication: {avg_days:,.1f}<br>"
    pre += "<table id='preprint_with_pub' class='tablesorter numbers'><thead><tr>" \
           + "<th>Journal</th><th>Average days to publication</th></tr></thead><tbody>"
    for jour, days in day_pub.items():
        avg_days = sum(days) / len(days) if days else 0
        pre += f"<tr><td>{jour}</td><td>{avg_days:,.1f}</td></tr>"
    pre += '</tbody></table>' + create_downloadable('preprint_with_pub', header, fileoutput)
    html = pre + html
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title="Preprints with journal publications", html=html,
                                         navbar=generate_navbar('Preprints')))


@app.route('/preprint_no_pub')
def preprint_no_pub():
    ''' Show preprints with publications
    '''
    payload = {"subtype": "preprint", "jrc_preprint": {"$exists": 0}}
    try:
        rows = DB['dis'].dois.find(payload).sort([("jrc_publishing_date", -1)])
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get preprint data from dois"),
                               message=error_message(err))
    fileoutput = ""
    header = ['Published', 'DOI', 'Title', 'Journal']
    html = "<table id='preprint_no_pub' class='tablesorter numbers'><thead><tr><th>" \
           + "</th><th>".join(header) + "</th></tr></thead><tbody>"
    cnt = 0
    for row in rows:
        cnt += 1
        ptitle = DL.get_title(row)
        fileoutput+= "\t".join([row['jrc_publishing_date'], row['doi'], DL.get_title(row),
                                row['jrc_journal']]) + "\n"
        html += f"<tr><td>{row['jrc_publishing_date']}</td>" \
                + f"<td>{doi_link(row['doi'])}</td><td>{ptitle}</td>" \
                + f"<td>{row['jrc_journal']}</td></tr>"
    html += '</tbody></table>'
    html = f"Preprints without journal publications: {cnt:,}<br><br>"  \
           + create_downloadable('preprint_no_pub', header, fileoutput) + html
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title="Preprints without journal publications", html=html,
                                         navbar=generate_navbar('Preprints')))


@app.route('/pub_no_preprint')
def pub_no_preprint():
    ''' Show publications without preprints
    '''
    payload = {"type": "journal-article", "jrc_preprint": {"$exists": 0}}
    try:
        rows = DB['dis'].dois.find(payload).sort([("jrc_publishing_date", -1)])
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get preprint data from dois"),
                               message=error_message(err))
    fileoutput = ""
    header = ['Published', 'DOI', 'Title', 'Journal']
    html = "<table id='pub_nopreprint' class='tablesorter numbers'><thead><tr><th>" \
           + "</th><th>".join(header) + "</th></tr></thead><tbody>"
    cnt = 0
    for row in rows:
        cnt += 1
        ptitle = DL.get_title(row)
        fileoutput+= "\t".join([row['jrc_publishing_date'], row['doi'], DL.get_title(row),
                                row['jrc_journal']]) + "\n"
        html += f"<tr><td>{row['jrc_publishing_date']}</td>" \
                + f"<td>{doi_link(row['doi'])}</td><td>{ptitle}</td>" \
                + f"<td>{row['jrc_journal']}</td></tr>"
    html += '</tbody></table>'
    html = f"Journal publications without preprints: {cnt:,}<br><br>"  \
           + create_downloadable('pub_no_preprint', header, fileoutput) + html
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title="Journal publications without preprints", html=html,
                                         navbar=generate_navbar('Preprints')))

# ******************************************************************************
# * UI endpoints (Journals)                                                    *
# ******************************************************************************

@app.route('/journals_dois/<string:year>')
@app.route('/journals_dois')
def show_journals_dois(year='All'):
    ''' Show journals in a table
    '''
    errmsg = "Could not get journal data from subscription collection"
    try:
        rows = DB['dis'].subscription.find({"type": "Journal"})
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning(errmsg),
                               message=error_message(err))
    subscribed = {}
    for row in rows:
        subscribed[row['title']] = row
    try:
        journal = get_top_journals(year, maxpub=True)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning(errmsg),
                               message=error_message(err))
    if not journal:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning(errmsg),
                               message='No journals were found')
    html = f"<h4>Journals found: {len(journal):,}</h4>" \
           + '<table id="journals" class="tablesorter numbers"><thead><tr>' \
           + '<th>Journal</th><th>Publisher</th><th>Count</th><th>Last published to</th>' \
           + '<th>Subscription</th></tr></thead><tbody>'
    for key in sorted(journal, key=lambda x: journal[x]['count'], reverse=True):
        if key in subscribed:
            jour = f"<a href='{subscribed[key]['url']}'>{key}</a>"
            jour = f"<a href='/subscription/{str(subscribed[key]['_id'])}'>{key}</a>"
            publisher = subscribed[key]['publisher']
            sub = '<span style="color: lime">YES</span>' \
                  if subscribed[key]['access'] == 'Subscription' \
                  else f"<span style='color: yellowgreen'>{subscribed[key]['access']}</span>"
        else:
            jour = key
            publisher = sub = ''
        html += f"<tr><td>{jour}</td><td>{publisher}</td>" \
                + f"<td><a href='/journal/{key}/{year}'>{journal[key]['count']:,}</a></td>" \
                + f"<td>{journal[key]['maxpub']}</td><td>{sub}</td></tr>"
    html += '</tbody></table>'
    title = "DOIs by journal"
    if year != 'All':
        title += f" ({year})"
    html = "Note: not all subscriptions are currently tracked - " \
           + "Subscription tracking is a work in process<br>" \
           + year_pulldown('journals_dois') + html
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         navbar=generate_navbar('Journals')))


@app.route('/top_journals/<string:year>/<int:top>')
@app.route('/top_journals/<string:year>')
@app.route('/top_journals')
def top_journals(year='All', top=10):
    ''' Show top journals
    '''
    top = min(top, 20)
    try:
        journal = get_top_journals(year, janelia=False)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get journal data from dois"),
                               message=error_message(err))
    if not journal:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get journal data from dois"),
                               message='No journals were found')
    html = "Note that this does not contain Janelia Research Campus (figshare)<br>" \
           + '<table id="journals" class="tablesorter numberlast"><thead><tr>' \
           + '<th>Journal</th><th>Count</th></tr></thead><tbody>'
    data = {}
    for key in sorted(journal, key=journal.get, reverse=True):
        val = journal[key]
        if len(data) >= top:
            continue
        data[key] = val
        html += f"<tr><td><a href='/journal/{key}/{year}'>{key}</a></td><td>{val:,}</td></tr>"
    html += '</tbody></table><br>' + year_pulldown('top_journals')
    title = "DOIs by journal"
    if year != 'All':
        title += f" ({year})"
    chartscript, chartdiv = DP.pie_chart(data, title, "source", width=875, height=550,
                                         colors='Category20')
    title = f"Top {top} DOI journals"
    if year != 'All':
        title += f" ({year})"
    endpoint_access()
    return make_response(render_template('bokeh.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         chartscript=chartscript, chartdiv=chartdiv,
                                         navbar=generate_navbar('Journals')))


@app.route('/dois_nojournal')
def dois_nojournal():
    ''' Show DOIs missing journal data
    '''
    # The payload is somewhat coarse and won't get everything (thanks, eLife...)
    payload = {"jrc_journal": {"$exists": False},
               "type": {"$nin": ["component", "grant"]}}
    try:
        rows = DB['dis'].dois.find(payload).sort([("doi", 1)])
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get journal data from dois"),
                               message=error_message(err))
    html = '<table id="articles" class="tablesorter standard"><thead><tr>' \
           + '<th>DOI</th><th>Title</th></tr></thead><tbody>'
    cnt = 0
    for row in rows:
        cnt += 1
        doi = row['doi']
        html += f"<tr><td><a href='/doiui/{doi}'>{doi}</a></td><td>{DL.get_title(row)}</td></tr>"
    html += '</tbody></table>'
    if not cnt:
        html = "<h5 style='color: lime'>No DOIs missing journal data</h5>"
    title = f"DOIs missing journals ({cnt})"
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         navbar=generate_navbar('Journals')))


@app.route('/journal/<string:jname>/<string:year>')
@app.route('/journal/<string:jname>')
def show_journal_ui(jname, year='All'):
    ''' Show journal DOIs
    '''
    try:
        payload = {"$or": [{"container-title": jname},
                           {"institution.name": jname}]}
        payload = {"jrc_journal": jname}
        if year != 'All':
            payload['jrc_publishing_date'] = {"$regex": "^"+ year}
        rows = DB['dis'].dois.find(payload).sort("jrc_publishing_date", -1)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get DOIs for journal"),
                               message=error_message(err))
    html, _ = standard_doi_table(rows)
    title = f"DOIs for {jname}"
    if year != 'All':
        title += f" ({year})"
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                            title=title, html=html,
                                            navbar=generate_navbar('Journals')))


@app.route('/journals_referenced/<string:year>')
@app.route('/journals_referenced')
def journals_referenced(year='All'):
    '''
    Return a report of journals referenced in DOIs
    '''
    ptitle = "Journals referenced by Crossref DOIs"
    if year != 'All':
        ptitle += f" in {year}"
    payload = [{"$match": {"reference.journal-title": {"$exists": True, "$ne": None}}},
               {"$unwind": "$reference"},
               {"$group": {"_id": "$reference.journal-title", "count": {"$sum": 1}}},
               {"$sort": {"count": -1, "_id": 1}}
    ]
    if year != 'All':
        payload[0]['$match']['jrc_publishing_date'] = {"$regex": "^"+ year}
    try:
        rows = DB['dis'].dois.aggregate(payload)
        # , collation={"locale": "en"}
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get DOIs"),
                               message=error_message(err))
    journals = refs = 0
    fileoutput = ""
    for row in rows:
        journals += 1
        refs += row['count']
        fileoutput += f"{row['_id']}\t{row['count']}\n"
    html = year_pulldown("journals_referenced") + "<br><br>" \
           + create_downloadable('journals', ['Journal', 'References'], fileoutput) \
           + f"<br><br>Journals: {journals:,}<br>References: {refs:,}"
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=ptitle, html=html,
                                         navbar=generate_navbar('Journals')))

# ******************************************************************************
# * UI endpoints (subscriptions)                                               *
# ******************************************************************************

@app.route('/subscriptions')
def show_subscription_summary():
    ''' Show subscription summary
    '''
    errmsg = "Could not get data from subscription collection"
    try:
        cnt = DB['dis'].subscription.count_documents({})
        oacnt = DB['dis'].subscription.count_documents({"access": "Open access"})
        pubcnt = DB['dis'].subscription.distinct("publisher")
        typs = DB['dis'].subscription.aggregate([{"$group": {"_id": "$type", "count": {"$sum": 1}}},
                                                 {"$sort": {"_id": 1}}])
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning(errmsg),
                               message=error_message(err))
    html = f"<h4>Found {cnt:,} subscriptions ({oacnt:,} open access) across " \
           + f"{len(pubcnt):,} publishers</h4>"
    types = {}
    for row in typs:
        types[row['_id']] = int(row['count'])
    payload = [{"$group": {"_id": {"publisher": "$publisher", "type": "$type"},
                           "count": {"$sum": 1}}},
               {"$sort": {"_id.publisher": 1, "_id.type": 1}}
              ]
    try:
        rows = DB['dis'].subscription.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning(errmsg),
                               message=error_message(err))
    transform = {}
    for row in rows:
        if row['_id']['publisher'] not in transform:
            transform[row['_id']['publisher']] = collections.defaultdict(lambda: 0, {})
        transform[row['_id']['publisher']][row['_id']['type']] = row['count']
        transform[row['_id']['publisher']]['TOTAL'] += row['count']
    html += "<table id='journals' class='tablesorter numbers'><thead><tr>" \
            + "<th>Publisher</th><th>" + ("</th><th>".join(types)) \
            + "</th><th>TOTAL</th></tr></thead><tbody>"
    for publisher, data in transform.items():
        count = []
        for typ in types:
            if typ in data:
                tcnt = f"<a href='/subscriptionlist/{publisher}/{typ}/publisher'>" \
                       + f"{data[typ]:,}</a>"
            else:
                tcnt = ""
            count.append(tcnt)
        html += f"<tr><td>{publisher}</td><td>" + "</td><td>".join(count) \
                + f"</td><td>{data['TOTAL']:,}</td></tr>"
    html += "</tbody><tfoot><tr><td style='text-align:right'>TOTAL</td><td>" \
            + "</td><td>".join(f"<a href='/subscriptions/{key}'>{val:,}</a>" \
                               for key, val in types.items()) \
            + f"</td><td>{cnt:,}</td></tr></tfoot>"
    html += '</table>'
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title='Subscription summary', html=html,
                                         navbar=generate_navbar('Subscriptions')))


@app.route('/subscriptions/<string:jtype>')
def show_subscriptions(jtype):
    ''' Show journals, books, etc. in a table
    '''
    errmsg = "Could not get data from subscription collection"
    try:
        cnt = DB['dis'].subscription.count_documents({"type": jtype})
        rows = DB['dis'].subscription.find({"type": jtype})
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning(errmsg),
                               message=error_message(err))
    if not rows:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning(errmsg),
                               message='No journals were found')
    html = '<table id="journals" class="tablesorter standard"><thead><tr>' \
           + '<th>Title</th><th>Publisher</th><th>Provider</th></tr></thead><tbody>'
    fileoutput = ""
    jlist = {}
    publist = {}
    for row in rows:
        jlist[row['title']] = True
        publist[row['publisher']] = True
        publist[row['publisher']] = True
        jour = f"<a href='{row['url']}'>{row['title']}</a>"
        jour = f"<a href='/subscription/{str(row['_id'])}'>{row['title']}</a>"
        html += f"<tr><td>{jour}</td><td>{row['publisher']}</td>" \
                + f"<td>{row['provider']}</td></tr>"
        fileoutput += f"{row['title']}\t{row['publisher']}\t{row['provider']}\n"
    html += '</tbody></table>'
    title = f"{jtype} subscriptions ({cnt:,})"
    html = create_downloadable(jtype, ['Title', 'Publisher', 'Provider'], fileoutput)
    titles = '<option>' + '</option><option>'.join(sorted(jlist.keys())) + '</option>'
    pubs = '<option>' + '</option><option>'.join(sorted(publist.keys())) + '</option>'
    endpoint_access()
    return make_response(render_template('subscription.html', urlroot=request.url_root,
                                         title=title, titles=titles, pubs=pubs,
                                         html=html, sub=jtype,
                                         navbar=generate_navbar('Subscriptions')))


@app.route('/subscriptionlist/<string:sub>/<string:stype>/<string:field>')
def show_subscriptionlist(sub, stype='Journal', field='title'):
    ''' Show subscription list for a title
    '''
    errmsg = "Could not get data from subscription collection"
    try:
        cnt = DB['dis'].subscription.count_documents({field: sub, "type": stype})
        rows = DB['dis'].subscription.find({field: sub, "type": stype})
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning(errmsg),
                               message=error_message(err))
    if not cnt:
        return render_template('warning.html', urlroot=request.url_root,
                               title=render_warning(errmsg),
                               message=f"No subscriptions were found for {sub}")
    if cnt == 1:
        return redirect(f"/subscription/{rows[0]['_id']}")
    html = "<table id='journals' class='tablesorter standard'><thead><tr>" \
           + '<th>Title</th><th>Publisher</th><th>Provider</th><th>Title ID</th>' \
           + '</tr></thead><tbody>'
    for row in rows:
        link = f"<a href='/subscription/{str(row['_id'])}'>{row['title']}</a>"
        html += f"<tr><td>{link}</td><td>{row['publisher']}</td>" \
                + f"<td>{row['provider']}</td><td>{row['title-id']}</td></tr>"
    html += '</tbody></table>'
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=f"{stype} subscriptions for {field} " \
                                               + f"{sub} ({cnt:,})", html=html,
                                         navbar=generate_navbar('Subscriptions')))


@app.route('/subscription/<string:sid>')
def show_subscription(sid):
    ''' Show subscription
    '''
    errmsg = "Could not get data from subscription collection"
    try:
        row = DB['dis'].subscription.find_one({"_id": bson.ObjectId(sid)})
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning(errmsg),
                               message=error_message(err))
    if not row:
        return render_template('warning.html', urlroot=request.url_root,
                               title=render_warning(errmsg),
                               message=f"No subscription was found for {sid}")
    html = f"<table class='proplist'><tr><td>Publisher</td><td>{row['publisher']}</td></tr>" \
           + f"<tr><td>Type</td><td>{row['type']}</td></tr>" \
           + f"<tr><td>Access</td><td>{row['access']}</td></tr>" \
           + f"<tr><td>Provider</td><td>{row['provider']}</td></tr>" \
           + f"<tr><td>Title ID</td><td>{row['title-id']}</td></tr>"
    html += "</table>"
    link = f"window.location.href=\'{row['url']}\'"
    html += '<br><div><button id="toggle-to-all" type="button" class="btn btn-success btn-small"' \
            + f"onclick=\"{link}\">Access {row['type']}</button></div>"
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=row['title'], html=html,
                                         navbar=generate_navbar('Subscriptions')))

# ******************************************************************************
# * UI endpoints (ORCID)                                                       *
# ******************************************************************************
@app.route('/orcidui/<string:oid>')
def show_oid_ui(oid):
    ''' Show ORCID user
    '''
    try:
        data = JRC.call_orcid(oid)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not retrieve ORCID ID"),
                               message=error_message(err))
    if 'person' not in data:
        if 'user-message' not in data:
            data['user-message'] = f"Could not find {oid} in orcid collection"
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
        orciddata, dois, _ = get_orcid_from_db(oid, use_eid=bool('userIdO365' in oid))
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
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root, pagetitle=oid,
                                         title=f"<a href='https://orcid.org/{oid}' " \
                                               + f"target='_blank'>{oid}</a>", html=html,
                                         navbar=generate_navbar('ORCID')))


@app.route('/userui/<string:eid>/<string:show>')
@app.route('/userui/<string:eid>')
def show_user_ui(eid, show='full'):
    ''' Show user record by employeeId (user ID)
    '''
    try:
        if "@" in eid:
            orciddata, _, full_name = get_orcid_from_db(eid, use_eid=True, bare=False, show=show)
        else:
            orciddata, _, full_name = get_orcid_from_db(eid, use_eid=False, bare=False, show=show)
    except CustomException as err:
        return render_template('error.html', urlroot=request.url_root,
                                title=render_warning(f"Could not find user ID {eid}",
                                                     'warning'),
                                message=error_message(err))
    if not orciddata:
        return render_template('warning.html', urlroot=request.url_root,
                               title=render_warning(f"Could not find user ID {eid}", 'warning'),
                               message="Could not find any information for this employee ID")
    buttons = journal_buttons(show, f"/userui/{eid}")
    if "DOIs:" in orciddata:
        orciddata = re.sub(r"(DOIs: [0-9,]+)", r"<br>\1" + buttons, orciddata)
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=full_name, html=orciddata,
                                         navbar=generate_navbar('ORCID')))


@app.route('/unvaluserui/<string:iid>')
def show_unvaluser_ui(iid):
    ''' Show user record by orcid collection ID
    '''
    try:
        orciddata, _, _ = get_orcid_from_db(iid, bare=True)
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
    endpoint_access()
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
    html = f"Search term: {name}<br><p>Number of authors: " \
           + f"<span id='totalrowsa'>{count}</span></p>" + html
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title="Authors", html=html,
                                         navbar=generate_navbar('ORCID')))


@app.route('/orcid_datepicker')
def show_orcid_date_picker():
    '''
    Show a datepicker for selecting employees
    '''
    after = '<a class="btn btn-success" role="button" onclick="lookup(); return False;">' \
            + 'Look up employees</a>'
    return make_response(render_template('orcid_picker.html', urlroot=request.url_root,
                                         title="Employee lookup by hire date",
                                         startdef=weeks_ago(4), stopdef=str(date.today()),
                                         after=after, navbar=generate_navbar('DOIs')))


@app.route('/orcid/hiredate/<string:startdate>/<string:stopdate>')
def show_hires(startdate, stopdate):
    '''
    Return employees that have been hired within the specified date range
    '''
    try:
        cnt = DB['dis'].orcid.count_documents({"hireDate": {"$gte" : startdate,
                                                            "$lte" : stopdate}})
        rows = DB['dis'].orcid.find({"hireDate": {"$gte" : startdate, "$lte" : stopdate}},
                                   {'_id': 0}).sort([("hireDate", -1), ("family", 1)])
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get employees"),
                               message=error_message(err))
    if not cnt:
        return render_template('warning.html', urlroot=request.url_root,
                               title=render_warning("Employees not found"),
                               message=f"No employees were hired {startdate} - {stopdate}")
    html = "<table id='hires' class='tablesorter standard'><thead><tr>" \
           + "<th>Hire Date</th><th>Name</th><th>ORCID</th><th>Affiliations</th>" \
           + "</tr></thead><tbody>"
    for row in rows:
        if 'orcid' not in row:
            row['orcid'] = ""
        who = f"{row['family'][0]}, {row['given'][0]}"
        if 'userIdO365' in row and row['userIdO365']:
            who = f"<a href='/userui/{row['userIdO365']}'>{who}</a>"
        badges = []
        if 'alumni' in row and row['alumni']:
            badges.append(f"{tiny_badge('alumni', 'Former employee')}")
        if 'workerType' in row and row['workerType'] and row['workerType'] != 'Employee':
            badges.append(f"{tiny_badge('contingent', row['workerType'])}")
        if 'group' in row:
            badges.append(f"{tiny_badge('lab', row['group'])}")
        if 'managed' in row and row['managed']:
            for key in row['managed']:
                badges.append(f"{tiny_badge('managed', key)}")
        if badges:
            who += f" {' '.join(badges)}"
        if 'affiliations' not in row:
            row['affiliations'] = []
        else:
            row['affiliations'] = sorted(list(row['affiliations']))
        html += f"<tr><td style='min-width:100px'>{row['hireDate']}</td><td>{who}</td>" \
                + f"<td style='min-width:180px'>{row['orcid']}</td>" \
                + f"<td>{', '.join(row['affiliations'])}</td></tr>"
    html += '</tbody></table>'
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=f"Employees hired {startdate} - {stopdate} " \
                                               + f"({cnt:,})",
                                         html=html, navbar=generate_navbar('ORCID')))


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
        cntaok = DB['dis'].orcid.count_documents({"alumni": {"$exists": True},
                                                  "orcid": {"$exists": True},
                                                  "employeeId": {"$exists": True}})
        cntane = DB['dis'].orcid.count_documents({"alumni": {"$exists": True},
                                                  "orcid": {"$exists": True},
                                                  "employeeId": {"$exists": False}})
        cntano = DB['dis'].orcid.count_documents({"alumni": {"$exists": True},
                                                  "orcid": {"$exists": False},
                                                  "employeeId": {"$exists": True}})
        cntax = DB['dis'].orcid.count_documents({"alumni": {"$exists": True},
                                                 "orcid": {"$exists": False},
                                                  "employeeId": {"$exists": False}})
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
    html += "<tr><td>&nbsp;&nbsp;Janelians with ORCID and employee ID</td>" \
            + f"<td>&nbsp;&nbsp;{cntb:,} ({cntb/cntj*100:.2f}%)</td></tr>"
    data['Janelians with ORCID and employee ID'] = cntb
    html += f"<tr><td>&nbsp;&nbsp;Janelians with ORCID only</td><td>&nbsp;&nbsp;{cnto:,}" \
            + f" ({cnto/cntj*100:.2f}%)</td></tr>"
    data['Janelians with ORCID only'] = cnto
    html += f"<tr><td>&nbsp;&nbsp;Janelians with employee ID only</td><td>&nbsp;&nbsp;{cnte:,}" \
            + f" ({cnte/cntj*100:.2f}%)</td></tr>"
    data['Janelians with employee ID only'] = cnte
    html += f"<tr><td>Janelians without affiliations/groups</td><td>{cntf:,}</td></tr>"
    html += f"<tr><td>Former employees</td><td>{cnta:,} ({cnta/total*100:.2f}%)</td></tr>"
    data['Former employees'] = cnta
    html += "<tr><td>&nbsp;&nbsp;Former employees with ORCID and employee ID</td>" \
            + f"<td>&nbsp;&nbsp;{cntaok:,} ({cntaok/cnta*100:.2f}%)</td></tr>"
    html += "<tr><td>&nbsp;&nbsp;Former employees with ORCID only</td>" \
            + f"<td>&nbsp;&nbsp;{cntane:,} ({cntane/cnta*100:.2f}%)</td></tr>"
    html += "<tr><td>&nbsp;&nbsp;Former employees with employee ID only</td>" \
            + f"<td>&nbsp;&nbsp;{cntano:,} ({cntano/cnta*100:.2f}%)</td></tr>"
    html += f"<tr><td>&nbsp;&nbsp;No ORCID or employee ID</td><td>&nbsp;&nbsp;{cntax:,} " \
            + f"({cntax/cnta*100:.2f}%)</td></tr>"
    html += '</tbody></table>'
    chartscript, chartdiv = DP.pie_chart(data, "ORCID entries", "type", height=500, width=600,
                                         colors=DP.TYPE_PALETTE, location="top_right")
    # Notifications
    if cnte:
        payload = {"employeeId": {"$exists": False}, "alumni": {"$exists": False}}
        cnt = DB['dis'].orcid.count_documents(payload)
        if cnt:
            rows = DB['dis'].orcid.find(payload).sort("family", 1)
            html += "<h5>Users with no employee ID</h5><p style='line-height:1.1'>"
            for row in rows:
                name = f"{row['given'][0]} {row['family'][0]}"
                dois = author_doi_count(row['given'], row['family'])
                html += f"<a href='/userui/{row['orcid']}'>{name}</a> {dois}<br>"
            html += "</p>"
        payload = {"orcid": {"$exists": False}, "alumni": {"$exists": False}}
        rows = DB['dis'].orcid.find(payload).sort("family", 1)
        noorc = ""
        cnt = 0
        for row in rows:
            if 'workerType' in row and row['workerType'] and row['workerType'] != 'Employee':
                continue
            name = f"{row['given'][0]} {row['family'][0]}"
            dois = author_doi_count(row['given'], row['family'])
            if dois:
                noorc += f"<a href='/userui/{row['userIdO365']}'>{name} {dois}</a><br>"
                cnt += 1
        noorc += "</p>"
        html += f"<h5>Published authors with no ORCID ({cnt:,})</h5>" \
                + f"<p style='line-height:1.1'>{noorc}"
    endpoint_access()
    return make_response(render_template('bokeh.html', urlroot=request.url_root,
                                         title="ORCID entries", html=html,
                                         chartscript=chartscript, chartdiv=chartdiv,
                                         navbar=generate_navbar('ORCID')))


@app.route('/orcid_duplicates')
def orcid_duplicates():
    ''' Show authors with multiple ORCIDs or employee IDs
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
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title="Authors with multiple ORCIDs", html=html,
                                         navbar=generate_navbar('ORCID')))


@app.route('/duplicate_authors')
def author_duplicates():
    ''' Show possible duplicate author records
    '''
    html = ""
    payload = [{"$group" : { "_id": "$family", "count": {"$sum": 1}}},
               {"$match": {"_id": {"$ne" : None} , "count" : {"$gt": 1}}},
               {"$sort": {"count" : -1}},
               {"$project": {"family" : "$_id", "count": 1, "_id" : 0}}
]
    try:
        frows = DB['dis'].orcid.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get family names from " \
                                                    + "orcid collection"),
                               message=error_message(err))
    given = {}
    for frow in frows:
        try:
            grows = DB['dis'].orcid.find({"family": frow['family'][0]})
        except Exception as err:
            return render_template('error.html', urlroot=request.url_root,
                                   title=render_warning("Could not get given names from " \
                                                        + "orcid collection"),
                                   message=error_message(err))
        for grow in grows:
            for giv in grow['given']:
                name = f"{giv} {frow['family'][0]}"
                add_to_name(given, name, grow)
    inner = []
    for name, occur in sorted(given.items(), key=lambda x: x[0].split(' ')[-1]):
        if len(occur) == 1:
            continue
        bcolor = get_dup_color(occur)
        inner.append(f"<div class='rounded' style='background-color: {bcolor};'>{name}<br>" \
                     + "<br>".join(f"&nbsp;&nbsp;&nbsp;&nbsp;{o}" for o in occur) + "</div>")
    html += "".join(inner)
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title="Duplicate authors", html=html,
                                         navbar=generate_navbar('ORCID')))

# ******************************************************************************
# * UI endpoints (External systems)                                            *
# ******************************************************************************

@app.route('/orgs')
@app.route('/orgs/<string:full>')
def peoporgsle(full=None):
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
    cnt = 0
    for key, val in sorted(orgs.items()):
        alink = f"<a href='/tag/{escape(key)}'>{aff[key]}</a>" if key in aff else ''
        tlink = ""
        if key in tag:
            onclick = "onclick='nav_post(\"jrc_tag.name\",\"" + key + "\")'"
            tlink = f"<a href='#' {onclick}>{tag[key]}</a>"
        if not full and not tlink:
            continue
        html += f"<tr><td>{key}</td><td>{val}</td><td>{alink}</td><td>{tlink}</td></tr>"
        cnt += 1
    html += "</tbody></table>"
    if full:
        default = "window.location.href='/orgs'"
        phtml = '<div><button id="toggle-to-all" type="button" class="btn btn-success btn-tiny"' \
               + f'onclick="{default}">Show suporgs with DOIs</button></div>'
        title = "Supervisory organizations"
    else:
        full = "window.location.href='/orgs/full'"
        phtml = '<div><button id="toggle-to-journal" type="button" ' \
               + 'class="btn btn-success btn-tiny"' \
               + f'onclick="{full}">Show all suporgs</button></div>'
        title = "Supervisory organizations with DOIs"
    html = phtml + html
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=f"{title} ({cnt:,})",
                                         html=html, navbar=generate_navbar('External systems')))


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
    endpoint_access()
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
    for field in ['employeeId', 'managerId']: # Remove employeeId
        if field in rec:
            del rec[field]
    if 'photoURL' in rec:
        title += f"&nbsp;<img src='{rec['photoURL']}' width=100 height=100 " \
                 + f"alt='Photo of {rec['nameFirstPreferred']}'>"
    html = f"<div class='scroll' style='height:750px'><pre>{json.dumps(rec, indent=2)}</pre></div>"
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         navbar=generate_navbar('External systems')))


@app.route('/ror/<string:rorid>')
@app.route('/ror')
def ror(rorid=None):
    ''' Show information from ROR
    '''
    if not rorid:
        return make_response(render_template('ror.html', urlroot=request.url_root,
                                             title="Search ROR", content="",
                                             navbar=generate_navbar('External systems')))
    try:
        resp = requests.get(f"https://api.ror.org/v2/organizations/{rorid}", timeout=10).json()
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning(f"Could not get ROR data for {rorid}"),
                               message=error_message(err))
    if not resp or 'errors' in resp:
        msg = '<br>'.join(resp['errors']) if 'errors' in resp else "No ROR ID found"
        return make_response(render_template('ror.html', urlroot=request.url_root,
                                             title="Search ROR",
                                             content=f"<br><h3>{msg}</h3>",
                                             navbar=generate_navbar('External systems')))
    link = f"<a href='{resp['id']}'>{resp['id']}</a>"
    html = f"<br><h3>{link}</h3>"
    for idx, name in enumerate(resp['names']):
        if 'ror_display' in name['types']:
            html += f"<h3>{resp['names'].pop(idx)['value']}</h3><br>"
            break
    if resp['names']:
        html += "<h4>Other names</h4><ul>"
        for name in resp['names']:
            html += f"<li>{name['value']}</li>"
        html += "</ul>"
    if 'relationships' in resp:
        html += "<h4>Relationships</h4><ul>"
        for rel in resp['relationships']:
            link = f"<a href='{rel['id'].split('/')[-1]}'>{rel['label']}</a>"
            html += f"<li>{rel['type']}: {link}</li>"
        html += "</ul>"
    endpoint_access()
    return make_response(render_template('ror.html', urlroot=request.url_root,
                                         title="Search ROR", content=html,
                                         navbar=generate_navbar('External systems')))

# ******************************************************************************
# * UI endpoints (Tag/affiliation)                                             *
# ******************************************************************************

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
        orgs = DL.get_supervisory_orgs(DB['dis'].suporg)
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
        link = f"<a href='/tag/{escape(tag)}'>{tag}</a>"
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
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=f"DOI tags ({len(tags):,})", html=html,
                                         navbar=generate_navbar('Tag/affiliation')))


@app.route('/dois_ack')
def dois_ack():
    ''' Show acknowledgements with counts
    '''
    payload = [{"$unwind" : "$jrc_acknowledge"},
               {"$project": {"_id": 0, "jrc_acknowledge.name": 1, "jrc_obtained_from": 1}},
               {"$group": {"_id": {"tag": "$jrc_acknowledge.name", "source": "$jrc_obtained_from"},
                           "count":{"$sum": 1}}},
               {"$sort": {"_id.tag": 1}}
              ]
    try:
        orgs = DL.get_supervisory_orgs(DB['dis'].suporg)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get supervisory orgs"),
                               message=error_message(err))
    try:
        rows = DB['dis'].dois.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get acknowledgements from " \
                                                    + "dois collection"),
                               message=error_message(err))
    html = '<table id="types" class="tablesorter numbers"><thead><tr>' \
           + '<th>Acknowledgement</th><th>SupOrg</th><th>Crossref</th><th>DataCite</th>' \
           + '</tr></thead><tbody>'
    tags = {}
    for row in rows:
        if row['_id']['tag'] not in tags:
            tags[row['_id']['tag']] = {}
        if row['_id']['source'] not in tags[row['_id']['tag']]:
            tags[row['_id']['tag']][row['_id']['source']] = row['count']
    for tag, val in tags.items():
        link = f"<a href='/tag/{escape(tag)}'>{tag}</a>"
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
                onclick = "onclick='nav_post(\"jrc_acknowledge.name\",\"" + tag \
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
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=f"DOI acknowledgements ({len(tags):,})", html=html,
                                         navbar=generate_navbar('Tag/affiliation')))


@app.route('/janelia_affiliations')
def janelia_affiliations():
    ''' Show Janelia affiliations
    '''
    payload = [{"$project": {"author": 1, "_id": 0}},
               {"$unwind": "$author"},
               {"$unwind": "$author.affiliation"},
               {"$match": {"author.affiliation.name": {"$regex": "Janelia"}}},
               {"$group": {"_id": "$author.affiliation.name", "count": {"$sum": 1}}}
              ]
    try:
        rows = DB['dis'].dois.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get affiliations " \
                                                    + "from dois collection"),
                               message=error_message(err))
    affiliations = {}
    for row in rows:
        affiliations[row['_id']] = row['count']
    payload = [{"$project": {"creators": 1, "_id": 0}},
               {"$unwind": "$creators"},
               {"$unwind": "$creators.affiliation"},
               {"$match": {"creators.affiliation": {"$regex": "Janelia"}}},
               {"$group": {"_id": "$creators.affiliation", "count": {"$sum": 1}}}
              ]
    try:
        rows = DB['dis'].dois.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get affiliations " \
                                                    + "from dois collection"),
                               message=error_message(err))
    for row in rows:
        if row['_id'] not in affiliations:
            affiliations[row['_id']] = row['count']
        else:
            affiliations[row['_id']] += row['count']
    html = '<table id="affiliations" class="tablesorter numbers"><thead><tr>' \
           + '<th>Affiliation</th><th>Count</th>' \
           + '</tr></thead><tbody>'
    for aff, count in sorted(affiliations.items(), key=lambda item: item[1], reverse=True):
        html += f"<tr><td>{aff}</td><td>{count:,}</td></tr>"
    html += '</tbody></table>'
    html = "<p> When publishing a paper, please use the following affiliation for all Janelia " \
           + "authors:<br><span style='color: lime;'>Janelia Research Campus, Howard Hughes " \
           + "Medical Institute, Ashburn, VA</span></p>" + html
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=f"DOI author affiliations ({len(affiliations):,})",
                                         html=html, navbar=generate_navbar('Tag/affiliation')))


@app.route('/orcid_tag')
def orcid_tag():
    ''' Show ORCID tags (affiliations) with counts
    '''
    payload = [{"$match": {"affiliations": {"$ne": None}}},
               {"$unwind" : "$affiliations"},
               {"$project": {"_id": 0, "affiliations": 1, "orcid": 1}},
               {"$group": {"_id": "$affiliations", "count":{"$sum": 1},
                           "orcid": {"$push": "$orcid"}}},
               {"$sort": {"_id": 1}}
              ]
    try:
        orgs = DL.get_supervisory_orgs(DB['dis'].suporg)
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
    html = "<button class=\"btn btn-outline-warning\" " \
              + "onclick=\"$('.other').toggle();\">Filter for active SupOrgs</button>"
    html += '<table id="types" class="tablesorter numbers"><thead><tr>' \
            + '<th>Affiliation</th><th>SupOrg</th><th>Authors</th><th>ORCID %</th>' \
            + '</tr></thead><tbody>'
    count = 0
    for row in rows:
        count += 1
        link = f"<a href='/tag/{escape(row['_id'])}'>{row['_id']}</a>"
        link2 = f"<a href='/tag/{escape(row['_id'])}'>{row['count']:,}</a>"
        rclass = 'other'
        if row['_id'] in orgs:
            if orgs[row['_id']]:
                if 'active' in orgs[row['_id']]:
                    org = "<span style='color: lime;'>Yes</span>"
                    rclass = 'active'
                else:
                    org = "<span style='color: yellow;'>Inactive</span>"
            else:
                org = "<span style='color: yellow;'>No code</span>"
        else:
            org = "<span style='color: red;'>No</span>"
        perc = float(f"{len(row['orcid'])/row['count']*100:.2f}")
        if perc == 100.0:
            perc = "<span style='color: lime;'>100.00%</span>"
        elif perc >= 50.0:
            perc = f"<span style='color: yellow;'>{perc}%</span>"
        else:
            perc = f"<span style='color: red;'>{perc}%</span>"
        html += f"<tr class={rclass}><td>{link}</td><td>{org}</td><td>{link2}</td>" \
                + f"<td>{perc}</td></tr>"
    html += '</tbody></table>'
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=f"Author affiliations ({count:,})", html=html,
                                         navbar=generate_navbar('Tag/affiliation')))


@app.route('/dois_top/<string:show>/<int:num>')
@app.route('/dois_top/<string:show>')
@app.route('/dois_top')
def dois_top(show="journal", num=10):
    ''' Show a chart of DOIs by top tags
    '''
    payload = [{"$unwind" : "$jrc_tag"},
               {"$match": {}},
               {"$project": {"_id": 0, "jrc_tag.name": 1, "jrc_publishing_date": 1}},
               {"$group": {"_id": {"tag": "$jrc_tag.name",
                                   "year": {"$substrBytes": ["$jrc_publishing_date", 0, 4]}},
                           "count": {"$sum": 1}},
                },
               {"$sort": {"_id.year": 1, "_id.tag": 1}}
              ]
    if show == 'journal':
        payload[1]["$match"] = {"$or": [{"type": "journal-article"}, {"subtype": "preprint"},
                                        {"types.resourceTypeGeneral": "Preprint"}]}
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
    colors = plasma(len(top))
    if len(top) <= 10:
        colors = all_palettes['Category10'][len(top)]
    elif len(top) <= 20:
        colors = all_palettes['Category20'][len(top)]
    chartscript, chartdiv = DP.stacked_bar_chart(data, f"DOIs published by year for top {num} tags",
                                                 xaxis="years", yaxis=top, width=900, height=height,
                                                 colors=colors)
    title = f"DOI tags{' for journal articles/preprints' if show =='journal' else ''} by year/tag"
    endpoint_access()
    return make_response(render_template('bokeh.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         chartscript=chartscript, chartdiv=chartdiv,
                                         navbar=generate_navbar('Tag/affiliation')))


@app.route('/projects')
@app.route('/projects/<string:option>')
def show_projects(option=None):
    ''' Show information on projects
    '''
    try:
        rows = DB['dis'].project_map.find({"project": {"$nin": ["$name"]}})
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get projects from " \
                                                    + "orcid collection"),
                               message=error_message(err))
    proj = {}
    for row in rows:
        if 'doNotUse' in row and not option:
            continue
        if row['project'] == row['name']:
            continue
        if row['project'] not in proj:
            proj[row['project']] = []
        proj[row['project']].append(row['name'])
    html = "<table id='projects' class='tablesorter standard'><thead><tr><th>Project</th>" \
           + "<th>Synonyms</th><th>Supervisory Organization</th></tr></thead><tbody>"
    cnt = 0
    _, suporgs = get_suporgs()
    for key, val in sorted(proj.items()):
        synonyms = []
        for tag in sorted(val):
            synonyms.append(f"<a href='/tag/{escape(tag)}'>{tag}</a>")
        if key in suporgs:
            status = 'Active' if suporgs[key]['active'] else 'Inactive'
            color = 'lime' if suporgs[key]['active'] else 'yellow'
        else:
            status = 'UNKNOWN'
            color = 'red'
        status = f"<span style='color:{color}'>{status}</span>"
        html += f"<tr><td><a href='/tag/{escape(key)}'>{key}</a></td>" \
                + f"<td>{', '.join(synonyms)}</td><td>{status}</td></tr>"
        cnt += 1
    html += "</tbody></table>"
    if not cnt:
        html = "<p>No projects found</p>"
    title = "Project mapping"
    if option == 'full':
        title += " (all)"
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         navbar=generate_navbar('Tags/affiliation')))


@app.route('/project/<string:name>')
def project(name):
    ''' Show information on a single project
    '''
    payload = {"$or": [{"author.name": name},
                       {"creators.name": name}]}
    try:
        rows = DB['dis'].dois.find(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get projects from " \
                                                    + "orcid collection"),
                               message=error_message(err))
    html, cnt = standard_doi_table(rows)
    if cnt:
        html = f"<p>Number of DOIs: {cnt:,}</p>" + html
    else:
        html = f"<br>No DOIs found for {name}"
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=f"Project: {name}", html=html,
                                         navbar=generate_navbar('Tags/affiliation')))


@app.route('/tag/<path:aff>/<string:year>')
@app.route('/tag/<path:aff>')
def orcid_affiliation(aff, year='All'):
    ''' Show ORCID tags (affiliations or projects) with counts
    '''
    # Authors
    payload = {"affiliations": aff}
    try:
        cnt = DB['dis'].orcid.count_documents(payload)
        if cnt:
            rows = DB['dis'].orcid.find(payload).sort("family", 1)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not find affiliations " \
                                                    + "in orcid collection"),
                               message=error_message(err))
    htmlp = get_tag_details(aff) + "<br>"
    if cnt:
        htmlp += f"<hr><p>Number of authors: <span id='totalrowsa'>{cnt:,}</span></p>"
        additional, _ = generate_user_table(rows)
        htmlp += additional
    # DOIs
    if year == 'All':
        payload = {"$or": [{"jrc_tag.name": aff},
                           {"jrc_acknowledge.name": aff},
                           {"author.name": aff},
                           {"creators.name": aff}]}
    else:
        payload = {"$and": [{"jrc_publishing_date": {"$regex": "^"+ year}},
                            {"$or": [{"jrc_tag.name": aff},
                                     {"author.name": aff},
                                     {"creators.name": aff}]}
                           ]}
    try:
        rows = DB['dis'].dois.find(payload).sort("jrc_publishing_date", -1)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not find tags " \
                                                    + "in dois collection"),
                               message=error_message(err))
    htmlp += "<hr>" + year_pulldown(f"tag/{aff}")
    #note = f" for {year}" if year != 'All' else ""
    html, cnt = standard_doi_table(rows)
    if cnt:
        html = htmlp + html
    else:
        html = f"{htmlp}<br>No DOIs found for {aff}"
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=aff,
                                         html=html,
                                         navbar=generate_navbar('Tag/affiliation')))

# ******************************************************************************
# * UI endpoints (system)                                                      *
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
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title="Database statistics", html=html,
                                         navbar=generate_navbar('System')))


@app.route('/cv')
@app.route('/cv/<string:cv>')
def cvs(cv=None):
    ''' Show CD information
    '''
    html = ""
    try:
        cnt = DB['dis'].cv.count_documents({})
        rows = DB['dis'].cv.find({}).sort("display", 1)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get cvs"),
                               message=error_message(err))
    if cnt:
        html = "<form>Select a CV to view: <select id='cv' onchange='find_cv()'>" \
               + "<option value=''>Select a CV</option>"
        display = ""
        for row in rows:
            if row['name'] == cv or cnt == 1:
                cv = row['name']
                sel = "selected"
                display = row['display']
            else:
                sel = ""
            html += f"<option value=\'{row['name']}\' {sel}>{row['display']}</option>"
    else:
        cv = rows[0]['name']
        display = rows[0]['display']
    html += "</select></form><br>"
    if cv:
        try:
            rows = DB['dis'].cv.find_one({"cv": cv})
        except Exception as err:
            return render_template('error.html', urlroot=request.url_root,
                                   title=render_warning("Could not get cv"),
                                   message=error_message(err))
        html += f"<h4>{display}</h4>"
        try:
            rows = DB['dis'].cvterm.find({"cv": cv}).sort("name", 1)
        except Exception as err:
            return render_template('error.html', urlroot=request.url_root,
                                   title=render_warning("Could not get cvterms"),
                                   message=error_message(err))
        html += '<table id="cvterms" class="tablesorter standard"><thead><tr><th>Name</th>' \
                + '<th>Display name</th><th>Definition</th><th>Format</th></tr></thead><tbody>'
        for row in rows:
            html += f"<tr><td>{row['name']}</td><td>{row['display']}</td>" \
                    + f"<td>{row['definition']}</td><td>{row['format']}</td></tr>"
        html += '</tbody></table>'
    endpoint_access()
    return make_response(render_template('cv.html', urlroot=request.url_root,
                                         title="Controlled vocabularies", html=html,
                                         navbar=generate_navbar('CV')))


@app.route('/doi_relationships')
def doi_relationships():
    ''' Show DOI relationship information
    '''
    payload = [{"$match": {"relation": {"$exists": 1}}},
               { "$addFields": {"relationship": { "$objectToArray": "$relation"}}},
               {"$unwind": "$relationship"},
               {"$group": {"_id": "$relationship.k", "count": {"$sum": 1}}},
               {"$sort": {"count": -1}}]
    try:
        rows = DB['dis'].dois.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get DOI relationships for Crossref"),
                               message=error_message(err))
    html = "<div class='flexrow'><div class='flexcol'><h3>Crossref</h3>"
    html += "<table id='crossref' class='tablesorter numbers'><thead><tr><th>Relationship</th>" \
            + "<th>Count</th></tr></thead><tbody>"
    for row in rows:
        onclick = f"onclick='nav_post(\"relation.{row['_id']}\",\"!EXISTS!\")'"
        html += f"<tr><td>{row['_id']}</td><td><a href='#' {onclick}>{row['count']}</a></td></tr>"
    html += "</tbody></table></div>"
    payload = [{"$match": {"relatedIdentifiers": {"$exists": 1}}},
               {"$unwind": "$relatedIdentifiers"},
               {"$group": {"_id": "$relatedIdentifiers.relationType", "count": {"$sum": 1}}},
               {"$sort": {"count": -1}}]
    try:
        rows = DB['dis'].dois.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get DOI relationships for DataCite"),
                               message=error_message(err))
    html += "<div class='flexcol' style='margin-left: 25px'><h3>DataCite</h3>"
    html += "<table id='datacite' class='tablesorter numbers'><thead><tr><th>Relationship</th>" \
            + "<th>Count</th></tr></thead><tbody>"
    for row in rows:
        onclick = "onclick='nav_post(\"relatedIdentifiers.relationType\",\"" + row['_id'] + "\")'"
        html += f"<tr><td>{row['_id']}</td><td><a href='#' {onclick}>{row['count']}</a></td></tr>"
    html += "</tbody></table></div></div>"
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title="DOI relationships", html=html,
                                         navbar=generate_navbar('DOI')))


@app.route('/stats_endpoints')
def stats_endpoints():
    ''' Show endpoint stats
    '''
    try:
        rows = DB['dis'].api_endpoint.find().sort("endpoint", 1)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get endpoint stats"),
                               message=error_message(err))
    html = '<table class="tablesorter numbercenter"><thead><tr><th>Endpoint</th>' \
           + '<th>Count</th></tr></thead><tbody>'
    for row in rows:
        html += f"<tr><td>{row['endpoint']}</td><td>{row['count']}</td></tr>"
    html += '</tbody></table>'
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title="Endpoint access counts", html=html,
                                         navbar=generate_navbar('System')))

# ******************************************************************************
# * Multi-role endpoints (ORCID)                                               *
# ******************************************************************************

@app.route('/labs')
def show_labs():
    '''
    Show group owners (labs) from ORCID
    Return records whose ORCIDs have a group
    ---
    tags:
      - ORCID
    responses:
      200:
        description: labs
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
            if 'employeeId' in row: # Remove employeeId
                del row['employeeId']
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
        name = ' '.join([row['given'][0], row['family'][0]])
        if 'userIdO365' in row:
            name = f"<a href='/userui/{row['userIdO365']}'>{name}</a>"
        elif 'orcid' in row:
            name = f"<a href='/userui/{row['orcid']}'>{name}</a>"
        if 'alumni' in row and row['alumni']:
            name += (f" {tiny_badge('alumni', 'Former employee')}")
        try:
            grow = DB['dis'].suporg.find_one({"name": row['group']})
        except Exception:
            grow = None
        glink = f"<a href='/tag/{row['group']}'>{row['group']}</a>" if grow else row['group']
        html += f"<tr><td>{name}</td>" \
                + f"<td style='width: 180px'>{row['orcid'] if 'orcid' in row else ''}</td>" \
                + f"<td>{glink}</td><td>{', '.join(row['affiliations'])}</td></tr>"
    html += '</tbody></table>'
    endpoint_access()
    return render_template('general.html', urlroot=request.url_root, title=f"Labs ({count:,})",
                           html=html, navbar=generate_navbar('ORCID'))

# *****************************************************************************

if __name__ == '__main__':
    if app.config["RUN_MODE"] == 'dev':
        app.run(debug=app.config["DEBUG"])
    else:
        app.run(debug=app.config["DEBUG"])
