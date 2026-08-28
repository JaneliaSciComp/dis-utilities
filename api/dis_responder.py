''' dis_responder.py
    UI and REST API for Data and Information Services
'''

import collections
import copy
from datetime import date, datetime, timedelta
from html import escape
import inspect
import itertools
from io import BytesIO
import json
from json import JSONEncoder
from math import pi
from operator import itemgetter
import os
import random
import re
import socket
import statistics
import string
import sys
from time import sleep, time
from types import SimpleNamespace
from urllib.parse import quote, unquote
import concurrent.futures
import dateutil.parser
import dateutil.tz
import bson
from flask import (Flask, make_response, render_template, request, jsonify, redirect, send_file)
from flask_cors import CORS
from apispec import APISpec
from apispec.yaml_utils import load_yaml_from_docstring
import pandas as pd
from pymongo.collation import Collation, CollationStrength
import requests
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL
import dis_plots as DP
from dis_html import (DOWNLOAD_ICON, add_jrc_fields, add_subjects, cell,
                      create_downloadable, dloop, doi_link, fcell,
                      generate_navbar, get_license, make_link,
                      oa_status_rank, registrar_switch, render_table, render_warning,
                      safe, see_also, stat_cards, tab_button, tab_pane, tiny_badge,
                      two_col, year_pulldown as _year_pulldown)
from dis_config import (ARTICLES, DATACITE, DOI, DO_NOT_DISPLAY, EMAIL,
                        EPT_ONE, EPT_TWO, LIBRARY, NCBI_MESH, OAREPORT,
                        OPENALEX, ORCID, PMCID, PMID, PREFERRED_AFF,
                        REPOSITORY, ROR, S2, S2_GRAPH, SOURCES, WORKDAY)
from dis_state import CVTERM, PROJECT

# pylint: disable=broad-exception-caught,broad-exception-raised,too-many-lines,too-many-locals,too-many-return-statements,too-many-branches,too-many-statements

__version__ = "120.22.4"
# Database
DB = {}
INSENSITIVE = Collation(locale='en', strength=CollationStrength.PRIMARY)
# Custom queries
CUSTOM_REGEX = {"publishing_year": {"field": "jrc_publishing_date",
                                    "value": "^!REPLACE!"}}
JOURNAL_ARTICLE = {"$or": [{"type": "journal-article"}, {"subtype": "preprint"},
                           {"types.resourceTypeGeneral": "Preprint"}]}
# HTML / CSS styles
HIGHLIGHT = "style='background-color:#00a450 !important; color:white !important'"
# Global
BOLD = "<span style='font-weight: bold'>"
ITALIC = "<span style='font-style: italic'>"

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


@app.url_defaults
def _static_cache_bust(endpoint, values):
    ''' Append a static file's last-modified time as a ?v= query param to every
        url_for('static', ...) URL, so a browser refetches an asset whenever it
        actually changes. This is automatic: no template edits and no coupling to
        __version__ - the param is derived from the file's mtime, so it changes
        exactly when the file does (and only then).
        Keyword arguments:
          endpoint: the endpoint being built a URL for
          values: the URL values dict (mutated in place to add 'v')
        Returns:
          None
    '''
    if endpoint == 'static' and 'filename' in values:
        try:
            fpath = os.path.join(app.static_folder, values['filename'])
            values['v'] = int(os.stat(fpath).st_mtime)
        except OSError:
            pass


def _load_config_ns(name):
    """Load config as SimpleNamespace (replaces JRC.get_config)"""
    config_path = os.path.join('/config', f'{name}.json')
    if not os.path.exists(config_path):
        config_path = os.path.join('config', f'{name}.json')
    with open(config_path) as f:
        data = json.load(f)
    return json.loads(json.dumps(data), object_hook=lambda d: SimpleNamespace(**d))


@app.before_request
def before_request():
    ''' Set transaction start time and increment counters.
        If needed, initilize global variables.
    '''
    if not DB:
        print("Initializing global variables")
        if "DIS_MONGO_URI" not in os.environ:
            return render_template('warning.html', urlroot=request.url_root,
                                   title=render_warning("Config error"),
                                   message="Missing environment variable DIS_MONGO_URI")
        dbo = SimpleNamespace(type="mongo",
                              uri=os.environ.get("DIS_MONGO_URI"),
                              client=os.environ.get("DIS_MONGO_DATABASE", "dis"))
        app.config["dis"] = JRC.simplenamespace_to_dict(_load_config_ns("dis"))
        print(f"Connecting to {dbo.client} prod")
        try:
            dis_db = JRC.connect_database(dbo)
        except Exception as err:
            return render_template('warning.html', urlroot=request.url_root,
                                   title=render_warning("Database connect error"), message=err)
        # Build CVTERM/PROJECT into locals and commit (along with DB['dis']) only
        # after every read succeeds. This keeps init atomic: a transient failure
        # here leaves DB empty so the next request retries, instead of leaving the
        # app permanently running with an unpopulated CVTERM.
        try:
            local_cvterm = {}
            rows = dis_db['cvterm'].find({})
            for row in rows:
                if row['cv'] not in local_cvterm:
                    local_cvterm[row['cv']] = {}
                local_cvterm[row['cv']][row['name']] = row
            local_project = {}
            rows = dis_db.project_map.find({"doNotUse": {"$exists": False}})
            for row in rows:
                local_project[row['name']] = True
                local_project[row['project']] = True
        except Exception as err:
            return render_template('warning.html', urlroot=request.url_root,
                                   title=render_warning("Database error"), message=err)
        CVTERM.update(local_cvterm)
        PROJECT.update(local_project)
        DB['dis'] = dis_db
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


def year_pulldown(prefix, all_years=True, suffix='', start_year=2006, query=False,
                  selected=None):
    ''' Request-aware wrapper around dis_html.year_pulldown. When the caller
        doesn't pass an explicit `selected`, derive the currently-active year
        from the request URL (the path segment after the prefix, or ?year= in
        query mode) so the button shows it after the caret. Falls back to "All"
        (all_years) or the current year when no year is present in the URL.
        dis_html stays request-agnostic; this app-layer shadow adds the request
        coupling. Endpoints whose default isn't "All"/current-year (e.g. the
        preprint pages) pass `selected` explicitly.
        Keyword arguments:
          prefix, all_years, suffix, start_year, query: as dis_html.year_pulldown
          selected: explicit value to show (skips the request-based derivation)
        Returns:
          Pulldown HTML
    '''
    if selected is None:
        if query:
            yr = request.args.get('year')
        else:
            path = request.path.strip('/')
            pfx = prefix.strip('/')
            yr = None
            if path.startswith(pfx + '/'):
                # The year is always the first path segment after the prefix (links
                # are built as {prefix}/{year}{suffix}), so take it directly. Avoids
                # the old suffix-stripping, which was case-/format-fragile - e.g.
                # top_entities' "/Crossref/10" suffix vs a lowercased URL segment.
                yr = path[len(pfx) + 1:].split('/')[0] or None
        if not yr:
            yr = 'All' if all_years else str(datetime.now().year)
        selected = "(all years)" if yr == 'All' else yr
    return _year_pulldown(prefix, all_years=all_years, suffix=suffix,
                          start_year=start_year, query=query, selected=selected)


def tag_pulldown(prefix, year='All', selected=None):
    ''' Generate a single-tag pulldown (the distinct jrc_tag.name values in the
        dois collection). Each option keeps the current publishing-year path
        segment and toggles a ?tag=<name> query param; "All tags" clears it.
        Mirrors year_pulldown's selected-button style so the two filters read as
        a matched pair, and carries a sticky typeahead box that live-filters the
        list (see filterTagPulldown + the shown.bs.dropdown focus handler in dis.js).
        Keyword arguments:
          prefix: navigation prefix (e.g. 'source_metrics')
          year: the currently-selected year, preserved in each tag link
          selected: the currently-selected tag, or None for "all tags"
        Returns:
          Pulldown HTML
    '''
    try:
        tags = sorted((t for t in DB['dis'].dois.distinct("jrc_tag.name") if t),
                      key=str.lower)
    except Exception:
        tags = []
    base = f"/{prefix}" if (not year or year == 'All') else f"/{prefix}/{year}"
    shown = selected if selected else "(all tags)"
    btn_label = ("<span style='opacity:0.85;'>Tag</span> "
                 "<span style='opacity:0.85;'>&#9662;</span> "
                 "<span style='opacity:0.5;'>|</span> "
                 f"<span style='color:#ffffff;font-weight:700;'>{escape(shown)}</span>")
    html = "<div class='dropdown'><button type='button' class='btn btn-info' " \
           + "style='min-width:240px;' data-toggle='dropdown' aria-haspopup='true' " \
           + f"aria-expanded='false'>{btn_label}</button>" \
           + "<div class='dropdown-menu' style='max-height:460px; overflow-y:auto;'>"
    # Sticky typeahead box. Clicks/keys inside an <input> in a BS4 dropdown-menu
    # don't dismiss the menu, so the list filters in place as you type.
    html += "<div style='position:sticky; top:0; background:#fff; z-index:2; " \
            + "padding:6px 12px 8px 12px;'>" \
            + "<input type='text' class='form-control form-control-sm " \
            + "tag-pulldown-search-box' placeholder='Filter tags…' " \
            + "autocomplete='off' oninput='filterTagPulldown(this)' " \
            + "onclick='event.stopPropagation()'></div>"
    html += f"<a class='dropdown-item{'' if selected else ' active'}' href='{base}'>" \
            + "All tags</a><div class='dropdown-divider'></div>"
    for tag in tags:
        cls = 'dropdown-item active' if tag == selected else 'dropdown-item'
        html += f"<a class='{cls} tag-pulldown-item' " \
                + f"href='{base}?tag={quote(tag, safe='')}'>{escape(tag)}</a>"
    html += "<span class='dropdown-item disabled tag-pulldown-empty' " \
            + "style='display:none;'>No matching tags</span>"
    html += "</div></div>"
    return html


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


# ******************************************************************************
# * HTML utility functions                                                     *
# ******************************************************************************


# ******************************************************************************
# * Navigation utility functions                                               *
# ******************************************************************************


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
    if request.method == 'GET' and request.args:
        return request.args.to_dict()
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
    elif ipd['value'] == "!NOTEXISTS!":
        ipd['value'] = {"$exists": 0}
    if ipd.get('title'):
        fdisplay = ipd['title']
    else:
        fdisplay = CVTERM['jrc'][ipd['field']]['display'] if ipd['field'] in CVTERM['jrc'] \
                   else ipd['field']
    ptitle = f"DOIs for {fdisplay} {display_value}"
    payload = {ipd['field']: ipd['value']}
    if ipd.get('jrc_obtained_from'):
        payload['jrc_obtained_from'] = ipd['jrc_obtained_from']
        ptitle += f" from {ipd['jrc_obtained_from']}"
    return payload, ptitle

# ******************************************************************************
# * Plotting utility functions                                                 *
# ******************************************************************************

def provider_pie_chart(provider, pyear):
    ''' Create a pie chart for a provider
        Keyword arguments:
          provider: provider name
          pyear: year
        Returns:
          Pie chart components
    '''
    data = {}
    # Pie chart
    #    payload = [{"$project": {"costArray": {"$objectToArray": "$cost" },
    #                             "provider": "$provider"}},
    #                            {"$unwind": "$costArray"},
    #                            {"$group": {"_id": {"provider": "$provider",
    #                                                "year": "$costArray.k"},
    #                                        "totalCost": {"$sum": {"$toDouble": "$costArray.v"}}}},
    #                            {"$match": {"_id.year": str(pyear)}},
    #                            {"$sort": {"totalCost": -1}}]
    payload = [{"$match": {"provider": provider, "cost": {"$exists": True}}},
    {"$addFields": {"costArray": {"$objectToArray": "$cost"}}},
    {"$addFields": {"maxYear": {"$max": "$costArray.k"}}},
    {"$addFields": {"recentCost":
                    {"$toDouble":
                     {"$arrayElemAt":
                      [{"$map":
                        {"input":
                         {"$filter":
                          {"input": "$costArray", "as": "e",
                           "cond": {"$eq": ["$$e.k", "$maxYear"]}}},
                         "as": "e", "in": "$$e.v"}}, 0]}}}},
    {"$group": {"_id": "$type", "totalCost": {"$sum": "$recentCost"},
                "mostRecentYear": {"$max":"$maxYear"}}},
    {"$sort": {"totalCost": -1}}]
    rows = DB['dis'].subscription.aggregate(payload)
    for row in rows:
        data[row['_id']] = row['totalCost']
    if len(data) > 1:
        colors = DP.get_colors_by_count(len(data))
        piescript, piediv = DP.pie_chart(data, f'Subscription costs by type for {pyear}',
                                         'provider', colors=colors,
                                         width=650, height=450, location="top_right", fmt='{$0,0}')
        return piescript, piediv
    return None, None


def provider_title_heat_map(provider):
    ''' Create a heat map of title costs by year for a provider
        Keyword arguments:
          provider: provider name
        Returns:
          Heat map components
    '''
    pipeline = [
        {"$match": {"provider": provider, "cost": {"$exists": True}}},
        {"$project": {"title": 1, "costArray": {"$objectToArray": "$cost"}}},
        {"$unwind": "$costArray"},
        {"$group": {"_id": {"title": "$title", "year": "$costArray.k"},
                    "totalCost": {"$sum": {"$toDouble": "$costArray.v"}}}},
        {"$sort": {"_id.year": 1, "_id.title": 1}}
    ]
    rows = DB['dis'].subscription.aggregate(pipeline, collation=INSENSITIVE)
    data = {'Year': [], 'Title': [], 'Cost': []}
    for row in rows:
        data['Year'].append(row['_id']['year'])
        data['Title'].append(row['_id']['title'])
        data['Cost'].append(row['totalCost'])
    if not data['Year'] or data['Title'][0] == data['Title'][-1]:
        return None, None
    chartscript, chartdiv = DP.heat_map(data,
                                        f'Subscription costs by title and year for {provider}',
                                        x_field='Year', y_field='Title', value_field='Cost')
    return chartscript, chartdiv


def provider_heat_map():
    ''' Create a heat map for providers that have data after two years ago
    Keyword arguments:
      None
    Returns:
      Heat map components
    '''
    errmsg = "Could not get max year data from subscription collection"
    two_years_ago = str(datetime.now().year - 2)
    payload = [{"$match": {"cost": {"$exists": True}}},
    {"$addFields": {"costYears": {"$objectToArray": "$cost"}}},
    {"$unwind": "$costYears"},
    {"$group": {"_id": "$provider", "maxYear": {"$max": "$costYears.k"}}},
    {"$match": {"maxYear": {"$lte": two_years_ago}}},
    {"$project": {"_id": 1}}
    ]
    try:
        rows = DB['dis'].subscription.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning(errmsg),
                               message=error_message(err))
    rows = list(rows)
    timed_out = [list(prov.values())[0] for prov in rows]
    pipeline = [
        {"$match": {"cost": {"$exists": True}}},
        {"$project": {"provider": 1, "costArray": {"$objectToArray": "$cost"}}},
        {"$unwind": "$costArray"},
        {"$group": {"_id": {"provider": "$provider", "year": "$costArray.k"},
                    "totalCost": {"$sum": {"$toDouble": "$costArray.v"}}}},
        {"$sort": {"_id.year": 1, "_id.provider": 1}}
    ]
    rows = DB['dis'].subscription.aggregate(pipeline, collation=INSENSITIVE)
    data = {'Year': [], 'Provider': [], 'Cost': []}
    for row in rows:
        if row['_id']['provider'] in timed_out:
            continue
        data['Year'].append(row['_id']['year'])
        data['Provider'].append(row['_id']['provider'])
        data['Cost'].append(row['totalCost'])
    if not data['Year']:
        return render_template('error.html', urlroot=request.url_root,
                               title='No costs found',
                               message="No subscription cost data found")
    chartscript, chartdiv = DP.heat_map(data, 'Subscription costs by provider and year',
                                        x_field='Year', y_field='Provider', value_field='Cost')
    return chartscript, chartdiv

# ******************************************************************************
# * ORCID utility functions                                                    *
# ******************************************************************************

def get_single_author_abbrev(auth, source):
    ''' Get a single author abbreviation
        Keyword arguments:
          auth: author record
          source: source of the record
        Returns:
          Author abbreviation (string)
    '''
    name = ""
    if source == 'DataCite':
        if 'familyName' not in auth:
            if 'name' in auth:
                name = auth['name']
            else:
                return ""
        else:
            name = auth['familyName']
            if 'givenName' in auth:
                name = f"{name}, {auth['givenName'][0]}."
        return name
    if 'family' not in auth:
        if 'name' in auth:
            name = auth['name']
        else:
            return ""
    else:
        name = auth['family']
        if 'given' in auth:
            name = f"{name}, {auth['given'][0]}."
    return name


def author_population(rec, limit=10):
    ''' Get a population of authors for one work
        Keyword arguments:
          rec: DOI record
          limit: limit of [first] authors to return
        Returns:
          Author list (string)
    '''
    cnt = 0
    if rec['jrc_obtained_from'] == 'DataCite':
        field = 'creators'
    else:
        field = 'author'
    if field not in rec:
        return ""
    arec = []
    for auth in rec[field]:
        auth_name = get_single_author_abbrev(auth, rec['jrc_obtained_from'])
        if not auth_name:
            continue
        arec.append(auth_name)
        cnt += 1
        if cnt == limit:
            break
    if len(arec) < len(rec[field]):
        auth_name = get_single_author_abbrev(rec[field][-1], rec['jrc_obtained_from'])
        if auth_name:
            arec.append(auth_name)
    return ", ".join(arec)


def get_author_works(orc, line):
    ''' Get the works for an author
        Keyword arguments:
          orc: ORCID record
          line: list of author information
        Returns:
          List of works
          Number of works
    '''
    works_blank = ['No works found', '', '', '', '', '', '', '', '', '']
    works = []
    try:
        if 'employeeId' in orc:
            rows = DB['dis'].dois.find({"jrc_author": orc['employeeId']})
        else:
            rows = []
    except Exception:
        rows = []
    if not rows:
        wrk = line.copy()
        wrk.extend(works_blank)
        works.append("\t".join(wrk))
        return works, 0
    cnt = 0
    for row in rows:
        wrk = line.copy()
        if 'subtype' not in row:
            row['subtype'] = ''
        if 'types' in row and 'resourceTypeGeneral' in row['types']:
            row['type'] = row['types']['resourceTypeGeneral']
        pub = ""
        if row['type'] in ['journal-article', 'book-chapter', 'proceedings-article', 'Preprint'] \
           or row['subtype'] == 'preprint':
            pub = 'Yes'
        first = last = ""
        if 'jrc_first_id' in row and orc['employeeId'] in row['jrc_first_id']:
            first = 'Yes'
        if 'jrc_last_id' in row and orc['employeeId'] == row['jrc_last_id']:
            last = 'Yes'
        wrk.extend([row['doi'], pub, row['type'] if 'type' in row else '',
                    row['subtype'] if 'subtype' in row else '', first, last,
                    row['jrc_publishing_date'], DL.get_journal(row, full=False, name_only=True),
                    DL.get_title(row), author_population(row)])
        works.append("\t".join(wrk))
        cnt += 1
    return works, cnt


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
    row = DB['dis'].org_group.find_one(payload)
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
    rows = DB['dis'].dois.find(payload)
    for row in rows:
        if DL.is_journal(row) and not DL.is_version(row):
            finds['janelia'].append(row['jrc_publishing_date'])
    payload['jrc_tag.name'] = {"$in": shared}
    rows = DB['dis'].dois.find(payload)
    for row in rows:
        if DL.is_journal(row) and not DL.is_version(row):
            finds['org'].append(row['jrc_publishing_date'])
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


def generate_works_table(rows, name=None, show="full", eid=None):
    ''' Generate table HTML for a person's works. Renders through standard_doi_table so
        the columns (Published | DOI | Journal | Title), version-filter toggle, TSV
        download, and count match every other DOI list in the app; a green checkmark is
        prefixed to the DOI of works the person authored while a Janelia employee.
        Keyword arguments:
          rows: rows from dois collection
          name: search key [optional] - when set, matching author names are listed above
          show: show full, or journal/preprint only
          eid: employee ID (drives the green-checkmark marker)
        Returns:
          HTML and a list of DOIs
    '''
    works = []
    dois = []
    authors = {}
    for row in rows:
        if show == "journal" and not (("type" in row and row['type'] == "journal-article") \
                                  or ("types" in row and "resourceTypeGeneral" in row["types"] \
                                      and row["types"]["resourceTypeGeneral"] == "Preprint") \
                                  or ("subtype" in row and row['subtype'] == "preprint")):
            continue
        dois.append(row['doi'])
        works.append(row)
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
        return "", []
    works.sort(key=DL.get_publishing_date, reverse=True)
    def mark_fn(row):
        ''' Green checkmark for works the person authored while a Janelia employee, else
            four non-breaking spaces to keep the DOIs left-aligned. Only meaningful when an
            eid is known; a name search (eid=None) has no single employee, so no marker. '''
        if not eid:
            return ''
        if 'jrc_author' in row and eid in row['jrc_author']:
            return "<i class='fa-solid fa-circle-check' style='color: lime'></i> "
        return "&nbsp;&nbsp;&nbsp;&nbsp;"
    table, _, _ = standard_doi_table(works, mark_fn=mark_fn, download_name='works')
    if authors:
        table = f"<br>Authors found: {', '.join(sorted(authors.values()))}<br>" \
                + f"This may include non-Janelia authors<br>{table}"
    # The green-checkmark preamble only makes sense when an eid drives the checks (the
    # person pages /orcidui and /userui). A name search (eid=None, e.g. /doisui_name) has
    # no single employee, so no check ever renders - show no preamble then, rather than
    # describing a checkmark that isn't there.
    if eid:
        preamble = f'''
    The works below were searched for using this author's name and ORCID. A green checkmark
    (<i class='fa-solid fa-circle-check' style='color: lime'></i>) indicates that automated or
    manual curation has determined that the author contributed to the publication while they were
    a Janelia employee. It is not uncommon for check marks to not appear for former employees. If
    you have a publication below without a check mark, it's most likely that affiliation or ORCID
    information was not provided to Crossref/DataCite. If one of your publications doesn't have a
    check (or is missing), please email the DOI to the Library at {LIBRARY}.
    '''
        html = f"<hr>{preamble}{table}"
    else:
        html = table
    return html, dois


def add_orcid_controls(orc, html):
    ''' Add ORCID and People controls to HTML
        Keyword arguments:
          orc: orcid record
          html: HTML
        Returns:
          HTML
    '''
    if 'orcid' in orc:
        olink = f"/orcidapi/{orc['orcid']}"
        html += f" {tiny_badge('info', 'Show ORCID data', olink)}"
        try:
            olink = f"{OPENALEX}authors?filter=orcid:{orc['orcid']}" \
                    + f"&mailto={EMAIL}"
            oa_params = {}
            if os.environ.get('OPENALEX_API_KEY'):
                oa_params['api_key'] = os.environ['OPENALEX_API_KEY']
            resp = requests.get(olink, params=oa_params, timeout=5)
            if resp.status_code == 200:
                html += f" {tiny_badge('info', 'Show OpenAlex data', olink)}"
        except Exception:
            pass
    if 'userIdO365' in orc:
        olink = f"/peoplerec/{orc['userIdO365']}"
        html += f" {tiny_badge('info', 'Show People data', olink)}"
    return html


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
    html += f"<tr><td>Given name:</td><td>{', '.join(sorted(orc['given']))}</td></tr>"
    html += f"<tr><td>Family name:</td><td>{', '.join(sorted(orc['family']))}</td></tr>"
    if 'orcid' in orc:
        cpaste = " <button style='background-color:transparent;border:none;' " \
                 + f"onclick=\"copyText('{orc['orcid']}')\">" \
                 + "<i class='fas fa-regular fa-copy shadow' " \
                 + "style='background-color:transparent'></i></button>"
        html += f"<tr><td>ORCID:</td><td><a href='{ORCID}{orc['orcid']}'>" \
                + f"{orc['orcid']}</a>{cpaste}</td></tr>"
    if 'userIdO365' in orc:
        link = "<a href='" + f"{WORKDAY}{orc['userIdO365']}" \
               + f"' target='_blank'>{orc['userIdO365']}</a>"
        html += f"<tr><td>User ID:</td><td>{link}</td></tr>"
    if 'affiliations' in orc:
        alinks = ', '.join(f"<a href='/tag/{requests.utils.quote(a)}'>{a}</a>"
                           for a in orc['affiliations'])
        html += f"<tr><td>Affiliations:</td><td>{alinks}</td></tr>"
    html += "</table><br>"
    html = add_orcid_controls(orc, html)
    html += "<br>"
    if use_eid:
        oid = orc['employeeId']
    rows = get_dois_for_orcid(oid, orc)
    eid = orc['employeeId'] if 'employeeId' in orc else None
    tablehtml, dois = generate_works_table(rows, name=None, show=show, eid=eid)
    sad = DL.get_single_author_details(orc, DB['dis'].orcid)
    if tablehtml:
        html = f"{' '.join(get_badges(sad, True))}{html}{tablehtml}"
    else:
        html = f"{' '.join(get_badges(sad, True))}{html}" \
               + render_warning("No works were found in the dois collection.", 'warning')
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
                     + f"<td>{render_title_html(wsumm['title']['title']['value'])}</td></tr>"
            continue
        link = ""
        if work['external-ids']['external-id'][0]['external-id-url']:
            if work['external-ids']['external-id'][0]['external-id-url']:
                link = "<a href='" \
                       + work['external-ids']['external-id'][0]['external-id-url']['value'] \
                       + f"' target='_blank'>{doi}</a>"
        else:
            link = f"{DOI}{doi}"
            link = f"<a href='{link}' target='_blank'>{doi}</a>"
        inner += f"<tr><td>{pdate}</td><td>{link}</td>" \
                 + f"<td>{render_title_html(wsumm['title']['title']['value'])}</td></tr>"
        results.append({"date": pdate, "doi": doi, "title": wsumm['title']['title']['value']})
    if inner:
        title = "title is" if works == 1 else f"{works} titles are"
        html += f"<hr>The additional {title} from ORCID. Note that titles below may " \
                + "be self-reported, may not have DOIs available, or may be from the author's " \
                + "employment outside of Janelia.</br>"
        html += '<table id="works" class="tablesorter standard-scroll"><thead><tr>' \
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
    if not endpoint:
        endpoint = str(request.url_rule)
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
    if endpoint.startswith('?'):
        return
    if '/' in endpoint:
        if endpoint.startswith('10.'):
            endpoint = 'doiui'
        else:
            eroot = endpoint.split('/')[0]
            parts = endpoint.split('/', 2)
            first_two = '/'.join(parts[:2])
            if first_two in EPT_TWO:
                endpoint = first_two
            if eroot in EPT_ONE:
                endpoint = eroot
    coll = DB['dis'].api_endpoint_log
    try:
        coll.insert_one({"endpoint": endpoint, "timestamp": datetime.now()})
    except Exception:
        pass


def generate_user_table(rows, download=None):
    ''' Build the canonical "list of Janelians" table: one row per person with the
        ORCID (linked to the canonical /userui route), given/family name(s),
        affiliations (as /tag/ links, matching the person page), and a Status column
        of badges - so former-employee status is a badge, not a literal "YES" column.
        A "Filter for current authors" toggle hides former employees. Used by the
        org-authors, affiliation, and name-search pages.
        Keyword arguments:
          rows: rows from the orcid collection
          download: optional base filename; when given, a TSV download button (ORCID,
                    name, affiliations, former-employee flag) is prepended
        Returns:
          (html, count) tuple
    '''
    count = 0
    trows = []
    row_classes = []
    dl_rows = []
    for row in rows:
        count += 1
        if row.get('orcid'):
            # Canonical person route is /userui; it resolves an ORCID string via the
            # orcid field (no '@' -> use_eid=False), so author links land on the same
            # page as author links elsewhere in the app (UI-audit row 226).
            link = f"<a href='/userui/{row['orcid']}'>{row['orcid']}</a>"
        elif row.get('userIdO365'):
            link = f"<a href='/userui/{row['userIdO365']}'>No ORCID found</a>"
        else:
            link = f"<a href='/unvaluserui/{row['_id']}'>No ORCID found</a>"
        given = ', '.join(sorted(row.get('given', [])))
        family = ', '.join(sorted(row.get('family', [])))
        # Affiliations = affiliations + managed orgs, deduped/sorted, each a /tag/ link.
        affs = sorted(set(list(row.get('affiliations', [])) + list(row.get('managed', []))))
        affil_html = ', '.join(f"<a href='/tag/{quote(a, safe='')}'>{escape(a)}</a>"
                               for a in affs)
        auth = DL.get_single_author_details(row, DB['dis'].orcid)
        is_alum = bool(auth and auth.get('alumni'))
        badges = get_badges(auth, True)
        row_classes.append('other' if is_alum else 'active')
        trows.append([safe(link), given, family, safe(affil_html), safe(' '.join(badges))])
        dl_rows.append([row.get('orcid', ''), given, family, ', '.join(affs),
                        'Yes' if is_alum else ''])
    header = ['ORCID', 'Given name(s)', 'Family name(s)', 'Affiliations', 'Status']
    table = render_table(header, trows, table_id='ops', row_classes=row_classes)
    cbutton = "<button class=\"btn btn-outline-warning\" " \
              + "onclick=\"toggler('ops', 'other', 'totalrowsa');\">" \
              + "Filter for current authors</button>"
    html = cbutton + table
    if download:
        dl_header = ['ORCID', 'Given name(s)', 'Family name(s)', 'Affiliations',
                     'Former employee']
        content = ''.join('\t'.join(r) + '\n' for r in dl_rows)
        html = create_downloadable(download, dl_header, content) + html
    return html, count

# ******************************************************************************
# * DOI utility functions                                                      *
# ******************************************************************************


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


def get_oa_year_counts():
    ''' Get open access year counts
    '''
    payload = [{'$match': {'jrc_is_oa': {'$exists': True}}},
               {'$project': {'year': {'$substr': ['$jrc_publishing_date', 0, 4]},
                             'doi': '$doi', 'status': '$jrc_oa_status'}},
               {'$group': {'_id': {'year': '$year', 'status': '$status'}, 'count': {'$sum': 1}}},
               {'$sort': {'_id.year': 1}}
              ]
    rows = DB['dis'].dois.aggregate(payload)
    return rows


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


def is_ignored(doi):
    ''' Check if a DOI is ignored
        Keyword arguments:
          doi: DOI
        Returns:
          True if the DOI is ignored, False otherwise
    '''
    row = DB['dis'].to_ignore.find_one({"type": "doi", "key": doi})
    return row is not None


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
    if isinstance(row.get('created'), dict) and row['created'].get('date-time'):
        this = str(row['created']['date-time']).split('T', maxsplit=1)[0]
        if last:
            date_list.append(get_separator(last, this))
        last = this
        date_list.append(f"{row['jrc_obtained_from']} {this}")
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
        date_list.append(f"<span style='color: limegreen;'>Newsletter {this}</span>")
    if updated:
        this = updated
        if last:
            date_list.append(get_separator(last, this))
        last = this
        date_list.append(f"Updated {this}")
    if date_list:
        return f"<span class='paperdata'>{''.join(date_list)}</span>"
    return ""


def get_legal_information(row):
    ''' Get legal information from a row
        Keyword arguments:
          row: DOI record
        Returns:
          HTML
    '''
    ltext = ""
    if row.get('jrc_license'):
        ltext = f"<h4>License</h4>{get_license(row['jrc_license'])}"
    return ltext


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
                           "type": {"$in": ARTICLES}}},
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
    for key in ARTICLES:
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


def get_first_last_authors(year, tag=None):
    ''' Get first and last author counts
        Keyword arguments:
          year: year to get counts for
          tag: optional jrc_tag.name to scope to
        Returns:
          First and last author counts
    '''
    tmatch = {"jrc_tag.name": tag} if tag else {}
    stat = {'first': {}, 'last': {}, 'any': {}}
    for which in ("first", "last", "any"):
        if which == 'any':
            payload = [{"$match": {"jrc_publishing_date": {"$regex": "^"+ year},
                                   "jrc_author": {"$exists": True}, **tmatch}},
                       {"$group": {"_id": {"type": "$type", "subtype": "$subtype",
                                           "DataCite": "$types.resourceTypeGeneral"},
                                   "count": {"$sum": 1}}}
                      ]
        else:
            payload = [{"$match": {"jrc_publishing_date": {"$regex": "^"+ year},
                                   f"jrc_{which}_author": {"$exists": True}, **tmatch}},
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
        cnt = DB['dis'].dois.count_documents(val)
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


def get_source_data(year, tag=None):
    ''' Get DOI data by source and type/subtype or resourceTypeGeneral
        Keyword arguments:
          year: year to get data for
          tag: optional jrc_tag.name to scope to
        Returns:
          Data dictionary and html dictionary
    '''
    # Crossref
    if year != 'All':
        match = {"jrc_obtained_from": "Crossref",
                 "jrc_publishing_date": {"$regex": "^"+ year}}
    else:
        match = {"jrc_obtained_from": "Crossref"}
    if tag:
        match["jrc_tag.name"] = tag
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
        key = "_".join([row['_id']['source'], row['_id']['type'], row['_id']['subtype']])
        if key not in hdict:
            hdict[key] = 0
        hdict[key] += row['count']
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
    url = f"{S2_GRAPH}paper/DOI:{doi}?fields=citationCount"
    headers = {'x-api-key': os.environ.get('S2_API_KEY')}
    try:
        resp = requests.get(url, headers=headers, timeout=5)
        if resp.status_code == 429:
            raise Exception("Rate limit exceeded")
        if resp.status_code != 200:
            return 0
        data = resp.json()
        if fmt == 'html' and data['citationCount']:
            cnt = f"<a href='{S2}{data['paperId']}' target='_blank'>" \
                  + f"{data['citationCount']}</a>"
        else:
            cnt = data['citationCount']
        return cnt
    except Exception:
        return 0


def highlight_subtext(text, subtext, is_regex=False):
    ''' Highlight a subtext in a text
        Keyword arguments:
          text: text to highlight
          subtext: subtext to highlight (treated as a regex if is_regex is True)
        Returns:
          Highlighted text
    '''
    if not subtext:   # nothing to highlight (e.g. a curator-based listing); avoid the
        return text   # empty-pattern match that would wrap every position in blank spans
    pattern = f"(<[^>]+>)|({subtext if is_regex else re.escape(subtext)})"
    def replace(m):
        if m.group(1):  # inside an HTML tag — leave it alone
            return m.group(1)
        return f'<span {HIGHLIGHT}>{m.group(2)}</span>'
    return re.sub(pattern, replace, text, flags=re.IGNORECASE)


def standard_ack_table(rows, ack, is_regex=False, show_count=True, match_key=None):
    ''' Create a standard table of DOIs/acknowledgements
        Keyword arguments:
          rows: rows from dois collection
          ack: acknowledgement text to highlight (treated as a regex if is_regex is True)
          show_count: if False, emit no built-in counter - for callers that display
                      the count themselves (e.g. via ack_stat_cards). That caller's
                      count element must carry id='totalrows' so the version/internal
                      -external filters still have something to update.
          match_key: when set (a search_regex key), rows whose jrc_acknowledge carries a
                     tag of that name get a 'haskeytag' class, and a "Show only untagged"
                     button (which hides 'haskeytag') is added - surfacing DOIs that
                     matched the key's regex but have no corresponding tag.
        Returns:
          html: HTML
          cnt: number of DOIs
          oacnt: number of Open Access
    '''
    # Materialize once: pre-scanned below (doi_types) to decide the initial
    # journal/preprint-vs-other filter state, so `rows` must survive two passes.
    rows = list(rows)
    # If the caller already restricted rows to a single doi_type (e.g.
    # /acksregexui's ?doi_type=internal|external), a promised count (e.g. an
    # acknowledgement-coverage click-through) should match what's visible on load -
    # so don't also hide 'other' rows by default in that case. Also drives the
    # Internal/External cycle button further down (see cyclebtn).
    doi_types = {row.get('doi_type', 'internal') for row in rows}
    restricted = len(doi_types) == 1
    header = ['Published', 'DOI', 'Acknowledgements', 'Tags']
    # data-initial-hide/data-counter: default the view to journal-articles/preprints
    # only (hide 'other' rows on load); the type cycle button below reveals all types.
    initial_hide = "" if restricted else "data-initial-hide='other' "
    html = "<table id='dois' class='tablesorter standard-scroll' " \
           + f"{initial_hide}data-counter='totalrows'><thead><tr>" \
           + ''.join([f"<th>{itm}</th>" for itm in header]) + "</tr></thead><tbody>"
    fileoutput = ""
    cnt = oacnt = untagged_count = 0
    tag_counts = collections.Counter()
    # Tag name -> CSS class, assigned in first-seen order. An index rather than
    # a slugified name, so two names that only differ in punctuation/spacing/
    # case (e.g. "Cryo-EM Facility" vs "Cryo EM Facility") can never collide
    # onto the same class and cross-contaminate the chip filter below.
    tag_class = {}
    for row in rows:
        if 'jrc_is_oa' in row and row['jrc_is_oa']:
            oacnt += 1
        version = DL.is_version(row)
        # external_dois documents only store the flattened jrc_publishing_date, not
        # the raw Crossref/DataCite record DL.get_publishing_date() expects, so use
        # the precomputed field directly (present on both internal and external rows).
        row['published'] = row.get('jrc_publishing_date', 'unknown')
        row['link'] = doi_link(row['doi'])
        row['jrc_ack2'] = row['jrc_acknowledgements'].replace('\n', '<br>')
        row['jrc_ack2'] = highlight_subtext(row['jrc_ack2'], ack, is_regex=is_regex)
        # dedupe by name (keeping the last tag object seen), in case a row
        # somehow carries the same tag name twice
        tags = list({tag['name']: tag for tag in row.get('jrc_acknowledge') or []}.values())
        # List tags alphabetically (case-insensitive) by name.
        tags.sort(key=lambda tag: tag['name'].lower())
        tag_names = [tag['name'] for tag in tags]
        # IRIS-curated tags render lime (see curator_display()); human-curated
        # tags get no override, so they fall back to the standard link color.
        row['jrc_ack_tags'] = ", ".join(
            (f"<a class='tag-iris' href='/tag/{escape(tag['name'])}'>{escape(tag['name'])}</a>"
             if tag.get('curator') == 'IRIS'
             else f"<a href='/tag/{escape(tag['name'])}'>{escape(tag['name'])}</a>")
            for tag in tags)
        tag_counts.update(tag_names)
        for name in tag_names:
            if name not in tag_class:
                tag_class[name] = f'tagidx-{len(tag_class)}'
        # 'jp' (journal-article/preprint) vs 'other' drives the type cycle button;
        # mirrors the JOURNAL_ARTICLE query filter the endpoints used to apply.
        is_jp = row.get('type') == 'journal-article' or row.get('subtype') == 'preprint' \
                or (row.get('types') or {}).get('resourceTypeGeneral') == 'Preprint'
        doi_type = row.get('doi_type', 'internal')
        cls = [doi_type, 'jp' if is_jp else 'other']
        if version:
            cls.append('ver')
        # tagidx-N classes let the chip bar below filter rows client-side by tag
        cls.extend(tag_class[name] for name in tag_names)
        # match_key "untagged" filter: flag rows that carry the key's tag; count those
        # that matched the regex but don't (see the Show only untagged button below).
        if match_key is not None:
            if match_key in tag_names:
                cls.append('haskeytag')
            else:
                untagged_count += 1
        html += f"<tr class=\'{' '.join(cls)}\'><td>" \
            + dloop(row, ['published', 'link', 'jrc_ack2', 'jrc_ack_tags'], "</td><td>") \
            + "</td></tr>"
        cnt += 1
        row['jrc_acknowledgements'] = row['jrc_acknowledgements'].replace('\n', ' ')
        row['jrc_ack_tags_plain'] = ", ".join(tag_names)
        fileoutput += dloop(row, ['published', 'doi', 'jrc_acknowledgements',
                                  'jrc_ack_tags_plain']) + "\n"
    html += '</tbody></table>'
    chipbar = tagkey = ""
    if tag_counts:
        chips = [f"<span class='tag-chip' data-tagclass='{tag_class[name]}' "
                 "onclick=\"filterByTag('dois', this, 'totalrows');\">"
                 f"{escape(name)} <span class='tag-chip-count'>{count}</span></span>"
                 for name, count in sorted(tag_counts.items(),
                                           key=lambda kv: (-kv[1], kv[0].lower()))]
        chipbar = "<p><b>Filter by tag:</b> " + "".join(chips) + "</p>"
        # Reuses the same tag-iris class/href-less anchor the Tags column links
        # get, so the key can never drift out of sync with the actual colors.
        tagkey = "<p style='font-size: 10pt'><b>Tag color key:</b> " \
                 "<a class='tag-iris' href='javascript:void(0);' " \
                 "onclick='return false;'>IRIS-tagged</a> &middot; " \
                 "<a href='javascript:void(0);' onclick='return false;'>" \
                 "Human-curated</a></p>"
    counter = "" if not show_count \
              else f"<p>Number of DOIs: <span id='totalrows'>{cnt:,}</span></p>"
    if len(doi_types) > 1:
        cyclebtn = "<button class=\"btn btn-outline-info\" " \
                   + "onclick=\"cycle_filter(this, 'dois', 'internal', 'external', " \
                   + "'Internal', 'External', 'totalrows');\">" \
                   + "Showing Internal &amp; External</button>&nbsp;"
    elif doi_types:
        # Caller restricted the query to one collection (e.g. /acksregexui's
        # ?doi_type=internal|external) - the other type never appears in
        # `rows` at all, so toggling is meaningless here. A plain-text label
        # (not a disabled button) makes clear this is a state, not a control
        # that happens to be unavailable right now.
        only_type = next(iter(doi_types)).capitalize()
        cyclebtn = f"<span style='color: gray'>Showing {only_type} only</span>&nbsp;"
    else:
        cyclebtn = ""
    # data-state must match the table's initial_hide above: state 1 (hide
    # 'other') for the normal default, state 0 (hide nothing) when restricted
    # - the button stays fully operable either way, just starting from
    # whichever state matches what's actually visible on load.
    if restricted:
        typebtn = "<button class=\"btn btn-outline-info\" data-state=\"0\" " \
                  + "onclick=\"cycle_filter(this, 'dois', 'jp', 'other', " \
                  + "'Journal/preprint', 'Other', 'totalrows');\">" \
                  + "Showing Journal/preprint &amp; Other</button>&nbsp;"
    else:
        typebtn = "<button class=\"btn btn-outline-info\" data-state=\"1\" " \
                  + "onclick=\"cycle_filter(this, 'dois', 'jp', 'other', " \
                  + "'Journal/preprint', 'Other', 'totalrows');\">" \
                  + "Showing Journal/preprint only</button>&nbsp;"
    cbutton = "<button id='verbtn' class=\"btn btn-outline-warning\" " \
              + "onclick=\"toggler('dois', 'ver', 'totalrows');\">" \
              + "Filter versioned DOIs</button>&nbsp;"
    # "Matched the key's regex but has no corresponding tag" filter - shown only for a
    # match_key caller and only when some rows actually lack the tag. Hides 'haskeytag'
    # rows (composing with the other filters), leaving the untagged matches to review.
    untagbtn = ""
    if match_key is not None:
        if untagged_count:
            untagbtn = ("<button class=\"btn btn-outline-secondary\" "
                        "onclick=\"toggleClass('dois', 'haskeytag', this, 'Show all matches', "
                        "'Show only untagged', 'totalrows');\">"
                        f"Show only untagged ({untagged_count:,})</button>&nbsp;")
        else:
            # Every match already carries the tag - show the state (grey text, like the
            # type-cycle "Showing X only" label) rather than hide the control entirely,
            # so the filter is always discoverable and "0 untagged" is explicit.
            untagbtn = "<span style='color: gray'>No untagged matches</span>&nbsp;"
    html = counter + cyclebtn + typebtn + cbutton + untagbtn \
           + create_downloadable('standard', header, fileoutput) + chipbar + tagkey + html
    return html, cnt, oacnt


def ack_stat_cards(cnt, internal, external):
    ''' Build stat cards for an acknowledgement DOI table. The card values
        carry ids/attributes that the dis.js row filters (toggler/cycle_filter)
        keep up to date.
        Keyword arguments:
          cnt: total number of DOIs
          internal: number of internal DOIs
          external: number of external DOIs
        Returns:
          HTML to prepend to the table
    '''
    return stat_cards([("DOIs", f"<span id='totalrows'>{cnt:,}</span>"),
                       ("Internal", f"<span data-filter-count='internal'>{internal:,}</span>"),
                       ("External", f"<span data-filter-count='external'>{external:,}</span>")],
                      div_id='acks-stats')


# Inline formatting tags publisher titles legitimately use (italic species names,
# superscripts, etc.); every other tag in a title stays escaped.
TITLE_SAFE_TAGS = ('i', 'b', 'em', 'strong', 'sub', 'sup')


def strip_tex_math(title):
    ''' Best-effort de-TeX of a title for display. Some Crossref/DataCite titles carry
        LaTeX math (e.g. "$${\\bf{Micro}}{{\\mathbb{S}}}{\\bf{plit}}$$: ...") that would
        otherwise show as raw markup. Drop $...$/$$...$$ delimiters, unwrap \\cmd{...}
        formatting to its content, and remove leftover TeX grouping braces. Only runs
        when the title carries a TeX signature (a \\command or "$$"), so plain titles
        (and a lone "$5") are left untouched; bare symbol commands like \\alpha remain.
        Keyword arguments:
          title: the DOI title string
        Returns:
          The de-TeX'd title (or the original, unchanged)
    '''
    if not title or not (re.search(r'\\[A-Za-z]', title) or '$$' in title):
        return title
    out = re.sub(r'\${1,2}', '', title)               # drop $...$ / $$...$$ delimiters
    for _ in range(8):                                # unwrap \cmd{...} -> ... (innermost first)
        new = re.sub(r'\\[A-Za-z]+\s*\{([^{}]*)\}', r'\1', out)
        if new == out:
            break
        out = new
    return out.replace('{', '').replace('}', '')      # drop leftover TeX grouping braces


def render_title_html(title):
    ''' Render a DOI title for on-screen display: strip any LaTeX math markup, HTML-escape
        the whole string (so any script/markup in this external Crossref/DataCite metadata
        is neutralized), then restore a small allowlist of inline formatting tags and
        collapse whitespace runs. Returns a safe() value. TSV downloads use the raw title.
        Keyword arguments:
          title: the DOI title (may be empty/None)
        Returns:
          A safe() HTML string
    '''
    if not title:
        return safe('')
    out = escape(strip_tex_math(title))
    for tag in TITLE_SAFE_TAGS:
        out = out.replace(f"&lt;{tag}&gt;", f"<{tag}>").replace(f"&lt;/{tag}&gt;", f"</{tag}>")
    return safe(re.sub(r'\s+', ' ', out).strip())


# JATS inline tags we map to a plain HTML equivalent when rendering an abstract.
JATS_TAG_MAP = {'italic': 'i', 'bold': 'b', 'sup': 'sup', 'sub': 'sub', 'monospace': 'code'}


def jats_to_html(text):
    ''' Best-effort render of a JATS/MathML abstract as safe HTML. Crossref delivers
        abstracts as JATS XML (e.g. <jats:p>, <jats:inline-formula> with a <jats:tex-math>
        + parallel <mml:math>); browsers ignore the unknown tags and leak raw markup.
        Keep the tex-math branch of each formula (de-TeX'd) and drop the MathML twin,
        drop the redundant <jats:title>, map a small allowlist of inline tags to HTML,
        turn <jats:p> into breaks, then escape everything and restore only the allowlist
        (so nothing else — script, styled spans, stray tags — can render). Returns a str.
        Keyword arguments:
          text: the raw abstract (JATS XML, plain, or None)
        Returns:
          Safe HTML string ('' if empty)
    '''
    if not text:
        return ''
    out = re.sub(r'<mml:math\b.*?</mml:math>', '', text, flags=re.S | re.I)   # drop MathML twin
    out = re.sub(r'<(?:jats:)?tex-math\b[^>]*>(.*?)</(?:jats:)?tex-math>', r'\1',
                 out, flags=re.S | re.I)                                      # keep TeX branch
    out = re.sub(r'<(?:jats:)?title\b[^>]*>.*?</(?:jats:)?title>', '',
                 out, flags=re.S | re.I)                                      # drop redundant title
    # Sentinel the allowlist tags so they survive the escape pass below (\x00 never
    # appears in real abstract text and is untouched by html.escape).
    for jats, htmltag in JATS_TAG_MAP.items():
        out = re.sub(rf'<(?:jats:)?{jats}\b[^>]*>', f'\x00{htmltag}\x00', out, flags=re.I)
        out = re.sub(rf'</(?:jats:)?{jats}>', f'\x00/{htmltag}\x00', out, flags=re.I)
    out = re.sub(r'</?(?:jats:)?p\b[^>]*>', '\x00br\x00', out, flags=re.I)    # paragraphs -> breaks
    out = re.sub(r'<[^>]+>', '', out)                                        # strip every other tag
    out = escape(strip_tex_math(out))                                        # de-TeX, then escape all
    out = re.sub(r'\s+', ' ', out)                                           # collapse source indentation
    for htmltag in list(JATS_TAG_MAP.values()) + ['br']:
        out = out.replace(f'\x00{htmltag}\x00', f'<{htmltag}>')
        out = out.replace(f'\x00/{htmltag}\x00', f'</{htmltag}>')
    out = re.sub(r'\s*<br>\s*', '<br>', out)                                 # tidy space around breaks
    out = re.sub(r'(?:<br>){3,}', '<br><br>', out)                           # cap consecutive breaks
    return re.sub(r'^(?:<br>)+|(?:<br>)+$', '', out).strip()                 # trim edge breaks


def standard_doi_table(rows, prefix=None, count_card=False, show_count=True,
                       extra_headers=None, extra_fn=None, mark_fn=None,
                       download_name='standard'):
    ''' Create a standard table of DOIs
        Keyword arguments:
          rows: rows from dois collection
          prefix: prefix for year pulldown
          count_card: if True, show the DOI count as a stat card instead of the
                      "Number of DOIs:" text line (opt-in; the count span keeps
                      id='totalrows' either way so the version-filter toggler works)
          show_count: if False, emit no built-in counter at all - for callers that
                      display the count themselves (e.g. in their own stat card).
                      That caller's count element must carry id='totalrows' so the
                      version-filter toggler still has something to update.
          extra_headers: optional list of extra column header(s) appended after "Title"
          extra_fn: optional callable(row) -> list of extra cell values (one per
                    extra_headers entry, in order) appended to each row after Title.
                    Table cells go through render_table (plain text is escaped; wrap a
                    value in safe()/cell() for trusted HTML or a numeric sort key); the
                    TSV download gets the same values with any HTML tags stripped.
          mark_fn: optional callable(row) -> trusted HTML string prefixed inside the DOI
                   cell before the link (e.g. a green checkmark marking a person's works);
                   the raw DOI in the TSV download is unaffected
          download_name: base filename for the TSV download button (default 'standard')
        Returns:
          html: HTML
          cnt: number of DOIs
          oacnt: number of Open Access
    '''
    header = ['Published', 'DOI', 'Journal', 'Title']
    if extra_headers:
        header = header + list(extra_headers)
    fileoutput = ""
    cnt = oacnt = 0
    trows = []
    row_classes = []
    for row in rows:
        if 'jrc_is_oa' in row and row['jrc_is_oa']:
            oacnt += 1
        version = DL.is_version(row)
        row['published'] = DL.get_publishing_date(row)
        row['link'] = doi_link(row['doi'])
        row['journal'] = DL.get_journal(row, full=False, name_only=True)
        row['title'] = DL.get_title(row)
        extra = list(extra_fn(row)) if extra_fn else []
        mark = mark_fn(row) if mark_fn else ''
        trows.append([row['published'], safe(mark + row['link']), row['journal'],
                      render_title_html(row['title'])] + extra)
        row_classes.append('ver' if version else '')
        if row['title']:
            row['title'] = row['title'].replace("\n", " ")
        cnt += 1
        line = dloop(row, ['published', 'doi', 'journal', 'title'])
        if extra:
            line += "\t" + "\t".join(re.sub(r'<[^>]+>', '', str(x)) for x in extra)
        fileoutput += line + "\n"
    html = render_table(header, trows, table_id='dois', css='tablesorter standard-scroll',
                        row_classes=row_classes)
    if not show_count:
        counter = ''
    elif count_card:
        counter = stat_cards([("DOIs", f"<span id='totalrows'>{cnt:,}</span>")],
                             div_id='dois-stats')
    else:
        counter = f"<p>Number of DOIs: <span id='totalrows'>{cnt:,}</span></p>"
    cbutton = "<button id='verbtn' class=\"btn btn-outline-warning\" " \
              + "onclick=\"toggler('dois', 'ver', 'totalrows');\">" \
              + "Filter versioned DOIs</button>&nbsp;"
    if prefix:
        html = counter + year_pulldown(prefix) + "&nbsp;"*5 \
               + cbutton + create_downloadable(download_name, header, fileoutput) + html
    else:
        html = counter + cbutton + create_downloadable(download_name, header, fileoutput) + html
    return html, cnt, oacnt

# ******************************************************************************
# * Badge utility functions                                                    *
# ******************************************************************************


def worker_badge(row, badges):
    ''' Get a badge for a worker type
        Keyword arguments:
          row: row from orcid collection
          badges: list of badges
        Returns:
          None
    '''
    if 'workerType' in row and row['workerType'] and row['workerType'] != 'Employee':
        badges.append(f"{tiny_badge('contingent', row['workerType'])}")


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
        if 'employeeId' not in auth or not auth['employeeId']:
            badges.append(f"{tiny_badge('alumni', 'No employee ID')}")
        if 'orcid' not in auth or not auth['orcid']:
            badges.append(f"{tiny_badge('noorcid', 'No ORCID')}")
        if auth['asserted']:
            badges.append(f"{tiny_badge('asserted', 'Janelia affiliation')}")
        elif 'match' in auth and auth['match'] == 'ORCID':
            badges.append(f"{tiny_badge('orcid', 'ORCID' if ignore_match else 'ORCID match')}")
        elif 'match' in auth and auth['match'] == 'name' and not ignore_match:
            badges.append(f"{tiny_badge('name', 'Name match')}")
        worker_badge(auth, badges)
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


def show_openalex_authors(doi, confirmed):
    ''' Show OpenAlex authors
        Keyword arguments:
          doi: DOI
          confirmed: list of confirmed authors
        Returns:
          List of HTML authors
    '''
    sleep(.05)
    try:
        data = DL.get_doi_record(doi, source='openalex')
        if not data:
            return "", 0
    except Exception as err:
        return f"<span style='color:red'>Error getting OpenAlex record for {doi}:" \
            + f"<br>{str(err)}</span>", 0
    alist = []
    for auth in data['authorships']:
        if 'author' in auth and 'display_name' in auth['author']:
            badges = []
            who = auth['author']['display_name']
            rec = None
            if 'orcid' in auth['author'] and auth['author']['orcid']:
                orc = auth['author']['orcid'].replace('https://orcid.org/', '')
                try:
                    rec = DB['dis'].orcid.find_one({"orcid": orc})
                except Exception:
                    pass
            if not rec:
                try:
                    fam = auth['author']['display_name'].split(" ")[-1]
                    giv = auth['author']['display_name'].replace(fam, "").strip()
                    rec = DB['dis'].orcid.find_one({"given": giv, "family": fam})
                except Exception:
                    pass
            if rec:
                if 'orcid' in rec and rec['orcid']:
                    who = f"<a href='/userui/{rec['orcid']}'>{who}</a>"
                elif 'userIdO365' in rec and rec['userIdO365']:
                    who = f"<a href='/userui/{rec['userIdO365']}'>{who}</a>"
                odata = DL.get_single_author_details(rec, DB['dis'].orcid)
                badges = get_badges(odata, who=who)
                if 'employeeId' in odata and odata['employeeId'] in confirmed:
                    badges.insert(0, tiny_badge('author', 'Janelia author'))
            for inst in auth.get('institutions', []):
                if 'Janelia' in inst['display_name']:
                    badges.append(tiny_badge('asserted', 'Janelia affiliation'))
                    break
            row = f"<td>{who}</td><td>{' '.join(badges)}</td>"
            alist.append(row)
    return f"<table class='borderless'><tr>{'</tr><tr>'.join(alist)}</tr></table>", \
           len(data['authorships'])


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
    rows = DB['dis'].subscription.find({"type": stype})
    sub = {}
    for row in rows:
        sub[row['title']] = True
    return sub


def get_top_journals(year, maxpub=False, janelia=True, source=None, tag=None):
    ''' Get top journals
        Keyword arguments:
          year: year to get data for
          maxpub: if True, get max publishing date
          tag: optional jrc_tag.name to scope to
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
    if source:
        match["jrc_obtained_from"] = source
    if tag:
        match["jrc_tag.name"] = tag
    payload = [{"$match": match},
               {"$group": {"_id": "$jrc_journal", "count":{"$sum": 1},
                           "maxpub": {"$max": "$jrc_publishing_date"}}}
               ]
    rows = DB['dis'].dois.aggregate(payload)
    journal = {}
    for row in rows:
        if maxpub:
            journal[row['_id']] = {"count": row['count'], "maxpub": row['maxpub']}
        else:
            journal[row['_id']] = row['count']
    if not journal:
        return {}
    return journal


def get_top_publishers(year, source, maxpub=False):
    ''' Get top publishers
        Keyword arguments:
          year: year to get data for
          source: source of DOIs
          maxpub: if True, get max publishing date
        Returns:
          Publisher data
    '''
    match = {"jrc_obtained_from": source,
             "doi": {"$not": {"$regex": r"^10\.(1101|64898)\/"}}}
    if year != 'All':
        match["jrc_publishing_date"] = {"$regex": "^"+ year}
    payload = [{"$match": match},
               {"$group": {"_id": "$publisher", "count":{"$sum": 1},
                           "maxpub": {"$max": "$jrc_publishing_date"}}},
               {"$sort": {"count": -1}}
              ]
    rows = DB['dis'].dois.aggregate(payload)
    publisher = {}
    for row in rows:
        if maxpub:
            publisher[row['_id']] = {"count": row['count'], "maxpub": row['maxpub']}
        else:
            publisher[row['_id']] = row['count']
    if not publisher:
        return {}
    return publisher

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
    hqorgs = DL.get_supervisory_orgs()
    rows = DB['dis'].suporg.find({})
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
    mgmt = DB['dis'].orcid.count_documents(payload)
    if mgmt:
        mgmt = DB['dis'].orcid.find_one(payload)
    payload = {"affiliations": tag}
    acnt = DB['dis'].orcid.count_documents(payload)
    tagtype = "Affiliation" if acnt else ""
    orgs = DL.get_supervisory_orgs(DB['dis'].suporg)
    payload = [{"$match": {"jrc_tag.name": tag}},
               {"$unwind": "$jrc_tag"},
               {"$match": {"jrc_tag.name": tag}},
               {"$group": {"_id": "$jrc_tag.type", "count": {"$sum": 1}}},
               {"$sort": {"_id": 1}}
              ]
    rows = DB['dis'].dois.aggregate(payload)
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


def grouped_by_year(data):
    ''' Group cost data by year
    Keyword arguments:
      data: cost data
    Returns:
      HTML string
    '''
    output = []
    sorted_years = sorted(data)
    groups = []
    start = sorted_years[0]
    prev_year = sorted_years[0]
    prev_cost = data[prev_year]
    for year in sorted_years[1:]:
        cost = data[year]
        if cost != prev_cost or int(year) != int(prev_year) + 1:
            groups.append((start, prev_year, prev_cost))
            start = year
        prev_year = year
        prev_cost = cost
    groups.append((start, prev_year, prev_cost))
    for start, end, cost in groups:
        year_range = start if start == end else f"{start}-{end}"
        output.append(f"{year_range}: ${cost:,.2f}")
    return "<br>".join(output)


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


def last_thursday():
    ''' Calculate the date of the most recent Thursday strictly before today (so when
        run on a Thursday it returns the previous week's Thursday).
        Keyword arguments:
          None
        Returns:
          Date of the most recent Thursday before today
    '''
    today = date.today()
    offset = (today.weekday() - 3) % 7 or 7   # on a Thursday, go to the prior week's
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


def source_pulldown(prefix, source, limit):
    ''' Generate a source pulldown
        Keyword arguments:
          prefix: navigation prefix
          limit: limit of DOIs to show
        Returns:
          Pulldown HTML
    '''
    html = "<div class='btn-group'><button type='button' class='btn btn-info dropdown-toggle' " \
           + "data-toggle='dropdown' aria-haspopup='true' aria-expanded='false'>" \
           + "Select publishing source</button><div class='dropdown-menu'>"
    for src in ('Crossref', 'DataCite'):
        cls = 'dropdown-item'
        if src == source:
            cls += ' active'
        html += f"<a class='{cls}' href='/{prefix}/{src}/{limit}'>{src}</a>"
    html += "</div></div>"
    return html


def source_limit_pulldown(prefix, source, limit):
    ''' Generate a source and limit pulldown
        Keyword arguments:
          prefix: navigation prefix
          source: source of DOIs
          limit: limit of DOIs to show
        Returns:
          Pulldown HTML
    '''
    html = source_pulldown(prefix, source, limit)
    html += "&nbsp;&nbsp;&nbsp;<div class='btn-group'><button type='button' " \
            + "class='btn btn-info dropdown-toggle' " \
            + "data-toggle='dropdown' aria-haspopup='true' aria-expanded='false'>" \
           + "Select limit</button><div class='dropdown-menu'>"
    for lim in (10, 25, 50):
        cls = 'dropdown-item'
        if lim == limit:
            cls += ' active'
        html += f"<a class='{cls}' href='/{prefix}/{source}/{lim}'>{lim}</a>"
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

OPENAPI_METHODS = ('get', 'post', 'put', 'delete', 'patch')


def _flask_path_to_openapi(rule):
    ''' Convert a Flask URL rule to an OpenAPI path template
        (e.g. /doi/<int:x>/<path:doi> -> /doi/{x}/{doi}).
        Keyword arguments:
          rule: Flask rule string
        Returns:
          OpenAPI path string
    '''
    return re.sub(r"<(?:[^:<>]+:)?([^<>]+)>", r"{\1}", rule)


VALID_SCHEMA_TYPES = ('string', 'number', 'integer', 'boolean', 'array', 'object', 'null')


def _normalize_param_schema(param):
    ''' Coerce a legacy hand-written parameter into valid OpenAPI 3 form: a non-body
        parameter's schema type must be a JSON Schema type. In particular the historic
        "type: path" (a Flask converter name, not a data type) becomes "string".
        Mutates the parameter in place. A no-op once a group's docstrings are converted.
        Keyword arguments:
          param: an OpenAPI parameter dict
        Returns:
          None
    '''
    if not isinstance(param, dict):
        return
    schema = param.get('schema')
    if isinstance(schema, dict):
        stype = schema.get('type')
        if isinstance(stype, str) and stype not in VALID_SCHEMA_TYPES:
            schema['type'] = 'string'


def _docstring_meta(docstring):
    ''' Pull the summary/description from the text before the YAML "---" marker,
        matching the flask-swagger convention (first line = summary, the rest =
        description) so endpoints not yet converted keep their prose.
        Keyword arguments:
          docstring: a view function's docstring
        Returns:
          (summary, description) - either may be None
    '''
    head = docstring.split('---', 1)[0].strip()
    if not head:
        return None, None
    lines = [ln.strip() for ln in head.splitlines() if ln.strip()]
    summary = lines[0] if lines else None
    description = ' '.join(lines[1:]) if len(lines) > 1 else None
    return summary, description


def _register_openapi_components(spec):
    ''' Register the reusable component schemas and the (optional) Bearer security
        scheme shared across the API. Endpoint docstrings $ref these.
        Keyword arguments:
          spec: an APISpec instance
        Returns:
          None
    '''
    spec.components.security_scheme(
        "bearerAuth",
        {"type": "http", "scheme": "bearer",
         "description": "Optional API key supplied as a Bearer token. When present and "
                        "valid it sets rest.authorized to true; endpoints that only read "
                        "data do not require it."})
    spec.components.schema("Rest", {
        "type": "object",
        "description": "Envelope of request metadata attached to every response.",
        "properties": {
            "requester": {"type": "string", "description": "Client IP address"},
            "authorized": {"type": "boolean",
                           "description": "True if a valid API key was supplied"},
            "url": {"type": "string"},
            "endpoint": {"type": "string"},
            "error": {"type": "boolean"},
            "elapsed_time": {"type": "string",
                             "description": "Server processing time (H:MM:SS)"},
            "row_count": {"type": "integer"},
            "pid": {"type": "integer"}}})
    spec.components.schema("Error", {
        "type": "object",
        "description": "Error response body.",
        "properties": {
            "rest": {
                "type": "object",
                "properties": {
                    "status_code": {"type": "integer"},
                    "error": {"type": "boolean"},
                    "error_text": {"type": "string"}}}}})
    spec.components.schema("Acknowledgement", {
        "type": "object",
        "description": "One DOI's stored acknowledgement record.",
        "properties": {
            "doi": {"type": "string"},
            "doi_type": {"type": "string", "enum": ["internal", "external"],
                         "description": "Source collection (dois vs external_dois)"},
            "jrc_publishing_date": {"type": "string",
                                    "description": "Publishing date (YYYY-MM-DD)"},
            "jrc_acknowledgements": {"type": "string",
                                     "description": "Acknowledgements text"},
            "jrc_journal": {"type": "string"},
            "title": {"type": "string"},
            "jrc_ack_first_author": {
                "description": "Acknowledged first author(s)",
                "oneOf": [{"type": "string"},
                          {"type": "array", "items": {"type": "string"}}]},
            "jrc_ack_last_author": {
                "description": "Acknowledged last author(s)",
                "oneOf": [{"type": "string"},
                          {"type": "array", "items": {"type": "string"}}]},
            "is_preprint": {"type": "boolean"},
            "jrc_tag": {"type": "array", "items": {"type": "string"},
                        "description": "Project/lab tag names"},
            "type": {"type": "string", "description": "Crossref work type"},
            "subtype": {"type": "string"}}})
    spec.components.schema("AcknowledgementList", {
        "type": "object",
        "properties": {
            "rest": {"$ref": "#/components/schemas/Rest"},
            "acknowledgements": {
                "type": "array",
                "items": {"$ref": "#/components/schemas/Acknowledgement"}}}})
    # --- ORCID group ---
    spec.components.schema("OrcidProfile", {
        "type": "object",
        "additionalProperties": True,
        "description": "A public ORCID profile as returned by the ORCID API. For an "
                       "unknown iD this instead carries ORCID's error payload (an "
                       "'error-code' field), still with a 200 status."})
    spec.components.schema("DoiDate", {
        "type": "object",
        "properties": {
            "doi": {"type": "string"},
            "jrc_publishing_date": {"type": "string",
                                    "description": "Publishing date (YYYY-MM-DD)"}}})
    spec.components.schema("AffiliationSummary", {
        "type": "object",
        "description": "Per-affiliation author counts and supervisory-org status.",
        "properties": {
            "affiliation": {"type": "string"},
            "authors": {"type": "integer", "description": "Authors with this affiliation"},
            "authors_with_orcid": {"type": "integer"},
            "orcid_percent": {"type": "number",
                              "description": "Percent of authors that have an ORCID iD"},
            "suporg": {"type": "string",
                       "enum": ["active", "inactive", "no code", "none"],
                       "description": "Supervisory-org status of the affiliation"}}})
    spec.components.schema("OrcidPerson", {
        "type": "object",
        "description": "An ORCID collection record (person). _id and employeeId are "
                       "omitted from responses.",
        "properties": {
            "orcid": {"type": "string"},
            "given": {"type": "array", "items": {"type": "string"},
                      "description": "Given-name variants"},
            "family": {"type": "array", "items": {"type": "string"},
                       "description": "Family-name variants"},
            "affiliations": {"type": "array", "items": {"type": "string"}},
            "current_affiliations": {"type": "array", "items": {"type": "string"}},
            "previous_affiliations": {"type": "array", "items": {"type": "string"}},
            "department": {"type": "string"},
            "group": {"type": "string", "description": "Group/lab name"},
            "alumni": {"type": "boolean"},
            "userIdO365": {"type": "string"}}})
    spec.components.schema("OrcidApiResult", {
        "type": "object",
        "properties": {
            "rest": {"$ref": "#/components/schemas/Rest"},
            "data": {"$ref": "#/components/schemas/OrcidProfile"}}})
    spec.components.schema("OrcidWorksResult", {
        "type": "object",
        "properties": {
            "rest": {"$ref": "#/components/schemas/Rest"},
            "janelia_dois": {"type": "array",
                             "items": {"$ref": "#/components/schemas/DoiDate"}},
            "last_janelia_doi": {"$ref": "#/components/schemas/DoiDate"},
            "orcid": {"$ref": "#/components/schemas/OrcidProfile"},
            "other_dois": {"type": "array", "items": {"type": "string"},
                           "description": "DOIs found on ORCID not yet in the DIS database"}}})
    spec.components.schema("AffiliationSummaryList", {
        "type": "object",
        "properties": {
            "rest": {"$ref": "#/components/schemas/Rest"},
            "data": {"type": "array",
                     "items": {"$ref": "#/components/schemas/AffiliationSummary"}}}})
    spec.components.schema("OrcidPersonList", {
        "type": "object",
        "properties": {
            "rest": {"$ref": "#/components/schemas/Rest"},
            "data": {"type": "array",
                     "items": {"$ref": "#/components/schemas/OrcidPerson"}}}})
    # --- Organizations / PubMed / People / Diagnostics groups ---
    spec.components.schema("OrganizationList", {
        "type": "object",
        "properties": {
            "rest": {"$ref": "#/components/schemas/Rest"},
            "organizations": {"type": "array", "items": {"type": "string"},
                              "description": "Organization (supervisory-org) names in the group"}}})
    spec.components.schema("PubmedRecordResult", {
        "type": "object",
        "properties": {
            "rest": {"$ref": "#/components/schemas/Rest"},
            "data": {"type": "object", "additionalProperties": True,
                     "description": "The source record - a PMC OAI-PMH GetRecord or a PubMed "
                                    "PubmedArticleSet. An empty object if nothing was found."}}})
    spec.components.schema("PeopleRecord", {
        "type": "object",
        "additionalProperties": True,
        "description": "An HHMI People record (employeeId and managerId omitted from responses)."})
    spec.components.schema("PeopleResult", {
        "type": "object",
        "properties": {
            "rest": {"$ref": "#/components/schemas/Rest"},
            "data": {"$ref": "#/components/schemas/PeopleRecord"}}})
    spec.components.schema("Stats", {
        "type": "object",
        "description": "Server uptime and request statistics. Counters (requests, "
                       "endpoint_counts, uptime, pid) are per worker process.",
        "properties": {
            "version": {"type": "string"},
            "requests": {"type": "integer"},
            "start_time": {"type": "string"},
            "uptime": {"type": "string"},
            "current_time": {"type": "string",
                             "description": "Current server time with UTC offset"},
            "python": {"type": "string"},
            "hostname": {"type": "string", "description": "Host that served the request"},
            "run_mode": {"type": "string", "description": "dev or prod"},
            "debug": {"type": "boolean"},
            "pid": {"type": "integer"},
            "endpoint_counts": {"type": "object",
                                "additionalProperties": {"type": "integer"},
                                "description": "Per-endpoint request counts this run"},
            "time_since_last_transaction": {"type": "number"},
            "dependencies": {"type": "object",
                             "additionalProperties": {"type": "string"},
                             "description": "Key dependency versions"},
            "database": {
                "type": "object",
                "description": "DIS MongoDB health",
                "properties": {
                    "reachable": {"type": "boolean"},
                    "ping_ms": {"type": "number"},
                    "server_version": {"type": "string"},
                    "host": {"type": "string"},
                    "error": {"type": "string"}}}}})
    spec.components.schema("StatsResult", {
        "type": "object",
        "properties": {
            "rest": {"$ref": "#/components/schemas/Rest"},
            "stats": {"$ref": "#/components/schemas/Stats"}}})
    # --- DOI group ---
    spec.components.schema("DoiRecord", {
        "type": "object",
        "additionalProperties": True,
        "description": "A DOI record - Crossref/DataCite metadata plus jrc_* enrichment. "
                       "For a DOI not yet stored, the raw Crossref message or DataCite "
                       "attributes are returned instead."})
    spec.components.schema("DoiRecordResult", {
        "type": "object",
        "properties": {
            "rest": {"$ref": "#/components/schemas/Rest"},
            "data": {"$ref": "#/components/schemas/DoiRecord"}}})
    spec.components.schema("DoiRecordList", {
        "type": "object",
        "properties": {
            "rest": {"$ref": "#/components/schemas/Rest"},
            "data": {"type": "array",
                     "items": {"$ref": "#/components/schemas/DoiRecord"}}}})
    spec.components.schema("CitationResult", {
        "type": "object",
        "properties": {
            "rest": {"$ref": "#/components/schemas/Rest"},
            "data": {"type": "string", "description": "The formatted citation"},
            "jrc_preprint": {"description": "Preprint linkage, if present on the record"}}})
    spec.components.schema("CitationMap", {
        "type": "object",
        "properties": {
            "rest": {"$ref": "#/components/schemas/Rest"},
            "data": {"type": "object",
                     "additionalProperties": {"type": "string"},
                     "description": "Map of DOI to citation string ('' if the DOI was not found)"}}})
    spec.components.schema("CitationComponents", {
        "type": "object",
        "description": "The parts of a DIS-style citation.",
        "properties": {
            "authors": {"type": "array", "items": {"type": "string"}},
            "journal": {"type": "string"},
            "publishing_date": {"type": "string"},
            "title": {"type": "string"},
            "abstract": {"type": "string",
                         "description": "Present only for Crossref-sourced records"}}})
    spec.components.schema("ComponentsResult", {
        "type": "object",
        "properties": {
            "rest": {"$ref": "#/components/schemas/Rest"},
            "data": {"$ref": "#/components/schemas/CitationComponents"}}})
    spec.components.schema("ComponentsListResult", {
        "type": "object",
        "properties": {
            "rest": {"$ref": "#/components/schemas/Rest"},
            "data": {"type": "array",
                     "items": {"allOf": [
                         {"$ref": "#/components/schemas/CitationComponents"},
                         {"type": "object",
                          "properties": {"doi": {"type": "string"}}}]}}}})
    spec.components.schema("TypesResult", {
        "type": "object",
        "properties": {
            "rest": {"$ref": "#/components/schemas/Rest"},
            "data": {"type": "object",
                     "additionalProperties": {
                         "type": "object",
                         "properties": {
                             "count": {"type": "integer"},
                             "subtype": {"type": ["string", "null"]}}},
                     "description": "Map of work type to its count and subtype"}}})
    spec.components.schema("DoiTag", {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "code": {"type": ["string", "null"]},
            "type": {"type": ["string", "null"]}}})
    spec.components.schema("DoiAuthorsResult", {
        "type": "object",
        "properties": {
            "rest": {"$ref": "#/components/schemas/Rest"},
            "data": {"type": "array",
                     "items": {"type": "object", "additionalProperties": True},
                     "description": "Author detail objects; employeeId omitted unless authorized"},
            "tags": {"type": "array",
                     "items": {"$ref": "#/components/schemas/DoiTag"},
                     "description": "Distinct Janelia tags across the authors, if any"}}})
    spec.components.schema("JrcAuthorResult", {
        "type": "object",
        "properties": {
            "rest": {"$ref": "#/components/schemas/Rest"},
            "data": {"type": "array", "items": {"type": "string"},
                     "description": "Employee IDs written to jrc_author"}}})


def build_openapi_spec():
    ''' Build the OpenAPI 3.1 document for the whole app by reading the YAML block
        (after "---") from each route's docstring. Endpoints without a YAML block
        are omitted. Runtime behaviour of the endpoints is untouched.
        Returns:
          OpenAPI 3.1 document as a dict
    '''
    spec = APISpec(title="Data and Information Services",
                   version=__version__,
                   openapi_version="3.1.0",
                   info={"description": "REST API and UI for Janelia Data and "
                                        "Information Services (DIS)."})
    _register_openapi_components(spec)
    for rule in app.url_map.iter_rules():
        view = app.view_functions.get(rule.endpoint)
        if view is None or not view.__doc__:
            continue
        try:
            operation = load_yaml_from_docstring(view.__doc__)
        except Exception:
            operation = None
        if not isinstance(operation, dict) or not operation:
            continue
        summary, description = _docstring_meta(view.__doc__)
        if summary:
            operation.setdefault('summary', summary)
        if description:
            operation.setdefault('description', description)
        methods = [m.lower() for m in sorted(rule.methods or [])
                   if m.lower() in OPENAPI_METHODS]
        if not methods:
            continue
        path = _flask_path_to_openapi(rule.rule)
        placeholders = set(re.findall(r"{([^{}]+)}", path))
        operations = {}
        for method in methods:
            op_copy = copy.deepcopy(operation)
            # Drop path parameters absent from this route's template. A docstring
            # shared across several @app.route decorators (e.g. /acknowledgements
            # and /acknowledgements/{which}) would otherwise leave a phantom path
            # parameter on the parameter-less route, which is invalid in OpenAPI 3.
            # Reconcile the operation's path parameters with this route's template.
            # Shared docstrings (multiple @app.route decorators on one view) otherwise
            # leave phantom path params on parameter-less routes, or omit a param a
            # longer route needs - both invalid in OpenAPI 3. Drop the phantoms and
            # synthesize any placeholder the docstring didn't declare.
            raw_params = op_copy.get('parameters')
            raw_params = raw_params if isinstance(raw_params, list) else []
            kept = []
            declared_path = set()
            for prm in raw_params:
                if isinstance(prm, dict) and prm.get('in') == 'path':
                    if prm.get('name') not in placeholders:
                        continue
                    declared_path.add(prm.get('name'))
                _normalize_param_schema(prm)
                kept.append(prm)
            for placeholder in sorted(placeholders - declared_path):
                kept.append({"name": placeholder, "in": "path", "required": True,
                             "schema": {"type": "string"}})
            if kept or 'parameters' in op_copy:
                op_copy['parameters'] = kept
            operations[method] = op_copy
        try:
            spec.path(path=path, operations=operations)
        except Exception:
            continue
    return spec.to_dict()


@app.route('/doc')
def get_doc_json():
    ''' Return the OpenAPI 3.1 specification (JSON) for the API.
    '''
    try:
        spec = build_openapi_spec()
    except Exception as err:
        return inspect_error(err, 'Could not build OpenAPI spec')
    return jsonify(spec)


@app.route('/help')
def show_swagger():
    ''' Show Swagger docs
    '''
    return render_template('swagger_ui.html')

# *****************************************************************************
# * Admin endpoints                                                           *
# *****************************************************************************

def _database_health():
    ''' Ping the DIS MongoDB and report reachability, latency, server version, and the
        connected host. Never raises - a down database is reported, not raised.
        Returns:
          dict with reachable plus (on success) ping_ms/server_version/host, else error
    '''
    info = {"reachable": False}
    try:
        client = DB['dis'].client
        start = time()
        client.admin.command('ping')
        info['ping_ms'] = round((time() - start) * 1000, 2)
        info['reachable'] = True
        try:
            info['server_version'] = client.server_info().get('version')
        except Exception:
            pass
        if client.address:
            info['host'] = f"{client.address[0]}:{client.address[1]}"
    except Exception as err:
        info['error'] = str(err)
    return info


def _dependency_versions():
    ''' Report installed versions of the key runtime dependencies.
        Returns:
          dict of distribution name -> version string ('unknown' if undetermined)
    '''
    import importlib.metadata as md
    out = {}
    for dist in ('flask', 'pymongo', 'apispec'):
        try:
            out[dist] = md.version(dist)
        except Exception:
            out[dist] = 'unknown'
    for dist, module in (('doi_common', DL), ('jrc_common', JRC)):
        try:
            out[dist] = md.version(dist)
        except Exception:
            out[dist] = getattr(module, '__version__', 'unknown')
    return out


@app.route("/stats")
def stats():
    '''
    Show stats
    Server uptime, total requests, per-endpoint counts, and runtime info (version,
    Python, PID). Always succeeds.
    ---
    tags:
      - Diagnostics
    responses:
      '200':
        description: Uptime and request statistics
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/StatsResult'
    '''
    tbt = time() - app.config['LAST_TRANSACTION']
    result = initialize_result()
    start = datetime.fromtimestamp(app.config['START_TIME']).strftime('%Y-%m-%d %H:%M:%S')
    up_time = datetime.now() - app.config['STARTDT']
    result['stats'] = {"version": __version__,
                       "requests": app.config['COUNTER'],
                       "start_time": start,
                       "uptime": str(up_time),
                       "current_time": datetime.now().astimezone().strftime('%Y-%m-%d %H:%M:%S %z'),
                       "python": sys.version,
                       "hostname": socket.gethostname(),
                       "run_mode": app.config.get('RUN_MODE'),
                       "debug": app.config.get('DEBUG'),
                       "pid": os.getpid(),
                       "endpoint_counts": app.config['ENDPOINTS'],
                       "time_since_last_transaction": tbt,
                       "dependencies": _dependency_versions(),
                       "database": _database_health(),
                      }
    return generate_response(result)


# ******************************************************************************
# * API endpoints (DOI)                                                        *
# ******************************************************************************
@app.route('/citations/incoming/<string:source>/<path:doi>')
def get_incoming_citations(source, doi):
    '''
    Download a DOI's incoming citations
    Download a CSV file of the incoming citations (citing works) for a DOI. Use
    source "pubmed" to resolve via the DOI's jrc_pmid.
    ---
    tags:
      - DOI
    parameters:
      - in: path
        name: source
        required: true
        schema:
          type: string
        description: Citation source (e.g. crossref, pubmed)
      - in: path
        name: doi
        required: true
        schema:
          type: string
        description: DOI
    responses:
      '200':
        description: CSV file of incoming citations (attachment)
        content:
          text/csv:
            schema:
              type: string
              format: binary
      '500':
        description: Error building the citation file
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/Error'
    '''
    doi = doi.lstrip('/').rstrip('/').lower()
    cinput = doi
    if source == 'pubmed' and not doi.isdigit():
        rec = DL.get_doi_record(doi, coll=DB['dis'].dois)
        if rec and 'jrc_pmid' in rec:
            cinput = rec['jrc_pmid']
    try:
        dois = DL.get_incoming_citations(cinput, source=source)
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    fname = f"{doi.replace('/', '_')}_{source}.csv"
    with open(f"/tmp/{fname}", 'w', encoding='ascii') as fileout:
        for itm in dois:
            fileout.write(itm + '\n')
    return download(fname)


@app.route('/doi/authors/<path:doi>')
def get_doi_authors(doi):
    '''
    Return a DOI's authors
    Return author details for a DOI. employeeId is included only for authorized
    requests (a valid Bearer token). Any distinct Janelia tags are also returned.
    ---
    tags:
      - DOI
    parameters:
      - in: path
        name: doi
        required: true
        schema:
          type: string
        description: DOI
    responses:
      '200':
        description: Author details (and any Janelia tags)
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/DoiAuthorsResult'
      '500':
        description: MongoDB error
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/Error'
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
    if not result['rest']['authorized']:
        for auth in authors:
            if 'employeeId' in auth:
                del auth['employeeId']
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
def get_doi_janelians(doi):
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
    resp = get_doi_authors(doi)
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
def get_doi_migration(doi):
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
        for fld in DO_NOT_DISPLAY:
            if fld in rec:
                del rec[fld]
    result['data'] = rec
    result['rest']['source'] = 'mongo'
    result['rest']['row_count'] = len(result['data'])
    return generate_response(result)


@app.route('/doi/migrations/<string:idate>', methods=['GET'])
def get_doi_migrations(idate):
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
def get_published_dois(start, end):
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
def get_doi_api(doi):
    '''
    Return a DOI
    Return the stored record for a DOI. If it is not in the dois collection it is
    fetched live from Crossref or DataCite (rest.source indicates which).
    ---
    tags:
      - DOI
    parameters:
      - in: path
        name: doi
        required: true
        schema:
          type: string
        description: DOI
    responses:
      '200':
        description: The DOI record (rest.source = mongo, crossref, or datacite)
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/DoiRecordResult'
      '500':
        description: MongoDB or upstream fetch error
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/Error'
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
def get_inserted(idate):
    '''
    Return DOIs inserted since a specified date
    Return every DOI record inserted since midnight on the given date.
    ---
    tags:
      - DOI
    parameters:
      - in: path
        name: idate
        required: true
        schema:
          type: string
          format: date
          example: '2026-01-01'
        description: Earliest insertion date, ISO format (YYYY-MM-DD)
    responses:
      '200':
        description: DOI records inserted on or after the date
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/DoiRecordList'
      '400':
        description: Invalid date
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/Error'
      '500':
        description: MongoDB error
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/Error'
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
def get_citation(doi):
    '''
    Return a DIS-style citation
    Return a DIS-style citation string for a DOI. Available at /citation/{doi} and the
    equivalent /citation/dis/{doi}.
    ---
    tags:
      - DOI
    parameters:
      - in: path
        name: doi
        required: true
        schema:
          type: string
        description: DOI
    responses:
      '200':
        description: The citation string
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/CitationResult'
      '404':
        description: DOI not found
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/Error'
      '500':
        description: MongoDB or formatting error
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/Error'
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
    result['data'] = f"{authors} {title}. {DOI}{doi}."
    if 'jrc_preprint' in row:
        result['jrc_preprint'] = row['jrc_preprint']
    return generate_response(result)


@app.route('/citations', defaults={'ctype': 'dis'}, methods=['OPTIONS', 'POST'])
@app.route('/citations/<string:ctype>', methods=['OPTIONS', 'POST'])
def show_multiple_citations(ctype='dis'):
    '''
    Return citations
    Return a map of DOI to citation string for a list of DOIs. The path segment sets
    the style (dis, flylight, or full); /citations defaults to dis.
    ---
    tags:
      - DOI
    parameters:
      - in: path
        name: ctype
        required: true
        schema:
          type: string
          enum: [dis, flylight, full]
        description: Citation style (only on /citations/{ctype}; /citations uses dis)
    requestBody:
      required: true
      content:
        application/json:
          schema:
            type: object
            required: [dois]
            properties:
              dois:
                type: array
                items:
                  type: string
                description: DOIs to cite
    responses:
      '200':
        description: Map of DOI to citation string
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/CitationMap'
      '400':
        description: No DOI list supplied
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/Error'
      '500':
        description: MongoDB or formatting error
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/Error'
    '''
    result = initialize_result()
    ipd = receive_payload()
    if "dois" not in ipd or not (ipd['dois']) or not isinstance(ipd['dois'], list):
        raise InvalidUsage("You must specify a list of DOIs")
    result['rest']['source'] = 'mongo'
    result['data'] = {}
    for doi in ipd['dois']:
        try:
            row = DB['dis'].dois.find_one({"doi": doi.lower()}, {'_id': 0})
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
            result['data'][doi] = f"{result['data'][doi]}. {DOI}{doi}."
        else:
            result['data'][doi] = f"{result['data'][doi]}. {journal}."
    return generate_response(result)


@app.route('/citation/flylight/<path:doi>')
def show_flylight_citation(doi):
    '''
    Return a FlyLight-style citation
    Return a FlyLight-style citation string for a DOI.
    ---
    tags:
      - DOI
    parameters:
      - in: path
        name: doi
        required: true
        schema:
          type: string
        description: DOI
    responses:
      '200':
        description: The citation string
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/CitationResult'
      '404':
        description: DOI not found
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/Error'
      '500':
        description: MongoDB or formatting error
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/Error'
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
    Return a full citation string (DIS style plus journal) for a DOI.
    ---
    tags:
      - DOI
    parameters:
      - in: path
        name: doi
        required: true
        schema:
          type: string
        description: DOI
    responses:
      '200':
        description: The citation string
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/CitationResult'
      '404':
        description: DOI not found
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/Error'
      '500':
        description: MongoDB or formatting error
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/Error'
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
    Return the parts of a DIS-style citation (authors, title, journal, date, and -
    for Crossref-sourced records - abstract) for a DOI.
    ---
    tags:
      - DOI
    parameters:
      - in: path
        name: doi
        required: true
        schema:
          type: string
        description: DOI
    responses:
      '200':
        description: Citation components
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/ComponentsResult'
      '404':
        description: DOI not found
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/Error'
      '500':
        description: MongoDB or formatting error
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/Error'
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
    Return the DOI records matching a raw MongoDB find filter.
    ---
    tags:
      - DOI
    requestBody:
      required: true
      content:
        application/json:
          schema:
            type: object
            required: [query]
            properties:
              query:
                type: object
                additionalProperties: true
                description: A MongoDB find filter document
    responses:
      '200':
        description: Matching DOI records (filter echoed in rest.query)
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/DoiRecordList'
      '400':
        description: No query supplied
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/Error'
      '500':
        description: MongoDB or formatting error
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/Error'
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
    Return citation components for every DOI carrying a given jrc_tag. The path segment
    sets the style (dis or flylight); /components defaults to dis.
    ---
    tags:
      - DOI
    parameters:
      - in: path
        name: ctype
        required: true
        schema:
          type: string
          enum: [dis, flylight]
        description: Citation style (only on /components/{ctype}; /components uses dis)
    requestBody:
      required: true
      content:
        application/json:
          schema:
            type: object
            required: [tag]
            properties:
              tag:
                type: string
                description: Group tag (matched against jrc_tag.name)
    responses:
      '200':
        description: Citation components per matching DOI
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/ComponentsListResult'
      '400':
        description: No tag supplied
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/Error'
      '500':
        description: MongoDB or formatting error
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/Error'
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
    Return the DOI work types (and DataCite), each with its subtype and count.
    ---
    tags:
      - DOI
    responses:
      '200':
        description: Map of work type to its count and subtype
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/TypesResult'
      '500':
        description: MongoDB error
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/Error'
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
    Recompute the Janelia authors (their employee IDs) from the stored record and
    write them to jrc_author. Returns the employee IDs written. Takes no request body.
    ---
    tags:
      - DOI
    parameters:
      - in: path
        name: doi
        required: true
        schema:
          type: string
        description: DOI
    responses:
      '200':
        description: Employee IDs written to jrc_author (rest.rows_updated if modified)
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/JrcAuthorResult'
      '400':
        description: DOI not found
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/Error'
      '500':
        description: MongoDB or formatting error
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/Error'
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


@app.route('/raw/<string:resource>/<path:doi>')
def get_raw(resource=None, doi=None):
    ''' JSON metadata for a DOI
    resource: arxiv, biorxiv, crossref, datacite, elife, elsevier, figshare, openalex,
              plos, protocols.io, pubmed, pmc, springer, unpaywall, zenodo
    '''
    doi = doi.lstrip('/').rstrip('/').lower()
    result = initialize_result()
    response = None
    content = 'json'
    if resource:
        resource = resource.lower()
        result['rest']['source'] = DL.doi_api_url(doi, source=resource)
    if resource in ('arxiv', 'biorxiv', 'elife', 'elsevier', 'openalex', 'plos', 'pmc',
                    'pubmed', 'springer', 'unpaywall', 'zenodo'):
        try:
            response = DL.get_doi_record(doi, source=resource, content=content)
        except Exception as err:
            raise InvalidUsage(str(err), 500) from err
    elif resource == 'crossref':
        try:
            response = JRC.call_crossref(doi)
        except Exception as err:
            raise InvalidUsage(str(err), 500) from err
    elif resource == 'datacite':
        try:
            response = JRC.call_datacite(doi)
        except Exception as err:
            raise InvalidUsage(str(err), 500) from err
    elif resource == 'figshare':
        try:
            response= DL.get_doi_record(doi, source='figshare')
        except Exception:
            pass
    elif resource == 'protocols.io':
        suffix = f"protocols/{doi}"
        try:
            response = JRC.call_protocolsio(suffix)
        except Exception as err:
            raise InvalidUsage(str(err), 500) from err
    if response:
        result['data'] = response
    return generate_response(result)


@app.route('/xml/download/<string:resource>/<path:doi>')
def download_xml(resource='elsevier', doi=None):
    ''' Stream an XML metadata file for a DOI
    resource: elsevier
    '''
    doi = doi.lstrip('/').rstrip('/').lower()
    resource = resource.lower()
    try:
        rec = DL.get_doi_record(doi, source='elsevier', content='xml')
        stream = BytesIO(rec)
        stream.seek(0)
        filename = f"{doi.replace('/', '_')}.xml"
        return send_file(stream,as_attachment=True,
                         download_name=filename, mimetype='application/xml')
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err


@app.route('/xml/<string:resource>/<path:doi>')
def return_xml(resource='elsevier', doi=None):
    ''' XML metadata for a DOI
    resource: elsevier
    '''
    doi = doi.lstrip('/').rstrip('/').lower()
    response = result = None
    resource = resource.lower()
    if resource in ('elsevier', 'pmc', 'pubmed'):
        try:
            response = DL.get_doi_record(doi, source=resource, content='xml')
        except Exception as err:
            raise InvalidUsage(str(err), 500) from err
    if response:
        result = response
    return result

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


@app.route('/orcid/active')
def show_active_oids():
    '''
    Return active ORCID records
    Return information for active ORCID IDs
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
    payload = [{"$match": {"workerType": "Employee", "alumni": {"$exists": False},
                           "hireDate": {"$exists": True}, "employeeId": {"$exists": True},
                           "orcid": {"$exists": True}}},
               {"$project": {"_id": 0}},
               {"$sort": {"family.0": 1}}]
    try:
        rows = list(DB['dis'].orcid.aggregate(payload, collation=INSENSITIVE))
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


@app.route('/orcid/works/<string:oid>')
def show_oid_works(oid):
    '''
    Show works given an ORCID ID
    Return information for an ORCID ID
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
      404:
        description: ORCID ID (or employee ID) not found
      500:
        description: MongoDB error
    '''
    result = initialize_result()
    result['rest']['source'] = 'mongo'
    result['data'] = []
    row = DL.single_orcid_lookup(oid, DB['dis'].orcid)
    if not row:
        raise InvalidUsage(f"ORCID {oid} was not found", 404)
    if 'employeeId' in row:
        eid = row['employeeId']
    else:
        raise InvalidUsage(f"ORCID {oid} has no employeee ID", 404)
    try:
        rows = DB['dis'].dois.find({"jrc_author": eid}, {'_id': 0})
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    for row in rows:
        result['data'].append(row)
    result['rest']['row_count'] = len(result['data'])
    return generate_response(result)


@app.route('/orcidapi/<string:oid>')
def show_oidapi(oid):
    '''
    Show an ORCID ID (using the ORCID API)
    Fetch the public ORCID profile for an ORCID iD directly from the ORCID API. An
    unknown iD is not an error - ORCID's own error payload is returned in "data" with
    a 200 status.
    ---
    tags:
      - ORCID
    parameters:
      - in: path
        name: oid
        required: true
        schema:
          type: string
          example: 0000-0001-8090-2810
        description: ORCID iD
    responses:
      '200':
        description: ORCID profile (or ORCID's error payload for an unknown iD)
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/OrcidApiResult'
      '500':
        description: Error calling the ORCID API
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/Error'
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
    For an ORCID iD, return its Janelia DOIs (with publishing dates), the most recent
    Janelia DOI, the full ORCID profile, and any works found on ORCID that are not yet
    in the DIS database.
    ---
    tags:
      - ORCID
    parameters:
      - in: path
        name: oid
        required: true
        schema:
          type: string
          example: 0000-0001-8090-2810
        description: ORCID iD
    responses:
      '200':
        description: Janelia DOIs, latest Janelia DOI, ORCID profile, and other works
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/OrcidWorksResult'
      '500':
        description: Error querying the database or the ORCID API
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/Error'
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
# * API endpoints (orgs)                                                       *
# ******************************************************************************

@app.route('/organizations/<string:grp>')
def show_organizations(grp):
    '''
    Return organizations in a group
    Return the organization (supervisory-org) names that belong to a named group.
    ---
    tags:
      - Organizations
    parameters:
      - in: path
        name: grp
        required: true
        schema:
          type: string
        description: Group name
    responses:
      '200':
        description: Organization names in the group
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/OrganizationList'
      '404':
        description: Group not found
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/Error'
      '500':
        description: MongoDB error
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/Error'
    '''
    result = initialize_result()
    data = []
    try:
        row = DB['dis'].org_group.find_one({"group": grp})
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    if not row:
        raise InvalidUsage(f"Group {grp} was not found", 404)
    for member in row['members']:
        data.append(member)
    result['organizations'] = data
    result['rest']['row_count'] = len(data)
    return generate_response(result)

# ******************************************************************************
# * API endpoints (ack)                                                       *
# ******************************************************************************

@app.route('/acknowledgements/<string:which>')
@app.route('/acknowledgements')
def show_acknowledgements(which="journal"):
    '''
    Return acknowledgements
    Return stored acknowledgement text (jrc_acknowledgements) for journal articles,
    merged across the dois (internal) and external_dois collections and sorted by
    publishing date, newest first. Pass "all" to include every DOI type. An optional
    Bearer token (a configured API key) sets rest.authorized in the response.
    ---
    tags:
      - Acknowledgements
    parameters:
      - in: path
        name: which
        required: true
        schema:
          type: string
          enum: [journal, all]
          default: journal
        description: Subset to return - "journal" (journal articles only, default) or "all"
    responses:
      '200':
        description: Acknowledgements, newest first
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/AcknowledgementList'
      '404':
        description: No acknowledgements found
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/Error'
      '500':
        description: Error querying the database
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/Error'
    '''
    result = initialize_result()
    data = []
    payload = JOURNAL_ARTICLE if which == "journal" else {}
    payload["jrc_acknowledgements"] = {"$exists": True}
    projection = {"_id": 0, "doi": 1, "jrc_publishing_date": 1, "jrc_acknowledgements": 1,
                  "jrc_journal": 1, "title": 1, "jrc_ack_first_author": 1, "jrc_ack_last_author": 1,
                  "is_preprint": 1, "jrc_tag": 1, "type": 1, "subtype": 1}
    try:
        cnt = DB['dis'].dois.count_documents(payload)
        rows = DB['dis'].dois.find(payload, projection)
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    if not cnt:
        raise InvalidUsage("No acknowledgements found", 404)
    for row in rows:
        row['doi_type'] = 'internal'
        row['is_preprint'] = DL.is_preprint(row)
        if not DL.is_datacite(row['doi']):
            row['DOI'] = row['doi']
        row['title'] = DL.get_title(row)
        if row.get('jrc_tag'):
            tags = []
            for tag in row['jrc_tag']:
                tags.append(tag['name'])
            row['jrc_tag'] = tags
        for field in ['DOI', 'titles']:
            if field in row:
                del row[field]
        data.append(row)
    rows = []
    # External DOIs
    payload = JOURNAL_ARTICLE if which == "journal" else {}
    try:
        cnt = DB['dis'].external_dois.count_documents(payload)
        if cnt:
            rows = DB['dis'].external_dois.find(payload, projection)
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    for row in rows:
        row['doi_type'] = 'external'
        data.append(row)
    data.sort(key=lambda x: x.get("jrc_publishing_date", ""), reverse=True)
    result['acknowledgements'] = data
    result['rest']['row_count'] = len(data)
    return generate_response(result)


def _build_ack_metrics(limit=10):
    ''' Build the "Stored acknowledgements" tab content for the acknowledgement
        metrics page - the acknowledgement corpus stats for the dois and
        external_dois collections: summary cards, top-journal tables, and the
        by-type pie charts (the year/heatmap views live on the "By year" tab).
        Returns (html, chartscript, chartdiv) for a tab.
        Keyword arguments:
          limit: top-N limit for the journal tables
        Returns:
          (html, chartscript, chartdiv) tuple
    '''
    ack_filter = {"jrc_acknowledgements": {"$exists": True}}
    # Type breakdown - internal DOIs
    pipeline = [
        {"$match": ack_filter},
        {"$group": {
            "_id": {
                "$cond": [
                    {"$ifNull": ["$type", False]},
                    "$type",
                    {"$ifNull": ["$types.resourceTypeGeneral", "Unknown"]}
                ]
            },
            "count": {"$sum": 1}
        }},
        {"$sort": {"count": -1}}
    ]
    try:
        rows = DB['dis'].dois.aggregate(pipeline)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get acknowledgement type " \
                                                    + "data from dois"),
                               message=error_message(err))
    dois_type_data = {}
    for row in rows:
        label = row['_id'] if row['_id'] else 'Unknown'
        dois_type_data[label] = row['count']
    # Type breakdown - external DOIs
    pipeline_ext = [
        {"$match": ack_filter},
        {"$group": {"_id": "$type", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    try:
        rows = DB['dis'].external_dois.aggregate(pipeline_ext)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get acknowledgement type data" \
                                                    + " from external_dois"),
                               message=error_message(err))
    ext_type_data = {}
    for row in rows:
        label = row['_id'] if row['_id'] else 'Unknown'
        ext_type_data[label] = row['count']
    # Collection totals for percentage calculation, broken down by jrc_obtained_from for dois
    try:
        dois_all = DB['dis'].dois.count_documents({})
        source_rows = DB['dis'].dois.aggregate([
            {"$group": {"_id": "$jrc_obtained_from", "count": {"$sum": 1}}}
        ])
        source_all = {row['_id']: row['count'] for row in source_rows}
        source_rows = DB['dis'].dois.aggregate([
            {"$match": ack_filter},
            {"$group": {"_id": "$jrc_obtained_from", "count": {"$sum": 1}}}
        ])
        source_ack = {row['_id']: row['count'] for row in source_rows}
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get collection totals"),
                               message=error_message(err))
    # Top journals (internal and external DOIs separately)
    journal_pipeline = [
        {"$match": {**ack_filter, "jrc_journal": {"$exists": True}}},
        {"$group": {"_id": "$jrc_journal", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": limit}
    ]
    try:
        int_journals = [(row['_id'], row['count'])
                        for row in DB['dis'].dois.aggregate(journal_pipeline)]
        ext_journals = [(row['_id'], row['count'])
                        for row in DB['dis'].external_dois.aggregate(journal_pipeline)]
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get top journal data"),
                               message=error_message(err))
    # Build HTML
    def summary_card(label, cnt, total):
        pct = cnt / total * 100 if total else 0
        return (label, f"{cnt:,}<div style='font-size:0.62em; font-weight:normal; "
                       + f"color:#a8c4e0;'>of {total:,} ({pct:.1f}%)</div>")
    dois_total = sum(dois_type_data.values())
    ext_total = sum(ext_type_data.values())
    cardlist = [summary_card("Internal DOIs", dois_total, dois_all)]
    for source in sorted(source_all):
        cardlist.append(summary_card(escape(source), source_ack.get(source, 0),
                                     source_all[source]))
    # Every external_dois record has acknowledgements, so a percentage
    # would always read 100% - just show the count.
    cardlist.append(("External DOIs", f"{ext_total:,}"))
    # Summary cards, then the top-journal tables (type distribution is shown by
    # the pie charts, so no separate by-type table).
    summary = stat_cards(cardlist, div_id='ack-summary')
    html = summary
    jcols = ""
    if int_journals:
        jtable = render_table(['Journal', 'Count'], [[j, f"{c:,}"] for j, c in int_journals],
                              table_id='top_journals_int', css='tablesorter numberlast-scroll')
        jcols += "<div class='flexcol' style='margin-right: 30px'>" \
                 + f"<h4>Top {limit} journals (Internal DOIs)</h4>{jtable}</div>"
    if ext_journals:
        jtable = render_table(['Journal', 'Count'], [[j, f"{c:,}"] for j, c in ext_journals],
                              table_id='top_journals_ext', css='tablesorter numberlast-scroll')
        jcols += "<div class='flexcol'>" \
                 + f"<h4>Top {limit} journals (External DOIs)</h4>{jtable}</div>"
    if jcols:
        html += f"<div class='flexrow'>{jcols}</div>"
    # Charts: two pies side-by-side, bar chart below
    chartscript = ""
    pie_divs = ""
    if dois_type_data:
        colors = DP.get_colors_by_count(len(dois_type_data))
        dois_sorted = dict(sorted(dois_type_data.items(), key=itemgetter(1), reverse=True))
        s, d = DP.pie_chart(dois_sorted, "Internal DOIs by type", "type",
                            width=500, height=450, colors=colors)
        chartscript += s
        pie_divs += f"<div class='flexcol'>{d}</div>"
    if ext_type_data:
        colors2 = DP.get_colors_by_count(len(ext_type_data))
        ext_sorted = dict(sorted(ext_type_data.items(), key=itemgetter(1), reverse=True))
        s, d = DP.pie_chart(ext_sorted, "External DOIs by type", "type",
                            width=500, height=450, colors=colors2)
        chartscript += s
        pie_divs += f"<div class='flexcol'>{d}</div>"
    chartdiv = f"<div class='flexrow'>{pie_divs}</div><br>"
    return html, chartscript, chartdiv


def _build_ack_sources():
    ''' Build the "Sources" tab: the jrc_ack_source distribution (PMC, eLife,
        Elsevier, arXiv, Europe PMC, Manual, ...) split by Internal (dois) vs
        External (external_dois). A "(source not recorded)" row accounts for
        acknowledgement text stored before source tracking, so the totals reconcile
        with the coverage numbers on this page. Returns (html, chartscript, chartdiv).
        Keyword arguments:
          None
        Returns:
          (html, chartscript, chartdiv) tuple
    '''
    sources = {}                              # source label -> {'Internal', 'External'}
    tot = {'Internal': 0, 'External': 0}      # named-source totals per collection
    unrec = {'Internal': 0, 'External': 0}    # ack text with no jrc_ack_source
    for coll_name, col in (('dois', 'Internal'), ('external_dois', 'External')):
        coll = DB['dis'][coll_name]
        for row in coll.aggregate([{"$match": {"jrc_ack_source": {"$exists": True}}},
                                   {"$group": {"_id": "$jrc_ack_source",
                                               "count": {"$sum": 1}}}]):
            src = row['_id']
            sources.setdefault(src, {'Internal': 0, 'External': 0})[col] += row['count']
            tot[col] += row['count']
        unrec[col] = coll.count_documents({"jrc_acknowledgements": {"$exists": True},
                                           "jrc_ack_source": {"$exists": False}})
    grand_int = tot['Internal'] + unrec['Internal']
    grand_ext = tot['External'] + unrec['External']
    src_cards = stat_cards([("Internal", f"{grand_int:,}"),
                            ("External", f"{grand_ext:,}"),
                            ("Total", f"{grand_int + grand_ext:,}")], div_id='src-stats')
    order = sorted(sources, key=lambda s: sources[s]['Internal'] + sources[s]['External'],
                   reverse=True)
    trows = []
    for src in order:
        i, e = sources[src]['Internal'], sources[src]['External']
        trows.append([escape(str(src)), f"{i:,}", f"{e:,}", f"{i + e:,}"])
    if unrec['Internal'] or unrec['External']:
        i, e = unrec['Internal'], unrec['External']
        trows.append([safe("<span style='color:#a8c4e0;'>(source not recorded)</span>"),
                      f"{i:,}", f"{e:,}", f"{i + e:,}"])
    footer = [fcell('Total'), fcell(f"{grand_int:,}", align='center'),
              fcell(f"{grand_ext:,}", align='center'),
              fcell(f"{grand_int + grand_ext:,}", align='center')]
    src_table = render_table(['Source', 'Internal', 'External', 'Total'], trows,
                             table_id='acksources', css='tablesorter numbers-scroll',
                             footer=footer, width=500)
    caption = ("<div style='color:#a8c4e0;font-size:0.82em;margin:6px 0 8px 0;'>"
               "Where each stored acknowledgement was fetched from, "
               "for Internal DOIs (dois) and External DOIs (external_dois). "
               "&ldquo;(source not recorded)&rdquo; = acknowledgement text stored "
               "before source tracking.</div>")
    html = src_cards + caption + src_table
    chartscript = chartdiv = ""
    chart_data = {src: {'Internal': sources[src]['Internal'],
                        'External': sources[src]['External']} for src in order}
    if chart_data:
        chartscript, chartdiv = DP.hbar_stacked_chart(
            chart_data, ['Internal', 'External'], "Acknowledgement sources",
            value_label="DOIs", value_format="0,0")
    return html, chartscript, chartdiv


def _build_ack_byyear():
    ''' Build the "By year" tab: the temporal view of the acknowledgement corpus,
        keyed on jrc_publishing_date year -
          1. Acknowledgements by year (Internal vs External)
          2. Internal DOI tagging coverage by year (Tagged vs Untagged)
          3. Acknowledgement sources by year (jrc_ack_source, both collections)
          4. DOIs with acknowledgements by year x type (heat map)
        Returns (html, chartscript, chartdiv) for the tab.
        Keyword arguments:
          None
        Returns:
          (html, chartscript, chartdiv) tuple
    '''
    yr = {"$substr": ["$jrc_publishing_date", 0, 4]}
    have_date = {"jrc_acknowledgements": {"$exists": True},
                 "jrc_publishing_date": {"$exists": True}}
    chartscript = chartdiv = ""
    # 1. Acknowledgements by year: Internal vs External
    yp = [{"$match": have_date}, {"$group": {"_id": yr, "count": {"$sum": 1}}},
          {"$sort": {"_id": 1}}]
    int_years = {r['_id']: r['count'] for r in DB['dis'].dois.aggregate(yp)}
    ext_years = {r['_id']: r['count'] for r in DB['dis'].external_dois.aggregate(yp)}
    years = sorted(set(int_years) | set(ext_years))
    if years:
        data = {"years": years,
                "Internal DOIs": [int_years.get(y, 0) for y in years],
                "External DOIs": [ext_years.get(y, 0) for y in years]}
        s, d = DP.stacked_bar_chart(data, "Acknowledgements by year", xaxis="years",
                                    yaxis=["Internal DOIs", "External DOIs"],
                                    colors=DP.SOURCE_PALETTE, orient=pi / 4,
                                    width=1000, height=350)
        chartscript += s
        chartdiv += d + "<br>"
    # 2. Internal DOI tagging coverage by year: Tagged vs Untagged
    cov = [r for r in DB['dis'].dois.aggregate([
        {"$match": have_date},
        {"$project": {"yr": yr, "tagged": {"$cond": [{"$gt": [
            {"$size": {"$ifNull": ["$jrc_acknowledge", []]}}, 0]}, 1, 0]}}},
        {"$group": {"_id": "$yr", "all": {"$sum": 1}, "tagged": {"$sum": "$tagged"}}},
        {"$sort": {"_id": 1}}]) if r['_id']]
    if cov:
        data = {"years": [r['_id'] for r in cov],
                "Tagged": [r['tagged'] for r in cov],
                "Untagged": [r['all'] - r['tagged'] for r in cov]}
        # Distinct vivid palette (green/purple) so tagging coverage is not visually
        # conflated with the blue/orange Internal/External split in the chart above.
        s, d = DP.stacked_bar_chart(data, "Internal DOI tagging coverage by year",
                                    xaxis="years", yaxis=["Tagged", "Untagged"],
                                    colors=["#2ca02c", "#9c27b0"], orient=pi / 4,
                                    width=1000, height=350)
        chartscript += s
        chartdiv += d + "<br>"
    # 3. Acknowledgement sources by year (both collections)
    src_year = {}
    totals = {}
    sp = [{"$match": {"jrc_ack_source": {"$exists": True},
                      "jrc_publishing_date": {"$exists": True}}},
          {"$group": {"_id": {"yr": yr, "src": "$jrc_ack_source"}, "count": {"$sum": 1}}}]
    for coll_name in ('dois', 'external_dois'):
        for r in DB['dis'][coll_name].aggregate(sp):
            y, sname = r['_id']['yr'], r['_id']['src']
            src_year.setdefault(y, {})
            src_year[y][sname] = src_year[y].get(sname, 0) + r['count']
            totals[sname] = totals.get(sname, 0) + r['count']
    if src_year:
        syears = sorted(src_year)
        src_order = sorted(totals, key=totals.get, reverse=True)
        data = {"years": syears}
        for sname in src_order:
            data[sname] = [src_year[y].get(sname, 0) for y in syears]
        s, d = DP.stacked_bar_chart(data, "Acknowledgement sources by year",
                                    xaxis="years", yaxis=src_order,
                                    colors=DP.get_colors_by_count(len(src_order)),
                                    orient=pi / 4, width=1000, height=350)
        chartscript += s
        chartdiv += d + "<br>"
    # 4. Year x type heat map (both collections)
    hm = {}
    for r in DB['dis'].dois.aggregate([
            {"$match": have_date},
            {"$group": {"_id": {"year": yr, "type": {"$cond": [
                {"$ifNull": ["$type", False]}, "$type",
                {"$ifNull": ["$types.resourceTypeGeneral", "Unknown"]}]}},
                "count": {"$sum": 1}}}]):
        k = (r['_id']['year'], r['_id']['type'])
        hm[k] = hm.get(k, 0) + r['count']
    for r in DB['dis'].external_dois.aggregate([
            {"$match": have_date},
            {"$group": {"_id": {"year": yr, "type": {"$ifNull": ["$type", "Unknown"]}},
                        "count": {"$sum": 1}}}]):
        k = (r['_id']['year'], r['_id']['type'])
        hm[k] = hm.get(k, 0) + r['count']
    heatmap_data = {"Year": [], "Type": [], "Count": []}
    for (year, typ), count in hm.items():
        heatmap_data["Year"].append(year)
        heatmap_data["Type"].append(typ)
        heatmap_data["Count"].append(count)
    if heatmap_data["Count"]:
        s, d = DP.heat_map(heatmap_data, "DOIs with acknowledgements by year/type",
                           "Year", "Type", "Count", value_format="0,0",
                           width=1000, col_totals="Total")
        chartscript += s
        chartdiv += d
    caption = ("<div style='color:#a8c4e0;font-size:0.82em;margin-bottom:10px;'>"
               "The acknowledgement corpus by publishing year: volume, tagging "
               "coverage, source mix, and type composition.</div>")
    return caption, chartscript, chartdiv


def _build_ack_heatmap():
    ''' Build the "Heatmap" tab: an acknowledgement-entity (row) x publishing-year
        (column) heat map. Each cell is the total number of acknowledgements of that
        entity in that year across BOTH collections (dois + external_dois) and ALL
        curators - i.e. Internal+External, which equals Human+IRIS (they partition the
        same set, so this is not a sum of all four). The row set is every entity in the
        jrc_acknowledge corpus (a small controlled vocabulary), so no top-N cutoff is
        needed. DOIs with no jrc_publishing_date cannot be placed on a year axis and are
        excluded.
        Keyword arguments:
          None
        Returns:
          (html, chartscript, chartdiv) tuple
    '''
    yr = {"$substr": ["$jrc_publishing_date", 0, 4]}
    payload = [{"$match": {"jrc_publishing_date": {"$exists": True}}},
               {"$unwind": "$jrc_acknowledge"},
               {"$group": {"_id": {"tag": "$jrc_acknowledge.name", "yr": yr},
                           "count": {"$sum": 1}}}]
    per = {}          # tag -> {year -> count}
    yrset = set()
    for coll_name in ('dois', 'external_dois'):
        for row in DB['dis'][coll_name].aggregate(payload):
            tag = row['_id'].get('tag')
            year = row['_id'].get('yr')
            if not (tag and year):
                continue
            entry = per.setdefault(tag, {})
            entry[year] = entry.get(year, 0) + row['count']
            yrset.add(year)
    caption = ("<div style='color:#a8c4e0;font-size:0.82em;margin-bottom:10px;'>"
               "Acknowledgement counts by entity and publishing year, summed across "
               "internal (<code>dois</code>) and external (<code>external_dois</code>) "
               "DOIs and all curators. Cell color = number of acknowledgements (scale at "
               "right); hover for exact numbers; the Total row/column give per-year and "
               "per-entity sums, and the largest total in each is outlined. DOIs without a "
               "publishing date are excluded.</div>")
    if not yrset:
        return caption + "<div>No dated acknowledgements.</div>", "", ""
    years = sorted(yrset)
    hx, hy, hval = [], [], []
    for tag, ymap in per.items():
        for year in years:
            cnt = ymap.get(year, 0)
            if cnt:
                hx.append(year)
                hy.append(tag)
                hval.append(cnt)
    chartscript = chartdiv = ""
    if hval:
        chartscript, chartdiv = DP.heat_map(
            {"Year": hx, "Acknowledgement": hy, "Acks": hval},
            "Acknowledgements by tag and publishing year",
            x_field="Year", y_field="Acknowledgement", value_field="Acks",
            width=1000, value_format="0,0",
            col_totals="Total", row_totals="Total", highlight_max_total=True)
    return caption, chartscript, chartdiv


@app.route('/acknowledgement_metrics/<int:limit>')
@app.route('/acknowledgement_metrics')
def show_acknowledgement_metrics(limit=10):
    ''' Acknowledgement metrics - a tabbed page over the acknowledgement corpus:
        the Stored acknowledgements tab (by type/year/journal), the Sources tab
        (jrc_ack_source split Internal vs External), the Tags tab (tagging
        coverage and per-tag counts), and the Heatmap tab (per-entity x publishing-year
        acknowledgement counts across both collections).
        Replaces the former /acknowledgement_stats and /dois_tag_ack/ack pages.
        Keyword arguments:
          limit: top-N limit for the Metrics-tab journal tables
    '''
    # Entities: aggregate jrc_acknowledge over dois (Internal) + external_dois (External)
    payload = [{"$unwind": "$jrc_acknowledge"},
               {"$project": {"_id": 0, "jrc_acknowledge.name": 1, "jrc_acknowledge.curator": 1}},
               {"$group": {"_id": {"tag": "$jrc_acknowledge.name",
                                   "curator": "$jrc_acknowledge.curator"},
                           "count": {"$sum": 1}}},
               {"$sort": {"_id.tag": 1}}]
    try:
        orgs = DL.get_supervisory_orgs(DB['dis'].suporg)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get supervisory orgs"),
                               message=error_message(err))
    regex_keys = set(DB['dis'].search_regex.distinct('key'))
    tags = {}
    for coll_name, doi_type_col in (('dois', 'Internal'), ('external_dois', 'External')):
        try:
            rows = DB['dis'][coll_name].aggregate(payload)
        except Exception as err:
            return render_template('error.html', urlroot=request.url_root,
                                   title=render_warning(f"Could not get acknowledgements from "
                                                        f"{coll_name}"),
                                   message=error_message(err))
        for row in rows:
            tag = row['_id'].get('tag')
            entry = tags.setdefault(tag, {})
            entry[doi_type_col] = entry.get(doi_type_col, 0) + row['count']
            curator_col = 'IRIS' if row['_id'].get('curator') == 'IRIS' else 'Human'
            entry[curator_col] = entry.get(curator_col, 0) + row['count']
    cols = ['Internal', 'External', 'IRIS', 'Human']
    trows = []
    row_classes = []
    total = {c: 0 for c in cols}
    active = 0
    for tag, val in tags.items():
        rclass = 'other'
        if tag in orgs:
            if 'active' in orgs[tag]:
                org = "<span style='color: lime;'>Yes</span>"
                rclass = 'active'
                active += 1
            else:
                org = "<span style='color: yellow;'>Inactive</span>"
        else:
            org = "<span style='color: red;'>No</span>"
        cells = [safe(f"<a href='/tag/{escape(tag)}'>{tag}</a>"), safe(org)]
        for col in cols:
            count = val.get(col, 0)
            if col in ('Internal', 'External') and count and tag in regex_keys:
                href = f"/acksregexui/{quote(tag, safe='')}?doi_type={col.lower()}"
                cell_html = f"<a href='{href}'>{count:,}</a>"
            else:
                cell_html = f"{count:,}" if count else ""
            cells.append(safe(cell_html))
            total[col] += count
        trows.append(cells)
        row_classes.append(rclass)
    # Numeric footer cells carry data-sum-col (0-based body column) so the active-
    # SupOrg filter (filterTagTable) re-sums them over the visible rows. The id must
    # be unique on the page (the tab pane is id='tags'), so use 'acktags'.
    footer = [fcell('Total', colspan=2)] + [
        safe(f"<th style='text-align: center;' data-sum-col='{i + 2}'>{total[c]:,}</th>")
        for i, c in enumerate(cols)]
    entity_table = render_table(['Acknowledgement', 'SupOrg'] + cols, trows,
                                table_id='acktags', css='tablesorter numbers-scroll',
                                row_classes=row_classes, footer=footer)
    cbutton = ("<button id=\"acktags-filter\" class=\"btn btn-outline-warning\" "
               "onclick=\"filterTagTable('acktags','acktags-filter','other',"
               "'Show all tags','Filter for active SupOrgs')\">"
               "Filter for active SupOrgs</button>")
    ent_cards = stat_cards([("Acknowledgements", f"{len(tags):,}"),
                            ("Active SupOrgs", f"{active:,}")], div_id='ent-stats')
    ent_chartscript = ent_chartdiv = ""
    if tags:
        chart_totals = {t: v.get('Internal', 0) + v.get('External', 0) for t, v in tags.items()}
        top_n = 15
        top_tags = sorted(chart_totals, key=chart_totals.get, reverse=True)[:top_n]
        chart_data = {t: {'Internal': tags[t].get('Internal', 0),
                          'External': tags[t].get('External', 0)} for t in top_tags}
        ctitle = "Top acknowledged entities"
        if len(chart_totals) > top_n:
            ctitle += f" (top {top_n} of {len(chart_totals):,})"
        nav = {t: f"/tag/{quote(t, safe='')}" for t in chart_data}
        ent_chartscript, ent_chartdiv = DP.hbar_stacked_chart(
            chart_data, ['Internal', 'External'], ctitle, value_label="DOIs",
            value_format="0,0", nav=nav)
    # Coverage 2x2 (shared header, above the tabs) - the bridge between corpus and entities
    base = {"jrc_acknowledgements": {"$exists": True}}
    tagged_query = {**base, "jrc_acknowledge.0": {"$exists": True}}
    cov = {}
    try:
        for coll_name, label in (('dois', 'Internal'), ('external_dois', 'External')):
            ctot = DB['dis'][coll_name].count_documents(base)
            ctag = DB['dis'][coll_name].count_documents(tagged_query)
            cov[label] = (ctag, ctot - ctag, ctot)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not compute acknowledgement coverage"),
                               message=error_message(err))
    def pct(num, den):
        return f"{num / den * 100:.1f}%" if den else "0.0%"
    cov_rows = [[label, f"{t:,} ({pct(t, tot)})", f"{u:,} ({pct(u, tot)})", f"{tot:,}"]
                for label, (t, u, tot) in cov.items()]
    gt = sum(v[0] for v in cov.values())
    gu = sum(v[1] for v in cov.values())
    gg = sum(v[2] for v in cov.values())
    cov_footer = [fcell('Total'), fcell(f"{gt:,} ({pct(gt, gg)})", align='center'),
                  fcell(f"{gu:,} ({pct(gu, gg)})", align='center'),
                  fcell(f"{gg:,}", align='center')]
    cov_table = render_table(['Collection', 'Tagged', 'Untagged', 'Total'], cov_rows,
                             table_id='ackcoverage', css='tablesorter numbers-scroll',
                             footer=cov_footer, width=460)
    coverage_html = ("<h5 style='margin:14px 0 6px 0;'>Acknowledgement tagging coverage</h5>"
                     "<div style='color:#a8c4e0;font-size:0.82em;margin-bottom:8px;'>"
                     "Internal = DOIs with acknowledgement text; External = all external_dois. "
                     "&ldquo;Tagged&rdquo; = at least one tagged entity.</div>" + cov_table)
    # Tagging gaps: acknowledgements that name "Janelia" but carry no entity tag -
    # likely missed attributions worth reviewing for entity/regex coverage.
    gap_query = {"jrc_acknowledgements": {"$regex": "janelia", "$options": "i"},
                 "jrc_acknowledge.0": {"$exists": False}}
    try:
        gap_int = DB['dis'].dois.count_documents(gap_query)
        gap_ext = DB['dis'].external_dois.count_documents(gap_query)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not compute tagging gaps"),
                               message=error_message(err))
    if gap_int or gap_ext:
        coverage_html += (
            "<div style='margin:10px 0 4px 0;padding:8px 12px;border-left:4px solid "
            "#f0ad4e;background:rgba(240,173,78,0.08);font-size:0.9em;'>"
            f"<b>Tagging gaps:</b> {gap_int:,} Internal and {gap_ext:,} External "
            "acknowledgements name &ldquo;Janelia&rdquo; but have no entity tag &ndash; "
            "<a href='/acks_untagged'>review the list</a> to improve entity/regex "
            "coverage.</div>")
    # Tab builders: Stored (corpus stats), By year (temporal), Sources (jrc_ack_source)
    m_html, m_chartscript, m_chartdiv = _build_ack_metrics(limit)
    by_html, by_chartscript, by_chartdiv = _build_ack_byyear()
    s_html, s_chartscript, s_chartdiv = _build_ack_sources()
    h_html, h_chartscript, h_chartdiv = _build_ack_heatmap()
    # Tabs
    active_tab = request.args.get('tab')
    if active_tab not in ('metrics', 'byyear', 'sources', 'tags', 'heatmap'):
        active_tab = 'metrics'
    metrics_body = m_html + m_chartdiv
    byyear_body = by_html + by_chartdiv
    sources_body = s_html + s_chartdiv
    heatmap_body = h_html + h_chartdiv
    # Table and top-entities chart side by side (mirrors the /tag_metrics Tags tab):
    # the table scrolls within a fixed-height pane, the chart sits beside it, and
    # the row wraps to stacked on a narrow viewport. The ack table is 6 columns, so
    # give it a wider basis and an x-scroll guard.
    ack_table_block = ("<div class='tag-scrollbox' style='flex:1 1 560px;min-width:400px;"
                       "overflow-x:auto;'>" + entity_table + "</div>")
    ack_chart_block = f"<div style='flex:0 0 auto;'>{ent_chartdiv}</div>" if ent_chartdiv else ""
    tags_body = (ent_cards + f"<div style='margin-bottom:8px;'>{cbutton}</div>"
                 + two_col(ack_table_block, ack_chart_block))
    tabs = ('<ul class="nav nav-tabs" role="tablist">'
            + tab_button('metrics', 'Stored acknowledgements', active_tab == 'metrics')
            + tab_button('byyear', 'By year', active_tab == 'byyear')
            + tab_button('sources', 'Sources', active_tab == 'sources')
            + tab_button('tags', 'Tags', active_tab == 'tags')
            + tab_button('heatmap', 'Heatmap', active_tab == 'heatmap')
            + '</ul><div class="tab-content">'
            + tab_pane('metrics', metrics_body, active_tab == 'metrics')
            + tab_pane('byyear', byyear_body, active_tab == 'byyear')
            + tab_pane('sources', sources_body, active_tab == 'sources')
            + tab_pane('tags', tags_body, active_tab == 'tags')
            + tab_pane('heatmap', heatmap_body, active_tab == 'heatmap')
            + '</div>')
    seealso = see_also([("DOI metrics", "/dois_metrics"),
                        ("Tags", "/tag_metrics"),
                        ("Impact by source", "/source_metrics")])
    html = (coverage_html + tabs + seealso + m_chartscript + by_chartscript
            + s_chartscript + ent_chartscript + h_chartscript)
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title="Acknowledgement metrics", html=html, bokeh=True,
                                         navbar=generate_navbar('Acknowledgements')))


@app.route('/tag_metrics/<int:limit>')
@app.route('/tag_metrics')
def show_tag_metrics(limit=15):
    ''' Tag metrics - a tabbed page over the jrc_tag field on the dois collection
        (Janelia author affiliation/lab/project tags; external_dois carry none).
        Tabs: Tags (per-tag Crossref/DataCite counts + top-tags chart), By year
        (tagging coverage + tagged-DOI volume by publishing year), Trends (per-tag
        heat maps by year, one per registrar), and Co-tagged (which tags share
        DOIs - top pairs table + a tag x tag co-occurrence heat map). A shared
        coverage header and an actionable untagged-DOIs callout sit above the tabs.
        Keyword arguments:
          limit: top-N limit for the top-tags chart and heat maps
        Returns:
          Response
    '''
    coll = DB['dis'].dois
    try:
        orgs = DL.get_supervisory_orgs(DB['dis'].suporg)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get supervisory orgs"),
                               message=error_message(err))
    # Coverage: tagged vs untagged DOIs, split by registrar (jrc_obtained_from)
    try:
        cov = {}
        for src in ('Crossref', 'DataCite'):
            tot = coll.count_documents({"jrc_obtained_from": src})
            tag = coll.count_documents({"jrc_obtained_from": src,
                                        "jrc_tag.0": {"$exists": True}})
            cov[src] = (tag, tot - tag, tot)
        # Per-tag Crossref/DataCite counts
        agg = list(coll.aggregate([{"$match": {"jrc_tag.0": {"$exists": True}}},
                                   {"$unwind": "$jrc_tag"},
                                   {"$group": {"_id": {"name": "$jrc_tag.name",
                                                       "src": "$jrc_obtained_from"},
                                               "count": {"$sum": 1}}}]))
        yr = {"$substr": ["$jrc_publishing_date", 0, 4]}
        yagg = list(coll.aggregate([
            {"$match": {"jrc_tag.0": {"$exists": True},
                        "jrc_publishing_date": {"$exists": True}}},
            {"$group": {"_id": {"yr": yr, "src": "$jrc_obtained_from"}, "n": {"$sum": 1}}}]))
        # Tagging coverage by year: tagged vs untagged (all dated Janelia DOIs)
        cyagg = list(coll.aggregate([
            {"$match": {"jrc_publishing_date": {"$exists": True}}},
            {"$project": {"yr": yr, "tagged": {"$cond": [{"$gt": [
                {"$size": {"$ifNull": ["$jrc_tag", []]}}, 0]}, 1, 0]}}},
            {"$group": {"_id": "$yr", "all": {"$sum": 1}, "tagged": {"$sum": "$tagged"}}}]))
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not compute tag metrics"),
                               message=error_message(err))

    def pct(num, den):
        return f"{num / den * 100:.1f}%" if den else "0.0%"
    # ----- shared coverage header -----
    cov_rows = [[src, f"{t:,} ({pct(t, tot)})", f"{u:,} ({pct(u, tot)})", f"{tot:,}"]
                for src, (t, u, tot) in cov.items()]
    gt = sum(v[0] for v in cov.values())
    gu = sum(v[1] for v in cov.values())
    gg = sum(v[2] for v in cov.values())
    cov_footer = [fcell('Total'), fcell(f"{gt:,} ({pct(gt, gg)})", align='center'),
                  fcell(f"{gu:,} ({pct(gu, gg)})", align='center'), fcell(f"{gg:,}", align='center')]
    cov_table = render_table(['Registrar', 'Tagged', 'Untagged', 'Total'], cov_rows,
                             table_id='tagcoverage', css='tablesorter numbers-scroll',
                             footer=cov_footer, width=460)
    coverage_html = ("<h5 style='margin:14px 0 6px 0;'>Tag coverage</h5>"
                     "<div style='color:#a8c4e0;font-size:0.82em;margin-bottom:8px;'>"
                     "Tags apply to Janelia-authored DOIs only. "
                     "&ldquo;Tagged&rdquo; = at least one tag.</div>" + cov_table)

    # Untagged actionable callout: drill into the untagged DOIs via /doiui/custom
    # (POST form; !NOTEXISTS! -> {"$exists": 0} on jrc_tag.0, matching the coverage
    # untagged counts). One button per non-empty registrar plus an "All" button.
    def untagged_button(label, count, src=None):
        src_input = (f'<input type="hidden" name="jrc_obtained_from" value="{src}">'
                     if src else '')
        return ('<form method="POST" action="/doiui/custom" style="display:inline;'
                'margin:0 6px 0 0;">'
                '<input type="hidden" name="field" value="jrc_tag.0">'
                '<input type="hidden" name="value" value="!NOTEXISTS!">'
                '<input type="hidden" name="title" value="Untagged">' + src_input
                + '<button type="submit" class="btn btn-sm btn-outline-warning" '
                f'style="padding:1px 8px;">{label}: {count:,}</button></form>')
    if gu:
        buttons = untagged_button("All", gu)
        for src in ('Crossref', 'DataCite'):
            if cov[src][1]:
                buttons += untagged_button(src, cov[src][1], src)
        coverage_html += (
            "<div style='margin:10px 0 4px 0;padding:8px 12px;border-left:4px solid "
            "#f0ad4e;background:rgba(240,173,78,0.08);font-size:0.9em;'>"
            f"<b>Untagged:</b> {gu:,} Janelia DOIs carry no tags. Review "
            "&ndash; " + buttons + "</div>")
    # ----- Tags tab: per-tag table + top-tags chart -----
    tags = {}
    for row in agg:
        name = row['_id'].get('name')
        if not name:
            continue
        entry = tags.setdefault(name, {'Crossref': 0, 'DataCite': 0})
        if row['_id'].get('src') in ('Crossref', 'DataCite'):
            entry[row['_id']['src']] += row['count']
    trows = []
    row_classes = []
    active = 0
    tot_cr = tot_dc = 0

    def count_cell(cnt, name, source):
        ''' A tag/registrar DOI count rendered as a drill-down link: nav_post posts
            to /doiui/custom for jrc_tag.name = name filtered to that registrar.
            Zero counts stay as plain text (no empty result page). '''
        if not cnt:
            return f"{cnt:,}"
        # str(escape(...)) keeps the name HTML-safe but leaves the attribute's own
        # quotes literal (a bare Markup would escape those too and break the tag).
        onclick = ("onclick='nav_post(\"jrc_tag.name\",\"" + str(escape(name))
                   + "\",\"" + source + "\")'")
        return f"<a href='#' {onclick}>{cnt:,}</a>"
    for name in sorted(tags, key=lambda n: tags[n]['Crossref'] + tags[n]['DataCite'], reverse=True):
        cri = tags[name]['Crossref']
        dci = tags[name]['DataCite']
        tot_cr += cri
        tot_dc += dci
        rclass = 'other'
        if name in orgs:
            if 'active' in orgs[name]:
                org = "<span style='color: lime;'>Yes</span>"
                rclass = 'active'
                active += 1
            else:
                org = "<span style='color: yellow;'>Inactive</span>"
        else:
            org = "<span style='color: red;'>No</span>"
        link = f"<a href='/tag/{quote(name, safe='')}'>{escape(name)}</a>"
        trows.append([safe(link), safe(org), safe(count_cell(cri, name, 'Crossref')),
                      safe(count_cell(dci, name, 'DataCite')), f"{cri + dci:,}"])
        row_classes.append(rclass)
    # Numeric footer cells carry data-sum-col (0-based body column) so the active-
    # SupOrg filter (filterTagTable) can re-sum them over the visible rows.
    tag_footer = [fcell('Total', colspan=2),
                  safe(f"<th style='text-align: center;' data-sum-col='2'>{tot_cr:,}</th>"),
                  safe(f"<th style='text-align: center;' data-sum-col='3'>{tot_dc:,}</th>"),
                  safe("<th style='text-align: center;' data-sum-col='4'>"
                       f"{tot_cr + tot_dc:,}</th>")]
    # id must NOT be 'tags' - the Bootstrap tab pane already uses id='tags', and a
    # duplicate id makes $('#tags ...') resolve to the pane, breaking the footer re-sum.
    tag_table = render_table(['Tag', 'SupOrg', 'Crossref', 'DataCite', 'Total'], trows,
                             table_id='tagtable', css='tablesorter numbers-scroll',
                             row_classes=row_classes, footer=tag_footer)
    entries = tot_cr + tot_dc
    avg_tags = entries / gt if gt else 0
    tag_cards = stat_cards([("Distinct tags", f"{len(tags):,}"),
                            ("Active SupOrgs", f"{active:,}"),
                            ("Avg tags / tagged DOI", f"{avg_tags:.2f}")], div_id='tag-stats')
    tag_note = ("<div style='color:#a8c4e0;font-size:0.82em;margin:6px 0 8px 0;'>"
                "Counts below are tag <b>entries</b>, not DOIs: a DOI can carry several "
                f"tags (avg {avg_tags:.2f} per tagged DOI), so the column totals "
                f"({entries:,}) exceed the {gt:,} tagged DOIs in the coverage table above."
                "</div>")
    cbutton = ("<button id=\"tags-filter\" class=\"btn btn-outline-warning\" "
               "onclick=\"filterTagTable('tagtable','tags-filter','other',"
               "'Show all tags','Filter for active SupOrgs')\">"
               "Filter for active SupOrgs</button>")
    tag_chartscript = tag_chartdiv = ""
    if tags:
        totals = {n: v['Crossref'] + v['DataCite'] for n, v in tags.items()}
        top = sorted(totals, key=totals.get, reverse=True)[:limit]
        chart_data = {n: {'Crossref': tags[n]['Crossref'], 'DataCite': tags[n]['DataCite']}
                      for n in top}
        ctitle = "Top tags"
        if len(totals) > limit:
            ctitle += f" (top {limit} of {len(totals):,})"
        nav = {n: f"/tag/{quote(n, safe='')}" for n in chart_data}
        tag_chartscript, tag_chartdiv = DP.hbar_stacked_chart(
            chart_data, ['Crossref', 'DataCite'], ctitle, value_label="DOIs",
            value_format="0,0", nav=nav)
    # Table and top-tags chart sit side by side (chart is a fixed 650x450 block,
    # the table scrolls within a matching height) to save vertical space; the row
    # wraps to stacked on narrow viewports.
    table_block = ("<div class='tag-scrollbox' style='flex:1 1 560px;min-width:360px;'>"
                   f"{tag_table}</div>")
    chart_block = f"<div style='flex:0 0 auto;'>{tag_chartdiv}</div>" if tag_chartdiv else ""
    tags_body = (tag_cards + tag_note + f"<div style='margin-bottom:8px;'>{cbutton}</div>"
                 + two_col(table_block, chart_block))
    # ----- By year tab: tagged DOIs per publishing year, Crossref vs DataCite -----
    ymap = {}
    for row in yagg:
        year = row['_id'].get('yr')
        if not year:
            continue
        entry = ymap.setdefault(year, {'Crossref': 0, 'DataCite': 0})
        if row['_id'].get('src') in ('Crossref', 'DataCite'):
            entry[row['_id']['src']] += row['n']
    # Coverage-by-year (tagged vs untagged); green/purple to distinguish it from the
    # blue/orange registrar split below (matches the acknowledgement-metrics page).
    cymap = {r['_id']: (r['tagged'], r['all']) for r in cyagg if r['_id']}
    cov_chartscript = cov_chartdiv = ""
    cyears = sorted(cymap)
    if cyears:
        cydata = {"years": cyears,
                  "Tagged": [cymap[y][0] for y in cyears],
                  "Untagged": [cymap[y][1] - cymap[y][0] for y in cyears]}
        cov_chartscript, cov_chartdiv = DP.stacked_bar_chart(
            cydata, "Tagging coverage by year (Tagged vs Untagged)", xaxis="years",
            yaxis=["Tagged", "Untagged"], colors=["#2ca02c", "#9c27b0"],
            orient=pi / 4, width=1000, height=350)
    by_chartscript = by_chartdiv = ""
    years = sorted(ymap)
    if years:
        ydata = {"years": years,
                 "Crossref": [ymap[y]['Crossref'] for y in years],
                 "DataCite": [ymap[y]['DataCite'] for y in years]}
        by_chartscript, by_chartdiv = DP.stacked_bar_chart(
            ydata, "Tagged DOIs by year (by registrar)", xaxis="years",
            yaxis=["Crossref", "DataCite"], colors=DP.SOURCE_PALETTE,
            orient=pi / 4, width=1000, height=350)
    byyear_body = ("<div style='color:#a8c4e0;font-size:0.82em;margin-bottom:8px;'>"
                   "Coverage = tagged vs untagged Janelia DOIs by publishing year; the "
                   "volume chart below splits the tagged DOIs by registrar.</div>"
                   + (cov_chartdiv + "<br>" if cov_chartdiv else "")
                   + (by_chartdiv or "<div>No dated tagged DOIs.</div>"))
    # ----- Trends tab: DOIs per publication year, one heat map per registrar
    # (Crossref / DataCite). Rows = tags, columns = years, cell color = DOI
    # count, with per-year (Total row) and per-tag (Total column) sums. The maps
    # show the top-N tags by total (same set in both, so they line up
    # row-for-row); each map also gets a tab-delimited download of the FULL
    # tags x years matrix for ALL tags in that registrar. -----
    trend_chartscript = trend_chartdiv = ""
    if tags:
        try:
            tyrows = list(coll.aggregate([
                {"$match": {"jrc_tag.0": {"$exists": True},
                            "jrc_publishing_date": {"$exists": True}}},
                {"$unwind": "$jrc_tag"},
                {"$group": {"_id": {"name": "$jrc_tag.name", "yr": yr,
                                    "src": "$jrc_obtained_from"}, "n": {"$sum": 1}}}]))
        except Exception as err:
            return render_template('error.html', urlroot=request.url_root,
                                   title=render_warning("Could not compute tag trends"),
                                   message=error_message(err))
        per = {'Crossref': {}, 'DataCite': {}}
        yrset = {'Crossref': set(), 'DataCite': set()}
        for row in tyrows:
            nm = row['_id'].get('name')
            year = row['_id'].get('yr')
            src = row['_id'].get('src')
            if not (nm and year) or src not in per:
                continue
            per[src].setdefault(nm, {})[year] = row['n']
            yrset[src].add(year)
        for src in ('Crossref', 'DataCite'):
            syears = sorted(yrset[src])
            if not syears:
                continue
            # Download: ALL tags for this registrar (rows = tags, cols = years, + Total),
            # biggest tags first. Covers dated tagged DOIs only (undated can't be placed).
            src_tags = sorted(per[src], key=lambda n: sum(per[src][n].values()), reverse=True)
            dl_header = ['Tag'] + syears + ['Total']
            dl_body = ""
            for nm in src_tags:
                counts = [per[src][nm].get(y, 0) for y in syears]
                dl_body += "\t".join([nm] + [str(c) for c in counts]
                                     + [str(sum(counts))]) + "\n"
            dl_button = create_downloadable(f"tag_year_{src.lower()}", dl_header, dl_body)
            dl_caption = (f"<div style='margin:8px 0 2px 0;'>All {len(src_tags):,} {src} "
                          f"tags (tags &times; years): {dl_button}</div>")
            # Heat map: the biggest N tags in THIS registrar
            src_top = src_tags[:limit]
            sub = f" (top {limit} of {len(src_tags):,})" if len(src_tags) > limit else ""
            hx, hy, hval = [], [], []
            for nm in src_top:
                for year in syears:
                    cnt = per[src].get(nm, {}).get(year, 0)
                    if cnt:
                        hx.append(year)
                        hy.append(nm)
                        hval.append(cnt)
            map_div = ""
            if hval:
                script, div = DP.heat_map(
                    {"Year": hx, "Tag": hy, "DOIs": hval},
                    f"{src} DOIs – tagged by publication year{sub}",
                    x_field="Year", y_field="Tag", value_field="DOIs",
                    width=1000, value_format="0,0",
                    col_totals="Total", row_totals="Total", highlight_max_total=True)
                trend_chartscript += script
                map_div = div
            trend_chartdiv += dl_caption + map_div + "<br><br>"
    trends_body = ("<div style='color:#a8c4e0;font-size:0.82em;margin-bottom:8px;'>"
                   f"DOI counts per publication year for the biggest {limit} tags in each "
                   "registrar, as a heat map. Cell color = DOI count (scale at right); hover "
                   "for exact numbers; the Total row/column give per-year and per-tag sums. "
                   "Each download button gives the full tags &times; years matrix for "
                   "<b>all</b> tags in that registrar as a tab-delimited file.</div>"
                   + (trend_chartdiv or "<div>No dated tagged DOIs.</div>"))
    # ----- Co-tagged tab: which tags share DOIs (collaboration/project structure).
    # Pair counts over DOIs carrying >=2 tags (only those can co-occur). -----
    pairs = collections.Counter()
    try:
        for row in coll.find({"jrc_tag.1": {"$exists": True}}, {"jrc_tag.name": 1}):
            names = sorted({tag['name'] for tag in row.get('jrc_tag', []) if tag.get('name')})
            for name_a, name_b in itertools.combinations(names, 2):
                pairs[(name_a, name_b)] += 1
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not compute tag co-occurrence"),
                               message=error_message(err))
    # Top co-tagged pairs table (each tag links to its page; count = shared DOIs).
    prows = []
    for (name_a, name_b), shared in pairs.most_common(limit):
        la = f"<a href='/tag/{quote(name_a, safe='')}'>{escape(name_a)}</a>"
        lb = f"<a href='/tag/{quote(name_b, safe='')}'>{escape(name_b)}</a>"
        prows.append([safe(la), safe(lb), f"{shared:,}"])
    pair_table = render_table(['Tag', 'Co-tagged with', 'Shared DOIs'], prows,
                              table_id='cotagpairs', css='tablesorter numbers-scroll')
    # Tag x tag co-occurrence heat map over the top-N tags by total (diagonal omitted).
    cotag_top = sorted(tags, key=lambda n: tags[n]['Crossref'] + tags[n]['DataCite'],
                       reverse=True)[:limit]
    cooccur_chartscript = cooccur_chartdiv = ""
    hx, hy, hval = [], [], []
    for name_a in cotag_top:
        for name_b in cotag_top:
            if name_a == name_b:
                continue
            shared = pairs.get(tuple(sorted((name_a, name_b))), 0)
            if shared:
                hy.append(name_a)
                hx.append(name_b)
                hval.append(shared)
    if hval:
        cooccur_chartscript, cooccur_chartdiv = DP.heat_map(
            {"Tag": hy, "Co-tag": hx, "Shared": hval},
            f"Tag co-occurrence: shared DOIs among the top {len(cotag_top)} tags",
            x_field="Co-tag", y_field="Tag", value_field="Shared",
            width=1000, value_format="0,0")
    cotag_body = ("<div style='color:#a8c4e0;font-size:0.82em;margin-bottom:8px;'>"
                  "Tags that share DOIs &ndash; collaboration and project structure. The "
                  f"table lists the top {len(prows)} co-tagged pairs (each tag links to its "
                  "page); the heat map shows shared-DOI counts among the top tags (diagonal "
                  "omitted).</div>"
                  + (pair_table if prows else "<div>No co-tagged DOIs.</div>")
                  + "<br>" + cooccur_chartdiv)
    # ----- tabs -----
    active_tab = request.args.get('tab')
    if active_tab not in ('tags', 'byyear', 'trends', 'cotag'):
        active_tab = 'tags'

    tabs = ('<ul class="nav nav-tabs" role="tablist">'
            + tab_button('tags', 'Tags', active_tab == 'tags')
            + tab_button('byyear', 'By year', active_tab == 'byyear')
            + tab_button('trends', 'Trends', active_tab == 'trends')
            + tab_button('cotag', 'Co-tagged', active_tab == 'cotag')
            + '</ul><div class="tab-content">'
            + tab_pane('tags', tags_body, active_tab == 'tags')
            + tab_pane('byyear', byyear_body, active_tab == 'byyear')
            + tab_pane('trends', trends_body, active_tab == 'trends')
            + tab_pane('cotag', cotag_body, active_tab == 'cotag')
            + '</div>')
    seealso = see_also([("DOI metrics", "/dois_metrics"),
                        ("Acknowledgements", "/acknowledgement_metrics"),
                        ("Impact by source", "/source_metrics")])
    html = (coverage_html + tabs + seealso + tag_chartscript + cov_chartscript
            + by_chartscript + trend_chartscript + cooccur_chartscript)
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title="Tag metrics", html=html, bokeh=True,
                                         navbar=generate_navbar('Tag/affiliation')))

# ******************************************************************************
# * UI endpoints (general)                                                     *
# ******************************************************************************
@app.route('/download/<string:fname>')
def download(fname):
    ''' Downloadable content
    '''
    try:
        return send_file('/tmp/' + fname, download_name=fname, as_attachment=True)  # pylint: disable=E1123
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
    badges = "&nbsp;&nbsp;<span class='paperdata'>"
    if '/protocols.io.' in doi:
        badges += f" {tiny_badge('publisher', 'protocols.io', f'/raw/protocols.io/{doi}')}"
    elif '/arxiv.' in doi.lower():
        badges += " " + tiny_badge('publisher', 'arXiv', f'/raw/arxiv/{doi}')
    elif 'elife' in doi.lower():
        badges += " " + tiny_badge('publisher', 'eLife', f'/raw/eLife/{doi}')
    elif 'publisher' in data and data['publisher'].startswith('Elsevier'):
        badges += " " + tiny_badge('publisher', 'Elsevier', f'/raw/elsevier/{doi}')
    elif 'publisher' in data and data['publisher'].startswith('Springer'):
        badges += " " + tiny_badge('publisher', 'Springer', f'/raw/springer/{doi}')
    elif 'zenodo' in doi.lower():
        badges += " " + tiny_badge('publisher', 'Zenodo', f'/raw/zenodo/{doi}')
    elif '10.1371/journal.' in doi.lower():
        badges += " " + tiny_badge('publisher', 'PLoS', f'/raw/plos/{doi}')
    rlink = f"/doi/{doi}"
    if local:
        jour = DL.get_journal(data)
        if jour:
            if 'bioRxiv' in jour:
                badges += f" {tiny_badge('publisher', 'bioRxiv', f'/raw/bioRxiv/{doi}')}"
        if '/janelia.' in doi:
            badges += f" {tiny_badge('publisher', 'figshare', f'/raw/figshare/{doi}')}"
        badges += f" {tiny_badge('primarysource', row['jrc_obtained_from'], rlink)}"
    else:
        badges += f" {tiny_badge('source', 'Raw data', rlink)}"
    if local:
        if 'jrc_pmc' in row:
            plink = f"/raw/pmc/{row['jrc_pmc']}"
            badges += f" {tiny_badge('source', 'PMC', plink)}"
        if 'jrc_pmid' in row:
            plink = f"/raw/pubmed/{row['jrc_pmid']}"
            badges += f" {tiny_badge('source', 'PubMed', plink)}"
    if 'janelia' not in doi:
        oresp = JRC.call_oa(doi)
        if oresp:
            olink = f"{OAREPORT}{doi}"
            badges += f" {tiny_badge('source', 'OA.Report', olink)}"
        if row.get('jrc_openalex_id'):
            sleep(0.02)
            try:
                oresp = DL.get_doi_record(doi, source='openalex')
                if oresp:
                    olink = f"/raw/openalex/{doi}"
                    badges += f" {tiny_badge('source', 'OpenAlex', olink)}"
            except Exception:
                pass
    if local and 'jrc_fulltext_url' in row:
        badges += f" {tiny_badge('pdf', 'Full text', row['jrc_fulltext_url'])}"
    #badges += f" {tiny_badge('info', 'HQ migration', f'/doi/migration/{doi}')}"
    badges += "</span>"
    return badges


@app.route('/andy')
def andy():
    ''' Show Andy's page
    '''
    payload = [{"$match": {"jrc_tag": {"$exists": True}}},
               {"$unwind": "$jrc_tag"},
               {"$group": {"_id": None, "uniqueValues": {"$addToSet": "$jrc_tag.name"}}},
               {"$project": {"_id": 0, "uniqueValues": 1}}]
    try:
        rows = DB['dis']['dois'].aggregate(payload)
    except Exception as err:
        return render_template('warning.html', urlroot=request.url_root,
                                title=render_warning("Could not find projects", 'error'),
                                message=error_message(err))
    plist = []
    for row in rows:
        for uval in row['uniqueValues']:
            plist.append(uval)
    projects = '<option>'
    projects += '</option><option>'.join(sorted(plist))
    projects += '</option>'
    return make_response(render_template('landing.html', urlroot=request.url_root,
                                         projects=projects, navbar=generate_navbar('Home')))


# ******************************************************************************
# * UI endpoints (personalized)                                                *
# ******************************************************************************

@app.route('/dois/mytags/<string:orcid>/<string:year>')
@app.route('/dois/mytags/<string:orcid>')
@app.route('/dois/mytags')
def dois_mytags(orcid="0000-0003-3118-1636", year='All'):
    ''' Show DOIs an author's affiliations
    '''
    try:
        row = DB['dis'].orcid.find_one({"orcid": orcid})
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not find DOIs for my affiliations"),
                               message=error_message(err))
    tags = []
    if 'group' in row:
        tags.append(row['group'])
    for ttype in ('affiliations', 'managed'):
        if ttype in row:
            for tag in row[ttype]:
                if tag not in tags:
                    tags.append(tag)
    payload = {"jrc_tag.name": {"$in": tags}}
    if year != 'All':
        payload['jrc_publishing_date'] = {"$regex": "^"+ year}
    try:
        coll = DB['dis'].dois
        rows = coll.find(payload).collation({"locale": "en"}).sort("jrc_publishing_date", -1)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not find DOIs for my affiliations"),
                               message=error_message(err))
    htmlp = year_pulldown(f"dois/mytags/{orcid}") + "<br>"
    html, cnt, _ = standard_doi_table(rows, count_card=True)
    title = "DOIs for my affiliations"
    if year != 'All':
        title += f" ({year})"
    if cnt:
        html = f"{htmlp}Tags: {', '.join(tags)}<br><br>{html}"
    else:
        html = htmlp + render_warning("No DOIs were found for your affiliations.", 'warning')
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         navbar=generate_navbar('Tag/affiliation')))


# ******************************************************************************
# * UI endpoints (DOI)                                                         *
# ******************************************************************************

def get_citation_counts(doi, row, partial=True):
    ''' Get citation counts
        Keyword arguments:
          doi: DOI
          row: row from dois collection
          partial: True to only show some counts, False to show all counts
        Returns:
          Citation counts as HTML
    '''
    # Citations (DataCite, Dimensions, eLife, OA.Report, OpenAlex, PubMed,
    # ScholeXplorer, Web of Science)
    doisec = ""
    tblrow = []
    # DataCite
    if row['jrc_obtained_from'] == 'DataCite':
        if (row.get('jrc_citation_sources') or {}).get('datacite'):
            tblrow.append(f"<td>DataCite: {row['jrc_citation_sources']['datacite']:,}</td>")
        elif row.get('citationCount', False):
            url = f"{DATACITE}{doi}"
            tblrow.append(f"<td>DataCite: <a href='{url}' target='_blank'>" \
                          + f"{row['citationCount']:,}</a></td>")
    # Dimensions
    try:
        citcnt, url = DL.get_citation_count(doi)
    except Exception:
        citcnt = 0
    if citcnt:
        tblrow.append(f"<td>Dimensions: {citcnt:,}{url}</td>")
    # eLife
    if 'elife' in doi.lower():
        try:
            citcnt, url = DL.get_citation_count(doi, 'elife')
        except Exception:
            citcnt = 0
        if citcnt:
            tblrow.append(f"<td>eLife: <a href='{url}' target='_blank'>{citcnt:,}</a>")
    # OA.Report
    if not partial:
        try:
            citcnt, url = DL.get_citation_count(doi, 'oa')
        except Exception:
            citcnt = 0
        if citcnt:
            tblrow.append(f"<td>OA.Report: {citcnt:,}</td>")
    # OpenAlex
    sleep(0.01)
    try:
        citcnt, url = DL.get_citation_count(doi, 'openalex')
    except Exception:
        citcnt = 0
    if citcnt:
        cbutton = '<a class="btn btn-outline-success btn-tiny" ' \
                  + f'href="/citations/incoming/openalex/{doi}" ' \
                  + f'role="button">{DOWNLOAD_ICON}Download tab-delimited file</a>'
        cbutton = f"<span style='line-height: 1.3'><br></span>{cbutton}"
        if url:
            tblrow.append(f"<td>OpenAlex: <a href='{url}' target='_blank'>{citcnt:,}</a>" \
                          + f"{cbutton}</td>")
        else:
            tblrow.append(f"<td>OpenAlex: {citcnt:,}</td>")
    elif (row.get('jrc_citation_sources') or {}).get('openalex'):
        tblrow.append(f"<td>OpenAlex: {row['jrc_citation_sources']['openalex']:,}</td>")
    # PubMed
    if 'jrc_pmid' in row and not partial:
        try:
            citcnt, url = DL.get_citation_count(row['jrc_pmid'], 'pubmed')
        except Exception:
            citcnt = 0
        if citcnt:
            cbutton = '<a class="btn btn-outline-success btn-tiny" ' \
                      + f'href="/citations/incoming/pubmed/{doi}" ' \
                      + f'role="button">{DOWNLOAD_ICON}Download tab-delimited file</a>'
            cbutton = f"<span style='line-height: 1.3'><br></span>{cbutton}"
            tblrow.append(f"<td>PubMed: <a href='{url}' target='_blank'>{citcnt:,}</a>" \
                          + f"{cbutton}</td>")
    # ScholeXplorer
    if (row.get('jrc_citation_sources') or {}).get('scholexplorer'):
        tblrow.append(f"<td>ScholeXplorer: {row['jrc_citation_sources']['scholexplorer']:,}</td>")
    # Semantic Scholar
    citcnt = s2_citation_count(doi, fmt='html')
    if citcnt:
        tblrow.append(f"<td>Semantic Scholar: {citcnt}</td>")
    # Web of Science
    citcnt, url = DL.get_citation_count(doi, 'wos',
                                        bool(row['jrc_obtained_from'] == 'DataCite'))
    if citcnt:
        tblrow.append(f"<td>Web of Science: <a href='{url}' target='_blank'>" \
                      + f"{citcnt:,}</a></td>")
    if tblrow:
        doisec += "<table id='citations' class='citations'><thead>" \
                  + f"<tr><th colspan={len(tblrow)}>Citation counts&nbsp;" \
                  + "<a href=\"#\" onclick=\"$('#verbiage').toggle();\">" \
                  + "<i class='fa-solid fa-circle-info' style='color: lime'></i></a></th></tr>" \
                  + "</thead><tbody>" + ''.join(tblrow) + "</tbody></table>"
    # DataCite downloads
    if row['jrc_obtained_from'] == 'DataCite':
        if 'downloadCount' in row and row['downloadCount']:
            doisec += f"<span class='paperdata'>Downloads: {row['downloadCount']:,}</span><br>"
    if tblrow:
        with open(f"{app.root_path}/static/html/citation_counts.html", 'r',
                  encoding='utf-8') as htmlin:
            doisec += '<div id="verbiage" style="display: none;">' + htmlin.read() + '</div>'
    return doisec


def get_figshare_counts(row):
    ''' Get figshare counts
        Keyword arguments:
          row: row from dois collection
        Returns:
          Figshare counts as HTML
    '''
    figshare = ""
    tblrow = []
    if not row.get('jrc_figshare_counts', False):
        return ""
    tblrow.append(f"<td>Views: {row['jrc_figshare_counts']['views']:,}</td>")
    tblrow.append(f"<td>Downloads: {row['jrc_figshare_counts']['downloads']:,}</td>")
    tblrow.append(f"<td>Shares: {row['jrc_figshare_counts']['shares']:,}</td>")
    if tblrow:
        figshare = "<table id='figshare' class='citations'><thead>" \
                  + f"<tr><th colspan={len(tblrow)}>Figshare counts</th></tr>" \
                  + "</thead><tbody>" + ''.join(tblrow) + "</tbody></table>"
    return figshare


def curator_display(tag):
    ''' Build a display label for a jrc_acknowledge tag's curator.
        Keyword arguments:
          tag: jrc_acknowledge tag object ({name, code, type, curator})
        Returns:
          "IRIS" (in lime) for machine-tagged entries; <name> (linked to their
          /userui/ record) for human-curated entries whose curator (looked up
          by userIdO365 in the orcid collection, since a human curator may not
          have an ORCID) resolves to a name; "Human curated" if no curator is
          set or no matching orcid record is found.
    '''
    curator = tag.get('curator')
    if curator == 'IRIS':
        return "<span style='color: lime'>IRIS</span>"
    if curator:
        try:
            orc = DB['dis'].orcid.find_one({"userIdO365": curator})
        except Exception:
            orc = None
        if orc:
            name = " ".join([orc['given'][0], orc['family'][0]])
            return f"<a href='/userui/{orc['userIdO365']}'>{name}</a>"
    return 'Human curated'


def doi_tabs(doi, row, rowext, data, authors):
    ''' Generate DOI tabs
        Keyword arguments:
          doi: DOI
          row: row from dois collection
          rowext: row from external_dois collection
          data: data from Crossref/DataCite API
          authors: authors from Crossref/DataCite API
        Returns:
          DOI tabs as HTML
    '''
    content = {}
    display_key = {'author': 'Author tags', 'citations': 'Citations',
                   'figshare': 'Figshare', 'abstract': 'Abstract',
                   'ack': 'Acknowledgements', 'subjects': 'Subjects', 'related': 'Related DOIs',
                   'legal': 'Legal information'}
    # Author tags
    if row and 'jrc_tag' in row:
        tags = []
        for tag in row['jrc_tag']:
            tags.append(f"<a href='/tag/{escape(tag['name'])}'>{tag['name']}</a>")
        content['author'] = "<br>".join(tags)
    # Citation counts
    if row:
        ahtml = get_citation_counts(doi, row)
        if ahtml:
            content['citations'] = ahtml
    # Figshare
    ahtml = ""
    if row and row['jrc_obtained_from'] == 'DataCite':
        if row.get('jrc_figshare_counts', False):
            ahtml = get_figshare_counts(row)
            if ahtml:
                content['figshare'] = ahtml
        if ('janelia' in doi or 'figshare' in doi):
            arec = DL.get_doi_record(doi, source='figshare')
            if arec and arec.get('files'):
                files = []
                for file in arec['files']:
                    if file.get('download_url'):
                        files.append(f"<a href='{file['download_url']}' " \
                                     + f"target='_blank'>{file['download_url']}</a>")
                if files:
                    if content.get('figshare'):
                        content['figshare'] += "<br>Files:<br>" + "<br>".join(files)
                    else:
                        content['figshare'] = "<br>Files:<br>" + "<br>".join(files)
    # Abstract
    abstract = ahtml = ""
    if 'type' in data and data['type'] == 'grant':
        if 'project' in data and data['project']:
            if data['project'][0].get('project-description', {}) \
               and data['project'][0]['project-description'][0].get('description', {}):
                abstract = jats_to_html(data['project'][0]['project-description'][0]['description'])
                ptitle = ""
                if 'project-title' in data['project'][0] and data['project'][0]['project-title']:
                    ptitle = f" ({data['project'][0]['project-title'][0]['title']})"
                ahtml += f"<h4>Grant{ptitle}</h4><div class='abstract'>{abstract}</div><br>"
    else:
        abstract = jats_to_html(DL.get_abstract(data))
        if abstract:
            ahtml += f"<h4>Abstract</h4><div class='abstract'>{abstract}</div>"
    if ahtml:
        content['abstract'] = ahtml
    # Acknowledgements
    ahtml = ""
    if row.get('jrc_acknowledge'):
        trows = []
        for tag in row['jrc_acknowledge']:
            curator = curator_display(tag)
            updated = str(tag['updated']).split(' ', maxsplit=1)[0] if tag.get('updated') else ''
            trows.append([safe(f"<a href='/tag/{escape(tag['name'])}'>{tag['name']}</a>"),
                          safe(curator), updated])
        if trows:
            ahtml += "<h4>Acknowledgement tags</h4>" \
                     + render_table(['Tag', 'Curator', 'Updated'], trows, table_id='acktags',
                                   css='tablesorter standard-scroll')
    acktext = asrc = ""
    if row.get('jrc_acknowledgements'):
        acktext = row['jrc_acknowledgements'].replace('\n', '<br>')
        asrc = row.get('jrc_ack_source', '')
    elif rowext and rowext.get('jrc_acknowledgements'):
        acktext = rowext['jrc_acknowledgements'].replace('\n', '<br>')
        asrc = rowext.get('jrc_ack_source', '')
    elif row and not row.get('jrc_inserted', False):
        # If this DOI isn't in our database, look for acknowledgements (this also
        # returns the source)
        try:
            acktext, asrc = DL.get_acknowledgements(doi)
        except Exception:
            pass
    if acktext:
        if not asrc:
            # No stored jrc_ack_source (source outside the eLife/Elsevier/PMC/arXiv
            # pipeline, or a record predating the field): infer by the same
            # precedence the pull pipeline uses, and show NO source rather than
            # guess when nothing matches.
            if 'elife' in doi:
                asrc = 'eLife'
            elif doi.startswith('10.1016/'):
                asrc = 'Elsevier'
            elif row.get('jrc_pmc') or (rowext and rowext.get('jrc_pmc')):
                asrc = 'PMC'
            elif 'arxiv' in doi:
                asrc = 'arXiv'
        highlight = ""
        try:
            highlight = DL.highlight_acknowledgments(acktext, DB['dis'])
            if acktext != highlight:
                acktext = highlight
            else:
                highlight = ""
        except Exception:
            pass
        ahtml += f"<h4 style='margin-top:22px;'>Acknowledgements</h4><div class='abstract'>{acktext}"
        if asrc:
            ahtml += "<br><span style='font-size:10pt;background-color:#777;" \
                     + f"color:aqua;'>Source: {asrc}</span>"
        ahtml += "</div>"
        if highlight:
            ahtml += "<br><span style='color:goldenrod'><i class='fa-solid fa-warning'></i>" \
                     + " Acknowledgment highlighting is an experimental feature</span>"
    if ahtml:
        content['ack'] = ahtml
    # Subjects
    ahtml = "&nbsp;"
    if row:
        try:
            ahtml = add_subjects(row, ahtml)
            if 'span' in ahtml:
                ahtml += "<br><i class='fa-solid fa-circle-info'></i> Subjects in " \
                         + "<span style='color: #88a'>gray-blue</span> are considered minor in MeSH"
        except Exception:
            pass
        if ahtml != "&nbsp;":
            content['subjects'] = ahtml
    # Relations
    rels = add_relations(data)
    if rels:
        content['related'] = rels
    # Legal information
    if row:
        ahtml = get_legal_information(row)
        if ahtml:
            content['legal'] = ahtml
    # Authors
    ahtml = ""
    if authors:
        alist, count = show_tagged_authors(authors, row['jrc_author'] \
                       if 'jrc_author' in row else [])
        if alist:
            abtn = tiny_badge('source', 'Details', f'/doi/authors/{doi}') \
                if count else ""
            ahtml = f"<h4>Potential Janelia authors ({count}) {abtn}</h4>" \
                    + f"<div class='scroll'>{''.join(alist)}</div>"
        if not alist or not count:
            alist, count = show_openalex_authors(doi, row['jrc_author'] \
                                                 if 'jrc_author' in row else [])
            if alist:
                ahtml = f"<h4>OpenAlex authors ({count}) " \
                        + "<span style='font-size:12pt;color:red'>(could not match Janelia " \
                        + f"authors in {row['jrc_obtained_from']})</span></h4>" \
                        + f"<div class='scroll'>{''.join(alist)}</div>"
    if not authors and not content:
        return "&nbsp;"
    # Tabs
    html = '<ul class="nav nav-tabs" id="myTab" role="tablist">'
    html += '<li class="nav-item" role="presentation">' \
           + '<button class="nav-link active" id="home-tab" data-toggle="tab" ' \
           + 'data-target="#home" type="button" role="tab" aria-controls="home" ' \
           + 'aria-selected="true">Authors</button></li>'
    for key in content:
        html += '<li class="nav-item" role="presentation">' \
                + f'<button class="nav-link" id="{key}-tab" data-toggle="tab" ' \
                + f'data-target="#{key}" type="button" role="tab" aria-controls="{key}" ' \
                + f'aria-selected="false">{display_key[key]}</button></li>'
    html += '</ul>'
    html += '<div class="tab-content" id="myTabContent">'
    html += '<div class="tab-pane fade show active" id="home" role="tabpanel" ' \
            + f'aria-labelledby="home-tab"><br>{ahtml}</div>'
    for key, val in content.items():
        html += f'<div class="tab-pane fade" id="{key}" role="tabpanel" ' \
                + f'aria-labelledby="{key}-tab"><br>{val}</div>'
    html += '</div>'
    return html


BATCH_MAX_BYTES = 2_000_000   # cap on an uploaded DOI/PMID list (~2 MB)
BATCH_MAX_TOKENS = 5000       # cap on the number of ids processed per request
BATCH_NOTFOUND_SHOWN = 200    # cap on how many not-found ids are listed (in a scrollbox)


@app.route('/doiui_batch', methods=['POST'])
def show_doi_batch():
    ''' Standard DOI table for a list of DOIs/PMIDs, submitted from the home page's
        "Find a DOI or PMID" box (pasted, whitespace-separated) or as an uploaded
        text/CSV file. A single pasted entry routes to /doiui client-side; this
        handles two or more, plus any uploaded file. Tokens are split on whitespace
        or commas (so space-, newline-, tab-, and comma-separated lists all work).
        PMIDs (all-digit tokens) are resolved via jrc_pmid, the same way /doiui does;
        tokens with no matching dois-collection record are listed as not found.
    '''
    raw = request.form.get('dois', '')
    upload = request.files.get('file')
    if upload and upload.filename:
        # Read at most the cap (+1 byte to detect oversize) rather than the whole file.
        data = upload.read(BATCH_MAX_BYTES + 1)
        if len(data) > BATCH_MAX_BYTES:
            return render_template('warning.html', urlroot=request.url_root,
                                   title=render_warning("File too large", 'warning'),
                                   message=f"The uploaded file exceeds the "
                                           f"{BATCH_MAX_BYTES // 1_000_000} MB limit.")
        raw += "\n" + data.decode('utf-8-sig', errors='replace')
    # Normalize + dedupe (preserving input order); split on any run of whitespace or
    # commas; all-digit tokens are PMIDs. Cap the count to bound the lookup.
    pmids, dois, seen = [], [], set()
    truncated = False
    for tok in re.split(r'[\s,]+', raw):
        token = tok.strip().strip('/').lower()
        if not token or token in seen:
            continue
        if len(seen) >= BATCH_MAX_TOKENS:
            truncated = True
            break
        seen.add(token)
        (pmids if token.isdigit() else dois).append(token)
    if not seen:
        return render_template('warning.html', urlroot=request.url_root,
                               title=render_warning("No DOIs or PMIDs", 'warning'),
                               message="You must enter or upload at least one DOI or PMID")
    try:
        rows = list(DB['dis'].dois.find({"$or": [{"doi": {"$in": dois}},
                                                 {"jrc_pmid": {"$in": pmids}}]}))
    except Exception as err:
        return inspect_error(err, 'Could not get DOIs')
    found_dois = {row['doi'] for row in rows}
    found_pmids = {str(row['jrc_pmid']) for row in rows if row.get('jrc_pmid') is not None}
    nf_dois = [t for t in dois if t not in found_dois]
    nf_pmids = [t for t in pmids if t not in found_pmids]
    notfound = nf_dois + nf_pmids
    # A not-found DOI may still be an external DOI (minimal data) rather than unknown.
    external = set()
    if nf_dois:
        try:
            external = {r['doi'] for r in
                        DB['dis'].external_dois.find({"doi": {"$in": nf_dois}}, {"doi": 1})}
        except Exception as err:
            return inspect_error(err, 'Could not check external DOIs')
    rows.sort(key=lambda row: DL.get_publishing_date(row) or '', reverse=True)
    html = ""
    if truncated:
        html += render_warning(f"Only the first {BATCH_MAX_TOKENS:,} entries were processed.",
                               'warning') + "<br>"
    if notfound:
        # Vertical, scrollable list of the not-found ids. Each not-found DOI links to
        # /doiui (live/external lookup) and gets an "external" pill when we hold a
        # minimal external record; PMIDs stay plain. Capped at BATCH_NOTFOUND_SHOWN.
        nf_doi_set = set(nf_dois)
        shown = notfound[:BATCH_NOTFOUND_SHOWN]
        lines = []
        for tok in shown:
            if tok in nf_doi_set:
                pill = " " + tiny_badge('external', 'external') if tok in external else ""
                lines.append(f"<div><a href='/doiui/{escape(tok)}'>{escape(tok)}</a>{pill}</div>")
            else:
                # A not-found PMID can't resolve here, but links out to PubMed.
                lines.append(f"<div><a href='https://pubmed.ncbi.nlm.nih.gov/{escape(tok)}/'"
                             f" target='_blank'>{escape(tok)}</a></div>")
        ext_note = f", {len(external):,} external" if external else ""
        html += render_warning(f"{len(notfound):,} of {len(seen):,} entered not found in the "
                               f"DOI collection{ext_note}:", 'warning')
        html += ("<div style='max-height:220px; overflow-y:auto; margin:6px 0 4px 0; "
                 "padding:6px 10px; border:1px solid rgba(168,196,224,0.35); "
                 "border-radius:4px;'>" + "".join(lines) + "</div>")
        if len(notfound) > len(shown):
            # More not found than the scrollbox shows; offer the complete list as a
            # download (id, type, and whether we hold a minimal external record).
            html += (f"<div style='color:#a8c4e0;'>&hellip; and "
                     f"{len(notfound) - len(shown):,} more</div>")
            nf_pmid_set = set(nf_pmids)
            dl_rows = [f"{tok}\t{'PMID' if tok in nf_pmid_set else 'DOI'}\t"
                       f"{'external' if tok in external else 'not found'}" for tok in notfound]
            html += "<div style='margin-top:6px;'>" \
                    + create_downloadable('notfound', ['ID', 'Type', 'Status'],
                                          "\n".join(dl_rows),
                                          label=f"Download all {len(notfound):,} not-found") \
                    + "</div>"
        html += "<br>"
    if rows:
        # Resolve every jrc_author employeeId across the rows to a linked name in one
        # orcid-collection query (eid -> (name, href)); jrc_author holds employeeIds.
        all_eids = {eid for row in rows for eid in row.get('jrc_author', [])}
        author_map = {}
        if all_eids:
            for arec in DB['dis'].orcid.find({"employeeId": {"$in": list(all_eids)}},
                                             {"employeeId": 1, "given": 1, "family": 1,
                                              "orcid": 1, "userIdO365": 1}):
                given = (arec.get('given') or [''])[0]
                family = (arec.get('family') or [''])[0]
                name = f"{given} {family}".strip() or arec['employeeId']
                if arec.get('orcid'):
                    href = f"/userui/{arec['orcid']}"
                elif arec.get('userIdO365'):
                    href = f"/userui/{arec['userIdO365']}"
                else:
                    href = f"/unvaluserui/{arec['_id']}"
                author_map[arec['employeeId']] = (name, href)

        def janelia_authors(row):
            ''' Comma-separated linked Janelia-author names for the row's jrc_author
                employeeIds; an eid with no orcid record is shown as-is. '''
            parts = []
            for eid in row.get('jrc_author', []):
                if eid in author_map:
                    name, href = author_map[eid]
                    parts.append(f"<a href='{href}'>{escape(name)}</a>")
                else:
                    parts.append(escape(str(eid)))
            return [safe(", ".join(parts))]

        table, cnt, oacnt = standard_doi_table(rows, show_count=False,
                                               extra_headers=['Janelia authors'],
                                               extra_fn=janelia_authors)
        # Rows with at least one Janelia author (a non-empty jrc_author employeeId list).
        jcount = sum(1 for row in rows if row.get('jrc_author'))
        # DOIs card carries id='totalrows' so the version-filter toggle keeps it current;
        # the Open-access and Janelia-authored cards are static totals of the result set.
        cards = stat_cards([("DOIs", f"<span id='totalrows'>{cnt:,}</span>"),
                            ("Open access", f"{oacnt:,}"),
                            ("Janelia-authored", f"{jcount:,}")], div_id='batch-stats')
        html += cards + table
    else:
        html += render_warning("None of the entered DOIs/PMIDs were found in the DOI "
                               + "collection.", 'warning')
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title="DOI report", html=html,
                                         navbar=generate_navbar('DOIs')))


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
    html = recsec = ""
    rowext = {}
    if row:
        recsec += '<h5 style="color:lime">This DOI is saved locally in the Janelia database</h5>'
        recsec += add_update_times(row)
        recsec += add_jrc_fields(row)
        local = True
    else:
        try:
            rowext = DB['dis'].external_dois.find_one({"doi": doi})
        except Exception as err:
            return inspect_error(err, 'Could not get DOI')
        if rowext:
            recsec = "<h4 style='color:goldenrod'><i class='fa-solid fa-warning'></i> " \
                     + "This DOI is saved locally as an external DOI (minimal data saved)</h4><br>"
        else:
            recsec = "<h4 style='color:red'><i class='fa-solid fa-warning'></i> " \
                     + "This DOI is not saved locally in the Janelia database</h4><br>"
        if is_ignored(doi):
            recsec += "<h4 style='color:red'><i class='fa-solid fa-warning'></i> " \
                     + "This DOI is in the ignore list</h4><br>"
    try:
        _, data = get_doi(doi)
    except Exception as err:
        return render_template('warning.html', urlroot=request.url_root,
                                title=render_warning("Could not get DOI", 'error'),
                                message=str(err))
    if not data:
        return render_template('warning.html', urlroot=request.url_root,
                                title=render_warning("Could not find DOI", 'warning'),
                                message=f"Could not find DOI {doi} in " \
                                        +f"{'DataCite' if DL.is_datacite(doi) else 'Crossref'}")
    try:
        authors = DL.get_author_list(data, orcid=True, project_map=DB['dis'].project_map)
    except Exception as err:
        auth = data['author'] if 'author' in data else data['creators']
        return render_template('error.html', urlroot=request.url_root,
                                title=render_warning("Could not generate author list"),
                                message=f"Could not generate author list for {doi}" \
                                        + "<br><pre style='color: black;'>" \
                                        + json.dumps(auth, indent=2, default=str) + "</pre>")
    title = DL.get_title(data)
    if not title:
        return render_template('error.html', urlroot=request.url_root,
                                title=render_warning("Could not find title"),
                                message=f"Could not find title for {doi}")
    title = render_title_html(title)   # de-TeX + allowlist HTML + escape (also closes XSS here)
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
    citations = strip_tex_math(citations)   # the short citation embeds the (possibly TeX) title
    # Citation
    citsec = cittype = ""
    if 'type' in data:
        cittype += data['type'].replace('-', ' ')
        if 'subtype' in data:
            cittype += f" {data['subtype'].replace('-', ' ')}"
    elif data.get('types', {}).get('resourceTypeGeneral'):
        cittype += data['types']['resourceTypeGeneral']
    citsec += f"<div id='div-full' class='citation'>{citationf} {journal}.</div>"
    citsec += f"<div id='div-short' class='citation'>{citations}</div>"
    citsec += "<br>"
    # Author details
    authors = None
    if not row:
        row = data
        row['jrc_obtained_from'] = 'DataCite' if DL.is_datacite(doi) else 'Crossref'
    try:
        authors = DL.get_author_details(row, DB['dis'].orcid)
    except Exception as err:
        return inspect_error(err, 'Could not get author list details')
    html += doi_tabs(doi, row, rowext, data, authors)
    # Title
    doilink = f"<a href='{DOI}{doi}' target='_blank'>{doi}</a>"
    badges = get_display_badges(doi, row, data, local)
    doilink += " <button style='background-color:transparent;border:none;' " \
               + f"onclick=\"copyText('{doi}')\">" \
               + "<i class='fas fa-regular fa-copy shadow' " \
               + "style='background-color:transparent'></i></button>"
    if row and row.get('jrc_pmid'):
        doititle = f"{doilink} (PMID: <a href='{PMID}{row['jrc_pmid']}/' " \
                   + f"target='_blank'>{row['jrc_pmid']}</a>)"
    else:
        doititle = doilink
    doititle += badges
    # Retraction banner: shown above the DOI line when the record is flagged retracted.
    banner = ""
    if row and row.get('jrc_retracted'):
        banner = ("<div class='retracted-banner'><i class='fa-solid fa-ban'></i>"
                  "<strong>Retracted</strong> &mdash; this publication has been "
                  "retracted.</div>")
    endpoint_access()
    return make_response(render_template('doi.html', urlroot=request.url_root, pagetitle=doi,
                                         title=doititle, banner=banner, recsec=recsec,
                                         #doisec=doisec,
                                         cittype=cittype, citsec=citsec,
                                         html=html,
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
    ''' Show DOIs for a given type/subtype, optionally scoped to a single
        jrc_tag.name via ?tag= (keeps the /dois_metrics Sources drill-down in
        sync with that page's tag filter).
    '''
    payload = {"jrc_obtained_from": src,
               ("type" if src == 'Crossref' else 'types.resourceTypeGeneral'): typ}
    if sub != 'None':
        payload["subtype"] = sub
    if year != 'All':
        payload['jrc_publishing_date'] = {"$regex": "^" + year}
    tag = request.args.get('tag') or None
    if tag:
        payload["jrc_tag.name"] = tag
    try:
        coll = DB['dis'].dois
        rows = coll.find(payload).collation({"locale": "en"}).sort("jrc_publishing_date", -1)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get DOIs from dois collection"),
                               message=error_message(err))
    prefix = f"doisui_type/{src}/{typ}/{sub}"
    html, cnt, oacnt = standard_doi_table(rows, prefix=prefix, count_card=True)
    desc = f"{src} {typ}"
    if sub != 'None':
        desc += f"/{sub}"
    if year != 'All':
        desc += f" ({year})"
    if tag:
        desc += f" &middot; {escape(tag)}"
    if not cnt:
        # No DOIs for this filter - advise but keep the year pulldown (tag-aware,
        # so switching years keeps the tag) so another year can be chosen (also
        # avoids a divide-by-zero in the OA percentage)
        html = year_pulldown(prefix, suffix=(f"?tag={quote(tag, safe='')}" if tag else '')) \
               + "<br><br>" \
               + render_warning(f"No DOIs were found for {desc}", 'warning')
        return make_response(render_template('custom.html', urlroot=request.url_root,
                                             title=f"DOIs for {desc}", html=html, oamsg='',
                                             chartscript='', chartdiv='',
                                             navbar=generate_navbar('DOIs')))
    chartscript, chartdiv = DP.wedge_chart({'shown': oacnt, 'total': cnt}) if oacnt else ['', '']
    oamsg = f"<span style='font-size: 18pt; color: lightgray'>{oacnt/cnt*100:.1f}%</span>" \
            + f"<span style='font-size: 12pt'><br>{oacnt:,}/{cnt:,}</span>"
    return make_response(render_template('custom.html', urlroot=request.url_root,
                                         title=f"DOIs for {desc}", html=html, oamsg=oamsg,
                                         chartscript=chartscript, chartdiv=chartdiv,
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
    # show_count=False: the count is shown in the card below (with id 'totalrows',
    # which the "Filter versioned DOIs" toggler updates)
    html, cnt, oacnt = standard_doi_table(union, show_count=False)
    if not cnt:
        return render_template('warning.html', urlroot=request.url_root,
                               title=render_warning("Could not find DOIs", 'warning'),
                               message=f"Could not find any DOIs with title matching {title}")
    cards = stat_cards([("DOIs", f"<span id='totalrows'>{cnt:,}</span>"),
                        ("Open Access", f"{oacnt:,}"),
                        ("Not Open Access", f"{cnt-oacnt:,}")],
                       div_id='titles-stats')
    html = cards + html
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=f"DOIs for {title}", html=html,
                                         navbar=generate_navbar('DOIs')))


@app.route('/dois_source/<string:year>')
@app.route('/dois_source')
def dois_source(year='All'):
    ''' Legacy route: the source-only page is now the Sources tab of the tabbed
        /dois_metrics page. Redirect there (preserving the year) so old links and
        bookmarks keep working.
        Keyword arguments:
          year: publishing year to scope the redirect
        Returns:
          Redirect to /dois_metrics
    '''
    return redirect('/dois_metrics' if year == 'All' else f"/dois_metrics/{year}")


@app.route('/dois_metrics/<string:year>')
@app.route('/dois_metrics')
def show_dois_metrics(year='All'):
    ''' DOI metrics - a tabbed page over the dois collection (supersedes the
        source-only /dois_source). A year pulldown scopes the page. Tabs: Sources
        (registrar x type/subtype table + by-source and by-load-method pies), By
        type (resource-type mix), By year (volume by registrar, all years), and
        Coverage (metadata completeness). Heavy standalone pages are linked, not
        absorbed (see the "See also" strip).
        Keyword arguments:
          year: publishing year to scope to, or 'All'
        Returns:
          Response
    '''
    coll = DB['dis'].dois
    yr = {"jrc_publishing_date": {"$regex": "^" + year}} if year != 'All' else {}
    tag = request.args.get('tag') or None
    if tag:
        # Scope the whole page (every tab) to DOIs carrying this jrc_tag.name.
        yr["jrc_tag.name"] = tag
    # The "By year" tab intentionally spans all years, but should still honor a
    # tag selection - so it carries the tag match on its own, not the year one.
    tmatch = {"jrc_tag.name": tag} if tag else {}
    year_suffix = f"?tag={quote(tag, safe='')}" if tag else ''
    scope = ([escape(year)] if year != 'All' else []) + ([escape(tag)] if tag else [])
    ysfx = f" ({', '.join(scope)})" if scope else ""

    def pctof(num, den):
        return f"{num / den * 100:.0f}%" if den else "0%"
    # ----- shared header: totals + page-wide year filter -----
    try:
        total = coll.count_documents(dict(yr))
        by_src = {s: coll.count_documents({**yr, "jrc_obtained_from": s})
                  for s in ('Crossref', 'DataCite')}
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not count DOIs"),
                               message=error_message(err))
    header = (f"<h5 style='margin:14px 0 6px 0;'>Overview</h5>"
              + stat_cards([("DOIs", f"{total:,}"),
                            ("Crossref", f"{by_src['Crossref']:,}"),
                            ("DataCite", f"{by_src['DataCite']:,}")], div_id='doi-stats')
              + year_pulldown('dois_metrics', suffix=year_suffix)
              + "&nbsp;&nbsp;&nbsp;" + tag_pulldown('dois_metrics', year, tag))
    charts = ""
    # ----- Sources tab (registrar x type/subtype table + two pies) -----
    try:
        sdata, hdict = get_source_data(year, tag)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get source data"),
                               message=error_message(err))
    srows, stotal = [], 0
    for key, val in sorted(hdict.items(), key=itemgetter(1), reverse=True):
        src, typ, sub = key.split('_')
        if not sub:
            sub = 'None'
        stotal += val
        link = (f"/doisui_type/{src}/{typ}/{sub}/{year}" if year != 'All'
                else f"/doisui_type/{src}/{typ}/{sub}")
        # Carry the active tag so the drill-down list matches the (tag-scoped) count.
        link += year_suffix
        srows.append([src, typ, sub if sub != 'None' else '',
                      safe(f"<a href='{link}'>{val:,}</a>")])
    src_table = render_table(['Source', 'Type', 'Subtype', 'Count'], srows,
                             table_id='doisrc', css='tablesorter numberlast-scroll',
                             footer=[fcell('Total', colspan=3), fcell(f"{stotal:,}")])
    s1, d1 = DP.pie_chart(sdata, f"DOIs by source", "source", width=450,
                          colors=DP.SOURCE_PALETTE)
    lm_payload = ([{"$match": dict(yr)}] if yr else []) + \
                 [{"$group": {"_id": "$jrc_load_source", "count": {"$sum": 1}}},
                  {"$sort": {"count": -1}}]
    try:
        lm = {(row['_id'] or 'None'): row['count'] for row in coll.aggregate(lm_payload)}
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get load methods"),
                               message=error_message(err))
    s2, d2 = DP.pie_chart(lm, f"DOIs by load method", "source", width=450,
                          colors=DP.get_colors_by_count(len(lm) or 1))
    charts += s1 + s2
    sources_body = two_col("<div class='tag-scrollbox' style='flex:1 1 440px;min-width:360px;"
                           f"overflow-x:auto;'>{src_table}</div>",
                           f"<div style='flex:0 0 auto;'>{d1}{d2}</div>")
    # ----- By type tab (resource-type mix; merges Crossref type + DataCite general) -----
    type_merge = {"dataset": "Dataset", "journal-article": "JournalArticle",
                  "posted-content": "Preprint", "book": "BookChapter",
                  "book-chapter": "BookChapter", "proceedings-article": "ConferenceProceeding",
                  "other": "Other", "ComputationalNotebook": "Software",
                  "DataPaper": "JournalArticle", "Image": "Audiovisual",
                  "component": "BiologicalComponent", "grant": "Grant",
                  "peer-review": "PeerReview"}
    tdata = {}
    try:
        for row in coll.aggregate([{"$match": {**yr, "jrc_obtained_from": "Crossref"}},
                                   {"$group": {"_id": "$type", "count": {"$sum": 1}}}]):
            lab = type_merge.get(row['_id'] or 'None', row['_id'] or 'None')
            tdata[lab] = tdata.get(lab, 0) + row['count']
        for row in coll.aggregate([{"$match": {**yr, "jrc_obtained_from": "DataCite"}},
                                   {"$group": {"_id": "$types.resourceTypeGeneral",
                                               "count": {"$sum": 1}}}]):
            lab = type_merge.get(row['_id'] or 'None', row['_id'] or 'None')
            tdata[lab] = tdata.get(lab, 0) + row['count']
        proto = coll.count_documents({**yr, "jrc_journal": "protocols.io"})
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get type data"),
                               message=error_message(err))
    if proto and 'Preprint' in tdata:
        tdata['Preprint'] -= proto
        tdata['Protocols'] = proto
    bytype_body = "<div>No typed DOIs.</div>"
    if tdata:
        trows = [[t, f"{c:,}"] for t, c in sorted(tdata.items(), key=itemgetter(1), reverse=True)]
        type_table = render_table(['Type', 'Count'], trows, table_id='doitype',
                                  css='tablesorter numberlast-scroll',
                                  footer=[fcell('Total'), fcell(f"{sum(tdata.values()):,}")])
        s3, d3 = DP.pie_chart(dict(sorted(tdata.items(), key=itemgetter(1), reverse=True)),
                              f"DOIs by type", "type", width=600, height=460,
                              colors=DP.get_colors_by_count(len(tdata)))
        charts += s3
        bytype_body = two_col("<div class='tag-scrollbox' style='flex:1 1 440px;min-width:320px;'>"
                              f"{type_table}</div>", f"<div style='flex:0 0 auto;'>{d3}</div>")
    # ----- By year tab (all years, by registrar) -----
    ymap = {}
    try:
        for row in coll.aggregate([
                {"$match": {"jrc_publishing_date": {"$exists": True}, **tmatch}},
                {"$group": {"_id": {"yr": {"$substr": ["$jrc_publishing_date", 0, 4]},
                                    "src": "$jrc_obtained_from"}, "n": {"$sum": 1}}}]):
            y = row['_id'].get('yr')
            if not y:
                continue
            entry = ymap.setdefault(y, {'Crossref': 0, 'DataCite': 0})
            if row['_id'].get('src') in entry:
                entry[row['_id']['src']] += row['n']
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get DOIs by year"),
                               message=error_message(err))
    byyear_body = "<div>No dated DOIs.</div>"
    years = sorted(ymap)
    if years:
        ydata = {"years": years, "Crossref": [ymap[y]['Crossref'] for y in years],
                 "DataCite": [ymap[y]['DataCite'] for y in years]}
        s4, d4 = DP.stacked_bar_chart(ydata, "DOIs by publishing year (by registrar)",
                                      xaxis="years", yaxis=["Crossref", "DataCite"],
                                      colors=DP.SOURCE_PALETTE, orient=pi / 4,
                                      width=1000, height=350)
        charts += s4
        byyear_body = ("<div style='color:#a8c4e0;font-size:0.82em;margin-bottom:8px;'>"
                       "All years (not scoped by the year filter). See the "
                       "<a href='/dois_yearly'>DOI yearly report</a> for detail.</div>" + d4)
    # ----- Coverage tab: metadata completeness -----
    # Each entry is (label, query, scope). Journal-article fields (PMID, PMC, MeSH,
    # full text, preprint, acknowledgements, newsletter, OpenAlex) essentially never
    # apply to DataCite datasets, so they are scored against the Crossref population
    # ('Crossref' scope) rather than the whole corpus; the rest are corpus-wide ('All').
    cov_fields = [("Author (jrc_author)", {"jrc_author.0": {"$exists": True}}, 'All'),
                  ("License (jrc_license)", {"jrc_license": {"$exists": True}}, 'All'),
                  ("Tag (jrc_tag)", {"jrc_tag.0": {"$exists": True}}, 'All'),
                  ("Full-text URL (jrc_fulltext_url)",
                   {"jrc_fulltext_url": {"$exists": True}}, 'Crossref'),
                  ("PubMed ID (jrc_pmid)", {"jrc_pmid": {"$exists": True}}, 'Crossref'),
                  ("Acknowledgements (jrc_acknowledgements)",
                   {"jrc_acknowledgements": {"$exists": True}}, 'Crossref'),
                  ("Newsletter (jrc_newsletter)",
                   {"jrc_newsletter": {"$exists": True}}, 'Crossref'),
                  ("PMC ID (jrc_pmc)", {"jrc_pmc": {"$exists": True}}, 'Crossref'),
                  ("OpenAlex ID (jrc_openalex_id)",
                   {"jrc_openalex_id": {"$exists": True}}, 'Crossref'),
                  ("Citation count (jrc_citation_count)",
                   {"jrc_citation_count": {"$exists": True}}, 'All'),
                  ("MeSH terms (jrc_mesh)", {"jrc_mesh.0": {"$exists": True}}, 'Crossref'),
                  ("Linked preprint (jrc_preprint)",
                   {"jrc_preprint.0": {"$exists": True}}, 'Crossref'),
                  ("Open access (jrc_is_oa)", {"jrc_is_oa": True}, 'All')]
    cx_total = by_src['Crossref']
    crows, cov = [], {}
    try:
        for label, cquery, scope in cov_fields:
            if scope == 'Crossref':
                num = coll.count_documents({**yr, "jrc_obtained_from": "Crossref", **cquery})
                den = cx_total
            else:
                num = coll.count_documents({**yr, **cquery})
                den = total
            # Show the friendly label in the cell; keep the jrc_ field name in a
            # hover tooltip for curators (rather than inline in user-facing text).
            friendly = label.split(' (')[0]
            field = label[label.find('(') + 1:label.rfind(')')] if '(' in label else ''
            field_cell = safe(f"<span class='field-tip' data-field='{escape(field)}'>"
                              f"{escape(friendly)}</span>") if field else friendly
            crows.append([field_cell, scope, f"{num:,}", pctof(num, den)])
            name = friendly
            if scope == 'Crossref':
                name += " (Crossref)"
            cov[name] = (num / den) if den else 0  # fraction; axis formats it as a %
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not compute coverage"),
                               message=error_message(err))
    cov_table = render_table(['Metadata field', 'Scope', 'DOIs', '% of scope'], crows,
                             table_id='doicov', css='tablesorter numberlast-scroll')
    cs, cd = DP.hbar_chart(cov, f"Metadata coverage (% of applicable DOIs)",
                           value_label="Coverage", value_format="0%", show_pct=False,
                           width=680, height=430)
    charts += cs
    coverage_body = ("<div style='color:#a8c4e0;font-size:0.82em;margin-bottom:8px;'>"
                     f"Share of DOIs carrying each field. Journal-article fields "
                     f"(scope <em>Crossref</em>) are scored against the {cx_total:,} "
                     "Crossref DOIs, since DataCite datasets can't carry them; the rest "
                     f"against all {total:,}.</div>"
                     + two_col("<div style='flex:1 1 440px;min-width:360px;overflow-x:auto;'>"
                               f"{cov_table}</div>",
                               f"<div style='flex:0 0 auto;'>{cd}</div>"))
    # ----- tabs -----
    active_tab = request.args.get('tab')
    if active_tab not in ('sources', 'bytype', 'byyear', 'coverage'):
        active_tab = 'sources'

    tabs = ('<ul class="nav nav-tabs" role="tablist">'
            + tab_button('sources', 'Sources', active_tab == 'sources')
            + tab_button('bytype', 'By type', active_tab == 'bytype')
            + tab_button('byyear', 'By year', active_tab == 'byyear')
            + tab_button('coverage', 'Coverage', active_tab == 'coverage')
            + '</ul><div class="tab-content">'
            + tab_pane('sources', sources_body, active_tab == 'sources')
            + tab_pane('bytype', bytype_body, active_tab == 'bytype')
            + tab_pane('byyear', byyear_body, active_tab == 'byyear')
            + tab_pane('coverage', coverage_body, active_tab == 'coverage')
            + '</div>')
    seealso = see_also([("Yearly report", "/dois_yearly"),
                        ("Publishers", "/dois_publisher"),
                        ("Licenses", "/dois_license"),
                        ("Citations", "/citation_metrics/crossref"),
                        ("Impact by source", "/source_metrics")])
    html = header + tabs + seealso + charts
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=f"DOI metrics{ysfx}", html=html, bokeh=True,
                                         navbar=generate_navbar('DOIs')))


@app.route('/dois_type/<string:year>')
@app.route('/dois_type')
def dois_type(year='All'):
    ''' Legacy route: the by-type breakdown is now the "By type" tab of the tabbed
        /dois_metrics page. Redirect there (preserving the year) so old links and
        bookmarks keep working.
        Keyword arguments:
          year: publishing year to scope the redirect
        Returns:
          Redirect to /dois_metrics (By type tab)
    '''
    base = '/dois_metrics' if year == 'All' else f"/dois_metrics/{year}"
    return redirect(f"{base}?tab=bytype")


@app.route('/dois_licenser/<string:source>/<string:lic>/<string:year>')
@app.route('/dois_licenser/<string:source>/<string:lic>')
@app.route('/dois_licenser/<string:source>')
def dois_license_report(source, lic=None, year='All'):
    ''' Show DOIs by license, optionally scoped to one jrc_tag.name via ?tag=
        (keeps the /dois_license drill-down in sync with its tag filter).
    '''
    if lic == 'None':
        lic = None
    payload = [{"$match": {"jrc_obtained_from": source, "jrc_license": lic}},
               {"$sort": {"count": -1}}]
    if year != 'All':
        payload[0]['$match']['jrc_publishing_date'] = {"$regex": "^"+ year}
    tag = request.args.get('tag') or None
    if tag:
        payload[0]['$match']['jrc_tag.name'] = tag
    try:
        rows = DB['dis'].dois.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get license data from dois"),
                               message=error_message(err))
    html, _, _ = standard_doi_table(rows, count_card=True)
    title = f"{source} DOIs for license {lic}"
    if year != 'All':
        title += f" for {year}"
    if tag:
        title += f" &middot; {escape(tag)}"
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         navbar=generate_navbar('DOIs')))


@app.route('/dois_license/<string:year>')
@app.route('/dois_license')
def dois_license(year='All'):
    ''' Show DOIs by license, optionally scoped to one jrc_tag.name (?tag=)
        alongside the year filter; drill-down links carry the tag through.
    '''
    ypayload = {} if year == 'All' else {"jrc_publishing_date": {"$regex": "^"+ year}}
    tag = request.args.get('tag') or None
    if tag:
        ypayload["jrc_tag.name"] = tag
    year_suffix = f"?tag={quote(tag, safe='')}" if tag else ''
    try:
        total = DB['dis'].dois.count_documents(ypayload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get total number of DOIs"),
                               message=error_message(err))
    payload = [{"$group": {"_id": {"source": "$jrc_obtained_from", "license": "$jrc_license"},
                           "count": {"$sum": 1}}},
               {"$sort": {"license": 1}}]
    if ypayload:
        payload.insert(0, {"$match": ypayload})
    try:
        rows = DB['dis'].dois.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get license data from dois"),
                               message=error_message(err))
    html = year_pulldown('dois_license', suffix=year_suffix) \
           + "&nbsp;&nbsp;&nbsp;" + tag_pulldown('dois_license', year, tag)
    trows = []
    cnt = {"Crossref": 0, "DataCite": 0}
    data = {}
    lines = {}
    for row in rows:
        orig = '/' + row['_id']['license'] if 'license' in row['_id'] else '/None'
        if 'license' not in row['_id']:
            lic = 'Not found'
        else:
            lic = row['_id']['license']
        if lic not in data:
            data[lic] = 0
        data[lic] += row['count']
        cnt[row['_id']['source']] += row['count']
        if lic not in lines:
            lines[lic] = {"Crossref": 0, "DataCite": 0, "orig": orig}
        lines[lic][row['_id']['source']] += row['count']
    srt = sorted(data.items(), key=lambda item: item[1], reverse=True)
    data = dict(srt)
    if not data or not total:
        html = year_pulldown('dois_license', suffix=year_suffix) \
               + "&nbsp;&nbsp;&nbsp;" + tag_pulldown('dois_license', year, tag) + "<br><br>" \
               + render_warning(f"No DOIs were found for {year}.", 'warning')
        endpoint_access()
        return make_response(render_template('general.html', urlroot=request.url_root,
                                             title="DOIs by license", html=html,
                                             navbar=generate_navbar('DOIs')))
    defcnt = cnt['Crossref'] + cnt['DataCite'] - data.get('Not found', 0)
    for lic in sorted(lines.keys(), key=str.casefold):
        clink = f"<a href='/dois_licenser/Crossref{lines[lic]['orig']}/{year}{year_suffix}'>" \
                + f"{lines[lic]['Crossref']}</a>" if lines[lic]['Crossref'] else '0'
        dlink = f"<a href='/dois_licenser/DataCite{lines[lic]['orig']}/{year}{year_suffix}'>" \
                + f"{lines[lic]['DataCite']}</a>" if lines[lic]['DataCite'] else '0'
        trows.append([lic, safe(clink), safe(dlink)])
    html += render_table(['Source', 'Crossref', 'DataCite'], trows, table_id='license',
                         css='tablesorter numbers-scroll',
                         footer=[fcell('Total'), fcell(f"{cnt['Crossref']:,}"),
                                 fcell(f"{cnt['DataCite']:,}")])
    pre = f"<span style='font-size: 18pt; color: lime'>{defcnt/total*100:.1f}%</span>" \
          + "<span style='font-size: 14pt'> of Janelia DOIs have a known license" \
          + f"</span><span style='font-size: 12pt'><br>{defcnt:,}/{total:,}</span><br>"
    html = pre + html
    title = "DOIs by license"
    scope = ([escape(year)] if year != 'All' else []) + ([escape(tag)] if tag else [])
    if scope:
        title += f" ({', '.join(scope)})"
    # Ranked categorical breakdown -> horizontal bar (audit chart policy). Colour
    # defaults to get_colors_by_count (SOURCE_PALETTE is reserved for the 2-way
    # registrar split); height scales with the number of licenses so bars stay legible.
    lic_height = max(360, 30 * len(data) + 90)
    chartscript, chartdiv = DP.hbar_chart(data, "DOIs by license", value_label="DOIs",
                                          width=700, height=lic_height, show_values=True)
    endpoint_access()
    return make_response(render_template('bokeh.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         chartscript=chartscript, chartdiv=chartdiv,
                                         navbar=generate_navbar('DOIs')))


@app.route('/dois_provider')
def dois_provider():
    ''' Superseded by the richer /subscription/provider summary table. The
        per-provider DOI list (/dois_provider/<prov>) is still reachable from
        the "Janelia publications" card on /subscription/provider/<prov>.
    '''
    return redirect('/subscription/provider')


@app.route('/dois_provider/<string:prov>')
def dois_provider_with_janelia(prov):
    ''' Show DOIs by provider with Janelia first author
    '''
    journals = []
    publishers = []
    try:
        rows = DB['dis'].subscription.find({"provider": prov})
        for row in rows:
            journals.append(row['title'])
            publishers.append(row['publisher'])
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get publishers by provider"),
                               message=error_message(err))
    payload = {"$or": [{"publisher": {"$in": publishers}},
                       {"jrc_journal": {"$in": journals}}],
               "jrc_first_author": {"$exists": True}}
    try:
        rows = DB['dis'].dois.find(payload).sort("jrc_publishing_date", -1)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get DOIs by provider"),
                               message=error_message(err))
    trows = []
    cnt = 0
    oa_data = {}
    sheet = []
    sheet.append("Published\tDOI\tPublisher\tJournal\tTitle\tStatus")
    for row in rows:
        stat = row.get('jrc_oa_status', '')
        sheet.append(f"{row['jrc_publishing_date']}\t{row['doi']}\t{row['publisher']}\t" \
                     + f"{row['jrc_journal']}\t{DL.get_title(row)}\t{stat}")
        statlabel = stat.capitalize() if stat else 'Unknown'
        oa_data[statlabel] = oa_data.get(statlabel, 0) + 1
        if stat:
            stat = safe(f"<span class='oa_{stat}'>{stat.capitalize()}</span>")
        trows.append([row['jrc_publishing_date'], safe(doi_link(row['doi'])),
                      row['publisher'], row['jrc_journal'],
                      render_title_html(DL.get_title(row)), stat])
        cnt += 1
    html = render_table(['Published', 'DOI', 'Publisher', 'Journal', 'Title', 'Status'], trows,
                        table_id='dois', css='tablesorter standard-scroll')
    cardlist = [("DOIs", f"{cnt:,}")]
    for key in sorted(oa_data, key=oa_status_rank):
        cardlist.append((key, f"{oa_data[key]:,}", DP.OA_COLORS.get(key, 'crimson')))
    cards = stat_cards(cardlist, div_id='provider-stats')
    sheet = create_downloadable(f"{prov}_dois", None, "\n".join(sheet))
    html = cards + sheet + "<br><br>" + html
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=f"DOIs by provider {prov} with " \
                                               + "Janelia first author",
                                         html=html, navbar=generate_navbar('DOIs')))


@app.route('/dois_report/<string:year>')
@app.route('/dois_report')
def dois_report(year=None):
    ''' Show year in review
    '''
    if year is None:
        year = str(datetime.now().year)
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
            stat[val] = f"{BOLD}{typed[key]:,}</span> {val.lower()}"
            if val in ('Journal articles', 'Preprints'):
                sheet.append(f"{val}\t{typed[key]}")
                if val == 'Journal articles':
                    stat[val] += f" in {BOLD}{cnt:,}</span> journals"
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
        stat['figshare'] = f"{BOLD}{cnt:,}</span> " \
                           + "figshare (unversioned) articles"
        sheet.append(f"figshare (unversioned) articles\t{cnt}")
        rows = coll.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get journal figshare metrics"),
                               message=error_message(err))
    if cnt:
        cnt = 0
        for row in rows:
            cnt += 1
        stat['figshare'] += f" with {BOLD}{cnt:,}</span> " \
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
        if row.get('employeeId') and row.get('orcid'):
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
    stat['ORCID'] = f"{BOLD}{cnt:,}</span> " \
                    + "distinct Janelia authors for all entries, " \
                    + f"{BOLD}{orc:,}</span> " \
                    + f"({(orc / cnt * 100) if cnt else 0:.2f}%) with ORCIDs"
    sheet.extend([f"Distinct Janelia authors\t{cnt}", f"Janelia authors with ORCIDs\t{orc}"])
    # Entries
    if 'DataCite' not in typed:
        typed['DataCite'] = 0
    for key in ('DataCite', 'Crossref'):
        sheet.insert(0, f"{key} entries\t{typed[key]}")
    stat['Entries'] = f"{BOLD}{typed['Crossref']:,}" \
                      + "</span> Crossref entries<br>" \
                      + f"{BOLD}{typed['DataCite']:,}" \
                      + "</span> DataCite entries"
    if 'Journal articles' not in stat:
        stat['Journal articles'] = f"{BOLD}0</span> journal articles<br>"
    if 'Preprints' not in stat:
        stat['Preprints'] = f"{BOLD}0</span> preprints<br>"
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
    stat['Author'] = f"{BOLD}{cnt:,}</span> " \
                     + "entries with all Janelia authors<br>"
    stat['Author'] += f"{BOLD}{total-cnt:,}</span> " \
                      + "entries with at least one external collaborator<br>"
    stat['Author'] += f"{BOLD}{middle:,}</span> " \
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
    stat['Preprints'] += f"{BOLD}{cnt['journal']:,}" \
                         + "</span> journal articles without preprints<br>"
    stat['Preprints'] += f"{BOLD}{cnt['preprint']:,}" \
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
    avg_tags = (total / cnt) if cnt else 0
    stat['Tags'] = f"{BOLD}{avg_tags:.1f}</span> " \
                   + "average tags per tagged entry"
    sheet.append(f"Average tags per tagged entry\t{avg_tags:.1f}")
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
           + f"<div class='yearstatd'>{html}</div>"
    html += '<br>' + year_pulldown('dois_report', all_years=False)
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=f"{year}", html=html,
                                         navbar=generate_navbar('DOIs')))


@app.route('/dois_yearly/<string:year>')
@app.route('/dois_yearly')
def dois_yearly(year=None):
    ''' Show year in review, optionally scoped to one jrc_tag.name (?tag=)
        alongside the (per-year) year filter.
    '''
    # A "year in review" is inherently per-year (its pulldown offers no "All");
    # fall back to the current year for a missing or hand-typed "All" value.
    if year in (None, 'All'):
        year = str(datetime.now().year)
    tag = request.args.get('tag') or None
    tmatch = {"jrc_tag.name": tag} if tag else {}
    year_suffix = f"?tag={quote(tag, safe='')}" if tag else ''
    pmap = {"journal-article": "Journal articles", "posted-content": "Posted content",
            "preprints": "Preprints", "proceedings-article": "Proceedings articles",
            "book-chapter": "Book chapters", "datasets": "Datasets",
            "peer-review": "Peer reviews", "grant": "Grants", "other": "Other"}
    payload = [{"$match": {"jrc_publishing_date": {"$regex": "^"+ year}, **tmatch}},
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
    first, last, anyauth = get_first_last_authors(year, tag)
    # Pre-seed every type at 0 so a year with no data (e.g. a future year) still has
    # stat['Journal articles'] / stat['Preprints'] etc. for the summary and can't KeyError.
    stat = {val: f"{BOLD}0</span> {val.lower()}" for val in pmap.values()}
    # Journal count
    payload = [{"$unwind" : "$container-title"},
               {"$match": {"container-title": {"$exists": True}, "type": "journal-article",
                           "jrc_publishing_date": {"$regex": "^"+ year}, **tmatch}},
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
            additional = {}
            if key in first:
                additional['first'] = first[key]
            if key in last:
                additional['last'] = last[key]
            if key in anyauth:
                additional['any'] = anyauth[key]
            stat[val] = f"{BOLD}{typed[key]:,}</span> {val.lower()}"
            if val in ('Journal articles', 'Preprints'):
                if val == 'Journal articles':
                    stat[val] += f" in {BOLD}{cnt:,}</span> journals<br>"
                    if additional.get('first'):
                        stat[val] += f"&nbsp;&nbsp;{BOLD}{additional['first']}</span> " \
                                     + "with Janelian first author<br>"
                    if additional.get('last'):
                        stat[val] += f"&nbsp;&nbsp;{BOLD}{additional['last']}</span> " \
                                     + "with Janelian last author<br>"
                    if additional.get('any'):
                        stat[val] += f"&nbsp;&nbsp;{BOLD}{additional['any']}</span> " \
                                     + "with any current Janelian author"
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
        if row.get('employeeId') and row.get('orcid'):
            orcs[row['employeeId']] = True
    payload = [{"$match": {"jrc_publishing_date": {"$regex": "^"+ year}, **tmatch}},
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
    stat['ORCID'] = f"{BOLD}{cnt:,}</span> " \
                    + "distinct Janelia authors for all entries, " \
                    + f"{BOLD}{orc:,}</span> " \
                    + f"({(orc / cnt * 100) if cnt else 0:.2f}%) with ORCIDs"
    # Journals
    journal = get_top_journals(year, tag=tag)
    cnt = 0
    stat['Topjournals'] = ""
    for key in sorted(journal, key=journal.get, reverse=True):
        if key in REPOSITORY:
            continue
        stat['Topjournals'] += f"&nbsp;&nbsp;&nbsp;&nbsp;{ITALIC}{key}</span>: {journal[key]}<br>"
        cnt += 1
        if cnt >= 10:
            break
    html = f"<h2 class='green1'>Articles</h2>{stat['Journal articles']}<br>{stat['Preprints']}" \
           + f"<br><br><h2 class='green1'>Authors</h2>{stat['ORCID']}" \
           + "<br><br><h2 class='green1'>Top journals</h2>" \
           + f"<p style='font-size: 14pt;line-height:90%;'>{stat['Topjournals']}</p>"
    tagsfx = f" &middot; {escape(tag)}" if tag else ''
    html = f"<div class='titlestat'>{year} YEAR IN REVIEW{tagsfx}</div><br>" \
           + f"<div class='yearstat'>{html}</div>"
    html += '<br>' + year_pulldown('dois_yearly', all_years=False, suffix=year_suffix) \
            + "&nbsp;&nbsp;&nbsp;" + tag_pulldown('dois_yearly', year, tag)
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=f"{year}{tagsfx}", html=html,
                                         navbar=generate_navbar('DOIs')))


@app.route('/dois_time/<string:period>/<string:year>')
@app.route('/dois_time/<string:period>')
def dois_time(period, year=None):
    ''' Show DOIs by year or month
        Keyword arguments:
          period: "year" or "month"
          year: year to filter (month only, defaults to current year)
    '''
    if period not in ('year', 'month'):
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Invalid period"),
                               message="period must be 'year' or 'month'")
    if period == 'month' and year is None:
        year = str(datetime.now().year)
    substr_len = 7 if period == 'month' else 4
    pipeline = []
    # Treat an explicit 'All' like no year filter (it's a label, not a date prefix -
    # "^All" would match nothing); the month nav below is already guarded for 'All'.
    if year and year != 'All':
        pipeline.append({"$match": {"jrc_publishing_date": {"$regex": "^" + year}}})
    pipeline += [
        {"$group": {"_id": {period: {"$substrBytes": ["$jrc_publishing_date", 0, substr_len]},
                            "source": "$jrc_obtained_from"},
                    "count": {"$sum": 1}}},
        {"$sort": {f"_id.{period}": 1}}
    ]
    try:
        rows = DB['dis'].dois.aggregate(pipeline)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get counts from dois collection"),
                               message=error_message(err))
    counter = collections.defaultdict(lambda: 0, {})
    trows = []
    nav = {}
    if period == 'year':
        periods = {}
        for row in rows:
            p = row['_id']['year']
            if p not in periods:
                periods[p] = {}
            periods[p][row['_id']['source']] = row['count']
        data = {'years': [], 'Crossref': [], 'DataCite': []}
        for yr in sorted(periods, reverse=True):
            if yr < '2006':
                continue
            data['years'].insert(0, str(yr))
            onclick = "onclick='nav_post(\"publishing_year\",\"" + yr + "\")'"
            cells = [safe(f"<a href='#' {onclick}>{yr}</a>")]
            for source in SOURCES:
                if source in periods[yr]:
                    data[source].insert(0, periods[yr][source])
                    onclick = "onclick='nav_post(\"publishing_year\",\"" + yr \
                              + "\",\"" + source + "\")'"
                    cells.append(safe(f"<a href='#' {onclick}>{periods[yr][source]:,}</a>"))
                    counter[source] += periods[yr][source]
                else:
                    data[source].insert(0, 0)
                    cells.append("")
            trows.append(cells)
        # Tap a year bar -> all DOIs published that year (mirrors the year cell)
        nav = {y: {"field": "publishing_year", "value": y} for y in data['years']}
        title = "DOIs published by year"
        chart_title = "DOIs published by year/source"
        pulldown = year_pulldown('dois_time/year')
    else:
        data = {'months': [f"{mon:02}" for mon in range(1, 13)],
                'Crossref': [0] * 12, 'DataCite': [0] * 12}
        for row in rows:
            data[row['_id']['source']][int(row['_id']['month'][-2:]) - 1] = row['count']
        for mon in data['months']:
            mname = date(1900, int(mon), 1).strftime('%B')
            cells = [mname]
            for source in SOURCES:
                if data[source][int(mon) - 1]:
                    onclick = "onclick='nav_post(\"publishing_year\",\"" \
                              + f"{year}-{mon}" + "\",\"" + source + "\")'"
                    cells.append(safe(f"<a href='#' {onclick}>{data[source][int(mon)-1]:,}</a>"))
                    counter[source] += data[source][int(mon) - 1]
                else:
                    cells.append("")
            trows.append(cells)
        # Tap a month bar -> all DOIs published that year-month
        if year and year != 'All':
            nav = {m: {"field": "publishing_year", "value": f"{year}-{m}"}
                   for m in data['months']}
        title = f"DOIs published by month ({year})"
        chart_title = title
        pulldown = year_pulldown('dois_time/month', all_years=False)
    footer = [fcell('Total')] + [fcell(f"{counter[source]:,}", align='center')
                                 for source in SOURCES]
    html = render_table([period.capitalize(), 'Crossref', 'DataCite'], trows,
                        table_id=f"{period}s", css='tablesorter numbers-scroll',
                        footer=footer) + "<br>" + pulldown
    xaxis = 'years' if period == 'year' else 'months'
    chartscript, chartdiv = DP.stacked_bar_chart(data, chart_title, xaxis=xaxis,
                                                 yaxis=SOURCES,
                                                 colors=DP.SOURCE_PALETTE, nav=nav)
    endpoint_access()
    return make_response(render_template('bokeh.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         chartscript=chartscript, chartdiv=chartdiv,
                                         navbar=generate_navbar('DOIs')))

@app.route('/doiui_group/<string:field>/<string:unwind>')
@app.route('/doiui_group/<string:field>')
def show_grouped_dois(field, unwind=None):
    ''' A grouping of DOIs as a table for a given field
    '''
    payload = ([{"$group": {"_id": f"${field}", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}
               ])
    if unwind:
        payload.insert(0, {"$unwind": f"${unwind}"})
    try:
        rows = DB['dis'].dois.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get DOIs from dois collection"),
                               message=error_message(err))
    trows = []
    total = cnt = 0
    data = {}
    for row in rows:
        cnt += 1
        trows.append([row['_id'], row['count']])
        if not isinstance(row['_id'], (dict, list)):
            data[row['_id']] = row['count']
        total += row['count']
    if not total:
        return render_template('warning.html', urlroot=request.url_root,
                               title=render_warning("Could not find DOIs", 'warning'),
                               message=f"Could not find any DOIs using {field}")
    html = render_table([field, 'Count'], trows, table_id='dois',
                        css='tablesorter numberlast-scroll',
                        footer=[fcell('Total'), fcell(f"{total}")])
    chartscript = chartdiv = ""
    if 1 < cnt <= 256 and data:
        chartscript, chartdiv = DP.treemap_chart(data, field, width=875, height=600,
                                                 value_format="0,0")
    endpoint_access()
    return make_response(render_template('bokeh.html', urlroot=request.url_root,
                                         title=f"{field} counts ({cnt:,})", html=html,
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
@app.route('/doiui/insert/<string:idate>/<string:source>')
def show_insert(idate, source='Crossref'):
    '''
    Return DOIs that have been inserted since a specified date
    '''
    try:
        isodate = datetime.strptime(idate,'%Y-%m-%d')
    except Exception as err:
        raise InvalidUsage(str(err), 400) from err
    try:
        rows = DB['dis'].dois.find({"jrc_inserted": {"$gte" : isodate},
                                    "jrc_obtained_from": source},
                                   {'_id': 0}).sort([("jrc_inserted", -1),
                                                     ("jrc_publishing_date", -1)])
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get DOIs"),
                               message=error_message(err))
    if not rows:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("DOIs not found"),
                               message=f"No DOIs were inserted on or after {idate}")
    trows = []
    row_classes = []
    fileoutput = ""
    limit = weeks_ago(2)
    for row in rows:
        typ = subtype = ""
        if row.get('type'):
            typ = row['type']
            if row.get('subtype'):
                subtype = row['subtype']
                typ += f" {subtype}"
        elif row.get('types', {}).get('resourceTypeGeneral'):
            typ = row['types']['resourceTypeGeneral']
        version = []
        for ver in row.get('relation', {}).get('is-version-of', []):
            if ver.get('id-type') == 'doi' and ver.get('id') not in version:
                version.append(ver.get('id'))
        version = doi_link(version) if version else ""
        news = row.get('jrc_newsletter', '')
        if (not news) and (row['jrc_publishing_date'] >= str(limit)) \
           and (typ == 'journal-article' or subtype == 'preprint' \
                or row['jrc_obtained_from'] == source):
            rclass = 'candidate'
        else:
            rclass = 'other'
        jpd = row['jrc_publishing_date'] if row['jrc_publishing_date'] >= str(limit) else \
              f"<span style='color: gray'>{row['jrc_publishing_date']}</span>"
        tags = ', '.join(sorted([tag['name'] for tag in row['jrc_tag']])) \
               if 'jrc_tag' in row else ""
        row_classes.append(rclass)
        trows.append([safe(doi_link(row['doi'])), typ, safe(jpd), row['publisher'],
                      str(row['jrc_inserted']), safe(version), news,
                      safe(f"<span style='font-size: 10pt;'>{tags}</span>")])
        # Download file keeps its own column set (no Publisher, raw values)
        frow = "\t".join([row['doi'], typ, row['jrc_publishing_date'],
                          str(row['jrc_inserted']), version, news, tags])
        fileoutput += f"{frow}\n"
    html = render_table(['DOI', 'Type', 'Published', 'Publisher', 'Inserted',
                         'Is version of', 'Newsletter', 'Tags'], trows,
                        table_id='dois', css='tablesorter numbers-scroll',
                        row_classes=row_classes)
    cbutton = "<button class=\"btn btn-outline-warning\" " \
              + "onclick=\"$('.other').toggle();\">Filter for candidate DOIs</button>"
    html = cbutton + f" &nbsp;{create_downloadable('jrc_inserted', None, fileoutput)}{html}"
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=f"DOIs inserted from {source} on or after {idate}",
                                         html=html, navbar=generate_navbar('DOIs')))


@app.route('/doiui/custom/<string:year>', methods=['OPTIONS', 'POST'])
@app.route('/doiui/custom', methods=['OPTIONS', 'POST'])
def show_doiui_custom(year='All'):
    '''
    Show a custom DOI query (UI)
    UI endpoint: render the DOI records matching field/value (or a raw query) as an
    HTML page. Optionally scope to a publishing year via the /doiui/custom/{year} form.
    ---
    tags:
      - DOI
    parameters:
      - in: path
        name: year
        required: true
        schema:
          type: string
        description: Publishing year to scope to (only on /doiui/custom/{year})
    requestBody:
      required: true
      content:
        application/x-www-form-urlencoded:
          schema:
            type: object
            required: [field, value]
            properties:
              field:
                type: string
                description: DOI field to match
              value:
                type: string
                description: Value to match (or the sentinels !EXISTS! / !NOTEXISTS!)
    responses:
      '200':
        description: HTML page of matching DOIs (or an HTML error/empty-result page)
        content:
          text/html:
            schema:
              type: string
    '''
    ipd = receive_payload()
    if request.form:
        for row in ('field', 'value'):
            if row not in ipd or not ipd[row]:
                return render_template('error.html', urlroot=request.url_root,
                                       title=render_warning(f"Missing {row}"),
                                       message=f"You must specify a {row}")
        display_value = '' if ipd['value'] in ("!EXISTS!", "!NOTEXISTS!") else ipd['value']
        payload, ptitle = get_custom_payload(ipd, display_value)
    else:
        payload = ipd['query']
        ipd['field'] = "_".join(list(payload.keys()))
        ptitle = ''
    if year != 'All':
        payload['jrc_publishing_date'] = {"$regex": "^"+ year}
    try:
        rows = list(DB['dis'].dois.find(payload))
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get DOIs"),
                               message=error_message(err))
    if not rows:
        # Valid query, no matches: inline warning on the custom page (keeps the page
        # title) instead of a full error page. Escape the user-supplied field/value -
        # the old error.html path was auto-escaped by Jinja; render_warning is not.
        emsg = f"No DOIs were found for {escape(ipd['field'])}=" \
               + f"{escape(str(ipd.get('value', '')))}."
        endpoint_access()
        return make_response(render_template('custom.html', urlroot=request.url_root,
                                             title=ptitle, html=render_warning(emsg, 'warning'),
                                             oamsg='', chartscript='', chartdiv='',
                                             navbar=generate_navbar('DOIs')))
    jorp = sum(1 for row in rows if DL.is_journal(row) or DL.is_preprint(row))
    newsletter = sum(1 for row in rows if row.get('jrc_newsletter'))
    rows.sort(key=DL.get_publishing_date, reverse=True)
    # Standard DOI-list table (version-filter toggle + download) with Newsletter kept
    # as a trailing extra column; the DOIs card carries id='totalrows' for the toggler.
    table_html, cnt, oacnt = standard_doi_table(rows, show_count=False,
                                                extra_headers=['Newsletter'],
                                                extra_fn=lambda row: [row.get('jrc_newsletter', '')])
    cards = stat_cards([("DOIs", f"<span id='totalrows'>{cnt:,}</span>"),
                        ("Journals/preprints", f"{jorp:,}"),
                        ("In newsletter", f"{newsletter:,}")], div_id='custom-stats')
    data = {'shown': oacnt, 'total': cnt}
    chartscript, chartdiv = DP.wedge_chart(data) if oacnt else ['', '']
    oamsg = f"<span style='font-size: 18pt; color: lightgray'>{oacnt/cnt*100:.1f}%</span>" \
            + f"<span style='font-size: 12pt'><br>{oacnt:,}/{cnt:,}</span>"
    html = cards + table_html
    endpoint_access()
    return make_response(render_template('custom.html', urlroot=request.url_root,
                                         title=ptitle, html=html, oamsg=oamsg,
                                         chartscript=chartscript, chartdiv=chartdiv,
                                         navbar=generate_navbar('DOIs')))


# Some DataCite subjectScheme values are inconsistent labels for the same
# vocabulary (e.g. "fos"/"FOS" and "FOR" are abbreviations). The input pipeline
# can't be changed, so map every known raw variant to one canonical label and
# merge them on read. Keys are the exact raw values stored in the dois collection.
SUBJECT_SCHEME_CANONICAL = {
    "fos": "Fields of Science and Technology (FOS)",
    "FOS": "Fields of Science and Technology (FOS)",
    "Fields of Science and Technology (FOS)": "Fields of Science and Technology (FOS)",
    "FOR": "ANZSRC Fields of Research",
    "ANZSRC Fields of Research": "ANZSRC Fields of Research",
}


def canonical_scheme(scheme):
    ''' Canonical display label for a subjectScheme, merging known variants
        Keyword arguments:
          scheme: raw subjectScheme value (may be None/empty)
        Returns:
          canonical label (unchanged value when there is no known variant)
    '''
    return SUBJECT_SCHEME_CANONICAL.get(scheme, scheme) if scheme else scheme


def scheme_variants(canonical):
    ''' Raw subjectScheme values that map to a canonical label, for querying
        Keyword arguments:
          canonical: canonical scheme label
        Returns:
          list of raw subjectScheme values (just the label itself when it has
          no known variants)
    '''
    variants = [raw for raw, canon in SUBJECT_SCHEME_CANONICAL.items()
                if canon == canonical]
    return variants or [canonical]


@app.route('/dois_subjectpicker')
def show_doi_subjectpicker():
    ''' Show DOI subjects
    '''
    cnt = {'crossref_mesh': 0}
    try:
        payload = [{"$match": {"subjects": {"$exists": True}}},
                   {"$unwind": "$subjects"},
                   {"$group": {"_id": {"subject": "$subjects.subject",
                                       "scheme": "$subjects.subjectScheme"},
                               "count": {"$sum": 1}}},
                   {"$sort": {"count": -1}}]
        rows = DB['dis'].dois.aggregate(payload)
    except Exception as err:
        return inspect_error(err, 'Could not get DataCite DOI subjects')
    if not rows:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not find subjects", 'warning'),
                               message="Could not find DataCite DOI subjects")
    subdict = {}
    scheme_cnt = collections.defaultdict(int)
    for row in rows:
        # Canonicalize known label variants; a missing/null scheme groups as
        # absent/None, so label it "DataCite unspecified"
        scheme = canonical_scheme(row['_id'].get('scheme')) or "DataCite unspecified"
        scheme_cnt[scheme] += 1
        if row['_id']['subject'] not in subdict:
            subdict[row['_id']['subject']] = [{"count": row['count'], "schema": scheme}]
        else:
            subdict[row['_id']['subject']].append({"count": row['count'], "schema": scheme})
    try:
        payload = [{"$match": {"jrc_mesh": {"$exists": 1}}},
                   {"$unwind": "$jrc_mesh"},
                   {"$group": {"_id": "$jrc_mesh.descriptor_name", "count": {"$sum": 1}}}]
        rows = DB['dis'].dois.aggregate(payload)
    except Exception as err:
        return inspect_error(err, 'Could not get MeSH DOI subjects')
    if not rows:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not find subjects", 'warning'),
                               message="Could not find MeSH DOI subjects")
    for row in rows:
        cnt['crossref_mesh'] += 1
        if row['_id'] not in subdict:
            subdict[row['_id']] = [{"count": row['count'], "schema": "MeSH"}]
        else:
            subdict[row['_id']].append({"count": row['count'], "schema": "MeSH"})
    options = []
    outlist = ""
    for subj, val in sorted(subdict.items()):
        outlist += f"{subj}\t"
        schlist = []
        for sch in val:
            schlist.append(f"{sch['schema']}: {sch['count']}")
        outlist += ", ".join(schlist) + "\n"
        # escape: some MeSH descriptors contain apostrophes/ampersands that would
        # otherwise break the option's value attribute or render incorrectly
        esc = escape(subj or "")
        options.append(f'<option value="{esc}">{esc}</option>')
    sublist = "".join(options)
    # Subjects grouped by source: the DataCite subjects[] schemes (like
    # /datacite_subject) and the separate Crossref MeSH field. A subject can
    # appear under more than one scheme, so the per-scheme tallies need not sum
    # to the unique-subject total. MeSH appears in both groups: as a DataCite
    # subjectScheme and as the Crossref jrc_mesh enrichment - hence the split.
    scheme_items = "".join(
        f"<li>{scheme}: {num:,}</li>"
        for scheme, num in sorted(scheme_cnt.items(), key=itemgetter(1), reverse=True))
    html = create_downloadable("subjects", ["Subject", "Schemes"], outlist) \
           + "<br><br>" \
           + "<span style='font-size:1.15em; font-weight:bold'>" \
           + f"Found {len(subdict):,} unique subjects</span><br><br>" \
           + "<b>Crossref subject schemes</b>" \
           + f"<ul class='unstyled'><li>MeSH: {cnt['crossref_mesh']:,}</li></ul>" \
           + "<b>DataCite subject schemes</b>" \
           + f"<ul class='unstyled'>{scheme_items}</ul>"
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
        rows = list(DB['dis'].dois.find(payload))
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get DOI subjects"),
                               message=error_message(err))
    title = f"DOIs with partial subject {subject}" if partial else f"DOIs for subject {subject}"
    if not rows:
        html = render_warning(f"No DOIs found for subject {subject}.", 'warning')
    else:
        # Standard DOI-list table + conventions; keep Source (registrar) - and, in
        # partial mode, the matched Subjects - as trailing extra columns.
        extra_headers = ['Source'] + (['Subjects'] if partial else [])
        def _subject_extra(row):
            vals = [row.get('jrc_obtained_from', 'DataCite')]
            if partial:
                vals.append(safe(add_subjects(row)))
            return vals
        table_html, _, _ = standard_doi_table(rows, count_card=True,
                                              extra_headers=extra_headers, extra_fn=_subject_extra)
        note = ""
        if partial and any(r.get('jrc_obtained_from') == 'Crossref' for r in rows):
            note = "<p><i class='fa-solid fa-circle-info'></i> Subjects in " \
                   + "<span style='color: #777'>dark gray</span> are considered minor in MeSH</p>"
        html = note + table_html
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         navbar=generate_navbar('DOIs')))


@app.route('/dois_recent/<string:source>/<int:limit>')
@app.route('/dois_recent/<string:source>')
@app.route('/dois_recent')
def dois_recent(source='Crossref', limit=10):
    ''' Show recent DOIs
    '''
    payload = [{"$match": {"jrc_obtained_from": source}},
               {"$sort": {"jrc_publishing_date": -1}},
               {"$limit": int(limit)}]
    try:
        rows = DB['dis'].dois.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get recent DOIs"),
                               message=error_message(err))
    html = source_limit_pulldown('dois_recent', source, limit) + "<br>"
    html2, _, _ = standard_doi_table(rows)
    html += html2
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=f"Recent {source} DOIs", html=html,
                                         navbar=generate_navbar('DOIs')))


# ******************************************************************************
# * UI endpoints (PubMed)                                                      *
# ******************************************************************************

@app.route('/pubmed/pmc/<string:pmcid>')
def show_doi_pmc(pmcid):
    '''
    Return a PubMed Central record
    Return the PubMed Central record for a PMC ID (as an OAI-PMH GetRecord). An unknown
    ID yields an empty data object with a 200 status.
    ---
    tags:
      - PubMed
    parameters:
      - in: path
        name: pmcid
        required: true
        schema:
          type: string
          example: PMC6039720
        description: PubMed Central ID
    responses:
      '200':
        description: PMC record (empty data object if not found)
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/PubmedRecordResult'
      '500':
        description: Error fetching the PMC record
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/Error'
    '''
    result = initialize_result()
    try:
        pmidjson = DL.get_doi_record(pmcid, source='pmc')
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    if pmidjson:
        result['rest']['row_count'] = 1
        result['rest']['source'] = 'pmc'
        if pmidjson.get('OAI-PMH', {}).get('GetRecord'):
            result['data'] = pmidjson.get('OAI-PMH', {})
        else:
            result['data'] = {}
    return generate_response(result)


@app.route('/pubmed/pubmed/<string:pmid>')
def show_doi_pubmed(pmid):
    '''
    Return a PubMed record
    Return the PubMed record for a PMID (as a PubmedArticleSet). An unknown ID yields
    an empty data object with a 200 status.
    ---
    tags:
      - PubMed
    parameters:
      - in: path
        name: pmid
        required: true
        schema:
          type: string
          example: '29622724'
        description: PubMed ID
    responses:
      '200':
        description: PubMed record (empty data object if not found)
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/PubmedRecordResult'
      '500':
        description: Error fetching the PubMed record
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/Error'
    '''
    result = initialize_result()
    try:
        pmidjson = DL.get_doi_record(pmid, source='pubmed')
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    if pmidjson:
        result['rest']['row_count'] = 1
        result['rest']['source'] = 'pubmed'
        result['data'] = pmidjson['PubmedArticleSet']
    return generate_response(result)


@app.route('/crossref_subject/<string:subject>/<string:year>')
@app.route('/crossref_subject/<string:subject>')
@app.route('/crossref_subject')
def crossref_subject(subject=None, year='All'):
    ''' Show Crossref MeSH DOI subjects
    '''
    if not subject:
        # List view: the year comes from a query param (the path year segment is
        # reserved for the subject-detail view)
        year = request.args.get('year', 'All')
    if subject:
        payload = {"jrc_mesh.descriptor_name": subject}
    else:
        # Crossref DOIs carry MeSH subjects in the jrc_mesh enrichment field;
        # MeSH is the only subject scheme present here
        match = {"jrc_obtained_from": "Crossref", "jrc_mesh": {"$exists": True}}
        if year != 'All':
            match["jrc_publishing_date"] = {"$regex": "^" + year}
        payload = [{"$match": match},
                   {"$unwind": "$jrc_mesh"},
                   {"$group": {"_id": "$jrc_mesh.descriptor_name", "count": {"$sum": 1}}},
                   {"$sort": {"count": -1}}]
    try:
        if subject:
            if year != 'All':
                payload['jrc_publishing_date'] = {"$regex": "^" + year}
            rows = DB['dis'].dois.find(payload)
        else:
            rows = DB['dis'].dois.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get DOI subjects"),
                               message=error_message(err))
    if subject:
        html, cnt, _ = standard_doi_table(rows, prefix=f"crossref_subject/{subject}",
                                          show_count=False)
        if not cnt:
            msg = f"No Crossref DOIs were found for subject {subject}"
            if year != 'All':
                msg += f" in publishing year {year}"
            html = year_pulldown(f"crossref_subject/{subject}") + "<br><br>" \
                   + render_warning(msg, 'warning')
        else:
            cards = stat_cards([("Subject", subject),
                                ("DOIs", f"<span id='totalrows'>{cnt:,}</span>")],
                               div_id='crsubj-stats')
            html = cards + html
        title = f"DOIs for {subject}"
        if year != 'All':
            title += f" ({year})"
    else:
        cnt = 0
        total = 0
        trows = []
        # Carry the active year into the subject drill-down links
        ysuffix = '' if year == 'All' else f"/{year}"
        for row in rows:
            cnt += 1
            total += row['count']
            subj = row['_id']
            trows.append([subj,
                          cell(safe(f"<a href='/crossref_subject/{subj}{ysuffix}'>"
                                    + f"{row['count']:,}</a>"), sort=row['count'])])
        pulldown = year_pulldown('crossref_subject', query=True)
        if not cnt:
            # No subjects for this filter - advise rather than show an empty table,
            # but keep the year pulldown so another year can be chosen
            msg = "No Crossref subjects were found"
            if year != 'All':
                msg += f" for publishing year {year}"
            html = pulldown + "<br><br>" + render_warning(msg, 'warning')
        else:
            table = render_table(['Subject', 'Count'], trows,
                                 table_id='subjects', css='tablesorter numberlast-scroll')
            cards = stat_cards([("DOI occurrences", f"{total:,}"),
                                ("Subjects", f"{cnt:,}"),
                                ("Scheme", "MeSH")],
                               div_id='crsubj-stats')
            html = cards + pulldown + "<br><br>" + table
        title = "Crossref subjects"
        if year != 'All':
            title += f" ({year})"
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
    if not subject:
        # List view: the year comes from a query param (the path year segment is
        # reserved for the subject-detail view)
        year = request.args.get('year', 'All')
    if subject:
        payload = {"subjects.subject": subject}
    else:
        match = {"subjects": {"$exists": True}}
        if year != 'All':
            match["jrc_publishing_date"] = {"$regex": "^" + year}
        payload = [{"$match": match},
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
        html, cnt, _ = standard_doi_table(rows, prefix=f"datacite_subject/{subject}",
                                          show_count=False)
        if not cnt:
            msg = f"No DataCite DOIs were found for subject {subject}"
            if year != 'All':
                msg += f" in publishing year {year}"
            html = year_pulldown(f"datacite_subject/{subject}") + "<br><br>" \
                   + render_warning(msg, 'warning')
        else:
            cards = stat_cards([("Subject", subject),
                                ("DOIs", f"<span id='totalrows'>{cnt:,}</span>")],
                               div_id='dcsubj-stats')
            html = cards + html
        title = f"DOIs for {subject}"
        if year != 'All':
            title += f" ({year})"
    else:
        cnt = 0
        total = 0
        schemes = set()
        trows = []
        # Carry the active year into the subject/scheme drill-down links
        ysuffix = '' if year == 'All' else f"/{year}"
        for row in rows:
            cnt += 1
            total += row['count']
            subj = row['_id']['subject']
            scheme = canonical_scheme(row['_id'].get('scheme', ''))
            if scheme:
                schemes.add(scheme)
                scheme_cell = safe(f"<a href='/datacite_scheme/{scheme}{ysuffix}'>{scheme}</a>")
            else:
                scheme_cell = scheme
            trows.append([subj, scheme_cell,
                          cell(safe(f"<a href='/datacite_subject/{subj}{ysuffix}'>"
                                    + f"{row['count']:,}</a>"), sort=row['count'])])
        pulldown = year_pulldown('datacite_subject', query=True)
        if not cnt:
            # No subjects for this filter - advise rather than show an empty table,
            # but keep the year pulldown so another year can be chosen
            msg = "No DataCite subjects were found"
            if year != 'All':
                msg += f" for publishing year {year}"
            html = pulldown + "<br><br>" + render_warning(msg, 'warning')
        else:
            table = render_table(['Subject', 'Subject scheme', 'Count'], trows,
                                 table_id='subjects', css='tablesorter numberlast-scroll')
            cards = stat_cards([("DOI occurrences", f"{total:,}"),
                                ("Subjects", f"{cnt:,}"),
                                ("Schemes", f"{len(schemes):,}")],
                               div_id='dcsubj-stats')
            html = cards + pulldown + "<br><br>" + table
        title = "DataCite subjects"
        if year != 'All':
            title += f" ({year})"
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         navbar=generate_navbar('DOIs')))


@app.route('/datacite_scheme/<string:scheme>/<string:year>')
@app.route('/datacite_scheme/<string:scheme>')
def datacite_scheme(scheme, year='All'):
    ''' Show DataCite DOIs whose subjects use a given subject scheme
    '''
    # scheme is the canonical label; match every raw variant that maps to it
    payload = {"subjects.subjectScheme": {"$in": scheme_variants(scheme)}}
    if year != 'All':
        payload['jrc_publishing_date'] = {"$regex": "^" + year}
    try:
        rows = DB['dis'].dois.find(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get DOI subjects"),
                               message=error_message(err))
    html, cnt, _ = standard_doi_table(rows, prefix=f"datacite_scheme/{scheme}",
                                      show_count=False)
    if not cnt:
        msg = f"No DataCite DOIs were found for subject scheme {scheme}"
        if year != 'All':
            msg += f" in publishing year {year}"
        html = year_pulldown(f"datacite_scheme/{scheme}") + "<br><br>" \
               + render_warning(msg, 'warning')
    else:
        cards = stat_cards([("Subject scheme", scheme),
                            ("DOIs", f"<span id='totalrows'>{cnt:,}</span>")],
                           div_id='dcscheme-stats')
        html = cards + html
    title = f"DataCite DOIs using subject scheme {scheme}"
    if year != 'All':
        title += f" ({year})"
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
    dois = {}
    for row in rows:
        if row['_id']['type'] not in types:
            types[row['_id']['type']] = 0
        types[row['_id']['type']] += row['count']
        if 'detail' not in row['_id']:
            row['_id']['detail'] = ""
        if row['_id']['type'] not in dois:
            dois[row['_id']['type']] = {}
        if row['_id']['detail'] not in dois[row['_id']['type']]:
            dois[row['_id']['type']][row['_id']['detail']] = {}
        if row['_id']['pub'] not in dois[row['_id']['type']][row['_id']['detail']]:
            dois[row['_id']['type']][row['_id']['detail']][row['_id']['pub']] = 0
        dois[row['_id']['type']][row['_id']['detail']][row['_id']['pub']] += row['count']
    # Summary
    trows = []
    for key, val in sorted(types.items(), key=itemgetter(1), reverse=True):
        link = f"/doisui_type/DataCite/{key}/None"
        trows.append([key, safe(f"<a href='{link}'>{val}</a>")])
    inner = render_table(['Type', 'Count'], trows, table_id='types',
                         css='tablesorter numberlast-scroll')
    html = f"<div class='flexrow'><div class='flexcol'>{inner}</div>" \
           + "<div class='flexcol' style='margin-left: 50px'>"
    # Details
    trows = []
    total = 0
    publishers = set()
    for typ, detail_dict in dois.items():
        for detail, pub_dict in detail_dict.items():
            for pub, cnt in pub_dict.items():
                total += cnt
                publishers.add(pub)
                link = f"/datacite_dois/{typ}/{detail}/{pub}"
                trows.append([typ, detail, pub, safe(f"<a href='{link}'>{cnt}</a>")])
    inner = render_table(['Type', 'Subtype', 'Publisher', 'Count'], trows, table_id='details',
                         css='tablesorter numberlast-scroll',
                         footer=[fcell('Total', colspan=3),
                                 fcell(f"{total:,}", align='center')])
    html += f"{inner}</div></div>"
    cards = stat_cards([("DataCite DOIs", f"{total:,}"),
                        ("Publishers", f"{len(publishers):,}"),
                        ("Resource types", f"{len(types):,}")], div_id='dcdois-stats')
    html = cards + html
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title="DataCite DOI metrics", html=html,
                                         navbar=generate_navbar('DataCite')))


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
    html, cnt, oacnt = standard_doi_table(rows, prefix=f"datacite_dois/{dtype}/{pub}",
                                          count_card=True)
    title = f"DOIs for {pub} {dtype} ({cnt:,})"
    if year != 'All':
        title += f" ({year})"
    chartscript, chartdiv = DP.wedge_chart({'shown': oacnt, 'total': cnt}) if oacnt else ['', '']
    if cnt:
        oamsg = f"<span style='font-size: 18pt; color: lightgray'>{oacnt/cnt*100:.1f}%</span>" \
                + f"<span style='font-size: 12pt'><br>{oacnt:,}/{cnt:,}</span>"
    else:
        oamsg = ""
        html += render_warning(f"No DOIs were found for {pub} {dtype}.", 'warning')
    endpoint_access()
    return make_response(render_template('custom.html', urlroot=request.url_root,
                                         title=title, html=html, oamsg=oamsg,
                                         chartscript=chartscript, chartdiv=chartdiv,
                                         navbar=generate_navbar('DOIs')))


@app.route('/datacite_citations')
def datacite_citations():
    ''' Legacy URL: redirect to /citation_list/datacite
    '''
    return redirect('/citation_list/datacite')


@app.route('/citation_list/<string:source>')
@app.route('/citation_list')
def citation_list(source='datacite'):
    ''' Show DOI citation counts for one registrar source
    '''
    source = source.lower()
    obtained_from = {"datacite": "DataCite", "crossref": "Crossref"}
    if source not in obtained_from:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Invalid source"),
                               message=f"{source} is not a valid source: use " \
                                       + " or ".join(sorted(obtained_from)))
    obtained = obtained_from[source]
    # Year comes from a query param (the path segment is reserved for the source)
    year = request.args.get('year', 'All')
    payload = {"jrc_obtained_from": obtained, "jrc_citation_count": {"$exists": 1}}
    if year != 'All':
        payload["jrc_publishing_date"] = {"$regex": "^" + year}
    coll = DB['dis'].dois
    try:
        rows = coll.find(payload).sort("jrc_citation_count", -1)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get data DOIs"),
                               message=error_message(err))
    trows = []
    row_classes = []
    fileoutput = ""
    total = total_ver = 0
    cnt = cnt_ver = 0
    for row in rows:
        n = row['jrc_citation_count']
        total += n
        cnt += 1
        is_ver = DL.is_version(row)
        if is_ver:
            total_ver += n
            cnt_ver += 1
        link = doi_link(row['doi'])
        raw_title = DL.get_title(row)
        title_text = strip_html_tags(raw_title)   # plain text for the TSV download
        trows.append([safe(link), render_title_html(raw_title), cell(f"{n:,}", sort=n)])
        row_classes.append('ver' if is_ver else '')
        fileoutput += f"{row['doi']}\t{title_text}\t{n}\n"
    pulldown = year_pulldown(f"citation_list/{source}", query=True)
    other = 'crossref' if source == 'datacite' else 'datacite'
    ysuffix = '' if year == 'All' else f"?year={year}"
    switch = registrar_switch(f"/citation_list/{other}{ysuffix}", obtained_from[other])
    title = f"{obtained} DOI citations"
    if year != 'All':
        title += f" ({year})"
    if not cnt:
        # No DOIs for this filter - advise but keep the year pulldown so another
        # year can be chosen
        msg = f"No {obtained} DOIs with citations were found"
        if year != 'All':
            msg += f" for publishing year {year}"
        html = switch + "<br>" + pulldown + "<br><br>" + render_warning(msg, 'warning')
        endpoint_access()
        return make_response(render_template('general.html', urlroot=request.url_root,
                                             title=title, html=html,
                                             navbar=generate_navbar('DOIs')))
    cnt_nv = cnt - cnt_ver
    total_nv = total - total_ver
    avg_all = f"{total/cnt:,.1f}" if cnt else "0"
    avg_nv = f"{total_nv/cnt_nv:,.1f}" if cnt_nv else "0"
    download = create_downloadable(f'citation_list_{source}',
                                   ['DOI', 'Title', 'Citations'], fileoutput)
    html = render_table(['DOI', 'Title', 'Citations'], trows, table_id='data',
                        css='tablesorter numberlast-scroll', row_classes=row_classes,
                        footer=[fcell('Total', colspan=2),
                                fcell(f"{total:,}", align='center')]) + "<br>"
    # data-* attrs store both states so the onclick can swap without a round-trip.
    # data-filtered tracks current state (0=all shown, 1=versioned filtered).
    cbutton = (
        "<button id='verbtn' class=\"btn btn-outline-warning\" style='margin-left: 10px' "
        + f"data-total-all=\"{total:,}\" data-avg-all=\"{avg_all}\" "
        + f"data-total-nv=\"{total_nv:,}\" data-avg-nv=\"{avg_nv}\" "
        + "data-filtered=\"0\" "
        + "onclick=\"toggler('data', 'ver', 'totalrows'); "
        + "var f=this.dataset.filtered==='1'; "
        + "this.dataset.filtered=f?'0':'1'; "
        + "document.getElementById('cite-total').textContent=f?this.dataset.totalAll:this.dataset.totalNv; "
        + "document.getElementById('cite-avg').textContent=f?this.dataset.avgAll:this.dataset.avgNv;\">"
        + "Filter versioned DOIs</button>"
    )
    cards = stat_cards([("DOIs with citations", f"<span id='totalrows'>{cnt:,}</span>"),
                        ("Total citations", f"<span id='cite-total'>{total:,}</span>"),
                        ("Avg. per DOI", f"<span id='cite-avg'>{avg_all}</span>")],
                       div_id='dccc-stats')
    html = switch + "<br>" + cards + pulldown + cbutton + download + "<br><br>" + html
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
    trows = []
    total = 0
    cnt = 0
    for row in rows:
        total += row['downloadCount']
        cnt += 1
        link = doi_link(row['doi'])
        trows.append([safe(link), render_title_html(DL.get_title(row)),
                      cell(f"{row['downloadCount']:,}", sort=row['downloadCount'])])
    html = render_table(['DOI', 'Title', 'Downloads'], trows, table_id='data',
                        css='tablesorter numberlast-scroll',
                        footer=[fcell('Total', colspan=2),
                                fcell(f"{total:,}", align='center')]) + "<br>"
    cards = stat_cards([("DOIs with downloads", f"{cnt:,}"),
                        ("Total downloads", f"{total:,}"),
                        ("Avg. per DOI", f"{total/cnt:,.1f}" if cnt else "0")],
                       div_id='dcdl-stats')
    html = cards + html
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title="DataCite DOI downloads", html=html,
                                         navbar=generate_navbar('DataCite')))


@app.route('/datacite_metrics')
def datacite_metrics():
    ''' Legacy URL: redirect to /citation_metrics/datacite
    '''
    return redirect('/citation_metrics/datacite')


@app.route('/citation_metrics/<string:source>')
@app.route('/citation_metrics')
def citation_metrics(source='datacite'):
    ''' Show citation totals by source
    '''
    source = source.lower()
    obtained_from = {"datacite": "DataCite", "crossref": "Crossref"}
    if source not in obtained_from:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Invalid source"),
                               message=f"{source} is not a valid source: use " \
                                       + " or ".join(sorted(obtained_from)))
    obtained = obtained_from[source]
    coll = DB['dis'].dois
    source_name = {"datacite": "DataCite", "openalex": "OpenAlex",
                   "scholexplorer": "ScholeXplorer", "crossref": "Crossref",
                   "wos": "Web of Science", "dimensions": "Dimensions"}
    # Citations by source
    payload = [{"$match": {"jrc_citation_sources": {"$exists": True},
                           "jrc_obtained_from": obtained}},
               {"$project": {"kv": {"$objectToArray": "$jrc_citation_sources"}}},
               {"$unwind": "$kv"},
               {"$group": {"_id": "$kv.k", "dois": {"$sum": 1}, "total": {"$sum": "$kv.v"}}},
               {"$sort": {"total": -1}}]
    try:
        rows = list(coll.aggregate(payload))
        # Per-DOI records drive the unique total, median, freshness, version
        # exclusion, and per-year breakdown (doi/relation are for DL.is_version)
        cited_rows = list(coll.find({"jrc_citation_sources": {"$exists": True},
                                     "jrc_obtained_from": obtained},
                                    {"doi": 1, "relation": 1,
                                     "jrc_citation_count": 1, "jrc_citation_sources": 1,
                                     "jrc_citation_updated": 1, "jrc_publishing_date": 1,
                                     "types": 1}))
        all_dois = coll.count_documents({"jrc_obtained_from": obtained})
        year_all = {rec['_id']: rec['dois'] for rec in coll.aggregate(
            [{"$match": {"jrc_obtained_from": obtained,
                         "jrc_publishing_date": {"$exists": True}}},
             {"$group": {"_id": {"$substrBytes": ["$jrc_publishing_date", 0, 4]},
                         "dois": {"$sum": 1}}}])}
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get citation sources " \
                                                    + "from dois collection"),
                               message=error_message(err))
    cite_dois = len(cited_rows)
    counts = [row.get('jrc_citation_count', 0) for row in cited_rows]
    unique_total = sum(counts)
    updated = [row['jrc_citation_updated'] for row in cited_rows
               if row.get('jrc_citation_updated')]
    # Version-deduped view: dataset .v1/.v2/... are separate dois records counted
    # separately upstream, so per-work impact metrics and the most-cited list use
    # only non-versioned records to avoid counting one work several times.
    nonver = [row for row in cited_rows if not DL.is_version(row)]
    nonver_counts = sorted((row.get('jrc_citation_count', 0) or 0 for row in nonver),
                           reverse=True)
    nonver_counts = [c for c in nonver_counts if c > 0]
    # h-index: largest h such that h works are each cited >= h times. i10/i100:
    # works cited at least 10/100 times (classic scholarly-impact measures).
    h_index = 0
    for idx, cnt in enumerate(nonver_counts, start=1):
        if cnt >= idx:
            h_index = idx
        else:
            break
    i10_index = sum(1 for c in nonver_counts if c >= 10)
    i100_index = sum(1 for c in nonver_counts if c >= 100)
    # Which single source best approximates the unique total? jrc_citation_count
    # is the deduplicated union of citing DOIs (OpenAlex + ScholeXplorer, floored
    # by the registrar's bare count and WoS), so a lone source usually undercounts
    # it. For each cited DOI find the source whose standalone count is nearest the
    # unique total (ties split the credit evenly), and accumulate the aggregate
    # error and coverage that make a source the best single-source proxy.
    close_stats = {}
    close_analyzed = 0
    # Union lift: how often (and how much) the deduped total beats the single best
    # source - the payoff of combining sources rather than trusting the strongest.
    union_exceed = 0
    best_single_sum = 0
    for row in cited_rows:
        cnt = row.get('jrc_citation_count', 0) or 0
        pairs = [(source_name.get(key, key.capitalize()), val)
                 for key, val in (row.get('jrc_citation_sources') or {}).items()
                 if isinstance(val, int)]
        if cnt <= 0 or not pairs:
            continue
        close_analyzed += 1
        best_single = max(val for _, val in pairs)
        best_single_sum += best_single
        if cnt > best_single:
            union_exceed += 1
        best = min(abs(val - cnt) for _, val in pairs)
        winners = [lbl for lbl, val in pairs if abs(val - cnt) == best]
        share = 1.0 / len(winners)
        for lbl, val in pairs:
            stat = close_stats.setdefault(lbl, {'present': 0, 'wins': 0.0,
                                                'abs_err': 0, 'src_sum': 0,
                                                'true_sum': 0, 'exact': 0})
            stat['present'] += 1
            stat['abs_err'] += abs(val - cnt)
            stat['src_sum'] += val
            stat['true_sum'] += cnt
            stat['exact'] += 1 if val == cnt else 0
        for lbl in winners:
            close_stats[lbl]['wins'] += share
    close_winner = max(close_stats, key=lambda lbl: close_stats[lbl]['wins']) \
        if close_stats else None
    cite_data = {}
    trows = []
    cite_total = 0
    for row in rows:
        label = source_name.get(row['_id'], row['_id'].capitalize())
        cite_data[label] = row['total']
        cite_total += row['total']
        trows.append([label, cell(f"{row['dois']:,}", sort=row['dois']),
                      cell(f"{row['total']:,}", sort=row['total'])])
    card_list = [("DOIs with citation sources",
                  f"<a href='/citation_list/{source}'>{cite_dois:,}</a>"),
                 ("% DOIs cited", f"{cite_dois/all_dois*100:,.1f}%" if all_dois else "0%"),
                 ("Total citations", f"{cite_total:,}"),
                 ("Total unique citations", f"{unique_total:,}"),
                 ("Avg. per DOI", f"{unique_total/cite_dois:,.1f}" if cite_dois else "0"),
                 ("Median per DOI",
                  f"{statistics.median(counts):,.1f}" if counts else "0"),
                 ("h-index / i10-index", f"{h_index:,} / {i10_index:,}")]
    if close_winner:
        win_pct = close_stats[close_winner]['wins'] / close_analyzed * 100
        card_list.append(("Closest source to unique total",
                          f"{close_winner} <span style='font-size:0.62em; " \
                          + f"font-weight:normal; color:#a8c4e0;'>({win_pct:.0f}%)</span>"))
    cards = stat_cards(card_list, div_id='dcm-cite-stats')
    note_style = "font-size:0.85em; color:#a8c4e0;"
    notes = ''
    if updated:
        newest, oldest = max(updated), min(updated)
        line = f"Citation data last updated {newest:%Y-%m-%d}"
        if oldest.date() != newest.date():
            line += f" (oldest record: {oldest:%Y-%m-%d})"
        notes += f"<div style='{note_style}'>{line}</div>"
    if h_index or i10_index:
        notes += f"<div style='{note_style}'><b>h-index</b> = the largest h such that " \
                 + "h works are each cited at least h times; <b>i10-index</b> = the number " \
                 + "of works cited at least 10 times (versioned DOIs excluded).</div>"
    # Versioned DOIs (.v1/.v2/...) are separate dois records, so a citing work
    # can be counted once per version it cites; show totals without versions
    # (nonver was built above and drives the impact metrics too). Applies to both
    # registrars (Crossref has versioned preprints too).
    if 0 < len(nonver) < cite_dois:
        nv_total = sum(row.get('jrc_citation_count', 0) for row in nonver)
        notes += f"<div style='{note_style}'>Versioned DOIs are counted " \
                 + f"separately; excluding the {cite_dois - len(nonver):,} versioned " \
                 + f"DOIs leaves {len(nonver):,} cited DOIs with {nv_total:,} unique " \
                 + f"citations (avg. {nv_total/len(nonver):,.1f})</div>"
    if notes:
        notes = f"<div style='margin:-8px 0 16px 0'>{notes}</div>"
    other = 'crossref' if source == 'datacite' else 'datacite'
    switch = registrar_switch(f"/citation_metrics/{other}", obtained_from[other])
    chtml = "<h4>Citations by source</h4>"
    chtml += render_table(['Source', 'DOIs', 'Citations'], trows, table_id='sources',
                          css='tablesorter numbers-scroll',
                          footer=[fcell('Total', colspan=2),
                                  fcell(f"{cite_total:,}", align='center')])
    chtml += f"<div style='{note_style} max-width:460px'>A citing work found by more " \
             + "than one source counts once per source here; &quot;Total unique " \
             + "citations&quot; is the deduplicated figure.</div><br>"
    # Closest source to the unique total: per source, how often its standalone
    # count is nearest jrc_citation_count (win share), plus the aggregate coverage
    # and mean absolute distance that quantify it as a single-source proxy.
    close_data = {}
    xtrows = []
    for lbl in sorted(close_stats, key=lambda k: close_stats[k]['wins'], reverse=True):
        stat = close_stats[lbl]
        mae = stat['abs_err'] / stat['present'] if stat['present'] else 0
        cover = stat['src_sum'] / stat['true_sum'] * 100 if stat['true_sum'] else 0
        win_pct = stat['wins'] / close_analyzed * 100 if close_analyzed else 0
        close_data[lbl] = stat['wins']
        xtrows.append([lbl, cell(f"{stat['present']:,}", sort=stat['present']),
                       cell(f"{stat['wins']:,.0f}", sort=stat['wins']),
                       cell(f"{win_pct:.1f}%", sort=win_pct),
                       cell(f"{cover:.1f}%", sort=cover),
                       cell(f"{mae:,.1f}", sort=mae),
                       cell(f"{stat['exact']:,}", sort=stat['exact'])])
    xhtml = ''
    if xtrows:
        xhtml = "<h4>Closest source to the unique total</h4>"
        xhtml += render_table(['Source', 'DOIs', 'Closest', 'Win %', 'Coverage',
                               'Avg. distance', 'Exact'], xtrows, table_id='closest',
                              css='tablesorter numbers-scroll',
                              data_attrs={"sortlist": "[[2,1]]"})
        xhtml += f"<div style='{note_style} max-width:460px'>&quot;Closest&quot; " \
                 + "counts the cited DOIs where a source's standalone count is " \
                 + "nearest the unique total (ties split evenly, so it sums to the " \
                 + f"{close_analyzed:,} DOIs compared); &quot;Coverage&quot; is the " \
                 + "source's summed count as a share of that total, and &quot;Avg. " \
                 + "distance&quot; is its mean gap from it. &quot;Exact&quot; is DOIs " \
                 + "where the source equals the total.</div>"
        # Union lift: the concrete payoff of deduping across sources.
        union_lift_pct = (unique_total - best_single_sum) / best_single_sum * 100 \
            if best_single_sum else 0
        xhtml += f"<div style='{note_style} max-width:460px; margin-top:6px'>" \
                 + "<b>Union lift:</b> the deduped union exceeds the best single source " \
                 + f"on <b>{union_exceed:,}</b> of {close_analyzed:,} cited DOIs " \
                 + f"(<b>{union_exceed/close_analyzed*100:.1f}%</b>), a " \
                 + f"<b>{union_lift_pct:+.1f}%</b> gain over trusting the strongest " \
                 + "source alone.</div><br>"
    # Citations by resource type (DataCite only; Crossref records carry no
    # types.resourceTypeGeneral). Citations and cited DOIs per resource type.
    rhtml = ''
    rtype_data = {}
    if source == 'datacite':
        rtype = {}
        for row in cited_rows:
            cnt = row.get('jrc_citation_count', 0) or 0
            if cnt <= 0:
                continue
            name = (row.get('types') or {}).get('resourceTypeGeneral') or 'Unknown'
            rec = rtype.setdefault(name, {'dois': 0, 'citations': 0})
            rec['dois'] += 1
            rec['citations'] += cnt
        if rtype:
            rtrows = []
            for name, rec in sorted(rtype.items(), key=lambda kv: -kv[1]['citations']):
                rtype_data[name] = rec['citations']
                rtrows.append([name, cell(f"{rec['dois']:,}", sort=rec['dois']),
                               cell(f"{rec['citations']:,}", sort=rec['citations'])])
            rhtml = "<h4>Citations by resource type</h4>"
            rhtml += render_table(['Resource type', 'Cited DOIs', 'Citations'], rtrows,
                                  table_id='restype', css='tablesorter numbers-scroll',
                                  data_attrs={"sortlist": "[[2,1]]"})
    # Citations by publishing year
    year_cited = {}
    for row in cited_rows:
        year = (row.get('jrc_publishing_date') or '')[:4]
        if year.isdigit():
            rec = year_cited.setdefault(year, {'dois': 0, 'citations': 0})
            rec['dois'] += 1
            rec['citations'] += row.get('jrc_citation_count', 0)
    by_year = {}
    ytrows = []
    ydata = {'Year': [], 'Citations': [], 'Cited': []}
    for year in sorted(yr for yr in set(year_all) | set(year_cited) if yr.isdigit()):
        alln = year_all.get(year, 0)
        cited = year_cited.get(year, {'dois': 0, 'citations': 0})
        pct = cited['dois'] / alln if alln else 0
        by_year[year] = {'dois': alln, 'cited_dois': cited['dois'],
                         'unique_citations': cited['citations']}
        ytrows.append([year, cell(f"{alln:,}", sort=alln),
                       cell(f"{cited['dois']:,}", sort=cited['dois']),
                       cell(f"{pct*100:.1f}%", sort=pct),
                       cell(f"{cited['citations']:,}", sort=cited['citations'])])
        ydata['Year'].append(year)
        ydata['Citations'].append(cited['citations'])
        ydata['Cited'].append(pct)
    yhtml = "<h4>Citations by publishing year</h4>"
    # Table shows years newest-first; the chart keeps ascending (chronological) order
    yhtml += render_table(['Year', 'DOIs', 'Cited DOIs', '% cited', 'Unique citations'],
                          ytrows[::-1], table_id='years', css='tablesorter numbers-scroll')
    if request.args.get('fmt') == 'json':
        result = initialize_result()
        result['data'] = {"source": obtained,
                          "dois": all_dois,
                          "cited_dois": cite_dois,
                          "pct_cited": round(cite_dois/all_dois*100, 2) if all_dois else 0,
                          "citations_by_source": cite_data,
                          "total_citations": cite_total,
                          "unique_citations": unique_total,
                          "avg_per_doi": round(unique_total/cite_dois, 2) if cite_dois else 0,
                          "median_per_doi": statistics.median(counts) if counts else 0,
                          "last_updated": max(updated).isoformat() if updated else None,
                          "h_index": h_index,
                          "i10_index": i10_index,
                          "i100_index": i100_index,
                          "union_lift": {
                              "dois_compared": close_analyzed,
                              "union_exceeds_best": union_exceed,
                              "union_exceeds_best_pct": round(
                                  union_exceed / close_analyzed * 100, 2) if close_analyzed else 0,
                              "best_single_total": best_single_sum,
                              "lift_pct": round((unique_total - best_single_sum)
                                                / best_single_sum * 100, 2) if best_single_sum else 0},
                          "citations_by_type": rtype_data,
                          "closest_source": {
                              "winner": close_winner,
                              "dois_compared": close_analyzed,
                              "by_source": {lbl: {
                                  "present": st['present'],
                                  "closest": round(st['wins'], 2),
                                  "win_pct": round(st['wins'] / close_analyzed * 100, 2)
                                               if close_analyzed else 0,
                                  "coverage_pct": round(st['src_sum'] / st['true_sum'] * 100, 2)
                                                    if st['true_sum'] else 0,
                                  "avg_distance": round(st['abs_err'] / st['present'], 2)
                                                    if st['present'] else 0,
                                  "exact": st['exact']}
                                            for lbl, st in close_stats.items()}},
                          "by_year": by_year}
        result['rest']['source'] = 'mongo'
        # data is a single stats object, not a row list; report the number of
        # cited DOIs the stats summarize rather than the dict's key count
        result['rest']['row_count'] = cite_dois
        endpoint_access()
        return generate_response(result)
    chartscript = cite_div = year_div = close_div = rtype_div = ''
    if cite_data:
        colors = DP.get_colors_by_count(len(cite_data))
        script, cite_div = DP.pie_chart(cite_data, "Citations by source", "source",
                                        width=500, colors=colors)
        chartscript += script
    if close_data:
        script, close_div = DP.hbar_chart(close_data, "Nearest to the unique total",
                                          value_label="Closest on", value_format="0,0",
                                          width=500, height=300, show_values=True)
        chartscript += script
    if rtype_data:
        script, rtype_div = DP.hbar_chart(rtype_data, "Citations by resource type",
                                          value_label="Citations", value_format="0,0",
                                          width=500, height=300, show_values=True)
        chartscript += script
    if ydata['Year']:
        # Tap a year bar -> the DOIs published that year for this registrar
        ynav = {yr: {"field": "publishing_year", "value": yr, "source": obtained}
                for yr in ydata['Year']}
        script, year_div = DP.dual_axis_chart(ydata, title="Citations by publishing year",
                                              x_field='Year', bar_field='Citations',
                                              line_field='Cited', line_label='% cited',
                                              bar_format="0,0", line_format="0%",
                                              width=650, height=400, nav=ynav)
        chartscript += script
    # Cards (with freshness/version notes) on their own full-width rows, then
    # each table paired with its chart in a flex row so the chart lines up
    # with the table, not the cards
    html = switch + cards + notes \
           + "<div class='flexrow' style='margin-bottom: 40px'><div class='flexcol'>" + chtml \
           + "</div>" \
           + "<div class='flexcol' style='margin: 10px 0 0 20px'>" + cite_div + "</div></div>"
    if xhtml:
        html += "<div class='flexrow' style='margin-bottom: 40px'><div class='flexcol'>" + xhtml \
                + "</div>" \
                + "<div class='flexcol' style='margin: 10px 0 0 20px'>" + close_div + "</div></div>"
    if rhtml:
        html += "<div class='flexrow' style='margin-bottom: 40px'><div class='flexcol'>" + rhtml \
                + "</div>" \
                + "<div class='flexcol' style='margin: 10px 0 0 20px'>" + rtype_div + "</div></div>"
    html += "<div class='flexrow' style='margin-bottom: 40px'><div class='flexcol'>" + yhtml \
            + "</div>" \
            + "<div class='flexcol' style='margin: 10px 0 0 20px'>" + year_div + "</div></div>"
    title = f"{obtained} citation metrics"
    endpoint_access()
    return make_response(render_template('bokeh.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         chartscript=chartscript, chartdiv='',
                                         chartscript2='', chartdiv2='',
                                         navbar=generate_navbar('DOIs')))


# Per-publisher usage fields aggregated by /source_metrics. Common subset
# reported: views (present on all four) + downloads (figshare/Zenodo/eLife;
# protocols.io has none, so it contributes 0 to downloads).
SOURCE_METRIC_USAGE_FIELDS = ("jrc_figshare_counts", "jrc_zenodo_counts",
                              "jrc_protocolsio_counts", "jrc_elife_counts")


def citation_impact_indices(counts):
    ''' Compute h-, g-, i10- and i100-index from a list of per-work citation
        counts (classic scholarly-impact measures). Callers pass version-deduped
        counts so one work isn't counted several times. h = largest h such that
        h works are each cited >= h times; g = largest g such that the top g
        works have >= g^2 citations between them (Egghe; keeps crediting the
        most-cited works beyond the h-core, so g >= h); i10/i100 = works cited
        >= 10/100 times.
        Keyword arguments:
          counts: iterable of per-work citation counts
        Returns:
          dict with keys h_index, g_index, i10, i100
    '''
    ordered = sorted((c for c in counts if c and c > 0), reverse=True)
    h_index = 0
    for idx, cnt in enumerate(ordered, start=1):
        if cnt >= idx:
            h_index = idx
        else:
            break
    # g-index: largest g where cumulative citations of the top g works >= g^2.
    # Capped at the actual work count (no fictitious zero-citation padding).
    g_index = 0
    cumulative = 0
    for idx, cnt in enumerate(ordered, start=1):
        cumulative += cnt
        if cumulative >= idx * idx:
            g_index = idx
        else:
            break
    return {"h_index": h_index, "g_index": g_index,
            "i10": sum(1 for c in ordered if c >= 10),
            "i100": sum(1 for c in ordered if c >= 100)}


def citation_distribution_prose(mean, median, pct_ge_100):
    ''' Describe a citation distribution's skew and depth in words that track
        the actual numbers, so the prose stays honest if the data shifts (e.g.
        the heavy-tailed whole corpus vs. a sparse single year where few works
        have accrued many citations). Returns (skew, depth) fragments; `depth`
        carries the sentence's terminal punctuation - the "broad depth" clause
        (leading comma) when the >=100-citation share justifies it, else just
        "." (rather than asserting broad depth regardless, as static text would).
        Keyword arguments:
          mean: mean citations per work
          median: median citations per work
          pct_ge_100: percent of works cited at least 100 times
        Returns:
          (skew_phrase, depth_clause) tuple of strings
    '''
    ratio = mean / median if median else 0
    if ratio >= 3:
        skew = "a strongly right-skewed, heavy-tailed distribution"
    elif ratio >= 1.5:
        skew = "a right-skewed distribution"
    else:
        skew = "a fairly even distribution"
    if pct_ge_100 >= 10:
        depth = ", so the top-end concentration coexists with broad depth."
    else:
        depth = "."
    return skew, depth


@app.route('/source_metrics/<string:year>')
@app.route('/source_metrics')
def source_metrics(year='All'):  # pylint: disable=too-many-locals
    ''' Combined citation + usage metrics for all Janelia DOIs, split by
        registrar source (Crossref vs DataCite) and deliberately NOT scoped to
        any single publisher (eLife/Zenodo/figshare/protocols.io) - fills the
        gap the per-publisher *_stats endpoints leave. dois collection only:
        usage data exists only there, and this parallels citation_metrics'
        scope. Citations are uniform (jrc_citation_count); usage rolls up the
        common views+downloads subset across the per-publisher count fields
        (Crossref usage = eLife + protocols.io; DataCite = figshare + Zenodo).
    '''
    coll = DB['dis'].dois
    # Shared filter merged into every population query (cited / all / usage) so
    # the report is scoped uniformly. Holds the optional publishing-year regex
    # and an optional single jrc_tag.name restriction from the ?tag= pulldown.
    ymatch = {}
    if year != 'All':
        ymatch["jrc_publishing_date"] = {"$regex": "^" + year}
    tag = request.args.get('tag') or None
    if tag:
        ymatch["jrc_tag.name"] = tag
    cite = {}   # source -> citation metrics
    use = {}    # source -> usage metrics
    # h5-index = h-index over works from the last 5 COMPLETE calendar years (a
    # recency lens, less dominated by old landmark papers - the measure Google
    # Scholar Metrics uses). Only meaningful for the all-years view; a
    # year-filtered report is already single-year, so h5 is omitted there.
    h5_range = None
    if year == 'All':
        cur_year = datetime.now().year
        h5_range = (cur_year - 5, cur_year - 1)
    # Pooled per-work data for the Combined column: median and the impact
    # indices are NOT additive, so Combined is computed over the pooled set,
    # not by summing the per-source columns.
    pooled_counts = []          # every cited DOI's count (combined total/median)
    pooled_nonver_works = []    # (count, pub_year) per non-versioned work
    try:
        for obtained in ("Crossref", "DataCite"):
            # Cited universe matches citation_metrics: DOIs carrying citation
            # sources. jrc_citation_count is the deduplicated union total.
            cmatch = {"jrc_obtained_from": obtained,
                      "jrc_citation_sources": {"$exists": True}}
            cmatch.update(ymatch)
            rows = list(coll.find(cmatch, {"doi": 1, "relation": 1,
                                           "jrc_citation_count": 1,
                                           "jrc_publishing_date": 1}))
            counts = [r.get('jrc_citation_count', 0) or 0 for r in rows]
            # Indices use non-versioned works only (a .v1/.v2 series is one work);
            # DL.is_version needs doi + relation, both projected above. Keep each
            # work's publishing year alongside its count for the h5 window.
            nonver_works = []
            for row in rows:
                if DL.is_version(row):
                    continue
                yname = (row.get('jrc_publishing_date') or '')[:4]
                nonver_works.append((row.get('jrc_citation_count', 0) or 0,
                                     int(yname) if yname.isdigit() else None))
            nonver = [c for c, _ in nonver_works]
            amatch = {"jrc_obtained_from": obtained}
            amatch.update(ymatch)
            all_dois = coll.count_documents(amatch)
            cite[obtained] = {"dois": len(counts), "all": all_dois,
                              "reach": len(counts) / all_dois * 100 if all_dois else 0,
                              "total": sum(counts),
                              "median": statistics.median(counts) if counts else 0,
                              **citation_impact_indices(nonver)}
            if h5_range:
                h5c = [c for c, y in nonver_works if y and h5_range[0] <= y <= h5_range[1]]
                cite[obtained]['h5'] = citation_impact_indices(h5c)['h_index']
            pooled_counts.extend(counts)
            pooled_nonver_works.extend(nonver_works)
            # Usage: sum views+downloads across whichever per-publisher count
            # field each DOI carries (protocols.io has no downloads -> adds 0).
            umatch = {"jrc_obtained_from": obtained,
                      "$or": [{f: {"$exists": True}} for f in SOURCE_METRIC_USAGE_FIELDS]}
            umatch.update(ymatch)
            uviews = udowns = udois = 0
            for doc in coll.find(umatch, {f: 1 for f in SOURCE_METRIC_USAGE_FIELDS}):
                udois += 1
                for field in SOURCE_METRIC_USAGE_FIELDS:
                    cnts = doc.get(field) or {}
                    uviews += cnts.get('views', 0)
                    udowns += cnts.get('downloads', 0)
            use[obtained] = {"dois": udois, "views": uviews, "downloads": udowns,
                             "dpv": udowns / uviews * 100 if uviews else 0,
                             "vpd": uviews / udois if udois else 0,
                             "dpd": udowns / udois if udois else 0}
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get source metrics " \
                                                    + "from dois collection"),
                               message=error_message(err))
    # Combined column (pooled, not per-source sums - see pooled_* above)
    pooled_nonver = [c for c, _ in pooled_nonver_works]
    c_all = cite['Crossref']['all'] + cite['DataCite']['all']
    c_dois = cite['Crossref']['dois'] + cite['DataCite']['dois']
    cite['Combined'] = {"dois": c_dois, "all": c_all,
                        "reach": c_dois / c_all * 100 if c_all else 0,
                        "total": sum(pooled_counts),
                        "median": statistics.median(pooled_counts) if pooled_counts else 0,
                        **citation_impact_indices(pooled_nonver)}
    if h5_range:
        h5c = [c for c, y in pooled_nonver_works if y and h5_range[0] <= y <= h5_range[1]]
        cite['Combined']['h5'] = citation_impact_indices(h5c)['h_index']
    u_views = use['Crossref']['views'] + use['DataCite']['views']
    u_downs = use['Crossref']['downloads'] + use['DataCite']['downloads']
    u_dois = use['Crossref']['dois'] + use['DataCite']['dois']
    use['Combined'] = {"dois": u_dois, "views": u_views, "downloads": u_downs,
                       "dpv": u_downs / u_views * 100 if u_views else 0,
                       "vpd": u_views / u_dois if u_dois else 0,
                       "dpd": u_downs / u_dois if u_dois else 0}
    if request.args.get('fmt') == 'json':
        result = initialize_result()
        result['data'] = {"citations": cite, "usage": use}
        result['rest']['source'] = 'mongo'
        result['rest']['year'] = year
        result['rest']['tag'] = tag
        result['rest']['row_count'] = cite['Combined']['dois'] + use['Combined']['dois']
        return generate_response(result)
    # ----- headline stat cards, grouped (h-index is the marquee combined impact
    # number). The Citations/Usage labels make the two-row split intentional -
    # they're two different populations, not one dashboard of six numbers. -----
    grp_label = ("font-size:0.8em; color:#a8c4e0; text-transform:uppercase; "
                 "letter-spacing:0.05em; font-weight:bold; margin-bottom:5px;")
    cards = f"<div style='{grp_label}'>Citations</div>" \
            + stat_cards([("DOIs cited", f"{cite['Combined']['dois']:,}"),
                          ("Total citations", f"{cite['Combined']['total']:,}"),
                          ("h-index", f"{cite['Combined']['h_index']:,}")],
                         div_id='src-cite-stats')
    cards += f"<div style='{grp_label}'>Usage</div>" \
             + stat_cards([("DOIs with usage", f"{use['Combined']['dois']:,}"),
                           ("Total views", f"{use['Combined']['views']:,}"),
                           ("Total downloads", f"{use['Combined']['downloads']:,}")],
                          div_id='src-use-stats')
    intro = "<div style='font-size:0.9em; color:#a8c4e0; max-width:640px; " \
            + "margin:-8px 0 16px 0'>All Janelia DOIs, split by registration " \
            + "source (Crossref vs. DataCite) regardless of publisher. Citations " \
            + "are the deduplicated union count; usage is the common views + " \
            + "downloads subset summed across figshare/Zenodo (DataCite) and " \
            + "eLife/protocols.io (Crossref). Impact indices (h/i10/i100) count " \
            + "each work once (versions merged). The citation and usage " \
            + "populations are largely different DOIs, so each has its own " \
            + "denominator.</div>"

    def mrow(label, data, key, kind='int'):
        ''' One 3-column (Combined/Crossref/DataCite) metric row, formatted. '''
        cells = [label]
        for col in ('Combined', 'Crossref', 'DataCite'):
            val = data[col][key]
            if kind == 'pct':
                cells.append(f"{val:,.1f}%")
            elif kind == 'median':
                cells.append(f"{val:,.0f}" if float(val).is_integer() else f"{val:,.1f}")
            elif kind == 'round':
                cells.append(f"{val:,.0f}")
            else:
                cells.append(f"{val:,}")
        return cells
    # ----- citations by source -----
    crows = [mrow("DOIs cited", cite, 'dois'),
             mrow("Reach (% of DOIs)", cite, 'reach', 'pct'),
             mrow("Citations", cite, 'total'),
             mrow("Median / DOI", cite, 'median', 'median'),
             mrow("h-index", cite, 'h_index'),
             mrow("g-index", cite, 'g_index')]
    if h5_range:
        crows.append(mrow(f"h5-index ({h5_range[0]}–{h5_range[1]})", cite, 'h5'))
    crows.append(mrow("i10-index", cite, 'i10'))
    crows.append(mrow("i100-index", cite, 'i100'))
    chtml = "<h4>Citations by source</h4>"
    chtml += render_table(['Metric', 'Combined', 'Crossref', 'DataCite'], crows,
                          table_id='src-cite', css='tablesorter numbers-scroll')
    h5_note = ("<b>h5-index</b>: the h-index over just the last 5 complete years - "
               "a recency lens, less dominated by older landmark works. "
               if h5_range else "")
    chtml += "<div style='font-size:0.9em; color:#a8c4e0; max-width:620px'>" \
             + "<b>h-index</b>: the largest number <i>h</i> of works that have each " \
             + "been cited at least <i>h</i> times (an h-index of 367 means 367 " \
             + "works were each cited &ge;367 times). " \
             + "<b>g-index</b>: the largest <i>g</i> whose top <i>g</i> works have " \
             + "<i>g</i>&sup2; citations between them - like the h-index but it keeps " \
             + "crediting the most-cited works, so it runs higher (a g near twice the " \
             + "h reflects a few very highly-cited papers). " \
             + h5_note \
             + "<b>i10-index</b> / <b>i100-index</b>: the number of works cited at " \
             + "least 10 / 100 times. Each counts a work once (versions merged), " \
             + "and is computed over the pooled works, so Combined is not the sum " \
             + "of the columns.<br>Per-source detail: " \
             + "<a href='/citation_metrics/crossref'>Crossref metrics</a> &middot; " \
             + "<a href='/citation_metrics/datacite'>DataCite metrics</a></div>"
    # ----- citation concentration (how much of the total rides on the top works;
    # combined + version-merged - a whole-corpus shape metric, not per-source) -----
    conc_sorted = sorted((c for c in pooled_nonver if c > 0), reverse=True)
    conc_total = sum(conc_sorted)
    conc_n = len(conc_sorted)

    def top_share(k):
        ''' Percent of all citations held by the k most-cited works. '''
        k = max(1, min(k, conc_n))
        return sum(conc_sorted[:k]) / conc_total * 100 if conc_total else 0
    one_pct = max(1, round(conc_n * 0.01))
    ten_pct = max(1, round(conc_n * 0.10))
    conc_pts = [("Top work", 1), ("Top 10 works", 10),
                (f"Top 1% ({one_pct:,} works)", one_pct),
                (f"Top 10% ({ten_pct:,} works)", ten_pct)]
    # Shape context for the concentration numbers: mean-vs-median makes the
    # right-skew explicit, and the ">=100 citations" share is a breadth
    # counterweight (concentration at the top coexists with broad depth).
    conc_mean = conc_total / conc_n if conc_n else 0
    conc_median = statistics.median(conc_sorted) if conc_sorted else 0
    conc_med_str = f"{conc_median:,.0f}" if float(conc_median).is_integer() \
                   else f"{conc_median:,.1f}"
    n100 = sum(1 for c in conc_sorted if c >= 100)
    pct100 = n100 / conc_n * 100 if conc_n else 0
    # Prose tracks the actual numbers (see citation_distribution_prose) so it
    # stays honest for a sparse single year, not just the heavy-tailed corpus.
    skew_phrase, depth_clause = citation_distribution_prose(conc_mean, conc_median, pct100)
    conchtml = "<h4>Citation concentration</h4>"
    conchtml += "<div style='font-size:0.95em; max-width:620px; margin-bottom:8px'>" \
                + f"Over <b>{conc_n:,}</b> cited works (versions merged, " \
                + ("all years" if year == 'All' else str(year)) + "): " \
                + f"mean <b>{conc_mean:,.0f}</b> vs. median <b>{conc_med_str}</b> " \
                + f"citations/work - <i>{skew_phrase}</i>. " \
                + f"<b>{n100:,}</b> works (<b>{pct100:.1f}%</b>) are cited " \
                + f"&ge;100 times{depth_clause}</div>"
    conchtml += render_table(['Most-cited works', 'Share of all citations'],
                             [[label, f"{top_share(k):.1f}%"] for label, k in conc_pts],
                             table_id='src-conc', css='tablesorter numberlast-scroll')
    conchtml += "<div style='font-size:0.9em; color:#a8c4e0; max-width:620px'>" \
                + "A high top-work share means overall impact leans on a few " \
                + "landmark papers rather than being spread evenly.</div>"
    # ----- usage by source -----
    uhtml = "<h4>Usage by source</h4>"
    uhtml += render_table(['Metric', 'Combined', 'Crossref', 'DataCite'],
                          [mrow("DOIs with usage", use, 'dois'),
                           mrow("Views", use, 'views'),
                           mrow("Downloads", use, 'downloads'),
                           mrow("Downloads / view", use, 'dpv', 'pct'),
                           mrow("Views / DOI", use, 'vpd', 'round'),
                           mrow("Downloads / DOI", use, 'dpd', 'round')],
                          table_id='src-use', css='tablesorter numbers-scroll')
    uhtml += "<div style='font-size:0.9em; color:#a8c4e0'>Crossref usage = eLife " \
             + "+ protocols.io; DataCite usage = figshare + Zenodo. Downloads " \
             + "exclude protocols.io (no downloads metric); views include it.</div>"
    # ----- share-by-source chart (percent-stacked: composition regardless of
    # the very different absolute magnitudes of citations vs views vs downloads) -----
    share_data = {"Citations": {"Crossref": cite['Crossref']['total'],
                                "DataCite": cite['DataCite']['total']},
                  "Views": {"Crossref": use['Crossref']['views'],
                            "DataCite": use['DataCite']['views']},
                  "Downloads": {"Crossref": use['Crossref']['downloads'],
                                "DataCite": use['DataCite']['downloads']}}
    share_script, share_div = DP.hbar_stacked_chart(
        share_data, ['Crossref', 'DataCite'], "Share by source",
        value_label="Total", value_format="0,0", colors=DP.SOURCE_PALETTE,
        normalize=True, sort=False, width=520, height=320)
    # ----- Lorenz-style concentration curve (visualizes the concentration
    # section: the top-N-share table rows are points on this curve) -----
    lorenz_script, lorenz_div = DP.lorenz_chart(
        conc_sorted, "Citation concentration", width=520, height=320)
    # The curve hugs the top-left (bows above the diagonal) because works are
    # ranked most-cited first; without this key that high shape reads as odd.
    lorenz_div += "<div style='font-size:0.85em; color:#a8c4e0; max-width:520px; " \
                  + "margin-top:4px'>How to read: each point pairs a top share of " \
                  + "works (horizontal) with the share of all citations they hold " \
                  + f"(vertical) - e.g. the top 10% of works hold {top_share(ten_pct):.0f}% " \
                  + "of citations. The curve stays well above the dashed &ldquo;even&rdquo; " \
                  + "line because a few works hold most of the citations - that gap is " \
                  + "the concentration.</div>"
    # All chart divs go inline beside their section (flexrow); their scripts are
    # accumulated into the template's chartscript slot (see bokeh.html) - a div
    # is hydrated by its script wherever the script sits on the page.
    chartscript = share_script + lorenz_script

    def flexrow(section_html, chart_div):
        ''' Pair a section and its chart side by side. '''
        return "<div class='flexrow' style='margin-bottom: 24px'>" \
               + f"<div class='flexcol'>{section_html}</div>" \
               + "<div class='flexcol' style='margin: 10px 0 0 20px'>" \
               + f"{chart_div}</div></div>"
    title = "Impact by source"
    bits = ([escape(year)] if year != 'All' else []) + ([escape(tag)] if tag else [])
    if bits:
        title += " (" + ", ".join(bits) + ")"
    # Tabbed layout, matching the sibling dashboards (dois_metrics / tag_metrics /
    # acknowledgement_metrics): the overview cards + intro are a shared header above
    # the tabs, and each metric section becomes a tab (content and table/chart
    # pairings unchanged). The charts are fixed-width, so they hydrate correctly even
    # inside an initially-hidden pane. general.html + an injected Bokeh CDN tag + the
    # chart scripts in the body is the same recipe the other tabbed dashboards use.
    header = cards + intro
    active_tab = request.args.get('tab')
    if active_tab not in ('citations', 'concentration', 'usage'):
        active_tab = 'citations'
    tabs = ('<ul class="nav nav-tabs" role="tablist">'
            + tab_button('citations', 'Citations', active_tab == 'citations')
            + tab_button('concentration', 'Concentration', active_tab == 'concentration')
            + tab_button('usage', 'Usage', active_tab == 'usage')
            + '</ul><div class="tab-content">'
            + tab_pane('citations', flexrow(chtml, share_div), active_tab == 'citations')
            + tab_pane('concentration', flexrow(conchtml, lorenz_div),
                       active_tab == 'concentration')
            + tab_pane('usage', uhtml, active_tab == 'usage')
            + '</div>')
    seealso = see_also([("DOI metrics", "/dois_metrics"),
                        ("Citations", "/citation_metrics/crossref"),
                        ("Acknowledgements", "/acknowledgement_metrics")])
    # Year + tag filters side by side; each preserves the other's selection (the
    # year links carry ?tag= as a suffix, the tag links keep the year path segment).
    year_suffix = f"?tag={quote(tag, safe='')}" if tag else ''
    filters = year_pulldown('source_metrics', suffix=year_suffix) \
              + "&nbsp;&nbsp;&nbsp;" + tag_pulldown('source_metrics', year, tag)
    html = filters + "<br><br>" + header + tabs + seealso + chartscript
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=title, html=html, bokeh=True,
                                         navbar=generate_navbar('DOIs')))


# figshare DOIs are DataCite-registered with one of these publisher strings
FIGSHARE_PUBLISHERS = ["Janelia Research Campus", "Figshare", "figshare"]
# Strip trailing "specifier" tokens from a figshare title to derive a groupable
# stem, e.g. "MouseLight Neuron AA0547" -> "MouseLight Neuron". Matches serials,
# bare numbers/ranges/dates, version tags, roman numerals, single letters, and
# bracketed ids; _FIG_SPEC_RE additionally strips lab specimen codes
# ("jrc_mus-skin-1"); _FIG_CONN strips a connector word left dangling after a
# code is removed ("... P7 mouse skin for" -> "... P7 mouse skin").
_FIG_ID_RE = re.compile(r'^(?:'
                        r'[vV]\d+(?:\.\d+)*'
                        r'|#?\d+(?:[.\-/]\d+)*'
                        r'|[A-Za-z]{1,5}[-_]?\d+[A-Za-z0-9\-_.]*'
                        r'|[IVXLCDM]{1,6}'
                        r'|[A-Za-z]'
                        r'|\(.*\)|\[.*\]|\{.*\}'
                        r')$')
_FIG_SPEC_RE = re.compile(r'^[a-z]{2,5}_[a-z0-9]+(?:[-_][a-z0-9]+)*$', re.I)
_FIG_TRAIL = ' \t-–—:;,.#/|_'
_FIG_CONN = {'for', 'of', 'in', 'from', 'the', 'a', 'an'}


def strip_html_tags(text):
    ''' Flatten a title to plain text by removing HTML tags. figshare/DataCite
        titles occasionally carry emphasis markup (<b>, <i>); it is stripped
        rather than rendered, since titles are untrusted external metadata.
        Keyword arguments:
          text: raw title string
        Returns:
          The title with tags removed and surrounding whitespace trimmed
    '''
    return re.sub(r'<[^>]+>', '', text or '').strip()


def figshare_title_stem(title):
    ''' Generalize a figshare title by stripping trailing identifier tokens.
        Keyword arguments:
          title: raw title string
        Returns:
          The groupable stem (the trimmed title if everything would be stripped)
    '''
    if not title:
        return ''
    toks = title.strip().split()
    while toks:
        last = toks[-1]
        if (_FIG_ID_RE.match(last) or _FIG_SPEC_RE.match(last.strip('()[]{}'))
                or not last.strip(_FIG_TRAIL) or last.lower() in _FIG_CONN):
            toks.pop()
        else:
            break
    stem = ' '.join(toks).strip(_FIG_TRAIL)
    return stem if stem else title.strip()


def figshare_title_groups(records):
    ''' Group per-DOI figshare records by title stem.
        Keyword arguments:
          records: list of dicts with title/views/downloads/citations
        Returns:
          List of group dicts (stem, count, views, downloads, citations)
          sorted by descending download count
    '''
    buckets = collections.defaultdict(list)
    casings = collections.defaultdict(collections.Counter)
    for rec in records:
        stem = figshare_title_stem(rec['title'])
        key = stem.lower()
        buckets[key].append(rec)
        casings[key][stem] += 1
    groups = []
    for key, recs in buckets.items():
        # Metrics are identical across versioned and non-versioned DOIs in the same
        # group; count only non-versioned DOIs to avoid double-counting. If the
        # group has no non-versioned DOI, count exactly one versioned DOI.
        nonver = [r for r in recs if not re.search(r'/janelia.+\.v\d+$', r['doi'])]
        metric_recs = nonver if nonver else recs[:1]
        groups.append({'stem': casings[key].most_common(1)[0][0],
                       'count': len(recs),
                       'views': sum(r['views'] for r in metric_recs),
                       'downloads': sum(r['downloads'] for r in metric_recs),
                       'citations': sum(r['citations'] for r in metric_recs)})
    groups.sort(key=lambda g: (-g['downloads'], -g['count']))
    return groups


@app.route('/figshare_stats/<string:year>')
@app.route('/figshare_stats')
def figshare_metrics(year='All'):  # pylint: disable=too-many-locals,too-many-branches,too-many-statements
    ''' Show figshare deposit, usage, and citation metrics.
        figshare DOIs are DataCite-registered DOIs whose publisher is one of
        FIGSHARE_PUBLISHERS (the Janelia portal plus generic figshare). Usage
        counts (views/downloads/shares) come from jrc_figshare_counts; citation
        totals/sources come from the nightly citation sync.
    '''
    coll = DB['dis'].dois
    match = {"jrc_obtained_from": "DataCite", "publisher": {"$in": FIGSHARE_PUBLISHERS}}
    if year != 'All':
        match["jrc_publishing_date"] = {"$regex": "^" + year}
    proj = {"doi": 1, "titles": 1, "publisher": 1, "relation": 1,
            "jrc_publishing_date": 1, "jrc_figshare_counts": 1,
            "jrc_citation_count": 1, "jrc_citation_sources": 1, "types": 1}
    try:
        docs = list(coll.find(match, proj))
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get figshare DOIs"),
                               message=error_message(err))
    source_name = {"datacite": "DataCite", "openalex": "OpenAlex",
                   "scholexplorer": "ScholeXplorer", "crossref": "Crossref"}
    total = len(docs)
    usage = {'Views': 0, 'Downloads': 0, 'Shares': 0}
    usage_dois = nonver = cited_dois = cite_total = 0
    counts = []
    by_pub = collections.defaultdict(int)
    by_type = collections.defaultdict(int)
    cite_src = collections.defaultdict(int)
    by_year = {}
    for row in docs:
        by_pub[row.get('publisher', 'Unknown')] += 1
        rtype = (row.get('types') or {}).get('resourceTypeGeneral') or 'Unknown'
        by_type[rtype] += 1
        if not DL.is_version(row):
            nonver += 1
        fcounts = row.get('jrc_figshare_counts') or {}
        views = fcounts.get('views', 0)
        downloads = fcounts.get('downloads', 0)
        shares = fcounts.get('shares', 0)
        if fcounts:
            usage_dois += 1
            usage['Views'] += views
            usage['Downloads'] += downloads
            usage['Shares'] += shares
        ccount = row.get('jrc_citation_count', 0) or 0
        if ccount:
            cited_dois += 1
            cite_total += ccount
            counts.append(ccount)
        for src, val in (row.get('jrc_citation_sources') or {}).items():
            cite_src[source_name.get(src, src.capitalize())] += val
        yname = (row.get('jrc_publishing_date') or '')[:4]
        if yname.isdigit():
            rec = by_year.setdefault(yname, {'dois': 0, 'views': 0,
                                             'downloads': 0, 'citations': 0})
            rec['dois'] += 1
            rec['views'] += views
            rec['downloads'] += downloads
            rec['citations'] += ccount
    if not total:
        msg = "No figshare DOIs were found"
        if year != 'All':
            msg += f" for publishing year {year}"
        html = year_pulldown('figshare_stats') + "<br><br>" + render_warning(msg, 'warning')
        endpoint_access()
        return make_response(render_template('general.html', urlroot=request.url_root,
                                             title="figshare metrics", html=html,
                                             navbar=generate_navbar('DataCite')))
    # ----- headline stat cards -----
    conv = usage['Downloads'] / usage['Views'] if usage['Views'] else 0
    cards = stat_cards([("figshare DOIs", f"{total:,}"),
                        ("DOIs with usage data",
                         f"{usage_dois:,} ({usage_dois/total*100:,.1f}%)"),
                        ("Total views", f"{usage['Views']:,}"),
                        ("Total downloads", f"{usage['Downloads']:,}"),
                        ("Total shares", f"{usage['Shares']:,}")],
                       div_id='fig-stats')
    cards += stat_cards([("Downloads / view", f"{conv*100:,.1f}%"),
                         ("DOIs cited", f"{cited_dois:,} ({cited_dois/total*100:,.1f}%)"),
                         ("Total citations", f"{cite_total:,}")],
                        div_id='fig-stats2')
    intro = "<div style='font-size:0.85em; color:#a8c4e0; max-width:620px; " \
            + "margin:-8px 0 16px 0'>Usage is the sum of per-record views/downloads " \
            + "across all versions. See <a href='/figshare_groups'>figshare title " \
            + "groups</a> for the same DOIs collapsed by title.</div>"
    # ----- usage totals -----
    uhtml = "<h4>Usage totals</h4>"
    uhtml += render_table(['Metric', 'Count'],
                          [[k, f"{v:,}"] for k, v in usage.items()],
                          table_id='fig-usage', css='tablesorter numberlast-scroll')
    # ----- deposits & usage by publishing year -----
    # A specific year is already the query filter, so a by-year breakdown would be
    # a single, redundant row/bar. Only build it for the all-years view.
    ydata = {'Year': [], 'DOIs': [], 'Downloads': []}
    yhtml = ''
    if year == 'All':
        ytrows = []
        for yname in sorted(by_year):
            rec = by_year[yname]
            ytrows.append([yname, f"{rec['dois']:,}", f"{rec['views']:,}",
                           f"{rec['downloads']:,}", f"{rec['citations']:,}"])
            ydata['Year'].append(yname)
            ydata['DOIs'].append(rec['dois'])
            ydata['Downloads'].append(rec['downloads'])
        yhtml = "<h4>Deposits &amp; usage by publishing year</h4>"
        yhtml += render_table(['Year', 'DOIs', 'Views', 'Downloads', 'Citations'],
                              ytrows, table_id='fig-years', css='tablesorter numberlast-scroll')
    # ----- resource type breakdown -----
    type_data = dict(by_type)
    thtml = "<h4>DOIs by resource type</h4>"
    thtml += render_table(['Resource type', 'DOIs'],
                          [[k, f"{v:,}"] for k, v in
                           sorted(by_type.items(), key=itemgetter(1), reverse=True)],
                          table_id='fig-types', css='tablesorter numberlast-scroll')
    # ----- publisher breakdown -----
    pub_data = dict(by_pub)
    phtml = "<h4>DOIs by publisher</h4>"
    phtml += render_table(['Publisher', 'DOIs'],
                          [[k, f"{v:,}"] for k, v in
                           sorted(by_pub.items(), key=itemgetter(1), reverse=True)],
                          table_id='fig-pubs', css='tablesorter numberlast-scroll')
    # ----- citation sources (which source surfaces figshare citations) -----
    src_data = dict(cite_src)
    shtml = ''
    if cite_src:
        shtml = "<h4>Citations by source</h4>"
        shtml += render_table(['Source', 'Citations'],
                              [[k, f"{v:,}"] for k, v in
                               sorted(cite_src.items(), key=itemgetter(1), reverse=True)],
                              table_id='fig-sources', css='tablesorter numberlast-scroll')
        shtml += "<div style='font-size:0.85em; color:#a8c4e0; max-width:460px'>" \
                 + "A citing work found by more than one source is counted once " \
                 + "per source here.</div>"
    if request.args.get('fmt') == 'json':
        result = initialize_result()
        result['data'] = {"dois": total, "non_versioned_dois": nonver,
                          "usage_dois": usage_dois, "usage": usage,
                          "downloads_per_view": round(conv, 4),
                          "cited_dois": cited_dois, "total_citations": cite_total,
                          "median_citations": statistics.median(counts) if counts else 0,
                          "by_publisher": pub_data, "by_type": type_data,
                          "citations_by_source": src_data,
                          "by_year": by_year}
        result['rest']['source'] = 'mongo'
        result['rest']['row_count'] = total
        return generate_response(result)
    # ----- charts -----
    chartscript = ''
    usage_div = year_div = type_div = pub_div = src_div = ''
    if any(usage.values()):
        script, usage_div = DP.pie_chart(usage, "Usage totals", "metric", width=500,
                                         fmt="{0,0}")
        chartscript += script
    if ydata['Year']:
        # Tap a year bar -> figshare metrics scoped to that year
        ynav = {yr: f"/figshare_stats/{yr}" for yr in ydata['Year']}
        script, year_div = DP.dual_axis_chart(
            ydata, title="Deposits & downloads by year", x_field='Year',
            bar_field='DOIs', line_field='Downloads', bar_label='DOIs deposited',
            line_label='Downloads', bar_color='darkorange', bar_format="0,0",
            line_format="0,0", width=650, height=400, nav=ynav)
        chartscript += script
    script, type_div = DP.hbar_chart(type_data, "DOIs by resource type",
                                     value_label="DOIs", width=500, height=320,
                                     value_format="0,0", show_values=True)
    chartscript += script
    script, pub_div = DP.pie_chart(pub_data, "DOIs by publisher", "publisher",
                                   width=500, fmt="{0,0}")
    chartscript += script
    if src_data:
        colors = DP.get_colors_by_count(len(src_data))
        script, src_div = DP.pie_chart(src_data, "Citations by source", "source",
                                       width=500, colors=colors, fmt="{0,0}")
        chartscript += script
    def flexrow(table_html, chart_div):
        ''' Pair a table and its chart side by side '''
        return "<div class='flexrow' style='margin-bottom: 40px'><div class='flexcol'>" \
               + table_html + "</div><div class='flexcol' style='margin: 10px 0 0 20px'>" \
               + chart_div + "</div></div>"
    title = "figshare metrics"
    if year != 'All':
        title += f" ({year})"
    html = year_pulldown('figshare_stats') + "<br><br>" + cards + intro \
           + flexrow(uhtml, usage_div)
    if yhtml:
        html += flexrow(yhtml, year_div)
    html += flexrow(thtml, type_div) + flexrow(phtml, pub_div)
    if shtml:
        html += flexrow(shtml, src_div)
    endpoint_access()
    return make_response(render_template('bokeh.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         chartscript=chartscript, chartdiv='',
                                         chartscript2='', chartdiv2='',
                                         navbar=generate_navbar('DataCite')))


# Shared by the figshare/Zenodo/protocols.io/eLife group-listing endpoints
# (figshare_groups, zenodo_groups, protocolsio_dois, elife_dois): each groups
# its publisher's DOIs under a concept/stem key and shows a ranked "top 20"
# table + companion bar chart per dimension (views/downloads/exports/count/
# citations). id_key/label_key/nav_param carry each publisher's naming
# (figshare groups by title "stem"; the others by version "concept" DOI, with
# the member's own most-common title as the display label).
def group_table(ordered, top_n, table_id, columns, base, id_key, nav_param, label_key,  # pylint: disable=too-many-arguments,too-many-positional-arguments
                metric_keys, plural_noun, show_remainder_count):
    ''' Render one "top N groups" table: a link (drill-down to the group's
        members) plus its ranked metrics, with a "+N more" summary footer row
        for anything past top_n.
        Keyword arguments:
          ordered: groups sorted by the dimension this table ranks
          top_n: how many rows to show before folding the rest into the footer
          table_id: HTML id for the rendered table (drives client-side sorting)
          columns: column headers, in order: [group label, count, *metric labels]
          base: the route's own base path (drill-down links are relative to it)
          id_key: which group field identifies the group in the drill-down URL
          nav_param: the drill-down query param name
          label_key: which group field to use as the link's display text
          metric_keys: group fields for the numeric columns after the count
                      column, in column order (e.g. ('views', 'downloads', 'citations'))
          plural_noun: what one row represents, for the footer ("groups", "deposits", ...)
          show_remainder_count: whether the footer also sums the count column for
                               the folded-in remainder (figshare/Zenodo do; protocols.io/
                               eLife instead colspan the label cell across the label and
                               count columns and drop that figure)
        Returns:
          HTML table
    '''
    rows = []
    for grp in ordered[:top_n]:
        label = f"<a href='{base}?{nav_param}={quote(grp[id_key])}'>{escape(grp[label_key])}</a>"
        rows.append([safe(label), f"{grp['count']:,}"]
                    + [f"{grp[key]:,}" for key in metric_keys])
    foot = None
    if len(ordered) > top_n:
        rest = ordered[top_n:]
        if show_remainder_count:
            foot = [fcell(f"+ {len(rest):,} more {plural_noun}"),
                    fcell(f"{sum(g['count'] for g in rest):,}", align='center')]
        else:
            foot = [fcell(f"+ {len(rest):,} more {plural_noun}", colspan=2)]
        foot.extend(fcell(f"{sum(g[key] for g in rest):,}", align='center')
                    for key in metric_keys)
    return render_table(columns, rows, table_id=table_id,
                        css='tablesorter numberlast-scroll', footer=foot)


def group_chart_data(ordered, value_key, base, label_key, id_key, nav_param):  # pylint: disable=too-many-arguments,too-many-positional-arguments
    ''' Build {label: value} and a parallel {label: drill-down URL} for a group
        table's companion bar chart. A group is skipped if its value for this
        dimension is zero - except the 'count' dimension, which by construction
        is never zero for an included (multi-version/multi-DOI) group. Long
        labels get a middle ellipsis (keeps the distinguishing tail, e.g.
        "...P7 mouse skin") so same-prefixed series stay readable; a uniqueness
        guard stops two labels from colliding and silently dropping a bar. The
        nav map carries the untruncated group key so a bar tap reaches the same
        drill-down page as the table row.
        Keyword arguments:
          ordered: groups sorted by the dimension being charted
          value_key: which group field to chart ('views'/'downloads'/'count'/etc.)
          base: the route's own base path (drill-down links are relative to it)
          label_key: which group field to use as the display label
          id_key: which group field identifies the group in the drill-down URL
          nav_param: the drill-down query param name
        Returns:
          (data, nav): {label: value} and {label: drill-down URL}
    '''
    data = {}
    nav = {}
    for grp in ordered[:15]:
        value = grp[value_key]
        if value_key != 'count' and not value:
            continue
        raw_label = grp[label_key]
        label = raw_label if len(raw_label) <= 41 else raw_label[:24] + '…' + raw_label[-16:]
        while label in data:
            label += ' '
        data[label] = value
        nav[label] = f"{base}?{nav_param}={quote(grp[id_key])}"
    return data, nav


def group_flexrow(table_html, chart_div):
    ''' Pair a table and its chart side by side '''
    return "<div class='flexrow' style='margin-bottom: 40px'><div class='flexcol'>" \
           + table_html + "</div><div class='flexcol' style='margin: 10px 0 0 20px'>" \
           + chart_div + "</div></div>"


@app.route('/figshare_groups/<string:year>')
@app.route('/figshare_groups')
def figshare_groups(year='All'):  # pylint: disable=too-many-locals,too-many-branches,too-many-statements
    ''' Show figshare DOIs grouped by generalized title.
        Many figshare deposits are series that differ only by a trailing
        identifier ("MouseLight Neuron AA0547" -> "MouseLight Neuron"); this
        collapses them (see figshare_title_stem) and ranks the groups both by
        downloads and by DOI count.
    '''
    coll = DB['dis'].dois
    match = {"jrc_obtained_from": "DataCite", "publisher": {"$in": FIGSHARE_PUBLISHERS}}
    if year != 'All':
        match["jrc_publishing_date"] = {"$regex": "^" + year}
    proj = {"doi": 1, "titles": 1, "jrc_figshare_counts": 1, "jrc_citation_count": 1}
    try:
        docs = list(coll.find(match, proj))
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get figshare DOIs"),
                               message=error_message(err))
    records = []
    for row in docs:
        fcounts = row.get('jrc_figshare_counts') or {}
        records.append({'doi': row['doi'],
                        'title': strip_html_tags(strip_tex_math(DL.get_title(row))),
                        'views': fcounts.get('views', 0),
                        'downloads': fcounts.get('downloads', 0),
                        'citations': row.get('jrc_citation_count', 0) or 0})
    total = len(records)
    if not total:
        msg = "No figshare DOIs were found"
        if year != 'All':
            msg += f" for publishing year {year}"
        html = year_pulldown('figshare_groups') + "<br><br>" \
               + render_warning(msg, 'warning')
        endpoint_access()
        return make_response(render_template('general.html', urlroot=request.url_root,
                                             title="figshare title groups", html=html,
                                             navbar=generate_navbar('DataCite')))
    base = '/figshare_groups' if year == 'All' else f"/figshare_groups/{year}"
    # Drill-down: ?stem=<group> lists the member DOIs of one title group
    stem = request.args.get('stem')
    if stem:
        members = [r for r in records
                   if figshare_title_stem(r['title']).lower() == stem.lower()]
        members.sort(key=itemgetter('downloads', 'views'), reverse=True)
        if request.args.get('fmt') == 'json':
            result = initialize_result()
            result['data'] = {"stem": stem, "dois": len(members), "members": members}
            result['rest']['source'] = 'mongo'
            result['rest']['row_count'] = len(members)
            return generate_response(result)
        dtitle = f"figshare group: {stem}"
        back = f"<a href='{base}' class='btn btn-outline-primary btn-sm'>" \
               + "&larr; all groups</a>"
        if not members:
            html = back + "<br><br>" \
                   + render_warning(f"No figshare DOIs match the group "
                                    f"\"{escape(stem)}\"", 'warning')
            endpoint_access()
            return make_response(render_template('general.html', urlroot=request.url_root,
                                                 title=dtitle, html=html,
                                                 navbar=generate_navbar('DataCite')))
        mrows = []
        mrow_classes = []
        tviews = tdl = tcit = 0
        nonver_seen = False
        for rec in members:
            is_ver = bool(re.search(r'/janelia.+\.v\d+$', rec['doi']))
            # Count metrics only for non-versioned DOIs to avoid double-counting
            # (versioned and non-versioned DOIs share identical metric values).
            if not is_ver:
                tviews += rec['views']
                tdl += rec['downloads']
                tcit += rec['citations']
                nonver_seen = True
            mrows.append([safe(doi_link(rec['doi'])), rec['title'], f"{rec['views']:,}",
                          f"{rec['downloads']:,}", f"{rec['citations']:,}"])
            mrow_classes.append('ver' if is_ver else '')
        if not nonver_seen and members:
            # All members are versioned; use one record's metrics (all are identical).
            tviews = members[0]['views']
            tdl = members[0]['downloads']
            tcit = members[0]['citations']
        ver_count = sum(1 for c in mrow_classes if c == 'ver')
        # When filter is ON only non-versioned rows remain; if ALL were versioned that's 0.
        tviews_nv = tviews if nonver_seen else 0
        tdl_nv = tdl if nonver_seen else 0
        tcit_nv = tcit if nonver_seen else 0
        mtable = render_table(['DOI', 'Title', 'Views', 'Downloads', 'Citations'], mrows,
                              table_id='fig-members', css='tablesorter numberlast-scroll',
                              row_classes=mrow_classes,
                              footer=[fcell('Total', colspan=2),
                                      fcell(f"{tviews:,}", align='center'),
                                      fcell(f"{tdl:,}", align='center'),
                                      fcell(f"{tcit:,}", align='center')])
        mcards = stat_cards([("DOIs in group", f"<span id='totalrows'>{len(members):,}</span>"),
                             ("Total views", f"<span id='fig-views'>{tviews:,}</span>"),
                             ("Total downloads", f"<span id='fig-dl'>{tdl:,}</span>"),
                             ("Total citations", f"<span id='fig-cit'>{tcit:,}</span>")],
                            div_id='figm-stats')
        verbtn = ""
        if ver_count:
            verbtn = (
                "<button id='verbtn' class='btn btn-outline-warning' "
                + f"data-views-all=\"{tviews:,}\" data-views-nv=\"{tviews_nv:,}\" "
                + f"data-dl-all=\"{tdl:,}\" data-dl-nv=\"{tdl_nv:,}\" "
                + f"data-cit-all=\"{tcit:,}\" data-cit-nv=\"{tcit_nv:,}\" "
                + "data-filtered=\"0\" "
                + "onclick=\"toggler('fig-members', 'ver', 'totalrows'); "
                + "var f=this.dataset.filtered==='1'; "
                + "this.dataset.filtered=f?'0':'1'; "
                + "document.getElementById('fig-views').textContent=f?this.dataset.viewsAll:this.dataset.viewsNv; "
                + "document.getElementById('fig-dl').textContent=f?this.dataset.dlAll:this.dataset.dlNv; "
                + "document.getElementById('fig-cit').textContent=f?this.dataset.citAll:this.dataset.citNv;\">"
                + "Filter versioned DOIs</button>&nbsp;<br><br>"
            )
        html = back + "<br><br>" + mcards + verbtn + mtable
        endpoint_access()
        return make_response(render_template('general.html', urlroot=request.url_root,
                                             title=dtitle, html=html,
                                             navbar=generate_navbar('DataCite')))
    all_groups = figshare_title_groups(records)
    groups = [g for g in all_groups if g['count'] > 1]
    grouped_dois = sum(g['count'] for g in groups)
    largest = max(groups, key=lambda g: g['count']) if groups else None
    top_n = 20
    by_dl = groups  # already sorted by downloads
    by_cnt = sorted(groups, key=lambda g: (-g['count'], -g['downloads']))
    if request.args.get('fmt') == 'json':
        result = initialize_result()
        result['data'] = {"dois": total, "all_groups": len(all_groups),
                          "multi_doi_groups": len(groups), "grouped_dois": grouped_dois,
                          "largest_group": {"stem": largest['stem'],
                                            "count": largest['count']} if largest else None,
                          "title_groups": [{k: g[k] for k in
                                            ('stem', 'count', 'views', 'downloads',
                                             'citations')} for g in groups]}
        result['rest']['source'] = 'mongo'
        result['rest']['row_count'] = len(groups)
        return generate_response(result)
    largest_card = (f"Largest group ({escape(largest['stem'])})", f"{largest['count']:,}") \
                   if largest else ("Largest group", "N/A")
    cards = stat_cards(
        [("figshare DOIs", f"{total:,}"),
         ("Title groups (all)", f"{len(all_groups):,}"),
         ("Multi-DOI groups", f"{len(groups):,}"),
         ("DOIs in a multi-DOI group",
          f"{grouped_dois:,} ({grouped_dois/total*100:,.1f}%)"),
         largest_card],
        div_id='figg-stats')
    cards += stat_cards([("Total views", f"{sum(g['views'] for g in groups):,}"),
                         ("Total downloads", f"{sum(g['downloads'] for g in groups):,}"),
                         ("Total citations", f"{sum(g['citations'] for g in groups):,}")],
                        div_id='figg-stats2')
    gnote = "<div style='font-size:0.85em; color:#a8c4e0; max-width:620px; " \
            + "margin:-8px 0 16px 0'>Groups are formed by stripping trailing " \
            + "identifiers from each title (serials like AA0547, specimen codes " \
            + "like jrc_mus-skin-1, versions, dates). Each group label links to " \
            + "the list of its member DOIs.</div>"
    # Each dimension's table+chart pair is shown only when it has something to
    # rank: "DOI count" is guaranteed nonzero whenever any multi-DOI group exists
    # (every group has count > 1 by construction), but "downloads" can be
    # entirely zero for a sparse year even though groups exist.
    total_dl = sum(g['downloads'] for g in groups)
    chartscript = ''
    sections = ''
    for label, ordered, total_metric, value_key, table_id, unit in (
            ('downloads', by_dl, total_dl, 'downloads', 'fig-grp-dl', 'Downloads'),
            ('DOI count', by_cnt, len(groups), 'count', 'fig-grp-cnt', 'DOIs')):
        if not total_metric:
            continue
        thtml = f"<h4>Title groups by {label} (top 20)</h4>" \
                + group_table(ordered, top_n, table_id,
                             ['Group', 'DOIs', 'Views', 'Downloads', 'Citations'], base,
                             'stem', 'stem', 'stem', ('views', 'downloads', 'citations'),
                             'groups', True)
        data, nav = group_chart_data(ordered, value_key, base, 'stem', 'stem', 'stem')
        chart_div = ''
        if data:
            script, chart_div = DP.hbar_chart(data, f"Top title groups by {label}",
                                              value_label=unit, width=620, height=420,
                                              value_format="0,0", show_values=True, nav=nav)
            chartscript += script
        sections += group_flexrow(thtml, chart_div)
    title = "figshare title groups"
    if year != 'All':
        title += f" ({year})"
    html = year_pulldown('figshare_groups') + "<br><br>" + cards + gnote + sections
    endpoint_access()
    return make_response(render_template('bokeh.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         chartscript=chartscript, chartdiv='',
                                         chartscript2='', chartdiv2='',
                                         navbar=generate_navbar('DataCite')))


# Zenodo DOIs are DataCite-registered with publisher "Zenodo" and a
# 10.5281/zenodo.<id> prefix. Per-record usage (views/downloads) is stored in
# jrc_zenodo_counts by the citation sync; the deposits page sums it across a
# concept's versions, and /zenodo_stats aggregates it site-wide.
ZENODO_PUBLISHERS = ["Zenodo"]


def zenodo_match(year='All'):
    ''' Build the dois query that selects Zenodo DOIs. A Zenodo DOI is
        DataCite-registered and is identified by publisher "Zenodo" or, as a
        belt-and-suspenders fallback, a 10.5281/zenodo. DOI (some carry a
        non-Zenodo publisher string). Optionally limited to a publishing year.
        Keyword arguments:
          year: 4-digit publishing year, or 'All'
        Returns:
          a MongoDB filter dict
    '''
    match = {"jrc_obtained_from": "DataCite",
             "$or": [{"publisher": {"$in": ZENODO_PUBLISHERS}},
                     {"doi": {"$regex": r"zenodo\.", "$options": "i"}}]}
    if year != 'All':
        match["jrc_publishing_date"] = {"$regex": "^" + year}
    return match


def zenodo_concept_doi(row):
    ''' Return a Zenodo record's concept (parent) DOI - the identifier every
        version of a deposit shares. A version record points at it via a
        relatedIdentifier with relationType "IsVersionOf"; a concept record (or
        an unversioned deposit) has no such link and groups under its own DOI.
        Keyword arguments:
          row: DOI record (needs doi and, optionally, relatedIdentifiers)
        Returns:
          The lower-cased concept DOI string
    '''
    for rel in row.get('relatedIdentifiers') or []:
        if (rel.get('relatedIdentifierType') == 'DOI'
                and rel.get('relationType') == 'IsVersionOf'):
            cand = (rel.get('relatedIdentifier') or '').strip().lower()
            if cand:
                return cand
    return (row.get('doi') or '').strip().lower()


def zenodo_concept_groups(records):
    ''' Group Zenodo records by concept DOI, summing usage and citations across
        the concept's versions.
        Keyword arguments:
          records: list of dicts with doi/title/views/downloads/citations/concept
        Returns:
          List of group dicts (concept, label, count, views, downloads, citations)
          sorted by descending DOI count then citations. The label is the most
          common member title (versions of one deposit usually share a title).
    '''
    buckets = collections.defaultdict(list)
    for rec in records:
        buckets[rec['concept']].append(rec)
    groups = []
    for concept, recs in buckets.items():
        casings = collections.Counter(r['title'] for r in recs if r['title'])
        label = casings.most_common(1)[0][0] if casings else concept
        groups.append({'concept': concept, 'label': label, 'count': len(recs),
                       'views': sum(r['views'] for r in recs),
                       'downloads': sum(r['downloads'] for r in recs),
                       'citations': sum(r['citations'] for r in recs)})
    groups.sort(key=lambda g: (-g['count'], -g['citations']))
    return groups


@app.route('/zenodo_groups/<string:year>')
@app.route('/zenodo_groups')
def zenodo_groups(year='All'):  # pylint: disable=too-many-locals,too-many-branches,too-many-statements
    ''' Show Zenodo deposits, each collapsing all versions of one software
        release or dataset under their shared concept (parent) DOI (see
        zenodo_concept_doi). No usage metrics are stored for Zenodo, so deposits
        are ranked by citation count and by version count.
    '''
    coll = DB['dis'].dois
    proj = {"doi": 1, "titles": 1, "jrc_citation_count": 1, "relatedIdentifiers": 1,
            "jrc_zenodo_counts": 1}
    try:
        docs = list(coll.find(zenodo_match(year), proj))
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get Zenodo DOIs"),
                               message=error_message(err))
    records = []
    for row in docs:
        zc = row.get('jrc_zenodo_counts') or {}
        records.append({'doi': row['doi'],
                        'title': strip_html_tags(strip_tex_math(DL.get_title(row))),
                        'views': zc.get('views', 0),
                        'downloads': zc.get('downloads', 0),
                        'citations': row.get('jrc_citation_count', 0) or 0,
                        'concept': zenodo_concept_doi(row)})
    total = len(records)
    if not total:
        msg = "No Zenodo DOIs were found"
        if year != 'All':
            msg += f" for publishing year {year}"
        html = year_pulldown('zenodo_groups') + "<br><br>" \
               + render_warning(msg, 'warning')
        endpoint_access()
        return make_response(render_template('general.html', urlroot=request.url_root,
                                             title="Zenodo deposits", html=html,
                                             navbar=generate_navbar('DataCite')))
    base = '/zenodo_groups' if year == 'All' else f"/zenodo_groups/{year}"
    # Drill-down: ?concept=<doi> lists the member (version) DOIs of one group
    concept = request.args.get('concept')
    if concept:
        members = [r for r in records if r['concept'] == concept.lower()]
        members.sort(key=itemgetter('downloads', 'citations'), reverse=True)
        if request.args.get('fmt') == 'json':
            result = initialize_result()
            result['data'] = {"concept": concept, "dois": len(members),
                              "members": members}
            result['rest']['source'] = 'mongo'
            result['rest']['row_count'] = len(members)
            return generate_response(result)
        back = f"<a href='{base}' class='btn btn-outline-primary btn-sm'>" \
               + "&larr; all deposits</a>"
        if not members:
            html = back + "<br><br>" \
                   + render_warning(f"No Zenodo DOIs match the deposit "
                                    f"\"{escape(concept)}\"", 'warning')
            endpoint_access()
            return make_response(render_template('general.html', urlroot=request.url_root,
                                                 title=f"Zenodo deposit: {concept}", html=html,
                                                 navbar=generate_navbar('DataCite')))
        casings = collections.Counter(r['title'] for r in members if r['title'])
        dtitle = f"Zenodo deposit: {casings.most_common(1)[0][0] if casings else concept}"
        mrows = []
        tviews = tdl = tcit = 0
        for rec in members:
            tviews += rec['views']
            tdl += rec['downloads']
            tcit += rec['citations']
            mrows.append([safe(doi_link(rec['doi'])), rec['title'], f"{rec['views']:,}",
                          f"{rec['downloads']:,}", f"{rec['citations']:,}"])
        mtable = render_table(['Version DOI', 'Title', 'Views', 'Downloads', 'Citations'],
                              mrows, table_id='zen-members',
                              css='tablesorter numberlast-scroll',
                              footer=[fcell('Total', colspan=2),
                                      fcell(f"{tviews:,}", align='center'),
                                      fcell(f"{tdl:,}", align='center'),
                                      fcell(f"{tcit:,}", align='center')])
        mcards = stat_cards([("Versions", f"{len(members):,}"),
                             ("Total views", f"{tviews:,}"),
                             ("Total downloads", f"{tdl:,}"),
                             ("Total citations", f"{tcit:,}")], div_id='zenm-stats')
        html = back + "<br><br>" + mcards + mtable
        endpoint_access()
        return make_response(render_template('general.html', urlroot=request.url_root,
                                             title=dtitle, html=html,
                                             navbar=generate_navbar('DataCite')))
    all_groups = zenodo_concept_groups(records)
    groups = [g for g in all_groups if g['count'] > 1]
    grouped_dois = sum(g['count'] for g in groups)
    largest = max(groups, key=lambda g: g['count']) if groups else None
    top_n = 20
    by_dl = sorted(groups, key=lambda g: (-g['downloads'], -g['count']))
    by_cnt = groups  # already sorted by DOI count
    by_cit = sorted(groups, key=lambda g: (-g['citations'], -g['count']))
    if request.args.get('fmt') == 'json':
        result = initialize_result()
        result['data'] = {"dois": total, "all_groups": len(all_groups),
                          "multi_version_groups": len(groups), "grouped_dois": grouped_dois,
                          "largest_group": {"concept": largest['concept'],
                                            "label": largest['label'],
                                            "count": largest['count']} if largest else None,
                          "concept_groups": [{k: g[k] for k in
                                              ('concept', 'label', 'count', 'views',
                                               'downloads', 'citations')}
                                             for g in groups]}
        result['rest']['source'] = 'mongo'
        result['rest']['row_count'] = len(groups)
        return generate_response(result)
    largest_card = (f"Most versions ({escape(largest['label'])})", f"{largest['count']:,}") \
                   if largest else ("Most versions", "N/A")
    cards = stat_cards(
        [("Zenodo DOIs", f"{total:,}"),
         ("Deposits (all)", f"{len(all_groups):,}"),
         ("Multi-version deposits", f"{len(groups):,}"),
         ("DOIs in multi-version deposits",
          f"{grouped_dois:,} ({grouped_dois/total*100:,.1f}%)"),
         largest_card],
        div_id='zeng-stats')
    cards += stat_cards([("Total views", f"{sum(g['views'] for g in groups):,}"),
                         ("Total downloads", f"{sum(g['downloads'] for g in groups):,}"),
                         ("Total citations", f"{sum(g['citations'] for g in groups):,}")],
                        div_id='zeng-stats2')
    gnote = "<div style='font-size:0.85em; color:#a8c4e0; max-width:620px; " \
            + "margin:-8px 0 16px 0'>Each deposit collapses every version of one " \
            + "Zenodo software release or dataset under the shared concept DOI that " \
            + "links them (the target of each version's <i>IsVersionOf</i> relation); " \
            + "unversioned deposits stand alone. Views, downloads, and citations are " \
            + "summed across a deposit's versions. Each label links to the deposit's " \
            + "version DOIs.</div>"
    # Each dimension's table+chart pair is shown only when it has something to
    # rank: "version count" is guaranteed nonzero whenever any multi-version
    # deposit exists (every group has count > 1 by construction), but
    # "downloads"/"citations" can each independently be zero for a sparse year.
    total_dl = sum(g['downloads'] for g in groups)
    total_cit = sum(g['citations'] for g in groups)
    chartscript = ''
    sections = ''
    for label, ordered, total_metric, value_key, table_id, unit in (
            ('downloads', by_dl, total_dl, 'downloads', 'zen-grp-dl', 'Downloads'),
            ('version count', by_cnt, len(groups), 'count', 'zen-grp-cnt', 'Versions'),
            ('citations', by_cit, total_cit, 'citations', 'zen-grp-cit', 'Citations')):
        if not total_metric:
            continue
        thtml = f"<h4>Deposits by {label} (top 20)</h4>" \
                + group_table(ordered, top_n, table_id,
                             ['Deposit', 'Versions', 'Views', 'Downloads', 'Citations'],
                             base, 'concept', 'concept', 'label',
                             ('views', 'downloads', 'citations'), 'deposits', True)
        data, nav = group_chart_data(ordered, value_key, base, 'label', 'concept', 'concept')
        chart_div = ''
        if data:
            script, chart_div = DP.hbar_chart(data, f"Top deposits by {label}",
                                              value_label=unit, width=620, height=420,
                                              value_format="0,0", show_values=True, nav=nav)
            chartscript += script
        sections += group_flexrow(thtml, chart_div)
    title = "Zenodo deposits"
    if year != 'All':
        title += f" ({year})"
    html = year_pulldown('zenodo_groups') + "<br><br>" + cards + gnote + sections
    endpoint_access()
    return make_response(render_template('bokeh.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         chartscript=chartscript, chartdiv='',
                                         chartscript2='', chartdiv2='',
                                         navbar=generate_navbar('DataCite')))


@app.route('/zenodo_stats/<string:year>')
@app.route('/zenodo_stats')
def zenodo_stats(year='All'):  # pylint: disable=too-many-locals,too-many-branches,too-many-statements
    ''' Show Zenodo deposit, usage, and citation metrics.
        Zenodo DOIs are DataCite-registered (see zenodo_match). Per-record usage
        (views/downloads) comes from jrc_zenodo_counts; citation totals/sources
        come from the citation sync. Resource type comes from DataCite metadata.
    '''
    coll = DB['dis'].dois
    proj = {"doi": 1, "types": 1, "jrc_publishing_date": 1, "jrc_zenodo_counts": 1,
            "jrc_citation_count": 1, "jrc_citation_sources": 1}
    try:
        docs = list(coll.find(zenodo_match(year), proj))
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get Zenodo DOIs"),
                               message=error_message(err))
    source_name = {"datacite": "DataCite", "openalex": "OpenAlex",
                   "scholexplorer": "ScholeXplorer", "crossref": "Crossref"}
    total = len(docs)
    usage = {'Views': 0, 'Downloads': 0}
    uniq = {'Unique views': 0, 'Unique downloads': 0}
    usage_dois = cited_dois = cite_total = 0
    counts = []
    by_type = collections.defaultdict(int)
    cite_src = collections.defaultdict(int)
    by_year = {}
    for row in docs:
        rtype = (row.get('types') or {}).get('resourceTypeGeneral') or 'Unknown'
        by_type[rtype] += 1
        zc = row.get('jrc_zenodo_counts') or {}
        views = zc.get('views', 0)
        downloads = zc.get('downloads', 0)
        if zc:
            usage_dois += 1
            usage['Views'] += views
            usage['Downloads'] += downloads
            uniq['Unique views'] += zc.get('unique_views', 0)
            uniq['Unique downloads'] += zc.get('unique_downloads', 0)
        ccount = row.get('jrc_citation_count', 0) or 0
        if ccount:
            cited_dois += 1
            cite_total += ccount
            counts.append(ccount)
        for src, val in (row.get('jrc_citation_sources') or {}).items():
            cite_src[source_name.get(src, src.capitalize())] += val
        yname = (row.get('jrc_publishing_date') or '')[:4]
        if yname.isdigit():
            rec = by_year.setdefault(yname, {'dois': 0, 'views': 0,
                                             'downloads': 0, 'citations': 0})
            rec['dois'] += 1
            rec['views'] += views
            rec['downloads'] += downloads
            rec['citations'] += ccount
    if not total:
        msg = "No Zenodo DOIs were found"
        if year != 'All':
            msg += f" for publishing year {year}"
        html = year_pulldown('zenodo_stats') + "<br><br>" + render_warning(msg, 'warning')
        endpoint_access()
        return make_response(render_template('general.html', urlroot=request.url_root,
                                             title="Zenodo metrics", html=html,
                                             navbar=generate_navbar('DataCite')))
    # ----- headline stat cards -----
    conv = usage['Downloads'] / usage['Views'] if usage['Views'] else 0
    cards = stat_cards([("Zenodo DOIs", f"{total:,}"),
                        ("DOIs with usage data",
                         f"{usage_dois:,} ({usage_dois/total*100:,.1f}%)"),
                        ("Total views", f"{usage['Views']:,}"),
                        ("Total downloads", f"{usage['Downloads']:,}")],
                       div_id='zen-stats')
    cards += stat_cards([("Downloads / view", f"{conv*100:,.1f}%"),
                         ("DOIs cited", f"{cited_dois:,} ({cited_dois/total*100:,.1f}%)"),
                         ("Total citations", f"{cite_total:,}")],
                        div_id='zen-stats2')
    intro = "<div style='font-size:0.85em; color:#a8c4e0; max-width:620px; " \
            + "margin:-8px 0 16px 0'>Usage is the sum of per-record views/downloads " \
            + "across all versions. See <a href='/zenodo_groups'>Zenodo deposits</a> " \
            + "for the same DOIs collapsed by concept (version series).</div>"
    # ----- usage totals -----
    uhtml = "<h4>Usage totals</h4>"
    uhtml += render_table(['Metric', 'Count'],
                          [[k, f"{v:,}"] for k, v in
                           list(usage.items()) + list(uniq.items())],
                          table_id='zen-usage', css='tablesorter numberlast-scroll')
    uhtml += "<div style='font-size:0.85em; color:#a8c4e0; max-width:560px; margin:6px 0 0 0'>" \
             + "<b>Views</b> and <b>Downloads</b> count every access event, including " \
             + "repeat visits by the same user or bot. <b>Unique views</b> and " \
             + "<b>Unique downloads</b> count each IP address only once per record, " \
             + "giving a closer approximation of distinct users reached.</div>"
    # ----- deposits & usage by publishing year -----
    # A specific year is already the query filter, so a by-year breakdown would be
    # a single, redundant row/bar. Only build it for the all-years view.
    ydata = {'Year': [], 'DOIs': [], 'Downloads': []}
    yhtml = ''
    if year == 'All':
        ytrows = []
        for yname in sorted(by_year):
            rec = by_year[yname]
            ytrows.append([yname, f"{rec['dois']:,}", f"{rec['views']:,}",
                           f"{rec['downloads']:,}", f"{rec['citations']:,}"])
            ydata['Year'].append(yname)
            ydata['DOIs'].append(rec['dois'])
            ydata['Downloads'].append(rec['downloads'])
        yhtml = "<h4>Deposits &amp; usage by publishing year</h4>"
        yhtml += render_table(['Year', 'DOIs', 'Views', 'Downloads', 'Citations'],
                              ytrows, table_id='zen-years', css='tablesorter numberlast-scroll')
    # ----- resource type breakdown -----
    type_data = dict(by_type)
    thtml = "<h4>DOIs by resource type</h4>"
    thtml += render_table(['Resource type', 'DOIs'],
                          [[k, f"{v:,}"] for k, v in
                           sorted(by_type.items(), key=itemgetter(1), reverse=True)],
                          table_id='zen-types', css='tablesorter numberlast-scroll')
    # ----- citation sources (which source surfaces Zenodo citations) -----
    src_data = dict(cite_src)
    shtml = ''
    if cite_src:
        shtml = "<h4>Citations by source</h4>"
        shtml += render_table(['Source', 'Citations'],
                              [[k, f"{v:,}"] for k, v in
                               sorted(cite_src.items(), key=itemgetter(1), reverse=True)],
                              table_id='zen-sources', css='tablesorter numberlast-scroll')
        shtml += "<div style='font-size:0.85em; color:#a8c4e0; max-width:460px'>" \
                 + "A citing work found by more than one source is counted once " \
                 + "per source here.</div>"
    if request.args.get('fmt') == 'json':
        result = initialize_result()
        result['data'] = {"dois": total, "usage_dois": usage_dois,
                          "usage": {**usage, **uniq},
                          "downloads_per_view": round(conv, 4),
                          "cited_dois": cited_dois, "total_citations": cite_total,
                          "median_citations": statistics.median(counts) if counts else 0,
                          "by_type": type_data, "citations_by_source": src_data,
                          "by_year": by_year}
        result['rest']['source'] = 'mongo'
        result['rest']['row_count'] = total
        return generate_response(result)
    # ----- charts -----
    chartscript = ''
    usage_div = year_div = type_div = src_div = ''
    if usage['Views'] or usage['Downloads']:
        script, usage_div = DP.pie_chart(usage, "Usage totals", "metric", width=500,
                                         fmt="{0,0}")
        chartscript += script
    if ydata['Year']:
        # Tap a year bar -> Zenodo metrics scoped to that year
        ynav = {yr: f"/zenodo_stats/{yr}" for yr in ydata['Year']}
        script, year_div = DP.dual_axis_chart(
            ydata, title="Deposits & downloads by year", x_field='Year',
            bar_field='DOIs', line_field='Downloads', bar_label='DOIs deposited',
            line_label='Downloads', bar_color='darkorange', bar_format="0,0",
            line_format="0,0", width=650, height=400, nav=ynav)
        chartscript += script
    script, type_div = DP.hbar_chart(type_data, "DOIs by resource type",
                                     value_label="DOIs", width=500, height=320,
                                     value_format="0,0", show_values=True)
    chartscript += script
    if src_data:
        colors = DP.get_colors_by_count(len(src_data))
        script, src_div = DP.pie_chart(src_data, "Citations by source", "source",
                                       width=500, colors=colors, fmt="{0,0}")
        chartscript += script
    def flexrow(table_html, chart_div):
        ''' Pair a table and its chart side by side '''
        return "<div class='flexrow' style='margin-bottom: 40px'><div class='flexcol'>" \
               + table_html + "</div><div class='flexcol' style='margin: 10px 0 0 20px'>" \
               + chart_div + "</div></div>"
    title = "Zenodo metrics"
    if year != 'All':
        title += f" ({year})"
    html = year_pulldown('zenodo_stats') + "<br><br>" + cards + intro \
           + flexrow(uhtml, usage_div)
    if yhtml:
        html += flexrow(yhtml, year_div)
    html += flexrow(thtml, type_div)
    if shtml:
        html += flexrow(shtml, src_div)
    endpoint_access()
    return make_response(render_template('bokeh.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         chartscript=chartscript, chartdiv='',
                                         chartscript2='', chartdiv2='',
                                         navbar=generate_navbar('DataCite')))


# protocols.io DOIs are Crossref-registered (unlike figshare/Zenodo, which are
# DataCite-registered) with prefix 10.17504 (10.17504/protocols.io.<slug>[/v<n>]).
# Per-protocol usage (views/exports/runs/forks/bookmarks/comments) is stored in
# jrc_protocolsio_counts by the citation sync; /protocolsio_stats aggregates it
# site-wide.
PROTOCOLSIO_DOI_RE = r"^10\.17504/"
# protocols.io versions a DOI with a trailing /v<n> segment (e.g.
# 10.17504/protocols.io.xxx/v2) - unlike Zenodo/figshare, there is no
# relatedIdentifiers link to a concept DOI, so the concept is derived by
# stripping that suffix; an unversioned DOI (no suffix) groups under its own DOI.
PROTOCOLSIO_VERSION_RE = re.compile(r'^(.*)/v\d+$', re.IGNORECASE)


def protocolsio_match(year='All'):
    ''' Build the dois query that selects protocols.io DOIs (Crossref-registered,
        DOI prefix 10.17504). Optionally limited to a publishing year.
        Keyword arguments:
          year: 4-digit publishing year, or 'All'
        Returns:
          a MongoDB filter dict
    '''
    match = {"jrc_obtained_from": "Crossref",
             "doi": {"$regex": PROTOCOLSIO_DOI_RE, "$options": "i"}}
    if year != 'All':
        match["jrc_publishing_date"] = {"$regex": "^" + year}
    return match


def protocolsio_concept_doi(doi):
    ''' Return a protocols.io DOI's concept (base) identifier - the identifier
        shared by every version of one protocol (see PROTOCOLSIO_VERSION_RE).
        Keyword arguments:
          doi: DOI string
        Returns:
          The lower-cased concept DOI string
    '''
    match = PROTOCOLSIO_VERSION_RE.match(doi)
    return (match.group(1) if match else doi).strip().lower()


def protocolsio_concept_groups(records):
    ''' Group protocols.io records by concept (base) DOI, summing usage and
        citations across the concept's versions.
        Keyword arguments:
          records: list of dicts with doi/title/views/exports/citations/concept
        Returns:
          List of group dicts (concept, label, count, views, exports, citations)
          sorted by descending DOI count then citations. The label is the most
          common member title (versions of one protocol usually share a title).
    '''
    buckets = collections.defaultdict(list)
    for rec in records:
        buckets[rec['concept']].append(rec)
    groups = []
    for concept, recs in buckets.items():
        casings = collections.Counter(r['title'] for r in recs if r['title'])
        label = casings.most_common(1)[0][0] if casings else concept
        groups.append({'concept': concept, 'label': label, 'count': len(recs),
                       'views': sum(r['views'] for r in recs),
                       'exports': sum(r['exports'] for r in recs),
                       'citations': sum(r['citations'] for r in recs)})
    groups.sort(key=lambda g: (-g['count'], -g['citations']))
    return groups


@app.route('/protocolsio_stats/<string:year>')
@app.route('/protocolsio_stats')
def protocolsio_stats(year='All'):  # pylint: disable=too-many-locals,too-many-branches,too-many-statements
    ''' Show protocols.io usage and citation metrics.
        protocols.io DOIs are Crossref-registered (see protocolsio_match). Usage
        (views/exports/runs/forks/bookmarks/comments) comes from
        jrc_protocolsio_counts; citation totals/sources come from the citation
        sync. Unlike Zenodo/figshare, resource type and publisher are homogenous
        across protocols.io DOIs (all Crossref "posted-content"/"preprint"), so
        no breakdown by those dimensions is shown here.
    '''
    coll = DB['dis'].dois
    proj = {"doi": 1, "jrc_publishing_date": 1, "jrc_protocolsio_counts": 1,
            "jrc_citation_count": 1, "jrc_citation_sources": 1}
    try:
        docs = list(coll.find(protocolsio_match(year), proj))
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get protocols.io DOIs"),
                               message=error_message(err))
    source_name = {"crossref": "Crossref", "openalex": "OpenAlex",
                   "scholexplorer": "ScholeXplorer", "wos": "Web of Science"}
    total = len(docs)
    usage = {'Views': 0, 'Exports': 0, 'Runs': 0, 'Bookmarks': 0, 'Comments': 0,
             'Forks (public)': 0, 'Forks (private)': 0}
    usage_dois = cited_dois = cite_total = 0
    counts = []
    cite_src = collections.defaultdict(int)
    by_year = {}
    for row in docs:
        pc = row.get('jrc_protocolsio_counts') or {}
        views = pc.get('views', 0)
        exports = pc.get('exports', 0)
        if pc:
            usage_dois += 1
            usage['Views'] += views
            usage['Exports'] += exports
            usage['Runs'] += pc.get('runs', 0)
            usage['Bookmarks'] += pc.get('bookmarks', 0)
            usage['Comments'] += pc.get('comments', 0)
            usage['Forks (public)'] += pc.get('forks_public', 0)
            usage['Forks (private)'] += pc.get('forks_private', 0)
        ccount = row.get('jrc_citation_count', 0) or 0
        if ccount:
            cited_dois += 1
            cite_total += ccount
            counts.append(ccount)
        for src, val in (row.get('jrc_citation_sources') or {}).items():
            cite_src[source_name.get(src, src.capitalize())] += val
        yname = (row.get('jrc_publishing_date') or '')[:4]
        if yname.isdigit():
            rec = by_year.setdefault(yname, {'dois': 0, 'views': 0,
                                             'exports': 0, 'citations': 0})
            rec['dois'] += 1
            rec['views'] += views
            rec['exports'] += exports
            rec['citations'] += ccount
    if not total:
        msg = "No protocols.io DOIs were found"
        if year != 'All':
            msg += f" for publishing year {year}"
        html = year_pulldown('protocolsio_stats') + "<br><br>" + render_warning(msg, 'warning')
        endpoint_access()
        return make_response(render_template('general.html', urlroot=request.url_root,
                                             title="protocols.io metrics", html=html,
                                             navbar=generate_navbar('DataCite')))
    # ----- headline stat cards -----
    conv = usage['Exports'] / usage['Views'] if usage['Views'] else 0
    cards = stat_cards([("protocols.io DOIs", f"{total:,}"),
                        ("DOIs with usage data",
                         f"{usage_dois:,} ({usage_dois/total*100:,.1f}%)"),
                        ("Total views", f"{usage['Views']:,}"),
                        ("Total exports", f"{usage['Exports']:,}")],
                       div_id='pio-stats')
    cards += stat_cards([("Exports / view", f"{conv*100:,.1f}%"),
                         ("DOIs cited", f"{cited_dois:,} ({cited_dois/total*100:,.1f}%)"),
                         ("Total citations", f"{cite_total:,}")],
                        div_id='pio-stats2')
    intro = "<div style='font-size:0.85em; color:#a8c4e0; max-width:620px; " \
            + "margin:-8px 0 16px 0'>Usage is the sum of per-protocol counts from " \
            + "the protocols.io API. Versioned protocols (.../v2, .../v3, ...) are " \
            + "stored as separate DOIs and counted separately here. See " \
            + "<a href='/protocolsio_dois'>protocols.io deposits</a> for the same " \
            + "DOIs collapsed by concept (version series).</div>"
    # ----- usage totals -----
    uhtml = "<h4>Usage totals</h4>"
    uhtml += render_table(['Metric', 'Count'],
                          [[k, f"{v:,}"] for k, v in usage.items()],
                          table_id='pio-usage', css='tablesorter numberlast-scroll')
    uhtml += "<div style='font-size:0.85em; color:#a8c4e0; max-width:560px; margin:6px 0 0 0'>" \
             + "<b>Views</b> and <b>Exports</b> are the largest signals (page views and " \
             + "PDF/format exports); <b>Runs</b> tracks protocols.io's \"start experiment\" " \
             + "feature, and <b>Forks</b> counts derivative copies (public/private).</div>"
    # ----- DOIs & usage by publishing year -----
    # A specific year is already the query filter, so a by-year breakdown would be
    # a single, redundant row/bar. Only build it for the all-years view.
    ydata = {'Year': [], 'DOIs': [], 'Views': []}
    yhtml = ''
    if year == 'All':
        ytrows = []
        for yname in sorted(by_year):
            rec = by_year[yname]
            ytrows.append([yname, f"{rec['dois']:,}", f"{rec['views']:,}",
                           f"{rec['exports']:,}", f"{rec['citations']:,}"])
            ydata['Year'].append(yname)
            ydata['DOIs'].append(rec['dois'])
            ydata['Views'].append(rec['views'])
        yhtml = "<h4>DOIs &amp; usage by publishing year</h4>"
        yhtml += render_table(['Year', 'DOIs', 'Views', 'Exports', 'Citations'],
                              ytrows, table_id='pio-years', css='tablesorter numberlast-scroll')
    # ----- citation sources (which source surfaces protocols.io citations) -----
    src_data = dict(cite_src)
    shtml = ''
    if cite_src:
        shtml = "<h4>Citations by source</h4>"
        shtml += render_table(['Source', 'Citations'],
                              [[k, f"{v:,}"] for k, v in
                               sorted(cite_src.items(), key=itemgetter(1), reverse=True)],
                              table_id='pio-sources', css='tablesorter numberlast-scroll')
        shtml += "<div style='font-size:0.85em; color:#a8c4e0; max-width:460px'>" \
                 + "A citing work found by more than one source is counted once " \
                 + "per source here.</div>"
    if request.args.get('fmt') == 'json':
        result = initialize_result()
        result['data'] = {"dois": total, "usage_dois": usage_dois, "usage": usage,
                          "exports_per_view": round(conv, 4),
                          "cited_dois": cited_dois, "total_citations": cite_total,
                          "median_citations": statistics.median(counts) if counts else 0,
                          "citations_by_source": src_data, "by_year": by_year}
        result['rest']['source'] = 'mongo'
        result['rest']['row_count'] = total
        return generate_response(result)
    # ----- charts -----
    chartscript = ''
    usage_div = year_div = src_div = ''
    if any(usage.values()):
        script, usage_div = DP.hbar_chart(usage, "Usage totals", value_label="Count",
                                          width=500, height=320, value_format="0,0",
                                          show_values=True)
        chartscript += script
    if ydata['Year']:
        # Tap a year bar -> protocols.io metrics scoped to that year
        ynav = {yr: f"/protocolsio_stats/{yr}" for yr in ydata['Year']}
        script, year_div = DP.dual_axis_chart(
            ydata, title="DOIs & views by year", x_field='Year',
            bar_field='DOIs', line_field='Views', bar_label='DOIs published',
            line_label='Views', bar_color='darkorange', bar_format="0,0",
            line_format="0,0", width=650, height=400, nav=ynav)
        chartscript += script
    if src_data:
        colors = DP.get_colors_by_count(len(src_data))
        script, src_div = DP.pie_chart(src_data, "Citations by source", "source",
                                       width=500, colors=colors, fmt="{0,0}")
        chartscript += script
    def flexrow(table_html, chart_div):
        ''' Pair a table and its chart side by side '''
        return "<div class='flexrow' style='margin-bottom: 40px'><div class='flexcol'>" \
               + table_html + "</div><div class='flexcol' style='margin: 10px 0 0 20px'>" \
               + chart_div + "</div></div>"
    title = "protocols.io metrics"
    if year != 'All':
        title += f" ({year})"
    html = year_pulldown('protocolsio_stats') + "<br><br>" + cards + intro \
           + flexrow(uhtml, usage_div)
    if yhtml:
        html += flexrow(yhtml, year_div)
    if shtml:
        html += flexrow(shtml, src_div)
    endpoint_access()
    return make_response(render_template('bokeh.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         chartscript=chartscript, chartdiv='',
                                         chartscript2='', chartdiv2='',
                                         navbar=generate_navbar('DataCite')))


@app.route('/protocolsio_dois/<string:year>')
@app.route('/protocolsio_dois')
def protocolsio_dois(year='All'):  # pylint: disable=too-many-locals,too-many-branches,too-many-statements
    ''' Show protocols.io deposits ranked by usage and citations, each collapsing
        a protocol's versions (10.17504/protocols.io.xxx/v2, /v3, ...) under
        their shared concept (base) DOI (see protocolsio_concept_doi); an
        unversioned DOI stands alone. Unlike Zenodo, rankings cover every
        deposit (not just multi-version ones): most protocols.io deposits
        currently have only one version stored, so restricting to multi-version
        deposits would leave the "top 20" tables nearly empty.
    '''
    coll = DB['dis'].dois
    proj = {"doi": 1, "DOI": 1, "title": 1, "jrc_citation_count": 1,
            "jrc_protocolsio_counts": 1}
    try:
        docs = list(coll.find(protocolsio_match(year), proj))
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get protocols.io DOIs"),
                               message=error_message(err))
    records = []
    for row in docs:
        pc = row.get('jrc_protocolsio_counts') or {}
        records.append({'doi': row['doi'],
                        'title': strip_html_tags(strip_tex_math(DL.get_title(row))),
                        'views': pc.get('views', 0),
                        'exports': pc.get('exports', 0),
                        'citations': row.get('jrc_citation_count', 0) or 0,
                        'concept': protocolsio_concept_doi(row['doi'])})
    total = len(records)
    if not total:
        msg = "No protocols.io DOIs were found"
        if year != 'All':
            msg += f" for publishing year {year}"
        html = year_pulldown('protocolsio_dois') + "<br><br>" \
               + render_warning(msg, 'warning')
        endpoint_access()
        return make_response(render_template('general.html', urlroot=request.url_root,
                                             title="protocols.io deposits", html=html,
                                             navbar=generate_navbar('DataCite')))
    base = '/protocolsio_dois' if year == 'All' else f"/protocolsio_dois/{year}"
    # Drill-down: ?concept=<doi> lists the member (version) DOIs of one deposit
    concept = request.args.get('concept')
    if concept:
        members = [r for r in records if r['concept'] == concept.lower()]
        members.sort(key=itemgetter('exports', 'citations'), reverse=True)
        if request.args.get('fmt') == 'json':
            result = initialize_result()
            result['data'] = {"concept": concept, "dois": len(members),
                              "members": members}
            result['rest']['source'] = 'mongo'
            result['rest']['row_count'] = len(members)
            return generate_response(result)
        back = f"<a href='{base}' class='btn btn-outline-primary btn-sm'>" \
               + "&larr; all deposits</a>"
        if not members:
            html = back + "<br><br>" \
                   + render_warning(f"No protocols.io DOIs match the deposit "
                                    f"\"{escape(concept)}\"", 'warning')
            endpoint_access()
            return make_response(render_template('general.html', urlroot=request.url_root,
                                                 title=f"protocols.io deposit: {concept}",
                                                 html=html, navbar=generate_navbar('DataCite')))
        casings = collections.Counter(r['title'] for r in members if r['title'])
        dtitle = f"protocols.io deposit: {casings.most_common(1)[0][0] if casings else concept}"
        mrows = []
        tviews = texp = tcit = 0
        for rec in members:
            tviews += rec['views']
            texp += rec['exports']
            tcit += rec['citations']
            mrows.append([safe(doi_link(rec['doi'])), rec['title'], f"{rec['views']:,}",
                          f"{rec['exports']:,}", f"{rec['citations']:,}"])
        mtable = render_table(['Version DOI', 'Title', 'Views', 'Exports', 'Citations'],
                              mrows, table_id='pio-members',
                              css='tablesorter numberlast-scroll',
                              footer=[fcell('Total', colspan=2),
                                      fcell(f"{tviews:,}", align='center'),
                                      fcell(f"{texp:,}", align='center'),
                                      fcell(f"{tcit:,}", align='center')])
        mcards = stat_cards([("Versions", f"{len(members):,}"),
                             ("Total views", f"{tviews:,}"),
                             ("Total exports", f"{texp:,}"),
                             ("Total citations", f"{tcit:,}")], div_id='piom-stats')
        html = back + "<br><br>" + mcards + mtable
        endpoint_access()
        return make_response(render_template('general.html', urlroot=request.url_root,
                                             title=dtitle, html=html,
                                             navbar=generate_navbar('DataCite')))
    all_groups = protocolsio_concept_groups(records)
    multi = [g for g in all_groups if g['count'] > 1]
    grouped_dois = sum(g['count'] for g in multi)
    largest = max(multi, key=lambda g: g['count']) if multi else None
    top_n = 20
    by_exports = sorted(all_groups, key=lambda g: (-g['exports'], -g['views']))
    by_views = sorted(all_groups, key=lambda g: (-g['views'], -g['exports']))
    by_cit = sorted(all_groups, key=lambda g: (-g['citations'], -g['views']))
    total_views = sum(g['views'] for g in all_groups)
    total_exports = sum(g['exports'] for g in all_groups)
    total_citations = sum(g['citations'] for g in all_groups)
    if request.args.get('fmt') == 'json':
        result = initialize_result()
        result['data'] = {"dois": total, "deposits": len(all_groups),
                          "multi_version_deposits": len(multi), "grouped_dois": grouped_dois,
                          "largest_deposit": {"concept": largest['concept'],
                                              "label": largest['label'],
                                              "count": largest['count']} if largest else None,
                          "total_views": total_views, "total_exports": total_exports,
                          "total_citations": total_citations,
                          "deposit_groups": [{k: g[k] for k in
                                              ('concept', 'label', 'count', 'views',
                                               'exports', 'citations')}
                                             for g in by_exports]}
        result['rest']['source'] = 'mongo'
        result['rest']['row_count'] = len(all_groups)
        return generate_response(result)
    largest_card = (f"Most versions ({escape(largest['label'])})", f"{largest['count']:,}") \
                   if largest else ("Most versions", "N/A")
    cards = stat_cards(
        [("protocols.io DOIs", f"{total:,}"),
         ("Deposits (all)", f"{len(all_groups):,}"),
         ("Multi-version deposits", f"{len(multi):,}"),
         ("DOIs in multi-version deposits",
          f"{grouped_dois:,} ({grouped_dois/total*100:,.1f}%)"),
         largest_card],
        div_id='piog-stats')
    cards += stat_cards([("Total views", f"{total_views:,}"),
                         ("Total exports", f"{total_exports:,}"),
                         ("Total citations", f"{total_citations:,}")],
                        div_id='piog-stats2')
    gnote = "<div style='font-size:0.85em; color:#a8c4e0; max-width:620px; " \
            + "margin:-8px 0 16px 0'>Each deposit collapses every version of one " \
            + "protocol under the shared concept (base) DOI derived by stripping " \
            + "its trailing <i>/v&lt;n&gt;</i> segment; unversioned DOIs stand " \
            + "alone. Views, exports, and citations are summed across a deposit's " \
            + "versions. Unlike Zenodo, rankings below cover every deposit, not " \
            + "just multi-version ones, since most protocols.io deposits " \
            + "currently have only one version stored. Each label links to the " \
            + "deposit's version DOIs.</div>"
    # Each dimension's table+chart pair is shown only when at least one deposit
    # has a non-zero value for it (e.g. a year with views but no citations yet).
    chartscript = ''
    sections = ''
    for label, ordered, total_metric, value_key, table_id, unit in (
            ('exports', by_exports, total_exports, 'exports', 'pio-grp-exp', 'Exports'),
            ('views', by_views, total_views, 'views', 'pio-grp-views', 'Views'),
            ('citations', by_cit, total_citations, 'citations', 'pio-grp-cit', 'Citations')):
        if not total_metric:
            continue
        thtml = f"<h4>Deposits by {label} (top 20)</h4>" \
                + group_table(ordered, top_n, table_id,
                             ['Deposit', 'Versions', 'Views', 'Exports', 'Citations'],
                             base, 'concept', 'concept', 'label',
                             ('views', 'exports', 'citations'), 'deposits', False)
        data, nav = group_chart_data(ordered, value_key, base, 'label', 'concept', 'concept')
        chart_div = ''
        if data:
            script, chart_div = DP.hbar_chart(data, f"Top deposits by {label}",
                                              value_label=unit, width=620, height=420,
                                              value_format="0,0", show_values=True, nav=nav)
            chartscript += script
        sections += group_flexrow(thtml, chart_div)
    title = "protocols.io deposits"
    if year != 'All':
        title += f" ({year})"
    html = year_pulldown('protocolsio_dois') + "<br><br>" + cards + gnote + sections
    endpoint_access()
    return make_response(render_template('bokeh.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         chartscript=chartscript, chartdiv='',
                                         chartscript2='', chartdiv2='',
                                         navbar=generate_navbar('DataCite')))


# eLife DOIs are Crossref-registered with prefix 10.7554
# (10.7554/elife.<id>[.<n>[.sa<n>]]). Per-article usage (views/downloads) is
# stored in jrc_elife_counts by the citation sync; /elife_stats aggregates it
# site-wide. Peer-review sub-documents (decision letters, author responses)
# share their parent article's numeric ID and aren't usage-eligible - excluded
# via `type` at the query level (see elife_match), same as the citation sync.
# Nav lives under DOIs (not DataCite, unlike figshare/Zenodo/protocols.io),
# since eLife is a Crossref-registered journal, not a DataCite repository.
ELIFE_DOI_RE = r"^10\.7554/elife\."
# eLife's "Reviewed Preprint" model versions a DOI with a trailing
# .<n>[.sa<n>] segment (e.g. 10.7554/elife.94168.2); the numeric article ID
# (leading zeros stripped) doubles as the canonical concept key, matching the
# extraction logic already used by the citation sync (sync_citations.py).
ELIFE_CONCEPT_RE = re.compile(r'^10\.7554/elife\.0*(\d+)', re.IGNORECASE)


def elife_match(year='All'):
    ''' Build the dois query that selects eLife DOIs (Crossref-registered, DOI
        prefix 10.7554), excluding peer-review sub-documents (decision letters,
        author responses - not usage-eligible; see elife_concept_doi).
        Optionally limited to a publishing year.
        Keyword arguments:
          year: 4-digit publishing year, or 'All'
        Returns:
          a MongoDB filter dict
    '''
    match = {"jrc_obtained_from": "Crossref",
             "doi": {"$regex": ELIFE_DOI_RE, "$options": "i"},
             "type": {"$ne": "peer-review"}}
    if year != 'All':
        match["jrc_publishing_date"] = {"$regex": "^" + year}
    return match


def elife_concept_doi(doi):
    ''' Return an eLife DOI's concept (base) identifier - the identifier shared
        by every version of one article (see ELIFE_CONCEPT_RE). eLife's
        "Reviewed Preprint" model versions a DOI with a trailing .<n>[.sa<n>]
        segment; the canonical numeric article ID (leading zeros stripped)
        doubles as the concept key, matching the citation sync's extraction.
        Keyword arguments:
          doi: DOI string
        Returns:
          The lower-cased concept DOI string (10.7554/elife.<id>)
    '''
    match = ELIFE_CONCEPT_RE.match(doi)
    return f"10.7554/elife.{match.group(1)}" if match else doi.strip().lower()


def elife_concept_groups(records):
    ''' Group eLife records by concept (base) DOI, summing usage and citations
        across the concept's versions.
        Keyword arguments:
          records: list of dicts with doi/title/views/downloads/citations/concept
        Returns:
          List of group dicts (concept, label, count, views, downloads, citations)
          sorted by descending DOI count then citations. The label is the most
          common member title (versions of one article usually share a title).
    '''
    buckets = collections.defaultdict(list)
    for rec in records:
        buckets[rec['concept']].append(rec)
    groups = []
    for concept, recs in buckets.items():
        casings = collections.Counter(r['title'] for r in recs if r['title'])
        label = casings.most_common(1)[0][0] if casings else concept
        groups.append({'concept': concept, 'label': label, 'count': len(recs),
                       'views': sum(r['views'] for r in recs),
                       'downloads': sum(r['downloads'] for r in recs),
                       'citations': sum(r['citations'] for r in recs)})
    groups.sort(key=lambda g: (-g['count'], -g['citations']))
    return groups


def elife_type_label(doc_type, subtype):
    ''' Map an eLife DOI's Crossref type/subtype to a friendly display label.
        Unlike protocols.io (homogenous "posted-content"/"preprint"), eLife
        DOIs split meaningfully between traditional journal articles and the
        newer "Reviewed Preprint" model, so this breakdown is worth showing.
        Delegates the type/subtype classification itself to doi_common (used
        the same way everywhere else in this file) rather than reimplementing
        it with a local type/subtype check.
        Keyword arguments:
          doc_type: the DOI record's Crossref `type` field
          subtype: the DOI record's Crossref `subtype` field
        Returns:
          "Journal article", "Reviewed preprint", or the raw type as a fallback
    '''
    rec = {'type': doc_type, 'subtype': subtype}
    if DL.is_journal(rec):
        return 'Journal article'
    if DL.is_preprint(rec):
        return 'Reviewed preprint'
    return doc_type or 'Unknown'


@app.route('/elife_stats/<string:year>')
@app.route('/elife_stats')
def elife_stats(year='All'):  # pylint: disable=too-many-locals,too-many-branches,too-many-statements
    ''' Show eLife usage and citation metrics.
        eLife DOIs are Crossref-registered (see elife_match), which already
        excludes peer-review sub-documents since they aren't usage-eligible.
        Usage (views/downloads) comes from jrc_elife_counts; citation totals/
        sources come from the citation sync.
    '''
    coll = DB['dis'].dois
    proj = {"doi": 1, "type": 1, "subtype": 1, "jrc_publishing_date": 1,
            "jrc_elife_counts": 1, "jrc_citation_count": 1, "jrc_citation_sources": 1}
    try:
        docs = list(coll.find(elife_match(year), proj))
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get eLife DOIs"),
                               message=error_message(err))
    source_name = {"crossref": "Crossref", "openalex": "OpenAlex",
                   "scholexplorer": "ScholeXplorer", "wos": "Web of Science"}
    total = len(docs)
    usage = {'Views': 0, 'Downloads': 0}
    usage_dois = cited_dois = cite_total = 0
    counts = []
    by_type = collections.defaultdict(int)
    cite_src = collections.defaultdict(int)
    by_year = {}
    for row in docs:
        ec = row.get('jrc_elife_counts') or {}
        views = ec.get('views', 0)
        downloads = ec.get('downloads', 0)
        if ec:
            usage_dois += 1
            usage['Views'] += views
            usage['Downloads'] += downloads
        by_type[elife_type_label(row.get('type'), row.get('subtype'))] += 1
        ccount = row.get('jrc_citation_count', 0) or 0
        if ccount:
            cited_dois += 1
            cite_total += ccount
            counts.append(ccount)
        for src, val in (row.get('jrc_citation_sources') or {}).items():
            cite_src[source_name.get(src, src.capitalize())] += val
        yname = (row.get('jrc_publishing_date') or '')[:4]
        if yname.isdigit():
            rec = by_year.setdefault(yname, {'dois': 0, 'views': 0,
                                             'downloads': 0, 'citations': 0})
            rec['dois'] += 1
            rec['views'] += views
            rec['downloads'] += downloads
            rec['citations'] += ccount
    if not total:
        msg = "No eLife DOIs were found"
        if year != 'All':
            msg += f" for publishing year {year}"
        html = year_pulldown('elife_stats') + "<br><br>" + render_warning(msg, 'warning')
        endpoint_access()
        return make_response(render_template('general.html', urlroot=request.url_root,
                                             title="eLife metrics", html=html,
                                             navbar=generate_navbar('DOIs')))
    # ----- headline stat cards -----
    conv = usage['Downloads'] / usage['Views'] if usage['Views'] else 0
    cards = stat_cards([("eLife DOIs", f"{total:,}"),
                        ("DOIs with usage data",
                         f"{usage_dois:,} ({usage_dois/total*100:,.1f}%)"),
                        ("Total views", f"{usage['Views']:,}"),
                        ("Total downloads", f"{usage['Downloads']:,}")],
                       div_id='eli-stats')
    cards += stat_cards([("Downloads / view", f"{conv*100:,.1f}%"),
                         ("DOIs cited", f"{cited_dois:,} ({cited_dois/total*100:,.1f}%)"),
                         ("Total citations", f"{cite_total:,}")],
                        div_id='eli-stats2')
    intro = "<div style='font-size:0.85em; color:#a8c4e0; max-width:620px; " \
            + "margin:-8px 0 16px 0'>Usage is the sum of per-article counts from " \
            + "the public eLife metrics API. Peer-review sub-documents (decision " \
            + "letters, author responses) are excluded - they share their parent " \
            + "article's numeric ID and aren't usage-eligible. Versioned " \
            + "\"Reviewed Preprint\" DOIs are stored as separate DOIs and counted " \
            + "separately here. See <a href='/elife_dois'>eLife articles</a> for " \
            + "the same DOIs collapsed by concept (version series).</div>"
    # ----- usage totals -----
    uhtml = "<h4>Usage totals</h4>"
    uhtml += render_table(['Metric', 'Count'],
                          [[k, f"{v:,}"] for k, v in usage.items()],
                          table_id='eli-usage', css='tablesorter numberlast-scroll')
    # ----- DOIs & usage by publishing year -----
    # A specific year is already the query filter (elife_match), so every doc
    # shares that one year - a by-year breakdown would be a single, redundant
    # row/bar. Only build it for the all-years view.
    ydata = {'Year': [], 'DOIs': [], 'Downloads': []}
    yhtml = ''
    if year == 'All':
        ytrows = []
        for yname in sorted(by_year):
            rec = by_year[yname]
            ytrows.append([yname, f"{rec['dois']:,}", f"{rec['views']:,}",
                           f"{rec['downloads']:,}", f"{rec['citations']:,}"])
            ydata['Year'].append(yname)
            ydata['DOIs'].append(rec['dois'])
            ydata['Downloads'].append(rec['downloads'])
        yhtml = "<h4>DOIs &amp; usage by publishing year</h4>"
        yhtml += render_table(['Year', 'DOIs', 'Views', 'Downloads', 'Citations'],
                              ytrows, table_id='eli-years', css='tablesorter numberlast-scroll')
    # ----- DOIs by type (journal article vs. Reviewed Preprint) -----
    type_data = dict(by_type)
    thtml = "<h4>DOIs by type</h4>"
    thtml += render_table(['Type', 'DOIs'],
                          [[k, f"{v:,}"] for k, v in
                           sorted(by_type.items(), key=itemgetter(1), reverse=True)],
                          table_id='eli-types', css='tablesorter numberlast-scroll')
    # ----- citation sources (which source surfaces eLife citations) -----
    src_data = dict(cite_src)
    shtml = ''
    if cite_src:
        shtml = "<h4>Citations by source</h4>"
        shtml += render_table(['Source', 'Citations'],
                              [[k, f"{v:,}"] for k, v in
                               sorted(cite_src.items(), key=itemgetter(1), reverse=True)],
                              table_id='eli-sources', css='tablesorter numberlast-scroll')
        shtml += "<div style='font-size:0.85em; color:#a8c4e0; max-width:460px'>" \
                 + "A citing work found by more than one source is counted once " \
                 + "per source here.</div>"
    if request.args.get('fmt') == 'json':
        result = initialize_result()
        result['data'] = {"dois": total, "usage_dois": usage_dois, "usage": usage,
                          "downloads_per_view": round(conv, 4),
                          "cited_dois": cited_dois, "total_citations": cite_total,
                          "median_citations": statistics.median(counts) if counts else 0,
                          "by_type": type_data, "citations_by_source": src_data,
                          "by_year": by_year}
        result['rest']['source'] = 'mongo'
        result['rest']['row_count'] = total
        return generate_response(result)
    # ----- charts -----
    chartscript = ''
    usage_div = year_div = type_div = src_div = ''
    if usage['Views'] or usage['Downloads']:
        script, usage_div = DP.pie_chart(usage, "Usage totals", "metric", width=500,
                                         fmt="{0,0}")
        chartscript += script
    if ydata['Year']:
        # Tap a year bar -> eLife metrics scoped to that year
        ynav = {yr: f"/elife_stats/{yr}" for yr in ydata['Year']}
        script, year_div = DP.dual_axis_chart(
            ydata, title="DOIs & downloads by year", x_field='Year',
            bar_field='DOIs', line_field='Downloads', bar_label='DOIs published',
            line_label='Downloads', bar_color='darkorange', bar_format="0,0",
            line_format="0,0", width=650, height=400, nav=ynav)
        chartscript += script
    script, type_div = DP.hbar_chart(type_data, "DOIs by type", value_label="DOIs",
                                     width=500, height=280, value_format="0,0",
                                     show_values=True)
    chartscript += script
    if src_data:
        colors = DP.get_colors_by_count(len(src_data))
        script, src_div = DP.pie_chart(src_data, "Citations by source", "source",
                                       width=500, colors=colors, fmt="{0,0}")
        chartscript += script
    def flexrow(table_html, chart_div):
        ''' Pair a table and its chart side by side '''
        return "<div class='flexrow' style='margin-bottom: 40px'><div class='flexcol'>" \
               + table_html + "</div><div class='flexcol' style='margin: 10px 0 0 20px'>" \
               + chart_div + "</div></div>"
    title = "eLife metrics"
    if year != 'All':
        title += f" ({year})"
    html = year_pulldown('elife_stats') + "<br><br>" + cards + intro + flexrow(uhtml, usage_div)
    if yhtml:
        html += flexrow(yhtml, year_div)
    html += flexrow(thtml, type_div)
    if shtml:
        html += flexrow(shtml, src_div)
    endpoint_access()
    return make_response(render_template('bokeh.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         chartscript=chartscript, chartdiv='',
                                         chartscript2='', chartdiv2='',
                                         navbar=generate_navbar('DOIs')))


@app.route('/elife_dois/<string:year>')
@app.route('/elife_dois')
def elife_dois(year='All'):  # pylint: disable=too-many-locals,too-many-branches,too-many-statements
    ''' Show eLife articles ranked by usage and citations, each collapsing an
        article's versions (10.7554/elife.NNNNN.1, .2, ...) under their shared
        concept (base) DOI (see elife_concept_doi); an unversioned DOI stands
        alone. Like protocols.io, rankings cover every article (not just
        multi-version ones), so a highly-cited single-version article isn't
        hidden from the "top 20" tables.
    '''
    coll = DB['dis'].dois
    proj = {"doi": 1, "DOI": 1, "title": 1, "jrc_citation_count": 1, "jrc_elife_counts": 1}
    try:
        docs = list(coll.find(elife_match(year), proj))
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get eLife DOIs"),
                               message=error_message(err))
    records = []
    for row in docs:
        ec = row.get('jrc_elife_counts') or {}
        records.append({'doi': row['doi'],
                        'title': strip_html_tags(strip_tex_math(DL.get_title(row))),
                        'views': ec.get('views', 0),
                        'downloads': ec.get('downloads', 0),
                        'citations': row.get('jrc_citation_count', 0) or 0,
                        'concept': elife_concept_doi(row['doi'])})
    total = len(records)
    if not total:
        msg = "No eLife DOIs were found"
        if year != 'All':
            msg += f" for publishing year {year}"
        html = year_pulldown('elife_dois') + "<br><br>" \
               + render_warning(msg, 'warning')
        endpoint_access()
        return make_response(render_template('general.html', urlroot=request.url_root,
                                             title="eLife articles", html=html,
                                             navbar=generate_navbar('DOIs')))
    base = '/elife_dois' if year == 'All' else f"/elife_dois/{year}"
    # Drill-down: ?concept=<doi> lists the member (version) DOIs of one article
    concept = request.args.get('concept')
    if concept:
        members = [r for r in records if r['concept'] == concept.lower()]
        members.sort(key=itemgetter('downloads', 'citations'), reverse=True)
        if request.args.get('fmt') == 'json':
            result = initialize_result()
            result['data'] = {"concept": concept, "dois": len(members),
                              "members": members}
            result['rest']['source'] = 'mongo'
            result['rest']['row_count'] = len(members)
            return generate_response(result)
        back = f"<a href='{base}' class='btn btn-outline-primary btn-sm'>" \
               + "&larr; all articles</a>"
        if not members:
            html = back + "<br><br>" \
                   + render_warning(f"No eLife DOIs match the article "
                                    f"\"{escape(concept)}\"", 'warning')
            endpoint_access()
            return make_response(render_template('general.html', urlroot=request.url_root,
                                                 title=f"eLife article: {concept}",
                                                 html=html, navbar=generate_navbar('DOIs')))
        casings = collections.Counter(r['title'] for r in members if r['title'])
        dtitle = f"eLife article: {casings.most_common(1)[0][0] if casings else concept}"
        mrows = []
        tviews = tdl = tcit = 0
        for rec in members:
            tviews += rec['views']
            tdl += rec['downloads']
            tcit += rec['citations']
            mrows.append([safe(doi_link(rec['doi'])), rec['title'], f"{rec['views']:,}",
                          f"{rec['downloads']:,}", f"{rec['citations']:,}"])
        mtable = render_table(['Version DOI', 'Title', 'Views', 'Downloads', 'Citations'],
                              mrows, table_id='eli-members',
                              css='tablesorter numberlast-scroll',
                              footer=[fcell('Total', colspan=2),
                                      fcell(f"{tviews:,}", align='center'),
                                      fcell(f"{tdl:,}", align='center'),
                                      fcell(f"{tcit:,}", align='center')])
        mcards = stat_cards([("Versions", f"{len(members):,}"),
                             ("Total views", f"{tviews:,}"),
                             ("Total downloads", f"{tdl:,}"),
                             ("Total citations", f"{tcit:,}")], div_id='elim-stats')
        html = back + "<br><br>" + mcards + mtable
        endpoint_access()
        return make_response(render_template('general.html', urlroot=request.url_root,
                                             title=dtitle, html=html,
                                             navbar=generate_navbar('DOIs')))
    all_groups = elife_concept_groups(records)
    multi = [g for g in all_groups if g['count'] > 1]
    grouped_dois = sum(g['count'] for g in multi)
    largest = max(multi, key=lambda g: g['count']) if multi else None
    top_n = 20
    by_downloads = sorted(all_groups, key=lambda g: (-g['downloads'], -g['views']))
    by_views = sorted(all_groups, key=lambda g: (-g['views'], -g['downloads']))
    by_cit = sorted(all_groups, key=lambda g: (-g['citations'], -g['views']))
    total_views = sum(g['views'] for g in all_groups)
    total_downloads = sum(g['downloads'] for g in all_groups)
    total_citations = sum(g['citations'] for g in all_groups)
    if request.args.get('fmt') == 'json':
        result = initialize_result()
        result['data'] = {"dois": total, "articles": len(all_groups),
                          "multi_version_articles": len(multi), "grouped_dois": grouped_dois,
                          "largest_article": {"concept": largest['concept'],
                                              "label": largest['label'],
                                              "count": largest['count']} if largest else None,
                          "total_views": total_views, "total_downloads": total_downloads,
                          "total_citations": total_citations,
                          "article_groups": [{k: g[k] for k in
                                              ('concept', 'label', 'count', 'views',
                                               'downloads', 'citations')}
                                             for g in by_downloads]}
        result['rest']['source'] = 'mongo'
        result['rest']['row_count'] = len(all_groups)
        return generate_response(result)
    largest_card = (f"Most versions ({escape(largest['label'])})", f"{largest['count']:,}") \
                   if largest else ("Most versions", "N/A")
    cards = stat_cards(
        [("eLife DOIs", f"{total:,}"),
         ("Articles (all)", f"{len(all_groups):,}"),
         ("Multi-version articles", f"{len(multi):,}"),
         ("DOIs in multi-version articles",
          f"{grouped_dois:,} ({grouped_dois/total*100:,.1f}%)"),
         largest_card],
        div_id='elig-stats')
    cards += stat_cards([("Total views", f"{total_views:,}"),
                         ("Total downloads", f"{total_downloads:,}"),
                         ("Total citations", f"{total_citations:,}")],
                        div_id='elig-stats2')
    gnote = "<div style='font-size:0.85em; color:#a8c4e0; max-width:620px; " \
            + "margin:-8px 0 16px 0'>Each article collapses every version - " \
            + "the eLife \"Reviewed Preprint\" model versions a DOI as " \
            + "<i>.1</i>, <i>.2</i>, ... - under a shared concept (base) DOI; " \
            + "unversioned DOIs stand alone. Views, downloads, and citations " \
            + "are summed across an article's versions. Rankings below cover " \
            + "every article, not just multi-version ones. Each label links " \
            + "to the article's version DOIs.</div>"
    # Each dimension's table+chart pair is shown only when at least one article
    # has a non-zero value for it (e.g. a year with views but no citations yet).
    chartscript = ''
    sections = ''
    for label, ordered, total_metric, value_key, table_id, unit in (
            ('downloads', by_downloads, total_downloads, 'downloads', 'eli-grp-dl', 'Downloads'),
            ('views', by_views, total_views, 'views', 'eli-grp-views', 'Views'),
            ('citations', by_cit, total_citations, 'citations', 'eli-grp-cit', 'Citations')):
        if not total_metric:
            continue
        thtml = f"<h4>Articles by {label} (top 20)</h4>" \
                + group_table(ordered, top_n, table_id,
                             ['Article', 'Versions', 'Views', 'Downloads', 'Citations'],
                             base, 'concept', 'concept', 'label',
                             ('views', 'downloads', 'citations'), 'articles', False)
        data, nav = group_chart_data(ordered, value_key, base, 'label', 'concept', 'concept')
        chart_div = ''
        if data:
            script, chart_div = DP.hbar_chart(data, f"Top articles by {label}",
                                              value_label=unit, width=620, height=420,
                                              value_format="0,0", show_values=True, nav=nav)
            chartscript += script
        sections += group_flexrow(thtml, chart_div)
    title = "eLife articles"
    if year != 'All':
        title += f" ({year})"
    html = year_pulldown('elife_dois') + "<br><br>" + cards + gnote + sections
    endpoint_access()
    return make_response(render_template('bokeh.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         chartscript=chartscript, chartdiv='',
                                         chartscript2='', chartdiv2='',
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
    html = stat_cards(
        [("All authors", f"{source['Crossref-all'] + source['DataCite-all']:,}"),
         ("Any Janelia author", f"{source['Crossref-jrc'] + source['DataCite-jrc']:,}"),
         ("First and/or last", f"{source['Crossref'] + source['DataCite']:,}")],
        div_id='author-stats')
    def _acell(num):
        return cell(f"{num:,}", sort=num)
    arows = [["All authors", _acell(source['Crossref-all']), _acell(source['DataCite-all'])],
             ["Any Janelia author", _acell(source['Crossref-jrc']),
              _acell(source['DataCite-jrc'])],
             ["First and/or last", _acell(source['Crossref']), _acell(source['DataCite'])],
             ["Additional only", _acell(source['Crossref-jrc'] - source['Crossref']),
              _acell(source['DataCite-jrc'] - source['DataCite'])]]
    html += render_table(['Authorship', 'Crossref', 'DataCite'], arows, table_id='authors',
                         css='tablesorter numbers-scroll') + "<br>" \
            + year_pulldown('dois_author')
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
    ftrows = [[row['_id'], f"{row['count']:,}"] for row in get_top_authors('first', year)]
    first = "<h2>Top first authors</h2>" \
            + render_table(['Author', 'DOIs'], ftrows, table_id='topauthors_first',
                           css='tablesorter numbers-scroll')
    ltrows = [[row['_id'], f"{row['count']:,}"] for row in get_top_authors('last', year)]
    last = "<h2>Top last authors</h2>" \
           + render_table(['Author', 'DOIs'], ltrows, table_id='topauthors_last',
                          css='tablesorter numbers-scroll')
    html = "<div class='flexrow'><div class='flexcol'>" + first + "</div>" \
           + "<div class='flexcol' style='margin-left: 40px;'>" + last + "</div></div>" \
           + "<br>" + year_pulldown('dois_top_author')
    title = "Top first and last authors"
    if year != 'All':
        title += f" ({year})"
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         navbar=generate_navbar('Authorship')))


@app.route('/doiui_firstlast/<string:year>/<string:which>')
@app.route('/doiui_firstlast/<string:year>')
@app.route('/doiui_firstlast')
def doiui_firstlast(year='All', which=None):
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
        html, _, _ = standard_doi_table(display_rows)
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
    frows = [["Lab head first author",
              cell(safe(f"<a href='/doiui_firstlast/{year}/first'>{cnt['first']:,}</a>"),
                   sort=cnt['first'])],
             ["Lab head last author",
              cell(safe(f"<a href='/doiui_firstlast/{year}/last'>{cnt['last']:,}</a>"),
                   sort=cnt['last'])]]
    html = render_table(['Authorship', 'DOIs'], frows, table_id='group',
                        css='tablesorter numbers-scroll') + "<br>" \
           + year_pulldown('doiui_firstlast')
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


@app.route('/dois_invalid_auth')
def dois_invalid_auth():
    ''' Show DOIs with invalid authors
    '''
    payload = {"$or": [{"author.given": {"$exists": True}, "author.family": {"$exists": False}},
                       {"author.given": {"$exists": False}, "author.family": {"$exists": True}},
                       {"author.given": {"$exists": False}, "author.family": {"$exists": False},
                        "author.name": {"$exists": False}, "author": {"$exists": True}},
                       {"creators.givenName": {"$exists": True},
                        "creators.familyName": {"$exists": False}},
                       {"creators.givenName": {"$exists": False},
                        "creators.familyName": {"$exists": True}},
                       {"creators.givenName": {"$exists": False},
                        "creators.familyName": {"$exists": False},
                        "creators.name": {"$exists": False},
                        "creators": {"$exists": True}}]}
    try:
        rows = DB['dis'].dois.find(payload, {"doi": 1, "author": 1,
                                             "creators": 1, "jrc_author": 1}).sort([("doi", 1)])
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get DOIs with invalid authors " \
                                                    + "from dois collection"),
                               message=error_message(err))
    title = "DOIs with invalid authors"
    fam = "<span style='color: red;'>Family:</span> "
    giv = "<span style='color: red;'>Given:</span> "
    trows = []
    row_classes = []
    cnt = 0
    for row in rows:
        cnt += 1
        names = []
        if row.get('author'):
            for auth in row['author']:
                if auth.get('family') and auth.get('given'):
                    continue
                if auth.get('given'):
                    names.append(f"{giv}{auth.get('given', '')}")
                if auth.get('family'):
                    names.append(f"{fam}{auth.get('family', '')}")
        elif row.get('creators'):
            for auth in row['creators']:
                if auth.get('givenName') and auth.get('familyName'):
                    continue
                if auth.get('givenName'):
                    names.append(f"{giv}{auth.get('givenName', '')}")
                if auth.get('familyName'):
                    names.append(f"{fam}{auth.get('familyName', '')}")
        row_classes.append('tagged' if row.get('jrc_author') else 'not')
        trows.append([safe(doi_link(row['doi'])), safe(', '.join(names))])
    html = render_table(['DOI', 'Name'], trows, table_id='invalid_auth',
                        css='tablesorter standard-scroll', row_classes=row_classes)
    cbutton = "<button class=\"btn btn-outline-warning\" " \
              + "onclick=\"toggler('invalid_auth', 'tagged', 'totalrows');\">" \
              + "Filter for DOIs with tagged authors</button>"
    html = cbutton + html
    title += f" (<span id='totalrows'>{cnt:,}</span>)"
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=title, html=html,
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
    intro = "These are journal articles/preprints with no Janelia authors<br>"
    title = "DOIs without Janelia authors"
    if year != 'All':
        title += f" for {year}"
    if not cnt:
        html = intro + year_pulldown('dois_no_janelia') + "<br><br>" \
               + render_warning(f"No DOIs without Janelia authors were found for {year}.",
                                'warning')
    else:
        # Standard DOI-list table (adds Journal plus the version-filter/download/count
        # conventions); keep this page's Publisher as a trailing extra column.
        table_html, _, _ = standard_doi_table(rows, prefix='dois_no_janelia', count_card=True,
                                              extra_headers=['Publisher'],
                                              extra_fn=lambda row: [row.get('publisher', '')])
        html = intro + table_html
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         navbar=generate_navbar('Authorship')))


@app.route('/coauth')
def show_coauth():
    ''' Coauthor report input
    '''
    ppl = '<option>'
    try:
        rows = DB['dis'].orcid.find({}).collation({"locale": "en"}).sort("family", 1)
    except Exception as err:
        return render_template('warning.html', urlroot=request.url_root,
                                title=render_warning("Could not find people", 'error'),
                                message=error_message(err))
    for row in rows:
        ppl += f"<option>{row['given'][0]}&nbsp;{row['family'][0]}</option>"
    ppl += '</option>'
    endpoint_access()
    return make_response(render_template('coauth.html', urlroot=request.url_root,
                                         people=ppl,
                                         navbar=generate_navbar('Home')))


@app.route('/dois_coauthors', methods=['OPTIONS', 'POST'])
def dois_coauthors():
    '''
    Return DOIs co-authored by two ORCIDs or employee IDs
    '''
    ipd = receive_payload()
    if ipd.get('orcid1') and ipd.get('orcid2'):
        inputs = [ipd['orcid1'], ipd['orcid2']]
        lookup = 'orcid'
    elif ipd.get('eid1') and ipd.get('eid2'):
        inputs = [ipd['eid1'], ipd['eid2']]
        lookup = 'employeeId'
    elif ipd.get('name1') and ipd.get('name2'):
        inputs = [ipd['name1'], ipd['name2']]
        lookup = 'name'
    else:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Invalid input"),
                               message="Invalid input")
    orc = []
    for oid in inputs:
        try:
            if lookup == 'name':
                if '\xa0' not in oid:
                    return render_template('error.html', urlroot=request.url_root,
                                   title=render_warning("Invalid input for name"),
                                   message=f"Invalid input: {oid}")
                given, family = oid.split('\xa0')
                row = DB['dis'].orcid.find_one({"given": given, "family": family})
            else:
                row = DL.single_orcid_lookup(oid, DB['dis'].orcid, lookup_by=lookup)
            orc.append(row)
        except Exception as err:
            return render_template('error.html', urlroot=request.url_root,
                                   title=render_warning("Could not get ORCID data for " \
                                                        + oid),
                                   message=error_message(err))
        if not row:
            return render_template('error.html', urlroot=request.url_root,
                                   title=render_warning("ORCID record not found for " \
                                                        + oid),
                                   message="ORCID record not found")
    payload = [{"$match": {"jrc_author": orc[0]['employeeId']}},
               {"$match": {"jrc_author": orc[1]['employeeId']}}
              ]
    try:
        rows = DB['dis'].dois.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get works by co-authors"),
                               message=error_message(err))
    html, cnt, _ = standard_doi_table(rows, count_card=True)
    title = f"Works co-authored by {orc[0]['given'][0]} {orc[0]['family'][0]} " \
            + f"and {orc[1]['given'][0]} {orc[1]['family'][0]}"
    if not cnt:
        return render_template('warning.html', urlroot=request.url_root,
                               title=render_warning("Could not find DOIs", 'warning'),
                               message="Could not find any DOIs with co-authors " \
                                       + f"{orc[0]['given'][0]} {orc[0]['family'][0]} " \
                                       + f"and {orc[1]['given'][0]} {orc[1]['family'][0]}")
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         navbar=generate_navbar('Authorship')))

# ******************************************************************************
# * UI endpoints (Organizations)                                               *
# ******************************************************************************

@app.route('/org_authors/<string:org_in>')
def show_org_authors(org_in):
    '''
    Return authors for an organization
    '''
    try:
        row = DB['dis'].org_group.find_one({"group": org_in})
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get organization groups"),
                               message=error_message(err))
    if row:
        orgs = row['members']
    else:
        orgs = [org_in]
    payload = {"$or": [{"managed": {"$in": orgs}}, {"affiliations": {"$in": orgs}}]}
    try:
        rows = DB['dis'].orcid.find(payload).collation({"locale": "en"}).sort("family", 1)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get authors from orcid collection"),
                               message=error_message(err))
    table, cnt = generate_user_table(rows, download=org_in.replace(' ', '_'))
    html = f"<p>Number of authors: <span id='totalrowsa'>{cnt:,}</span></p>" + table
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=f"Authors for {org_in}", html=html,
                                         navbar=generate_navbar('Authorship')))


@app.route('/org_detail/<string:org_in>/<string:year>/<string:show>')
@app.route('/org_detail/<string:org_in>/<string:year>')
@app.route('/org_detail/<string:org_in>')
def show_organization(org_in, year=None, show="full"):
    '''
    Return DOIs for an organization
    '''
    if year is None:
        year = str(datetime.now().year)
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
    trows = []
    dcnt = org_journal_cnt = 0
    content = ""
    for row in rows:
        if DL.is_journal(row) and not DL.is_version(row):
            org_journal_cnt += 1
        dcnt += 1
        title = DL.get_title(row)
        if not title:
            title = ""
        tags = []
        for tag in row['jrc_tag']:
            if tag['name'] in orgs:
                tags.append(tag['name'])
        trows.append([row['jrc_publishing_date'], safe(doi_link(row['doi'])),
                      ', '.join(sorted(tags)), render_title_html(title)])
        authors = DL.get_author_list(row)
        content += f"{row['jrc_publishing_date']}\t{row['doi']}\t" \
                   + f"{', '.join(sorted(tags))}\t{title}\t{authors}\n"
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
    html = render_table(['Published', 'DOI', 'Tags', 'Title'], trows,
                        table_id='dois', css='tablesorter standard-scroll')
    if not dcnt:
        html = year_pulldown(f"org_detail/{org_in}") + subtitle \
               + render_warning(f"No DOIs were found for {org_in}.", 'warning') \
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
                                         navbar=generate_navbar('Authorship')))


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
        html, _, _ = standard_doi_table(display_rows)
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
    c1 = f"<a href='/org_summary/all/{year}/first'>{len(finds['first']):,}</a>" \
        if finds['first'] else ""
    c2 = f"<a href='/org_summary/{org}/{year}/first'>{len(finds['firstsr']):,}</a>" \
         if finds['firstsr'] else ""
    row1 = ['Lab head first author', safe(c1), safe(c2)]
    c1 = f"<a href='/org_summary/all/{year}/last'>{len(finds['last']):,}</a>" \
         if finds['last'] else ""
    c2 = f"<a href='/org_summary/{org}/{year}/last'>{len(finds['lastsr']):,}</a>" \
         if finds['lastsr'] else ""
    row2 = ['Lab head last author', safe(c1), safe(c2)]
    html = render_table(['', 'All', org], [row1, row2], table_id='org',
                        css='tablesorter numbers-scroll') \
           + "<br>" + year_pulldown(f"org_summary/{org}")
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
    total = {'Janelia': 0, org: 0}
    trows = []
    for yr in data['years']:
        total['Janelia'] += years['Janelia'][yr]
        total[org] += years[org][yr]
        c1 = f"<a href='/org_summary/all/{yr}/last'>{years['Janelia'][yr]}</a>"
        c2 = f"<a href='/org_summary/{org}/{yr}/last'>{years[org][yr]}</a>"
        trows.append([yr, safe(c1), safe(c2)])
    c1 = f"<a href='/org_summary/all/All/last'>{total['Janelia']}</a>"
    c2 = f"<a href='/org_summary/{org}/All/last'>{total[org]}</a>"
    html = render_table(['Year', 'All', org], trows, table_id='years',
                        css='tablesorter numbers-scroll',
                        footer=[fcell('Total', header=False), fcell(safe(c1), header=False),
                                fcell(safe(c2), header=False)]) + "<br>"
    data[f"With {org} authors"] = data.pop(org)
    data[f"No {org} authors"] = data.pop("Janelia")
    # Tap a year bar -> all Janelia last-author pubs that year (mirrors the "All" cell)
    nav = {yr: f"/org_summary/all/{yr}/last" for yr in data['years']}
    chartscript, chartdiv = DP.stacked_bar_chart(data, title, xaxis="years",
                                                 yaxis=(f"No {org} authors", f"With {org} authors"),
                                                 colors=DP.SOURCE_PALETTE, nav=nav)
    endpoint_access()
    return make_response(render_template('bokeh.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         chartscript=chartscript, chartdiv=chartdiv,
                                         navbar=generate_navbar('Authorship')))

# ******************************************************************************
# * UI endpoints (Preprints)                                                  *
# ******************************************************************************

@app.route('/dois_preprint/<string:year>')
@app.route('/dois_preprint')
def dois_preprint(year='All'):
    ''' Show preprints
    '''
    source = {}
    for src in SOURCES:
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
    trows = [
        ['Preprints with journal articles', f"{preprint['journal-article']:,}",
         f"{preprint['DataCite']}"],
        ['Journal articles with preprints', f"{preprint['posted-content']:,}", "0"],
        ['Journals without preprints', f"{no_relation['Crossref']['journal']:,}",
         f"{no_relation['DataCite']['journal']:,}"],
        ['Preprints without journals', f"{no_relation['Crossref']['preprint']:,}",
         f"{no_relation['DataCite']['preprint']:,}"],
    ]
    html = render_table(['Status', 'Crossref', 'DataCite'], trows, table_id='preprints',
                        css='tablesorter numbers-scroll') + "<br>" + year_pulldown('dois_preprint')
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
    jrn = pre = 0
    trows = []
    for idx in range(len(data['years'])):
        trows.append([data['years'][idx], f"{data['Journal article'][idx]:,}",
                      f"{data['Preprint'][idx]:,}"])
        jrn += data['Journal article'][idx]
        pre += data['Preprint'][idx]
    html = render_table(['Year', 'Journal articles', 'Preprints'], trows, table_id='years',
                        css='tablesorter numbers-scroll',
                        footer=[fcell('Total'), fcell(f"{jrn:,}", align='center'),
                                fcell(f"{pre:,}", align='center')])
    # Tap a year bar -> the DOIs published that year
    ynav = {yr: {"field": "publishing_year", "value": yr} for yr in data['years']}
    chartscript, chartdiv = DP.stacked_bar_chart(data, "DOIs published by year/preprint status",
                                                 xaxis="years",
                                                 yaxis=('Journal article', 'Preprint'),
                                                 colors=DP.SOURCE_PALETTE, nav=ynav)
    endpoint_access()
    return make_response(render_template('bokeh.html', urlroot=request.url_root,
                                         title="DOIs preprint status by year", html=html,
                                         chartscript=chartscript, chartdiv=chartdiv,
                                         navbar=generate_navbar('Preprints')))


@app.route('/preprint_with_pub')
@app.route('/preprint_with_pub/<string:year>')
def preprint_with_pub(year=None):
    ''' Show preprints with publications
        Keyword arguments:
          year: preprint publishing year to filter by; defaults to the current
                year, or "All" for no year filter
    '''
    payload = {"subtype": "preprint", "jrc_preprint": {"$exists": 1}}
    if year is None:
        year = str(datetime.now().year)
    if year != 'All':
        payload['jrc_publishing_date'] = {"$regex": "^" + year}
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
    trows = []
    for row in rows:
        if len(row['jrc_preprint']) == 1:
            prep = row['jrc_preprint'][0]
        else:
            prep = None
            for pdoi in row['jrc_preprint']:
                prow = DL.get_doi_record(pdoi, coll=coll)
                if not prow:
                    continue
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
        if 'jrc_journal' not in row:
            row['jrc_journal'] = ""
        if row['jrc_journal'] not in day_pub:
            day_pub[row['jrc_journal']] = []
        day_pub[row['jrc_journal']].append(days)
        fileoutput+= "\t".join([row['jrc_publishing_date'], row['doi'], DL.get_title(row),
                                row['jrc_journal']]) + "\n"
        trows.append([row['jrc_publishing_date'], safe(doi_link(row['doi'])),
                      DL.get_title(row), row['jrc_journal']])
    html = render_table(header, trows, table_id='preprint_with_pub',
                        css='tablesorter numbers-scroll')
    avg_days = sum(day_count) / len(day_count) if day_count else 0
    cards = [("Preprints matched", f"{len(day_count):,}"),
             ("Avg. days to publication", f"{avg_days:,.1f}")]
    if day_count:
        fastest = f"{min(day_count):,} days"
        if min(day_count) < 0:
            fastest = f"<a href='/preprint_date_errors'>{fastest}</a>"
        cards.append(("Fastest", fastest))
        cards.append(("Slowest", f"{max(day_count):,} days"))
    pre = stat_cards(cards, div_id='preprint-stats')
    ptrows = []
    for jour, days in day_pub.items():
        avg_days = sum(days) / len(days) if days else 0
        ptrows.append([jour, f"{avg_days:,.1f}"])
    pre += render_table(['Journal', 'Average days to publication'], ptrows,
                        table_id='preprint_with_pub', css='tablesorter numbers-scroll') \
           + create_downloadable('preprint_with_pub', header, fileoutput)
    # Defaults to the current year (not "All"), so pass the button value explicitly.
    selected = "(all years)" if year == 'All' else year
    html = f"{year_pulldown('preprint_with_pub', selected=selected)}<br><br>" + pre + html
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title="Preprints with journal publications", html=html,
                                         navbar=generate_navbar('Preprints')))


@app.route('/preprint_relation/<string:relation_type>')
@app.route('/preprint_relation/<string:relation_type>/<string:year>')
def show_preprint_relation(relation_type, year=None):
    ''' Show preprints without publications or publications without preprints
        Keyword arguments:
          relation_type: "preprint_no_pub" or "pub_no_preprint"
          year: publishing year to filter by; defaults to the current year, or
                "All" for no year filter
    '''
    relation_config = {
        'preprint_no_pub': {'payload': {"subtype": "preprint", "jrc_preprint": {"$exists": 0}},
                            'title': "Preprints without journal publications"},
        'pub_no_preprint': {'payload': {"type": "journal-article", "jrc_preprint": {"$exists": 0}},
                            'title': "Journal publications without preprints"},
    }
    if relation_type not in relation_config:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Invalid relation type"),
                               message="relation_type must be one of: " \
                                       + f"{', '.join(relation_config)}")
    cfg = relation_config[relation_type]
    payload = dict(cfg['payload'])
    # Both relation types offer a publishing-year pulldown that defaults to the
    # current year (pick "All" for no year filter).
    if year is None:
        year = str(datetime.now().year)
    if year != 'All':
        payload['jrc_publishing_date'] = {"$regex": "^" + year}
    try:
        rows = DB['dis'].dois.find(payload).sort([("jrc_publishing_date", -1)])
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get preprint data from dois"),
                               message=error_message(err))
    fileoutput = ""
    header = ['Published', 'DOI', 'Title', 'Journal']
    trows = []
    cnt = 0
    for row in rows:
        cnt += 1
        ptitle = render_title_html(DL.get_title(row))
        journal = row.get('jrc_journal', '')
        fileoutput += "\t".join([row['jrc_publishing_date'], row['doi'],
                                 DL.get_title(row), journal]) + "\n"
        trows.append([row['jrc_publishing_date'], safe(doi_link(row['doi'])), ptitle, journal])
    html = render_table(header, trows, table_id=relation_type, css='tablesorter numbers-scroll')
    label = "all years" if year == 'All' else year
    # These pages default to the current year (not "All"), so pass the button's
    # selected value explicitly rather than letting the wrapper derive it.
    selected = "(all years)" if year == 'All' else year
    top = year_pulldown(f"preprint_relation/{relation_type}", selected=selected) + "<br><br>"
    count_line = f"{cfg['title']} ({label}): {cnt:,}"
    html = f"{top}{count_line}<br><br>" \
           + create_downloadable(relation_type, header, fileoutput) + html
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=cfg['title'], html=html,
                                         navbar=generate_navbar('Preprints')))

# ******************************************************************************
# * UI endpoints (Journals)                                                    *
# ******************************************************************************

@app.route('/dois_publisher/<string:year>')
@app.route('/dois_publisher')
def dois_publisher(year='All'):
    ''' Show publishers with counts, optionally scoped to one jrc_tag.name (?tag=)
        alongside the year filter.
    '''
    if year == 'All':
        match = {}
    else:
        match = {"jrc_publishing_date": {"$regex": "^"+ year}}
    tag = request.args.get('tag') or None
    if tag:
        match["jrc_tag.name"] = tag
    payload = [{"$match": match},
               {"$group": {"_id": {"publisher": "$publisher", "source": "$jrc_obtained_from"},
                           "count":{"$sum": 1}}},
               {"$sort": {"_id.publisher": 1}}
              ]
    try:
        rows = DB['dis'].dois.aggregate(payload, collation=INSENSITIVE)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get publishers " \
                                                    + "from dois collection"),
                               message=error_message(err))
    trows = []
    pubs = {}
    for row in rows:
        if row['_id']['publisher'] not in pubs:
            pubs[row['_id']['publisher']] = {}
        if row['_id']['source'] not in pubs[row['_id']['publisher']]:
            pubs[row['_id']['publisher']][row['_id']['source']] = row['count']
    total = {src: 0 for src in SOURCES}
    for pub, val in pubs.items():
        onclick = "onclick='nav_post(\"publisher\",\"" + pub + "\")'"
        link = f"<a href='#' {onclick}>{pub}</a>"
        cells = [safe(link)]
        for source in SOURCES:
            if source in val:
                onclick = "onclick='nav_post(\"publisher\",\"" + pub \
                          + "\",\"" + source + "\")'"
                link = f"<a href='#' {onclick}>{val[source]:,}</a>"
                total[source] += val[source]
            else:
                link = ""
            cells.append(safe(link))
        trows.append(cells)
    footer = [fcell('Total')] + [fcell(f"{total[source]:,}", align='center')
                                 for source in SOURCES]
    html = render_table(['Publisher', 'Crossref', 'DataCite'], trows, table_id='types',
                        css='tablesorter numbers-scroll', footer=footer)
    cards = [("Total DOIs", f"{sum(total.values()):,}"),
             ("Publishers", f"{len(pubs):,}")] \
            + [(f"{src} DOIs", f"{total[src]:,}") for src in SOURCES]
    year_suffix = f"?tag={quote(tag, safe='')}" if tag else ''
    html = stat_cards(cards, div_id='pub-stats') \
           + year_pulldown('dois_publisher', suffix=year_suffix) \
           + "&nbsp;&nbsp;&nbsp;" + tag_pulldown('dois_publisher', year, tag) + html
    title = "DOIs by publisher"
    scope = ([escape(year)] if year != 'All' else []) + ([escape(tag)] if tag else [])
    if scope:
        title += f" ({', '.join(scope)})"
    # Ranked hbar of the top publishers by total DOIs (audit chart policy); the table
    # above lists every publisher. Default colour = get_colors_by_count (SOURCE_PALETTE
    # is reserved for the 2-way registrar split); height scales with the bar count.
    pub_totals = {pub: sum(val.values()) for pub, val in pubs.items()}
    top_pub = dict(sorted(pub_totals.items(), key=lambda kv: kv[1], reverse=True)[:20])
    chart_title = f"Top {len(top_pub)} publishers by DOIs"
    pub_height = max(360, 34 * len(top_pub) + 90)
    chartscript, chartdiv = DP.hbar_chart(top_pub, chart_title, value_label="DOIs",
                                          width=760, height=pub_height, show_values=True)
    endpoint_access()
    return make_response(render_template('bokeh.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         chartscript=chartscript, chartdiv=chartdiv,
                                         navbar=generate_navbar('DOIs')))


@app.route('/dois_heatmap/<string:groupby>/<string:source>/<int:top>')
@app.route('/dois_heatmap/<string:groupby>/<string:source>')
@app.route('/dois_heatmap/<string:groupby>')
def show_dois_heatmap(groupby, source='Crossref', top=10):
    ''' Show a heatmap of DOI counts by publisher or journal and year
        Keyword arguments:
          groupby: "publisher" or "journal"
          source: jrc_obtained_from value (default: Crossref)
          top: number of top entries to display (default: 25)
    '''
    field_map = {'publisher': {'mongo': 'publisher', 'label': 'Publisher'},
                 'journal':   {'mongo': 'jrc_journal', 'label': 'Journal'}}
    if groupby not in field_map:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Invalid groupby parameter"),
                               message=f"groupby must be one of: {', '.join(field_map)}")
    errmsg = f"Could not get {groupby} data from subscription collection"
    pubcount = {}
    html = ''
    if groupby == 'publisher':
        pipeline = [{"$match": {"apc": {"$exists": True}}},
                    {"$group": {"_id": {"publisher": "$publisher", "provider": "$provider"},
                                "count": {"$sum": 1}}},]
        try:
            rows = DB['dis'].subscription.aggregate(pipeline)
        except Exception as err:
            return render_template('error.html', urlroot=request.url_root,
                                   title=render_warning(errmsg),
                                   message=error_message(err))
        for row in rows:
            pubcount[row['_id']['publisher']] = {'count': row['count'],
                                                 'provider': row['_id']['provider']}
    else:
        try:
            rows = DB['dis'].subscription.find({"apc": {"$exists": True}}, {"title": 1})
        except Exception as err:
            return render_template('error.html', urlroot=request.url_root,
                                   title=render_warning(errmsg),
                                   message=error_message(err))
        for row in rows:
            pubcount[row['title']] = row['_id']
    mongo_field = field_map[groupby]['mongo']
    label = field_map[groupby]['label']
    errmsg = f"Could not get {groupby} data from dois collection"
    source = 'DataCite' if source.lower() == 'datacite' else 'Crossref'
    pipeline = [
        {"$match": {mongo_field: {"$exists": True},
                    "jrc_obtained_from": source,
                    "jrc_publishing_date": {"$exists": True}}},
        {"$group": {"_id": {groupby: f"${mongo_field}",
                            "year": {"$substr": ["$jrc_publishing_date", 0, 4]}},
                    "count": {"$sum": 1}}},
        {"$sort": {"_id.year": 1}}
    ]
    try:
        rows = DB['dis'].dois.aggregate(pipeline)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning(errmsg),
                               message=error_message(err))
    raw = [{'year': r['_id']['year'], groupby: r['_id'][groupby],
            'count': r['count']} for r in rows]
    if not raw:
        # No data for this source: inline warning on the normal template instead of a
        # full error page (groupby/source are validated/normalized, so no escaping).
        emsg = f"No {groupby}/year data was found for source {source}."
        endpoint_access()
        return make_response(render_template('bokeh.html', urlroot=request.url_root,
                                             title=f'{source} {label.lower()} heatmap',
                                             html=render_warning(emsg, 'warning'), chartscript='',
                                             chartdiv='', navbar=generate_navbar('Journals')))
    totals = {}
    for r in raw:
        totals[r[groupby]] = totals.get(r[groupby], 0) + r['count']
    top_entries = {e for e, _ in sorted(totals.items(), key=lambda x: x[1],
                                        reverse=True)[:top]}
    data = {'Year': [], label: [], 'Count': []}
    headers = ['Publisher', 'Journal count'] if groupby == 'publisher' else ['Journal']
    trows = []
    for r in raw:
        if r[groupby] in top_entries:
            data['Year'].append(r['year'])
            data[label].append(r[groupby])
            data['Count'].append(r['count'])
            if groupby == 'publisher' and r[groupby] in pubcount:
                jcount = pubcount[r[groupby]]['count']
                link = f"<a href='/subscription/apc/{pubcount[r[groupby]]['provider']}/" \
                       + f"{r[groupby]}'>{jcount:,}</a>"
                trows.append([r[groupby], cell(safe(link), sort=jcount)])
                del pubcount[r[groupby]]
            elif groupby == 'journal' and r[groupby] in pubcount:
                link = f"<a href='/subscription/{pubcount[r[groupby]]}'>{r[groupby]}</a>"
                trows.append([safe(link)])
                del pubcount[r[groupby]]
    if trows:
        heading = 'APCs by publisher' if groupby == 'publisher' else 'Journals with APCs'
        html += f"<br><h3>{heading}</h3>" \
                + render_table(headers, trows,
                               table_id=('publishers' if groupby == 'publisher' else 'journals'),
                               css=('tablesorter numbers-scroll' if groupby == 'publisher'
                                    else 'tablesorter standard-scroll'))
    else:
        html = ''
    chartscript, chartdiv = DP.heat_map(data,
                                        f'Top {top} {source} {groupby}s published to by year',
                                        x_field='Year', y_field=label, value_field='Count',
                                        value_format='0,0', row_totals="ALL")
    endpoint_access()
    return make_response(render_template('bokeh.html', urlroot=request.url_root,
                                         title=f'{source} {label.lower()} heatmap',
                                         html=html, chartscript=chartscript, chartdiv=chartdiv,
                                         navbar=generate_navbar('Journals')))


@app.route('/dois_oa')
def show_open_access():
    ''' Show DOIs by year
    '''
    html = '''
    <ul>
    <li>Citation counts are available starting 2010. These counts represent the number of
    DOIs (from any source) published that year that cite any Janelia DOI (from any year).</li>
    <li>Closed DOIs are DOIs that are in a non-Open Access journal that we cannot find
    freely-available open text for.</li>
    </ul>
    '''
    # OpenAlex meters requests against a daily budget. Anonymous requests get $0,
    # so we join the polite pool (mailto) and send the API key when one is set.
    params = {'search': 'Janelia', 'mailto': EMAIL}
    if os.environ.get('OPENALEX_API_KEY'):
        params['api_key'] = os.environ['OPENALEX_API_KEY']
    try:
        resp = requests.get(f"{OPENALEX}institutions", params=params, timeout=5)
        payload = resp.json()
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get institution information " \
                                                    + "from OpenAlex"),
                               message=error_message(err))
    if 'results' not in payload:
        # Budget-exhausted/error responses carry no 'results' key; show OpenAlex's
        # own error text rather than a generic exception message.
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get institution information " \
                                                    + "from OpenAlex"),
                               message=payload.get('message') or payload.get('error') \
                                       or 'OpenAlex returned no results')
    results = payload['results'][0]
    counts = results['counts_by_year']
    counts = sorted(counts, key=lambda x: x['year'])
    try:
        internal = get_oa_year_counts()
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning('Could not get Open access data from ' \
                                                    + 'dois collection'),
                               message=error_message(err))
    adj = {}
    for row in internal:
        yr = str(row['_id']['year'])
        if yr not in adj:
            adj[yr] = {"open": 0, "closed": 0}
        if row['_id']['status'] == 'closed':
            adj[yr]['closed'] += row['count']
        else:
            adj[yr]['open'] += row['count']
    adjusted = []
    years = []
    for row in counts:
        yr = str(row['year'])
        years.append(yr)
        adjusted.append({"year": int(yr), "org_closed": row['works_count']-row['oa_works_count'],
                         "org_open": row['oa_works_count'], "cited_by_count": row['cited_by_count'],
                         "closed": adj[yr]['closed'], "open": adj[yr]['open']})
    for key, row in adj.items():
        if key in years:
            continue
        adjusted.append({"year": int(key), "org_closed": 0, "org_open": 0,
                         "cited_by_count": 0, "closed": row['closed'], "open": row['open']})
    adjusted = sorted(adjusted, key=lambda x: x['year'])
    html += f"<h5>Total citations for Janelia DOIs since {counts[0]['year']}: " \
            + f"{results['cited_by_count']:,}" + "</h5>"
    data = {'years': [str(itm['year']) for itm in adjusted],
            'Closed': [itm['closed'] for itm in adjusted],
            'Open': [itm['open'] for itm in adjusted],
            'Citations': [itm['cited_by_count'] for itm in adjusted]}
    html2 = "<br>Source for citations: " \
            + "<a href='https://api.openalex.org/institutions?search=Janelia' " \
            + "target='_blank'>OpenAlex</a>"
    tt = [("Year", "@years"), ("Open", "@Open"), ("Closed", "@Closed"), ("Citations", "@Citations")]
    # Tap a year bar -> the DOIs published that year
    ynav = {yr: {"field": "publishing_year", "value": yr} for yr in data['years']}
    chartscript, chartdiv = DP.stacked_bar_chart(data, 'OpenAlex DOIs', xaxis="years", orient=pi/4,
                                                 width=900, height=550,
                                                 yaxis=('Closed', 'Open'), yaxis2='Citations',
                                                 colors=['maroon', 'green'], tooltip=tt, nav=ynav)
    endpoint_access()
    return make_response(render_template('bokeh.html', urlroot=request.url_root,
                                         title="DOIs/citations by year", html=html,
                                         chartscript=chartscript, chartdiv=chartdiv,
                                         html2=html2,
                                         navbar=generate_navbar('DOIs')))


@app.route('/dois_oa_details/<string:year>')
@app.route('/dois_oa_details')
def show_open_access_details(year='All'):
    ''' Show open access DOIs
    '''
    match = {'jrc_is_oa': {"$exists": True}}
    if year != 'All':
        match["jrc_publishing_date"] = {"$regex": "^"+ year}
    payload = [{'$match': match},
               {'$group': {'_id': '$jrc_oa_status', 'count': {"$sum": 1}}},
               {'$sort': {'count': -1}}
              ]
    try:
        rows = DB['dis'].dois.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning('Could not get Open access data'),
                               message=error_message(err))
    total = total_oa = 0
    trows = []
    data = {}
    palette = []
    for row in rows:
        total += row['count']
        if row['_id'] != 'closed':
            total_oa += row['count']
        onclick = "onclick='nav_post_year(\"jrc_oa_status\",\"" + row['_id'] \
                          + "\",\"" + year + "\")'"
        link = f"<a href='#' {onclick}>{row['count']}</a>"
        trows.append([safe(f"<span class='oa_{row['_id']}'>{row['_id'].capitalize()}</span>"),
                      CVTERM['oa_status'][row['_id']]['display'], safe(link)])
        data[row['_id'].capitalize()] = row['count']
        palette.append(DP.OA_COLORS[row['_id'].capitalize()])
    html = render_table(['Status', 'Description', 'Count'], trows, table_id='dois',
                        css='tablesorter numberlast-scroll', width=500,
                        footer=[fcell('Total', colspan=2),
                                fcell(f"{total:,}", align='center')])
    title = 'DOIs by Open Access status'
    if year != 'All':
        title += f" ({year})"
    chartscript, chartdiv = DP.pie_chart(data, title, "oa_status", width=550, height=450,
                                         colors=palette)
    ymsg = f" for {year}" if year != 'All' else ''
    pre = f"<span style='font-size: 18pt; color: lime'>{total_oa/total*100:.1f}%</span>" \
          + f"<span style='font-size: 14pt'> of Janelia DOIs {ymsg} are " \
          + "open access</span>"
    html = pre + '<br>' + year_pulldown('dois_oa_details') + html
    endpoint_access()
    return make_response(render_template('bokeh.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         chartscript=chartscript, chartdiv=chartdiv,
                                         navbar=generate_navbar('Journals')))


@app.route('/journals_dois/<string:year>')
@app.route('/journals_dois')
def show_journals_dois(year=None):
    ''' Show journals in a table
    '''
    if year is None:
        year = 'All'
    errmsg = "Could not get journal data from subscription collection"
    try:
        rows = DB['dis'].subscription.find({"type": {"$in": ["Journal", "Repository"]}})
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
        # Empty result for a valid year: keep the year pulldown so another year can be
        # chosen, and show the standard inline warning instead of a full error page.
        etitle = "DOIs by journal" + (f" ({year})" if year != 'All' else "")
        emsg = "No journals were found" + (f" for {year}" if year != 'All' else "") + "."
        ehtml = year_pulldown('journals_dois') + "<br><br>" + render_warning(emsg, 'warning')
        endpoint_access()
        return make_response(render_template('bokeh.html', urlroot=request.url_root,
                                             title=etitle, html=ehtml, chartscript='',
                                             chartdiv='', navbar=generate_navbar('DOIs')))
    tracked = sum(1 for key in journal if key in subscribed)
    subscribed_cnt = sum(1 for key in journal if key in subscribed
                         and subscribed[key].get('access') == 'Subscription')
    free_cnt = tracked - subscribed_cnt
    cards = [("Journals found", f"{len(journal):,}"),
             ("Tracked subscriptions", f"{tracked:,}")]
    if free_cnt:
        cards.append(("Open access", f"{free_cnt:,}", "lime"))
    if subscribed_cnt:
        cards.append(("Subscribed journals", f"{subscribed_cnt:,}", "yellowgreen"))
    trows = []
    for key in sorted(journal, key=lambda x: journal[x]['count'], reverse=True):
        if key in subscribed:
            jcell = safe(f"<a href='/subscription/{str(subscribed[key]['_id'])}'>"
                         + f"{escape(key)}</a>")
            pcell = subscribed[key].get('publisher', '')
            access = subscribed[key].get('access', '')
            if access == 'Subscription':
                subcell = safe('<span style="color: yellowgreen">YES</span>')
            else:
                subcell = safe(f"<span style='color: lime'>{escape(access)}</span>")
        else:
            jcell = key
            subcell = ''
            try:
                pubs = DB['dis'].dois.distinct('publisher', {'jrc_journal': key})
                pcell = safe('<br>'.join(escape(p) for p in sorted(pubs, key=str.lower)))
            except Exception:
                pcell = ''
        ccount = journal[key]['count']
        countcell = cell(safe(f"<a href='/journal/{key}/{year}'>{ccount:,}</a>"), sort=ccount)
        trows.append([jcell, pcell, countcell, journal[key]['maxpub'], subcell])
    html = render_table(['Journal', 'Publisher', 'Count', 'Last published to', 'Subscription'],
                        trows, table_id='journals', css='tablesorter numbers-scroll')
    title = "DOIs by journal"
    if year != 'All':
        title += f" ({year})"
    html = "Note: not all subscriptions are currently tracked - " \
           + "Subscription tracking is a work in process<br>" \
           + stat_cards(cards, div_id='jdois-stats') \
           + year_pulldown('journals_dois') + html
    # Ranked hbar of the top journals by DOI count (audit chart policy); the table
    # above lists every journal. Default colour = get_colors_by_count. The wide
    # 5-column table means this sits below the table rather than beside it.
    top_jour = dict(sorted(((k, v['count']) for k, v in journal.items()),
                           key=lambda kv: kv[1], reverse=True)[:20])
    chart_title = f"Top {len(top_jour)} journals by DOIs"
    jour_height = max(360, 34 * len(top_jour) + 90)
    chartscript, chartdiv = DP.hbar_chart(top_jour, chart_title, value_label="DOIs",
                                          width=760, height=jour_height, show_values=True)
    endpoint_access()
    return make_response(render_template('bokeh.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         chartscript=chartscript, chartdiv=chartdiv,
                                         navbar=generate_navbar('DOIs')))


@app.route('/top_entities/<string:entity_type>/<string:year>/<string:source>/<int:top>')
@app.route('/top_entities/<string:entity_type>/<string:year>/<string:source>')
@app.route('/top_entities/<string:entity_type>/<string:year>')
@app.route('/top_entities/<string:entity_type>')
def top_entities(entity_type, year='All', source='crossref', top=10):
    ''' Show top journals or publishers
        Keyword arguments:
          entity_type: "journal" or "publisher"
          year: year to filter (default: All)
          source: jrc_obtained_from value for publishers (default: crossref)
          top: number of top entries to display (default: 10, max: 20)
    '''
    entity_config = {
        'journal':   {'label': 'Journal',   'note': "Note that this does not contain "
                                                     "Janelia Research Campus (figshare)<br>"},
        'publisher': {'label': 'Publisher', 'note': ''},
    }
    if entity_type not in entity_config:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Invalid entity type"),
                               message=f"entity_type must be one of: {', '.join(entity_config)}")
    cfg = entity_config[entity_type]
    top = min(top, 20)
    fsource = 'Crossref' if source.lower() == 'crossref' else 'DataCite'
    try:
        if entity_type == 'journal':
            raw = get_top_journals(year, janelia=False, source=fsource)
            entities = dict(raw)
        else:
            raw = get_top_publishers(year, fsource, maxpub=True)
            entities = {k: v['count'] for k, v in raw.items()}
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning(f"Could not get {entity_type} data from dois"),
                               message=error_message(err))
    if not entities:
        # Empty result for a valid filter: keep the year pulldown (carrying the
        # source/top for publishers) and show the standard inline warning.
        esuffix = f"/{fsource}/{top}" if entity_type == 'publisher' else ''
        etitle = f"DOIs by {entity_type}"
        if entity_type == 'publisher':
            etitle += f" for {fsource}"
        if year != 'All':
            etitle += f" ({year})"
        emsg = f"No {entity_type}s were found" + (f" for {year}" if year != 'All' else "") + "."
        ehtml = year_pulldown(f'top_entities/{entity_type}', suffix=esuffix) + "<br><br>" \
                + render_warning(emsg, 'warning')
        endpoint_access()
        return make_response(render_template('bokeh.html', urlroot=request.url_root,
                                             title=etitle, html=ehtml, chartscript='',
                                             chartdiv='', navbar=generate_navbar('Journals')))
    trows = []
    data = {}
    for key in sorted(entities, key=entities.get, reverse=True):
        val = entities[key]
        if len(data) >= top:
            continue
        data[key] = val
        if entity_type == 'journal':
            link = f"<a href='/journal/{key}/{year}'>{val:,}</a>"
        else:
            onclick = "onclick='nav_post(\"publisher\",\"" + key + "\")'"
            link = f"<a href='#' {onclick}>{val:,}</a>"
        trows.append([key, cell(safe(link), sort=val)])
    suffix = f"/{fsource}/{top}" if entity_type == 'publisher' else ''
    html = cfg['note'] + render_table([cfg['label'], 'Count'], trows, table_id='journals',
                                      css='tablesorter numberlast-scroll') \
           + "<br>" + year_pulldown(f'top_entities/{entity_type}', suffix=suffix)
    title = f"DOIs by {entity_type}"
    if entity_type == 'publisher':
        title += f" for {fsource}"
    if year != 'All':
        title += f" ({year})"
    # Ranked categorical breakdown -> horizontal bar (audit chart policy). Default
    # colour = get_colors_by_count (SOURCE_PALETTE is reserved for the 2-way registrar
    # split); height scales with the number of bars (<= top).
    ent_height = max(360, 34 * len(data) + 90)
    chartscript, chartdiv = DP.hbar_chart(data, title, value_label="DOIs",
                                          width=850, height=ent_height, show_values=True)
    title = f"Top {top} DOI {entity_type}s"
    title += f" for {fsource}"
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
    trows = []
    cnt = 0
    for row in rows:
        cnt += 1
        doi = row['doi']
        trows.append([safe(f"<a href='/doiui/{doi}'>{doi}</a>"),
                      render_title_html(DL.get_title(row))])
    html = render_table(['DOI', 'Title'], trows,
                        table_id='articles', css='tablesorter standard-scroll')
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
    # show_count=False: the count is shown in the card below (with id 'totalrows',
    # which the "Filter versioned DOIs" toggler updates)
    html, cnt, _ = standard_doi_table(rows, show_count=False)
    payload = [{"$match": payload},
               {"$group": {"_id": "$jrc_oa_status", "count": {"$sum": 1}}}]
    try:
        rows = DB['dis'].dois.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get status counts for journal"),
                               message=error_message(err))
    total = 0
    data = {}
    for row in rows:
        total += row['count']
        data[row['_id'].capitalize() if row['_id'] else 'Unknown'] = row.get('count', 0)
    if total < cnt:
        data['Unknown'] = data.get('Unknown', 0) + cnt - total
    cards = [("DOIs", f"<span id='totalrows'>{cnt:,}</span>")]
    for key in sorted(data, key=oa_status_rank):
        stat = DP.OA_COLORS.get(key.capitalize(), 'crimson')
        cards.append((key.capitalize(), f"{data[key]:,}", stat))
    title = f"DOIs for {jname}"
    if year != 'All':
        title += f" ({year})"
    html = stat_cards(cards, div_id='journal-stats') + html
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
    cards = stat_cards([("Journals", f"{journals:,}"),
                        ("References", f"{refs:,}")], div_id='jref-stats')
    html = year_pulldown("journals_referenced") + "<br><br>" + cards \
           + create_downloadable('journals', ['Journal', 'References'], fileoutput)
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
    provider = {}
    try:
        rows = DB['dis'].subscription.find({'provider': {"$exists": True}})
        for row in rows:
            provider[row['publisher']] = row['provider']
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning(errmsg),
                               message=error_message(err))
    try:
        cnt = DB['dis'].subscription.count_documents({})
        oacnt = DB['dis'].subscription.count_documents({"access": "Free to read"})
        pcount = len(DB['dis'].subscription.distinct("provider"))
        pubcnt = DB['dis'].subscription.distinct("publisher")
        typs = DB['dis'].subscription.aggregate([{"$group": {"_id": "$type", "count": {"$sum": 1}}},
                                                 {"$sort": {"_id": 1}}])
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning(errmsg),
                               message=error_message(err))
    # Most recent subscription cost (across all providers)
    cost_year = cost_total = None
    try:
        cost_rows = list(DB['dis'].subscription.aggregate([
            {"$match": {"cost": {"$exists": True}}},
            {"$addFields": {"costArray": {"$objectToArray": "$cost"}}},
            {"$unwind": "$costArray"},
            {"$group": {"_id": "$costArray.k",
                        "total": {"$sum": {"$toDouble": "$costArray.v"}}}},
            {"$sort": {"_id": -1}},
            {"$limit": 1}
        ]))
        if cost_rows:
            cost_year = cost_rows[0]['_id']
            cost_total = cost_rows[0]['total']
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning(errmsg),
                               message=error_message(err))
    # APC stats: count of titles, min/max/avg across all APC values
    apc_stats = None
    try:
        apc_rows = list(DB['dis'].subscription.aggregate([
            {"$match": {"apc": {"$exists": True}}},
            {"$addFields": {"apcArray": {"$objectToArray": "$apc"}}},
            {"$unwind": "$apcArray"},
            {"$group": {"_id": None,
                        "titles": {"$addToSet": "$_id"},
                        "maxYear": {"$max": "$apcArray.k"},
                        "minv": {"$min": {"$toDouble": "$apcArray.v"}},
                        "maxv": {"$max": {"$toDouble": "$apcArray.v"}},
                        "avg":  {"$avg": {"$toDouble": "$apcArray.v"}}}},
            {"$project": {"_id": 0,
                          "count": {"$size": "$titles"},
                          "maxYear": 1, "minv": 1, "maxv": 1,
                          "avg": {"$round": ["$avg", 2]}}}
        ]))
        if apc_rows:
            apc_stats = apc_rows[0]
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning(errmsg),
                               message=error_message(err))
    # Stat cards
    cards = [("Providers", f"{pcount:,}"),
             ("Publishers", f"{len(pubcnt):,}"),
             ("Titles", f"{cnt:,}"),
             ("Open access", f"{oacnt:,}", "lime")]
    if cost_total is not None:
        cost_link = f"<a href='/subscription/cost'>${cost_total:,.2f}</a>"
        cards.append((f"Subscription cost ({cost_year})", cost_link))
    if apc_stats:
        apc_body = (f"<span style='font-size:0.75em; font-weight:normal;'>"
                    f"<a href='/subscription/apc'>"
                    f"${apc_stats['minv']:,.2f} - ${apc_stats['maxv']:,.2f} "
                    f"(Avg. ${apc_stats['avg']:,.2f})</a></span>")
        cards.append((f"{apc_stats['count']:,} journals with APCs ({apc_stats['maxYear']})",
                      apc_body))
    html = stat_cards(cards, div_id='sub-stats')
    types = {}
    for row in typs:
        types[row['_id']] = int(row['count'])
    payload = [{"$group": {"_id": {"publisher": "$publisher", "type": "$type"},
                           "count": {"$sum": 1}}},
               {"$sort": {"_id.publisher": 1, "_id.type": 1}}
              ]
    try:
        rows = DB['dis'].subscription.aggregate(payload, collation=INSENSITIVE)
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
    headers = ['Publisher', 'Provider'] + list(types) + ['TOTAL']
    trows = []
    for publisher, data in transform.items():
        count = []
        for typ in types:
            if typ in data:
                tcnt = f"<a href='/subscriptionlist/{publisher}/publisher/{typ}'>" \
                       + f"{data[typ]:,}</a>"
            else:
                tcnt = ""
            count.append(tcnt)
        pp = provider.get(publisher, '')
        if pp:
            link = f"<a href='/subscription/provider/{provider.get(publisher, '')}'>{pp}</a>"
        else:
            link = ""
        dtl = f"<a href='/subscriptionlist/{publisher}'>{data['TOTAL']:,}</a>"
        trows.append([publisher, safe(link)] + [safe(c) for c in count] + [safe(dtl)])
    footer = ([fcell('Total', colspan=2, align='right', header=False)]
              + [fcell(safe(f"<a href='/subscriptions/type/{key}'>{val:,}</a>"), header=False)
                 for key, val in types.items()]
              + [fcell(f"{cnt:,}", header=False)])
    html += render_table(headers, trows, table_id='journals',
                         css='tablesorter numbers-scroll', footer=footer)
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title='Subscription summary', html=html,
                                         navbar=generate_navbar('Subscriptions')))


@app.route('/subscription/provider/<path:prov>')
def show_subscription_summary_by_provider(prov):
    ''' Show subscription summary by provider
    '''
    errmsg = "Could not get data from subscription collection"
    try:
        payload = [{"$match": {"provider": prov}},
                   {"$group": {"_id": "$type", "count": {"$sum": 1}}},
                   {"$sort": {"_id": 1}}]
        typs = DB['dis'].subscription.aggregate(payload, collation=INSENSITIVE)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning(errmsg),
                               message=error_message(err))
    types = {}
    for row in typs:
        types[row['_id']] = int(row['count'])
    payload = [{"$match": {"provider": prov}},
               {"$group": {"_id": {"publisher": "$publisher", "type": "$type"},
                           "count": {"$sum": 1}}},
               {"$sort": {"_id.publisher": 1, "_id.type": 1}}
              ]
    try:
        rows = DB['dis'].subscription.aggregate(payload, collation=INSENSITIVE)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning(errmsg),
                               message=error_message(err))
    transform = {}
    cnt = 0
    for row in rows:
        if row['_id']['publisher'] not in transform:
            transform[row['_id']['publisher']] = collections.defaultdict(lambda: 0, {})
        transform[row['_id']['publisher']][row['_id']['type']] = row['count']
        transform[row['_id']['publisher']]['TOTAL'] += row['count']
    cnt = 0
    for publisher, data in transform.items():
        for typ in types:
            if typ in data:
                cnt += data[typ]
    # Open access count
    try:
        oa_cnt = DB['dis'].subscription.count_documents({"provider": prov,
                                                         "access": "Free to read"})
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning(errmsg),
                               message=error_message(err))
    # Most recent subscription cost
    cost_year = cost_total = None
    try:
        cost_rows = list(DB['dis'].subscription.aggregate([
            {"$match": {"provider": prov, "cost": {"$exists": True}}},
            {"$addFields": {"costArray": {"$objectToArray": "$cost"}}},
            {"$unwind": "$costArray"},
            {"$group": {"_id": "$costArray.k",
                        "total": {"$sum": {"$toDouble": "$costArray.v"}}}},
            {"$sort": {"_id": -1}},
            {"$limit": 1}
        ]))
        if cost_rows:
            cost_year = cost_rows[0]['_id']
            cost_total = cost_rows[0]['total']
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning(errmsg),
                               message=error_message(err))
    # APC stats: count of titles, min/max/avg across all APC values
    apc_stats = None
    try:
        apc_rows = list(DB['dis'].subscription.aggregate([
            {"$match": {"provider": prov, "apc": {"$exists": True}}},
            {"$addFields": {"apcArray": {"$objectToArray": "$apc"}}},
            {"$unwind": "$apcArray"},
            {"$group": {"_id": None,
                        "titles": {"$addToSet": "$_id"},
                        "maxYear": {"$max": "$apcArray.k"},
                        "minv": {"$min": {"$toDouble": "$apcArray.v"}},
                        "maxv": {"$max": {"$toDouble": "$apcArray.v"}},
                        "avg":  {"$avg": {"$toDouble": "$apcArray.v"}}}},
            {"$project": {"_id": 0,
                          "count": {"$size": "$titles"},
                          "maxYear": 1, "minv": 1, "maxv": 1,
                          "avg": {"$round": ["$avg", 2]}}}
        ]))
        if apc_rows:
            apc_stats = apc_rows[0]
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning(errmsg),
                               message=error_message(err))
    # Stat cards
    cards = [("Titles", f"{cnt:,}"),
             ("Publishers", f"{len(transform):,}")]
    if oa_cnt:
        cards.append(("Open access", f"{oa_cnt:,}", "lime"))
    if cost_total is not None:
        cost_link = f"<a href='/subscription/cost/{prov}'>${cost_total:,.2f}</a>"
        cards.append((f"Subscription cost ({cost_year})", cost_link))
    if apc_stats:
        apc_body = (f"<span style='font-size:0.75em; font-weight:normal;'>"
                    f"<a href='/subscription/apc/{prov}'>"
                    f"${apc_stats['minv']:,.2f} - ${apc_stats['maxv']:,.2f} "
                    f"(Avg. ${apc_stats['avg']:,.2f})</a></span>")
        cards.append((f"{apc_stats['count']:,} journals with APCs ({apc_stats['maxYear']})",
                      apc_body))
    # Janelia first-author publications under this provider's titles/publishers,
    # linking to the publication-centric /dois_provider/<prov> view.
    try:
        sub_titles = DB['dis'].subscription.distinct("title", {"provider": prov})
        sub_pubs = DB['dis'].subscription.distinct("publisher", {"provider": prov})
        pub_doi_cnt = DB['dis'].dois.count_documents(
            {"$or": [{"publisher": {"$in": sub_pubs}},
                     {"jrc_journal": {"$in": sub_titles}}],
             "jrc_first_author": {"$exists": True}})
    except Exception:
        pub_doi_cnt = 0
    if pub_doi_cnt:
        cards.append(("Janelia publications",
                      f"<a href='/dois_provider/{prov}'>{pub_doi_cnt:,}</a>"))
    html = stat_cards(cards, div_id='prov-stats')
    headers = ['Publisher'] + list(types) + ['TOTAL']
    trows = []
    for publisher, data in transform.items():
        count = []
        for typ in types:
            if typ in data:
                tcnt = f"<a href='/subscriptionlist/{publisher}/publisher/{typ}'>" \
                       + f"{data[typ]:,}</a>"
            else:
                tcnt = ""
            count.append(tcnt)
        link = f"<a href='/subscriptionlist/{publisher}'>{data['TOTAL']:,}</a>"
        pub_link = f"<a href='/subscriptionlist/{publisher}'>{publisher}</a>"
        trows.append([safe(pub_link)] + [safe(c) for c in count] + [safe(link)])
    footer = ([fcell('Total', colspan=1, align='right', header=False)]
              + [fcell(safe(f"<a href='/subscriptions/type/{key}'>{val:,}</a>"), header=False)
                 for key, val in types.items()]
              + [fcell(f"{cnt:,}", header=False)])
    html += render_table(headers, trows, table_id='journals',
                         css='tablesorter numbers-scroll', footer=footer)
    html += "<br><a class='btn btn-outline-info btn-med' " \
            + f"href='/subscriptionlist/{prov}/provider'" \
            + " role='button'>Show details</a>"
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=f"Subscription summary for provider {prov}",
                                         html=html, navbar=generate_navbar('Subscriptions')))


@app.route('/subscription/year')
@app.route('/subscription/year/<string:year>')
def show_subscription_year(year=None):
    ''' Show subscription costs for a specific year
    '''
    if year is None:
        year = str(datetime.now().year)
    errmsg = "Could not get data from subscription collection"
    sortorder = [("provider", 1), ("publisher", 1), ("title", 1)]
    try:
        rows = DB['dis'].subscription.find({f"cost.{year}": {"$exists": True}}) \
                                      .collation({"locale": "en"}).sort(sortorder)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning(errmsg),
                               message=error_message(err))
    html = "<br>" + year_pulldown("subscription/year", all_years=False, start_year=2011)
    trows = []
    data = {}
    total = 0
    sub_cnt = 0
    for row in rows:
        if row['provider'] not in data:
            data[row['provider']] = 0
        sub_cnt += 1
        total += float(row["cost"][year])
        data[row['provider']] += float(row["cost"][year])
        trows.append([row['provider'], row['publisher'], row['title'],
                      cell(f"${float(row['cost'][year]):,.2f}", sort=float(row['cost'][year]))])
    html2 = render_table(['Provider', 'Publisher', 'Title', 'Cost'], trows, table_id='costs',
                         css='tablesorter numberlast-scroll',
                         footer=[fcell('Total', colspan=3, align='right'),
                                 fcell(f"${total:,.2f}")])
    if not data:
        html += render_warning(f"No subscription costs were found for {year}.", 'warning')
        endpoint_access()
        return make_response(render_template('general.html', urlroot=request.url_root,
                                             title=f"Subscription costs by provider for {year}",
                                             html=html,
                                             navbar=generate_navbar('Subscriptions')))
    html += stat_cards([("Providers", f"{len(data):,}"),
                        ("Titles", f"{sub_cnt:,}"),
                        ("Total cost", f"${total:,.2f}")], div_id='subyear-stats')
    # Tap a provider bar -> that provider's cost-by-year report
    pnav = {prov: f"/subscription/cost/{quote(prov, safe='')}" for prov in data}
    barscript, bardiv = DP.hbar_chart(data, f'Subscription costs by provider for {year}',
                                      value_label='Cost', value_format="$0,0",
                                      width=650, height=450, nav=pnav)
    endpoint_access()
    return make_response(render_template('bokeh.html', urlroot=request.url_root,
                                        title=f"Subscription costs by provider for {year}",
                                        html=html, html2=html2,
                                        chartscript=barscript, chartdiv=bardiv,
                                        navbar=generate_navbar('Subscriptions')))


@app.route('/subscription/missingcost')
@app.route('/subscription/missingcost/<string:year>')
def show_subscription_missingcost(year=None):
    ''' Show providers missing costs for a specific year
    '''
    if year is None:
        year = str(datetime.now().year)
    errmsg = "Could not get data from subscription collection"
    two_years_ago = str(datetime.now().year - 2)
    # Providers included in the heatmap: have cost data and max cost year > two_years_ago
    pipeline = [{"$match": {"cost": {"$exists": True}}},
                {"$addFields": {"costYears": {"$objectToArray": "$cost"}}},
                {"$unwind": "$costYears"},
                {"$group": {"_id": "$provider", "maxYear": {"$max": "$costYears.k"}}},
                {"$match": {"maxYear": {"$gt": two_years_ago}}}]
    try:
        active_providers = {row['_id'] for row in DB['dis'].subscription.aggregate(pipeline)}
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning(errmsg),
                               message=error_message(err))
    # Providers that have at least one cost entry for the selected year
    try:
        covered = set(DB['dis'].subscription.distinct(
            "provider", {f"cost.{year}": {"$exists": True}}))
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning(errmsg),
                               message=error_message(err))
    missing = sorted(active_providers - covered, key=str.lower)
    html = "<br>" + year_pulldown("subscription/missingcost", all_years=False, start_year=2011)
    html += "<br><br>"
    if not missing:
        html += f"<p>No providers are missing costs for {year}.</p>"
        endpoint_access()
        return make_response(render_template('general.html', urlroot=request.url_root,
                                             title=f"Providers missing costs for {year}",
                                             html=html,
                                             navbar=generate_navbar('Subscriptions')))
    html += "<ul>"
    for prov in missing:
        html += f"<li><a href='/subscription/cost/{prov}'>{prov}</a></li>"
    html += "</ul>"
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=f"Providers missing costs for {year}",
                                         html=html,
                                         navbar=generate_navbar('Subscriptions')))


@app.route('/subscription/cost')
@app.route('/subscription/cost/<string:provider>')
def show_subscription_costs(provider=None):
    ''' Show subscription costs
    '''
    errmsg = "Could not get data from subscription collection"
    payload = [{"$project": {"costArray": {"$objectToArray": "$cost" }}},
                            {"$unwind": "$costArray"},
                            {"$group": {"_id": "$costArray.k",
                                        "totalCost": {"$sum": {"$toDouble": "$costArray.v"}},
                                        "count": {"$sum": 1}
                              }},
                              {"$sort": {"_id": 1}}]
    if provider:
        payload.insert(0, {"$match": {"provider": provider}})
        providers = []
    else:
        # Per (provider, year): summed cost and the count of subscriptions active that
        # year (any type - journal, database, repository, etc.), so the Providers table
        # can show current-year subscriptions + cost next to the all-time total (a flat
        # count would include subscriptions dropped years ago).
        pipeline = [{"$match": {"cost": {"$exists": True}}},
                    {"$project": {"provider": 1, "years": {"$objectToArray": "$cost"}}},
                    {"$unwind": "$years"},
                    {"$group": {"_id": {"provider": "$provider", "year": "$years.k"},
                                "cost": {"$sum": {"$toDouble": "$years.v"}},
                                "titles": {"$sum": 1}}}]
        try:
            provrows = list(DB['dis'].subscription.aggregate(pipeline))
        except Exception as err:
            return render_template('error.html', urlroot=request.url_root,
                                   title=render_warning(errmsg),
                                   message=error_message(err))
        prov_year = collections.defaultdict(dict)
        for row in provrows:
            prov_year[row['_id']['provider']][row['_id']['year']] = row
        providers = sorted(prov_year, key=str.lower)
    try:
        rows = DB['dis'].subscription.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning(errmsg),
                               message=error_message(err))
    data = {'Year': [], 'Cost': [], 'Count': []}
    last_cost = 0
    perc = {}
    for row in rows:
        perc[row['_id']] = {'percent': None, 'count': row['count'], 'cost': row['totalCost']}
        if last_cost:
            perc[row['_id']]['percent'] = ((row['totalCost'] - last_cost) / last_cost) * 100
        last_cost = row['totalCost']
        data['Year'].append(row['_id'])
        data['Cost'].append(row['totalCost'])
        data['Count'].append(row['count'])
    if not data['Year']:
        return render_template('error.html', urlroot=request.url_root,
                               title='No costs found',
                               message=f"No costs found for provider {provider}")
    title = 'Subscription costs by year'
    # Table
    perclist = []
    trows = []
    for year, val in perc.items():
        if val['percent'] is not None:
            perclist.append(val['percent'])
            pp = cell(f"{val['percent']:+.2f}%", sort=val['percent'])
        else:
            pp = ""
        link = f"<a href='/subscription/year/{year}'>{year}</a>"
        trows.append([safe(link), val['count'],
                      cell(f"${val['cost']:,.2f}", sort=val['cost']), pp])
    footer = None
    if perclist:
        footer = [fcell('AVERAGE % change', colspan=3, align='right'),
                  fcell(f"{sum(perclist)/len(perclist):+.2f}%")]
    # Stat-cards header (mirrors show_subscription_year, which has one) so the cost
    # pair presents the same way.
    cards = stat_cards([("Years", f"{len(data['Year']):,}"),
                        ("Total cost", f"${sum(data['Cost']):,.2f}"),
                        (f"Latest ({data['Year'][-1]})", f"${data['Cost'][-1]:,.2f}")],
                       div_id='subcost-stats')
    html = cards + render_table(['Year', 'Subscriptions', 'Cost', '% change'], trows,
                                table_id='costs', css='tablesorter numbers-scroll',
                                footer=footer)
    if not provider:
        curyear = data['Year'][-1]   # latest year present in the cost data
        prows = []
        tot_titles = tot_curcost = tot_all = 0
        for pp in providers:
            years = prov_year[pp]
            all_cost = sum(val['cost'] for val in years.values())
            cur = years.get(curyear, {})
            ctitles, ccost = cur.get('titles', 0), cur.get('cost', 0)
            tot_titles += ctitles
            tot_curcost += ccost
            tot_all += all_cost
            prows.append([
                safe(f"<a href='/subscription/cost/{quote(pp, safe='')}'>{escape(pp)}</a>"),
                cell(f"{ctitles:,}", sort=ctitles),
                cell(f"${ccost:,.2f}", sort=ccost),
                cell(f"${all_cost:,.2f}", sort=all_cost)])
        pfooter = [fcell('Total', align='right'), fcell(f"{tot_titles:,}"),
                   fcell(f"${tot_curcost:,.2f}"), fcell(f"${tot_all:,.2f}")]
        html += "<br><br><h3>Providers</h3>"
        html += render_table(['Provider', f'Subscriptions ({curyear})', f'{curyear} cost',
                              'Total cost'], prows, table_id='providers',
                             css='tablesorter numbers-scroll', footer=pfooter)
    # Bar/line chart; tap a year bar -> that year's costs (mirrors the year cell)
    ynav = {str(yr): f"/subscription/year/{yr}" for yr in data['Year']}
    chartscript, chartdiv = DP.dual_axis_chart(data, title=title,
                                               x_field='Year', bar_field='Cost',
                                               line_field='Count', bar_trend=True, nav=ynav)
    chartscript2 = None
    try:
        if provider:
            chartscript2, chartdiv2 = provider_title_heat_map(provider)
        else:
            chartscript2, chartdiv2 = provider_heat_map()
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning(errmsg),
                               message=error_message(err))
    if chartscript2 is not None:
        chartscript += chartscript2
        chartdiv += chartdiv2
    # Title list (provider view only)
    if provider:
        try:
            titles = DB['dis'].subscription.find(
                {"provider": provider, "cost": {"$exists": True}},
                {"title": 1}
            ).collation({"locale": "en"}).sort("title", 1)
            title_rows = list(titles)
        except Exception as err:
            return render_template('error.html', urlroot=request.url_root,
                                   title=render_warning(errmsg),
                                   message=error_message(err))
        if title_rows:
            html += f"<br><h4>Titles ({len(title_rows):,})</h4><ul>"
            for row in title_rows:
                sid = row['_id']
                html += f"<li><a href='/subscription/{sid}'>{row['title']}</a></li>"
            html += "</ul>"
    endpoint_access()
    title = f"Subscription costs for {provider}" if provider is not None \
            else f"Subscription costs (2011-{datetime.now().year})"
    return make_response(render_template('bokeh.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         chartscript=chartscript, chartdiv=chartdiv,
                                         navbar=generate_navbar('Subscriptions')))


@app.route('/subscriptions/type/<string:jtype>')
def show_subscriptions(jtype):
    ''' Show journals, books, etc. in a table
    '''
    errmsg = "Could not get data from subscription collection"
    try:
        rows = DB['dis'].subscription.find({"type": jtype})
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning(errmsg),
                               message=error_message(err))
    if not rows:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning(errmsg),
                               message='No journals were found')
    trows = []
    fileoutput = ""
    jlist = {}
    publist = {}
    provlist = {}
    for row in rows:
        jlist[row['title']] = True
        publist[row['publisher']] = True
        provlist[row['provider']] = True
        jour = f"<a href='{row['urls'][0]}'>{row['title']}</a>" if row.get('urls') else row['title']
        jour = f"<a href='/subscription/{str(row['_id'])}'>{row['title']}</a>"
        trows.append([safe(jour), row['publisher'], row['provider']])
        fileoutput += f"{row['title']}\t{row['publisher']}\t{row['provider']}\n"
    html = render_table(['Title', 'Publisher', 'Provider'], trows,
                        table_id='journals', css='tablesorter standard-scroll')
    title = f"{jtype} subscriptions"
    html = stat_cards([("Providers", f"{len(provlist):,}"),
                       ("Publishers", f"{len(publist):,}"),
                       ("Titles", f"{len(jlist):,}")], div_id='subtype-stats') \
           + create_downloadable(jtype, ['Title', 'Publisher', 'Provider'], fileoutput)
    titles = '<option>' + '</option><option>'.join(sorted(jlist.keys())) + '</option>'
    pubs = '<option>' + '</option><option>'.join(sorted(publist.keys())) + '</option>'
    endpoint_access()
    return make_response(render_template('subscription.html', urlroot=request.url_root,
                                         title=title, titles=titles, pubs=pubs,
                                         html=html, sub=jtype,
                                         navbar=generate_navbar('Subscriptions')))


@app.route('/subscriptionlist/<path:sub>')
@app.route('/subscriptionlist/<path:sub>/<string:field>')
@app.route('/subscriptionlist/<path:sub>/<string:field>/<string:stype>')
def show_subscriptionlist(sub, field='publisher', stype=None):
    ''' Show subscription list for a title
    '''
    errmsg = "Could not get data from subscription collection"
    payload = {field: sub, "type": stype} if stype else {field: sub}
    try:
        cnt = DB['dis'].subscription.count_documents(payload)
        srt = [("type", 1), ("title", 1)]
        rows = DB['dis'].subscription.find(payload).collation({"locale": "en"}).sort(srt)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning(errmsg),
                               message=error_message(err))
    if not cnt:
        return render_template('warning.html', urlroot=request.url_root,
                               title=render_warning(errmsg),
                               message=f"No subscriptions were found for {sub}<br>" \
                                       + f"<pre>{json.dumps(payload, indent=4)}</pre>")
    if cnt == 1:
        return redirect(f"/subscription/{rows[0]['_id']}")
    pubcount = {}
    if field == 'publisher':
        try:
            payload = [{"$match": {"jrc_journal": {"$exists": 1}}},
                       {"$group": {"_id": "$jrc_journal", "count": {"$sum": 1}}}]
            pubs = DB['dis'].dois.aggregate(payload)
            for row in pubs:
                pubcount[row['_id']] = row['count']
        except Exception as err:
            return render_template('error.html', urlroot=request.url_root,
                                   title=render_warning(errmsg),
                                   message=error_message(err))
    header = ([] if stype else ['Type']) \
             + ['Title', 'Publisher', 'Provider', 'Title ID', 'Access', 'APC']
    if field == 'publisher':
        header.append('Janelia publications')
    fileoutput = ""
    pubto = 0
    subscribed_cnt = 0
    free_cnt = 0
    janelia_pubs = 0
    trows = []
    for row in rows:
        cells = []
        if not stype:
            if field == 'provider':
                cells.append(safe(f"<a href='/subscriptionlist/{sub}/provider/{row['type']}'>"
                                  + f"{row['type']}</a>"))
            else:
                cells.append(row['type'])
        link = f"<a href='/subscription/{str(row['_id'])}'>{row['title']}</a>"
        if row['access'] == 'Subscription':
            subscribed_cnt += 1
        else:
            free_cnt += 1
        access = '<span style="color: yellowgreen">YES</span>' \
                  if row['access'] == 'Subscription' \
                  else f"<span style='color: lime'>{row['access']}</span>"
        apc_obj = ''
        apc_export = ''
        if row.get('apc'):
            apc_year = max(row['apc'])
            apc_val = float(row['apc'][apc_year])
            apc_export = f"${apc_val:,.2f} ({apc_year})"
            apc_link = f"<a href='/subscription/apc/{row['provider']}/{row['publisher']}'>" \
                       + f"{apc_export}</a>"
            apc_obj = cell(safe(apc_link), sort=apc_val)
        cells += [safe(link), row['publisher'], row['provider'], row['title-id'],
                  safe(access), apc_obj]
        file_cells = ([] if stype else [row['type']]) \
                     + [row['title'], row['publisher'], row['provider'],
                        row['title-id'], row['access'], apc_export]
        if field == 'publisher':
            if pubcount.get(row['title']):
                pubto += 1
                janelia_pubs += pubcount[row['title']]
            jlink = f"<a href='/journal/{row['title']}'>{pubcount.get(row['title'])}</a>" \
                    if pubcount.get(row['title']) else ''
            cells.append(cell(safe(jlink), align='center'))
            file_cells.append(str(pubcount.get(row['title'], '')))
        trows.append(cells)
        fileoutput += "\t".join(file_cells) + "\n"
    html = render_table(header, trows, table_id='journals', css='tablesorter standard-scroll')
    title = f"ubscriptions for {field} {sub}"
    title = f"{stype} s{title}" if stype else f"S{title}"
    cards = [("Titles", f"{cnt:,}")]
    if free_cnt:
        cards.append(("Open access", f"{free_cnt:,}", "lime"))
    if subscribed_cnt:
        cards.append(("Subscribed journals", f"{subscribed_cnt:,}", "yellowgreen"))
    if janelia_pubs:
        cards.append(("Janelia publications", f"{janelia_pubs:,}"))
    html = stat_cards(cards, div_id='sublist-stats') \
           + create_downloadable("subscriptions", header, fileoutput) \
           + "<br><br>" + html
    chartscript = chartdiv = ""
    if pubto:
        perc = (pubto / cnt) * 100 if pubto else 0
        chartscript, chartdiv = DP.venn_diagram('Subscribed to', 'Published to',
                                                'Subscribed and published',
                                                 perc, colors=['DarkOrange', 'SpringGreen'],
                                                 title='Journals subscribed/published to for ' \
                                                       + sub)
    endpoint_access()
    return make_response(render_template('bokeh.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         chartscript=chartscript, chartdiv=chartdiv,
                                         navbar=generate_navbar('Subscriptions')))


def process_charges(ptype):
    ''' Process subscription charges
    Keyword arguments:
      ptype: publication type
    Returns:
      HTML string
    '''
    html = ""
    errmsg = "Could not get data from subscription collection"
    payload = [{"$match": {"type": ptype, "cost": {"$exists": True, "$ne": None}}},
               {"$project": {"title": 1, "provider": 1,
                             "latestCost": {"$reduce": {"input": {"$objectToArray": "$cost"},
                                                        "initialValue": {"k": "", "v": 0},
                                                        "in": {"$cond": {"if":
                                                                         {"$gt": ["$$this.k",
                                                                                  "$$value.k"]},
                                                                         "then": "$$this",
                                                                         "else": "$$value"}}}}}},
               {"$set": {"costValue": {"$toDouble": "$latestCost.v"}}},
               {"$match": {"costValue": {"$ne": None}}},
               {"$sort": {"costValue": 1}},
               {"$group": {"_id": "-", "minv": {"$first": "$costValue"},
                           "mint": {"$first": "$title"}, "minp": {"$first": "$provider"},
                           "maxv": {"$last": "$costValue"}, "maxt": {"$last": "$title"},
                           "maxp": {"$last": "$provider"},
                           "avg": {"$avg": "$costValue"}}},
               {"$project": {"_id": 0, "minv": 1, "mint": 1, "minp": 1, "maxv": 1,
                             "maxt": 1, "maxp": 1, "avg": {"$round": ["$avg", 2]}}}]
    try:
        rows = DB['dis'].subscription.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning(errmsg),
                               message=error_message(err))
    try:
        row = list(rows)[0]
    except Exception:
        return ""
    html += f"<h4>{ptype}</h4>"
    html += f"Minimum: ${row['minv']:,.2f} ({row['minp']} - {row['mint']})<br>"
    html += f"Maximum: ${row['maxv']:,.2f} ({row['maxp']} - {row['maxt']})<br>"
    html += f"Average: ${row['avg']:,.2f}"
    return html


@app.route('/subscription/provider')
def show_subscription_providers():
    ''' Show subscription information across all providers
    '''
    errmsg = "Could not get data from subscription collection"
    payload = [{"$group": {"_id": {"provider": "$provider", "type": "$type"},
                "count":      {"$sum": 1},
                "publishers": {"$addToSet": "$publisher"}}},
               {"$group": {"_id": "$_id.provider","count": {"$sum": "$count"},
                           "publishers": {"$push": "$publishers"},
                           "types": {"$push": {"type": "$_id.type", "count": "$count"}}}},
               {"$project": {"count": 1,
                             "distinct_publishers": {"$size":
                                                     {"$reduce":
                                                      {"input": "$publishers",
                                                       "initialValue": [],
                                                       "in": {"$setUnion": ["$$value", "$$this"]}
                                                      }}}, "types": 1}},
               {"$sort": {"_id": 1}}]
    cnt = 0
    pub_total = 0
    try:
        rows = DB['dis'].subscription.aggregate(payload, collation=INSENSITIVE)
        pubcnt = len(DB['dis'].subscription.distinct("publisher"))
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning(errmsg), message=error_message(err))
    trows = []
    for row in rows:
        cnt += 1
        pub_total += row['count']
        types = safe("<br>".join(f"{escape(typ['type'])}: {typ['count']:,}"
                                 for typ in sorted(row['types'], key=lambda x: x['count'],
                                                   reverse=True)))
        link = safe(f"<a href='/subscription/provider/{row['_id']}'>{escape(row['_id'])}</a>")
        trows.append([cell(link, style='vertical-align: middle'),
                      cell(row['distinct_publishers'], sort=row['distinct_publishers'],
                           align='center', style='vertical-align: middle'),
                      cell(f"{row['count']:,}", sort=row['count'],
                           align='center', style='vertical-align: middle'),
                      types])
    table = render_table(['Provider', 'Publishers', 'Publication count', 'Publication types'],
                         trows, table_id='providers', css='tablesorter standard-scroll')
    html = stat_cards([("Providers", f"{cnt:,}"),
                       ("Publications", f"{pub_total:,}"),
                       ("Publishers", f"{pubcnt:,}")], div_id='providers-stats')
    html += table
    # Subscription charges
    html += "<br><br><h3>Subscription charges</h3>"
    for ptype in ['Journal', 'Collection', 'Database', 'DataService']:
        phtml = process_charges(ptype)
        html += phtml
    # APCs
    html += "<br><br><h3>Account Processing Charges (APCs)</h3>"
    payload = [{"$project": {"title": 1, "provider": 1, "apcValues": {"$objectToArray": "$apc"}}},
               {"$unwind": "$apcValues"},
               {"$set": {"apcValue": {"$toDouble": "$apcValues.v"}}},
               {"$sort": {"apcValue": 1}},
               {"$group": {"_id": "-", "minv": {"$first": "$apcValue"},
                           "mint": {"$first": "$title"}, "minp": {"$first": "$provider"},
                           "maxv": {"$last": "$apcValue"},
                           "maxt": {"$last": "$title"}, "maxp": {"$last": "$provider"},
                           "avg": {"$avg": "$apcValue"}}},
               {"$project": {"_id": 0, "minv": 1, "mint": 1,
                             "maxv": 1, "maxt": 1, "minp": 1, "maxp": 1,
                             "avg": {"$round": ["$avg", 2]}}}]
    try:
        rows = DB['dis'].subscription.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning(errmsg),
                               message=error_message(err))
    try:
        row = list(rows)[0]
        ctx_style = "font-size:0.7em; font-weight:normal; color:#a8c4e0;"
        min_val = (f"<a href='/subscription/apc'>${row['minv']:,.2f}</a>"
                   f"<div style='{ctx_style}'>{row['minp']} - {row['mint']}</div>")
        max_val = (f"<a href='/subscription/apc'>${row['maxv']:,.2f}</a>"
                   f"<div style='{ctx_style}'>{row['maxp']} - {row['maxt']}</div>")
        html += stat_cards([("Minimum APC", min_val),
                            ("Maximum APC", max_val),
                            ("Average APC", f"${row['avg']:,.2f}")], div_id='apc-stats')
    except Exception:
        pass
    title = "Subscription providers"
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         navbar=generate_navbar('Subscriptions')))


@app.route('/subscription/apc')
@app.route('/subscription/apc/<string:provider>')
@app.route('/subscription/apc/<string:provider>/<string:publisher>')
def show_subscription_apcs(provider=None, publisher=None):
    ''' Show APC costs from subscription collection
    '''
    errmsg = "Could not get data from subscription collection"
    query = {"apc": {"$exists": True}}
    if provider:
        query["provider"] = provider
    if publisher:
        query["publisher"] = publisher
    sortorder = [("provider", 1), ("publisher", 1), ("title", 1)]
    provider_list = {}
    publisher_list = {}
    try:
        rows = list(DB['dis'].subscription.find(query).collation(
            {"locale": "en"}).sort(sortorder))
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning(errmsg),
                               message=error_message(err))
    if not rows:
        return render_template('warning.html', urlroot=request.url_root,
                               title=render_warning(errmsg),
                               message="No APC records were found for " \
                                       + f"{publisher if publisher else provider}")
    years = sorted({yr for row in rows for yr in row['apc']})
    if publisher:
        header = ['Journal'] + years
    elif provider:
        header = ['Publisher', 'Journal'] + years
    else:
        header = ['Provider', 'Publisher', 'Journal'] + years
    fileoutput = ""
    trows = []
    cnt = 0
    for row in rows:
        tlink = f"<a href='/subscription/{row['_id']}'>{row['title']}</a>"
        provider_list[row['provider']] = True
        publisher_list[row['publisher']] = True
        if publisher:
            cells = [safe(tlink)]
            file_cells = [row['title']]
        elif provider:
            cells = [row['publisher'], safe(tlink)]
            file_cells = [row['publisher'], row['title']]
        else:
            cells = [row['provider'], row['publisher'], safe(tlink)]
            file_cells = [row['provider'], row['publisher'], row['title']]
        for yr in years:
            if yr in row['apc']:
                apc_val = float(row['apc'][yr])
                cells.append(cell(f"${apc_val:,.2f}", sort=apc_val))
                file_cells.append(f"${apc_val:,.2f}")
            else:
                cells.append("")
                file_cells.append("")
        trows.append(cells)
        fileoutput += "\t".join(file_cells) + "\n"
        cnt += 1
    download_name = f"apcs_{provider}" if provider else "apcs"
    html = create_downloadable(download_name, header, fileoutput) \
           + "<br><br>" + render_table(header, trows, table_id='apcs',
                                       css='tablesorter numberlast-scroll')
    if not provider:
        prehtml = "<h3>Providers</h3>"
        for prv in sorted(provider_list.keys(), key=str.lower):
            prehtml += f"<a href='/subscription/apc/{prv}'>{prv}</a><br>"
        html = prehtml + "<br><br>" + html
    elif not publisher:
        prehtml = "<h3>Publishers</h3>"
        for pub in sorted(publisher_list.keys(), key=str.lower):
            prehtml += f"<a href='/subscription/apc/{provider}/{pub}'>{pub}</a><br>"
        html = prehtml + "<br><br>" + html
    title = f"APC costs for {provider}" if provider else "APC costs"
    if publisher:
        title = f"{title} / {publisher}"
    html = stat_cards([("Providers", f"{len(provider_list):,}"),
                       ("Publishers", f"{len(publisher_list):,}"),
                       ("Titles", f"{cnt:,}")], div_id='apclist-stats') + html
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         navbar=generate_navbar('Subscriptions')))


@app.route('/subscription/<string:sid>')
def show_subscription(sid):
    ''' Show subscription information for a single publication (by ID)
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
    row['publisher'] = row.get('publisher', 'Unknown')
    dlio = lio = nvo = ''
    if row.get('volumes'):
        vol = row['volumes'][-1]
        dlio = vol['date_last_issue_online'] if vol.get('date_last_issue_online') else ''
        lio = vol['num_last_issue_online'] if vol.get('num_last_issue_online') else ''
        nvo = vol['num_last_vol_online'] if vol.get('num_last_vol_online') else ''
    color = 'yellowgreen' if row['access'] == 'Subscription' else 'lime'
    html = ''
    html += f"<table class='proplist'><tr><td>Publisher</td><td>{row['publisher']}</td></tr>" \
            + f"<tr><td>Type</td><td>{row['type']}</td></tr>" \
            + f"<tr><td>Access</td><td><span style='color: {color}'>{row['access']}</span>"
    if row.get('oa_status'):
        html += f"<tr><td>Open Access status</td><td><span class='oa_{row['oa_status']}'" \
                + f"style='font-weight: bold;'>{row['oa_status'].capitalize()}</span>"
    html += f"</td></tr><tr><td>Provider</td><td>{row['provider']}</td></tr>" \
            + f"<tr><td>Print ISSN</td><td>{row['print-identifier']}</td></tr>" \
            + f"<tr><td>Online ISSN</td><td>{row['online-identifier']}</td></tr>" \
            + f"<tr><td>Title ID</td><td>{row['title-id']}</td></tr>"
    vols = []
    idates = []
    if row.get('apc'):
        html += f"<tr><td>APC</td><td>{grouped_by_year(row['apc'])}</td></tr>"
    if row.get('volumes'):
        for vol in row['volumes']:
            txt = []
            if vol.get('num_first_vol_online'):
                txt.append(f"Volumes online: {vol['num_first_vol_online']} - {nvo}")
            if vol.get('num_first_issue_online'):
                txt.append(f"Issues online: {vol['num_first_issue_online']} - {lio}")
            if vol.get('date_first_issue_online'):
                txt.append(f"Issue dates online: {vol['date_first_issue_online']} - {dlio}")
                idates.append(f"{vol['date_first_issue_online']} - {dlio}")
            if txt:
                vols.append(", ".join(txt))
        if vols:
            html += f"<tr><td>Volumes</td><td>{'<br>'.join(vols)}</td></tr>"
    # Cost
    chartscript = chartdiv = ""
    if row.get('cost'):
        data = {'year': [str(itm) for itm in row['cost'].keys()],
                'cost': [float(itm) for itm in row['cost'].values()]}
        html += f"<tr><td>Cost for FY {data['year'][-1]}</td><td>" \
                + f"${data['cost'][-1]:,.2f}</td></tr>"
        if len(data['year']) > 1:
            delta = ((data['cost'][-1] - data['cost'][-2]) / data['cost'][-2]) * 100
            delta = f"+{delta:.2f}" if delta > 0 else f"{delta:.2f}"
            html += f"<tr><td>% cost change from FY {data['year'][-2]}</td><td>" \
                    + f"{delta}%</td></tr>"
        title = 'Subscription cost by year'
        # Single cost series -> dual_axis_chart with no line series: gives a $-formatted
        # ($0,0) y-axis (the old stacked_bar_chart abuse left bare numbers on the axis)
        # and matches the other subscription cost charts.
        chartscript, chartdiv = DP.dual_axis_chart(data, title=title, x_field='year',
                                                   bar_field='cost', bar_label='Cost',
                                                   width=500, height=400)
    # Close table and show button(s)
    html += '</table>'
    if row.get('urls'):
        idx = 0
        for url in row['urls']:
            link = f"window.location.href=\'{url}\'"
            label = row['type']
            if len(row['urls']) > 1:
                if idates:
                    label += f" ({idates[idx]})"
                else:
                    label += f" ({dlio if dlio else 'unknown'})"
            idx += 1
            html += '<br><div><button id="toggle-to-all" type="button" ' \
                    + 'class="btn btn-success btn-small"' \
                    + f"onclick=\"{link}\">Access {label}</button></div>"
    try:
        rows = DB['dis'].dois.find({"jrc_journal": row['title']}) \
                   .collation({"locale": "en"}) \
                   .sort("jrc_publishing_date", -1)
    except Exception as err:
        return render_template('error.html', urlroot=request.url.root,
                               title=render_warning("Could not get DOIs for journal"),
                               message=error_message(err))
    jtbl, cnt, _ = standard_doi_table(rows)
    html2 = f"<br><br><h3>DOIs</h3>{jtbl}" if cnt else ''
    endpoint_access()
    return make_response(render_template('bokeh.html', urlroot=request.url_root,
                                         title=row['title'], html=html, html2=html2,
                                         chartscript=chartscript, chartdiv=chartdiv,
                                         navbar=generate_navbar('Subscriptions')))

# ******************************************************************************
# * UI endpoints (ORCID)                                                       *
# ******************************************************************************
def _render_person(orciddata, full_name, show, prefix, extra=''):
    ''' Render a person page (shared by /orcidui and /userui) with a consistent
        header/body: the DB name as the page title, the shared orcid-collection body,
        the journal/preprint toggle spliced in right after the works count, and any
        route-specific HTML (e.g. /orcidui's live-ORCID extra works) appended below.
        Keyword arguments:
          orciddata: person body HTML from get_orcid_from_db
          full_name: the person's name (page title)
          show: 'full' or 'journal' (drives the toggle button)
          prefix: route base for the toggle links (e.g. /userui/<eid> or /orcidui/<oid>)
          extra: optional trailing HTML appended after the body
        Returns:
          Response
    '''
    buttons = journal_buttons(show, prefix)
    # The works table renders the count as "Number of DOIs: <span id='totalrows'>N</span>";
    # splice the journal/preprint toggle in right after it.
    count_re = r"(Number of DOIs: <span id='totalrows'>[\d,]+</span>)"
    if re.search(count_re, orciddata):
        orciddata = re.sub(count_re, lambda m: m.group(1) + buttons, orciddata, count=1)
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=full_name, html=orciddata + extra,
                                         navbar=generate_navbar('Authorship')))


@app.route('/orcidui/<string:oid>/<string:show>')
@app.route('/orcidui/<string:oid>')
def show_oid_ui(oid, show='full'):
    ''' Show ORCID user. Shares the canonical person header/body with /userui
        (DB name as title, copyable ORCID in the body, journal/preprint toggle) and
        additionally appends the live-ORCID extra-works list below.
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
    try:
        orciddata, dois, full_name = get_orcid_from_db(oid, use_eid=bool('userIdO365' in oid),
                                                       show=show)
    except CustomException as err:
        return render_template('error.html', urlroot=request.url_root,
                                title=render_warning(f"Could not find ORCID ID {oid}", 'error'),
                                message=error_message(err))
    if not orciddata:
        return render_template('warning.html', urlroot=request.url_root,
                               title=render_warning(f"Could not find ORCID ID {oid}", 'warning'),
                               message="Could not find any information for this ORCID ID")
    extra = ''
    if data.get('activities-summary', {}).get('works', {}).get('group'):
        extra = add_orcid_works(data, dois)
    return _render_person(orciddata, full_name, show, f"/orcidui/{oid}", extra=extra)


@app.route('/userui/<string:eid>/<string:show>')
@app.route('/userui/<string:eid>')
def show_user_ui(eid, show='full'):
    ''' Show user record by employeeId (user ID) or ORCID
    '''
    try:
        orciddata, _, full_name = get_orcid_from_db(eid, use_eid=bool('@' in eid),
                                                    bare=False, show=show)
    except CustomException as err:
        return render_template('error.html', urlroot=request.url_root,
                                title=render_warning(f"Could not find user ID {eid}",
                                                     'warning'),
                                message=error_message(err))
    if not orciddata:
        return render_template('warning.html', urlroot=request.url_root,
                               title=render_warning(f"Could not find user ID {eid}", 'warning'),
                               message="Could not find any information for this employee ID")
    return _render_person(orciddata, full_name, show, f"/userui/{eid}")


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
                                         html=orciddata, navbar=generate_navbar('Authorship')))


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
           + f"<span id='totalrowsa'>{count:,}</span></p>" + html
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title="Authors", html=html,
                                         navbar=generate_navbar('Authorship')))


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
                                         after=after, navbar=generate_navbar('System')))


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
    trows = []
    for row in rows:
        if 'orcid' not in row:
            row['orcid'] = ""
        who = f"{row['family'][0]}, {row['given'][0]}"
        if row.get('userIdO365'):
            who = f"<a href='/userui/{row['userIdO365']}'>{who}</a>"
        badges = []
        if row.get('alumni'):
            badges.append(f"{tiny_badge('alumni', 'Former employee')}")
        worker_badge(row, badges)
        if row.get('group'):
            badges.append(f"{tiny_badge('lab', row['group'])}")
        if row.get('managed'):
            for key in row['managed']:
                badges.append(f"{tiny_badge('managed', key)}")
        if badges:
            who += f" {' '.join(badges)}"
        if 'affiliations' not in row:
            row['affiliations'] = []
        else:
            row['affiliations'] = sorted(list(row['affiliations']))
        trows.append([row['hireDate'], safe(who), row['orcid'],
                      ', '.join(row['affiliations'])])
    html = render_table(['Hire Date', 'Name', 'ORCID', 'Affiliations'], trows,
                        table_id='hires', css='tablesorter standard-scroll')
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=f"Employees hired {startdate} - {stopdate} " \
                                               + f"({cnt:,})",
                                         html=html, navbar=generate_navbar('System')))


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
    html = '<table id="types" class="tablesorter standard-scroll"><tbody>'
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
            if row.get('workerType') and row['workerType'] != 'Employee':
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
                                         navbar=generate_navbar('Authorship')))


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
            collabel = 'ORCIDs' if check == 'employeeId' else 'User IDs'
            trows = []
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
                    other.append(f"<a href=\"{ORCID}{rec['orcid']}\">" \
                                 + f"{rec['orcid']}</a>")
                trows.append([', '.join(names), safe(', '.join(other))])
            html += check + render_table(['Name', collabel], trows, table_id='duplicates',
                                         css='tablesorter standard-scroll')
        if not html:
            html = "<p>No duplicates found</p>"
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title="Authors with multiple ORCIDs", html=html,
                                         navbar=generate_navbar('System')))


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
                                         navbar=generate_navbar('System')))


def _ratelimit_epoch(epoch):
    ''' Convert an epoch-seconds value to a tz-aware UTC datetime '''
    return datetime.fromtimestamp(int(epoch), tz=dateutil.tz.tzutc())


def _ratelimit_fmt_reset(when):
    ''' Format a tz-aware reset datetime in local time (em-dash if unknown) '''
    if not when:
        return '—'
    return when.astimezone(dateutil.tz.gettz()).strftime("%Y-%m-%d %H:%M:%S %Z")


def _ratelimit_record(service, window):
    ''' Seed a normalized rate-limit record '''
    return {'service': service, 'window': window, 'note': '', 'error': None,
            'limit': None, 'used': None, 'remaining': None, 'resets': None}


def _ratelimit_from_headers(rec, headers, status, limit_key, remaining_key, reset_key=None):
    ''' Populate a record from X-RateLimit-style response headers. Used is derived
        as limit - remaining. Sets rec['error'] if the expected headers are absent.
    '''
    lim, rem = headers.get(limit_key), headers.get(remaining_key)
    if lim is not None and rem is not None:
        rec['limit'], rec['remaining'] = int(lim), int(rem)
        rec['used'] = rec['limit'] - rec['remaining']
        if reset_key and headers.get(reset_key):
            rec['resets'] = _ratelimit_epoch(headers[reset_key])
    else:
        rec['error'] = f"no rate-limit headers (HTTP {status})"
    return rec


def _ratelimit_openalex():
    ''' OpenAlex: dedicated /rate-limit endpoint returns a daily credit budget '''
    rec = _ratelimit_record('OpenAlex', 'Daily')
    try:
        rl = requests.get('https://api.openalex.org/rate-limit', timeout=8,
                          verify=False,
                          headers={'Authorization':
                                   f'Bearer {os.environ["OPENALEX_API_KEY"]}'}
                          ).json()['rate_limit']
        rec['limit'] = rl['credits_limit']
        rec['used'] = rl['credits_used']
        rec['remaining'] = rl.get('credits_remaining', rl['credits_limit'] - rl['credits_used'])
        rec['resets'] = dateutil.parser.parse(rl['resets_at'])
        if rl.get('daily_budget_usd'):
            rec['note'] = f"${rl.get('daily_used_usd', 0):.2f} / " \
                          + f"${rl['daily_budget_usd']:.2f} budget"
    except Exception as err:
        rec['error'] = str(err)
    return rec


def _ratelimit_elsevier():
    ''' Elsevier: weekly quota exposed as X-RateLimit-* headers on any response '''
    rec = _ratelimit_record('Elsevier', 'Weekly')
    try:
        resp = requests.get('https://api.elsevier.com/content/search/sciencedirect',
                            headers={'X-ELS-APIKey': os.environ['ELSEVIER_API_KEY'],
                                     'Accept': 'application/json'},
                            params={'query': 'janelia', 'count': 1}, timeout=12,
                            verify=False)
        _ratelimit_from_headers(rec, resp.headers, resp.status_code,
                                'X-RateLimit-Limit', 'X-RateLimit-Remaining',
                                'X-RateLimit-Reset')
    except Exception as err:
        rec['error'] = str(err)
    return rec


def _ratelimit_wos():
    ''' Web of Science Starter: per-day quota (plus a per-second burst) headers '''
    rec = _ratelimit_record('Web of Science', 'Daily')
    try:
        resp = requests.get('https://api.clarivate.com/apis/wos-starter/v1/documents',
                            headers={'X-ApiKey': os.environ['WOS_API_KEY']},
                            params={'q': 'TS=janelia', 'limit': 1, 'page': 1}, timeout=15,
                            verify=False)
        _ratelimit_from_headers(rec, resp.headers, resp.status_code,
                                'x-ratelimit-limit-day', 'x-ratelimit-remaining-day')
        burst = resp.headers.get('x-ratelimit-limit-second')
        if burst:
            rec['note'] = f"{burst}/sec burst"
    except Exception as err:
        rec['error'] = str(err)
    return rec


def _ratelimit_zenodo():
    ''' Zenodo: per-minute window exposed as x-ratelimit-* headers '''
    rec = _ratelimit_record('Zenodo', 'Per minute')
    try:
        resp = requests.get('https://zenodo.org/api/records',
                            headers={'Authorization':
                                     f'Bearer {os.environ["ZENODO_API_KEY"]}'},
                            params={'size': 1}, timeout=12, verify=False)
        _ratelimit_from_headers(rec, resp.headers, resp.status_code,
                                'x-ratelimit-limit', 'x-ratelimit-remaining',
                                'x-ratelimit-reset')
    except Exception as err:
        rec['error'] = str(err)
    return rec


@app.route('/ratelimit')
def ratelimit_all():
    ''' Show API rate-limit / usage status for every service that exposes it
        (OpenAlex, Elsevier, Web of Science, Zenodo). Each row is fetched live;
        a service that errors or stops exposing headers shows an error note
        rather than breaking the page. Springer and NCBI expose no usable
        limit and are omitted.
    '''
    with concurrent.futures.ThreadPoolExecutor() as executor:
        fns = [_ratelimit_openalex, _ratelimit_elsevier,
               _ratelimit_wos, _ratelimit_zenodo]
        records = list(executor.map(lambda f: f(), fns))
    records.sort(key=lambda rec: rec['service'].lower())
    trows = []
    for rec in records:
        if rec['error']:
            trows.append([rec['service'], cell('—', align='right'),
                          cell('—', align='right'), cell('—', align='right'),
                          cell('—', align='right'), rec['window'], '—',
                          safe(f"<span style='color:#e74c3c'>{escape(rec['error'])}</span>")])
            continue
        limit, used, remaining = rec['limit'], rec['used'], rec['remaining']
        pct = used / limit * 100 if limit else 0
        color = '#e74c3c' if pct >= 90 else '#f0c674' if pct >= 70 else '#7ed321'
        # color must live on a <span>: ".standard-scroll td" forces cell text
        # color with !important, which an inline <td> color would not override
        trows.append([rec['service'],
                      cell(f"{used:,}", sort=used, align='right'),
                      cell(f"{remaining:,}", sort=remaining, align='right'),
                      cell(f"{limit:,}", sort=limit, align='right'),
                      cell(safe(f"<span style='color:{color}'>{pct:.1f}%</span>"),
                           sort=round(pct, 2), align='right'),
                      rec['window'], _ratelimit_fmt_reset(rec['resets']),
                      rec['note'] or ''])
    note = "<div style='font-size:0.85em; color:#a8c4e0; max-width:700px; " \
           + "margin-top:10px'>Limits are read live. OpenAlex has a dedicated " \
           + "rate-limit endpoint; the others report limits as response headers, so " \
           + "viewing this page consumes one request against Elsevier, Web of Science, " \
           + "and Zenodo. Springer (no limit headers) and NCBI (fixed 10 req/sec " \
           + "throttle, no quota) are omitted.</div>"
    html = render_table(['Service', 'Used', 'Remaining', 'Limit', '% used', 'Window',
                         'Resets at', 'Notes'], trows, table_id='ratelimits',
                        css='tablesorter standard-scroll') + note
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title="API rate limits", html=html,
                                         navbar=generate_navbar('System')))


# Janelia Research Campus in OpenAlex (ROR 013sk6x84)
JANELIA_OPENALEX_ID = 'I195573530'


@app.route('/openalex_stats')
def openalex_stats():
    ''' Show Janelia's publication footprint in OpenAlex (institution profile:
        works, citations, h-index, i10-index, and a by-year breakdown), all
        affiliation-matched. See /pubmed_stats for the PubMed counterpart.
    '''
    # OpenAlex institution profile (fatal on failure - it is the page)
    try:
        oa_params = {}
        if os.environ.get('OPENALEX_API_KEY'):
            oa_params['api_key'] = os.environ['OPENALEX_API_KEY']
        resp = requests.get(f'https://api.openalex.org/institutions/{JANELIA_OPENALEX_ID}',
                            params=oa_params, timeout=10)
        inst = resp.json()
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get OpenAlex institution data"),
                               message=error_message(err))
    if resp.status_code != 200 or 'works_count' not in inst:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get OpenAlex institution data"),
                               message=f"OpenAlex returned HTTP {resp.status_code} for "
                                       + f"institution {JANELIA_OPENALEX_ID}")
    works = inst.get('works_count', 0)
    cited = inst.get('cited_by_count', 0)
    stats = inst.get('summary_stats') or {}
    by_year = inst.get('counts_by_year') or []
    if request.args.get('fmt') == 'json':
        result = initialize_result()
        result['rest']['source'] = 'openalex'
        result['data'] = {"openalex_id": JANELIA_OPENALEX_ID, "ror": inst.get('ror'),
                          "works_count": works, "cited_by_count": cited,
                          "summary_stats": stats, "counts_by_year": by_year}
        result['rest']['row_count'] = 1
        return generate_response(result)
    cards = stat_cards([("Works", f"{works:,}"),
                        ("Citations", f"{cited:,}"),
                        ("h-index", f"{stats.get('h_index', 0):,}"),
                        ("i10-index", f"{stats.get('i10_index', 0):,}"),
                        ("2-yr mean citedness", f"{stats.get('2yr_mean_citedness', 0):.2f}")],
                       div_id='oa-inst-stats')
    note = "<div style='font-size:0.85em; color:#a8c4e0; max-width:700px; " \
           + "margin:-8px 0 16px 0'>OpenAlex institution " \
           + f"<a href='https://openalex.org/{JANELIA_OPENALEX_ID}' target='_blank'>" \
           + f"{JANELIA_OPENALEX_ID}</a> (ROR 013sk6x84); works and citations are " \
           + "affiliation-matched. Yearly counts reflect OpenAlex's own " \
           + "attribution and may not match the dois collection.</div>"
    glossary = "<div style='font-size:0.85em; color:#a8c4e0; max-width:700px; " \
               + "margin:0 0 16px 0'><b>h-index</b>: h works each cited at least h " \
               + "times.<br><b>i10-index</b>: number of works with at least 10 " \
               + "citations.<br><b>2-yr mean citedness</b>: average citations in the " \
               + "current year to works published in the previous two years (OpenAlex's " \
               + "institution-level analog of the journal impact factor).</div>"
    # By-year table (newest first) paired with a works/citations chart (chronological)
    ytrows = []
    ydata = {'Year': [], 'Works': [], 'Citations': []}
    for rec in sorted(by_year, key=lambda r: r['year']):
        yname = str(rec['year'])
        ytrows.append([yname, f"{rec.get('works_count', 0):,}",
                       f"{rec.get('oa_works_count', 0):,}", f"{rec.get('cited_by_count', 0):,}"])
        ydata['Year'].append(yname)
        ydata['Works'].append(rec.get('works_count', 0))
        ydata['Citations'].append(rec.get('cited_by_count', 0))
    yhtml = "<h4>Works &amp; citations by year</h4>"
    yhtml += render_table(['Year', 'Works', 'OA works', 'Cited-by'], ytrows[::-1],
                          table_id='oa-years', css='tablesorter numberlast-scroll')
    chartscript = year_div = ''
    if ydata['Year']:
        chartscript, year_div = DP.dual_axis_chart(
            ydata, title="Works & citations by year", x_field='Year',
            bar_field='Works', line_field='Citations', bar_label='Works',
            line_label='Cited-by', bar_color='darkorange', bar_format="0,0",
            line_format="0,0", width=650, height=400)
    html = cards + note + glossary \
           + "<div class='flexrow'><div class='flexcol'>" + yhtml + "</div>" \
           + "<div class='flexcol' style='margin: 10px 0 0 20px'>" + year_div + "</div></div>"
    endpoint_access()
    return make_response(render_template('bokeh.html', urlroot=request.url_root,
                                         title="Janelia in OpenAlex", html=html,
                                         chartscript=chartscript, chartdiv='',
                                         chartscript2='', chartdiv2='',
                                         navbar=generate_navbar('System')))


# Janelia first appears in PubMed affiliations ~2007 (campus opened 2006)
PUBMED_AFFIL_TERM = 'Janelia[Affiliation]'
PUBMED_START_YEAR = 2007


def _pubmed_count(term, **extra):
    ''' Return the PubMed esearch result count for a term (raises on error).
        extra kwargs (e.g. datetype/mindate/maxdate) are passed to esearch.
    '''
    params = {'db': 'pubmed', 'term': term, 'retmode': 'json', 'retmax': 0,
              'api_key': os.environ.get('NCBI_API_KEY', '')}
    params.update(extra)
    resp = requests.get('https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi',
                        params=params, timeout=10)
    return int(resp.json()['esearchresult']['count'])


@app.route('/pubmed_stats')
def pubmed_stats():
    ''' Show Janelia's publication footprint in PubMed (affiliation-matched):
        total publications, the PMC (free full text) subset, reviews, and a
        by-year breakdown. PubMed is a bibliographic index, not a citation
        database, so no citation/h-index metrics are available (see
        /openalex_stats for those). The by-year figures are one esearch per
        year, so the page makes ~20 NCBI calls.
    '''
    try:
        total = _pubmed_count(PUBMED_AFFIL_TERM)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get PubMed data"),
                               message=error_message(err))
    # PMC subset and reviews are non-fatal (shown as n/a on failure)
    try:
        pmc = _pubmed_count(f"{PUBMED_AFFIL_TERM} AND pubmed pmc[sb]")
    except Exception:
        pmc = None
    try:
        reviews = _pubmed_count(f"{PUBMED_AFFIL_TERM} AND review[pt]")
    except Exception:
        reviews = None
    # By-year counts: one esearch per year; skip any year that errors
    by_year = {}
    for yname in range(PUBMED_START_YEAR, date.today().year + 1):
        try:
            by_year[yname] = _pubmed_count(PUBMED_AFFIL_TERM, datetype='pdat',
                                           mindate=str(yname), maxdate=str(yname))
        except Exception:
            continue
    if request.args.get('fmt') == 'json':
        result = initialize_result()
        result['rest']['source'] = 'pubmed'
        result['data'] = {"term": PUBMED_AFFIL_TERM, "total": total, "in_pmc": pmc,
                          "reviews": reviews, "by_year": by_year}
        result['rest']['row_count'] = 1
        return generate_response(result)
    if pmc is None:
        pmc_label = "n/a"
    elif total:
        pmc_label = f"{pmc:,} ({pmc/total*100:,.1f}%)"
    else:
        pmc_label = f"{pmc:,}"
    cards = stat_cards([("Total publications", f"{total:,}"),
                        ("In PMC (free full text)", pmc_label),
                        ("Reviews", f"{reviews:,}" if reviews is not None else "n/a")],
                       div_id='pm-stats')
    note = "<div style='font-size:0.85em; color:#a8c4e0; max-width:700px; " \
           + "margin:-8px 0 16px 0'>Affiliation-matched via the " \
           + "<code>Janelia[Affiliation]</code> filter. PubMed is a bibliographic " \
           + "index, not a citation database, so no citation or h-index metrics are " \
           + "available - see <a href='/openalex_stats'>Janelia in OpenAlex</a> for " \
           + "those.</div>"
    glossary = "<div style='font-size:0.85em; color:#a8c4e0; max-width:700px; " \
               + "margin:0 0 16px 0'><b>Total publications</b>: PubMed records with a " \
               + "Janelia affiliation.<br><b>In PMC (free full text)</b>: the subset " \
               + "available as free full text in PubMed Central (an open-access " \
               + "indicator).<br><b>Reviews</b>: works tagged as review articles " \
               + "(PubMed publication type &quot;review&quot;).</div>"
    # By-year table (newest first) paired with a chronological publications chart
    ytrows = []
    ydata = {'Year': [], 'Publications': []}
    for yname in sorted(by_year):
        ytrows.append([str(yname), f"{by_year[yname]:,}"])
        ydata['Year'].append(str(yname))
        ydata['Publications'].append(by_year[yname])
    yhtml = "<h4>Publications by year</h4>"
    yhtml += render_table(['Year', 'Publications'], ytrows[::-1], table_id='pm-years',
                          css='tablesorter numberlast-scroll')
    chartscript = year_div = ''
    if ydata['Year']:
        chartscript, year_div = DP.dual_axis_chart(
            ydata, title="Publications by year", x_field='Year',
            bar_field='Publications', bar_label='Publications', bar_color='steelblue',
            bar_format="0,0", width=650, height=400)
    html = cards + note + glossary \
           + "<div class='flexrow'><div class='flexcol'>" + yhtml + "</div>" \
           + "<div class='flexcol' style='margin: 10px 0 0 20px'>" + year_div + "</div></div>"
    endpoint_access()
    return make_response(render_template('bokeh.html', urlroot=request.url_root,
                                         title="Janelia in PubMed", html=html,
                                         chartscript=chartscript, chartdiv='',
                                         chartscript2='', chartdiv2='',
                                         navbar=generate_navbar('System')))


# Total holdings of each external data source we have a /raw call for. Eight
# expose a clean total (fetched live); the other five do not (_SOURCE_COUNT_NA).
_SOURCE_UA = {'User-Agent': 'JaneliaDIS/1.0 (mailto:svirskasr@janelia.hhmi.org)'}
# Rate limits and auth method for each /raw source (for display in /data_sources).
# Format: (requests/sec, auth description or None)
_SOURCE_RATE = {
    'arXiv':        ('1/3sec',  None),
    'Web of Science': ('5/sec', 'API key (WOS_API_KEY)'),
    'bioRxiv':      ('~1/sec',  None),
    'Crossref':     ('50/sec',  'mailto: header'),
    'DataCite':     ('~15/sec', None),
    'eLife':        ('~2/sec',  None),
    'Elsevier':     ('10/sec',  'API key (ELSEVIER_API_KEY)'),
    'figshare':     ('~5/sec',  None),
    'OpenAlex':     ('10/sec',  'API key (OPENALEX_API_KEY)'),
    'PLoS':         ('10/sec',  None),
    'PMC':          ('10/sec',  'API key (NCBI_API_KEY)'),
    'PubMed':       ('10/sec',  'API key (NCBI_API_KEY)'),
    'protocols.io': ('~6/sec',  'Bearer token (PROTOCOLS_API_TOKEN)'),
    'Springer':     ('10/sec',  'API key (SPRINGER_META_API_KEY)'),
    'Unpaywall':    ('~10/sec', 'Email address'),
    'Zenodo':       ('~1/sec',  'Bearer token (ZENODO_API_KEY)'),
}
_SOURCE_COUNT_NA = {
    'arXiv': 'Broad wildcard queries unsupported by API',
    'Web of Science': 'No supported query for global record count',
    'Elsevier': 'No public total (broad queries blocked)',
    'Springer': 'No public total (broad queries blocked)',
    'protocols.io': 'Requires protocols.io API authentication',
    'figshare': 'API has no global count endpoint (even authenticated)',
    'Unpaywall': 'No global count endpoint; ~50M DOIs per their documentation',
}


def _zenodo_total(payload):
    ''' Zenodo hits.total is an int on some deployments, {value: n} on others '''
    tot = payload['hits']['total']
    return tot['value'] if isinstance(tot, dict) else tot


def _srccount(source, url, extract, params=None, headers=None, note=''):
    ''' Fetch one source's total record count into a normalized record. Sets
        error (rather than raising) so a single bad source can't break the page.
    '''
    rec = {'source': source, 'count': None, 'note': note, 'error': None}
    try:
        resp = requests.get(url, params=params, headers=headers, timeout=15, verify=False)
        if resp.status_code != 200:
            rec['error'] = f"HTTP {resp.status_code}"
        else:
            rec['count'] = int(extract(resp.json()))
    except Exception as err:
        rec['error'] = str(err)
    return rec


def _source_counts():
    ''' Collect total record counts for every /raw source: live for the sources
        that expose a usable total, n/a for the rest. All live fetches run in
        parallel via ThreadPoolExecutor.
    '''
    zen = {'Authorization': f'Bearer {os.environ.get("ZENODO_API_KEY", "")}'}
    einfo = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/einfo.fcgi'
    nkey = os.environ.get('NCBI_API_KEY', '')
    oa_params = {'per-page': 1}
    if os.environ.get('OPENALEX_API_KEY'):
        oa_params['api_key'] = os.environ['OPENALEX_API_KEY']
    specs = [
        ('OpenAlex', 'https://api.openalex.org/works',
         lambda j: j['meta']['count'], {'params': oa_params}),
        ('Crossref', 'https://api.crossref.org/works',
         lambda j: j['message']['total-results'],
         {'params': {'rows': 0}, 'headers': _SOURCE_UA}),
        ('DataCite', 'https://api.datacite.org/dois',
         lambda j: j['meta']['total'], {'params': {'page[size]': 1}}),
        ('PubMed', einfo, lambda j: j['einforesult']['dbinfo'][0]['count'],
         {'params': {'db': 'pubmed', 'retmode': 'json', 'api_key': nkey}}),
        ('PMC', einfo, lambda j: j['einforesult']['dbinfo'][0]['count'],
         {'params': {'db': 'pmc', 'retmode': 'json', 'api_key': nkey}}),
        ('Zenodo', 'https://zenodo.org/api/records', _zenodo_total,
         {'params': {'size': 1}, 'headers': zen}),
        ('PLoS', 'http://api.plos.org/search',
         lambda j: j['response']['numFound'],
         {'params': {'q': '*:*', 'rows': 0, 'wt': 'json'}}),
        ('eLife', 'https://api.elifesciences.org/search',
         lambda j: j['total'], {'params': {'per-page': 1}, 'headers': _SOURCE_UA}),
        ('bioRxiv',
         'https://api.biorxiv.org/details/biorxiv/2013-01-01/'
         + f'{date.today().isoformat()}/0',
         lambda j: j['messages'][0]['count_new_papers'],
         {'note': 'Distinct new preprints (excludes versions)'}),
    ]
    with concurrent.futures.ThreadPoolExecutor() as executor:
        recs = list(executor.map(
            lambda s: _srccount(s[0], s[1], s[2], **s[3]), specs))
    for source, reason in _SOURCE_COUNT_NA.items():
        recs.append({'source': source, 'count': None, 'note': reason, 'error': None})
    return recs


@app.route('/data_sources')
def source_data_sources():
    ''' Show the total number of records each external data source holds (the
        service's own holdings, not Janelia-specific) for every source we have.
        Counts are fetched live - one request per source - so the
        nine sources with a usable total are queried on each view; the other
        four expose no public total and show as n/a.
    '''
    recs = _source_counts()
    # Largest holdings first; sources without a count (n/a or error) sort last
    recs.sort(key=lambda r: (r['count'] is None, -(r['count'] or 0)))
    if request.args.get('fmt') == 'json':
        result = initialize_result()
        result['rest']['source'] = 'external'
        result['data'] = [{'source': r['source'], 'count': r['count'],
                           'rate_limit': _SOURCE_RATE.get(r['source'], ('—', None))[0],
                           'auth': _SOURCE_RATE.get(r['source'], (None, None))[1],
                           'note': r['note'], 'error': r['error']} for r in recs]
        result['rest']['row_count'] = len(recs)
        return generate_response(result)
    trows = []
    for r in recs:
        if r['count'] is not None:
            cnt = cell(f"{r['count']:,}", sort=r['count'], align='right')
        else:
            cnt = cell(safe("<span style='color:#a8c4e0'>n/a</span>"), sort=-1, align='right')
        rnote = r['note']
        if r['error']:
            err = f"<span style='color:#e74c3c'>({escape(r['error'])})</span>"
            rnote = f"{rnote} {err}" if rnote else err
        rps, auth = _SOURCE_RATE.get(r['source'], ('—', None))
        auth_html = escape(auth) if auth else safe("<span style='color:#a8c4e0'>none</span>")
        trows.append([r['source'], cnt, cell(rps, align='center'), safe(auth_html), safe(rnote)])
    intro = "<div style='font-size:0.85em; color:#a8c4e0; max-width:720px; " \
            + "margin-top:10px'>Total records each external service holds (the " \
            + "service's own holdings, not Janelia-specific), fetched live - so this " \
            + "page makes one request per source. Six sources expose no usable " \
            + "public total and show as n/a (arXiv, Unpaywall, and " \
            + "Elsevier/Springer/protocols.io/figshare have no accessible count endpoint).</div>"
    html = render_table(['Source', 'Records', 'Rate limit', 'Auth', 'Notes'], trows,
                        table_id='srccounts', css='tablesorter standard-scroll',
                        data_attrs={"sortlist": "[[0,0]]"}) + intro
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title="Data sources", html=html,
                                         navbar=generate_navbar('System')))


@app.route('/orcid/bulk_search')
def orcid_bulk_search():
    ''' Show ORCID search input
    '''
    top = '''
    This will allow you to bulk search the author database for author names. Input
    is an Excel spreadsheet with two columns: "Given name" and "Family name". After hitting
    the "Bulk search" button, the spreadsheet will be uploaded and the results will be displayed.
    '''
    bottom = ''
    return make_response(render_template('upload.html', urlroot=request.url_root,
                                         title="ORCID bulk search", top=top, bottom=bottom,
                                         navbar=generate_navbar('Authorship')))


@app.route('/orcid/run_bulk_search', methods=['OPTIONS', 'POST'])
def orcid_run_bulk_search():
    ''' Bulk search for ORCIDs
    '''
    file = request.files['file']
    if not file:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("No file uploaded"),
                               message="No file uploaded")
    try:
        df = pd.read_excel(file)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not read file"),
                               message=error_message(err))
    html = f"Read {len(df)} row{'' if len(df) == 1 else 's'}"
    family = given = ""
    for col in df.columns:
        if re.search(r'given|first', col, re.IGNORECASE):
            given = col
        elif re.search(r'family|last', col, re.IGNORECASE):
            family = col
    if not given or not family:
        arr = []
        if not given:
            arr.append("Given name")
        if not family:
            arr.append("Family name")
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not find required columns"),
                               message="Could not find required columns: " + ", ".join(arr))
    trows = []
    outrow = []
    for _, row in df.iterrows():
        try:
            payload = {"$and": [{"family": {"$regex": row[family].strip(), "$options" : "i"}},
                                {"given": {"$regex": row[given].strip(), "$options" : "i"}}]}
            orc = DB['dis'].orcid.find_one(payload)
        except Exception as err:
            return render_template('error.html', urlroot=request.url_root,
                                   title=render_warning("Could not find ORCID record for " \
                                                        + f"{row[given]} {row[family]}"),
                                   message=error_message(err))
        res = "<span style='color:red'>Not found</span>"
        if orc:
            res = f"<a href='/userui/{orc['orcid']}'>{orc['orcid']}</a>" if 'orcid' in orc \
                  else f"<a href='/userui/{orc['userIdO365']}'>{orc['userIdO365']}</a>"
        line = [row[given], row[family], 'Yes' if orc else 'No',
                orc['userIdO365'] if orc and 'userIdO365' in orc else '',
                orc['orcid'] if orc and 'orcid' in orc else '']
        pubs, cnt = get_author_works(orc, line)
        if cnt <= 1:
            outrow.append(pubs[0])
        else:
            outrow.append("\n".join(pubs))
        trows.append([row[given], row[family], safe(res), cnt])
    pre = ""
    if outrow:
        header = ['Given name', 'Family name', 'In database', 'User ID', 'ORCID', 'DOI',
                  'Publication', 'Type', 'Subtype', 'First author', 'Last author', 'Published',
                  'Journal', 'Title', 'Authors']
        pre = create_downloadable('dois', header, "\n".join(outrow)) + "<br><br>"
    html += render_table(['Given name', 'Family name', 'ORCID/user ID', 'Works'], trows,
                        table_id='bulk', css='tablesorter numberlast-scroll')
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title="ORCID bulk search", html=pre+html,
                                         navbar=generate_navbar('Authorship')))

# ******************************************************************************
# * UI endpoints (External systems)                                            *
# ******************************************************************************

@app.route('/orgs')
@app.route('/orgs/<string:full>')
def peoporgsle(full=None):
    ''' Show information on supervisory orgs
    '''
    # Find org managers - not currently used
    manager = {}
    payload = {"$or": [{"group": {"$exists": True}}, {"managed": {"$exists": True}}],
               "alumni": {"$exists": False}, "workerType": "Employee", "orcid": {"$exists": True}}
    try:
        rows = DB['dis'].orcid.find(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get org managers from " \
                                                    + "orcid collection"),
                               message=error_message(err))
    for row in rows:
        if row.get('group'):
            manager[row['group']] = ' '.join([row['given'][0], row['family'][0]])
        if row.get('managed'):
            for mgr in row['managed']:
                manager[mgr] = ' '.join([row['given'][0], row['family'][0]])
    # Find affiliations
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
    headers = ["Name", "Code", "Authors", "DOI tags"]
    trows = []
    cnt = 0
    for key, val in sorted(orgs.items()):
        alink = f"<a href='/tag/{escape(key)}'>{aff[key]}</a>" if key in aff else ''
        tlink = ""
        if key in tag:
            onclick = "onclick='nav_post(\"jrc_tag.name\",\"" + key + "\")'"
            tlink = f"<a href='#' {onclick}>{tag[key]}</a>"
        if not full and not tlink:
            continue
        mgr = manager[key] if key in manager else ''
        trows.append([key, val, safe(alink), safe(tlink)])
        cnt += 1
    html = render_table(headers, trows, table_id='orgs', css='tablesorter numbers-scroll')
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
                                         html=html, navbar=generate_navbar('System')))


@app.route('/people/<string:name>')
@app.route('/people')
def people(name=None):
    ''' Show information from the People system
    '''
    if not name:
        return make_response(render_template('people.html', urlroot=request.url_root,
                                             title="Search People system", content="",
                                             navbar=generate_navbar('System')))
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
                                             navbar=generate_navbar('System')))
    html = "<br><br><h3>Select a name for details:</h3>"
    trows = []
    for rec in response:
        pname = f"{rec['nameFirstPreferred']} {rec['nameLastPreferred']}"
        link = f"<a href='/peoplerec/{rec['userIdO365']}'>{pname}</a>"
        loc = rec['locationName'] if 'locationName' in rec else ""
        if "Janelia" in loc:
            loc = safe(f"<span style='color:lime'>{escape(loc)}</span>")
        trows.append([safe(link), rec['businessTitle'], loc])
    html += render_table(['Name', 'Title', 'Location'], trows,
                         table_id='people', css='tablesorter standard-scroll')
    endpoint_access()
    return make_response(render_template('people.html', urlroot=request.url_root,
                                         title="Search People system", content=html,
                                         navbar=generate_navbar('System')))


# Friendly labels + section grouping for the /peoplerec HTML view. A People
# record is a flat dict of scalars plus a few list-of-dict fields; the HTML view
# renders friendly-labeled Field/Value tables per section (empties skipped) plus
# an affiliations table. The JSON response path still returns the raw record.
PEOPLE_LABELS = {
    'nameFirstPreferred': 'Preferred first name', 'nameFirst': 'First name',
    'nameMiddlePreferred': 'Preferred middle name', 'nameMiddle': 'Middle name',
    'nameLastPreferred': 'Preferred last name', 'nameLast': 'Last name',
    'businessTitle': 'Business title', 'jobTitle': 'Job title', 'jobCode': 'Job code',
    'jobFamily': 'Job family', 'jobProfileVisiting': 'Visiting job profile',
    'workerType': 'Worker type', 'employeeType': 'Employee type', 'ecaType': 'ECA type',
    'fteType': 'FTE type', 'cwrVisiting': 'Contingent worker (visiting)',
    'enabled': 'Enabled', 'spendLimit': 'Spend limit',
    'supOrgName': 'Supervisory org', 'supOrgCode': 'Supervisory org code',
    'supOrgSubType': 'Supervisory org subtype', 'ccDescr': 'Cost center',
    'costCenter': 'Cost center code', 'teamCode': 'Team',
    'rollUpGroup': 'Roll-up group', 'subRollUpGroup': 'Sub roll-up group',
    'locationName': 'Location', 'locationCode': 'Location code', 'building': 'Building',
    'workSpaceName': 'Workspace', 'secondaryLocation': 'Secondary location',
    'secondaryWorkspace': 'Secondary workspace', 'departmentAddress1': 'Address line 1',
    'departmentAddress2': 'Address line 2', 'departmentAddress3': 'Address line 3',
    'departmentCity': 'City', 'departmentState': 'State',
    'departmentPostalCode': 'Postal code', 'departmentCountry': 'Country',
    'email': 'Email', 'phone1': 'Phone', 'phone1Type': 'Phone type',
    'phone2': 'Phone 2', 'phone2Type': 'Phone 2 type', 'fax': 'Fax',
    'hireDate': 'Hire date', 'terminationDate': 'Termination date',
    'contingentWorkerEndDate': 'Contingent worker end date', 'modifiedOn': 'Modified on',
    'userId': 'User ID', 'userIdO365': 'O365 user ID'}
PEOPLE_SECTIONS = (
    ('Name', ('nameFirstPreferred', 'nameFirst', 'nameMiddlePreferred', 'nameMiddle',
              'nameLastPreferred', 'nameLast')),
    ('Role', ('businessTitle', 'jobTitle', 'jobCode', 'jobFamily', 'jobProfileVisiting',
              'workerType', 'employeeType', 'ecaType', 'fteType', 'cwrVisiting',
              'enabled', 'spendLimit')),
    ('Organization', ('supOrgName', 'supOrgCode', 'supOrgSubType', 'ccDescr', 'costCenter',
                      'teamCode', 'rollUpGroup', 'subRollUpGroup')),
    ('Location', ('locationName', 'locationCode', 'building', 'workSpaceName',
                  'secondaryLocation', 'secondaryWorkspace', 'departmentAddress1',
                  'departmentAddress2', 'departmentAddress3', 'departmentCity',
                  'departmentState', 'departmentPostalCode', 'departmentCountry')),
    ('Contact', ('email', 'phone1', 'phone1Type', 'phone2', 'phone2Type', 'fax')),
    ('Dates', ('hireDate', 'terminationDate', 'contingentWorkerEndDate', 'modifiedOn')),
    ('Identifiers', ('userId', 'userIdO365')))


def _people_record_html(rec):
    ''' Render a People record as friendly-labeled Field/Value tables grouped into
        sections (empty values skipped), plus an affiliations table. Replaces the
        former raw json.dumps <pre> dump; the JSON response path is unchanged.
        Keyword arguments:
          rec: People record dict (employeeId/managerId already removed)
        Returns:
          HTML string
    '''
    def present(val):
        return val not in (None, '', [], {})
    html = ""
    shown = set()
    for section, keys in PEOPLE_SECTIONS:
        rows = []
        for key in keys:
            shown.add(key)
            val = rec.get(key)
            if not present(val):
                continue
            if isinstance(val, bool):
                val = 'Yes' if val else 'No'
            label = PEOPLE_LABELS.get(key, key)
            if key == 'email':
                val = safe(f"<a href='mailto:{escape(str(val))}'>{escape(str(val))}</a>")
            rows.append([label, val])
        if rows:
            html += (f"<h5 style='margin:14px 0 6px 0;'>{section}</h5>"
                     + render_table(['Field', 'Value'], rows, width=560))
    affs = rec.get('affiliations') or []
    if affs:
        arows = [[a.get('role', ''), a.get('supOrgName', ''), a.get('supOrgSubType', ''),
                  a.get('type', '')] for a in affs]
        html += ("<h5 style='margin:14px 0 6px 0;'>Affiliations</h5>"
                 + render_table(['Role', 'Supervisory org', 'Subtype', 'Type'], arows,
                                width=560))
    # Future-proofing: surface any scalar field not covered above so nothing new
    # from People silently disappears (nested/empty structures are still omitted;
    # the raw record remains available via the JSON response).
    handled = shown | {'affiliations', 'photoURL', 'employeeId', 'managerId'}
    extra = [[k, rec[k]] for k in sorted(rec)
             if k not in handled and present(rec[k]) and not isinstance(rec[k], (list, dict))]
    if extra:
        html += ("<h5 style='margin:14px 0 6px 0;'>Other</h5>"
                 + render_table(['Field', 'Value'], extra, width=560))
    return html


@app.route('/peoplerec/<string:eid>')
def peoplerec(eid):
    '''
    Show a single People record
    Browsers (Accept: text/html) get the HTML page; other clients get the HHMI People
    record as JSON (employeeId and managerId omitted).
    ---
    tags:
      - People
    parameters:
      - in: path
        name: eid
        required: true
        schema:
          type: string
          example: SMITHJ@hhmi.org
        description: Employee ID (userIdO365)
    responses:
      '200':
        description: HTML page (browser) or the People record as JSON
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/PeopleResult'
          text/html:
            schema:
              type: string
      '404':
        description: People record not found
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/Error'
      '500':
        description: People system error
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/Error'
    '''
    expected = 'html' if 'Accept' in request.headers \
                         and 'html' in request.headers['Accept'] else 'json'
    try:
        rec = JRC.call_people_by_id(eid)
    except Exception as err:
        if expected == 'html':
            return render_template('error.html', urlroot=request.url_root,
                                   title=render_warning(f"Could not get People data for {eid}"),
                                   message=error_message(err))
        raise InvalidUsage(str(err), 500) from err
    if not rec:
        if expected == 'html':
            return render_template('warning.html', urlroot=request.url_root,
                                   title=render_warning(f"Could not find People record for {eid}"),
                                   message="No record found")
        raise InvalidUsage(f"Could not find People record for {eid}", 404)
    if expected == 'json':
        for field in ['employeeId', 'managerId']:
            rec.pop(field, None)
        result = initialize_result()
        result['rest']['source'] = 'people'
        result['data'] = rec
        return generate_response(result)
    title = f"{rec['nameFirstPreferred']} {rec['nameLastPreferred']}"
    for field in ['employeeId', 'managerId']: # Remove employeeId
        if field in rec:
            del rec[field]
    if 'photoURL' in rec:
        title += f"&nbsp;<img src='{rec['photoURL']}' width=100 height=100 " \
                 + f"alt='Photo of {rec['nameFirstPreferred']}'>"
    html = _people_record_html(rec)
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         navbar=generate_navbar('System')))


@app.route('/ror/<string:rorid>')
@app.route('/ror')
def ror(rorid=None):
    ''' Show information from ROR
    '''
    if not rorid:
        return make_response(render_template('ror.html', urlroot=request.url_root,
                                             title="Search ROR", content="",
                                             navbar=generate_navbar('System')))
    try:
        resp = requests.get(f"{ROR}{rorid}", timeout=5).json()
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning(f"Could not get ROR data for {rorid}"),
                               message=error_message(err))
    if not resp or 'errors' in resp:
        msg = '<br>'.join(resp['errors']) if 'errors' in resp else "No ROR ID found"
        return make_response(render_template('ror.html', urlroot=request.url_root,
                                             title="Search ROR",
                                             content=f"<br><h3>{msg}</h3>",
                                             navbar=generate_navbar('System')))
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
                                         navbar=generate_navbar('System')))

# ******************************************************************************
# * UI endpoints (Tag/affiliation)                                             *
# ******************************************************************************

@app.route('/dois_tag_ack/<string:tagtype>')
def show_tag_ack(tagtype):  # pylint: disable=unused-argument
    ''' Legacy URL: the DOI-tag table now lives on the tabbed /tag_metrics page
        (Tags tab), which supersedes this standalone view. Redirect there so old
        links and bookmarks keep working. tagtype is ignored (only "tag" existed).
        Keyword arguments:
          tagtype: former tag type (ignored)
        Returns:
          Redirect to /tag_metrics
    '''
    return redirect('/tag_metrics')


@app.route('/dois_lab')
def dois_lab():
    ''' Show labs with counts
    '''
    try:
        orgs = DL.get_supervisory_orgs(DB['dis'].suporg)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get supervisory orgs"),
                               message=error_message(err))
    payload = {"jrc_tag.name": {"$regex": " Lab"},
               "$or": [{"type": "journal-article"}, {"subtype": "preprint"},
                       {"types.resourceTypeGeneral": "Preprint"}]}
    try:
        rows = DB['dis'].dois.find(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get lab DOIs from dois collection"),
                               message=error_message(err))
    pre = "Reported below are journal articles/preprints for all current labs. " \
          + "DOIs in <span style='color: lime;'>green</span> are for multiple labs.<br>"
    header = ['Lab', 'Published', 'DOI', 'Type', 'Title']
    dois = []
    ddois = {}
    multi = []
    for row in rows:
        cnt = 0
        for tag in row['jrc_tag']:
            if ' Lab' not in tag['name'] or tag['name'] not in orgs \
                or 'active' not in orgs[tag['name']] or not orgs[tag['name']]['active']:
                continue
            typ = 'Journal article' if 'type' in row  \
                                    and row['type'] == 'journal-article' else 'Preprint'
            dois.append([tag['name'], row['jrc_publishing_date'], typ,
                         row['doi'], DL.get_title(row)])
            ddois[row['doi']] = True
            cnt += 1
        if cnt > 1:
            multi.append(row['doi'])
    dois.sort(key=lambda x: (x[0], x[1]))
    fileoutput = ""
    trows = []
    for doi in dois:
        # doi = [lab, published, type, doi, title]. Column order Lab | Published | DOI |
        # Type | Title (DOI beside Published, matching the standard DOI list); TSV mirrors.
        link = doi_link(doi[3], 'lime') if doi[3] in multi else doi_link(doi[3])
        trows.append([doi[0], doi[1], safe(link), doi[2], doi[4]])
        fileoutput += "\t".join([doi[0], doi[1], doi[3], doi[2], doi[4]]) + "\n"
    html = render_table(header, trows, table_id='types', css='tablesorter standard-scroll')
    cards = [("DOIs", f"{len(ddois):,}"),
             ("Labs", f"{len({doi[0] for doi in dois}):,}")]
    html = stat_cards(cards, div_id='lab-stats') + pre \
           + create_downloadable('dois', header, fileoutput) + html
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title="DOIs by lab", html=html,
                                         navbar=generate_navbar('Tag/affiliation')))


@app.route('/dois_janelia_affiliations/<string:aff>')
def dois_janelia_affiliations(aff):
    ''' Show DOIs for Janelia affiliations
    '''
    payload = {"$or": [{"author.affiliation.name": aff},
                       {"creators.affiliation": aff}
                      ]}
    try:
        rows = list(DB['dis'].dois.find(payload))
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get DOIs for Janelia affiliations"),
                               message=error_message(err))
    title = f"DOIs with authors affiliated with {aff}"
    if not rows:
        html = render_warning(f"No DOIs found with authors affiliated with {aff}.", 'warning')
    else:
        crossref = sum(1 for r in rows if r.get('jrc_obtained_from') == 'Crossref')
        # Standard DOI-list table + version-filter/download conventions; the registrar
        # split rides in the header cards (the DOIs card carries id='totalrows' so the
        # version-filter toggler keeps working).
        cards = stat_cards([("DOIs", f"<span id='totalrows'>{len(rows):,}</span>"),
                            ("Crossref", f"{crossref:,}"),
                            ("DataCite", f"{len(rows) - crossref:,}")], div_id='aff-stats')
        table_html, _, _ = standard_doi_table(rows, show_count=False)
        html = cards + table_html
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=title, html=html,
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
    trows = []
    for aff, count in sorted(affiliations.items(), key=lambda item: item[1], reverse=True):
        daff = aff
        if aff in PREFERRED_AFF:
            daff = safe(f"<span style='color: lime;'>{escape(aff)}</span>")
        dlink = f"<a href='/dois_janelia_affiliations/{aff}'>{count:,}</a>"
        trows.append([daff, safe(dlink)])
    html = render_table(['Affiliation', 'Author count'], trows,
                        table_id='affiliations', css='tablesorter numbers-scroll')
    html = "<p> When publishing a paper, please use the following affiliation for all Janelia " \
           + f"authors:<br><span style='color: lime;'>{PREFERRED_AFF[0]}</span>" \
           + " (if published domestically)" \
           + f"<br><span style='color: lime;'>{PREFERRED_AFF[1]}</span>" \
           + " (if published internationally)</p>" + html
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=f"DOI author affiliations ({len(affiliations):,})",
                                         html=html, navbar=generate_navbar('Tag/affiliation')))


@app.route('/affiliations')
def orcid_affiliations():
    '''
    Show ORCID affiliations with author counts
    Browsers (Accept: text/html) get the HTML page; other clients get one row per
    affiliation - author count, how many have an ORCID iD, the ORCID percentage, and
    the supervisory-org status - as JSON.
    ---
    tags:
      - ORCID
    responses:
      '200':
        description: HTML page (browser) or the per-affiliation summary as JSON
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/AffiliationSummaryList'
          text/html:
            schema:
              type: string
      '500':
        description: MongoDB error
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/Error'
    '''
    expected = 'html' if 'Accept' in request.headers \
                         and 'html' in request.headers['Accept'] else 'json'
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
        if expected == 'html':
            return render_template('error.html', urlroot=request.url_root,
                                   title=render_warning("Could not get supervisory orgs"),
                                   message=error_message(err))
        raise InvalidUsage(str(err), 500) from err
    try:
        # locale "en" collation sorts affiliations case-insensitively (dictionary
        # order) rather than MongoDB's default uppercase-before-lowercase
        rows = DB['dis'].orcid.aggregate(payload, collation={"locale": "en"})
    except Exception as err:
        if expected == 'html':
            return render_template('error.html', urlroot=request.url_root,
                                   title=render_warning("Could not get affiliations " \
                                                        + "from orcid collection"),
                                   message=error_message(err))
        raise InvalidUsage(str(err), 500) from err
    if expected == 'json':
        result = initialize_result()
        result['rest']['source'] = 'mongo'
        result['data'] = []
        for row in rows:
            name = row['_id']
            authors = row['count']
            with_orcid = len(row['orcid'])
            if name in orgs:
                if orgs[name]:
                    status = 'active' if 'active' in orgs[name] else 'inactive'
                else:
                    status = 'no code'
            else:
                status = 'none'
            result['data'].append({"affiliation": name,
                                   "authors": authors,
                                   "authors_with_orcid": with_orcid,
                                   "orcid_percent": round(with_orcid / authors * 100, 2)
                                                    if authors else 0,
                                   "suporg": status})
        result['rest']['row_count'] = len(result['data'])
        return generate_response(result)
    try:
        total_authors = DB['dis'].orcid.count_documents({"affiliations": {"$ne": None}})
        total_orcid = DB['dis'].orcid.count_documents({"affiliations": {"$ne": None},
                                                       "orcid": {"$ne": None}})
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get author counts " \
                                                    + "from orcid collection"),
                               message=error_message(err))
    trows = []
    row_classes = []
    count = 0
    active = 0
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
                    active += 1
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
        trows.append([safe(link), safe(org), safe(link2), safe(perc)])
        row_classes.append(rclass)
    oa_perc = f"{total_orcid/total_authors*100:.2f}%" if total_authors else "0.00%"
    cards = stat_cards([("Affiliations", f"{count:,}"),
                        ("Active SupOrgs", f"{active:,}"),
                        ("Authors", f"{total_authors:,}"),
                        ("Authors with ORCID", f"{total_orcid:,}"),
                        ("Overall ORCID %", oa_perc)],
                       div_id='orcid-tag-stats')
    html = cards \
           + "<button class=\"btn btn-outline-warning\" " \
           + "onclick=\"$('.other').toggle();\">Filter for active SupOrgs</button>"
    html += render_table(['Affiliation', 'SupOrg', 'Authors', 'ORCID %'], trows,
                         table_id='types', css='tablesorter numbers-scroll',
                         row_classes=row_classes)
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title="Author affiliations", html=html,
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
    # Audit chart policy: route N-category colours through get_colors_by_count
    # (it applies the same Category10/20/plasma tiers this used to hand-roll, and
    # safely handles 1-2 series where Category10[<3] would KeyError).
    colors = DP.get_colors_by_count(len(top))
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
    trows = []
    cnt = 0
    active = 0
    inactive = 0
    unknown = 0
    _, suporgs = get_suporgs()
    for key, val in sorted(proj.items()):
        synonyms = []
        for tag in sorted(val):
            synonyms.append(f"<a href='/tag/{escape(tag)}'>{tag}</a>")
        if key in suporgs:
            if suporgs[key]['active']:
                status = 'Active'
                color = 'lime'
                active += 1
            else:
                status = 'Inactive'
                color = 'yellow'
                inactive += 1
        else:
            status = 'UNKNOWN'
            color = 'red'
            unknown += 1
        status = f"<span style='color:{color}'>{status}</span>"
        trows.append([safe(f"<a href='/tag/{escape(key)}'>{key}</a>"),
                      safe(', '.join(synonyms)), safe(status)])
        cnt += 1
    html = render_table(['Project', 'Synonyms', 'Supervisory Organization'], trows,
                        table_id='projects', css='tablesorter standard-scroll')
    if not cnt:
        html = "<p>No projects found</p>"
    else:
        card_items = [("Projects", f"{cnt:,}"),
                      ("Active", f"{active:,}"),
                      ("Inactive", f"{inactive:,}")]
        if unknown:
            card_items.append(("Unknown", f"{unknown:,}"))
        cards = stat_cards(card_items, div_id='proj-stats')
        html = cards + html
    title = "Project mapping"
    if option == 'full':
        title += " (all)"
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         navbar=generate_navbar('Tag/affiliation')))


@app.route('/tag/<path:aff>/<string:year>')
@app.route('/tag/<path:aff>')
def orcid_affiliation(aff, year='All'):
    '''
    Show ORCID tag (affiliation or project) information
    Browsers (Accept: text/html) get the HTML page; other clients get the matching
    ORCID records as JSON (_id and employeeId omitted), split into current/previous
    affiliations where the People system can resolve them.
    ---
    tags:
      - ORCID
    parameters:
      - in: path
        name: aff
        required: true
        schema:
          type: string
          example: Biology
        description: Affiliation or project tag name
      - in: path
        name: year
        required: true
        schema:
          type: string
          example: '2024'
        description: Publishing year to filter on. Use the /tag/{aff} form (no year) for All.
    responses:
      '200':
        description: HTML page (browser) or matching ORCID records as JSON
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/OrcidPersonList'
          text/html:
            schema:
              type: string
      '500':
        description: MongoDB error
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/Error'
    '''
    expected = 'html' if 'Accept' in request.headers \
                         and 'html' in request.headers['Accept'] else 'json'
    # Authors
    payload = {"affiliations": aff}
    try:
        cnt = DB['dis'].orcid.count_documents(payload)
        if cnt:
            rows = DB['dis'].orcid.find(payload).collation({"locale": "en"}).sort("family", 1)
    except Exception as err:
        if expected == 'html':
            return render_template('error.html', urlroot=request.url_root,
                                   title=render_warning("Could not find affiliations " \
                                                        + "in orcid collection"),
                                   message=error_message(err))
        raise InvalidUsage(str(err), 500) from err
    if expected == 'json':
        result = initialize_result()
        result['rest']['source'] = 'mongo'
        result['data'] = []
        if cnt:
            for row in rows:
                row.pop('_id', None)       # ObjectId; not JSON-serializable
                orcid_affiliations = row.pop('affiliations', [])
                eid = row.pop('employeeId', None)
                if row.get('alumni'):
                    row['previous_affiliations'] = orcid_affiliations
                elif eid:
                    try:
                        people = JRC.call_people_by_id(eid)
                    except Exception:
                        people = None
                    if people:
                        current_set = {a['supOrgName'] for a in people.get('affiliations', [])
                                       if 'supOrgName' in a}
                        cc_descr = people.get('ccDescr') or ''
                        current, previous = [], []
                        for a in orcid_affiliations:
                            if a in current_set:
                                current.append(a)
                            elif a == cc_descr:
                                row['department'] = a
                            else:
                                previous.append(a)
                        row['current_affiliations'] = current
                        row['previous_affiliations'] = previous
                    else:
                        row['previous_affiliations'] = orcid_affiliations
                else:
                    row['previous_affiliations'] = orcid_affiliations
                result['data'].append(row)
        result['rest']['row_count'] = len(result['data'])
        return generate_response(result)
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
    html, cnt, _ = standard_doi_table(rows, count_card=True)
    if cnt:
        html = htmlp + html
    else:
        html = htmlp + render_warning(f"No DOIs were found for {aff}.", 'warning')
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=aff, html=html,
                                         navbar=generate_navbar('Tag/affiliation')))


@app.route('/tagnh/<string:aff>/<string:year>')
@app.route('/tagnh/<string:aff>')
def tag_nohead(aff, year='All'):
    ''' Show ORCID tags (affiliations or projects) with counts
    '''
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
    html, cnt, oacnt = standard_doi_table(rows, prefix=f"tagnh/{aff}", count_card=True)
    chartscript, chartdiv = DP.wedge_chart({'shown': oacnt, 'total': cnt}) if oacnt else ['', '']
    if cnt:
        oamsg = f"<span style='font-size: 18pt; color: lightgray'>{oacnt/cnt*100:.1f}%</span>" \
                + f"<span style='font-size: 12pt'><br>{oacnt:,}/{cnt:,}</span>"
    else:
        oamsg = ""
        html += render_warning(f"No DOIs were found for {aff}.", 'warning')
    title = aff
    if year != 'All':
        title += f" ({year})"
    endpoint_access()
    return make_response(render_template('custom.html', urlroot=request.url_root,
                                         title=title, html=html, oamsg=oamsg,
                                         chartscript=chartscript, chartdiv=chartdiv,
                                         navbar=generate_navbar('Tag/affiliation')))

# ******************************************************************************
# * UI endpoints (acknowledgements)                                            *
# ******************************************************************************
@app.route('/acksui/<string:ack>')
def show_doi_by_ack_ui(ack):
    ''' Show DOIs for a given acknowledgement text
    '''
    union = []
    # Search all DOI types; the type cycle button in standard_ack_table defaults
    # the view to journal-articles/preprints and lets the user reveal the rest.
    payload = {}
    payload["jrc_acknowledgements"] = {"$regex": ack, "$options" : "i"}
    try:
        rows = DB['dis'].dois.find(payload).sort("jrc_publishing_date", -1)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get DOIs from dois collection"),
                               message=error_message(err))
    internal = 0
    for row in rows:
        row['doi_type'] = 'internal'
        union.append(row)
        internal += 1
    # External DOIs
    external = 0
    try:
        rows = DB['dis'].external_dois.find(payload).sort("jrc_publishing_date", -1)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get DOIs from external_dois " \
                                                    + "collection"),
                               message=error_message(err))
    for row in rows:
        row['doi_type'] = 'external'
        union.append(row)
        external += 1
    union.sort(key=lambda x: x.get("jrc_publishing_date", ""), reverse=True)
    # show_count=False: the count is shown in the card below (with id 'totalrows',
    # which the version/internal-external filters update)
    html, cnt, _ = standard_ack_table(union, ack, show_count=False)
    if not cnt:
        return render_template('warning.html', urlroot=request.url_root,
                               title=render_warning("Could not find DOIs", 'warning'),
                               message=f"Could not find any DOIs with acknowledgement {ack}")
    html = ack_stat_cards(cnt, internal, external) + html
    title = f"DOIs with acknowledgement text <span style='color:#51b447 !important'>{ack}</span>"
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         navbar=generate_navbar('Acknowledgements')))


def curator_display_name(curator):
    ''' Human-readable label for a jrc_acknowledge curator value: "IRIS (automated)"
        for machine tags, else the orcid given+family name for a userIdO365, falling
        back to the raw value when unresolved.
        Keyword arguments:
          curator: the stored curator value
        Returns:
          Display string
    '''
    if curator == 'IRIS':
        return 'IRIS (automated)'
    try:
        rec = DB['dis'].orcid.find_one({"userIdO365": curator}, {"given": 1, "family": 1})
    except Exception:
        rec = None
    if rec:
        name = f"{(rec.get('given') or [''])[0]} {(rec.get('family') or [''])[0]}".strip()
        if name:
            return name
    return curator


def curator_doi_counts():
    ''' Count distinct DOIs per jrc_acknowledge curator across dois + external_dois.
        Keyword arguments:
          None
        Returns:
          {curator: distinct-DOI count}
    '''
    pipeline = [{"$unwind": "$jrc_acknowledge"},
                {"$match": {"jrc_acknowledge.curator": {"$exists": True, "$ne": None}}},
                {"$group": {"_id": "$jrc_acknowledge.curator", "dois": {"$addToSet": "$doi"}}},
                {"$project": {"count": {"$size": "$dois"}}}]
    counts = {}
    for coll in ('dois', 'external_dois'):
        for doc in DB['dis'][coll].aggregate(pipeline):
            counts[doc['_id']] = counts.get(doc['_id'], 0) + doc['count']
    return counts


@app.route('/acks_by_curator')
@app.route('/acks_by_curator/<path:curator>')
def show_acks_by_curator(curator=None):
    ''' Acknowledgements by curator. With no curator, show a table of each curator and
        the number of DOIs they curated, each count linking to that curator's DOI list.
        With a curator, show the standard acknowledgement table of every DOI that
        curator tagged (dois + external_dois).
    '''
    if not curator:
        try:
            counts = curator_doi_counts()
        except Exception as err:
            return inspect_error(err, 'Could not aggregate curators')
        if not counts:
            return render_template('warning.html', urlroot=request.url_root,
                                   title=render_warning("No curated acknowledgements", 'warning'),
                                   message="No acknowledgement tags carry a curator yet.")
        trows = []
        for cur, cnt in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0].lower())):
            link = f"/acks_by_curator/{quote(cur, safe='')}"
            trows.append([curator_display_name(cur), safe(f"<a href='{link}'>{cnt:,}</a>")])
        html = stat_cards([("Curators", f"{len(counts):,}"),
                           ("DOIs curated", f"{sum(counts.values()):,}")],
                          div_id='curator-stats')
        html += render_table(['Curator', 'DOIs curated'], trows, table_id='curators')
        endpoint_access()
        return make_response(render_template('general.html', urlroot=request.url_root,
                                             title="Acknowledgements by curator", html=html,
                                             navbar=generate_navbar('Acknowledgements')))
    # Per-curator DOI list.
    payload = {"jrc_acknowledge.curator": curator}
    union = []
    internal = external = 0
    for coll, dtype in (('dois', 'internal'), ('external_dois', 'external')):
        try:
            rows = DB['dis'][coll].find(payload).sort("jrc_publishing_date", -1)
        except Exception as err:
            return inspect_error(err, f"Could not get DOIs from {coll}")
        for row in rows:
            row['doi_type'] = dtype
            # standard_ack_table reads jrc_acknowledgements unconditionally; a DOI tagged
            # by hand (tag_acknowledgement.py) may lack it, so default it to empty.
            row.setdefault('jrc_acknowledgements', '')
            union.append(row)
            if dtype == 'internal':
                internal += 1
            else:
                external += 1
    if not union:
        return render_template('warning.html', urlroot=request.url_root,
                               title=render_warning("No DOIs", 'warning'),
                               message=f"No DOIs were curated by {escape(curator)}.")
    union.sort(key=lambda row: row.get('jrc_publishing_date', ''), reverse=True)
    # ack='' -> no highlight (highlight_subtext no-ops on an empty subtext).
    table, cnt, _ = standard_ack_table(union, '', show_count=False)
    html = ack_stat_cards(cnt, internal, external) + table
    title = ("DOIs curated by <span style='color:#51b447 !important'>"
             f"{escape(curator_display_name(curator))}</span>")
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         navbar=generate_navbar('Acknowledgements')))


@app.route('/acks_untagged')
def show_untagged_acks():
    ''' Review queue: DOIs whose acknowledgement text mentions Janelia but carry no
        jrc_acknowledge entity tag - likely missed attributions to curate. Rendered
        with the standard ack table (Janelia mentions highlighted) across dois +
        external_dois. This is the drill-down behind the /acknowledgement_metrics
        "tagging gaps" count, and a worklist for tag_acknowledgement.py.
    '''
    payload = {"jrc_acknowledgements": {"$regex": "janelia", "$options": "i"},
               "jrc_acknowledge.0": {"$exists": False}}
    union = []
    internal = external = 0
    for coll, dtype in (('dois', 'internal'), ('external_dois', 'external')):
        try:
            rows = DB['dis'][coll].find(payload).sort("jrc_publishing_date", -1)
        except Exception as err:
            return inspect_error(err, f"Could not get DOIs from {coll}")
        for row in rows:
            row['doi_type'] = dtype
            union.append(row)
            if dtype == 'internal':
                internal += 1
            else:
                external += 1
    if not union:
        return render_template('warning.html', urlroot=request.url_root,
                               title=render_warning("No untagged acknowledgements", 'warning'),
                               message="Every Janelia-mentioning acknowledgement already "
                                       "carries an entity tag.")
    union.sort(key=lambda row: row.get('jrc_publishing_date', ''), reverse=True)
    # Highlight the Janelia mention so the curator can see the context to tag.
    table, cnt, _ = standard_ack_table(union, 'Janelia', show_count=False)
    html = ack_stat_cards(cnt, internal, external) + table
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title="Untagged acknowledgements mentioning Janelia",
                                         html=html, navbar=generate_navbar('Acknowledgements')))




@app.route('/acks_coverage')
def show_acks_coverage():
    ''' Acknowledgement-harvest coverage for internal DOIs (the dois collection): how
        many have acknowledgement text, how many were checked and had none
        (jrc_no_acknowledgements), and how many are still unchecked - the pull_internal_acks
        backlog. external_dois are acknowledgement-bearing by construction (100% covered),
        so this focuses on dois. The three buckets partition the collection.
    '''
    coll = DB['dis'].dois
    no_text = {"jrc_acknowledgements": {"$exists": False}}
    article = {"$or": [{"type": {"$in": ["journal-article", "posted-content",
                                         "proceedings-article", "book-chapter"]}},
                       {"subtype": "preprint"}]}
    try:
        total = coll.count_documents({})
        has_text = coll.count_documents({"jrc_acknowledgements": {"$exists": True}})
        checked = coll.count_documents({**no_text, "jrc_no_acknowledgements": True})
        unchecked = coll.count_documents({**no_text, "jrc_no_acknowledgements": {"$exists": False}})
        backlog = coll.count_documents({**no_text, "jrc_no_acknowledgements": {"$exists": False},
                                        **article})
    except Exception as err:
        return inspect_error(err, 'Could not compute acknowledgement coverage')

    def pct(num):
        return f"{100 * num / total:.1f}%" if total else "0%"
    cards = stat_cards([("Internal DOIs", f"{total:,}"),
                        ("With acknowledgements", f"{has_text:,}"),
                        ("Checked, none found", f"{checked:,}"),
                        ("Not yet checked", f"{unchecked:,}")], div_id='ackcov-stats')
    # Proportional coverage bar (flex-grow ratios; a zero bucket collapses to nothing).
    segs = [("With acknowledgements", has_text, '#51b447'),
            ("Checked, none found", checked, '#777'),
            ("Not yet checked", unchecked, '#f0ad4e')]
    bar = "<div style='display:flex;height:24px;border-radius:4px;overflow:hidden;margin:6px 0 4px;'>"
    for lbl, num, col in segs:
        bar += f"<div title=\"{lbl}: {num:,}\" style='flex:{num} 0 0;background:{col};'></div>"
    bar += "</div><div style='font-size:0.85em;margin-bottom:12px;'>" \
           + " &nbsp; ".join(f"<span style='color:{c};'>&#9632;</span> {l}" for l, _, c in segs) \
           + "</div>"
    trows = [["With acknowledgement text", f"{has_text:,}", pct(has_text)],
             ["Checked – no acknowledgements found", f"{checked:,}", pct(checked)],
             ["Not yet checked", f"{unchecked:,}", pct(unchecked)]]
    table = render_table(['Status', 'DOIs', '% of internal'], trows, table_id='ackcov',
                         css='tablesorter numbers-scroll')
    note = ("<div style='margin:12px 0;font-size:0.9em;color:#a8c4e0;'>Of the "
            f"{unchecked:,} not yet checked, <b>{backlog:,}</b> are article-like (the "
            "acknowledgement-fetch backlog for pull_internal_acks); the remaining "
            f"{unchecked - backlog:,} are datasets/software, which the pipeline does not "
            "fetch acknowledgements for. External DOIs are acknowledgement-bearing by "
            "construction, so this report covers the internal (dois) collection.</div>")
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title="Acknowledgement harvest coverage",
                                         html=cards + bar + table + note,
                                         navbar=generate_navbar('Acknowledgements')))


@app.route('/acksregexsearch')
def show_acks_regex_search():
    ''' Show a pulldown of all search_regex keys; selecting one navigates to
        the matching /acksregexui/<key> result page.
    '''
    # Keys live in the search_regex collection (single source of truth, shared with
    # the tag_janelia_acks.py tagger). Each doc is {key, regex, description}.
    try:
        rows = DB['dis'].search_regex.find({}).collation({"locale": "en"}).sort("key", 1)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not read search_regex"),
                               message=error_message(err))
    keys = '<option value="">Select a project or department</option>'
    for row in rows:
        key = row.get('key')
        if not key:
            continue
        # description, when present, is shown as a hover tooltip on the option
        desc = row.get('description', '')
        title = f' title="{escape(desc)}"' if desc else ''
        keys += f'<option value="{escape(key)}"{title}>{escape(key)}</option>'
    endpoint_access()
    return make_response(render_template('acks_search.html', urlroot=request.url_root,
                                         keys=keys,
                                         navbar=generate_navbar('Acknowledgements')))


@app.route('/acksregexui/<string:group>')
def show_doi_by_ack_regex_ui(group):
    ''' Show DOIs with acknowledgements matching a group's configured regex
        Keyword arguments:
          group: search_regex key
        Query arguments:
          doi_type: optional "internal" or "external" - restrict to just the
                    dois or external_dois collection instead of both
    '''
    # Regexes live in the search_regex collection (single source of truth, shared with
    # the tag_janelia_acks.py tagger); group is the document key.
    try:
        entry = DB['dis'].search_regex.find_one({"key": group})
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not read search_regex"),
                               message=error_message(err))
    if not entry or not entry.get('regex'):
        return render_template('warning.html', urlroot=request.url_root,
                               title=render_warning("No search regex", 'warning'),
                               message=f"No search regex is configured for {group}")
    regex = entry['regex']
    # Optional ?doi_type=internal|external restricts to just that collection
    # (e.g. from an acknowledgement-coverage click-through) - any other value, or no
    # param at all, keeps the default union-of-both behavior.
    doi_type = request.args.get('doi_type')
    union = []
    # Search all DOI types; the type cycle button in standard_ack_table defaults
    # the view to journal-articles/preprints and lets the user reveal the rest.
    payload = {}
    payload["jrc_acknowledgements"] = {"$regex": regex, "$options" : "i"}
    internal = 0
    if doi_type != 'external':
        try:
            rows = DB['dis'].dois.find(payload).sort("jrc_publishing_date", -1)
        except Exception as err:
            return render_template('error.html', urlroot=request.url_root,
                                   title=render_warning("Could not get DOIs from dois " \
                                                        + "collection"),
                                   message=error_message(err))
        for row in rows:
            row['doi_type'] = 'internal'
            union.append(row)
            internal += 1
    # External DOIs
    external = 0
    if doi_type != 'internal':
        try:
            rows = DB['dis'].external_dois.find(payload).sort("jrc_publishing_date", -1)
        except Exception as err:
            return render_template('error.html', urlroot=request.url_root,
                                   title=render_warning("Could not get DOIs from external_dois " \
                                                        + "collection"),
                                   message=error_message(err))
        for row in rows:
            row['doi_type'] = 'external'
            union.append(row)
            external += 1
    union.sort(key=lambda x: x.get("jrc_publishing_date", ""), reverse=True)
    # show_count=False: the count is shown in the card below (with id 'totalrows',
    # which the version/internal-external filters update)
    html, cnt, _ = standard_ack_table(union, regex, is_regex=True, show_count=False,
                                      match_key=group)
    if not cnt:
        return render_template('warning.html', urlroot=request.url_root,
                               title=render_warning("Could not find DOIs", 'warning'),
                               message=f"Could not find any DOIs with acknowledgements for {group}")
    html = ack_stat_cards(cnt, internal, external) + html
    if entry.get('description'):
        html = f"<p><i>Acknowledgements matching {entry['description']} " \
               + "(case-insensitive)</i></p>" + html
    title = f"DOIs with acknowledgements for <span style='color:#51b447 !important'>{group}</span>"
    if doi_type in ('internal', 'external'):
        title += f" ({doi_type.capitalize()} only)"
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         navbar=generate_navbar('Acknowledgements')))


@app.route('/acks_no_janelia_refs')
def acks_no_janelia_refs():
    ''' Show external_dois with Janelia acknowledgements but no Janelia DOIs
        in their reference list. Fetches all external_dois that have both
        jrc_acknowledgements and reference fields, then excludes any whose
        reference list contains at least one DOI present in the dois collection.
        Query arguments:
          year: optional publishing year (from the year pulldown); defaults to
                the current year, or "All" for no year filter
    '''
    year = request.args.get('year')
    if not year:
        year = str(datetime.now().year)
    ext_coll = DB['dis'].external_dois
    doi_coll = DB['dis'].dois
    proj = {"_id": 0, "doi": 1, "jrc_acknowledgements": 1, "reference": 1,
            "jrc_publishing_date": 1}
    match = {"jrc_acknowledgements": {"$exists": True},
             "reference": {"$exists": True}}
    if year != 'All':
        match["jrc_publishing_date"] = {"$regex": "^" + year}
    try:
        docs = list(ext_coll.find(match, proj))
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get external DOIs"),
                               message=error_message(err))
    # Build set of all Janelia DOIs for fast membership testing
    try:
        janelia_dois = set(r['doi'] for r in doi_coll.find({}, {"_id": 0, "doi": 1}))
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get Janelia DOIs"),
                               message=error_message(err))
    rows = []
    fileoutput = ""
    for doc in docs:
        refs = doc.get('reference') or []
        ref_dois = [r['DOI'].lower() for r in refs if r.get('DOI')]
        # Skip if any reference DOI is a Janelia DOI
        if any(d in janelia_dois for d in ref_dois):
            continue
        ack = doc.get('jrc_acknowledgements', '')
        published = doc.get('jrc_publishing_date', '')
        rows.append([safe(doi_link(doc['doi'])), published, escape(ack)])
        fileoutput += f"{doc['doi']}\t{published}\t{ack}\n"
    rows.sort(key=lambda r: r[1], reverse=True)
    title = "Janelia acknowledgements without Janelia references"
    if year != 'All':
        title += f" ({year})"
    # Defaults to the current year (not "All"), so pass the button value explicitly.
    selected = "(all years)" if year == 'All' else year
    pulldown = year_pulldown('acks_no_janelia_refs', query=True, selected=selected)
    if not rows:
        html = pulldown + "<br><br>" + render_warning("No matching DOIs found", 'warning')
        endpoint_access()
        return make_response(render_template('general.html', urlroot=request.url_root,
                                             title=title, html=html,
                                             navbar=generate_navbar('Acknowledgements')))
    cards = stat_cards([("External DOIs checked", f"{len(docs):,}"),
                        ("No Janelia references", f"{len(rows):,}")],
                       div_id='acks-noref-stats')
    download = create_downloadable('acks_no_janelia_refs',
                                   ['DOI', 'Published', 'Acknowledgements'], fileoutput)
    table = render_table(['DOI', 'Published', 'Acknowledgements'], rows,
                         table_id='acks-no-refs', css='tablesorter standard-scroll')
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title=title,
                                         html=pulldown + "<br><br>" + cards + download \
                                              + "<br><br>" + table,
                                         navbar=generate_navbar('Acknowledgements')))


# ******************************************************************************
# * UI endpoints (system)                                                      *
# ******************************************************************************
@app.route('/stats_database')
def stats_database():
    ''' Show database stats
    '''
    collection = {}
    total_free_storage = 0
    try:
        cnames = DB['dis'].list_collection_names()
        for cname in cnames:
            stat = DB['dis'].command('collStats', cname)
            indices = []
            for key, val in stat['indexSizes'].items():
                indices.append(f"{key} ({humansize(val, space='mem')})")
            storage_size = stat.get('storageSize', 0)
            free_storage = stat.get('freeStorageSize', 0)
            free_raw = (free_storage / storage_size * 100) if storage_size else 0.0
            total_free_storage += free_storage
            if 'avgObjSize' not in stat:
                stat['avgObjSize'] = 0
            nidx = stat.get('nindexes', len(stat['indexSizes']))
            total_idx_size = stat.get('totalIndexSize', sum(stat['indexSizes'].values()))
            data_size = stat.get('size', 0)
            ratio_raw = data_size / storage_size if storage_size else 0
            collection[cname] = {"docs": f"{stat['count']:,}",
                                 "docs_raw": stat['count'],
                                 "docsize": humansize(stat['avgObjSize'], space='mem'),
                                 "docsize_raw": stat['avgObjSize'],
                                 "datasize": humansize(data_size, space='mem'),
                                 "datasize_raw": data_size,
                                 "storagesize": humansize(storage_size, space='mem'),
                                 "storagesize_raw": storage_size,
                                 "ratio": f"{ratio_raw:.2f}x" if storage_size else "N/A",
                                 "ratio_raw": ratio_raw,
                                 "free": f"{free_raw:.2f}%",
                                 "free_raw": free_raw,
                                 "idxsize": f"{nidx} ({humansize(total_idx_size, space='mem')})",
                                 "idxsize_raw": total_idx_size,
                                 "idx": ", ".join(indices)
                                }
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get collection metrics"),
                               message=error_message(err))
    try:
        dbstat = DB['dis'].command('dbStats')
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get database metrics"),
                               message=error_message(err))
    db_data = dbstat.get('dataSize', 0)
    db_storage = dbstat.get('storageSize', 0)
    db_index = dbstat.get('indexSize', 0)
    db_ratio = f"{db_data / db_storage:.2f}x" if db_storage else "N/A"
    frag_used = min(total_free_storage, db_storage)
    summary_html = stat_cards([
        ("Collections", f"{dbstat.get('collections', 0):,}"),
        ("Documents", f"{dbstat.get('objects', 0):,}"),
        ("Uncompressed", humansize(db_data, space='mem')),
        ("On-disk", humansize(db_storage, space='mem'))])
    charts_html = (
        "<div style='display:flex;gap:2rem;align-items:flex-start;margin-bottom:1.5rem'>"
        + DP.donut_chart(dbstat.get('fsUsedSize', 0), dbstat.get('fsTotalSize', 0),
                         title="Filesystem usage", element_id="fsChart")
        + DP.donut_chart(db_storage, db_storage + db_index,
                         title="Data vs index storage", element_id="dataIdxChart",
                         include_cdn=False, labels=['Data', 'Indexes'],
                         colors=['#2ecc71', '#e74c3c'])
        + DP.donut_chart(frag_used, db_storage,
                         title="Storage fragmentation", element_id="fragChart",
                         include_cdn=False, labels=['Wasted', 'Active'])
        + "<div style='border:1px solid #555;border-radius:6px;padding:1rem;"
          "font-size:0.85em;max-width:420px;align-self:center'>"
          "<p><strong>Filesystem usage</strong> — how full the disk volume is. "
          "Includes all data on the server, not just MongoDB.</p>"
          "<p><strong>Data vs index storage</strong> — what fraction of MongoDB's "
          "footprint is document data versus indexes. A large index share may indicate "
          "over-indexing.</p>"
          "<p style='margin-bottom:0'><strong>Storage fragmentation</strong> — wasted space "
          "(red) from deleted or updated documents not yet reclaimed. A large red slice "
          "means <code>compact</code> could recover significant disk space.</p>"
          "</div>"
        + "</div>")

    trows = []
    for cname, val in sorted(collection.items()):
        empty_badge = (" <span style='color:#e74c3c !important;font-size:0.8em'>[empty]</span>"
                       if val['docs_raw'] == 0 else "")
        frag = val['free_raw']
        frag_color = "#2ecc71" if frag < 5 else ("#f39c12" if frag < 20 else "#e74c3c")
        ratio_style = "color:#f39c12 !important" if 0 < val['ratio_raw'] < 1.0 else ""
        trows.append([
            safe(f"{cname}{empty_badge}"),
            cell(val['docs'], sort=val['docs_raw']),
            cell(val['docsize'], sort=val['docsize_raw']),
            cell(val['datasize'], sort=val['datasize_raw']),
            cell(val['storagesize'], sort=val['storagesize_raw']),
            cell(val['ratio'], sort=val['ratio_raw'], style=ratio_style or None),
            cell(val['free'], sort=val['free_raw'], style=f"color:{frag_color} !important"),
            cell(val['idxsize'], sort=val['idxsize_raw']),
            safe(val['idx']),
        ])
    footer_val = {"objects": f"{dbstat.get('objects', 0):,}",
                  "avgObjSize": humansize(dbstat.get('avgObjSize', 0), space='mem'),
                  "dataSize": humansize(db_data, space='mem'),
                  "storageSize": humansize(db_storage, space='mem'),
                  "ratio": db_ratio,
                  "blank": "",
                  "indexSize": f"{dbstat.get('indexes', 0)} indices "
                               + f"({humansize(dbstat.get('indexSize', 0), space='mem')})",
                  "blank2": ""}
    footer = [fcell('Total', align='right')] \
             + [fcell(footer_val[k], align='center') for k in
                ['objects', 'avgObjSize', 'dataSize', 'storageSize', 'ratio',
                 'blank', 'indexSize', 'blank2']]
    html = (summary_html + charts_html
            + render_table(['Collection', 'Documents', 'Avg. doc size',
                            'Uncompressed size', 'On-disk size', 'Compression ratio',
                            'Fragmentation', 'Indexes', 'Index details'], trows,
                           table_id='collections', css='tablesorter numbercenter-scroll',
                           footer=footer))
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title="Database metrics", html=html,
                                         navbar=generate_navbar('System')))


@app.route('/cv')
@app.route('/cv/<string:cv>')
def cvs(cv=None):
    ''' Show CV information
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
        trows = []
        for row in rows:
            trows.append([row['name'], row['display'], row['definition'], row['format']])
        html += render_table(['Name', 'Display name', 'Definition', 'Format'], trows,
                             table_id='cvterms', css='tablesorter standard-scroll')
    endpoint_access()
    return make_response(render_template('cv.html', urlroot=request.url_root,
                                         title="Controlled vocabularies", html=html,
                                         navbar=generate_navbar('System')))


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
    trows = []
    for row in rows:
        onclick = f"onclick='nav_post(\"relation.{row['_id']}\",\"!EXISTS!\")'"
        trows.append([row['_id'], safe(f"<a href='#' {onclick}>{row['count']}</a>")])
    html += render_table(['Relationship', 'Count'], trows, table_id='crossref',
                         css='tablesorter numbers-scroll') + "</div>"
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
    trows = []
    for row in rows:
        onclick = "onclick='nav_post(\"relatedIdentifiers.relationType\",\"" + row['_id'] + "\")'"
        trows.append([row['_id'], safe(f"<a href='#' {onclick}>{row['count']}</a>")])
    html += render_table(['Relationship', 'Count'], trows, table_id='datacite',
                         css='tablesorter numbers-scroll') + "</div></div>"
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title="DOI relationships", html=html,
                                         navbar=generate_navbar('System')))


@app.route('/stats_endpoints')
def stats_endpoints():
    ''' Show endpoint stats
    '''
    payload = [{"$group": {"_id": "$endpoint", "count": {"$sum": 1}}},
               {"$sort": {"endpoint": 1}}]
    try:
        rows = DB['dis'].api_endpoint_log.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get endpoint metrics"),
                               message=error_message(err))
    trows = []
    for row in rows:
        trows.append([row['_id'], row['count']])
    html = render_table(['Endpoint', 'Count'], trows, css='tablesorter numbercenter-scroll')
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title="Endpoint access counts", html=html,
                                         navbar=generate_navbar('System')))


@app.route('/ignore')
@app.route('/ignore/<string:typ>')
def ignore(typ=None):
    ''' Show ignore information
    '''
    html = ""
    title = "Ignore list"
    try:
        rows = DB['dis'].to_ignore.distinct("type")
        cnt = len(rows)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get cvs"),
                               message=error_message(err))
    if cnt:
        html = "<form>Select an ignore list to view: <select id='type' onchange='find_ignore()'>" \
               + "<option value=''>Select a list</option>"
        for row in rows:
            if row == typ or cnt == 1:
                typ = row
                sel = "selected"
            else:
                sel = ""
            html += f"<option value=\'{row}\' {sel}>{row}</option>"
    html += "</select></form><br>"
    if typ:
        title = f"{typ} ignore list"
        try:
            irows = DB['dis'].to_ignore.find({"type": typ}).sort("key", 1)
        except Exception as err:
            return render_template('error.html', urlroot=request.url_root,
                                   title=render_warning("Could not get ignore list"),
                                   message=error_message(err))
        reason_present = False
        rows = []
        for row in irows:
            if 'reason' in row:
                reason_present = True
            rows.append(row)
        header = ['Key'] + (['Reason'] if reason_present else [])
        trows = []
        for row in rows:
            cells = [row['key']]
            if reason_present:
                cells.append(row.get('reason', ''))
            trows.append(cells)
        html += render_table(header, trows, table_id='ignores',
                             css='tablesorter standard-scroll')
    endpoint_access()
    return make_response(render_template('ignore.html', urlroot=request.url_root,
                                         title=title, html=html,
                                         navbar=generate_navbar('System')))


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
    if not cnt:
        return render_template('warning.html', urlroot=request.url_root,
                               title=render_warning("No DOIs found", 'info'),
                               message="No DOIs are awaiting processing. This isn't an error," \
                                       + " it just means that we're all caught up on " \
                                       + "DOI processing.")
    trows = []
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
              if row.get('url') else doi_link(row['doi'])
        trows.append([safe(url), row['inserted'], etime])
    html = render_table(['DOI', 'Inserted', 'Time waiting'], trows, table_id='types',
                        css='tablesorter numbers-scroll')
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title="DOIs awaiting processing", html=html,
                                         navbar=generate_navbar('System')))


@app.route('/dois_missing_oa')
def show_missing_oa():
    ''' Show DOIs missing Open Access status
    '''
    payload = [{"$match": {"jrc_oa_status": {"$exists": False}}},
               {"$sort": {"jrc_obtained_from": 1, "doi": 1}}]
    try:
        cnt = DB['dis'].dois.count_documents(payload[0]['$match'])
        rows = DB['dis'].dois.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get DOIs " \
                                                    + "missing Open Access status"),
                               message=error_message(err))
    if not cnt:
        return render_template('warning.html', urlroot=request.url_root,
                               title=render_warning("No DOIs found", 'info'),
                               message="No DOIs are missing Open Access status.")
    trows = []
    for row in rows:
        doi = row['doi']
        publisher = row.get('publisher', 'Unknown')
        journal = DL.get_journal(row)
        trows.append([safe(doi_link(doi)), row['jrc_obtained_from'], publisher, journal])
    html = render_table(['DOI', 'Source', 'Publisher', 'Journal'], trows, table_id='types',
                        css='tablesorter standard-scroll')
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title="DOIs missing Open Access status", html=html,
                                         navbar=generate_navbar('System')))


@app.route('/preprint_date_errors')
def preprint_date_errors():
    ''' Show preprints whose linked journal publication is dated BEFORE the preprint.
        This is a data-quality flag - it almost always means the publication carries an
        imprecise (year-only, defaulted to Jan 1) jrc_publishing_date that needs correcting.
    '''
    payload = {"subtype": "preprint", "jrc_preprint": {"$exists": 1}}
    coll = DB['dis'].dois
    try:
        rows = coll.find(payload).sort([("jrc_publishing_date", -1)])
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get preprint data from dois"),
                               message=error_message(err))
    flagged = []
    for row in rows:
        if len(row['jrc_preprint']) == 1:
            prep = row['jrc_preprint'][0]
        else:
            prep = None
            for pdoi in row['jrc_preprint']:
                prow = DL.get_doi_record(pdoi, coll=coll)
                if not prow:
                    continue
                if not DL.is_version(prow):
                    prep = pdoi
                    break
            if not prep:
                prep = row['jrc_preprint'][0]
        jour = DL.get_doi_record(prep, coll=coll)
        if not jour or 'jrc_publishing_date' not in jour:
            continue
        preprint_date = datetime.strptime(row['jrc_publishing_date'], '%Y-%m-%d')
        journal_date = datetime.strptime(jour['jrc_publishing_date'], '%Y-%m-%d')
        days = (journal_date - preprint_date).days
        if days >= 0:
            continue
        flagged.append((days, row, jour))
    if not flagged:
        return render_template('warning.html', urlroot=request.url_root,
                               title=render_warning("No DOIs found", 'info'),
                               message="No preprints have a linked publication dated before " \
                                       + "the preprint.")
    flagged.sort(key=lambda x: x[0])
    header = ['Preprint', 'Preprint date', 'Publication', 'Publication date', 'Days', 'Journal']
    fileoutput = ""
    trows = []
    for days, row, jour in flagged:
        journal = jour.get('jrc_journal') or DL.get_journal(jour)
        trows.append([safe(doi_link(row['doi'])), row['jrc_publishing_date'],
                      safe(doi_link(jour['doi'])), jour['jrc_publishing_date'],
                      cell(f"{days:,}", sort=days), journal])
        fileoutput += "\t".join([row['doi'], row['jrc_publishing_date'], jour['doi'],
                                 jour['jrc_publishing_date'], str(days), journal]) + "\n"
    html = render_table(header, trows, table_id='preprint_dates',
                        css='tablesorter numbers-scroll')
    cards = stat_cards([("Flagged pairs", f"{len(flagged):,}")], div_id='predate-stats')
    html = cards + create_downloadable('preprint_date_errors', header, fileoutput) \
           + "<br><br>" + html
    endpoint_access()
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         title="Publications dated before their preprint",
                                         html=html, navbar=generate_navbar('System')))


# ******************************************************************************
# * Multi-role endpoints (ORCID)                                               *
# ******************************************************************************

def _affiliation_pills(affiliations):
    ''' Render affiliation names as small inline pills (raw HTML; wrap in safe()).
        Keyword arguments:
          affiliations: a list of names, a single name string, or None
        Returns:
          HTML string of pill spans (empty string if none)
    '''
    if isinstance(affiliations, str):
        affiliations = [affiliations] if affiliations else []
    style = ("display: inline-block; background: #e9f3f3; color: #2e8b8c; "
             "border-radius: 10px; padding: 1px 9px; margin: 0 4px 3px 0; "
             "font-size: 0.82em; white-space: nowrap; text-decoration: none;")
    return ''.join(f"<a href='/tag/{quote(a, safe='')}' style='{style}'>{escape(a)}</a>"
                   for a in sorted(affiliations or [], key=str.lower))


@app.route('/labs')
def show_labs():
    '''
    Show group owners (labs) from ORCID
    Browsers (Accept: text/html) get the HTML table; other clients get the ORCID
    records that have a group (lab) as JSON (_id and employeeId omitted).
    ---
    tags:
      - ORCID
    responses:
      '200':
        description: HTML page (browser) or lab-owner ORCID records as JSON
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/OrcidPersonList'
          text/html:
            schema:
              type: string
      '500':
        description: MongoDB error
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/Error'
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
    labs = list(rows)
    # Tag-based DOI counts: one aggregation over the dois collection (jrc_tag.name).
    try:
        tagcnt = {r['_id']: r['count'] for r in DB['dis'].dois.aggregate(
            [{"$unwind": "$jrc_tag"},
             {"$group": {"_id": "$jrc_tag.name", "count": {"$sum": 1}}}])}
    except Exception:
        tagcnt = {}
    trows = []
    row_classes = []
    group_names = []
    orcid_count = former_count = 0
    for row in labs:
        group_names.append(row['group'])
        name = ' '.join([row['given'][0], row['family'][0]])
        surname_key = f"{row['family'][0]} {row['given'][0]}"
        if row.get('userIdO365'):
            name = f"<a href='/userui/{row['userIdO365']}'>{name}</a>"
        elif row.get('orcid'):
            name = f"<a href='/userui/{row['orcid']}'>{name}</a>"
        is_former = bool(row.get('alumni'))
        if is_former:
            former_count += 1
            name += f" {tiny_badge('alumni', 'Former employee')}"
        if row.get('orcid'):
            orcid_count += 1
        badges = []
        worker_badge(row, badges)
        name += f" {' '.join(badges)}"
        try:
            grow = DB['dis'].suporg.find_one({"name": row['group']})
        except Exception:
            grow = None
        glink = f"<a href='/tag/{row['group']}'>{row['group']}</a>" if grow else row['group']
        if row.get('orcid'):
            oid = row['orcid'].replace('https://orcid.org/', '')
            orcid_cell = cell(safe(f"<a href='https://orcid.org/{escape(oid)}' "
                                   f"target='_blank'>{escape(oid)}</a>"), style='width: 180px')
        else:
            orcid_cell = cell('', style='width: 180px')
        doi = tagcnt.get(row['group'], 0)
        trows.append([cell(safe(name), sort=surname_key), orcid_cell, safe(glink),
                      safe(_affiliation_pills(row.get('affiliations'))),
                      f"{doi:,}"])
        row_classes.append('former' if is_former else '')
    try:
        lab_dois = DB['dis'].dois.count_documents({"jrc_tag.name": {"$in": group_names}}) \
                   if group_names else 0
    except Exception:
        lab_dois = 0
    cards = stat_cards([("Labs", f"{len(labs):,}"),
                        ("With ORCID iD", f"{orcid_count:,}"),
                        ("Former", f"{former_count:,}"),
                        ("Lab DOIs", f"{lab_dois:,}")])
    controls = ("<div style='margin-bottom: 12px;'>"
                "<input type='text' class='form-control' "
                "style='display: inline-block; width: 260px; margin-right: 12px;' "
                "placeholder='Filter labs…' "
                "oninput=\"filterByText('labs', this, 'labscount')\">")
    if former_count:
        controls += ("<button class='btn btn-info' "
                     "onclick=\"toggleClass('labs', 'former', this, "
                     f"'Show former labs ({former_count:,})', "
                     f"'Hide former labs ({former_count:,})', 'labscount')\">"
                     f"Hide former labs ({former_count:,})</button>")
    controls += ("&nbsp;<span style='color: #6c757d;'>Showing "
                 f"<span id='labscount'>{len(labs):,}</span> labs</span></div>")
    table = render_table(['Name', 'ORCID', 'Group', 'Affiliations', 'DOIs'], trows,
                         table_id='labs', css='tablesorter numberlast-scroll',
                         row_classes=row_classes, data_attrs={"sortlist": "[[0,0]]"})
    endpoint_access()
    return render_template('general.html', urlroot=request.url_root,
                           title=f"Labs ({len(labs):,})",
                           html=cards + controls + table,
                           navbar=generate_navbar('Tag/affiliation'))

# *****************************************************************************

if __name__ == '__main__':
    if app.config["RUN_MODE"] == 'dev':
        app.run(debug=app.config["DEBUG"])
    else:
        app.run(debug=app.config["DEBUG"])
