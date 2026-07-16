''' dis_html.py
    HTML-generating presentation helpers for the DIS UI, extracted from
    dis_responder.py to keep that module smaller. These are pure string
    builders with no DB, request, or app coupling.
'''

import json
import random
import re
import string
from datetime import datetime
from html import escape

from dis_config import DO_NOT_DISPLAY, NCBI_MESH, PMCID
from dis_state import CVTERM


# Bold white down-arrow prefixed to download-button labels
DOWNLOAD_ICON = '<i class="fa-solid fa-arrow-down" ' \
                + 'style="color:white;-webkit-text-stroke:1px white"></i> '


# Navigation
NAV = {"Home": "",
       "DOIs": {"DOIs by insertion date": "dois_insertpicker",
                "DOI metrics": "dois_source",
                "DOIs by": {"Month": "dois_time/month", "Year": "dois_time/year",
                            "Journal": "journals_dois", "License": "dois_license",
                            "Publisher": "dois_publisher",
                            "Subject": {"Crossref": "crossref_subject",
                                        "DataCite": "datacite_subject",
                                        "Search": "dois_subjectpicker"},
                            "Type": "dois_type"},
                "DOI yearly report": "dois_yearly",
                "Citations": {"Crossref metrics": "citation_metrics/crossref",
                              "DataCite metrics": "citation_metrics/datacite",
                              "Crossref cited DOIs": "citation_list/crossref",
                              "DataCite cited DOIs": "citation_list/datacite"},
                "eLife": {"eLife metrics": "elife_stats",
                         "eLife articles": "elife_dois"}},
       "DataCite": {"DataCite DOI metrics": "datacite_dois",
                    "DataCite DOI downloads": "datacite_downloads",
                    "figshare": {"figshare metrics": "figshare_stats",
                                 "figshare title groups": "figshare_groups"},
                    "protocols.io": {"protocols.io metrics": "protocolsio_stats",
                                     "protocols.io deposits": "protocolsio_dois"},
                    "Zenodo": {"Zenodo metrics": "zenodo_stats",
                               "Zenodo deposits": "zenodo_groups"}},
       "Authorship": {"Authors": "orcid_entry",
                      "DOIs by authorship": "dois_author",
                      "DOIs with lab head first/last authors": "doiui_firstlast",
                      "Top first and last authors": "dois_top_author",
                      "ORCID bulk search": "orcid/bulk_search",
                      "DOIs by coauthors": "coauth",
                      "DOIs without Janelia authors": "dois_no_janelia",
                      "DOIs with invalid authors": "dois_invalid_auth",
                      },
       "Preprints": {"DOIs by preprint status": "dois_preprint",
                     "DOIs by preprint status by year": "dois_preprint_year",
                     "Preprints with journal publications": "preprint_with_pub",
                     "Preprints without journal publications": "preprint_relation/preprint_no_pub",
                     "Journal publications without preprints": "preprint_relation/pub_no_preprint"},
       "Journals": {"Open access": {"Report": "dois_oa", "Details": "dois_oa_details"},
                    "Top": {"Crossref": {"Publishers": "top_entities/publisher",
                                         "Journals": "top_entities/journal"},
                            "DataCite": {"Publishers": "top_entities/publisher/All/DataCite",
                                         "Journals": "top_entities/journal/All/DataCite"}},
                    "Heatmaps": {"Crossref": {"Publisher": "dois_heatmap/publisher/Crossref",
                                              "Journal": "dois_heatmap/journal/Crossref"},
                                 "DataCite": {"Publisher": "dois_heatmap/publisher/DataCite",
                                              "Journal": "dois_heatmap/journal/DataCite"},
                                 "All": {"Publisher": "dois_heatmap/publisher/All",
                                         "Journal": "dois_heatmap/journal/All"}},
                    "DOIs missing journals": "dois_nojournal",
                    "Journals referenced": "journals_referenced"},
       "Subscriptions": {"Summary": "subscriptions",
                         "Provider": {"Summary": "subscription/provider",
                                      "Cost": "subscription/cost",
                                      "APCs": "subscription/apc"},
                         "Journals": "subscriptions/type/Journal",
                         "Repositories": "subscriptions/type/Repository",
                         "Books": "subscriptions/type/Book",
                         "Book series": "subscriptions/type/Book series",
                         "Monographs": "subscriptions/type/Monograph",
                         "Missing costs": "subscription/missingcost",
                        },
       "Tag/affiliation": {"DOIs by": {"Tag": "dois_tag_ack/tag",
                                       "Lab": "dois_lab"},
                           "Top DOI tags by year": "dois_top",
                           "Author affiliations": {"P&C": "affiliations",
                                                   "Janelia": "janelia_affiliations"},
                           "Labs": "labs",
                           "Projects": "projects"},
       "Acknowledgements": {"DOIs by acknowledgement": "dois_tag_ack/ack",
                            "Acknowledgement metrics": "acknowledgement_stats",
                            "Search by project or department": "acksregexsearch",
                            "Janelia acks without Janelia references": "acks_no_janelia_refs"},
       "System" : {"Database metrics": "stats_database",
                   "External systems": {"Search HHMI People system": "people",
                                        "HHMI Supervisory Organizations": "orgs/full",
                                        "ROR": "ror",
                                        "Janelia in OpenAlex": "openalex_stats",
                                        "Janelia in PubMed": "pubmed_stats",
                                        "API rate limits": "ratelimit",
                                        "Data sources": "data_sources"},
                   "Controlled vocabularies": "cv",
                   "DOI relationships": "doi_relationships",
                   "Endpoints": "stats_endpoints",
                   "Ignore lists": "ignore",
                   "DOIs awaiting processing": "dois_pending",                   
                   "Latest hires": "orcid_datepicker",
                   "Error reports": {"DOIs missing Open Access status": "dois_missing_oa",
                                     "Publications dated before preprint": "preprint_date_errors",
                                     "Authors with multiple ORCIDs": "orcid_duplicates",
                                     "Duplicate authors": "duplicate_authors"}
                  },
      }


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


# Open Access statuses ordered least → most restrictive, for status-card display
OA_STATUS_ORDER = ('Diamond', 'Gold', 'Hybrid', 'Green', 'Bronze', 'Closed', 'Unknown')


def oa_status_rank(label):
    ''' Sort key placing OA statuses least→most restrictive (unknowns last) '''
    label = label.capitalize()
    return OA_STATUS_ORDER.index(label) if label in OA_STATUS_ORDER else len(OA_STATUS_ORDER)


def stat_cards(cards, div_id='stat-cards'):
    ''' Build a row of stat cards
        Keyword arguments:
          cards: list of (label, value) or (label, value, color) tuples; an
                 optional third element overrides the value's text color
          div_id: id for the wrapper div (scopes the link color style)
        Returns:
          HTML for the card row
    '''
    card_style = ("display:inline-block; border:1px solid #2e5c8a; border-radius:6px; "
                  "padding:12px 20px; margin:0 10px 10px 0; min-width:160px; "
                  "vertical-align:top; background:#1e3a5f;")
    label_style = "font-size:0.82em; color:#a8c4e0; margin-bottom:4px;"
    value_style = "font-size:1.35em; font-weight:bold; color:#fff;"
    html = f"<style>#{div_id} a {{color:#7eb8e8;}}</style>" \
           + f"<div id='{div_id}' style='margin-bottom:18px;'>"
    for card in cards:
        label, value = card[0], card[1]
        color = card[2] if len(card) > 2 else None
        vstyle = value_style.replace('color:#fff;', f'color:{color};') if color else value_style
        html += (f"<div style='{card_style}'>"
                 f"<div style='{label_style}'>{label}</div>"
                 f"<div style='{vstyle}'>{value}</div>"
                 f"</div>")
    html += "</div>"
    return html


class Safe(str):
    ''' Marks a string as already-rendered, trusted HTML so that render_table()
        will not escape it. Plain str cells are HTML-escaped. '''
    __slots__ = ()


def safe(value):
    ''' Mark a value as trusted HTML (see Safe) '''
    return value if isinstance(value, Safe) else Safe('' if value is None else str(value))


def _render_cell(value):
    ''' Render one table cell: pass Safe values through, HTML-escape everything else '''
    if isinstance(value, Safe):
        return str(value)
    return escape('' if value is None else str(value))


class _Cell(str):
    ''' A fully-rendered <td>...</td> (produced by cell()); render_table emits it
        verbatim instead of wrapping it again. '''
    __slots__ = ()


def cell(value, sort=None, align=None, style=None):
    ''' Build a body <td> with an optional custom sort key, alignment, and/or
        arbitrary inline style.
        Keyword arguments:
          value: cell content (escaped unless wrapped in safe())
          sort: value for tablesorter's data-sort (use the raw number for
                currency/comma-formatted columns so they sort numerically)
          align: optional text-align (e.g. 'center', 'right')
          style: optional extra inline CSS (e.g. 'color:#e74c3c !important')
        Returns:
          A fully-rendered <td> cell for use in a render_table() row
    '''
    attrs = ''
    if sort is not None:
        attrs += f' data-sort="{escape(str(sort))}"'
    styles = []
    if align:
        styles.append(f"text-align: {align}")
    if style:
        styles.append(style)
    if styles:
        attrs += f" style='{'; '.join(styles)}'"
    return _Cell(f"<td{attrs}>{_render_cell(value)}</td>")


def fcell(value, colspan=None, align=None, header=True):
    ''' Build a single <tfoot> cell for render_table()'s footer.
        Keyword arguments:
          value: cell content (escaped unless wrapped in safe())
          colspan: optional column span
          align: optional text-align (e.g. 'center', 'right')
          header: True for a <th> cell, False for a <td> cell
        Returns:
          A Safe <th>/<td> string
    '''
    tag = 'th' if header else 'td'
    attrs = f" colspan='{colspan}'" if colspan else ''
    if align:
        attrs += f" style='text-align: {align};'"
    return Safe(f"<{tag}{attrs}>{_render_cell(value)}</{tag}>")


def render_table(headers, rows, table_id=None, css="tablesorter standard-scroll",
                 row_classes=None, footer=None, width=None, data_attrs=None):
    ''' Build a standard data table.
        Keyword arguments:
          headers: list of column-header values (escaped unless wrapped in safe())
          rows: list of rows, each a list of cell values (escaped unless safe())
          table_id: optional id attribute for the <table>
          css: table CSS class(es)
          row_classes: optional list, one entry per row, giving each <tr>'s class
                       (falsy entries get no class)
          footer: optional list of <tfoot> cells. fcell() results (Safe) are used
                  verbatim; plain values become default <th> cells (escaped).
          width: optional fixed table width (px)
          data_attrs: optional dict of data-* attributes to add to the <table> tag,
                      e.g. {"sortlist": "[[0,0]]"} → data-sortlist="[[0,0]]"
        Returns:
          HTML table as a string
    '''
    idattr = f' id="{table_id}"' if table_id else ''
    idattr += f' width="{width}"' if width else ''
    if data_attrs:
        idattr += ''.join(f' data-{k}="{v}"' for k, v in data_attrs.items())
    head = ''.join(f"<th>{_render_cell(h)}</th>" for h in headers)
    body = []
    for idx, cells in enumerate(rows):
        rcls = row_classes[idx] if row_classes else None
        tr_open = f"<tr class='{rcls}'>" if rcls else "<tr>"
        body.append(tr_open
                    + ''.join(c if isinstance(c, _Cell) else f"<td>{_render_cell(c)}</td>"
                              for c in cells)
                    + "</tr>")
    foot = ''
    if footer is not None:
        cells = ''.join(c if isinstance(c, Safe) else f"<th>{_render_cell(c)}</th>"
                        for c in footer)
        foot = f"<tfoot><tr>{cells}</tr></tfoot>"
    return (f'<table{idattr} class="{css}"><thead><tr>{head}</tr></thead>'
            + f"<tbody>{''.join(body)}</tbody>{foot}</table>")


def generate_navbar_items(items):
    ''' Recursively render dropdown menu items to any nesting depth. A string
        value is a leaf link; a dict value is a nested submenu (rendered with
        the .dropdown-submenu CSS/JS, which support arbitrary depth).
        Keyword arguments:
          items: dict of label -> link string or nested dict
        Returns:
          HTML string of dropdown items
    '''
    html = ""
    for itm, val in items.items():
        if itm == 'divider':
            html += "<div class='dropdown-divider'></div>"
            continue
        if isinstance(val, dict):
            html += "<div class='dropdown-submenu'>"
            html += f"<a class='dropdown-item dropdown-toggle' href='#'>{itm}</a>"
            html += "<div class='dropdown-menu'>"
            html += generate_navbar_items(val)
            html += "</div></div>"
            continue
        link = f"/{val}" if val else ('/' + itm.replace(" ", "_")).lower()
        html += f"<a class='dropdown-item' href='{link}'>{itm}</a>"
    return html


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
            nav += generate_navbar_items(subhead)
            nav += '</div></li>'
        else:
            nav += basic
            link = ('/' + heading.replace(" ", "_")).lower()
            nav += f"<a class='nav-link' href='{link}'>{heading}</a></li>"
    nav += '</ul></div></nav>'
    return nav


def doi_link(doi, color=None):
    ''' Return a link to a DOI or DOIs
        Keyword arguments:
          doi: DOI
          color: color of the link
        Returns:
          newdoi: HTML link(s) to DOI(s) as a string
    '''
    if not doi:
        return ""
    doilist = [doi] if isinstance(doi, str) else doi
    newdoi = []
    for item in doilist:
        if color:
            newdoi.append(f"<a href='/doiui/{item}' style='color: {color};'>{item}</a>")
        else:
            newdoi.append(f"<a href='/doiui/{item}'>{item}</a>")
    if isinstance(doi, str):
        newdoi = newdoi[0]
    else:
        newdoi = ", ".join(newdoi)
    return newdoi


def make_link(url):
    ''' Create a link from a URL
        Keyword arguments:
          url: URL
        Returns:
          HTML link
    '''
    return f"<a href='{url}' target='_blank'>{url}</a>"


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


def random_string(strlen=8):
    ''' Generate a random string of letters and digits
        Keyword arguments:
          strlen: length of generated string
    '''
    cmps = string.ascii_letters + string.digits
    return ''.join(random.choice(cmps) for i in range(strlen))


def create_downloadable(name, header, content, size='btn-med'):
    ''' Generate a downloadable content file
        Keyword arguments:
          name: base file name
          header: table header (list of strings)
          content: table content (string)
        Returns:
          File name
    '''
    fname = f"{name}_{random_string()}_{datetime.today().strftime('%Y%m%d%H%M%S')}.tsv"
    with open(f"/tmp/{fname}", "w", encoding="utf8") as text_file:
        if header:
            content = "\t".join(header) + "\n" + content
        text_file.write(content)
    return f'<a class="btn btn-outline-success {size}" href="/download/{fname}" ' \
                + f'role="button">{DOWNLOAD_ICON}Download tab-delimited file</a>'


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


def year_pulldown(prefix, all_years=True, suffix = '', start_year=2006, query=False,
                  selected=None):
    ''' Generate a year pulldown
        Keyword arguments:
          prefix: navigation prefix
          all_years: if True, include all years
          suffix: suffix to add to the pulldown
          start_year: start year
          query: if True, link as /<prefix>?year=<year> (and /<prefix> for All)
                 instead of the path form /<prefix>/<year><suffix>; use when the
                 path's positional segments are reserved for something else
          selected: if given, the currently-selected value is shown on a widened
                    button (the value in bright white), instead of the generic
                    "Select publishing year" label
        Returns:
          Pulldown HTML
    '''
    years = ['All'] if all_years else []
    if start_year:
        start_year -= 1
    for year in range(datetime.now().year, start_year, -1):
        years.append(str(year))
    if selected is not None:
        # Own caret glyph in the label so the selected value can sit on the far
        # side of the arrow (separated by a bar); Bootstrap's dropdown-toggle
        # auto-caret always renders last, so drop that class here. Use a
        # .dropdown wrapper (not .btn-group): btn-group would square the
        # button's right corners for a non-:last-child, non-.dropdown-toggle
        # button. The dropdown still opens via data-toggle="dropdown".
        wrapper_class = 'dropdown'
        btn_class = "btn btn-info"
        btn_label = ("<span style='opacity:0.85;'>Publishing year</span> "
                     "<span style='opacity:0.85;'>&#9662;</span> "
                     "<span style='opacity:0.5;'>|</span> "
                     f"<span style='color:#ffffff;font-weight:700;'>{selected}</span>")
        btn_style = " style='min-width:240px;'"
    else:
        wrapper_class = 'btn-group'
        btn_class = "btn btn-info dropdown-toggle"
        btn_label = "Select publishing year"
        btn_style = ""
    html = f"<div class='{wrapper_class}'><button type='button' class='{btn_class}'" \
           + f"{btn_style} data-toggle='dropdown' aria-haspopup='true' aria-expanded='false'>" \
           + f"{btn_label}</button><div class='dropdown-menu'>"
    for year in years:
        if query:
            url = f"/{prefix}" if year == 'All' else f"/{prefix}?year={year}"
        else:
            url = f"/{prefix}/{year}{suffix}"
        html += f"<a class='dropdown-item' href='{url}'>{year}</a>"
    html += "</div></div>"
    return html



# --- Helpers moved from dis_responder.py (read CVTERM via dis_state,
# --- config constants via dis_config); pure HTML, no DB/request/app coupling.

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
        if not re.match(prog, key) or key in DO_NOT_DISPLAY:
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
        if key == 'jrc_pmc':
            val = f"<a href='{PMCID}PMC{val}/' target='_blank'>{val}</a>"
        if key == 'jrc_license' and val in CVTERM['license']:
            newval = f"{CVTERM['license'][val]['definition']}"
            if CVTERM['license'][val]['definition'] != CVTERM['license'][val]['display']:
                newval += f" ({CVTERM['license'][val]['display']})"
            val = newval
        if key == 'jrc_oa_status':
            val = f"<span class='oa_{val}' style='font-weight: bold;'>{val.capitalize()}</span>"
        html += f"<tr><td>{CVTERM['jrc'][key]['display'] if key in CVTERM['jrc'] else key}</td>" \
                + f"<td>{val}</td></tr>"
    html += "</table><br>"
    return html


def get_license(lic):
    ''' Get a license from a license string
        Keyword arguments:
          lic: license string
        Returns:
          HTML license
    '''
    if lic not in CVTERM['license']:
        return lic
    if lic == CVTERM['license'][lic]['definition']:
        return lic
    return f"{lic} ({CVTERM['license'][lic]['definition']})"


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
        if row and row['jrc_obtained_from'] == 'DataCite' and 'subjects' in row \
           and row['subjects']:
            if html:
                html += "<h4>DataCite subjects</h4>" \
                        + f"{', '.join(sub['subject'] for sub in row['subjects'])}"
            else:
                return f"{', '.join(sub['subject'] for sub in row['subjects'])}"
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
                    subj = f"<a href='{NCBI_MESH}{mesh['key']}' " \
                           + f"target='_blank'>{subj}</a>"
                subjects.append(subj)
        if subjects:
            if html:
                html += f"<h4>MeSH subjects</h4>{', '.join(subjects)}"
            else:
                return f"{', '.join(subjects)}"
    return html
