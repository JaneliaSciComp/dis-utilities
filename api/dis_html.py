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
                "DOIs by newsletter date": "dois_newsletterpicker",
                "DOI metrics": "dois_metrics",
                "Publishing impact": "source_metrics",
                "Funders": "funders",
                "Awards": "awards",
                "DOIs by": {"Month": "dois_time/month", "Year": "dois_time/year",
                            "Journal": "journals_dois", "License": "dois_license",
                            "Publisher": "dois_publisher",
                            "Subject": {"Crossref": "crossref_subject",
                                        "DataCite": "datacite_subject",
                                        "Search": "dois_subjectpicker"}},
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
                                 "figshare title groups": "figshare_groups",
                                 "figshare users": "figshare_users"},
                    "protocols.io": {"protocols.io metrics": "protocolsio_stats",
                                     "protocols.io deposits": "protocolsio_dois",
                                     "protocols.io users": "protocolsio_users"},
                    "Dryad": {"Dryad deposits": "dryad_dois",
                              "Dryad users": "dryad_users"},
                    "Zenodo": {"Zenodo metrics": "zenodo_stats",
                               "Zenodo deposits": "zenodo_groups",
                               "Zenodo users": "zenodo_users"}},
       "Authorship": {"Authors": "orcid_entry",
                      "DOIs by authorship": "dois_author",
                      "Uncredited Janelia authors": "dois_uncredited",
                      "Authorship mismatches": "dois_authorship_mismatch",
                      "Author name mismatches": "dois_name_mismatch",
                      "ORCID/name mismatches": "orcid_mismatch",
                      "DOIs with lab head first/last authors": "doiui_firstlast",
                      "Top first and last authors": "dois_top_author",
                      "ORCID bulk search": "orcid/bulk_search",
                      "DOIs by coauthors": "coauth",
                      "DOIs without Janelia authors": "dois_no_janelia",
                      "DOIs with invalid authors": "dois_invalid_auth",
                      },
       "Related DOIs": {"Summary": "relations",
                        "All relations": "dois_related",
                        "Integrity checks": "relation_integrity",
                        "Preprints":
                            {"DOIs by preprint status": "dois_preprint",
                             "DOIs by preprint status by year": "dois_preprint_year",
                             "Preprint to publication": "preprint_lag",
                             "Preprints with journal publications": "preprint_with_pub",
                             "Preprints without journal publications":
                                 "preprint_relation/preprint_no_pub",
                             "Journal publications without preprints":
                                 "preprint_relation/pub_no_preprint"},
                        "Dataset supplements":
                            {"Datasets supplementing an article":
                                 "supplement_relation/dataset_supplements",
                             "Articles with supplementary datasets":
                                 "supplement_relation/article_supplements"},
                        "Companion resources": "dois_companion"},
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
       "Tag/affiliation": {"Tag metrics": "tag_metrics",
                           "DOIs by lab": "dois_lab",
                           "Top DOI tags by year": "dois_top",
                           "Author affiliations": {"P&C": "affiliations",
                                                   "Janelia": "janelia_affiliations"},
                           "Labs": "labs",
                           "Projects": "projects"},
       "Acknowledgements": {"Acknowledgement metrics": "acknowledgement_metrics",
                            "Search by project, department or lab": "acksregexsearch",
                            "Acknowledgements by curator": "acknowledgement_metrics?tab=curators",
                            "Acknowledgement harvest coverage": "acks_coverage",
                            "Untagged Janelia acknowledgements": "acks_untagged",
                            "Janelia acks without Janelia references": "acks_no_janelia_refs"},
       "System" : {"Database metrics": "stats_database",
                   "External systems": {"Search HHMI People system": "people",
                                        "HHMI Supervisory Organizations": "orgs/full",
                                        "ROR": "ror",
                                        "RRID": "rrid",
                                        "Janelia in OpenAlex": "openalex_stats",
                                        "Janelia in PubMed": "pubmed_stats",
                                        "API rate limits": "ratelimit",
                                        "Data sources": "data_sources"},
                   "Controlled vocabularies": "cv",
                   # Admin-only, like "cv" above. Both are hidden from anyone
                   # who cannot open them by the navbar's own filter, so they
                   # need no special casing here. Until now /authorizations was
                   # reachable only from /profile, which meant the page that
                   # grants access was the harder of the two to find.
                   "Authorizations": "authorizations",
                   # Same admin-only treatment. Listed because a service account
                   # is otherwise invisible: unlike a person's token, nobody
                   # whose profile page would show it ever signs in.
                   "Service accounts": "service_accounts",
                   # Who can reach the application and when they last did.
                   # Admin-only, gated by the decorator like its neighbours.
                   "Access": "access",
                   "DOI relationships": "doi_relationships",
                   "Endpoints": "stats_endpoints",
                   "Library versions": "system_versions",
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
    # escape(): render_warning's output is always handed to a template that
    # renders it with |safe (general.html and bokeh.html both do, for html= and
    # title= alike), so the message is a raw-HTML sink. Callers routinely build
    # it from a path segment or query parameter - a year, an ORCID, an employee
    # ID - which made every one of those a reflected XSS. No caller passes HTML
    # in (all 330 checked), so escaping here is safe and closes the whole class.
    return safe(f"<span class='fas fa-{icon} fa-{size}' style='color:{color}'></span>"
                + f"&nbsp;{escape(str(msg))}")


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


def tab_button(key, label, is_active):
    ''' One Bootstrap nav-tab <li>/<button>, shared by the tabbed metrics
        dashboards (dois_metrics, tag_metrics, acknowledgement_metrics).
        Keyword arguments:
          key: tab key (drives the id and the #target)
          label: visible button text
          is_active: whether this tab is the active one
        Returns:
          HTML for one nav-tab button
    '''
    return ('<li class="nav-item" role="presentation">'
            f'<button class="nav-link{" active" if is_active else ""}" id="{key}-tab" '
            f'data-toggle="tab" data-target="#{key}" type="button" role="tab" '
            f'aria-controls="{key}" aria-selected="{"true" if is_active else "false"}">'
            f'{label}</button></li>')


def tab_pane(key, body, is_active):
    ''' One Bootstrap tab-content pane, paired with tab_button().
        Keyword arguments:
          key: tab key (matches the button's target)
          body: pane HTML
          is_active: whether this pane is the active one
        Returns:
          HTML for one tab pane
    '''
    cls = "tab-pane fade show active" if is_active else "tab-pane fade"
    return (f'<div class="{cls}" id="{key}" role="tabpanel" '
            f'aria-labelledby="{key}-tab"><br>{body}</div>')


def two_col(left, right):
    ''' Wrap two already-styled blocks in the standard "table beside its chart"
        flex row (left column then right). Each block keeps its own width/scroll
        styling; this only supplies the shared wrapping row.
        Keyword arguments:
          left: left-column HTML (e.g. a table / scroll box)
          right: right-column HTML (e.g. a chart)
        Returns:
          HTML for the two-column flex row
    '''
    return ("<div style='display:flex;flex-wrap:wrap;gap:18px;align-items:flex-start;'>"
            f"{left}{right}</div>")


def registrar_switch(url, label):
    ''' Canonical registrar-switch button (the Crossref <-> DataCite toggle used by
        the twin pages). One wording ("Switch to <label>") and one spacing, so the
        button looks the same on every page that offers the toggle.
        Keyword arguments:
          url: target URL for the other registrar's view
          label: the other registrar's display name (e.g. "Crossref")
        Returns:
          HTML string (an anchor styled as a small outline button)
    '''
    return (f"<a href='{url}' class='btn btn-outline-primary btn-sm' "
            f"style='margin-bottom:12px'>Switch to {escape(label)}</a>")


def see_also(links):
    ''' Canonical "See also:" cross-navigation footer strip for a page family.
        Keyword arguments:
          links: list of (label, url) pairs; empty/falsy -> no strip rendered
        Returns:
          HTML string (a styled footer div), or '' if there are no links
    '''
    if not links:
        return ''
    items = " &middot; ".join(f"<a href='{url}'>{escape(label)}</a>" for label, url in links)
    return ("<div style='margin-top:16px;font-size:0.85em;color:#a8c4e0;'>See also: "
            + items + "</div>")


class Safe(str):
    ''' Marks a string as already-rendered, trusted HTML so that render_table()
        will not escape it. Plain str cells are HTML-escaped. '''
    __slots__ = ()

    def __html__(self):
        ''' Jinja's escape protocol. Without this, a Safe value handed to a
            template is indistinguishable from a plain str, which is why the
            templates reached for |safe on title= and message= and turned every
            one of those into a raw-HTML sink. With it, Safe is the single trust
            marker for both render_table() and the templates: trusted HTML
            passes through, and anything else is escaped.
        '''
        return str(self)


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
          css: table CSS class(es). Scroll-variant convention (pick by numeric-column
               layout): `standard-scroll` = text / DOI-list tables (no numeric-only
               columns to right-align); `numberlast-scroll` = a single trailing numeric
               column (e.g. a Count or Cost); `numbers-scroll` = two or more numeric
               columns. Always keep the `tablesorter` prefix.
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


def generate_navbar(active, menu=None):
    ''' Generate the web navigation bar
        Keyword arguments:
          active: heading to mark as current
          menu: menu to render, defaulting to the module's NAV. Passed in by the
               request-aware shadow in dis_responder, which drops entries the
               viewer may not open - this module stays request-free.
        Returns:
          Navigation bar
    '''
    nav = '''
    <nav class="navbar navbar-expand-lg navbar-dark bg-dark">
      <div class="collapse navbar-collapse" id="navbarSupportedContent">
        <ul class="navbar-nav mr-auto">
    '''
    # Named menu, not nav: the accumulator below is called nav, and a parameter
    # of that name is overwritten before this line is ever reached.
    for heading, subhead in (NAV if menu is None else menu).items():
        basic = '<li class="nav-item active">' if heading == active else '<li class="nav-item">'
        drop = '<li class="nav-item dropdown active">' if heading == active \
               else '<li class="nav-item dropdown">'
        # Unique id per dropdown (there are ~9): a hardcoded id="navbarDropdown" on
        # every one is invalid HTML and makes each menu's aria-labelledby resolve to
        # the first toggle, so screen readers mislabel every menu.
        did = "navbarDropdown-" + heading.lower().replace("/", "-").replace(" ", "-")
        menuhead = f'<a class="nav-link dropdown-toggle" href="#" id="{did}" ' \
                   + 'role="button" data-toggle="dropdown" aria-haspopup="true" ' \
                   + f"aria-expanded=\"false\">{heading}</a><div class=\"dropdown-menu\" "\
                   + f'aria-labelledby="{did}">'
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


def filter_frame(label, options):
    ''' A bordered control saying what the table below is showing.

        Styled by .filter-frame in main.css rather than inline, because the
        status badge taught us what happens when a style is pasted into three
        reports and then needs changing in all three.
        Keyword arguments:
          label: the compartment on the left, e.g. "Showing"
          options: list of (text, href, count, active). count may be None.
        Returns:
          HTML string
    '''
    chips = ''.join(
        f"<a class='tag-chip{' active' if active else ''}' href='{href}'>{escape(text)}"
        + (f"<span class='tag-chip-count'>{count:,}</span>" if count is not None else '')
        + "</a>"
        for text, href, count, active in options)
    return (f"<div class='filter-frame'><span class='filter-frame-label'>{escape(label)}"
            f"</span><span class='filter-frame-items'>{chips}</span></div>")


def create_downloadable(name, header, content, label=None):
    ''' Generate a downloadable content file
        Keyword arguments:
          name: base file name
          header: table header (list of strings)
          content: table content (string)
          label: button text (default "Download tab-delimited file")
        Returns:
          File name
    '''
    fname = f"{name}_{random_string()}_{datetime.today().strftime('%Y%m%d%H%M%S')}.tsv"
    with open(f"/tmp/{fname}", "w", encoding="utf8") as text_file:
        if header:
            content = "\t".join(header) + "\n" + content
        text_file.write(content)
    label = label or "Download tab-delimited file"
    # No size class: btn-med was never defined in main.css or Bootstrap, so every
    # download button in the app was carrying a class that did nothing, and the size
    # argument was a knob no caller ever turned. Bootstrap's default is the size we
    # want anyway.
    # escape(): fname is built from the caller's download_name, which on many
    # pages interpolates a path segment (a year, a provider, a journal type).
    return f'<a class="btn btn-outline-success" href="/download/{escape(fname)}" ' \
                + f'role="button">{DOWNLOAD_ICON}{label}</a>'


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
                     "<span style='color:#ffffff;font-weight:700;'>"
                     f"{escape(str(selected))}</span>")
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
        # escape(): prefix/suffix are supplied by the caller and routinely carry
        # a path segment from the request (an org, a subject, a source), so this
        # href is attacker-reachable. The attribute is single-quoted and
        # html.escape() escapes ' as well as & < > ", which is what closes it.
        html += f"<a class='dropdown-item' href='{escape(url)}'>{escape(str(year))}</a>"
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


# Below this, OpenAlex's own confidence in a topic assignment is low enough to
# flag. 14% of this corpus falls here - mostly interdisciplinary work that no
# single topic describes well.
TOPIC_MIN_CONFIDENCE = 0.6
# The app's warning amber, used elsewhere for the same purpose. Deliberately NOT
# the #88a this page uses for a minor MeSH term: an indexer calling a term
# secondary and a classifier being unsure are different statements, and one
# colour for both would say they are the same.
LOW_CONFIDENCE_COLOR = '#f0ad4e'


def openalex_topic_html(row):
    """ OpenAlex's topic for a work, with its subfield and the classifier's
        confidence

        A category, not a judgement - which is why it belongs beside MeSH and
        DataCite subjects while the field-weighted citation figure on the same
        record does not. Topic is the NARROWEST of OpenAlex's four levels
        (domain > field > subfield > topic); field and domain are omitted as
        both derive from the subfield and are too broad to inform ("Life
        Sciences" covers nearly this whole corpus).

        Both classification levels render in normal text; only the annotation
        about them is muted. The topic is NEVER greyed, deliberately. Greying it would borrow the treatment this page
        gives a minor MeSH term, and the two mean different things: a minor
        MeSH term is an indexer deciding the term is secondary to the paper,
        while a low score is a classifier unsure what the paper is about. The
        label is not less true, so it is not shown as lesser - the uncertainty
        is attached to the figure it belongs to.
        Keyword arguments:
          row: row from dois collection
        Returns:
          HTML string, or '' when the work has no topic
    """
    impact = row.get('jrc_openalex_impact') or {}
    topic = impact.get('topic')
    if not topic:
        return ""
    # Both levels render normally: a subfield is not a lesser classification
    # than a topic - it is the one FWCI normalizes against. Only the annotation
    # about them is muted.
    out = escape(topic)
    if impact.get('subfield'):
        out += f" in {escape(impact['subfield'])}"
    score = impact.get('topic_score')
    if isinstance(score, (int, float)):
        # A percentage, because the score is "the classifier's calibrated
        # probability that the topic is the work's main one" - calibrated, so
        # 82% means what it says. Worded as "the main topic" rather than
        # "confidence", because a work can legitimately be about several things
        # and a low score often means exactly that, not that the label is wrong.
        pct = f"{score * 100:.0f}%"
        if score < TOPIC_MIN_CONFIDENCE:
            out += f" <span style='color:{LOW_CONFIDENCE_COLOR}' " \
                   + "title='OpenAlex&#39;s calibrated probability that this is the " \
                   + "work&#39;s main topic'>" \
                   + f"&#9888; only {pct} likely the main topic</span>"
        else:
            out += f" <span style='color:#888' title='OpenAlex&#39;s calibrated " \
                   + "probability that this is the work&#39;s main topic'>" \
                   + f"&middot; {pct} likely the main topic</span>"
    return out


def add_subjects(row, html=None):
    ''' Add subjects to the HTML

        Two modes. With html, this builds the Subjects tab and appends every
        vocabulary the record has. Without it, it returns a single string for
        the matched-subjects column on /dois_subject - which searches DataCite
        subjects and MeSH, so an OpenAlex topic there would be a term the
        reader did not search for.
        Keyword arguments:
          row: row from dois collection
          html: HTML to add subjects to
        Returns:
          HTML with subjects added, or the bare subject string in compact mode
    '''
    heading, text, note = "", "", ""
    if row and row.get('jrc_obtained_from') == 'DataCite' and row.get('subjects'):
        heading = "DataCite subjects"
        text = ', '.join(sub['subject'] for sub in row['subjects'])
    elif row and 'jrc_mesh' in row:
        subjects = []
        for mesh in row['jrc_mesh']:
            if 'descriptor_name' not in mesh:
                continue
            if mesh.get('major_topic'):
                subj = mesh['descriptor_name']
            else:
                subj = f"<span style='color: #88a'>{mesh['descriptor_name']}</span>"
            if mesh.get('key'):
                subj = f"<a href='{NCBI_MESH}{mesh['key']}' target='_blank'>{subj}</a>"
            subjects.append(subj)
        if subjects:
            heading = "MeSH subjects"
            text = ', '.join(subjects)
            # Emitted with its own section rather than at the foot of the tab,
            # or it reads as a caption for whatever section came last - which,
            # since the OpenAlex topic was added below, it was.
            if any(not mesh.get('major_topic') for mesh in row['jrc_mesh']):
                note = "<br><i class='fa-solid fa-circle-info'></i> Subjects in " \
                       + "<span style='color: #88a'>gray-blue</span> are considered " \
                       + "minor in MeSH"
    if html is None:
        return text
    if text:
        html += f"<h4>{heading}</h4>{text}{note}"
    # Additive, not an elif: a record may carry a registrar vocabulary AND an
    # OpenAlex topic, and 1,464 Crossref DOIs have a topic with no MeSH at all -
    # an empty tab until now.
    topic = openalex_topic_html(row) if row else ""
    if topic:
        html += f"<h4>OpenAlex topic</h4>{topic}"
    return html
