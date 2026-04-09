#!/usr/bin/env python3
"""Generate a graphical map of DIS API endpoint navigation flows.

NODES and EDGES are built dynamically by parsing dis_responder.py and
the Jinja2 templates — no hardcoded route lists needed.
"""

import re
from pathlib import Path
from collections import defaultdict

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch  # noqa: E402
import matplotlib.patheffects as pe                             # noqa: E402

# ── file paths ─────────────────────────────────────────────────────────────────
_HERE     = Path(__file__).parent
RESPONDER = _HERE / 'dis_responder.py'
TMPL_DIR  = _HERE / 'templates'

# ── colours ────────────────────────────────────────────────────────────────────
COLORS = {
    'entry':  '#2E86AB',
    'search': '#A23B72',
    'detail': '#3BB273',
    'chart':  '#F18F01',
    'custom': '#C73E1D',
    'orcid':  '#6A4C93',
    'org':    '#1B998B',
    'sub':    '#8B6914',
    'admin':  '#555B6E',
    'api':    '#E8F4F8',
    'async':  '#FFE8E8',
}

# ── route classification rules ─────────────────────────────────────────────────
# (path_regex, category, column) – first match wins
CLASSIFY_RULES = [
    # Entry / pickers
    (r'^/$|^/home$|^/<',         'entry',  0),
    (r'^/andy$',                  'entry',  0),
    (r'^/coauth$',                'entry',  0),
    (r'^/cv\b',                   'entry',  0),
    (r'^/ror\b',                  'entry',  0),
    (r'^/people$|^/people/',     'entry',  0),
    (r'^/labs\b',                 'entry',  0),
    (r'^/orcid/bulk_search',     'entry',  0),
    (r'^/dois_insertpicker',     'entry',  8),
    (r'^/dois_subjectpicker',    'entry',  8),
    (r'^/orcid_datepicker',      'entry',  8),
    # Custom/filtered (before /doiui detail)
    (r'^/doiui/custom',          'custom',  8),
    (r'^/doiui/insert',          'custom',  8),
    (r'^/doisui_type\b',         'custom',  8),
    (r'^/tagnh\b',               'custom',  8),
    (r'^/datacite_dois\b',       'custom',  8),
    # Detail
    (r'^/doiui\b',               'detail',  2),
    (r'^/pmidui\b',              'detail',  2),
    # Charts
    (r'^/dois_source\b',         'chart',   4),
    (r'^/dois_license\b',        'chart',   4),
    (r'^/dois_author\b',         'chart',   4),
    (r'^/dois_preprint_year\b',  'chart',   8),
    (r'^/dois_preprint\b',       'chart',   4),
    (r'^/dois_oa_details\b',     'chart',   4),
    (r'^/dois_oa\b',             'chart',   4),
    (r'^/dois_top\b',            'chart',   4),
    (r'^/dois_heatmap\b',        'chart',   4),
    (r'^/doiui_group\b',         'chart',   4),
    (r'^/dois_time\b',           'chart',   4),
    (r'^/top_entities\b',        'chart',   4),
    (r'^/org_year$',             'chart',   4),
    # Org sub-pages col 2
    (r'^/org_detail\b',          'org',     2),
    (r'^/org_authors\b',         'org',     2),
    (r'^/org_summary\b',         'org',     2),
    (r'^/org_year/',             'org',     2),
    # Org col 10
    (r'^/orgs\b',                'org',    10),
    (r'^/peoplerec\b',           'org',    10),
    (r'^/userui\b',              'org',    10),
    (r'^/unvaluserui\b',         'org',    10),
    (r'^/janelia_aff',           'org',    10),
    (r'^/dois_janelia',          'org',    10),
    (r'^/dois_no_janelia\b',     'org',    10),
    (r'^/duplicate_auth',        'org',    10),
    # ORCID
    (r'^/orcidui\b',             'orcid',   2),
    (r'^/orcid_entry\b',         'orcid',  10),
    (r'^/orcid/hiredate',        'orcid',  10),
    (r'^/orcid_duplicates\b',    'orcid',  10),
    (r'^/orcid_tag\b',           'orcid',  10),
    # Search col 2
    (r'^/doisui_name\b',         'search',  2),
    (r'^/titlesui\b',            'search',  2),
    (r'^/journal/',              'search',  2),
    (r'^/tag/',                  'search',  2),
    (r'^/namesui\b',             'search',  2),
    # Search col 6
    (r'^/dois_yearly\b',         'search',  6),
    (r'^/dois_report\b',         'search',  6),
    (r'^/dois_recent\b',         'search',  6),
    (r'^/dois_subject\b',        'search',  6),
    (r'^/dois_provider\b',       'search',  6),
    (r'^/dois_publisher\b',      'search',  6),
    (r'^/dois_top_cited\b',      'search',  6),
    (r'^/dois_top_author\b',     'search',  6),
    (r'^/doiui_firstlast\b',     'search',  6),
    (r'^/journals_dois\b',       'search',  6),
    (r'^/journals_referenced\b', 'search',  6),
    (r'^/dois_nojournal\b',      'search',  6),
    (r'^/dois/mytags\b',         'search',  6),
    # Search col 8
    (r'^/datacite_subject\b',    'search',  8),
    (r'^/datacite_cit',          'search',  8),
    (r'^/datacite_down',         'search',  8),
    (r'^/preprint_with_pub\b',   'search',  8),
    (r'^/preprint_relation\b',   'search',  8),
    # Subscriptions
    (r'^/subscriptions\b',       'sub',    12),
    (r'^/subscription\b',        'sub',    12),
    (r'^/subscriptionlist\b',    'sub',    12),
    # Routes that should not appear as UI nodes
    (r'^/doc$',                  'api',    14),
    (r'^/help$',                 'api',    14),
    (r'^/download\b',            'api',    14),
    # Admin
    (r'^/ignore\b',              'admin',  12),
    (r'^/dois_lab\b',            'admin',  12),
    (r'^/dois_invalid\b',        'admin',  12),
    (r'^/dois_tag_ack\b',        'admin',  12),
    (r'^/stats_database\b',      'admin',  12),
    (r'^/stats_endpoints\b',     'admin',  12),
    (r'^/ratelimit\b',           'admin',  12),
    (r'^/dois_pending\b',        'admin',  12),
    (r'^/dois_missing\b',        'admin',  12),
    (r'^/doi_relationships\b',   'admin',  12),
    (r'^/projects\b',            'admin',  12),
    (r'^/project/',              'admin',  12),
    (r'^/dois_licenser\b',       'admin',  12),
    # Async – own column so they don't crowd the API column
    (r'^/dois_coauthors\b',      'async',  16),
    (r'^/orcid/run_bulk_search', 'async',  16),
]

# ── parsing helpers ────────────────────────────────────────────────────────────

def clean_path(path):
    """Replace Flask typed params <type:name> → <name> for display."""
    return re.sub(r'<\w+:(\w+)>', r'<\1>', path)

def make_node_id(paths):
    """Derive a short stable ID from a route group's paths."""
    primary = min(paths, key=lambda p: (p.count('<'), len(p)))
    nid = primary.lstrip('/')
    nid = re.sub(r'/<[^>]+>.*', '', nid)
    nid = re.sub(r'<[^>]+>', '', nid)
    nid = nid.rstrip('/')
    nid = nid.replace('/', '_')
    return nid or 'home'

def make_label(paths):
    """Combine multiple paths into a newline-separated display label."""
    seen, unique = set(), []
    for p in (clean_path(p) for p in paths):
        if p not in seen:
            seen.add(p)
            unique.append(p)
    return '\n'.join(unique)

def classify(primary_path, is_ui):
    """Return (category, column) for a route path."""
    for pattern, cat, col in CLASSIFY_RULES:
        if re.match(pattern, primary_path):
            return cat, col
    return ('api', 14) if not is_ui else ('admin', 12)

# ── route group parser ─────────────────────────────────────────────────────────

def parse_route_groups(filepath):
    """
    Parse dis_responder.py and return a list of route-group dicts:
      paths, methods, is_ui, func_name, template

    Consecutive @app.route decorators on the same def are grouped together.
    Scans the function body to determine whether it renders a template (is_ui)
    and which template file it uses.
    """
    lines = Path(filepath).read_text(encoding='utf-8').splitlines()
    n = len(lines)
    groups = []
    i = 0
    while i < n:
        if not re.match(r'\s*@app\.route\(', lines[i]):
            i += 1
            continue
        # Collect all consecutive decorator lines
        paths, methods = [], []
        while i < n and lines[i].strip().startswith('@'):
            dec = lines[i].strip()
            # Join continuation lines when parens are unbalanced
            while dec.count('(') > dec.count(')') and i + 1 < n:
                i += 1
                dec += ' ' + lines[i].strip()
            if '@app.route(' in dec:
                m = re.search(r"['\"]([^'\"]+)['\"]", dec)
                if m:
                    paths.append(m.group(1))
                mm = re.search(r"methods=\[([^\]]+)\]", dec)
                if mm:
                    for meth in re.findall(r"['\"](\w+)['\"]", mm.group(1)):
                        if meth not in methods:
                            methods.append(meth)
            i += 1
        if not paths:
            continue
        # Skip the def line
        func_name = None
        if i < n and re.match(r'\s*def\s+', lines[i]):
            fm = re.match(r'\s*def\s+(\w+)', lines[i])
            if fm:
                func_name = fm.group(1)
            i += 1
        # Scan function body for render_template.
        # Skip error/warning templates — they appear before the real template
        # in most routes due to early-return error handling.
        _SKIP_TMPLS = {'error.html', 'warning.html'}
        is_ui, template = False, None
        while i < n:
            line = lines[i]
            if not line.strip():
                i += 1
                continue
            if line[0] not in (' ', '\t'):
                break
            if 'render_template' in line:
                is_ui = True
                tm = re.search(r"render_template\(['\"]([^'\"]+)['\"]", line)
                if tm:
                    tmpl_name = tm.group(1)
                    if tmpl_name not in _SKIP_TMPLS:
                        template = tmpl_name
                        break   # found the real template
                    # error/warning template — keep scanning
            i += 1
        groups.append({'paths': paths, 'methods': methods,
                       'is_ui': is_ui, 'func_name': func_name, 'template': template})
    return groups

# ── node builder ───────────────────────────────────────────────────────────────

def build_nodes_and_map(groups):
    """
    Convert route groups → NODES 5-tuples (nid, label, cat, col, row).
    Also returns path_to_nid mapping normalised route prefixes → node IDs.
    Rows are auto-assigned within each column in source order.
    """
    items = []
    for g in groups:
        if not g['paths']:
            continue
        primary = min(g['paths'], key=lambda p: (p.count('<'), len(p)))
        cat, col = classify(primary, g['is_ui'])
        nid   = make_node_id(g['paths'])
        label = make_label(g['paths'])
        # Prefix API/async labels with HTTP method for clarity
        if cat in ('api', 'async'):
            meths = sorted({m for m in g['methods'] if m not in ('OPTIONS', 'HEAD')})
            if meths:
                label = '/'.join(meths) + ' ' + label
        items.append({'nid': nid, 'label': label, 'cat': cat, 'col': col,
                      'paths': g['paths']})

    # Deduplicate by nid – earlier route definition wins
    seen, deduped = set(), []
    for item in items:
        if item['nid'] not in seen:
            seen.add(item['nid'])
            deduped.append(item)

    # Auto-assign rows within each column, sorted alphabetically by nid
    col_groups = defaultdict(list)
    for item in deduped:
        col_groups[item['col']].append(item)

    nodes = []
    for col in sorted(col_groups.keys()):
        for row_idx, item in enumerate(sorted(col_groups[col],
                                              key=lambda x: x['nid'])):
            nodes.append((item['nid'], item['label'], item['cat'],
                          col, row_idx * 2))

    # Build normalised path → nid lookup
    path_to_nid = {}
    for item in deduped:
        for raw in item['paths']:
            norm = re.sub(r'/<[^>]+>', '', raw).rstrip('/')
            if norm:
                path_to_nid[norm] = item['nid']

    return nodes, path_to_nid

# ── edge helpers ───────────────────────────────────────────────────────────────

def resolve_nid(url_prefix, path_to_nid, unresolved=None):
    """Map a URL prefix to the best-matching node ID.

    If unresolved is a set, any prefix that cannot be matched is added to it
    so callers can report gaps in CLASSIFY_RULES or path_to_nid.
    """
    prefix = url_prefix.rstrip('/')
    if prefix in path_to_nid:
        return path_to_nid[prefix]
    parts = prefix.split('/')
    while len(parts) > 1:
        candidate = '/'.join(parts)
        if candidate in path_to_nid:
            return path_to_nid[candidate]
        parts.pop()
    if unresolved is not None:
        unresolved.add(url_prefix)
    return None

def find_helper_hrefs(filepath):
    """
    Pre-scan module-level helper functions (not @app.route handlers) and
    return {func_name: {url_prefix, ...}} for any href= patterns they contain.
    These are detected later when route bodies call those helpers.
    """
    lines = Path(filepath).read_text(encoding='utf-8').splitlines()
    n = len(lines)
    helper_hrefs = {}
    i = 0
    while i < n:
        fm = re.match(r'^def\s+(\w+)', lines[i])
        if not fm:
            i += 1
            continue
        fname = fm.group(1)
        hrefs = set()
        j = i + 1
        while j < n:
            bl = lines[j]
            if not bl.strip():
                j += 1
                continue
            if bl[0] not in (' ', '\t'):
                break
            for m in re.finditer(r"""href=f?['"]?(/[\w/{}._-]+)""", bl):
                raw = m.group(1)
                prefix = raw.split('{')[0].rstrip('/')
                if prefix:
                    hrefs.add(prefix)
            j += 1
        if hrefs:
            helper_hrefs[fname] = hrefs
        i = j
    return helper_hrefs

# ── template edge parser ───────────────────────────────────────────────────────

# Categories that meaningfully expose nav_post() filter controls.
# Admin, orcid, sub, and async pages render templates with nav_post defined
# but don't actually expose the filter UI — suppress their post edges.
_POST_CATS = {'chart', 'search', 'detail', 'custom', 'entry'}

def parse_template_edges(groups, path_to_nid, nid_to_cat, unresolved=None):
    """
    Parse every template file for JS navigation patterns:
      - url = "/prefix/"               → window.location nav edge
      - window.location[.href] = "/"  → direct location assignment nav edge
      - function nav_post              → POST edge to /doiui/custom (filtered by category)
      - fetch('/route'                 → async fetch edge
    Source nodes are the routes that render each template.
    """
    edges = []
    tmpl_to_nodes = defaultdict(list)
    for g in groups:
        if g['template'] and g['paths']:
            nid = make_node_id(g['paths'])
            if nid not in tmpl_to_nodes[g['template']]:
                tmpl_to_nodes[g['template']].append(nid)

    for tmpl_file in sorted(TMPL_DIR.glob('*.html')):
        text      = tmpl_file.read_text(encoding='utf-8')
        tmpl_name = tmpl_file.name
        src_nodes = tmpl_to_nodes.get(tmpl_name, [])

        # url = "/static/prefix/" near window.location
        for m in re.finditer(r'''url\s*=\s*["'](/[\w/]+)["']''', text):
            prefix = m.group(1)
            dst = resolve_nid(prefix, path_to_nid, unresolved)
            if dst:
                for src in src_nodes:
                    if src != dst:
                        edges.append((src, dst, 'nav',
                                      f'window.location → {prefix}'))

        # Direct window.location[.href] = "/hardcoded/path" assignments
        for m in re.finditer(
                r'''window\.location(?:\.href)?\s*=\s*["'](/[\w/]+)["']''', text):
            prefix = m.group(1)
            dst = resolve_nid(prefix, path_to_nid, unresolved)
            if dst:
                for src in src_nodes:
                    if src != dst:
                        edges.append((src, dst, 'nav',
                                      f'window.location.href → {prefix}'))

        # nav_post / nav_post_year → POST to /doiui/custom
        # Only emit for pages that actually expose the filter controls
        if re.search(r'function nav_post', text):
            dst = path_to_nid.get('/doiui/custom')
            if dst:
                for src in src_nodes:
                    if src != dst and nid_to_cat.get(src) in _POST_CATS:
                        edges.append((src, dst, 'post', 'nav_post()'))

        # fetch('/route', {...})
        for m in re.finditer(r"""fetch\(\s*['"]([^'"]+)['"]""", text):
            prefix = m.group(1)
            dst = resolve_nid(prefix, path_to_nid, unresolved)
            if dst:
                for src in src_nodes:
                    edges.append((src, dst, 'fetch',
                                  f'fetch POST → {prefix}'))

    return edges

# ── Python href edge parser ────────────────────────────────────────────────────

def parse_href_edges(path_to_nid, func_to_nid, unresolved=None):
    """
    Scan each route function body in dis_responder.py for:
      - Direct href= patterns (including f-strings)
      - Calls to helper functions that are known to generate <a href> links
      - redirect(url_for('view_name')) server-side redirects
    """
    helper_hrefs = find_helper_hrefs(RESPONDER)
    lines = Path(RESPONDER).read_text(encoding='utf-8').splitlines()
    n = len(lines)
    edges = []
    i = 0

    while i < n:
        if not re.match(r'\s*@app\.route\(', lines[i]):
            i += 1
            continue
        # Collect paths for this route group
        func_paths = []
        while i < n and lines[i].strip().startswith('@'):
            dec = lines[i].strip()
            m = re.search(r"['\"]([^'\"]+)['\"]", dec)
            if m and '@app.route(' in dec:
                func_paths.append(m.group(1))
            i += 1
        if not func_paths:
            continue
        src_nid = make_node_id(func_paths)
        # Skip def line
        if i < n and re.match(r'\s*def\s+', lines[i]):
            i += 1
        # Scan function body
        while i < n:
            line = lines[i]
            if not line.strip():
                i += 1
                continue
            if line[0] not in (' ', '\t'):
                break
            # href= patterns (direct and f-string)
            for m in re.finditer(r"""href=f?['"]?(/[\w/{}._-]+)""", line):
                raw    = m.group(1)
                prefix = raw.split('{')[0].rstrip('/')
                if not prefix:
                    continue
                dst = resolve_nid(prefix, path_to_nid, unresolved)
                if dst and dst != src_nid:
                    edge = (src_nid, dst, 'nav', f'<a href> {prefix}')
                    if edge not in edges:
                        edges.append(edge)
            # Calls to link-generating helper functions
            for m in re.finditer(r'\b(\w+)\s*\(', line):
                hname = m.group(1)
                if hname in helper_hrefs:
                    for prefix in helper_hrefs[hname]:
                        dst = resolve_nid(prefix, path_to_nid, unresolved)
                        if dst and dst != src_nid:
                            edge = (src_nid, dst, 'nav',
                                    f'<a href> {hname}()')
                            if edge not in edges:
                                edges.append(edge)
            # redirect(url_for('view_function')) server-side redirects
            for m in re.finditer(r"""url_for\(\s*['"](\w+)['"]""", line):
                view = m.group(1)
                dst  = func_to_nid.get(view)
                if dst and dst != src_nid:
                    edge = (src_nid, dst, 'nav',
                            f'redirect(url_for({view!r}))')
                    if edge not in edges:
                        edges.append(edge)
            i += 1

    return edges

# ── build NODES and EDGES ──────────────────────────────────────────────────────

_groups             = parse_route_groups(RESPONDER)
NODES, _path_to_nid = build_nodes_and_map(_groups)

# nid → category map required by parse_template_edges post-edge filtering
_nid_to_cat  = {nid: cat for nid, _, cat, _, _ in NODES}
# Flask view-function name → node ID for redirect(url_for(...)) resolution
_func_to_nid = {g['func_name']: make_node_id(g['paths'])
                for g in _groups if g['func_name'] and g['paths']}
# Collect URL prefixes that could not be resolved to any node
_unresolved: set = set()

_raw_edges = (parse_template_edges(_groups, _path_to_nid, _nid_to_cat, _unresolved)
            + parse_href_edges(_path_to_nid, _func_to_nid, _unresolved))

# Deduplicate edges (keep first occurrence of each src/dst/style triple)
_seen, EDGES = set(), []
for _e in _raw_edges:
    key = (_e[0], _e[1], _e[2])
    if key not in _seen:
        _seen.add(key)
        EDGES.append(_e)

# Drop spurious 'nav' edges where a 'post' edge for the same src→dst exists.
# These arise because nav_post() in templates contains url="/doiui/custom"
# which the window.location scanner picks up as a nav edge.
_post_pairs = {(_e[0], _e[1]) for _e in EDGES if _e[2] == 'post'}
EDGES = [_e for _e in EDGES
         if not (_e[2] == 'nav' and (_e[0], _e[1]) in _post_pairs)]

# Remove orphan nodes (no incoming or outgoing edges) — suppress from diagram
_connected   = {nid for src, dst, _, _ in EDGES for nid in (src, dst)}
_orphan_nids = sorted(nid for nid, *_ in NODES if nid not in _connected)
NODES        = [n for n in NODES if n[0] in _connected]

# ── layout parameters ──────────────────────────────────────────────────────────
COL_W   = 3.8
ROW_H   = 1.25
BOX_W   = 3.4
BOX_H   = 1.0
FONT_SZ = 7.5

def col_x(col):
    return col * COL_W

def row_y(row):
    return -row * ROW_H

# ── node-position lookup ───────────────────────────────────────────────────────
pos = {nid: (col_x(col), row_y(row))
       for nid, _, _, col, row in NODES}

# ── dynamic figure sizing ──────────────────────────────────────────────────────
max_col = max((col for _, _, _, col, _ in NODES), default=14)
max_row = max((row for _, _, _, _, row in NODES), default=60)
mid_col = col_x(max_col // 2)

fig_w = (max_col + 1) * COL_W + 2
fig_h = (max_row + 8) * ROW_H + 3
fig, ax = plt.subplots(figsize=(fig_w, fig_h))
ax.set_xlim(-BOX_W / 2 - 0.3, col_x(max_col + 1) + 0.5)
ax.set_ylim(row_y(max_row + 8), 2.4)
ax.axis('off')
ax.set_facecolor('#F7F9FC')
fig.patch.set_facecolor('#F7F9FC')

# ── title & subtitle ───────────────────────────────────────────────────────────
ax.text(mid_col, 2.15,
        'DIS Responder — UI Endpoint Navigation Map',
        ha='center', va='center', fontsize=16, fontweight='bold', color='#1A1A2E')
ax.text(mid_col, 1.75,
        'Blue arrows = window.location redirect  |  '
        'Orange dashed = form POST  |  '
        'Red dotted = async fetch()',
        ha='center', va='center', fontsize=8.5, color='#555')

# ── column headers ─────────────────────────────────────────────────────────────
HEADERS = [
    (0,  'Entry Points'),
    (2,  'Primary Search\n(from Home)'),
    (4,  'Chart / Analytics\n(bokeh.html)'),
    (6,  'DOI List & Report\n(general.html)'),
    (8,  'Custom / Picker\n(custom.html)'),
    (10, 'Org & ORCID\nSub-pages'),
    (12, 'Subscriptions\n& Admin'),
    (14, 'Pure API\n(JSON)'),
    (16, 'Async\n(fetch)'),
]
_col_node_count = defaultdict(int)
for _, _, _, _col, _ in NODES:
    _col_node_count[_col] += 1

for col, header in HEADERS:
    count = _col_node_count.get(col, 0)
    ax.text(col_x(col), 1.25, f'{header}\n({count})',
            ha='center', va='center', fontsize=8.5, fontweight='bold',
            color='#1A1A2E',
            bbox=dict(boxstyle='round,pad=0.3', fc='#DDE8F0',
                      ec='#99B8CC', lw=0.8))

# ── draw nodes ─────────────────────────────────────────────────────────────────
DARK_CATS = {'entry', 'search', 'detail', 'chart', 'custom',
             'orcid', 'org', 'sub', 'admin'}

# In-degree count: nodes with many incoming edges are highlighted as hubs
_in_degree = defaultdict(int)
for _src, _dst, _, _ in EDGES:
    _in_degree[_dst] += 1
_HUB_THRESHOLD = 5  # edges needed to be considered a hub

for nid, label, cat, col, row in NODES:
    cx, cy = pos[nid]
    fc  = COLORS.get(cat, '#CCCCCC')
    tc  = 'white' if cat in DARK_CATS else '#1A1A2E'
    lw  = 1.0
    ec  = 'white'
    if cat in ('api', 'async'):
        ec = '#99B8CC' if cat == 'api' else '#FFB3B3'
        lw = 0.8
    is_hub = _in_degree[nid] >= _HUB_THRESHOLD
    if is_hub:
        ec  = '#FFD700'   # gold border marks high-traffic hub nodes
        lw  = 2.5
    patch = FancyBboxPatch(
        (cx - BOX_W/2, cy - BOX_H/2), BOX_W, BOX_H,
        boxstyle='round,pad=0.08',
        facecolor=fc, edgecolor=ec, linewidth=lw, zorder=3)
    if is_hub:
        patch.set_path_effects(
            [pe.withStroke(linewidth=lw + 2.5, foreground='black')])
    ax.add_patch(patch)
    ax.text(cx, cy, label,
            ha='center', va='center',
            fontsize=FONT_SZ, color=tc, zorder=4,
            multialignment='center')

# ── draw edges ─────────────────────────────────────────────────────────────────
EDGE_STYLES = {
    'nav':   dict(color='#2176AE', lw=1.8, ls='-',
                  arrowstyle='->', mutation_scale=12),
    'post':  dict(color='#E07B00', lw=1.4, ls='--',
                  arrowstyle='->', mutation_scale=11),
    'fetch': dict(color='#C0392B', lw=1.8, ls=':',
                  arrowstyle='->', mutation_scale=13),
}
edge_counts = defaultdict(int)

for src_id, dst_id, style, _label in EDGES:
    if src_id not in pos or dst_id not in pos:
        continue
    x0, y0 = pos[src_id]
    x1, y1 = pos[dst_id]
    st = EDGE_STYLES.get(style, EDGE_STYLES['nav'])
    dx, dy = x1 - x0, y1 - y0
    if (dx**2 + dy**2) == 0:
        continue
    sx = x0 + BOX_W/2 if x1 > x0 else (x0 - BOX_W/2 if x1 < x0 else x0)
    ex = x1 - BOX_W/2 if x1 > x0 else (x1 + BOX_W/2 if x1 < x0 else x1)
    key = (src_id, dst_id)
    vy  = edge_counts[key] * 0.06
    edge_counts[key] += 1
    ax.add_patch(FancyArrowPatch(
        (sx, y0 + vy), (ex, y1 + vy),
        connectionstyle='arc3,rad=0.0' if abs(dy) < 0.5 else 'arc3,rad=0.15',
        arrowstyle=st['arrowstyle'], mutation_scale=st['mutation_scale'],
        color=st['color'], linewidth=st['lw'], linestyle=st['ls'],
        zorder=2, alpha=0.7))

# ── column separators ──────────────────────────────────────────────────────────
for c in [1, 3, 5, 7, 9, 11, 13, 15]:
    ax.axvline(col_x(c), color='#CCD5E0', lw=0.5, ls='--', zorder=1)

# ── legend ─────────────────────────────────────────────────────────────────────
ITEM_H  = 0.65
LINE_LEN= 1.60
LEG_PAD = 0.30
LEG_GAP = 0.60
BAR_W   = 4.0
BAR_H   = 0.45

legend_x = col_x(0) + 0.5
legend_y = row_y(max_row + 2)

CAT_LABELS = [
    ('entry',  'Entry / Home pages'),
    ('search', 'Search / List pages  (general.html)'),
    ('detail', 'DOI Detail  (doi.html)'),
    ('chart',  'Chart pages  (bokeh.html)'),
    ('custom', 'Custom / Filtered  (custom.html)'),
    ('orcid',  'ORCID pages'),
    ('org',    'Org / People pages'),
    ('sub',    'Subscription pages'),
    ('admin',  'Admin / Internal'),
    ('api',    'Pure JSON API endpoints'),
    ('async',  'Async AJAX endpoints (fetch)'),
]
EDGE_ITEMS = [
    ('nav',   'window.location redirect'),
    ('post',  'Form POST submission'),
    ('fetch', 'Async fetch() call'),
]

node_inner_h = 0.5 + len(CAT_LABELS)  * ITEM_H
edge_inner_h = 0.5 + len(EDGE_ITEMS)  * ITEM_H
node_box_w   = BAR_W + 2 * LEG_PAD
edge_box_w   = LEG_PAD + LINE_LEN + 0.25 + 2.20 + LEG_PAD

nl_left   = legend_x - LEG_PAD
nl_right  = nl_left + node_box_w
nl_top    = legend_y + 0.6
nl_bottom = nl_top - node_inner_h - LEG_PAD

el_left   = nl_right + LEG_GAP
el_right  = el_left + edge_box_w
el_top    = nl_top
el_bottom = el_top - edge_inner_h - LEG_PAD

# Node-types box
ax.add_patch(FancyBboxPatch(
    (nl_left, nl_bottom), nl_right - nl_left, nl_top - nl_bottom,
    boxstyle='round,pad=0.1', facecolor='#F0F4F8',
    edgecolor='#7AABCC', lw=1.8, zorder=4))
ax.text((nl_left + nl_right) / 2, legend_y + 0.38, 'Node types:',
        ha='center', fontsize=8.5, fontweight='bold', color='#1A1A2E', zorder=5)

for i, (cat, lbl) in enumerate(CAT_LABELS):
    yy = legend_y - i * ITEM_H
    tc = 'white' if cat in DARK_CATS else '#1A1A2E'
    ax.add_patch(FancyBboxPatch(
        (legend_x, yy - BAR_H / 2), BAR_W, BAR_H,
        boxstyle='round,pad=0.05',
        facecolor=COLORS[cat], edgecolor='white', lw=0.6, zorder=5))
    ax.text(legend_x + BAR_W / 2, yy, lbl,
            ha='center', va='center', fontsize=7.5, color=tc, zorder=6)

# Edge-types box
ax.add_patch(FancyBboxPatch(
    (el_left, el_bottom), el_right - el_left, el_top - el_bottom,
    boxstyle='round,pad=0.1', facecolor='#F0F4F8',
    edgecolor='#7AABCC', lw=1.8, zorder=4))
el_x = el_left + LEG_PAD
ax.text((el_left + el_right) / 2, el_top - 0.28, 'Edge types:',
        ha='center', fontsize=8.5, fontweight='bold', color='#1A1A2E', zorder=5)

for i, (style, lbl) in enumerate(EDGE_ITEMS):
    st = EDGE_STYLES[style]
    yy = el_top - 0.72 - i * ITEM_H
    ax.plot([el_x, el_x + LINE_LEN], [yy, yy],
            color=st['color'], lw=4, linestyle=st['ls'],
            solid_capstyle='round', zorder=5)
    ax.plot([el_x + LINE_LEN], [yy], marker='>',
            markersize=9, color=st['color'], zorder=5, linestyle='none')
    ax.text(el_x + LINE_LEN + 0.25, yy, lbl,
            va='center', fontsize=7.5, color='#1A1A2E', zorder=5)

# ── save ───────────────────────────────────────────────────────────────────────
out = str(_HERE / 'endpoint_map.pdf')
plt.savefig(out, format='pdf', bbox_inches='tight', dpi=150)
print(f"Saved: {out}  ({len(NODES)} nodes, {len(EDGES)} edges)")

# Orphan report — nodes hidden from diagram because they have no edges
if _orphan_nids:
    print(f"\nOrphan nodes hidden from diagram (no edges — {len(_orphan_nids)}):")
    for _o in _orphan_nids:
        print(f"  {_o}")

# Unresolved URL report — URLs that parse_template_edges / parse_href_edges
# could not map to any node (gaps in CLASSIFY_RULES or path_to_nid)
if _unresolved:
    _skip = {'/', ''}   # expected non-routes
    _report = sorted(_unresolved - _skip)
    if _report:
        print(f"\nUnresolved URLs ({len(_report)}) — consider adding CLASSIFY_RULES entries:")
        for _u in _report:
            print(f"  {_u}")
