# DIS Endpoint Navigation Map — `generate_endpoint_map.py`

Generates `endpoint_map.pdf`: a graphical map of every UI route in
`dis_responder.py` and how they navigate to one another (via
`window.location`, form POST, or async `fetch()`).

NODES and EDGES are built **dynamically at runtime** by parsing
`dis_responder.py` and the Jinja2 templates — no hardcoded route lists
are needed. Adding a new route or navigation link to the codebase will
automatically appear in the next PDF generation.

---

## Requirements

The script uses only standard scientific-Python libraries.

```
matplotlib
```

Install with:

```bash
pip install matplotlib
```

No other third-party packages are needed. The script does **not** require
`graphviz` or any native system binaries.

---

## Running

From the `api/` directory:

```bash
python3 generate_endpoint_map.py
```

Output is written to:

```
api/endpoint_map.pdf
```

The script prints `Saved: <path>  (N nodes, M edges)` on success.

---

## Output overview

The PDF is a large multi-column diagram. Open it in any PDF viewer and
zoom to the area of interest.

### Columns (left → right)

| Column | Contents |
|--------|----------|
| 0 | Entry / Home pages |
| 2 | Primary search pages (destinations of `home.html` navigation) |
| 4 | Chart / Analytics pages (`bokeh.html`) |
| 6 | DOI list & report pages (`general.html`) |
| 8 | Custom / filtered pages and pickers (`custom.html`) |
| 10 | Org, People, and ORCID sub-pages |
| 12 | Subscription pages and Admin / internal pages |
| 14 | Pure JSON API endpoints and async AJAX endpoints |

### Node colours

| Colour | Category |
|--------|----------|
| Dark blue | Entry / Home pages |
| Purple | Search / list pages (`general.html`) |
| Green | DOI detail page (`doi.html`) |
| Amber | Chart pages (`bokeh.html`) |
| Red | Custom / filtered pages (`custom.html`) |
| Violet | ORCID pages |
| Teal | Org / People pages |
| Brown | Subscription pages |
| Grey | Admin / internal pages |
| Light blue (outline) | Pure JSON API endpoints |
| Light red (outline) | Async AJAX endpoints |

### Edge (arrow) types

| Style | Meaning |
|-------|---------|
| Solid blue | `window.location` redirect (JavaScript navigation) |
| Dashed orange | Form POST submission (`nav_post()` / `nav_post_year()`) |
| Dotted red | Async `fetch()` call |

---

## How the script is structured

### File paths

```python
_HERE     = Path(__file__).parent
RESPONDER = _HERE / 'dis_responder.py'
TMPL_DIR  = _HERE / 'templates'
```

All parsing is relative to these paths, so the script always reads the
current state of the codebase.

### `COLORS` dict

Maps category name → hex colour string. Edit these to change node colours
throughout the entire diagram.

### `CLASSIFY_RULES` list

Controls which column (and category) each route is assigned to. Each
entry is a 3-tuple:

```python
(path_regex, category, column)
```

Rules are evaluated in order; the first match wins. Routes not matched by
any rule fall back to `'api'` (col 14) if they return JSON, or `'admin'`
(col 12) if they render a template.

**When to edit this list:** only when you add a new *kind* of route that
doesn't fit any existing pattern — e.g. a whole new subsystem. Ordinary
new routes within an existing category (e.g. a new `/dois_*` list page)
are picked up automatically without any changes here.

### Dynamic parsing pipeline

The script runs four parsing steps at startup to build `NODES` and
`EDGES`:

#### 1. `parse_route_groups(filepath)`

Reads `dis_responder.py` and groups consecutive `@app.route` decorators
with their function. For each group it records:

- `paths` — all route paths (e.g. `['/dois_source', '/dois_source/<year>']`)
- `methods` — HTTP methods
- `is_ui` — `True` if the function calls `render_template`
- `template` — the template filename (skipping `error.html` / `warning.html`,
  which appear before the real template in most routes due to early-return
  error handling)

#### 2. `build_nodes_and_map(groups)`

Converts route groups into `NODES` 5-tuples `(nid, label, cat, col, row)`:

- **`nid`** — derived from the shortest path: strip leading `/`, remove
  parameter segments, replace `/` with `_`
- **`label`** — all paths joined with `\n`, with Flask typed params
  simplified (`<string:year>` → `<year>`)
- **`cat` / `col`** — from `CLASSIFY_RULES`
- **`row`** — auto-assigned by enumeration order within each column

Also returns `path_to_nid`: a dict mapping normalised route prefixes to
node IDs, used for edge resolution.

#### 3. `parse_template_edges(groups, path_to_nid)`

Scans every `.html` file in `templates/` for:

| Pattern | Edge type | Example |
|---------|-----------|---------|
| `url = "/prefix/"` near `window.location` | `nav` | `home.html` search functions |
| `function nav_post` defined in template | `post` → `/doiui/custom` | `doi.html`, `general.html`, `bokeh.html`, `custom.html` |
| `fetch('/route', ...)` | `fetch` | `coauth.html`, `upload.html` |

Source nodes are the routes whose `render_template(...)` call names that
template file.

#### 4. `parse_href_edges(groups, path_to_nid)`

Scans each route function body in `dis_responder.py` for:

- **Direct `href=` patterns** — both plain strings and f-strings; the
  static prefix before the first `{` is extracted and resolved to a node
- **Helper function calls** — `find_helper_hrefs()` pre-scans all
  module-level helper functions for `href=` patterns; when a route body
  calls one of these helpers, the corresponding edge is added

#### Edge post-processing

After combining template and href edges:

1. **Deduplication** — one edge per `(src, dst, style)` triple
2. **Spurious nav removal** — `nav` edges are dropped where a `post` edge
   for the same `src → dst` already exists. This cleans up false positives
   caused by `url = "/doiui/custom"` appearing inside the `nav_post()`
   function body in template files.

### Layout constants

```python
COL_W   = 3.8   # horizontal distance between column centres (data units)
ROW_H   = 1.25  # vertical distance between row centres (data units)
BOX_W   = 3.4   # node box width (data units)
BOX_H   = 1.0   # node box height (data units)
FONT_SZ = 7.5   # node label font size (points)
```

Increase `COL_W` / `ROW_H` to spread the diagram out. Increase `BOX_W` /
`BOX_H` if labels overflow their boxes.

### Figure size (dynamic)

```python
max_col = max(col for _, _, _, col, _ in NODES)
max_row = max(row for _, _, _, _, row in NODES)
fig_w = (max_col + 1) * COL_W + 2
fig_h = (max_row + 8) * ROW_H + 3
```

The figure and axis limits grow automatically with the number of nodes —
no manual adjustment needed when routes are added.

### Legend

Positioned at `row_y(max_row + 2)`, automatically below the last row of
nodes. Two side-by-side boxes:

- **Node types** — full-width coloured bars
- **Edge types** — line samples with arrowhead markers

---

## Maintenance tasks

### Adding a new UI endpoint

Just add the `@app.route` decorator and function to `dis_responder.py` as
normal. On the next run the node appears automatically in whichever column
matches the path via `CLASSIFY_RULES`.

If the path doesn't match any existing rule, add one line to
`CLASSIFY_RULES`:

```python
(r'^/my_new_section\b', 'search', 6),
```

### Adding navigation from a new endpoint

- **JavaScript `window.location`** in a template → auto-detected
- **`nav_post()` call** in a template → auto-detected (POST edge to `/doiui/custom`)
- **`fetch('/route')`** in a template → auto-detected
- **`href=f'/route/{var}'`** in a route function body → auto-detected
- **Link via a helper function** containing `href=` → auto-detected via
  `find_helper_hrefs()`

No manual edge entries are needed for any of the above patterns.

### Changing a node's colour category

Update the matching rule in `CLASSIFY_RULES`. To add a new category,
define a new colour in `COLORS` and add it to `CAT_LABELS` in the legend
section:

```python
COLORS['new_cat'] = '#AABBCC'
# then in CAT_LABELS:
('new_cat', 'My New Category'),
```

### Adjusting arrow appearance

Edit `EDGE_STYLES`:

```python
EDGE_STYLES = {
    'nav':   dict(color='#2176AE', lw=1.8, ls='-',  arrowstyle='->', mutation_scale=12),
    'post':  dict(color='#E07B00', lw=1.4, ls='--', arrowstyle='->', mutation_scale=11),
    'fetch': dict(color='#C0392B', lw=1.8, ls=':',  arrowstyle='->', mutation_scale=13),
}
```

### Changing the output path or format

Edit the last lines of the script:

```python
out = str(_HERE / 'endpoint_map.pdf')   # change filename here
plt.savefig(out, format='pdf', ...)     # or 'png', 'svg', etc.
```

For PNG output, increase `dpi` (e.g. `dpi=300`) for a sharper raster image.

---

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| New route missing from diagram | Path doesn't match any `CLASSIFY_RULES` entry and was classified as `api` or `admin` | Add a rule to `CLASSIFY_RULES` for the new path prefix |
| New route in wrong column | `CLASSIFY_RULES` match is too broad and catches an unintended path | Make the matching regex more specific (e.g. add `\b` or lengthen the prefix) |
| New navigation edge missing | Link is not an `href=`, `window.location`, `nav_post`, or `fetch` pattern | These are the only patterns auto-detected; no other mechanism exists |
| Node box appears half-clipped on left | `xlim` left boundary too close to column-0 centre | The left `xlim` is computed as `−BOX_W/2 − 0.3`; if you increase `BOX_W` this updates automatically |
| Text appears off-centre in boxes | `fontfamily='monospace'` causes bounding-box miscalculation | Do not add `fontfamily` to the `ax.text()` call for nodes |
| Edge legend lines invisible | Lines drawn in data coordinates become sub-pixel at full zoom | The legend uses `lw=4` and `ax.plot()`; do not switch back to `FancyArrowPatch` for the legend |
| Spurious extra edges visible | Helper function body matched a route that calls it indirectly | Inspect `find_helper_hrefs()` output; rename the helper or narrow its `href=` pattern |
| Two routes produce the same node ID | Both paths reduce to the same string after stripping params and `/` | They will be merged into one node (first occurrence wins); this is expected for near-duplicate routes |
