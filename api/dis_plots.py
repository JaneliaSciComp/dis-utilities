''' dis_plots.py
    Plot functions for the DIS UI
'''

import colorsys
import json
from math import ceil, pi
import numpy as np
from bokeh.colors import named as _bokeh_named
from bokeh.models import (BasicTicker, ColorBar, CustomJS, HoverTool, LabelSet, LinearAxis,
                          LinearColorMapper, ColumnDataSource, NumeralTickFormatter,
                          PrintfTickFormatter, Range1d, TapTool)
from bokeh.embed import components
from bokeh.palettes import all_palettes, plasma, Turbo256
from bokeh.plotting import figure
from bokeh.transform import cumsum, transform
import pandas as pd


# Tap-to-navigate callback shared by the bar charts. Clicking a glyph either
# follows a URL (GET) or submits a hidden form to /doiui/custom (POST, the app's
# DOI drill-down endpoint, which is POST-only). Inputs are set as DOM properties
# (not interpolated HTML) so labels with quotes/ampersands can't break the form.
_NAV_TAP_JS = """
    const inds = cb_obj.indices;
    if (!inds || !inds.length) { return; }
    const d = src.data;
    const i = inds[0];
    const url = d['_nav_url'][i];
    if (url) { window.location.href = url; return; }
    const field = d['_nav_field'][i];
    if (!field) { return; }
    const form = document.createElement('form');
    form.method = 'POST';
    form.action = '/doiui/custom';
    form.style.display = 'none';
    const add = (n, v) => {
        const e = document.createElement('input');
        e.type = 'hidden';
        e.name = n;
        e.value = v;
        form.appendChild(e);
    };
    add('field', field);
    add('value', d['_nav_value'][i]);
    if (d['_nav_source'][i]) { add('jrc_obtained_from', d['_nav_source'][i]); }
    document.body.appendChild(form);
    form.submit();
"""


def _make_clickable(plot, source, keys, nav, renderers=None):
    ''' Wire a TapTool so clicking a glyph navigates to a drill-down target.
        Keyword arguments:
          plot:      the Bokeh figure
          source:    the ColumnDataSource backing the glyphs
          keys:      list of category keys aligned to the source rows
          nav:       dict mapping a key (matched by str()) to either a URL string
                     (GET navigation) or a dict {"field":.., "value":.., "source":..}
                     (POST to /doiui/custom). Keys with no entry stay inert.
          renderers: glyph renderers the TapTool should hit (default: all)
        Adds hidden _nav_* columns to `source` and a tap callback. No-op when
        `nav` is falsy. Must be called before components().
    '''
    if not nav:
        return
    urls, fields, values, sources = [], [], [], []
    for key in keys:
        target = nav.get(str(key))
        if isinstance(target, str):
            urls.append(target)
            fields.append("")
            values.append("")
            sources.append("")
        elif isinstance(target, dict):
            urls.append("")
            fields.append(str(target.get("field", "")))
            values.append(str(target.get("value", "")))
            sources.append(str(target.get("source", "")))
        else:
            urls.append("")
            fields.append("")
            values.append("")
            sources.append("")
    source.data["_nav_url"] = urls
    source.data["_nav_field"] = fields
    source.data["_nav_value"] = values
    source.data["_nav_source"] = sources
    plot.add_tools(TapTool(renderers=renderers) if renderers else TapTool())
    source.selected.js_on_change('indices', CustomJS(args={"src": source},
                                                     code=_NAV_TAP_JS))


def _darken_color(color, factor=0.7):
    ''' Return a darkened version of a CSS named or hex color.
        Keyword arguments:
          color: CSS color name or hex string
          factor: multiplier applied to the HLS lightness (default 0.7)
        Returns:
          Hex color string
    '''
    if color.startswith('#'):
        r, g, b = int(color[1:3], 16), int(color[3:5], 16), int(color[5:7], 16)
    else:
        c = getattr(_bokeh_named, color.lower().replace(' ', '_'), None)
        if c is None:
            return color
        r, g, b = c.r, c.g, c.b
    h, lgt, s = colorsys.rgb_to_hls(r / 255, g / 255, b / 255)
    r2, g2, b2 = colorsys.hls_to_rgb(h, max(0.0, lgt * factor), s)
    return f'#{int(r2 * 255):02x}{int(g2 * 255):02x}{int(b2 * 255):02x}'


OA_COLORS = {"Bronze": "#CD7F32", "Closed": "red", "Diamond": "lightgray",
             "Gold": "#FFD700", "Green": "green", "Hybrid": "cyan"}
SOURCE_PALETTE = ["mediumblue", "darkorange"]
SOURCE3_PALETTE = ["mediumblue", "darkorange", "wheat"]
TYPE_PALETTE = ["mediumblue", "darkorange", "wheat", "darkgray"]

def make_stretched_palette(palette, low_frac=0.16, low_share=0.50, count=256):
    ''' Build an n-color palette with increased color separation in the lower range.
        Keyword arguments:
          palette: palette to stretch
          low_frac: bottom fraction of data range to stretch (0.2 = lower 20%)
          low_share: fraction of palette colors allocated to that range (0.5 = 50%)
          count: number of colors to return
        Returns:
          List of colors
    '''
    src = list(palette)
    src_len = len(src)
    indices = []
    for i in range(count):
        t = i / (count - 1)
        if t <= low_frac:
            src_t = (t / low_frac) * low_share
        else:
            src_t = low_share + ((t-low_frac) / (1-low_frac)) * (1-low_share)
        indices.append(min(int(src_t * (src_len - 1)), src_len - 1))
    return [src[i] for i in indices]
TURBO256_STRETCHED = make_stretched_palette(Turbo256)

# ******************************************************************************
# * Utility functions                                                          *
# ******************************************************************************
def _preprint_type_piechart(coll, year):
    ''' Create a preprint type pie chart
        Keyword arguments:
          coll: dois collection
          year: year or "All"
        Returns:
          Chart components
    '''
    match = {"type": "posted-content"}
    if year != 'All':
        match['jrc_publishing_date'] = {"$regex": "^"+ year}
    payload = [{"$match": match},
               {"$group": {"_id": {"institution": "$institution"},"count": {"$sum": 1}}}]
    rows = coll.aggregate(payload)
    data = {}
    for row in rows:
        if not row['_id']['institution']:
            data['No institution'] = row['count']
        else:
            data[row['_id']['institution'][0]['name']] = row['count']
    if not data:
        return None, None
    title = "Preprint DOI institutions"
    if year != 'All':
        title += f" ({year})"
    return pie_chart(dict(sorted(data.items())), title,
                     "source", width=600, height=400, location='bottom_right')


def _preprint_capture_piechart(coll, year):
    ''' Create a preprint capture pie chart
        Keyword arguments:
          coll: dois collection
          year: year or "All"
        Returns:
          Chart components
    '''
    data = {}
    payload = {"subtype": "preprint", "jrc_preprint": {"$exists": 1},
               "relation.is-preprint-of": {"$exists": 0}}
    if year != 'All':
        payload['jrc_publishing_date'] = {"$regex": "^"+ year}
    data['Fuzzy matching'] = coll.count_documents(payload)
    del payload['relation.is-preprint-of']
    data['Crossref relation'] = coll.count_documents(payload)
    data['Crossref relation'] = data['Crossref relation'] - data['Fuzzy matching']
    if not data['Crossref relation'] and not data['Fuzzy matching']:
        return None, None
    title = "Preprint capture method"
    if year != 'All':
        title += f" ({year})"
    return pie_chart(data, title, "source", colors=SOURCE_PALETTE, width=600, height=400)


def preprint_pie_charts(data, year, coll):
    ''' Create a preprint capture pie chart
        Keyword arguments:
          data: dictionary of data
          year: year or "All"
          coll: dois collection
        Returns:
          Chart components
    '''
    title = "DOIs by preprint status"
    if year != 'All':
        title += f" ({year})"
    chartscript, chartdiv = pie_chart(data, title, "source",
                                      colors=SOURCE_PALETTE, width=600, height=400)
    # Preprint types
    script2, div2 = _preprint_type_piechart(coll, year)
    if script2:
        chartscript += script2
        chartdiv += div2
    # Preprint capture
    script2, div2 = _preprint_capture_piechart(coll, year)
    if script2:
        chartscript += script2
        chartdiv += div2
    return chartscript, chartdiv

def get_colors_by_count(cnt):
    ''' Get colors by count
        Keyword arguments:
          cnt: count
        Returns:
          List of colors
    '''
    if cnt <= 0:
        return []
    if cnt == 1:
        return ['green']
    if cnt == 2:
        return SOURCE_PALETTE
    if cnt <= 10:
        return all_palettes['Category10'][cnt]
    if cnt <= 20:
        return all_palettes['Category20'][cnt]
    return plasma(cnt)

# ******************************************************************************
# * Basic charts                                                               *
# ******************************************************************************

def _fmt_bytes(num):
    ''' Return a human-readable byte size using SI units (1000-based).
        Keyword arguments:
          num: size in bytes
        Returns:
          Formatted string
    '''
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if abs(num) < 1000.0:
            return f"{num:.2f}{unit}"
        num /= 1000.0
    return f"{num:.2f}PB"


def donut_chart(used, total, title="Usage", element_id="donutChart", include_cdn=True,
                labels=None, colors=None):
    ''' Return HTML+JS for a Chart.js doughnut chart showing used vs. free.
        Keyword arguments:
          used: bytes in the first (left) segment
          total: total bytes (used + remainder)
          title: chart title
          element_id: canvas element id
          include_cdn: emit Chart.js CDN script and fmtBytes helper (set False
                       for subsequent charts on the same page)
          labels: two-element list of segment labels (default ['Used', 'Free'])
          colors: two-element list of CSS colors (default red/green)
        Returns:
          HTML string, or empty string if total is 0
    '''
    if not total:
        return ""
    if labels is None:
        labels = ['Used', 'Free']
    if colors is None:
        colors = ['#e74c3c', '#2ecc71']
    free = total - used
    pct = used / total * 100
    caption = f"{_fmt_bytes(used)} used of {_fmt_bytes(total)} ({pct:.1f}% full)"
    cdn = (
        '<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>\n'
        "<script>\n"
        "function fmtBytes(n) {\n"
        "  const u=['B','KB','MB','GB','TB']; let i=0;\n"
        "  while(n>=1000&&i<u.length-1){n/=1000;i++;}\n"
        "  return n.toFixed(2)+' '+u[i]; }\n"
    ) if include_cdn else "<script>\n"
    return (
        f"<div style='width:220px;text-align:center'>"
        f"<canvas id='{element_id}'></canvas>"
        f"<small>{caption}</small></div>"
        + cdn
        + f"new Chart(document.getElementById('{element_id}'), {{\n"
        "  type: 'doughnut',\n"
        f"  data: {{\n"
        f"    labels: {json.dumps(labels)},\n"
        f"    datasets: [{{ data: [{used}, {free}],\n"
        f"                  backgroundColor: {json.dumps(colors)},\n"
        "                  borderWidth: 1 }]\n"
        "  },\n"
        "  options: {\n"
        "    plugins: {\n"
        f"      title: {{ display: true, text: '{title}' }},\n"
        "      legend: { position: 'bottom', labels: { color: '#ddd', font: { weight: 'bold', size: 13 } } },\n"
        "      tooltip: { callbacks: { label: ctx => fmtBytes(ctx.raw) } }\n"
        "    }\n"
        "  }\n"
        "});\n"
        "</script>\n")

def pie_chart(data, title, legend, height=300, width=400, location="right",
              colors=None, style=None, fmt=None):
    ''' Create a pie chart
        Keyword arguments:
          data: dictionary of data
          title: chart title
          legend: data key name
          height: height of the chart (optional)
          width: width of the chart (optional)
          location: location of the legend (optional)
          colors: list of colors (optional)
          style: "bare" for a borderless chart
          fmt: format string for the values (optional)
        Returns:
          Figure components
    '''
    if not data:
        return components(figure(title=title, toolbar_location=None,
                                 height=height, width=width))
    if len(data) == 1 and colors is None:
        colors = ["mediumblue"]
    elif len(data) == 2 and colors is None:
        colors = SOURCE_PALETTE
    if not colors:
        colors = all_palettes['Category10'][len(data)]
    elif isinstance(colors, str):
        colors = all_palettes[colors][len(data)]
    pdata = pd.Series(data).reset_index(name='value').rename(columns={'index': legend})
    pdata['angle'] = pdata['value']/pdata['value'].sum() * 2*pi
    pdata['percentage'] = pdata['value']/pdata['value'].sum()*100
    pdata['color'] = colors
    tooltips = f"@{legend}: @value{fmt if fmt else ''} (@percentage%)"
    if style == 'bare':
        plt = figure(toolbar_location=None, height=height, width=width, min_border_left=0,
                 min_border_right=0, min_border_top=0, min_border_bottom=0,
                 background_fill_color=None, outline_line_color=None)
        plt.wedge(x=0, y=1, radius=0.4,
                  start_angle=cumsum('angle', include_zero=True), end_angle=cumsum('angle'),
                  line_color="white", fill_color='color', source=pdata)
    else:
        plt = figure(title=title, toolbar_location=None, height=height, width=width,
                     tools="hover", tooltips=tooltips, x_range=(-0.5, 1.0))
        plt.wedge(x=0, y=1, radius=0.4,
                  start_angle=cumsum('angle', include_zero=True), end_angle=cumsum('angle'),
                  line_color="white", fill_color='color', legend_field=legend, source=pdata)
    plt.axis.axis_label = None
    plt.axis.visible = False
    plt.grid.grid_line_color = None
    plt.legend.location = location
    return components(plt)


def stacked_bar_chart(data, title, xaxis, yaxis, colors=None, width=None, height=None,
                      orient=None, yaxis2=None, tooltip=None, legend=True, nav=None):
    ''' Create a stacked bar chart
        Keyword arguments:
          data: dictionary of data
          title: chart title
          xaxis: x-axis column name
          yaxis: list of y-axis column names
          colors: list of colors (optional)
          width: width of chart (optional)
          height: height of chart (optional)
          orient: orientation of x-axis labels (optional)
          yaxis2: extra y-axis column name (optional)
          tooltip: list of tooltip tuples (optional)
          legend: display legend (optional)
        Returns:
          Figure components
    '''
    if not colors:
        colors = plasma(len(yaxis))
    tt = tooltip if tooltip else f"$name @{xaxis}: @$name"
    plt = figure(x_range=data[xaxis], title=title,
                 toolbar_location=None)
    if width and height:
        plt.width = width
        plt.height = height
    cds = ColumnDataSource(data)
    if legend:
        bar_renderers = plt.vbar_stack(yaxis, x=xaxis, width=0.9,
                                       color=colors, source=cds,
                                       legend_label=yaxis)
    else:
        bar_renderers = plt.vbar_stack(yaxis, x=xaxis, width=0.9,
                                       color=colors, source=cds)
    plt.add_tools(HoverTool(renderers=bar_renderers, tooltips=tt))
    if orient:
        plt.xaxis.major_label_orientation = orient
    if yaxis2:
        # Secondary linear plot
        plt.yaxis.axis_label = ' + '.join(yaxis)
        ymax = 0
        for y in yaxis:
            ymax += max(data[y])
        plt.y_range = Range1d(0, ymax)
        ymax = max(data[yaxis2])
        if ymax > 1000:
            ymax = ceil(ymax / 1000) * 1000
        plt.extra_y_ranges = {yaxis2: Range1d(start=0, end=ymax)}
        plt.add_layout(LinearAxis(y_range_name=yaxis2, axis_label=yaxis2), 'right')
        if legend:
            line_renderer = plt.line(xaxis, yaxis2, color="black", source=cds,
                                     line_width=2, legend_label=yaxis2,
                                     y_range_name=yaxis2)
        else:
            line_renderer = plt.line(xaxis, yaxis2, color="black", source=cds,
                                     line_width=2, y_range_name=yaxis2)
        plt.add_tools(HoverTool(renderers=[line_renderer],
                                tooltips=f"@{xaxis}: @{yaxis2}"))
    if legend:
        plt.legend.location = 'top_left'
    if width and height and legend:
        plt.add_layout(plt.legend[0], 'right')
    plt.xgrid.grid_line_color = None
    plt.y_range.start = 0
    plt.background_fill_color = "ghostwhite"
    _make_clickable(plt, cds, list(data[xaxis]), nav, renderers=bar_renderers)
    return components(plt)


def dual_axis_chart(  # pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals
    data,
    title="Cost and Count by Year",
    x_field="Year",
    bar_field="Cost",
    line_field=None,
    bar_label=None,
    line_label=None,
    bar_color="green",
    line_color="black",
    bar_format="$0,0",
    line_format="0,0",
    width=900,
    height=450,
    bar_trend=False,
    trend_color="firebrick",
    nav=None,
):
    """
    Generate Bokeh chart script and div for a dual-axis chart with a
    categorical X-axis, a vertical bar series on the left Y-axis, and
    an optional line series on the right Y-axis.
    Args:
        data        : dict with list values keyed by x_field, bar_field,
                      and line_field
        title       : chart title string
        x_field     : key for categorical X-axis values
        bar_field   : key for left Y-axis (vertical bars)
        line_field  : key for right Y-axis (line); omit or pass None to disable
        bar_label   : legend label for bars  (defaults to bar_field)
        line_label  : legend label for line  (defaults to line_field)
        bar_color   : hex/CSS color for bars
        line_color  : hex/CSS color for line
        bar_format  : NumeralJS format string for the left Y-axis (default "$0,0")
        line_format : NumeralJS format string for the right Y-axis (default "0,0")
        width       : figure width in pixels
        height      : figure height in pixels
        bar_trend   : overlay a linear trend line on the bar data (default False)
        trend_color : color for the trend line (default "firebrick")
    Returns:
        (chartscript, chartdiv) — Bokeh JS <script> block and HTML <div>
    """
    bar_label = bar_label or bar_field
    line_label = line_label or line_field
    x_vals = [str(v) for v in data[x_field]]
    bar_vals = list(data[bar_field])
    source_data = {x_field: x_vals, bar_field: bar_vals}
    if line_field:
        line_vals = list(data[line_field])
        source_data[line_field] = line_vals
    source = ColumnDataSource(source_data)
    # Left y-axis range (bars), with headroom for legend
    bar_max = max(bar_vals) * 1.2
    p = figure(x_range=x_vals, y_range=(0, bar_max), title=title, width=width, height=height,
               toolbar_location=None, background_fill_color = "ghostwhite")
    # --- Left Y-axis: vertical bars ---
    bars = p.vbar(x=x_field, top=bar_field, source=source, width=0.6,
                  color=bar_color, alpha=0.85, legend_label=bar_label)
    # --- Bar trend line (optional) ---
    if bar_trend and len(bar_vals) >= 2:
        indices = np.arange(len(bar_vals))
        slope, intercept = np.polyfit(indices, bar_vals, 1)
        trend_vals = (slope * indices + intercept).tolist()
        trend_source = ColumnDataSource({x_field: x_vals, 'trend': trend_vals})
        p.line(x=x_field, y='trend', source=trend_source, color=trend_color,
               line_width=2, line_dash='dashed', legend_label=f"{bar_label} trend")
    # --- Right Y-axis: line (optional) ---
    if line_field:
        line_min = min(line_vals) * 0.85
        line_max = max(line_vals) * 1.15
        p.extra_y_ranges = {"right": Range1d(start=line_min, end=line_max)}
        p.add_layout(LinearAxis(y_range_name="right",
                                axis_label=line_label,
                                formatter=NumeralTickFormatter(format=line_format)), "right")
        p.line(x=x_field,
               y=line_field, source=source, color=line_color, line_width=2.5,
               y_range_name="right", legend_label=line_label)
    # --- Axis styling ---
    p.xaxis.axis_label = x_field
    p.yaxis[0].axis_label = bar_label
    p.yaxis[0].formatter = NumeralTickFormatter(format=bar_format)
    p.xaxis.major_label_orientation = 0.6
    # --- Hover tool (attached to bars) ---
    tooltips = [
        (x_field, f"@{x_field}"),
        (bar_label, f"@{bar_field}{{{bar_format}}}"),
    ]
    if line_field:
        tooltips.append((line_label, f"@{line_field}{{{line_format}}}"))
    p.add_tools(HoverTool(renderers=[bars], tooltips=tooltips))
    # --- Legend ---
    p.legend.location = "top_left"
    p.legend.click_policy = "hide"
    _make_clickable(p, source, x_vals, nav, renderers=[bars])
    chartscript, chartdiv = components(p)
    return chartscript, chartdiv


def wedge_chart(data, height=100, width=100, color='green'):
    ''' Create an annular wedge chart
        Keyword arguments:
          data: dictionary of data
          height: height of the chart (optional)
          width: width of the chart (optional)
          color: color of the chart (optional)
        Returns:
          Figure components
    '''
    plt = figure(toolbar_location=None, height=height, width=width, min_border_left=0,
                 min_border_right=0, min_border_top=0, min_border_bottom=0,
                 background_fill_color=None, outline_line_color=None)
    plt.annular_wedge(x=data['shown'], y=data['shown'], inner_radius=0.5, outer_radius=1,
                      start_angle_units='deg', start_angle=270,
                      end_angle_units='deg', end_angle=270 - data['shown'] / data['total'] * 360,
                      color=color, direction='clock')
    plt.axis.axis_label = None
    plt.axis.visible = False
    plt.grid.grid_line_color = None
    return components(plt)


def venn_diagram(set1_name, set2_name, intersection_name, percent_overlap,
                 width=600, height=400, colors=None, title=None, match_aspect=False):
    ''' Create a two-set Venn diagram
        Keyword arguments:
          set1_name: name of the first dataset
          set2_name: name of the second dataset
          intersection_name: name of the intersection
          percent_overlap: percent overlap between the two sets (0-100)
          width: width of the chart (optional)
          height: height of the chart (optional)
          colors: list of two colors for the circles (optional)
          title: chart title (optional)
          match_aspect: enforce equal x/y scaling to keep circles round (optional)
        Returns:
          Figure components
    '''
    if colors is None:
        colors = SOURCE_PALETTE
    radius = 1.0
    # Distance between centers: 2r when 0% overlap, 0 when 100% overlap
    dist = 2 * radius * (1 - percent_overlap / 100)
    cx1, cx2 = -dist / 2, dist / 2
    # X range with padding
    x_pad = radius * 1.6
    y_pad = radius * 1.4
    exclusive = 100 - percent_overlap
    source = ColumnDataSource(dict(
        x=[cx1, cx2], y=[0, 0],
        w=[2 * radius, 2 * radius],
        h=[2 * radius, 2 * radius],
        fill_color=colors,
        line_color=colors,
        name=[set1_name, set2_name],
        overlap=[f"{percent_overlap:.1f}%", f"{percent_overlap:.1f}%"],
        exclusive=[f"{exclusive:.1f}%", f"{exclusive:.1f}%"],
    ))
    plt = figure(width=width, height=height,
                 title=title,
                 x_range=(cx1 - x_pad, cx2 + x_pad),
                 y_range=(-y_pad, y_pad),
                 toolbar_location=None,
                 background_fill_color="ghostwhite",
                 outline_line_color=None,
                 match_aspect=match_aspect)
    plt.add_tools(HoverTool(tooltips=[
        ("Dataset",     "@name"),
        ("Overlap",     "@overlap"),
        ("Exclusive",   "@exclusive"),
    ]))
    plt.ellipse(x="x", y="y", width="w", height="h",
                fill_color="fill_color",
                fill_alpha=0.35,
                line_color="line_color",
                line_width=2,
                source=source)
    # Set name labels — positioned toward outer edge of each circle
    label_offset = radius * 0.55
    plt.text(x=[cx1 - label_offset, cx2 + label_offset], y=[0, 0],
             text=[set1_name, set2_name],
             text_align=["center", "center"],
             text_baseline="middle",
             text_font_size="13px",
             text_font_style="bold",
             text_color=[_darken_color(c) for c in colors])
    # Intersection label and percent
    plt.text(x=[0], y=[0.15],
             text=[intersection_name],
             text_align=["center"],
             text_baseline="middle",
             text_font_size="11px",
             text_color=["black"])
    plt.text(x=[0], y=[-0.2],
             text=[f"{percent_overlap:.1f}%"],
             text_align=["center"],
             text_baseline="middle",
             text_font_size="13px",
             text_font_style="bold",
             text_color=["black"])
    plt.axis.visible = False
    plt.grid.grid_line_color = None
    return components(plt)


def _totals_font_size(texts, cell_px, max_size=11, min_size=7):
    ''' Calculate a font size that fits the longest text label within a cell.
        Keyword arguments:
          texts: list of formatted string values to display
          cell_px: estimated cell width in pixels
          max_size: maximum font size in points (optional)
          min_size: minimum font size in points (optional)
        Returns:
          Font size string (e.g. "10px")
    '''
    max_chars = max((len(t) for t in texts), default=1)
    size = int((cell_px * 0.85) / (max_chars * 0.62))
    return f"{max(min_size, min(max_size, size))}px"


def _format_heatmap_value(val, fmt):
    ''' Format a numeric value using a NumeralJS-style format string.
        Keyword arguments:
          val: numeric value
          fmt: NumeralJS format string (e.g. "$0,0", "0,0", "0.0%")
        Returns:
          Formatted string
    '''
    if fmt.startswith('$'):
        return f"${val:,.0f}"
    if '%' in fmt:
        return f"{val:.1%}"
    return f"{val:,.0f}"


def heat_map(data, title, x_field, y_field, value_field, width=950, height=500,
             value_format="$0,0", palette=None, col_totals=False, row_totals=False):
    ''' Create a heat map
        Keyword arguments:
          data: dict with lists keyed by x_field, y_field, and value_field
          title: chart title
          x_field: key for the X-axis (categorical, e.g. Year)
          y_field: key for the Y-axis (categorical, e.g. Provider)
          value_field: key for the cell color value (e.g. Cost)
          width: figure width in pixels (optional)
          height: figure height in pixels (optional)
          value_format: NumeralJS format string for colorbar and tooltip (optional)
          palette: list of colors to use (optional, defaults to TURBO256_STRETCHED)
          col_totals: label string for a totals row summing each column; False disables (optional)
          row_totals: label string for a totals column summing each row; False disables (optional)
        Returns:
          Figure components (chartscript, chartdiv)
    '''
    if not len(data[x_field]) == len(data[y_field]) == len(data[value_field]):
        raise ValueError("heat_map: all data lists must have the same length")
    if palette is None:
        palette = TURBO256_STRETCHED
    x_vals = sorted(set(data[x_field]))
    y_vals = sorted(set(data[y_field]), key=str.lower, reverse=True)
    df = pd.DataFrame({x_field: data[x_field], y_field: data[y_field],
                       value_field: data[value_field]})
    col_total_source = None
    if col_totals and isinstance(col_totals, str):
        col_sums = df.groupby(x_field)[value_field].sum()
        y_vals = [col_totals] + list(y_vals)
        col_total_vals = [col_sums.get(x, 0) for x in x_vals]
        col_total_source = ColumnDataSource({
            x_field: x_vals,
            y_field: [col_totals] * len(x_vals),
            value_field: col_total_vals,
            'text': [_format_heatmap_value(v, value_format) for v in col_total_vals]
        })
    row_total_source = None
    if row_totals and isinstance(row_totals, str):
        row_sums = df.groupby(y_field)[value_field].sum()
        x_vals = list(x_vals) + [row_totals]
        row_total_vals = [row_sums.get(y, 0) for y in y_vals if y != col_totals]
        row_total_source = ColumnDataSource({
            x_field: [row_totals] * len(row_total_vals),
            y_field: [y for y in y_vals if y != col_totals],
            value_field: row_total_vals,
            'text': [_format_heatmap_value(v, value_format) for v in row_total_vals]
        })
    cell_px = (width - 100) / len(x_vals)
    source = ColumnDataSource(data)
    mapper = LinearColorMapper(palette=palette,
                               low=min(data[value_field]),
                               high=max(data[value_field]))
    p = figure(title=title, x_range=x_vals, y_range=y_vals,
               width=width, height=max(height, len(y_vals) * 30 + 100),
               toolbar_location=None, background_fill_color="ghostwhite")
    p.rect(x=x_field, y=y_field, width=1, height=1, source=source,
           fill_color=transform(value_field, mapper), line_color=None)
    for tot_source in (col_total_source, row_total_source):
        if tot_source is not None:
            font_size = _totals_font_size(tot_source.data['text'], cell_px)
            p.rect(x=x_field, y=y_field, width=1, height=1, source=tot_source,
                   fill_color="lightgray", line_color="white", line_width=0.5)
            p.text(x=x_field, y=y_field, text='text', source=tot_source,
                   text_align='center', text_baseline='middle',
                   text_font_size=font_size, text_color='black', text_font_style='bold')
    color_bar = ColorBar(color_mapper=mapper,
                         ticker=BasicTicker(desired_num_ticks=8),
                         formatter=NumeralTickFormatter(format=value_format),
                         label_standoff=8, border_line_color=None, location=(0, 0))
    p.add_layout(color_bar, 'right')
    p.add_tools(HoverTool(tooltips=[
        (x_field, f"@{x_field}"),
        (y_field, f"@{y_field}"),
        (value_field, f"@{value_field}{{{value_format}}}")]))
    p.xaxis.axis_label = x_field
    p.yaxis.axis_label = y_field
    p.xaxis.major_label_orientation = 0.6
    return components(p)


def hbar_chart(data, title, value_label="Value", width=650, height=450,  # pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals
               color=None, value_format="$0,0", show_pct=True, show_values=False, nav=None):
    ''' Create a horizontal bar chart sorted by value (largest at top).
        Accommodates many categories in a fixed footprint and makes relative
        magnitudes easy to compare by bar length.
        Keyword arguments:
          data: dictionary of {label: value}
          title: chart title
          value_label: label for the value in the hover tooltip
          width: figure width in pixels (optional)
          height: figure height in pixels (optional)
          color: single color, list of colors, or palette name (optional)
          value_format: NumeralJS format for the axis/tooltip (default "$0,0")
          show_pct: include percent-of-total in the hover tooltip (default True)
          show_values: print each value at the end of its bar (default False)
        Returns:
          Figure components (chartscript, chartdiv)
    '''
    items = sorted(data.items(), key=lambda kv: kv[1], reverse=True)
    labels = [str(k) for k, _ in items]
    values = [float(v) for _, v in items]
    if not values:
        return components(figure(title=title, width=width, height=height,
                                 toolbar_location=None,
                                 background_fill_color="ghostwhite"))
    total = sum(values) or 1
    pct = [v / total * 100 for v in values]
    if color is None:
        colors = get_colors_by_count(len(labels))
    elif isinstance(color, str) and color in all_palettes:
        colors = all_palettes[color][len(labels)]
    elif isinstance(color, (list, tuple)):
        colors = list(color)
    else:
        colors = [color] * len(labels)
    prefix = "$" if value_format.startswith("$") else ""
    source = ColumnDataSource({"label": labels, "value": values,
                               "pct": pct, "color": colors,
                               "value_text": [f"{prefix}{v:,.0f}" for v in values]})
    # Bokeh places the first categorical factor at the bottom of the y-axis, so
    # reverse the descending order to put the largest value at the top.
    # Value labels sit past the bar ends, so leave them extra headroom.
    xmax = max(values) * (1.18 if show_values else 1.05)
    p = figure(y_range=list(reversed(labels)), title=title, width=width,
               height=height, toolbar_location=None,
               background_fill_color="ghostwhite",
               x_range=(0, xmax) if values else (0, 1))
    bars = p.hbar(y="label", right="value", height=0.8, source=source,
                  fill_color="color", line_color=None, alpha=0.9)
    if show_values:
        p.add_layout(LabelSet(x="value", y="label", text="value_text", source=source,
                              x_offset=4, text_baseline="middle", text_font_size="8pt",
                              text_color="dimgray"))
    p.xaxis.formatter = NumeralTickFormatter(format=value_format)
    p.ygrid.grid_line_color = None
    p.yaxis.major_label_text_font_size = "7pt"
    tooltips = [("", "@label"), (value_label, f"@value{{{value_format}}}")]
    if show_pct:
        tooltips.append(("% of total", "@pct{0.0}%"))
    p.add_tools(HoverTool(renderers=[bars], tooltips=tooltips))
    _make_clickable(p, source, labels, nav, renderers=[bars])
    return components(p)


def hbar_stacked_chart(data, segments, title, value_label="Value", width=650, height=450,  # pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals
                       colors=None, value_format="0,0", show_pct=True, nav=None,
                       normalize=False, sort=True):
    ''' Create a horizontal stacked bar chart - same footprint/legibility as
        hbar_chart (long category labels stay on the y-axis, not rotated on an
        x-axis), but each bar splits into a per-segment colored stack (e.g.
        Internal vs External) instead of one solid color.
        Keyword arguments:
          data: dict of {label: {segment: value, ...}} - a label's total is the
                sum of its segment values; a label missing a segment defaults
                that segment to 0
          segments: ordered list of segment names - stack order (bottom of each
                    bar first) and legend order; color is assigned by this
                    list's position, not per-label, so it stays fixed across bars
          title: chart title
          value_label: label for the bar's total in the hover tooltip
          width: figure width in pixels (optional)
          height: figure height in pixels (optional)
          colors: list of colors, one per segment (optional; defaults to the
                  same get_colors_by_count palette hbar_chart uses)
          value_format: NumeralJS format for absolute values in the tooltip
                        (default "0,0")
          show_pct: include each segment's percent of that bar's own total in
                    the hover tooltip (default True; always shown when normalize)
          nav: dict mapping label -> nav target (see _make_clickable) -
               clicking any segment of a bar navigates the same place
          normalize: if True, plot each bar as its segments' share of that bar's
                     own total (every bar fills to 100%), so composition is
                     comparable across bars of very different absolute size
                     (e.g. citations vs views); the x-axis becomes a percent
                     scale and the tooltip still shows absolute values.
          sort: if True (default) order bars by descending total; if False keep
                the insertion order of `data` (for a fixed, caller-chosen row
                order that magnitude shouldn't reshuffle).
        Returns:
          Figure components (chartscript, chartdiv)
    '''
    totals = {label: sum(vals.get(seg, 0) for seg in segments) for label, vals in data.items()}
    labels = sorted(totals, key=totals.get, reverse=True) if sort else list(data)
    if not labels:
        return components(figure(title=title, width=width, height=height,
                                 toolbar_location=None,
                                 background_fill_color="ghostwhite"))
    colors = colors or get_colors_by_count(len(segments))
    cds_data = {"label": labels, "total": [float(totals[label]) for label in labels]}
    for seg in segments:
        abs_vals = [float(data[label].get(seg, 0)) for label in labels]
        pcts = [(abs_vals[i] / totals[label] * 100) if totals[label] else 0
                for i, label in enumerate(labels)]
        cds_data[f"{seg}_abs"] = abs_vals
        cds_data[f"{seg}_pct"] = pcts
        # The column hbar_stack actually plots: shares (0-100) when normalized,
        # absolute values otherwise. Tooltip reads the _abs/_pct columns either way.
        cds_data[seg] = pcts if normalize else abs_vals
    source = ColumnDataSource(cds_data)
    xmax = 100 if normalize else (max(cds_data["total"]) * 1.05 if cds_data["total"] else 1)
    # Bokeh places the first categorical factor at the bottom of the y-axis, so
    # reverse the order to put the first/largest bar at the top.
    p = figure(y_range=list(reversed(labels)), title=title, width=width,
              height=height, toolbar_location=None,
              background_fill_color="ghostwhite", x_range=(0, xmax))
    bar_renderers = p.hbar_stack(segments, y="label", height=0.8, color=colors,
                                 source=source, legend_label=segments)
    tooltips = [("", "@label")]
    for seg in segments:
        seg_tt = f"@{seg}_abs{{{value_format}}}"
        if show_pct or normalize:
            seg_tt += f" (@{seg}_pct{{0.0}}%)"
        tooltips.append((seg, seg_tt))
    tooltips.append((value_label, f"@total{{{value_format}}}"))
    p.add_tools(HoverTool(renderers=bar_renderers, tooltips=tooltips))
    # Normalized axis is a 0-100 percent scale: PrintfTickFormatter appends a
    # literal %, unlike NumeralTickFormatter's "%" which would multiply by 100.
    p.xaxis.formatter = (PrintfTickFormatter(format="%d%%") if normalize
                         else NumeralTickFormatter(format=value_format))
    p.ygrid.grid_line_color = None
    p.yaxis.major_label_text_font_size = "7pt"
    p.legend.location = 'bottom_right'
    p.legend.orientation = 'horizontal'
    p.legend.background_fill_alpha = 0.7
    _make_clickable(p, source, labels, nav, renderers=bar_renderers)
    return components(p)


def lorenz_chart(values, title, width=520, height=320, color=None,  # pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals
                 x_label="Share of works (most-cited first)",
                 y_label="Cumulative share of citations"):
    ''' Lorenz-style cumulative concentration curve: how much of a total is held
        by the top-ranked items. `values` are per-item magnitudes (e.g. per-work
        citation counts); they're sorted descending internally, then plotted as
        (cumulative % of items, cumulative % of the total). A dashed diagonal
        marks a perfectly even distribution, so the area between the curve and
        the diagonal is the concentration - the more the curve bows toward the
        top-left, the more a few items dominate.
        Keyword arguments:
          values: iterable of per-item magnitudes
          title: chart title
          width: figure width in pixels
          height: figure height in pixels
          color: curve color (default HHMI teal; the diagonal is always gray)
          x_label: x-axis label
          y_label: y-axis label
        Returns:
          Figure components (chartscript, chartdiv)
    '''
    vals = sorted((v for v in values if v and v > 0), reverse=True)
    n = len(vals)
    total = sum(vals)
    if not n or not total:
        return components(figure(title=title, width=width, height=height,
                                 toolbar_location=None,
                                 background_fill_color="ghostwhite"))
    work_pct = [0.0]
    cite_pct = [0.0]
    cum = 0
    for i, val in enumerate(vals, start=1):
        cum += val
        work_pct.append(i / n * 100)
        cite_pct.append(cum / total * 100)
    # Downsample for rendering: a Lorenz curve is smooth, so ~400 points draw
    # identically to thousands while keeping the embedded payload small. Keep
    # the steep leading segment (the top works) dense and thin the flat tail.
    max_pts = 400
    if len(work_pct) > max_pts:
        head = 120
        keep = set(range(head))
        keep.update(range(head, len(work_pct), max(1, (len(work_pct) - head) // (max_pts - head))))
        keep.add(len(work_pct) - 1)
        idx = sorted(keep)
        work_pct = [work_pct[i] for i in idx]
        cite_pct = [cite_pct[i] for i in idx]
    color = color or "#028d96"
    source = ColumnDataSource({"work_pct": work_pct, "cite_pct": cite_pct})
    p = figure(title=title, width=width, height=height, toolbar_location=None,
               background_fill_color="ghostwhite", x_range=(0, 100), y_range=(0, 100))
    p.line([0, 100], [0, 100], line_color="#888888", line_width=1.5,
           line_dash="dashed", legend_label="Even distribution")
    curve = p.line("work_pct", "cite_pct", source=source, line_color=color,
                   line_width=2.5, legend_label="Actual")
    # Use a literal Unicode arrow, not the &rarr; entity: Bokeh's tuple-form
    # tooltip HTML-escapes literal text, so an entity would show verbatim.
    p.add_tools(HoverTool(renderers=[curve], mode="vline",
                          tooltips=[("Concentration",
                                     "Top @work_pct{0.0}% of works → "
                                     "@cite_pct{0.0}% of citations")]))
    # PrintfTickFormatter (literal %%), NOT NumeralTickFormatter's "%" which
    # multiplies the value by 100 (would render a 100 axis as "10000%").
    p.xaxis.formatter = PrintfTickFormatter(format="%d%%")
    p.yaxis.formatter = PrintfTickFormatter(format="%d%%")
    p.xaxis.axis_label = x_label
    p.yaxis.axis_label = y_label
    p.legend.location = "bottom_right"
    p.legend.background_fill_alpha = 0.7
    return components(p)


# ******************************************************************************
# * Treemap (squarified) — area proportional to value                          *
# ******************************************************************************
# Self-contained squarified-treemap layout (Bruls, Huizing & van Wijk), adapted
# from the MIT-licensed `squarify` package so no extra runtime dependency is
# needed. Each helper returns/consumes rectangles as {"x", "y", "dx", "dy"}.

def _tm_normalize_sizes(sizes, dx, dy):
    ''' Scale raw sizes so their total equals the canvas area (dx*dy). '''
    total_size = sum(sizes)
    total_area = dx * dy
    return [s * total_area / total_size for s in sizes]


def _tm_layoutrow(sizes, x, y, dy):
    ''' Lay a group of sizes out as a vertical stack (a row of width `width`). '''
    width = sum(sizes) / dy
    rects = []
    for size in sizes:
        rects.append({"x": x, "y": y, "dx": width, "dy": size / width})
        y += size / width
    return rects


def _tm_layoutcol(sizes, x, y, dx):
    ''' Lay a group of sizes out as a horizontal strip (a column of `height`). '''
    height = sum(sizes) / dx
    rects = []
    for size in sizes:
        rects.append({"x": x, "y": y, "dx": size / height, "dy": height})
        x += size / height
    return rects


def _tm_layout(sizes, x, y, dx, dy):
    ''' Lay sizes along the shorter side of the remaining rectangle. '''
    return _tm_layoutrow(sizes, x, y, dy) if dx >= dy \
        else _tm_layoutcol(sizes, x, y, dx)


def _tm_leftover(sizes, x, y, dx, dy):
    ''' Return the (x, y, dx, dy) of the area left after placing `sizes`. '''
    if dx >= dy:
        width = sum(sizes) / dy
        return x + width, y, dx - width, dy
    height = sum(sizes) / dx
    return x, y + height, dx, dy - height


def _tm_worst_ratio(sizes, x, y, dx, dy):
    ''' Worst (most elongated) aspect ratio produced by laying out `sizes`. '''
    return max(max(r["dx"] / r["dy"], r["dy"] / r["dx"])
               for r in _tm_layout(sizes, x, y, dx, dy))


def _tm_squarify(sizes, x, y, dx, dy):
    ''' Recursively place `sizes` (already normalized, all > 0, descending)
        into the rectangle at (x, y) of dimensions dx by dy, keeping tiles as
        square as possible. Returns a list of rectangles in input order.
    '''
    sizes = [float(s) for s in sizes]
    if not sizes:
        return []
    if len(sizes) == 1:
        return _tm_layout(sizes, x, y, dx, dy)
    i = 1
    while i < len(sizes) and _tm_worst_ratio(sizes[:i], x, y, dx, dy) \
            >= _tm_worst_ratio(sizes[:i + 1], x, y, dx, dy):
        i += 1
    current, remaining = sizes[:i], sizes[i:]
    lx, ly, ldx, ldy = _tm_leftover(current, x, y, dx, dy)
    return _tm_layout(current, x, y, dx, dy) \
        + _tm_squarify(remaining, lx, ly, ldx, ldy)


def treemap_chart(data, title, width=650, height=450, color=None,  # pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals
                  value_format="$0,0", label_min_share=0.03):
    ''' Create a treemap where each tile's area is proportional to its value.
        Fits many categories into a fixed footprint and shows each as a share
        of the whole; small values become small (and unlabeled) tiles.
        Keyword arguments:
          data: dictionary of {label: value}
          title: chart title
          width: figure width in pixels (optional)
          height: figure height in pixels (optional)
          color: list of colors or palette name (optional)
          value_format: NumeralJS format for the hover tooltip (default "$0,0")
          label_min_share: only label tiles whose value is at least this
                           fraction of the total (default 0.03)
        Returns:
          Figure components (chartscript, chartdiv)
    '''
    items = [(str(k), float(v)) for k, v in
             sorted(data.items(), key=lambda kv: kv[1], reverse=True) if v > 0]
    labels = [k for k, _ in items]
    values = [v for _, v in items]
    if not values:
        return components(figure(title=title, width=width, height=height,
                                 toolbar_location=None,
                                 background_fill_color="white"))
    total = sum(values) or 1
    rects = _tm_squarify(_tm_normalize_sizes(values, width, height),
                         0, 0, width, height)
    if color is None:
        colors = get_colors_by_count(len(labels))
    elif isinstance(color, str) and color in all_palettes:
        colors = all_palettes[color][len(labels)]
    else:
        colors = list(color)
    pct = [v / total * 100 for v in values]
    text = [lab if v / total >= label_min_share else ""
            for lab, v in zip(labels, values)]
    source = ColumnDataSource({
        "left": [r["x"] for r in rects],
        "right": [r["x"] + r["dx"] for r in rects],
        "bottom": [r["y"] for r in rects],
        "top": [r["y"] + r["dy"] for r in rects],
        "cx": [r["x"] + r["dx"] / 2 for r in rects],
        "cy": [r["y"] + r["dy"] / 2 for r in rects],
        "label": labels, "value": values, "pct": pct,
        "color": colors, "text": text})
    p = figure(title=title, width=width, height=height, toolbar_location=None,
               x_range=(0, width), y_range=(0, height),
               background_fill_color="white")
    tiles = p.quad(left="left", right="right", top="top", bottom="bottom",
                   source=source, fill_color="color", line_color="white",
                   line_width=1)
    p.text(x="cx", y="cy", text="text", source=source, text_align="center",
           text_baseline="middle", text_font_size="8pt", text_color="white")
    p.add_tools(HoverTool(renderers=[tiles], tooltips=[
        ("", "@label"), ("Value", f"@value{{{value_format}}}"),
        ("% of total", "@pct{0.0}%")]))
    p.axis.visible = False
    p.grid.grid_line_color = None
    return components(p)
