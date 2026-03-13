''' dis_plots.py
    Plot functions for the DIS UI
'''

import colorsys
from math import ceil, pi
from bokeh.colors import named as _bokeh_named
from bokeh.models import (BasicTicker, ColorBar, HoverTool, LinearAxis, LinearColorMapper,
                          ColumnDataSource, NumeralTickFormatter, Range1d)
from bokeh.embed import components
from bokeh.palettes import all_palettes, interp_palette, plasma, Turbo256
from bokeh.plotting import figure
from bokeh.transform import cumsum, transform
import pandas as pd

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
    try:
        rows = coll.aggregate(payload)
    except Exception as err:
        raise err
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
    try:
        data['Fuzzy matching'] = coll.count_documents(payload)
    except Exception as err:
        raise err
    del payload['relation.is-preprint-of']
    try:
        data['Crossref relation'] = coll.count_documents(payload)
    except Exception as err:
        raise err
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
    try:
        script2, div2 = _preprint_type_piechart(coll, year)
        if script2:
            chartscript += script2
            chartdiv += div2
    except Exception as err:
        raise err
    # Preprint capture
    try:
        script2, div2 = _preprint_capture_piechart(coll, year)
        if script2:
            chartscript += script2
            chartdiv += div2
    except Exception as err:
        raise err
    return chartscript, chartdiv

def get_colors_by_count(cnt):
    ''' Get colors by count
        Keyword arguments:
          cnt: count
        Returns:
          List of colors
    '''
    colors = plasma(cnt)
    if cnt == 1:
        colors = ['green']
    elif cnt == 2:
        colors = SOURCE_PALETTE
    elif cnt <= 10:
        colors = all_palettes['Category10'][cnt]
    elif cnt <= 20:
        colors = all_palettes['Category20'][cnt]
    return colors

# ******************************************************************************
# * Basic charts                                                               *
# ******************************************************************************

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
        print("BARE")
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
                      orient=None,yaxis2=None, tooltip=None, legend=True):
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
    if legend:
        bar_renderers = plt.vbar_stack(yaxis, x=xaxis, width=0.9,
                                       color=colors, source=data,
                                       legend_label=yaxis)
    else:
        bar_renderers = plt.vbar_stack(yaxis, x=xaxis, width=0.9,
                                       color=colors, source=data)
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
            line_renderer = plt.line(xaxis, yaxis2, color="black", source=data,
                                     line_width=2, legend_label=yaxis2,
                                     y_range_name=yaxis2)
        else:
            line_renderer = plt.line(xaxis, yaxis2, color="black", source=data,
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
):
    """
    Generate Bokeh chart script and div for a dual-axis chart with a
    categorical X-axis, a vertical bar series on the left Y-axis, and
    an optional line series on the right Y-axis.
    Args:
        data       : dict with list values keyed by x_field, bar_field,
                     and line_field
        title      : chart title string
        x_field    : key for categorical X-axis values
        bar_field  : key for left Y-axis (vertical bars)
        line_field : key for right Y-axis (line); omit or pass None to disable
        bar_label  : legend label for bars  (defaults to bar_field)
        line_label : legend label for line  (defaults to line_field)
        bar_color  : hex/CSS color for bars
        line_color : hex/CSS color for line
        bar_format : NumeralJS format string for the left Y-axis (default "$0,0")
        line_format: NumeralJS format string for the right Y-axis (default "0,0")
        width      : figure width in pixels
        height     : figure height in pixels
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
    chartscript, chartdiv = components(p)
    return chartscript, chartdiv


def wedge_chart(data, height=100, width=100, color='green'):
    ''' Create a pie chart
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
                 width=600, height=400, colors=None, title=None):
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
                 outline_line_color=None)
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


def heat_map(data, title, x_field, y_field, value_field, width=950, height=500,
             value_format="$0,0", palette=None):
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
        Returns:
          Figure components (chartscript, chartdiv)
    '''
    if palette is None:
        palette = TURBO256_STRETCHED
    x_vals = sorted(set(data[x_field]))
    y_vals = sorted(set(data[y_field]), key=str.lower)
    source = ColumnDataSource(data)
    mapper = LinearColorMapper(palette=palette,
                               low=min(data[value_field]),
                               high=max(data[value_field]))
    p = figure(title=title, x_range=x_vals, y_range=y_vals,
               width=width, height=max(height, len(y_vals) * 30 + 100),
               toolbar_location=None, background_fill_color="ghostwhite")
    p.rect(x=x_field, y=y_field, width=1, height=1, source=source,
           fill_color=transform(value_field, mapper), line_color=None)
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
