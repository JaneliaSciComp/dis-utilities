"""dis_plots.py
Plot functions for the DIS UI
"""

from math import pi
import pandas as pd
from bokeh.embed import components
from bokeh.palettes import all_palettes, plasma
from bokeh.plotting import figure
from bokeh.transform import cumsum

SOURCE_PALETTE = ["mediumblue", "darkorange"]
SOURCE3_PALETTE = ["mediumblue", "darkorange", "wheat"]
TYPE_PALETTE = ["mediumblue", "darkorange", "wheat", "darkgray"]


# ******************************************************************************
# * Utility functions                                                          *
# ******************************************************************************
def _preprint_type_piechart(coll, year):
    """Create a preprint type pie chart
    Keyword arguments:
      coll: dois collection
      year: year or "All"
    Returns:
      Chart components
    """
    match = {"type": "posted-content"}
    if year != "All":
        match["jrc_publishing_date"] = {"$regex": "^" + year}
    payload = [
        {"$match": match},
        {"$group": {"_id": {"institution": "$institution"}, "count": {"$sum": 1}}},
    ]
    try:
        rows = coll.aggregate(payload)
    except Exception as err:
        raise err
    data = {}
    for row in rows:
        if not row["_id"]["institution"]:
            data["No institution"] = row["count"]
        else:
            data[row["_id"]["institution"][0]["name"]] = row["count"]
    if not data:
        return None, None
    title = "Preprint DOI institutions"
    if year != "All":
        title += f" ({year})"
    return pie_chart(
        dict(sorted(data.items())),
        title,
        "source",
        width=600,
        height=400,
        location="bottom_right",
    )


def _preprint_capture_piechart(coll, year):
    """Create a preprint capture pie chart
    Keyword arguments:
      coll: dois collection
      year: year or "All"
    Returns:
      Chart components
    """
    data = {}
    payload = {
        "subtype": "preprint",
        "jrc_preprint": {"$exists": 1},
        "relation.is-preprint-of": {"$exists": 0},
    }
    if year != "All":
        payload["jrc_publishing_date"] = {"$regex": "^" + year}
    try:
        data["Fuzzy matching"] = coll.count_documents(payload)
    except Exception as err:
        raise err
    del payload["relation.is-preprint-of"]
    try:
        data["Crossref relation"] = coll.count_documents(payload)
    except Exception as err:
        raise err
    data["Crossref relation"] = data["Crossref relation"] - data["Fuzzy matching"]
    if not data["Crossref relation"] and not data["Fuzzy matching"]:
        return None, None
    title = "Preprint capture method"
    if year != "All":
        title += f" ({year})"
    return pie_chart(
        data, title, "source", colors=SOURCE_PALETTE, width=600, height=400
    )


def preprint_pie_charts(data, year, coll):
    """Create a preprint capture pie chart
    Keyword arguments:
      data: dictionary of data
      year: year or "All"
      coll: dois collection
    Returns:
      Chart components
    """
    title = "DOIs by preprint status"
    if year != "All":
        title += f" ({year})"
    chartscript, chartdiv = pie_chart(
        data, title, "source", colors=SOURCE_PALETTE, width=600, height=400
    )
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


# ******************************************************************************
# * Basic charts                                                               *
# ******************************************************************************


def pie_chart(
    data, title, legend, height=300, width=400, location="right", colors=None
):
    """Create a pie chart
    Keyword arguments:
      data: dictionary of data
      title: chart title
      legend: data key name
      height: height of the chart (optional)
      width: width of the chart (optional)
      colors: list of colors (optional)
    Returns:
      Figure components
    """
    if len(data) == 1:
        colors = ["mediumblue"]
    elif len(data) == 2:
        colors = SOURCE_PALETTE
    if not colors:
        colors = all_palettes["Category10"][len(data)]
    elif isinstance(colors, str):
        print(colors)
        colors = all_palettes[colors][len(data)]
    pdata = pd.Series(data).reset_index(name="value").rename(columns={"index": legend})
    pdata["angle"] = pdata["value"] / pdata["value"].sum() * 2 * pi
    pdata["percentage"] = pdata["value"] / pdata["value"].sum() * 100
    pdata["color"] = colors
    tooltips = f"@{legend}: @value (@percentage%)"
    plt = figure(
        title=title,
        toolbar_location=None,
        height=height,
        width=width,
        tools="hover",
        tooltips=tooltips,
        x_range=(-0.5, 1.0),
    )
    plt.wedge(
        x=0,
        y=1,
        radius=0.4,
        start_angle=cumsum("angle", include_zero=True),
        end_angle=cumsum("angle"),
        line_color="white",
        fill_color="color",
        legend_field=legend,
        source=pdata,
    )
    plt.axis.axis_label = None
    plt.axis.visible = False
    plt.grid.grid_line_color = None
    plt.legend.location = location
    return components(plt)


def stacked_bar_chart(data, title, xaxis, yaxis, colors=None, width=None, height=None):
    """Create a stacked bar chart
    Keyword arguments:
      data: dictionary of data
      title: chart title
      xaxis: x-axis column name
      yaxis: list of y-axis column names
      colors: list of colors (optional)
      width: width of chart (optional)
      height: height of chart (optional)
    Returns:
      Figure components
    """
    if not colors:
        colors = plasma(len(yaxis))
    plt = figure(
        x_range=data[xaxis],
        title=title,
        toolbar_location=None,
        tools="hover",
        tooltips=f"$name @{xaxis}: @$name",
    )
    if width and height:
        plt.width = width
        plt.height = height
    plt.vbar_stack(
        yaxis, x=xaxis, width=0.9, color=colors, source=data, legend_label=yaxis
    )
    plt.legend.location = "top_left"
    if width and height:
        plt.add_layout(plt.legend[0], "right")
    plt.xgrid.grid_line_color = None
    plt.y_range.start = 0
    plt.background_fill_color = "ghostwhite"
    return components(plt)
