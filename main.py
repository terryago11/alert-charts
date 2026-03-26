#!/usr/bin/env python3
"""
Israeli Homefront Command – Alert Bubble Chart
===============================================
Loads alert history, maps each city to its Homefront Command zone, deduplicates
per-zone per-minute, then saves a full-screen interactive chart to
output/night_alerts.html with a dark/light mode toggle.

Quick start
-----------
1. pip install -r requirements.txt
2. python main.py
3. Open output/night_alerts.html
"""

import json
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
import plotly.graph_objects as go
import requests

from regions import GROUP_COLORS, NIGHT_END, NIGHT_START, ZONE_GROUP

# ── Constants ─────────────────────────────────────────────────────────────────

CITIES_JSON_URL = (
    "https://raw.githubusercontent.com/eladnava/pikud-haoref-api/master/cities.json"
)

SHEETS_ID  = "14yX1jocodglfqioKvnRaR9oHJ1ppHcb2-8mMZxtKVN8"
SHEETS_GID = 0

DATA_DIR   = Path("data")
OUTPUT_DIR = Path("output")


# ── City / Zone helpers ───────────────────────────────────────────────────────

def load_city_data() -> Tuple[dict, dict]:
    """Return (city_to_zone, zone_centroid) from pikud-haoref-api."""
    print("Fetching city→zone mapping from pikud-haoref-api …")
    resp = requests.get(CITIES_JSON_URL, timeout=30)
    resp.raise_for_status()
    cities = resp.json()

    city_to_zone: dict = {}
    zone_coords: dict  = defaultdict(list)

    for entry in cities:
        name    = (entry.get("name") or entry.get("value") or "").strip()
        zone_en = (entry.get("zone_en") or "").strip()
        lat     = entry.get("lat")
        lng     = entry.get("lng")

        if name and zone_en and zone_en != "Select All":
            city_to_zone[name] = zone_en
        if zone_en and zone_en != "Select All" and lat is not None and lng is not None:
            zone_coords[zone_en].append((float(lat), float(lng)))

    zone_centroid = {
        zone: (
            sum(c[0] for c in coords) / len(coords),
            sum(c[1] for c in coords) / len(coords),
        )
        for zone, coords in zone_coords.items()
    }

    print(f"  {len(city_to_zone):,} cities mapped across {len(set(city_to_zone.values()))} zones.")
    return city_to_zone, zone_centroid


# ── Alert data loading ────────────────────────────────────────────────────────

def fetch_sheet() -> Optional[pd.DataFrame]:
    if not SHEETS_ID:
        return None
    url = (
        f"https://docs.google.com/spreadsheets/d/{SHEETS_ID}"
        f"/export?format=csv&gid={SHEETS_GID}"
    )
    print(f"Fetching Google Sheet … ({url})")
    try:
        resp = requests.get(url, timeout=30, allow_redirects=True)
        if resp.status_code != 200 or "text/csv" not in resp.headers.get("Content-Type", ""):
            print(
                f"  Sheet not publicly accessible (HTTP {resp.status_code}).\n"
                "  → Export as .xlsx and place in data/"
            )
            return None
        from io import StringIO
        df = pd.read_csv(StringIO(resp.text), dtype=str)
        print(f"  Loaded {len(df):,} rows from Google Sheet.")
        return df
    except requests.RequestException as exc:
        print(f"  Could not reach Google Sheet: {exc}")
        return None


def find_data_file() -> Optional[Path]:
    for pattern in ("*.xlsx", "*.xls", "*.csv"):
        for p in sorted(DATA_DIR.glob(pattern)):
            return p
    return None


def _detect_column(df: pd.DataFrame, keywords: list) -> Optional[str]:
    for col in df.columns:
        if any(kw in str(col).lower() for kw in keywords):
            return col
    return None


def _normalise_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Detect city and datetime columns and return a normalised DataFrame with:
      city_raw  – raw city/area string
      dt        – pd.Timestamp floored to the minute (or NaT)
      hour      – integer 0–23 (or None)
      date_str  – "YYYY-MM-DD" string (or None)
    """
    print(f"  {len(df):,} rows  |  columns: {list(df.columns)}")

    # Detect city column
    city_keywords = ["city", "cities", "area", "areas", "location",
                     "data", "ערים", "עיר", "אזור", "מיקום"]
    city_col = _detect_column(df, city_keywords)
    if city_col is None:
        city_col = next((c for c in df.columns if df[c].dtype == object), None)
    if city_col is None:
        raise ValueError("Could not detect a city/area column.")
    print(f"  Using city column: '{city_col}'")

    # Detect datetime column
    dt_col   = _detect_column(df, ["datetime", "timestamp", "alertdate", "alert_date"])
    time_col = _detect_column(df, ["time", "שעה", "hour"]) if dt_col is None else None
    date_col = _detect_column(df, ["date", "תאריך"])       if dt_col is None else None

    if dt_col:
        print(f"  Using datetime column: '{dt_col}'")
        dts = pd.to_datetime(df[dt_col], errors="coerce")
    elif time_col and date_col:
        print(f"  Combining date='{date_col}' + time='{time_col}'")
        dts = pd.to_datetime(
            df[date_col].astype(str) + " " + df[time_col].astype(str), errors="coerce"
        )
    elif time_col:
        print(f"  Using time column: '{time_col}'")
        dts = pd.to_datetime(df[time_col], errors="coerce")
    elif date_col:
        print(f"  Using date column: '{date_col}' (no time)")
        dts = pd.to_datetime(df[date_col], errors="coerce")
    else:
        print("  WARNING: No date/time column detected.")
        dts = pd.Series([pd.NaT] * len(df))

    dt_floor  = [ts.floor("min") if pd.notna(ts) else None for ts in dts]
    hours     = [int(ts.hour)            if pd.notna(ts) else None for ts in dts]
    date_strs = [ts.strftime("%Y-%m-%d") if pd.notna(ts) else None for ts in dts]

    alert_col = _detect_column(df, ["alert_type", "alerttype", "type"])
    alert_types = df[alert_col].astype(str) if alert_col else pd.Series(["Unknown"] * len(df))

    return pd.DataFrame({
        "city_raw":   df[city_col].astype(str),
        "dt":         dt_floor,
        "hour":       hours,
        "date_str":   date_strs,
        "alert_type": alert_types.values,
    })


def load_alerts(filepath: Path) -> pd.DataFrame:
    print(f"Loading {filepath} …")
    raw = pd.read_csv(filepath, dtype=str) if filepath.suffix.lower() == ".csv" \
          else pd.read_excel(filepath, dtype=str)
    return _normalise_df(raw)


# ── Aggregation ───────────────────────────────────────────────────────────────

def is_night(hour: Optional[int]) -> bool:
    if hour is None:
        return False
    return hour >= NIGHT_START or hour < NIGHT_END


def aggregate(
    df: pd.DataFrame, city_to_zone: dict
) -> Tuple[dict, dict, pd.DataFrame]:
    """
    Deduplicate: each unique (zone, alertDateTime-minute) pair counts as one
    alert event.  If the same zone is hit by multiple cities at the exact same
    minute, that's one event, not N.

    Returns (zone_total, zone_night, chart_df).
    chart_df has columns [date_str, hour, count] — deduplicated event counts
    per (date, hour) cell, suitable for the bubble chart.
    """
    seen: set         = set()
    zone_total        = defaultdict(int)
    zone_night        = defaultdict(int)
    chart_rows: list  = []
    unmatched: set    = set()

    for _, row in df.iterrows():
        raw      = str(row["city_raw"]).strip()
        dt       = row["dt"]
        hour     = row["hour"]
        date_str = row["date_str"]

        cities = [c.strip() for c in re.split(r"[,،;|\n]+", raw) if c.strip()]

        alert_type = str(row.get("alert_type", "Unknown"))

        for city in cities:
            zone = city_to_zone.get(city)
            if not zone:
                unmatched.add(city)
                continue

            if dt is not None and not pd.isna(dt):
                key = (zone, dt)
                if key in seen:
                    continue
                seen.add(key)

            zone_total[zone] += 1
            if is_night(hour):
                zone_night[zone] += 1
            if date_str and hour is not None:
                group = ZONE_GROUP.get(zone, "Other")
                chart_rows.append({
                    "date_str":   date_str,
                    "hour":       int(hour),
                    "group":      group,
                    "alert_type": alert_type,
                })

    if unmatched:
        print(f"\n  Note: {len(unmatched)} unmatched city names "
              f"(first 15): {sorted(unmatched)[:15]}")

    if chart_rows:
        chart_df = (
            pd.DataFrame(chart_rows)
            .groupby(["date_str", "hour", "group", "alert_type"])
            .size()
            .reset_index(name="count")
        )
    else:
        chart_df = pd.DataFrame(columns=["date_str", "hour", "group", "alert_type", "count"])

    return dict(zone_total), dict(zone_night), chart_df


# ── Chart ─────────────────────────────────────────────────────────────────────

def build_chart(chart_df: pd.DataFrame) -> None:
    """
    Full-screen interactive chart with two views toggled by a tab bar:

    "By Hour"  – stacked bar chart, X=hour 0-23, Y=alert count per region.
                 A date range selector (mini sparkline below) and alert-type
                 toggle buttons filter the data in-place via JS.
    "By Date"  – cumulative step lines, X=date, Y=running total per region,
                 with Plotly's built-in range selector buttons.

    Clicking any bar / line opens a small-multiples modal (day-by-day hourly
    bars for that region, filtered to the current date+type selection).
    Dark / light toggle persists across both views.
    """
    if chart_df.empty:
        print("\nNo data to chart – check that city column matched correctly.")
        return

    groups      = sorted(chart_df["group"].unique())
    alert_types = sorted(chart_df["alert_type"].unique())
    all_dates   = sorted(chart_df["date_str"].unique())

    # ── "By Hour" initial traces (all dates, all types) ───────────────────
    night_shapes = [
        dict(type="rect", xref="x", yref="paper",
             x0=NIGHT_START - 0.5, x1=23.5, y0=0, y1=1,
             fillcolor="rgba(120,120,220,0.10)", line=dict(width=0), layer="below"),
        dict(type="rect", xref="x", yref="paper",
             x0=-0.5, x1=NIGHT_END - 0.5, y0=0, y1=1,
             fillcolor="rgba(120,120,220,0.10)", line=dict(width=0), layer="below"),
    ]

    hourly_agg = chart_df.groupby(["hour", "group"])["count"].sum().reset_index()
    hour_traces = []
    for group in groups:
        color = GROUP_COLORS.get(group, "#888888")
        gdata = hourly_agg[hourly_agg["group"] == group].set_index("hour")["count"]
        ys = [int(gdata.get(h, 0)) for h in range(24)]
        hour_traces.append(go.Bar(
            x=list(range(24)), y=ys, name=group,
            marker=dict(color=color),
            hovertemplate=f"<b>{group}</b><br>%{{x:02d}}:00 — <b>%{{y:,}}</b> alerts<extra></extra>",
        ))

    hour_fig = go.Figure(hour_traces)
    hour_fig.update_layout(
        barmode="stack",
        title=dict(
            text="IDF Homefront Command — Alert Activity by Hour of Day<br>"
                 "<sup>Stacked by region · drag date slider below to filter"
                 " · click a bar to drill down by day</sup>",
            x=0.5, font=dict(size=15, color="#cccccc"),
        ),
        xaxis=dict(
            title="Hour of Day", tickmode="array",
            tickvals=list(range(24)),
            ticktext=[f"{h:02d}:00" for h in range(24)],
            showgrid=False, zeroline=False, color="#cccccc", range=[-0.5, 23.5],
        ),
        yaxis=dict(
            title="Alert Count", showgrid=True,
            gridcolor="#2a2a3e", zeroline=False, color="#cccccc",
        ),
        plot_bgcolor="#1a1a2e", paper_bgcolor="#0f0f1a",
        font=dict(family="Arial, Helvetica, sans-serif", color="#cccccc"),
        legend=dict(
            title=dict(text="Region"), font=dict(size=11, color="#cccccc"),
            bgcolor="rgba(26,26,46,0.85)", bordercolor="#444", borderwidth=1,
        ),
        shapes=night_shapes,
        margin=dict(t=80, b=40, l=70, r=40),
    )

    # ── Date mini-chart (for hour-view date slider) ────────────────────────
    daily_total = chart_df.groupby("date_str")["count"].sum().reset_index()
    date_mini_fig = go.Figure(go.Bar(
        x=daily_total["date_str"].tolist(),
        y=daily_total["count"].tolist(),
        marker=dict(color="#4466aa", opacity=0.75),
        hovertemplate="%{x}: <b>%{y:,}</b> alerts<extra></extra>",
        showlegend=False,
    ))
    date_mini_fig.update_layout(
        xaxis=dict(
            showgrid=False, color="#777",
            rangeslider=dict(visible=True, thickness=0.4),
        ),
        yaxis=dict(showgrid=False, color="#777", title=""),
        plot_bgcolor="#1a1a2e", paper_bgcolor="#0f0f1a",
        font=dict(family="Arial, Helvetica, sans-serif", color="#777", size=10),
        margin=dict(t=4, b=16, l=70, r=40),
        showlegend=False,
    )

    # ── "By Date" cumulative traces ────────────────────────────────────────
    date_traces = []
    for group in groups:
        color = GROUP_COLORS.get(group, "#888888")
        gdf = (
            chart_df.groupby(["date_str", "group"])["count"].sum()
            .reset_index()
            .pipe(lambda d: d[d["group"] == group])
            .set_index("date_str")["count"]
            .reindex(all_dates, fill_value=0)
            .reset_index()
        )
        gdf.columns = ["date_str", "daily"]
        gdf["cumulative"] = gdf["daily"].cumsum()
        date_traces.append(go.Scatter(
            x=gdf["date_str"].tolist(), y=gdf["cumulative"].tolist(),
            mode="lines+markers", name=group,
            line=dict(color=color, width=2.5), marker=dict(size=5, color=color),
            customdata=list(zip(gdf["daily"].tolist(), gdf["cumulative"].tolist())),
            hovertemplate=(
                f"<b>{group}</b><br>%{{x}}<br>"
                "Cumulative: <b>%{customdata[1]:,}</b> (+%{customdata[0]:,})"
                "<extra></extra>"
            ),
        ))

    date_fig = go.Figure(date_traces)
    date_fig.update_layout(
        title=dict(
            text="IDF Homefront Command — Cumulative Alerts by Region<br>"
                 "<sup>Deduplicated per zone · use range selector or slider"
                 " · click a line to drill down by day</sup>",
            x=0.5, font=dict(size=15, color="#cccccc"),
        ),
        xaxis=dict(
            title="Date", showgrid=True, gridcolor="#2a2a3e",
            zeroline=False, color="#cccccc",
            rangeslider=dict(visible=True, thickness=0.05),
            rangeselector=dict(
                buttons=[
                    dict(count=7,  label="1w",  step="day", stepmode="backward"),
                    dict(count=14, label="2w",  step="day", stepmode="backward"),
                    dict(step="all", label="All"),
                ],
                bgcolor="#252540", activecolor="#444470",
                font=dict(color="#cccccc"),
            ),
        ),
        yaxis=dict(
            title="Cumulative Alerts", showgrid=True,
            gridcolor="#2a2a3e", zeroline=False, color="#cccccc",
        ),
        plot_bgcolor="#1a1a2e", paper_bgcolor="#0f0f1a",
        font=dict(family="Arial, Helvetica, sans-serif", color="#cccccc"),
        legend=dict(
            title=dict(text="Region"), font=dict(size=11, color="#cccccc"),
            bgcolor="rgba(26,26,46,0.85)", bordercolor="#444", borderwidth=1,
        ),
        hovermode="x unified",
        margin=dict(t=80, b=60, l=70, r=40),
    )

    # ── Serialise everything for JS ────────────────────────────────────────
    def fig_json(fig):
        d = json.loads(fig.to_json())
        return json.dumps(d["data"]), json.dumps(d["layout"])

    hour_data_js,      hour_layout_js      = fig_json(hour_fig)
    date_mini_data_js, date_mini_layout_js = fig_json(date_mini_fig)
    date_data_js,      date_layout_js      = fig_json(date_fig)

    hourly_js      = json.dumps(chart_df.to_dict(orient="records"))
    group_colors_js = json.dumps(GROUP_COLORS)
    groups_js      = json.dumps(groups)
    alert_types_js = json.dumps(alert_types)

    dark_main  = json.dumps({"plot_bgcolor": "#1a1a2e", "paper_bgcolor": "#0f0f1a",
                              "font.color": "#cccccc", "title.font.color": "#cccccc",
                              "xaxis.color": "#cccccc", "xaxis.gridcolor": "#2a2a3e",
                              "yaxis.color": "#cccccc", "yaxis.gridcolor": "#2a2a3e"})
    light_main = json.dumps({"plot_bgcolor": "white",   "paper_bgcolor": "#fafafa",
                              "font.color": "#333",     "title.font.color": "#333",
                              "xaxis.color": "#333",    "xaxis.gridcolor": "#e0e0e0",
                              "yaxis.color": "#333",    "yaxis.gridcolor": "#e0e0e0"})
    dark_mini  = json.dumps({"plot_bgcolor": "#1a1a2e", "paper_bgcolor": "#0f0f1a",
                              "xaxis.color": "#777",    "yaxis.color": "#777"})
    light_mini = json.dumps({"plot_bgcolor": "white",   "paper_bgcolor": "#fafafa",
                              "xaxis.color": "#777",    "yaxis.color": "#777"})

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>IDF Alert Activity</title>
  <style>
    *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
    html, body {{
      width: 100%; height: 100%; overflow: hidden;
      background: #0f0f1a; transition: background 0.25s;
      font-family: Arial, Helvetica, sans-serif;
    }}
    body.light {{ background: #fafafa; }}

    /* ── Top bar ── */
    #topbar {{
      position: fixed; top: 0; left: 0; right: 0; height: 46px; z-index: 100;
      display: flex; align-items: center; gap: 10px; padding: 0 16px;
      background: rgba(10,10,28,0.92); backdrop-filter: blur(6px);
      border-bottom: 1px solid #2a2a3e;
    }}
    body.light #topbar {{
      background: rgba(245,245,252,0.95); border-color: #ddd;
    }}
    .tb-btn {{
      padding: 5px 13px; border-radius: 16px; cursor: pointer;
      font-size: 12px; font-weight: 600; border: 1px solid #555;
      background: #252540; color: #bbb; user-select: none;
      transition: background 0.15s, color 0.15s;
    }}
    .tb-btn:hover  {{ background: #333360; color: #fff; }}
    .tb-btn.active {{ background: #4455cc; color: #fff; border-color: #4455cc; }}
    body.light .tb-btn          {{ background: #eee; color: #555; border-color: #ccc; }}
    body.light .tb-btn:hover    {{ background: #dde; color: #222; }}
    body.light .tb-btn.active   {{ background: #4455cc; color: #fff; border-color: #4455cc; }}

    #sep {{ width: 1px; height: 22px; background: #333; margin: 0 4px; flex-shrink:0; }}
    body.light #sep {{ background: #ccc; }}
    #type-label {{ font-size: 11px; color: #777; white-space: nowrap; }}

    /* ── Chart area ── */
    #view-hour, #view-date {{ position: absolute; left:0; right:0; }}
    #view-hour {{
      top: 46px;
      bottom: 180px;   /* room for date slider */
    }}
    #view-date {{
      top: 46px;
      bottom: 0;
      display: none;
    }}
    #main-chart, #date-full-chart {{ width:100%; height:100%; }}
    #date-mini-wrap {{
      position: fixed; bottom: 0; left: 0; right: 0;
      height: 180px; z-index: 50;
      background: #0f0f1a;
      border-top: 1px solid #2a2a3e;
    }}
    body.light #date-mini-wrap {{ background: #fafafa; border-color: #ddd; }}
    #date-mini-chart {{ width:100%; height:100%; }}

    /* ── Modal ── */
    #modal-backdrop {{
      display: none; position: fixed; inset: 0; z-index: 2000;
      background: rgba(0,0,0,0.75); align-items: center; justify-content: center;
    }}
    #modal-backdrop.open {{ display: flex; }}
    #modal {{
      background: #12122a; color: #ccc; border-radius: 10px;
      width: 93vw; max-height: 88vh; display: flex; flex-direction: column;
      box-shadow: 0 8px 40px rgba(0,0,0,0.65); border: 1px solid #333;
    }}
    body.light #modal {{ background: #fff; color: #333; border-color: #ddd; }}
    #modal-header {{
      display: flex; align-items: center; justify-content: space-between;
      padding: 13px 18px; border-bottom: 1px solid #2a2a4a; flex-shrink: 0;
    }}
    body.light #modal-header {{ border-color: #ddd; }}
    #modal-title {{ font-size: 15px; font-weight: 700; }}
    #modal-close {{
      cursor: pointer; font-size: 22px; line-height: 1;
      background: none; border: none; color: #999; padding: 0 4px;
    }}
    #modal-close:hover {{ color: #fff; }}
    body.light #modal-close:hover {{ color: #000; }}
    #modal-body {{ overflow-y: auto; flex: 1; padding: 10px; }}
    #modal-chart {{ width: 100%; }}
    #date-select {{
      background: #252540; color: #ccc; border: 1px solid #444;
      border-radius: 6px; padding: 3px 8px; font-size: 12px; cursor: pointer;
    }}
    body.light #date-select {{ background: #fff; color: #333; border-color: #bbb; }}
  </style>
</head>
<body>

  <!-- Top bar -->
  <div id="topbar">
    <button class="tb-btn" onclick="toggleTheme()" id="theme-btn">&#9728;&#65039;&nbsp;Light</button>
    <div id="sep"></div>
    <button class="tb-btn active" onclick="setView('hour')" id="btn-hour">&#9200;&nbsp;By Hour</button>
    <button class="tb-btn"        onclick="setView('date')" id="btn-date">&#128197;&nbsp;By Date</button>
    <div id="sep2" style="width:1px;height:22px;background:#333;margin:0 4px;flex-shrink:0;"></div>
    <label for="date-select" style="font-size:12px;color:#aaa;white-space:nowrap;">Date:</label>
    <select id="date-select" onchange="onDateSelect(this.value)"></select>
    <div id="sep3" style="width:1px;height:22px;background:#333;margin:0 4px;flex-shrink:0;"></div>
    <span id="type-label">Alert type:</span>
    <div id="type-btns" style="display:flex;gap:6px;"></div>
  </div>

  <!-- By-hour view -->
  <div id="view-hour">
    <div id="main-chart"></div>
  </div>

  <!-- Date slider (only shown in hour view) -->
  <div id="date-mini-wrap">
    <div id="date-mini-chart"></div>
  </div>

  <!-- By-date cumulative view -->
  <div id="view-date">
    <div id="date-full-chart"></div>
  </div>

  <!-- Small-multiples modal -->
  <div id="modal-backdrop" onclick="closeModal(event)">
    <div id="modal">
      <div id="modal-header">
        <span id="modal-title"></span>
        <button id="modal-close" onclick="closeModal()">&#x2715;</button>
      </div>
      <div id="modal-body"><div id="modal-chart"></div></div>
    </div>
  </div>

  <script src="https://cdn.plot.ly/plotly-latest.min.js" charset="utf-8"></script>
  <script>
    // ── Data ────────────────────────────────────────────────────────────────
    var hourlyData   = {hourly_js};
    var groupColors  = {group_colors_js};
    var allGroups    = {groups_js};
    var allTypes     = {alert_types_js};

    var hourData    = {hour_data_js};
    var hourLayout  = {hour_layout_js};
    var miniData    = {date_mini_data_js};
    var miniLayout  = {date_mini_layout_js};
    var dateData    = {date_data_js};
    var dateLayout  = {date_layout_js};

    var darkMain    = {dark_main};
    var lightMain   = {light_main};
    var darkMini    = {dark_mini};
    var lightMini   = {light_mini};

    // ── State ───────────────────────────────────────────────────────────────
    var isDark            = true;
    var currentView       = 'hour';
    var currentDateRange  = null;          // null = all dates
    var activeTypes       = new Set(allTypes);

    // ── Init ─────────────────────────────────────────────────────────────────
    Plotly.newPlot('main-chart',      hourData, hourLayout, {{responsive:true}});
    Plotly.newPlot('date-mini-chart', miniData, miniLayout, {{responsive:true}});
    Plotly.newPlot('date-full-chart', dateData, dateLayout, {{responsive:true}});

    // ── Populate date selector ───────────────────────────────────────────────
    (function() {{
      var sel = document.getElementById('date-select');
      var dates = [...new Set(hourlyData.map(function(r) {{ return r.date_str; }}))].sort();
      var opt0 = document.createElement('option');
      opt0.value = ''; opt0.textContent = 'All dates';
      sel.appendChild(opt0);
      dates.forEach(function(d) {{
        var opt = document.createElement('option');
        opt.value = d; opt.textContent = d;
        sel.appendChild(opt);
      }});
    }})();

    function onDateSelect(val) {{
      currentDateRange = val ? [val, val] : null;
      updateHourChart();
    }}

    // Build alert-type toggle buttons
    var typeBtnsEl = document.getElementById('type-btns');
    allTypes.forEach(function(t) {{
      var b = document.createElement('button');
      b.className = 'tb-btn active';
      b.textContent = t;
      b.dataset.type = t;
      b.onclick = function() {{ toggleType(t, b); }};
      typeBtnsEl.appendChild(b);
    }});

    // ── Date slider → update hour chart ────────────────────────────────────
    document.getElementById('date-mini-chart').on('plotly_relayout', function(e) {{
      var r0 = e['xaxis.range[0]'], r1 = e['xaxis.range[1]'];
      if (r0 !== undefined) {{
        currentDateRange = [r0.slice(0,10), r1.slice(0,10)];
        // If range collapses to a single day, sync the dropdown
        if (currentDateRange[0] === currentDateRange[1]) {{
          document.getElementById('date-select').value = currentDateRange[0];
        }} else {{
          document.getElementById('date-select').value = '';
        }}
      }} else if (e['xaxis.autorange']) {{
        currentDateRange = null;
        document.getElementById('date-select').value = '';
      }}
      if (currentView === 'hour') updateHourChart();
    }});

    // ── Click bar/line → small-multiples modal ─────────────────────────────
    document.getElementById('main-chart').on('plotly_click', function(d) {{
      openSmallMultiples(d.points[0].data.name);
    }});
    document.getElementById('date-full-chart').on('plotly_click', function(d) {{
      openSmallMultiples(d.points[0].data.name);
    }});

    // ── View toggle ─────────────────────────────────────────────────────────
    var dateFull_rendered = true;  // rendered on load (but hidden)

    function setView(v) {{
      currentView = v;
      document.getElementById('view-hour').style.display      = v === 'hour' ? 'block' : 'none';
      document.getElementById('view-date').style.display      = v === 'date' ? 'block' : 'none';
      document.getElementById('date-mini-wrap').style.display = v === 'hour' ? 'block' : 'none';
      document.getElementById('btn-hour').classList.toggle('active', v === 'hour');
      document.getElementById('btn-date').classList.toggle('active', v === 'date');
      document.getElementById('type-label').style.display = v === 'hour' ? 'inline' : 'none';
      document.getElementById('type-btns').style.display  = v === 'hour' ? 'flex'   : 'none';
      // Force Plotly to redraw at the now-correct dimensions
      setTimeout(function() {{
        if (v === 'date') {{
          Plotly.Plots.resize('date-full-chart');
        }} else {{
          Plotly.Plots.resize('main-chart');
          Plotly.Plots.resize('date-mini-chart');
        }}
      }}, 30);
    }}

    // ── Alert type toggle ────────────────────────────────────────────────────
    function toggleType(t, btn) {{
      if (activeTypes.has(t)) {{
        if (activeTypes.size === 1) return;   // keep at least one
        activeTypes.delete(t);
        btn.classList.remove('active');
      }} else {{
        activeTypes.add(t);
        btn.classList.add('active');
      }}
      updateHourChart();
    }}

    // ── Recompute hour-chart traces from filtered data ────────────────────
    function updateHourChart() {{
      var filtered = hourlyData.filter(function(r) {{
        var dateOk = !currentDateRange ||
          (r.date_str >= currentDateRange[0] && r.date_str <= currentDateRange[1]);
        var typeOk = activeTypes.has(r.alert_type);
        return dateOk && typeOk;
      }});

      var agg = {{}};
      filtered.forEach(function(r) {{
        var k = r.group + '|' + r.hour;
        agg[k] = (agg[k] || 0) + r.count;
      }});

      var newTraces = allGroups.map(function(group) {{
        var ys = Array.from({{length:24}}, function(_, h) {{ return agg[group+'|'+h] || 0; }});
        return {{
          type: 'bar',
          x: Array.from({{length:24}}, function(_, h) {{ return h; }}),
          y: ys, name: group,
          marker: {{ color: groupColors[group] || '#888' }},
          hovertemplate: '<b>' + group + '</b><br>%{{x:02d}}:00 — <b>%{{y:,}}</b> alerts<extra></extra>',
        }};
      }});
      Plotly.react('main-chart', newTraces, hourLayout);
    }}

    // ── Small-multiples modal ───────────────────────────────────────────────
    function openSmallMultiples(group) {{
      var rows = hourlyData.filter(function(r) {{
        var dateOk = !currentDateRange ||
          (r.date_str >= currentDateRange[0] && r.date_str <= currentDateRange[1]);
        var typeOk = activeTypes.has(r.alert_type);
        return r.group === group && dateOk && typeOk;
      }});
      if (!rows.length) return;

      var color = groupColors[group] || '#888';
      var days  = [...new Set(rows.map(function(r) {{ return r.date_str; }}))].sort();
      var COLS  = 7, ROWS = Math.ceil(days.length / COLS);

      var traces = [], layout = {{
        grid: {{ rows: ROWS, columns: COLS, pattern: 'independent', ygap: 0.18, xgap: 0.08 }},
        showlegend: false,
        margin: {{ t: 30, b: 10, l: 30, r: 10 }},
        height: ROWS * 140 + 40,
        paper_bgcolor: isDark ? '#12122a' : '#fff',
        plot_bgcolor:  isDark ? '#1a1a2e' : 'white',
        font: {{ color: isDark ? '#ccc' : '#333', size: 10, family: 'Arial' }},
      }};

      days.forEach(function(day, i) {{
        var axN    = i + 1;
        var xKey   = axN === 1 ? 'xaxis'  : 'xaxis'  + axN;
        var yKey   = axN === 1 ? 'yaxis'  : 'yaxis'  + axN;
        var xTrace = axN === 1 ? 'x'      : 'x'      + axN;
        var yTrace = axN === 1 ? 'y'      : 'y'      + axN;

        var hourMap = {{}};
        rows.filter(function(r) {{ return r.date_str === day; }})
            .forEach(function(r) {{ hourMap[r.hour] = (hourMap[r.hour]||0) + r.count; }});
        var hours  = Array.from({{length:24}}, function(_, h) {{ return h; }});
        var counts = hours.map(function(h) {{ return hourMap[h] || 0; }});
        var colors = hours.map(function(h) {{ return (h>=22||h<6) ? color : color+'99'; }});

        traces.push({{
          type: 'bar', x: hours, y: counts,
          xaxis: xTrace, yaxis: yTrace,
          marker: {{ color: colors }},
          hovertemplate: '%{{x}}:00 — %{{y}} alerts<extra>' + day + '</extra>',
          showlegend: false,
        }});

        var ax = {{ showgrid:false, zeroline:false,
                    color: isDark?'#888':'#666', tickfont:{{size:8}} }};
        layout[xKey] = Object.assign({{}}, ax, {{
          title: {{ text: day, font: {{size:9}} }},
          tickmode:'array', tickvals:[0,6,12,18,23], ticktext:['0','6','12','18','23'],
        }});
        layout[yKey] = Object.assign({{}}, ax, {{title:''}});
      }});

      document.getElementById('modal-title').textContent = group + ' — Daily Alert Distribution';
      document.getElementById('modal-backdrop').classList.add('open');
      Plotly.newPlot('modal-chart', traces, layout, {{responsive:true}});
    }}

    function closeModal(e) {{
      if (e && e.target !== document.getElementById('modal-backdrop') &&
          e.target !== document.getElementById('modal-close')) return;
      document.getElementById('modal-backdrop').classList.remove('open');
      Plotly.purge('modal-chart');
    }}

    // ── Dark / light toggle ─────────────────────────────────────────────────
    function toggleTheme() {{
      isDark = !isDark;
      document.body.classList.toggle('light', !isDark);
      document.getElementById('theme-btn').innerHTML =
        isDark ? '&#9728;&#65039;&nbsp;Light' : '&#127769;&nbsp;Dark';
      Plotly.relayout('main-chart',      isDark ? darkMain : lightMain);
      Plotly.relayout('date-mini-chart', isDark ? darkMini : lightMini);
      Plotly.relayout('date-full-chart', isDark ? darkMain : lightMain);
      document.getElementById('date-mini-wrap').style.background =
        isDark ? '#0f0f1a' : '#fafafa';
    }}
  </script>
</body>
</html>"""

    OUTPUT_DIR.mkdir(exist_ok=True)
    outfile = OUTPUT_DIR / "night_alerts.html"
    outfile.write_text(html, encoding="utf-8")
    print(f"\nChart saved → {outfile}")


# ── Console summary ───────────────────────────────────────────────────────────

def print_summary(zone_total: dict, zone_night: dict) -> None:
    rows = []
    for zone, total in zone_total.items():
        night = zone_night.get(zone, 0)
        pct   = round(night / total * 100, 1) if total > 0 else 0.0
        group = ZONE_GROUP.get(zone, "Other")
        rows.append((group, zone, total, night, pct))

    rows.sort(key=lambda r: r[3], reverse=True)

    print(f"\n{'Group':<22} {'Zone':<26} {'Total':>8} {'Night':>8} {'%Night':>8}")
    print("─" * 76)
    for group, zone, total, night, pct in rows:
        if total > 0:
            print(f"{group:<22} {zone:<26} {total:>8,} {night:>8,} {pct:>7.1f}%")


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    # 1. City → zone mapping
    city_to_zone, _ = load_city_data()

    # 2. Alert data — Google Sheet first, then local file fallback
    raw_df = fetch_sheet()
    if raw_df is not None:
        df = _normalise_df(raw_df)
    else:
        data_file = find_data_file()
        if data_file is None:
            print(
                "\nNo data found.\n"
                "Either share your Google Sheet publicly and re-run, or\n"
                "export it as .xlsx / .csv and place in data/"
            )
            sys.exit(1)
        df = load_alerts(data_file)

    # 3. Aggregate (with deduplication)
    print("\nAggregating alerts by zone …")
    zone_total, zone_night, chart_df = aggregate(df, city_to_zone)

    total_alerts = sum(zone_total.values())
    total_night  = sum(zone_night.values())
    print(f"  Deduplicated alert events : {total_alerts:,}")
    print(f"  Of which at night         : {total_night:,}  "
          f"({round(total_night / total_alerts * 100, 1) if total_alerts else 0}%)")

    # 4. Summary table
    print_summary(zone_total, zone_night)

    # 5. Chart
    build_chart(chart_df)
    print("\nDone.  Open output/night_alerts.html in your browser.")


if __name__ == "__main__":
    main()
