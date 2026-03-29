#!/usr/bin/env python3
"""
Israeli Homefront Command – Alert Bubble Chart
===============================================
Loads alert history, maps each city to its Homefront Command zone, deduplicates
per-zone per-minute, then saves a full-screen interactive chart to
output/ira_alerts.html with a dark/light mode toggle.

Quick start
-----------
1. pip install -r requirements.txt
2. python main.py
3. Open output/ira_alerts.html
"""

import json
import re
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
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

GITHUB_CSV_URL = "https://raw.githubusercontent.com/dleshem/israel-alerts-data/main/israel-alerts.csv"
CUTOFF_DATE    = pd.Timestamp("2026-02-28")
ALERT_TRANSLATIONS = {
    "בדקות הקרובות צפויות להתקבל התרעות באזורך": "Pre-alert",
    "חדירת כלי טיס עוין":                         "Drone alert",
    "ירי רקטות וטילים":                            "Missile alert",
}

DATA_DIR    = Path("data")
OUTPUT_DIR  = Path("output")
PAIR_WINDOW  = timedelta(minutes=15)   # max gap between pre-alert and missile alert
SALVO_WINDOW = timedelta(minutes=30)   # max consecutive gap within a salvo cluster


# ── City / Zone helpers ───────────────────────────────────────────────────────

CITY_MAPPING_CSV = DATA_DIR / "city_region_mapping.csv"


def load_city_data() -> Tuple[dict, dict]:
    """Return (city_to_zone, zone_centroid) from local city_region_mapping.csv."""
    print(f"Loading city→zone mapping from {CITY_MAPPING_CSV} …")
    mapping = pd.read_csv(CITY_MAPPING_CSV, dtype=str)

    city_to_zone: dict = {}
    zone_coords: dict  = defaultdict(list)

    for _, row in mapping.iterrows():
        name    = str(row.get("city_he") or "").strip()
        zone_en = str(row.get("zone")    or "").strip()
        try:
            lat = float(row["lat"])
            lng = float(row["lng"])
        except (KeyError, ValueError, TypeError):
            lat = lng = None

        if name and zone_en and zone_en != "Select All":
            city_to_zone[name] = zone_en
        if zone_en and zone_en != "Select All" and lat is not None and lng is not None:
            zone_coords[zone_en].append((lat, lng))

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

def fetch_github_csv() -> Optional[pd.DataFrame]:
    print(f"Fetching GitHub CSV … ({GITHUB_CSV_URL})")
    try:
        resp = requests.get(GITHUB_CSV_URL, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as exc:
        print(f"  Could not reach GitHub CSV: {exc}")
        return None

    from io import StringIO
    raw = pd.read_csv(StringIO(resp.text), dtype=str)

    # Filter to known categories and cutoff date
    raw = raw[raw["category_desc"].isin(ALERT_TRANSLATIONS)]
    raw["_dt"] = pd.to_datetime(raw["alertDate"].str.replace(" ", "T"), errors="coerce")
    raw = raw[raw["_dt"] >= CUTOFF_DATE].drop(columns=["_dt"])

    # Drop unused columns, rename to match existing normalise logic
    raw = raw.drop(columns=["matrix_id", "category"], errors="ignore")
    raw = raw.rename(columns={"data": "location", "alertDate": "alertDateTime"})
    raw["alert_type"] = raw["category_desc"].map(ALERT_TRANSLATIONS)
    raw = raw.drop(columns=["category_desc"])

    print(f"  Loaded {len(raw):,} rows from GitHub CSV.")
    return raw


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


# ── Mismatch analysis ─────────────────────────────────────────────────────────

def compute_mismatches(df: pd.DataFrame, city_to_zone: dict) -> pd.DataFrame:
    """
    For each city, pair Pre-alerts with Missile alerts within PAIR_WINDOW.

    A Pre-alert is 'paired'      if a Missile alert follows within 15 min.
    A Pre-alert is 'pre_only'    if no Missile alert follows within 15 min.
    A Missile alert is 'missile_only' if no Pre-alert preceded it within 15 min.

    Drone alerts are excluded from pairing.

    Returns DataFrame: [city, zone, group, date_str, event_type]
    where event_type ∈ {'paired', 'pre_only', 'missile_only'}.
    """
    # Build per-city alert lists
    city_events: dict = defaultdict(list)
    for _, row in df.iterrows():
        raw        = str(row["city_raw"]).strip()
        dt         = row["dt"]
        alert_type = str(row.get("alert_type", "Unknown"))
        if dt is None or pd.isna(dt):
            continue
        if alert_type not in ("Pre-alert", "Missile alert", "Drone alert"):
            continue
        cities = [c.strip() for c in re.split(r"[,،;|\n]+", raw) if c.strip()]
        for city in cities:
            city_events[city].append((dt, alert_type))

    rows = []
    for city, events in city_events.items():
        zone = city_to_zone.get(city)
        if not zone:
            continue
        group = ZONE_GROUP.get(zone, "Other")

        pre_dts     = sorted(dt for dt, t in events if t == "Pre-alert")
        missile_dts = sorted(dt for dt, t in events if t == "Missile alert")
        drone_dts   = sorted(dt for dt, t in events if t == "Drone alert")

        # Pre-alerts: missile takes priority over drone if both follow
        for pre_dt in pre_dts:
            has_missile = any(pre_dt <= m <= pre_dt + PAIR_WINDOW for m in missile_dts)
            has_drone   = any(pre_dt <= d <= pre_dt + PAIR_WINDOW for d in drone_dts)
            if has_missile:
                gap = min((m - pre_dt).total_seconds()
                          for m in missile_dts if pre_dt <= m <= pre_dt + PAIR_WINDOW)
                evt = "paired_missile"
            elif has_drone:
                gap = min((d - pre_dt).total_seconds()
                          for d in drone_dts if pre_dt <= d <= pre_dt + PAIR_WINDOW)
                evt = "paired_drone"
            else:
                gap = None
                evt = "pre_only"
            rows.append({
                "city": city, "zone": zone, "group": group,
                "date_str":   pre_dt.strftime("%Y-%m-%d"),
                "event_type": evt,
                "gap_seconds": gap,
            })

        # Missile alerts: missile_only if no pre-alert preceded within PAIR_WINDOW
        for m_dt in missile_dts:
            if not any(m_dt - PAIR_WINDOW <= p <= m_dt for p in pre_dts):
                rows.append({
                    "city": city, "zone": zone, "group": group,
                    "date_str":   m_dt.strftime("%Y-%m-%d"),
                    "event_type": "missile_only",
                    "gap_seconds": None,
                })

        # Drone alerts: drone_only if no pre-alert preceded within PAIR_WINDOW
        for d_dt in drone_dts:
            if not any(d_dt - PAIR_WINDOW <= p <= d_dt for p in pre_dts):
                rows.append({
                    "city": city, "zone": zone, "group": group,
                    "date_str":   d_dt.strftime("%Y-%m-%d"),
                    "event_type": "drone_only",
                    "gap_seconds": None,
                })

    if rows:
        return pd.DataFrame(rows)
    return pd.DataFrame(columns=["city", "zone", "group", "date_str", "event_type", "gap_seconds"])


ALL_EVENT_TYPES = ("paired_missile", "paired_drone", "pre_only", "missile_only", "drone_only")


def mismatch_daily_data(mismatch_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate mismatch events per calendar day.
    Returns DataFrame: [date_str, paired_missile, paired_drone, pre_only, missile_only, drone_only].
    """
    if mismatch_df.empty:
        return pd.DataFrame(columns=["date_str", *ALL_EVENT_TYPES])

    counts = (
        mismatch_df.groupby(["date_str", "event_type"])
        .size()
        .reset_index(name="count")
        .pivot(index="date_str", columns="event_type", values="count")
        .fillna(0)
        .astype(int)
        .reset_index()
    )
    for col in ALL_EVENT_TYPES:
        if col not in counts.columns:
            counts[col] = 0

    return counts[["date_str", *ALL_EVENT_TYPES]].sort_values("date_str")


def compute_salvos(df: pd.DataFrame, city_to_zone: dict) -> pd.DataFrame:
    """
    Detect salvo clusters: groups of >= 2 Missile alerts to the same zone
    where the gap between every consecutive pair is <= SALVO_WINDOW.

    Returns DataFrame: [zone, group, date_str, cluster_start, cluster_size]
    where cluster_size >= 2.
    """
    zone_times: dict = defaultdict(list)
    for _, row in df.iterrows():
        if str(row.get("alert_type", "")) != "Missile alert":
            continue
        dt = row["dt"]
        if dt is None or pd.isna(dt):
            continue
        raw    = str(row["city_raw"]).strip()
        cities = [c.strip() for c in re.split(r"[,،;|\n]+", raw) if c.strip()]
        for city in cities:
            zone = city_to_zone.get(city)
            if zone:
                zone_times[zone].append(dt)

    rows = []
    for zone, times in zone_times.items():
        group        = ZONE_GROUP.get(zone, "Other")
        sorted_times = sorted(set(times))   # dedup same-minute hits

        if len(sorted_times) < 2:
            continue

        cluster_start = sorted_times[0]
        cluster_count = 1

        for i in range(1, len(sorted_times)):
            gap = sorted_times[i] - sorted_times[i - 1]
            if gap <= SALVO_WINDOW:
                cluster_count += 1
            else:
                if cluster_count >= 2:
                    rows.append({
                        "zone":          zone,
                        "group":         group,
                        "date_str":      cluster_start.strftime("%Y-%m-%d"),
                        "cluster_start": cluster_start.isoformat(),
                        "cluster_size":  cluster_count,
                    })
                cluster_start = sorted_times[i]
                cluster_count = 1

        if cluster_count >= 2:
            rows.append({
                "zone":          zone,
                "group":         group,
                "date_str":      cluster_start.strftime("%Y-%m-%d"),
                "cluster_start": cluster_start.isoformat(),
                "cluster_size":  cluster_count,
            })

    if rows:
        return pd.DataFrame(rows)
    return pd.DataFrame(columns=["zone", "group", "date_str", "cluster_start", "cluster_size"])


# ── Chart ─────────────────────────────────────────────────────────────────────

def build_chart(chart_df: pd.DataFrame, mismatch_df: Optional[pd.DataFrame] = None,
                salvo_df: Optional[pd.DataFrame] = None,
                partial_day: Optional[str] = None, partial_hour: Optional[int] = None) -> None:
    """
    Full-screen interactive chart with three views toggled by a tab bar:

    "By Hour"    – stacked bar chart, X=hour 0-23, Y=alert count per region.
    "By Date"    – cumulative step lines, X=date, Y=running total per region.
    "Mismatches" – stacked bar chart showing paired / pre_only / missile_only
                   event counts per day; toggle between absolute and % view.

    Clicking any bar / line in Hour or Date views opens a small-multiples modal.
    Dark / light toggle persists across all views.
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
        annotations=([dict(
            x=partial_day, y=1.06, xref="x", yref="paper",
            text=f"\u26a0 {partial_day}: partial day (data through {partial_hour:02d}:xx)",
            showarrow=False, font=dict(size=11, color="#ffaa44"),
            xanchor="center",
        )] if partial_day else []),
    )

    # ── Mismatch records for JS-side chart building ────────────────────────
    # Pre-aggregate to (group, date_str, event_type, count) — much smaller than raw rows
    if mismatch_df is not None and not mismatch_df.empty:
        mismatch_agg = (
            mismatch_df.groupby(["group", "date_str", "event_type"])
            .size()
            .reset_index(name="count")
        )
        mismatch_records_js = json.dumps(mismatch_agg.to_dict(orient="records"))
    else:
        mismatch_records_js = "[]"

    # ── Gap-seconds arrays for lead-time histogram ──────────────────────────
    if mismatch_df is not None and not mismatch_df.empty and "gap_seconds" in mismatch_df.columns:
        gap_missile_js = json.dumps(
            mismatch_df[mismatch_df["event_type"] == "paired_missile"][["group", "gap_seconds"]]
            .dropna().assign(gap_seconds=lambda d: d["gap_seconds"].astype(int))
            .rename(columns={"gap_seconds": "gap"}).to_dict(orient="records")
        )
        gap_drone_js = json.dumps(
            mismatch_df[mismatch_df["event_type"] == "paired_drone"][["group", "gap_seconds"]]
            .dropna().assign(gap_seconds=lambda d: d["gap_seconds"].astype(int))
            .rename(columns={"gap_seconds": "gap"}).to_dict(orient="records")
        )
    else:
        gap_missile_js = "[]"
        gap_drone_js   = "[]"

    # ── Salvo cluster records for JS (individual clusters, aggregated client-side) ─
    if salvo_df is not None and not salvo_df.empty:
        salvo_records_js = json.dumps(
            salvo_df[["group", "date_str", "cluster_size"]].to_dict(orient="records")
        )
        salvo_max_size_js = int(salvo_df["cluster_size"].max())
    else:
        salvo_records_js  = "[]"
        salvo_max_size_js = 10

    # ── Serialise everything for JS ────────────────────────────────────────
    def fig_json(fig):
        d = json.loads(fig.to_json())
        return json.dumps(d["data"]), json.dumps(d["layout"])

    hour_data_js,      hour_layout_js      = fig_json(hour_fig)
    date_mini_data_js, date_mini_layout_js = fig_json(date_mini_fig)
    date_data_js,      date_layout_js      = fig_json(date_fig)

    hourly_js       = json.dumps(chart_df.to_dict(orient="records"))
    group_colors_js = json.dumps(GROUP_COLORS)
    groups_js       = json.dumps(groups)
    alert_types_js  = json.dumps(alert_types)
    partial_day_js  = json.dumps(partial_day)
    partial_hour_js = json.dumps(partial_hour)

    dark_main  = json.dumps({"plot_bgcolor": "#1a1a2e", "paper_bgcolor": "#0f0f1a",
                              "font.color": "#cccccc", "title.font.color": "#cccccc",
                              "xaxis.color": "#cccccc", "xaxis.gridcolor": "#2a2a3e",
                              "yaxis.color": "#cccccc", "yaxis.gridcolor": "#2a2a3e",
                              "legend.bgcolor": "rgba(26,26,46,0.85)", "legend.bordercolor": "#444",
                              "legend.font.color": "#cccccc",
                              "xaxis.rangeselector.bgcolor": "#252540",
                              "xaxis.rangeselector.activecolor": "#444470",
                              "xaxis.rangeselector.font.color": "#cccccc"})
    light_main = json.dumps({"plot_bgcolor": "white",   "paper_bgcolor": "#fafafa",
                              "font.color": "#333",     "title.font.color": "#333",
                              "xaxis.color": "#333",    "xaxis.gridcolor": "#e0e0e0",
                              "yaxis.color": "#333",    "yaxis.gridcolor": "#e0e0e0",
                              "legend.bgcolor": "rgba(255,255,255,0.85)", "legend.bordercolor": "#ccc",
                              "legend.font.color": "#333",
                              "xaxis.rangeselector.bgcolor": "#e8e8f0",
                              "xaxis.rangeselector.activecolor": "#c0c0e0",
                              "xaxis.rangeselector.font.color": "#333"})
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
    /* ── Chart area ── */
    #view-hour, #view-date, #view-mismatch, #view-leadtime, #view-salvos {{ position: absolute; left:0; right:0; }}
    #view-hour {{
      top: 46px;
      bottom: 180px;   /* room for date slider */
    }}
    #view-date {{
      top: 46px;
      bottom: 0;
      display: none;
    }}
    #view-mismatch {{
      top: 46px;
      bottom: 0;
      display: none;
    }}
    #view-leadtime {{
      top: 46px;
      bottom: 0;
      display: none;
    }}
    #view-salvos {{
      top: 46px;
      bottom: 0;
      display: none;
    }}
    #leadtime-chart {{ width:100%; height:100%; }}
    #salvos-chart {{ width:100%; height:100%; }}
    #main-chart, #date-full-chart {{ width:100%; height:100%; }}
    #mismatch-chart-bar {{ width:100%; height:57%; }}
    #mismatch-chart-cum {{ width:100%; height:43%; }}
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

    .tb-sep {{ width:1px; height:22px; background:#333; margin:0 4px; flex-shrink:0; }}
    body.light .tb-sep {{ background: #ccc; }}

    #date-select-label {{ font-size:12px; color:#aaa; white-space:nowrap; }}
    body.light #date-select-label {{ color: #555; }}

    #type-label {{ font-size: 11px; color: #777; white-space: nowrap; }}
    body.light #type-label {{ color: #555; }}

    .tb-region-select {{
      background: #252540; color: #ccc; border: 1px solid #444;
      border-radius: 6px; padding: 3px 8px; font-size: 12px; cursor: pointer;
    }}
    body.light .tb-region-select {{ background: #fff; color: #333; border-color: #bbb; }}

    body.light #modal-close {{ color: #555; }}
  </style>
</head>
<body>

  <!-- Top bar -->
  <div id="topbar">
    <button class="tb-btn" onclick="toggleTheme()" id="theme-btn">&#9728;&#65039;&nbsp;Light</button>
    <div id="sep"></div>
    <button class="tb-btn active" onclick="setView('hour')"     id="btn-hour">&#9200;&nbsp;By Hour</button>
    <button class="tb-btn"        onclick="setView('date')"     id="btn-date">&#128197;&nbsp;By Date</button>
    <button class="tb-btn"        onclick="setView('mismatch')" id="btn-mismatch">&#9888;&#65039;&nbsp;Mismatches</button>
    <button class="tb-btn"        onclick="setView('leadtime')" id="btn-leadtime">&#9203;&nbsp;Lead Time</button>
    <button class="tb-btn"        onclick="setView('salvos')"  id="btn-salvos">&#128165;&nbsp;Salvos</button>
    <div id="sep2" class="tb-sep"></div>
    <label for="date-select" id="date-select-label">Date:</label>
    <select id="date-select" onchange="onDateSelect(this.value)"></select>
    <div id="sep3" class="tb-sep"></div>
    <span id="type-label">Alert type:</span>
    <div id="type-btns" style="display:flex;gap:6px;"></div>
    <div id="mismatch-controls" style="display:none;align-items:center;gap:6px;">
      <div class="tb-sep"></div>
      <select id="mismatch-region-select" class="tb-region-select" onchange="onMismatchRegion(this.value)">
        <option value="">All regions</option>
      </select>
      <button class="tb-btn" onclick="toggleMismatchMode()" id="mismatch-mode-btn">%&nbsp;View</button>
    </div>
    <div id="leadtime-controls" style="display:none;align-items:center;gap:6px;">
      <div class="tb-sep"></div>
      <select id="leadtime-region-select" class="tb-region-select" onchange="onLeadtimeRegion(this.value)">
        <option value="">All regions</option>
      </select>
    </div>
    <div id="salvos-controls" style="display:none;align-items:center;gap:6px;">
      <div class="tb-sep"></div>
      <select id="salvos-region-select" class="tb-region-select" onchange="onSalvosRegion(this.value)">
        <option value="">All regions</option>
      </select>
      <div class="tb-sep"></div>
      <span id="salvos-date-label" style="font-size:12px;color:#aaa;white-space:nowrap;">From:</span>
      <select id="salvos-date-from" class="tb-region-select" onchange="onSalvosDateFrom(this.value)"></select>
      <span style="font-size:12px;color:#aaa;">To:</span>
      <select id="salvos-date-to" class="tb-region-select" onchange="onSalvosDateTo(this.value)"></select>
      <div class="tb-sep"></div>
      <span style="font-size:12px;color:#aaa;white-space:nowrap;">Min size:</span>
      <input type="range" id="salvos-size-slider" min="2" max="10" value="2" step="1"
             style="width:70px;cursor:pointer;accent-color:#4455cc;"
             oninput="onSalvosMinSize(this.value)">
      <span id="salvos-size-label" style="font-size:12px;color:#ccc;min-width:14px;">2</span>
      <div class="tb-sep"></div>
      <button class="tb-btn" onclick="toggleSalvosMode()" id="salvos-mode-btn">&#128200;&nbsp;Missiles</button>
    </div>
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

  <!-- Mismatch view -->
  <div id="view-mismatch">
    <div id="mismatch-chart-bar"></div>
    <div id="mismatch-chart-cum"></div>
  </div>

  <!-- Lead-time histogram view -->
  <div id="view-leadtime">
    <div id="leadtime-chart"></div>
  </div>

  <!-- Salvos view -->
  <div id="view-salvos">
    <div id="salvos-chart"></div>
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

  <script src="plotly.min.js" charset="utf-8"></script>
  <script>
    // ── Data ────────────────────────────────────────────────────────────────
    var hourlyData   = {hourly_js};
    var groupColors  = {group_colors_js};
    var allGroups    = {groups_js};
    var allTypes     = {alert_types_js};

    var hourData            = {hour_data_js};
    var hourLayout          = {hour_layout_js};
    var miniData            = {date_mini_data_js};
    var miniLayout          = {date_mini_layout_js};
    var dateData            = {date_data_js};
    var dateLayout          = {date_layout_js};
    var allMismatchRecords  = {mismatch_records_js};
    var gapMissileSeconds   = {gap_missile_js};
    var gapDroneSeconds     = {gap_drone_js};
    var allSalvoRecords     = {salvo_records_js};
    var salvoMaxSize        = {salvo_max_size_js};
    var partialDay          = {partial_day_js};
    var partialHour         = {partial_hour_js};

    var darkMain    = {dark_main};
    var lightMain   = {light_main};
    var darkMini    = {dark_mini};
    var lightMini   = {light_mini};

    // ── State ───────────────────────────────────────────────────────────────
    var isDark            = true;
    var currentView       = 'hour';
    var currentDateRange  = null;          // null = all dates
    var activeTypes       = new Set(allTypes);
    var salvosIsMissiles  = false;
    var salvosRegion      = '';
    var salvosDateFrom    = '';
    var salvosDateTo      = '';
    var salvosMinSize     = 2;

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
      document.getElementById('view-hour').style.display      = v === 'hour'     ? 'block' : 'none';
      document.getElementById('view-date').style.display      = v === 'date'     ? 'block' : 'none';
      document.getElementById('view-mismatch').style.display  = v === 'mismatch' ? 'block' : 'none';
      document.getElementById('view-leadtime').style.display  = v === 'leadtime' ? 'block' : 'none';
      document.getElementById('view-salvos').style.display    = v === 'salvos'   ? 'block' : 'none';
      document.getElementById('date-mini-wrap').style.display = v === 'hour'     ? 'block' : 'none';
      document.getElementById('btn-hour').classList.toggle('active',     v === 'hour');
      document.getElementById('btn-date').classList.toggle('active',     v === 'date');
      document.getElementById('btn-mismatch').classList.toggle('active', v === 'mismatch');
      document.getElementById('btn-leadtime').classList.toggle('active', v === 'leadtime');
      document.getElementById('btn-salvos').classList.toggle('active',   v === 'salvos');
      document.getElementById('type-label').style.display        = v === 'hour'     ? 'inline' : 'none';
      document.getElementById('type-btns').style.display         = v === 'hour'     ? 'flex'   : 'none';
      document.getElementById('date-select-label').style.display = v === 'hour'     ? 'inline' : 'none';
      document.getElementById('date-select').style.display       = v === 'hour'     ? 'inline' : 'none';
      document.getElementById('sep3').style.display              = v === 'hour'     ? 'block'  : 'none';
      document.getElementById('mismatch-controls').style.display  = v === 'mismatch' ? 'flex'   : 'none';
      document.getElementById('leadtime-controls').style.display  = v === 'leadtime' ? 'flex'   : 'none';
      document.getElementById('salvos-controls').style.display    = v === 'salvos'   ? 'flex'   : 'none';
      // Force Plotly to redraw at the now-correct dimensions
      setTimeout(function() {{
        if (v === 'date') {{ Plotly.Plots.resize('date-full-chart'); }}
        else if (v === 'mismatch') {{
          Plotly.Plots.resize('mismatch-chart-bar');
          Plotly.Plots.resize('mismatch-chart-cum');
        }} else if (v === 'leadtime') {{
          buildLeadTimeChart(leadtimeRegion);
        }} else if (v === 'salvos') {{
          buildSalvosChart();
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

    // ── Mismatch chart ───────────────────────────────────────────────────────
    var mismatchIsPct    = false;
    var mismatchRegion   = '';
    var leadtimeRegion   = '';

    var EVT_ORDER  = ['paired_missile','paired_drone','pre_only','missile_only','drone_only'];
    var EVT_LABELS = {{
      paired_missile: 'Paired (missile)',
      paired_drone:   'Paired (drone)',
      pre_only:       'Pre-alert only',
      missile_only:   'Missile only',
      drone_only:     'Drone only',
    }};
    var EVT_COLORS = {{
      paired_missile: '#2ca02c',
      paired_drone:   '#17becf',
      pre_only:       '#f5c542',
      missile_only:   '#d62728',
      drone_only:     '#ff7f0e',
    }};

    // Populate region dropdown once
    (function() {{
      var sel = document.getElementById('mismatch-region-select');
      var regions = [...new Set(allMismatchRecords.map(function(r) {{ return r.group; }}))].sort();
      regions.forEach(function(g) {{
        var o = document.createElement('option');
        o.value = g; o.textContent = g;
        sel.appendChild(o);
      }});
    }})();
    buildMismatchCharts('');

    // Populate leadtime region dropdown once
    (function() {{
      var sel = document.getElementById('leadtime-region-select');
      var regions = [...new Set(
        gapMissileSeconds.concat(gapDroneSeconds).map(function(r) {{ return r.group; }})
      )].sort();
      regions.forEach(function(g) {{
        var o = document.createElement('option');
        o.value = g; o.textContent = g;
        sel.appendChild(o);
      }});
    }})();

    // Populate salvos dropdowns once
    (function() {{
      var dates   = [...new Set(allSalvoRecords.map(function(r) {{ return r.date_str; }}))].sort();
      var regions = [...new Set(allSalvoRecords.map(function(r) {{ return r.group; }}))].sort();

      var rSel = document.getElementById('salvos-region-select');
      regions.forEach(function(g) {{
        var o = document.createElement('option'); o.value = g; o.textContent = g; rSel.appendChild(o);
      }});

      var fromSel = document.getElementById('salvos-date-from');
      var toSel   = document.getElementById('salvos-date-to');
      var allOpt  = document.createElement('option'); allOpt.value = ''; allOpt.textContent = 'All';
      fromSel.appendChild(allOpt.cloneNode(true));
      toSel.appendChild(allOpt);
      dates.forEach(function(d) {{
        var of = document.createElement('option'); of.value = d; of.textContent = d; fromSel.appendChild(of);
        var ot = document.createElement('option'); ot.value = d; ot.textContent = d; toSel.appendChild(ot);
      }});
    }})();
    buildSalvosChart();

    // Set slider max from data
    (function() {{
      var sl = document.getElementById('salvos-size-slider');
      sl.max = salvoMaxSize;
    }})();

    function onSalvosRegion(val)   {{ salvosRegion  = val; buildSalvosChart(); }}
    function onSalvosDateFrom(val) {{ salvosDateFrom = val; buildSalvosChart(); }}
    function onSalvosDateTo(val)   {{ salvosDateTo   = val; buildSalvosChart(); }}
    function onSalvosMinSize(val)  {{
      salvosMinSize = parseInt(val);
      document.getElementById('salvos-size-label').textContent = val;
      buildSalvosChart();
    }}

    function onLeadtimeRegion(val) {{
      leadtimeRegion = val;
      buildLeadTimeChart(val);
    }}

    function onMismatchRegion(val) {{
      mismatchRegion = val;
      buildMismatchCharts(val);
    }}

    function toggleMismatchMode() {{
      mismatchIsPct = !mismatchIsPct;
      document.getElementById('mismatch-mode-btn').innerHTML =
        mismatchIsPct ? 'Abs&nbsp;View' : '%&nbsp;View';
      buildMismatchCharts(mismatchRegion);
    }}

    function toggleSalvosMode() {{
      salvosIsMissiles = !salvosIsMissiles;
      document.getElementById('salvos-mode-btn').innerHTML =
        salvosIsMissiles ? '&#128200;&nbsp;Clusters' : '&#128200;&nbsp;Missiles';
      buildSalvosChart();
    }}

    function buildMismatchCharts(region) {{
      var records = region
        ? allMismatchRecords.filter(function(r) {{ return r.group === region; }})
        : allMismatchRecords;

      // Aggregate daily counts
      var dateMap = {{}};
      records.forEach(function(r) {{
        if (!dateMap[r.date_str]) {{
          dateMap[r.date_str] = {{paired_missile:0,paired_drone:0,pre_only:0,missile_only:0,drone_only:0}};
        }}
        if (dateMap[r.date_str][r.event_type] !== undefined) {{
          dateMap[r.date_str][r.event_type] += r.count;
        }}
      }});

      var dates = Object.keys(dateMap).sort();
      var theme = isDark
        ? {{bg:'#1a1a2e', paper:'#0f0f1a', grid:'#2a2a3e', text:'#cccccc', roll:'#ffffff', legendBg:'rgba(26,26,46,0.85)',    legendBorder:'#444'}}
        : {{bg:'white',   paper:'#fafafa', grid:'#e0e0e0', text:'#333333', roll:'#555555', legendBg:'rgba(255,255,255,0.85)', legendBorder:'#ccc'}};

      if (!dates.length) {{
        var empty = {{plot_bgcolor:theme.bg, paper_bgcolor:theme.paper,
                     font:{{color:theme.text}},
                     title:{{text:'No data for selected region', x:0.5,
                             font:{{color:theme.text}}}}}};
        Plotly.react('mismatch-chart-bar', [], empty);
        Plotly.react('mismatch-chart-cum', [], empty);
        return;
      }}

      // Per-type daily arrays
      var daily = {{}};
      EVT_ORDER.forEach(function(et) {{
        daily[et] = dates.map(function(d) {{ return dateMap[d][et] || 0; }});
      }});
      var totals = dates.map(function(_, i) {{
        return EVT_ORDER.reduce(function(s, et) {{ return s + daily[et][i]; }}, 0);
      }});

      // 7-day rolling mismatch rate (pre_only + missile_only + drone_only) / total
      var rollRate = dates.map(function(_, i) {{
        var start = Math.max(0, i - 6);
        var wTot = 0, wMis = 0;
        for (var j = start; j <= i; j++) {{
          wTot += totals[j];
          wMis += daily.pre_only[j] + daily.missile_only[j] + daily.drone_only[j];
        }}
        return wTot > 0 ? parseFloat((wMis / wTot * 100).toFixed(1)) : null;
      }});

      // Bar traces
      var barTraces = EVT_ORDER.map(function(et) {{
        var ys = mismatchIsPct
          ? dates.map(function(_, i) {{
              return totals[i] > 0
                ? parseFloat((daily[et][i] / totals[i] * 100).toFixed(1)) : 0;
            }})
          : daily[et];
        return {{
          type:'bar', x:dates, y:ys, name:EVT_LABELS[et],
          marker:{{color:EVT_COLORS[et]}},
          hovertemplate: mismatchIsPct
            ? '<b>'+EVT_LABELS[et]+'</b><br>%{{x}}: <b>%{{y:.1f}}</b>%<extra></extra>'
            : '<b>'+EVT_LABELS[et]+'</b><br>%{{x}}: <b>%{{y:,}}</b><extra></extra>',
        }};
      }});
      barTraces.push({{
        type:'scatter', x:dates, y:rollRate, name:'7d mismatch %', yaxis:'y2',
        line:{{color:theme.roll, width:2, dash:'dot'}}, mode:'lines',
        hovertemplate:'7d mismatch rate: <b>%{{y:.1f}}%</b><extra></extra>',
      }});

      var regionLabel = region ? ' — ' + region : ' — All Regions';
      var barLayout = {{
        barmode:'stack',
        title:{{text:'Mismatch by Day'+regionLabel+'<br><sup>15-min pairing · 7-day mismatch % (dotted) on right axis</sup>',
                x:0.5, font:{{size:14,color:theme.text}}}},
        xaxis:{{showgrid:true, gridcolor:theme.grid, color:theme.text, zeroline:false,
                rangeslider:{{visible:true, thickness:0.05}},
                rangeselector:{{
                  buttons:[
                    {{count:7,label:'1w',step:'day',stepmode:'backward'}},
                    {{count:14,label:'2w',step:'day',stepmode:'backward'}},
                    {{step:'all',label:'All'}},
                  ],
                  bgcolor:'#252540', activecolor:'#444470', font:{{color:'#cccccc'}},
                }}}},
        yaxis:{{title:mismatchIsPct?'% of Events':'Event Count',
                showgrid:true, gridcolor:theme.grid, zeroline:false, color:theme.text}},
        yaxis2:{{title:'Mismatch %', overlaying:'y', side:'right',
                 showgrid:false, zeroline:false, color:theme.text, range:[0,100]}},
        plot_bgcolor:theme.bg, paper_bgcolor:theme.paper,
        font:{{family:'Arial, Helvetica, sans-serif', color:theme.text}},
        legend:{{font:{{size:11,color:theme.text}},
                 bgcolor:theme.legendBg, bordercolor:theme.legendBorder, borderwidth:1}},
        margin:{{t:70, b:60, l:70, r:70}},
        annotations: partialDay ? [{{
          x: partialDay, y: 1.06, xref: 'x', yref: 'paper',
          text: '\u26a0 ' + partialDay + ': partial day (data to ' + partialHour + ':xx)',
          showarrow: false, font: {{size: 10, color: '#ffaa44'}},
          xanchor: 'center',
        }}] : [],
      }};

      // Cumulative traces
      var cumTraces = EVT_ORDER.map(function(et) {{
        var cum = 0;
        var cumY = daily[et].map(function(v) {{ cum += v; return cum; }});
        return {{
          type:'scatter', x:dates, y:cumY, name:EVT_LABELS[et],
          line:{{color:EVT_COLORS[et], width:2.5}},
          mode:'lines+markers', marker:{{size:4, color:EVT_COLORS[et]}},
          hovertemplate:'<b>'+EVT_LABELS[et]+'</b><br>%{{x}}: cumulative <b>%{{y:,}}</b><extra></extra>',
        }};
      }});
      var cumLayout = {{
        title:{{text:'Cumulative'+regionLabel, x:0.5, font:{{size:14,color:theme.text}}}},
        xaxis:{{showgrid:true, gridcolor:theme.grid, color:theme.text, zeroline:false,
                rangeslider:{{visible:true, thickness:0.05}}}},
        yaxis:{{title:'Cumulative Events', showgrid:true, gridcolor:theme.grid,
                zeroline:false, color:theme.text}},
        hovermode:'x unified',
        plot_bgcolor:theme.bg, paper_bgcolor:theme.paper,
        font:{{family:'Arial, Helvetica, sans-serif', color:theme.text}},
        legend:{{font:{{size:11,color:theme.text}},
                 bgcolor:theme.legendBg, bordercolor:theme.legendBorder, borderwidth:1}},
        margin:{{t:50, b:60, l:70, r:40}},
        annotations: partialDay ? [{{
          x: partialDay, y: 1.06, xref: 'x', yref: 'paper',
          text: '\u26a0 ' + partialDay + ': partial day (data to ' + partialHour + ':xx)',
          showarrow: false, font: {{size: 10, color: '#ffaa44'}},
          xanchor: 'center',
        }}] : [],
      }};

      Plotly.react('mismatch-chart-bar', barTraces, barLayout, {{responsive:true}});
      Plotly.react('mismatch-chart-cum', cumTraces, cumLayout, {{responsive:true}});
    }}

    // ── Lead-time histogram ──────────────────────────────────────────────────
    function buildLeadTimeChart(region) {{
      var theme = isDark
        ? {{bg:'#1a1a2e', paper:'#0f0f1a', grid:'#2a2a3e', text:'#cccccc', legendBg:'rgba(26,26,46,0.85)',    legendBorder:'#444'}}
        : {{bg:'white',   paper:'#fafafa', grid:'#e0e0e0', text:'#333333', legendBg:'rgba(255,255,255,0.85)', legendBorder:'#ccc'}};

      // Filter by region then convert gap seconds → minutes
      function toMinutes(arr) {{
        var filtered = region ? arr.filter(function(r) {{ return r.group === region; }}) : arr;
        return filtered.map(function(r) {{ return parseFloat((r.gap / 60).toFixed(2)); }});
      }}

      var missileMin = toMinutes(gapMissileSeconds);
      var droneMin   = toMinutes(gapDroneSeconds);

      var traces = [];
      if (missileMin.length) {{
        traces.push({{
          type: 'histogram',
          x: missileMin,
          name: 'Paired (missile)',
          autobinx: false,
          xbins: {{start: 0, end: 15, size: 0.5}},
          marker: {{color: '#2ca02c', opacity: 0.75}},
          hovertemplate: '<b>Paired (missile)</b><br>%{{x:.1f}}–%{{customdata:.1f}} min: <b>%{{y:,}}</b><extra></extra>',
          customdata: missileMin.map(function(v) {{ return v + 0.5; }}),
        }});
      }}
      if (droneMin.length) {{
        traces.push({{
          type: 'histogram',
          x: droneMin,
          name: 'Paired (drone)',
          autobinx: false,
          xbins: {{start: 0, end: 15, size: 0.5}},
          marker: {{color: '#17becf', opacity: 0.75}},
          hovertemplate: '<b>Paired (drone)</b><br>%{{x:.1f}}–%{{customdata:.1f}} min: <b>%{{y:,}}</b><extra></extra>',
          customdata: droneMin.map(function(v) {{ return v + 0.5; }}),
        }});
      }}
      if (!traces.length) {{
        traces = [{{type:'scatter', x:[], y:[], showlegend:false}}];
      }}

      var nMissile = missileMin.length;
      var nDrone   = droneMin.length;
      var regionLabel = region ? ' — ' + region : '';
      var subtitle = 'Paired events only · 30-second bins · '
        + nMissile.toLocaleString() + ' missile pairs'
        + (nDrone ? ', ' + nDrone.toLocaleString() + ' drone pairs' : '');

      var viewEl = document.getElementById('view-leadtime');
      var viewH  = viewEl.offsetHeight || (window.innerHeight - 46);
      var viewW  = viewEl.offsetWidth  || window.innerWidth;

      var layout = {{
        barmode: 'overlay',
        height: viewH,
        width:  viewW,
        title: {{
          text: 'Warning Lead Time Distribution' + regionLabel + '<br><sup>' + subtitle + '</sup>',
          x: 0.5, font: {{size: 15, color: theme.text}},
        }},
        xaxis: {{
          title: 'Minutes from Pre-alert to Paired Alert',
          range: [0, 15], dtick: 1,
          showgrid: true, gridcolor: theme.grid,
          zeroline: false, color: theme.text,
        }},
        yaxis: {{
          title: 'Number of Pre-alerts',
          showgrid: true, gridcolor: theme.grid,
          zeroline: false, color: theme.text,
        }},
        plot_bgcolor:  theme.bg,
        paper_bgcolor: theme.paper,
        font: {{family: 'Arial, Helvetica, sans-serif', color: theme.text}},
        legend: {{
          font: {{size: 11, color: theme.text}},
          bgcolor: theme.legendBg, bordercolor: theme.legendBorder, borderwidth: 1,
        }},
        margin: {{t: 80, b: 60, l: 70, r: 40}},
      }};

      Plotly.react('leadtime-chart', traces, layout, {{responsive: true}});
    }}

    buildLeadTimeChart('');

    // ── Salvo clusters heatmap ───────────────────────────────────────────────
    function buildSalvosChart() {{
      var yKey   = salvosIsMissiles ? 'total_missiles' : 'cluster_count';
      var yLabel = salvosIsMissiles ? 'Total Missiles in Salvos' : 'Salvo Clusters';

      var colorscale = isDark
        ? [[0,'#0f0f1a'],[0.001,'#3d1515'],[0.25,'#7f0000'],[0.5,'#c0392b'],[0.75,'#e74c3c'],[1,'#ff8c00']]
        : [[0,'#f5f5f5'],[0.001,'#fee0d2'],[0.25,'#fc9272'],[0.5,'#fb6a4a'],[0.75,'#cb181d'],[1,'#67000d']];
      var textColor = isDark ? '#cccccc' : '#333333';
      var paperBg   = isDark ? '#0f0f1a' : '#fafafa';

      // Apply filters to individual cluster records
      var filtered = allSalvoRecords.filter(function(r) {{
        return r.cluster_size >= salvosMinSize
          && (!salvosRegion   || r.group    === salvosRegion)
          && (!salvosDateFrom || r.date_str >= salvosDateFrom)
          && (!salvosDateTo   || r.date_str <= salvosDateTo);
      }});

      // Aggregate filtered records to (group, date_str)
      var aggMap = {{}};
      filtered.forEach(function(r) {{
        var key = r.group + '|' + r.date_str;
        if (!aggMap[key]) aggMap[key] = {{group: r.group, date_str: r.date_str, cluster_count: 0, total_missiles: 0}};
        aggMap[key].cluster_count++;
        aggMap[key].total_missiles += r.cluster_size;
      }});
      var records = Object.keys(aggMap).map(function(k) {{
        var a = aggMap[k];
        a.avg_size = Math.round(a.total_missiles / a.cluster_count * 10) / 10;
        return a;
      }});

      if (!records.length) {{
        Plotly.react('salvos-chart', [], {{
          paper_bgcolor: paperBg, font: {{color: textColor}},
          title: {{text: 'No salvo data for selected filters', x: 0.5, font: {{color: textColor}}}},
        }}, {{responsive: true}});
        return;
      }}

      // Build dimension arrays and lookup
      var dateSet = {{}}, regionSet = {{}};
      records.forEach(function(r) {{
        dateSet[r.date_str] = true;
        regionSet[r.group]  = true;
      }});
      var dates   = Object.keys(dateSet).sort();
      var regions = Object.keys(regionSet).sort();

      var lookup = {{}};
      records.forEach(function(r) {{
        if (!lookup[r.group]) lookup[r.group] = {{}};
        lookup[r.group][r.date_str] = r;
      }});

      // Build Z matrix and hover text
      var z    = regions.map(function(reg) {{
        return dates.map(function(d) {{
          var rec = lookup[reg] && lookup[reg][d];
          return rec ? rec[yKey] : null;
        }});
      }});
      var text = regions.map(function(reg) {{
        return dates.map(function(d) {{
          var rec = lookup[reg] && lookup[reg][d];
          if (!rec) return reg + '<br>' + d + '<br>No salvos';
          return '<b>' + reg + '</b><br>' + d +
            '<br>Clusters: <b>' + rec.cluster_count + '</b>' +
            '<br>Missiles: <b>' + rec.total_missiles + '</b>' +
            '<br>Avg size: <b>' + rec.avg_size + '</b>';
        }});
      }});

      var trace = {{
        type: 'heatmap',
        x: dates,
        y: regions,
        z: z,
        text: text,
        hoverinfo: 'text',
        colorscale: colorscale,
        zmin: 0,
        xgap: 1, ygap: 2,
        colorbar: {{
          title: {{text: yLabel, side: 'right', font: {{color: textColor, size: 11}}}},
          thickness: 14,
          tickfont: {{color: textColor, size: 10}},
          outlinecolor: textColor,
          outlinewidth: 0.5,
        }},
      }};

      var sizeNote = salvosMinSize > 2 ? ' \u00b7 min size \u2265' + salvosMinSize : '';
      var annotations = partialDay ? [{{
        x: partialDay, y: -0.12, xref: 'x', yref: 'paper',
        text: '\u26a0 partial', showarrow: false,
        font: {{size: 9, color: '#ffaa44'}}, xanchor: 'center',
      }}] : [];

      var viewEl = document.getElementById('view-salvos');
      var viewH  = viewEl.offsetHeight || (window.innerHeight - 46);
      var viewW  = viewEl.offsetWidth  || window.innerWidth;

      var layout = {{
        height: viewH,
        width:  viewW,
        title: {{
          text: 'Salvo Intensity by Region & Day' +
                '<br><sup>Color = ' + yLabel +
                ' \u00b7 salvo = 2+ missile alerts, gap \u226430 min' + sizeNote + '</sup>',
          x: 0.5, font: {{size: 14, color: textColor}},
        }},
        xaxis: {{
          type: 'category',
          tickangle: -55,
          tickfont: {{size: 9, color: textColor}},
          color: textColor,
          showgrid: false,
        }},
        yaxis: {{
          automargin: true,
          tickfont: {{size: 11, color: textColor}},
          color: textColor,
          showgrid: false,
        }},
        plot_bgcolor:  paperBg,
        paper_bgcolor: paperBg,
        font: {{family: 'Arial, Helvetica, sans-serif', color: textColor}},
        margin: {{t: 80, b: 90, l: 145, r: 80}},
        annotations: annotations,
      }};

      Plotly.react('salvos-chart', [trace], layout, {{responsive: true}});
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
      buildMismatchCharts(mismatchRegion);
      buildLeadTimeChart(leadtimeRegion);
      buildSalvosChart();
      document.getElementById('date-mini-wrap').style.background =
        isDark ? '#0f0f1a' : '#fafafa';
    }}
  </script>
</body>
</html>"""

    OUTPUT_DIR.mkdir(exist_ok=True)
    outfile = OUTPUT_DIR / "index.html"
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

    # 2. Alert data — GitHub CSV first, local file as fallback
    raw = fetch_github_csv()
    if raw is not None:
        df = _normalise_df(raw)
    else:
        data_file = find_data_file()
        if data_file is None:
            print("\nNo data found. Place an .xlsx/.csv in data/ or check network.")
            sys.exit(1)
        df = load_alerts(data_file)

    # 2b. Detect partial day: if the latest date in the data is today, the day isn't over.
    #     Use current clock time for the "through hour" annotation, not the last alert time.
    last_date   = df["date_str"].dropna().max()
    today_str   = date.today().isoformat()
    is_partial  = last_date == today_str
    partial_day = last_date if is_partial else None
    partial_hour = datetime.now().hour if is_partial else None
    if partial_day:
        print(f"  Partial day detected: {partial_day} — fetched at hour {partial_hour:02d}:xx")

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

    # 5. Pre-alert / missile mismatch analysis
    print("\nComputing pre-alert / missile mismatches …")
    mismatch_df = compute_mismatches(df, city_to_zone)
    if not mismatch_df.empty:
        counts = mismatch_df["event_type"].value_counts()
        for et in ALL_EVENT_TYPES:
            print(f"  {et:<16}: {counts.get(et, 0):,}")

        OUTPUT_DIR.mkdir(exist_ok=True)
        xlsx_path = OUTPUT_DIR / "mismatches.xlsx"
        mismatch_df.to_excel(xlsx_path, index=False)
        print(f"  Saved → {xlsx_path}")

    # 5b. Salvo cluster analysis
    print("\nComputing salvo clusters …")
    salvo_df = compute_salvos(df, city_to_zone)
    if not salvo_df.empty:
        print(f"  Salvo clusters found    : {len(salvo_df):,}")
        print(f"  Total missiles in salvos: {salvo_df['cluster_size'].sum():,}")
        print(f"  Zones with salvos       : {salvo_df['zone'].nunique():,}")
    else:
        print("  No salvo clusters found.")

    # 6. Chart
    build_chart(chart_df, mismatch_df, salvo_df=salvo_df,
                partial_day=partial_day, partial_hour=partial_hour)
    print("\nDone.  Open output/index.html in your browser.")


if __name__ == "__main__":
    main()
