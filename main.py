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


# ── Situation Room ─────────────────────────────────────────────────────────────

def compute_situation(chart_df: pd.DataFrame) -> dict:
    """Return last-night and today alert summaries for the Situation Room tab."""
    from datetime import timezone
    now   = datetime.now(timezone.utc)
    today = now.date()
    yest  = today - timedelta(days=1)

    ln_start = datetime(yest.year,  yest.month,  yest.day,  NIGHT_START)
    ln_end   = datetime(today.year, today.month, today.day, NIGHT_END)
    td_start = ln_end
    td_end   = datetime(today.year, today.month, today.day, now.hour, now.minute)

    def period_stats(start_dt: datetime, end_dt: datetime, label: str) -> dict:
        def row_in_range(row) -> bool:
            ds = row.get("date_str")
            h  = row.get("hour")
            if not ds or h is None:
                return False
            try:
                dt = datetime(int(ds[:4]), int(ds[5:7]), int(ds[8:10]), int(h))
            except (ValueError, TypeError):
                return False
            return start_dt <= dt < end_dt

        sub = chart_df[chart_df.apply(row_in_range, axis=1)]

        def type_sum(atype: str) -> int:
            return int(sub[sub["alert_type"] == atype]["count"].sum()) if not sub.empty else 0

        per_region_hourly: dict = {}
        for _, row in sub.iterrows():
            grp  = row.get("group") or "Other"
            hour = int(row.get("hour", 0))
            cnt  = int(row.get("count", 0))
            if grp not in per_region_hourly:
                per_region_hourly[grp] = [0] * 24
            per_region_hourly[grp][hour] += cnt

        return {
            "label":             label,
            "start_iso":         start_dt.isoformat(),
            "end_iso":           end_dt.isoformat(),
            "total_missile":     type_sum("Missile alert"),
            "total_pre":         type_sum("Pre-alert"),
            "total_drone":       type_sum("Drone alert"),
            "regions":           sorted(per_region_hourly.keys()),
            "per_region_hourly": per_region_hourly,
        }

    return {
        "last_night": period_stats(
            ln_start, ln_end,
            f"{NIGHT_START}:00 yesterday \u2192 {NIGHT_END:02d}:00 today"
        ),
        "today": period_stats(
            td_start, td_end,
            f"{NIGHT_END:02d}:00 today \u2192 now ({now.strftime('%H:%M')} UTC)"
        ),
    }


# ── Processed-data serialisation ──────────────────────────────────────────────

def save_processed(chart_df: pd.DataFrame, mismatch_df: Optional[pd.DataFrame],
                   salvo_df: Optional[pd.DataFrame],
                   partial_day: Optional[str], partial_hour: Optional[int],
                   fetched_at: Optional[str] = None) -> None:
    """Serialise aggregated data to data/processed.json for use by build_chart.py."""
    DATA_DIR.mkdir(exist_ok=True)
    payload = {
        "chart_df":    chart_df.to_dict(orient="records"),
        "mismatch_df": mismatch_df.to_dict(orient="records") if mismatch_df is not None else [],
        "salvo_df":    salvo_df.to_dict(orient="records")    if salvo_df    is not None else [],
        "partial_day":  partial_day,
        "partial_hour": partial_hour,
        "fetched_at":   fetched_at or datetime.now().isoformat(),
    }
    path = DATA_DIR / "processed.json"
    path.write_text(json.dumps(payload, default=str), encoding="utf-8")
    print(f"  Processed data saved → {path}")


# ── Chart ─────────────────────────────────────────────────────────────────────

def build_chart(chart_df: pd.DataFrame, mismatch_df: Optional[pd.DataFrame] = None,
                salvo_df: Optional[pd.DataFrame] = None,
                partial_day: Optional[str] = None, partial_hour: Optional[int] = None,
                situation_data: Optional[dict] = None,
                fetched_at: Optional[str] = None) -> None:
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
                 "<sup>Stacked by region · use filters above to narrow by date/region"
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
        n = len(gdf)
        label_text = [""] * (n - 1) + [group] if n > 0 else []
        date_traces.append(go.Scatter(
            x=gdf["date_str"].tolist(), y=gdf["cumulative"].tolist(),
            mode="lines+markers+text", name=group, showlegend=False,
            line=dict(color=color, width=2.5), marker=dict(size=5, color=color),
            text=label_text, textposition="middle right",
            textfont=dict(size=10, color=color),
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
                 "<sup>Deduplicated per zone · use date filters above to zoom</sup>",
            x=0.5, font=dict(size=15, color="#cccccc"),
        ),
        xaxis=dict(
            title="Date", showgrid=True, gridcolor="#2a2a3e",
            zeroline=False, color="#cccccc",
            type="date", dtick=86400000, tickformat="%b %d", tickangle=-45,
        ),
        yaxis=dict(
            title="Cumulative Alerts", showgrid=True,
            gridcolor="#2a2a3e", zeroline=False, color="#cccccc",
        ),
        plot_bgcolor="#1a1a2e", paper_bgcolor="#0f0f1a",
        font=dict(family="Arial, Helvetica, sans-serif", color="#cccccc"),
        showlegend=False,
        hovermode="closest",
        margin=dict(t=80, b=80, l=70, r=100),
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
            salvo_df[["group", "date_str", "cluster_size", "cluster_start"]].to_dict(orient="records")
        )
    else:
        salvo_records_js = "[]"

    # ── Situation Room data ───────────────────────────────────────────────────
    _empty_period = {"label": "", "start_iso": "", "end_iso": "",
                     "total_missile": 0, "total_pre": 0, "total_drone": 0,
                     "regions": [], "per_region_hourly": {}}
    situation_data_js = json.dumps(
        situation_data if situation_data else {"last_night": _empty_period, "today": _empty_period}
    )

    # ── Serialise everything for JS ────────────────────────────────────────
    def fig_json(fig):
        d = json.loads(fig.to_json())
        return json.dumps(d["data"]), json.dumps(d["layout"])

    hour_data_js,  hour_layout_js  = fig_json(hour_fig)
    date_data_js,  date_layout_js  = fig_json(date_fig)

    hourly_js       = json.dumps(chart_df.to_dict(orient="records"))
    group_colors_js = json.dumps(GROUP_COLORS)
    groups_js       = json.dumps(groups)
    alert_types_js  = json.dumps(alert_types)
    partial_day_js  = json.dumps(partial_day)
    partial_hour_js = json.dumps(partial_hour)
    fetched_at_js   = json.dumps(fetched_at or datetime.now().isoformat())

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
  <link rel="icon" type="image/svg+xml" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'><rect x='2' y='18' width='6' height='12' fill='%234e79a7'/><rect x='10' y='10' width='6' height='20' fill='%23f28e2b'/><rect x='18' y='4' width='6' height='26' fill='%23e15759'/><rect x='26' y='13' width='6' height='17' fill='%2376b7b2'/></svg>">
  <style>
    :root {{ --tb-h: 74px; }}
    *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
    html, body {{
      width: 100%; height: 100%; overflow: hidden;
      background: #fafafa; transition: background 0.25s;
      font-family: Arial, Helvetica, sans-serif;
    }}
    body.dark {{ background: #0f0f1a; }}

    /* ── Top bar ── */
    #topbar {{
      position: fixed; top: 0; left: 0; right: 0; z-index: 100;
      display: flex; flex-direction: column; align-items: stretch;
      background: rgba(245,245,252,0.97); backdrop-filter: blur(6px);
      border-bottom: 1px solid #ddd;
    }}
    #nav-row {{
      display: flex; align-items: center; gap: 10px; padding: 4px 16px; height: 40px;
    }}
    #nav-spacer {{ flex: 1; }}
    #filter-row {{
      display: flex; align-items: center; gap: 8px; padding: 3px 16px; min-height: 32px;
      border-top: 1px solid #ddd; background: rgba(230,230,248,0.9);
    }}
    body.dark #topbar {{ background: rgba(10,10,28,0.95); border-color: #2a2a3e; }}
    body.dark #filter-row {{ background: rgba(5,5,20,0.82); border-color: #2a2a3e; }}
    #nav-tabs {{ display: flex; align-items: center; gap: 8px; }}
    #hamburger-btn {{ display: none; align-items: center; gap: 5px; }}
    .tb-btn {{
      padding: 4px 12px; border-radius: 16px; cursor: pointer;
      font-size: 12px; font-weight: 600; border: 1px solid #ccc;
      background: #eee; color: #555; user-select: none;
      transition: background 0.15s, color 0.15s;
    }}
    .tb-btn:hover  {{ background: #dde; color: #222; }}
    .tb-btn.active {{ background: #4455cc; color: #fff; border-color: #4455cc; }}
    body.dark .tb-btn          {{ background: #252540; color: #bbb; border-color: #555; }}
    body.dark .tb-btn:hover    {{ background: #333360; color: #fff; }}
    body.dark .tb-btn.active   {{ background: #4455cc; color: #fff; border-color: #4455cc; }}

    /* ── Tab-style overrides for nav buttons only ── */
    #nav-tabs .tb-btn {{
      border-radius: 4px 4px 0 0;
      border: 1px solid transparent;
      border-bottom: 2px solid transparent;
      background: transparent;
      color: #666;
      padding: 5px 14px;
      position: relative;
      top: 1px;
    }}
    #nav-tabs .tb-btn:hover {{ background: rgba(68,85,204,0.08); color: #333; border-color: #ddd #ddd transparent; }}
    #nav-tabs .tb-btn.active {{ background: rgba(245,245,252,0.97); color: #4455cc; border-color: #ddd #ddd transparent; border-bottom: 2px solid #4455cc; }}
    body.dark #nav-tabs .tb-btn {{ background: transparent; color: #888; border-color: transparent; }}
    body.dark #nav-tabs .tb-btn:hover {{ background: rgba(68,85,204,0.15); color: #ccc; border-color: #444 #444 transparent; }}
    body.dark #nav-tabs .tb-btn.active {{ background: rgba(10,10,28,0.95); color: #7788ee; border-color: #444 #444 transparent; border-bottom: 2px solid #7788ee; }}

    #sep {{ width: 1px; height: 22px; background: #ccc; margin: 0 4px; flex-shrink:0; }}
    body.dark #sep {{ background: #333; }}
    /* ── Chart area ── */
    #view-hour, #view-date, #view-mismatch, #view-leadtime, #view-salvos {{ position: absolute; left:0; right:0; }}
    #view-hour  {{ top: var(--tb-h); bottom: 0; }}
    #view-date  {{ top: var(--tb-h); bottom: 0; display: none; }}
    #view-mismatch {{ top: var(--tb-h); bottom: 0; display: none; }}
    #view-leadtime {{ top: var(--tb-h); bottom: 0; display: none; }}
    #view-salvos   {{ top: var(--tb-h); bottom: 0; display: none; }}
    /* ── Situation Room ── */
    #view-situation {{ position: absolute; left:0; right:0; top: var(--tb-h); bottom: 0; display: none; overflow-y: auto; }}
    .sit-section {{ margin-bottom: 28px; }}
    .sit-section-title {{ font-size: 15px; font-weight: 700; margin-bottom: 6px; color: #4455cc; }}
    body.dark .sit-section-title {{ color: #7788ee; }}
    .sit-sublabel {{ font-size: 11px; color: #888; margin-bottom: 8px; }}
    .sit-summary {{ font-size: 13px; color: #444; margin-bottom: 12px; line-height: 1.6; }}
    body.dark .sit-summary {{ color: #aaa; }}
    .sit-quiet {{ font-style: italic; color: #888; }}
    .sit-sparklines {{ display: flex; flex-wrap: wrap; gap: 12px; }}
    .sit-sparkline-cell {{ display: flex; flex-direction: column; align-items: center; min-width: 110px; }}
    .sit-sparkline-label {{ font-size: 10px; color: #666; margin-top: 4px; text-align: center; max-width: 110px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
    body.dark .sit-sparkline-label {{ color: #888; }}
    #leadtime-chart {{ width:100%; height:100%; }}
    #salvos-chart {{ width:100%; height:100%; }}
    #main-chart, #date-full-chart {{ width:100%; height:100%; }}
    #mismatch-chart-bar {{ width:100%; height:100%; }}

    /* ── Modal ── */
    #modal-backdrop {{
      display: none; position: fixed; inset: 0; z-index: 2000;
      background: rgba(0,0,0,0.75); align-items: center; justify-content: center;
    }}
    #modal-backdrop.open {{ display: flex; }}
    #modal {{
      background: #fff; color: #333; border-radius: 10px;
      width: 93vw; max-height: 88vh; display: flex; flex-direction: column;
      box-shadow: 0 8px 40px rgba(0,0,0,0.25); border: 1px solid #ddd;
    }}
    body.dark #modal {{ background: #12122a; color: #ccc; border-color: #333; box-shadow: 0 8px 40px rgba(0,0,0,0.65); }}
    #modal-header {{
      display: flex; align-items: center; justify-content: space-between;
      padding: 13px 18px; border-bottom: 1px solid #ddd; flex-shrink: 0;
    }}
    body.dark #modal-header {{ border-color: #2a2a4a; }}
    #modal-title {{ font-size: 15px; font-weight: 700; }}
    #modal-close {{
      cursor: pointer; font-size: 22px; line-height: 1;
      background: none; border: none; color: #999; padding: 0 4px;
    }}
    #modal-close:hover {{ color: #000; }}
    body.dark #modal-close:hover {{ color: #fff; }}
    #modal-body {{ overflow-y: auto; flex: 1; padding: 10px; }}
    #modal-chart {{ width: 100%; }}
    #date-select {{
      background: #fff; color: #333; border: 1px solid #bbb;
      border-radius: 6px; padding: 3px 8px; font-size: 12px; cursor: pointer;
    }}
    body.dark #date-select {{ background: #252540; color: #ccc; border-color: #444; }}

    .tb-sep {{ width:1px; height:22px; background:#ccc; margin:0 4px; flex-shrink:0; }}
    body.dark .tb-sep {{ background: #333; }}

    #date-select-label {{ font-size:12px; color:#555; white-space:nowrap; }}
    body.dark #date-select-label {{ color: #aaa; }}

    #type-label {{ font-size: 11px; color: #555; white-space: nowrap; }}
    body.dark #type-label {{ color: #777; }}

    .tb-region-select {{
      background: #fff; color: #333; border: 1px solid #bbb;
      border-radius: 6px; padding: 3px 8px; font-size: 12px; cursor: pointer;
    }}
    body.dark .tb-region-select {{ background: #252540; color: #ccc; border-color: #444; }}

    /* ── Mobile ── */
    @media (max-width: 768px) {{
      #nav-row {{ padding: 4px 8px; }}
      #filter-row {{ padding: 3px 8px; flex-wrap: wrap; row-gap: 4px; }}

      /* Hamburger visible; nav tabs become a fixed dropdown */
      #hamburger-btn {{ display: inline-flex; }}
      #nav-tabs {{
        display: none;
        position: fixed;
        top: 40px;
        left: 0; right: 0;
        flex-direction: column;
        align-items: stretch;
        gap: 4px;
        padding: 8px 12px;
        background: rgba(245,245,252,0.97);
        border-bottom: 1px solid #ddd;
        z-index: 200;
        backdrop-filter: blur(6px);
      }}
      body.dark #nav-tabs {{
        background: rgba(10,10,28,0.97);
        border-color: #2a2a3e;
      }}
      #nav-tabs.open {{ display: flex; }}
      #nav-tabs .tb-btn {{
        text-align: left; padding: 9px 14px;
        font-size: 13px; border-radius: 8px;
        border: 1px solid transparent; top: 0;
      }}
      #nav-tabs > #sep {{ display: none; }}

      /* Hide desktop-only separators */
      #sep {{ display: none; }}
      #sep2 {{ display: none !important; }}

      .tb-btn {{ padding: 4px 7px; font-size: 11px; }}

      #date-select, .tb-region-select {{
        max-width: 110px;
        font-size: 11px;
      }}

      #salvos-controls, #mismatch-controls, #leadtime-controls, #hour-controls, #date-controls {{
        flex-wrap: wrap;
        row-gap: 4px;
      }}

      #salvos-controls input[type=range] {{ width: 55px; }}

      #modal {{ width: 98vw; max-height: 92vh; }}
      #modal-body {{ padding: 6px; }}
    }}
  </style>
</head>
<body class="light">

  <!-- Top bar -->
  <div id="topbar">

    <!-- Row 1: navigation -->
    <div id="nav-row">
      <button id="hamburger-btn" class="tb-btn" onclick="toggleNavDrawer()">&#9776;&nbsp;<span id="hamburger-label">Situation Room</span></button>
      <div id="nav-tabs">
        <div id="sep"></div>
        <button class="tb-btn active" onclick="setView('situation')" id="btn-situation">&#9889;&nbsp;Situation Room</button>
        <button class="tb-btn"        onclick="setView('hour')"      id="btn-hour">&#9200;&nbsp;By Hour</button>
        <button class="tb-btn"        onclick="setView('date')"     id="btn-date">&#128197;&nbsp;By Date</button>
        <button class="tb-btn"        onclick="setView('mismatch')" id="btn-mismatch">&#9888;&#65039;&nbsp;Mismatches</button>
        <button class="tb-btn"        onclick="setView('leadtime')" id="btn-leadtime">&#9203;&nbsp;Lead Time</button>
        <button class="tb-btn"        onclick="setView('salvos')"   id="btn-salvos">&#128165;&nbsp;Salvos</button>
      </div>
      <div id="nav-spacer"></div>
      <button class="tb-btn" onclick="toggleTheme()" id="theme-btn">&#127769;&nbsp;Dark</button>
    </div>

    <!-- Row 2: view-specific filters -->
    <div id="filter-row">

      <!-- By Hour controls -->
      <div id="hour-controls" style="display:none;align-items:center;gap:6px;">
        <select id="hour-region-select" class="tb-region-select" onchange="onHourRegion(this.value)">
          <option value="">All regions</option>
        </select>
        <div class="tb-sep"></div>
        <span style="font-size:12px;color:#555;white-space:nowrap;" id="lbl-from">From:</span>
        <select id="hour-date-from" class="tb-region-select" onchange="onHourDateFrom(this.value)"></select>
        <span style="font-size:12px;color:#555;">To:</span>
        <select id="hour-date-to" class="tb-region-select" onchange="onHourDateTo(this.value)"></select>
        <div class="tb-sep"></div>
        <div id="type-btns" style="display:flex;gap:6px;">
          <button class="tb-btn" id="type-pre" onclick="setTypeMode('pre')">Pre-alert</button>
          <button class="tb-btn active" id="type-md" onclick="setTypeMode('missile_drone')">Missile &amp; Drone</button>
        </div>
      </div>

      <!-- By Date controls -->
      <div id="date-controls" style="display:none;align-items:center;gap:6px;">
        <span style="font-size:12px;color:#555;white-space:nowrap;">From:</span>
        <select id="date-view-from" class="tb-region-select" onchange="onDateViewFrom(this.value)"></select>
        <span style="font-size:12px;color:#555;">To:</span>
        <select id="date-view-to" class="tb-region-select" onchange="onDateViewTo(this.value)"></select>
      </div>

      <!-- Mismatch controls -->
      <div id="mismatch-controls" style="display:none;align-items:center;gap:6px;">
        <select id="mismatch-region-select" class="tb-region-select" onchange="onMismatchRegion(this.value)">
          <option value="">All regions</option>
        </select>
        <button class="tb-btn" onclick="toggleMismatchMode()" id="mismatch-mode-btn">%&nbsp;View</button>
      </div>

      <!-- Lead Time controls -->
      <div id="leadtime-controls" style="display:none;align-items:center;gap:6px;">
        <select id="leadtime-region-select" class="tb-region-select" onchange="onLeadtimeRegion(this.value)">
          <option value="">All regions</option>
        </select>
      </div>

      <!-- Salvos controls -->
      <div id="salvos-controls" style="display:none;align-items:center;gap:6px;">
        <select id="salvos-region-select" class="tb-region-select" onchange="onSalvosRegion(this.value)">
          <option value="">All regions</option>
        </select>
        <div class="tb-sep"></div>
        <span style="font-size:12px;color:#555;white-space:nowrap;">From:</span>
        <select id="salvos-date-from" class="tb-region-select" onchange="onSalvosDateFrom(this.value)"></select>
        <span style="font-size:12px;color:#555;">To:</span>
        <select id="salvos-date-to" class="tb-region-select" onchange="onSalvosDateTo(this.value)"></select>
      </div>

    </div><!-- /filter-row -->
  </div><!-- /topbar -->

  <!-- Situation Room view -->
  <div id="view-situation">
    <div id="situation-content" style="padding:20px 24px;box-sizing:border-box;"></div>
  </div>

  <!-- By-hour view -->
  <div id="view-hour">
    <div id="main-chart"></div>
  </div>

  <!-- By-date cumulative view -->
  <div id="view-date">
    <div id="date-full-chart"></div>
  </div>

  <!-- Mismatch view -->
  <div id="view-mismatch">
    <div id="mismatch-chart-bar"></div>
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

  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js" charset="utf-8"></script>
  <script>
    // ── Data ────────────────────────────────────────────────────────────────
    var hourlyData   = {hourly_js};
    var groupColors  = {group_colors_js};
    var allGroups    = {groups_js};
    var allTypes     = {alert_types_js};

    var hourData            = {hour_data_js};
    var hourLayout          = {hour_layout_js};
    var dateData            = {date_data_js};
    var dateLayout          = {date_layout_js};
    var allMismatchRecords  = {mismatch_records_js};
    var gapMissileSeconds   = {gap_missile_js};
    var gapDroneSeconds     = {gap_drone_js};
    var allSalvoRecords     = {salvo_records_js};
    var situationData       = {situation_data_js};
    var partialDay          = {partial_day_js};
    var partialHour         = {partial_hour_js};
    var fetchedAt           = {fetched_at_js};

    var darkMain    = {dark_main};
    var lightMain   = {light_main};
    var darkMini    = {dark_mini};
    var lightMini   = {light_mini};

    // ── Mobile helpers ───────────────────────────────────────────────────────
    function isMobile() {{ return window.innerWidth < 768; }}

    function syncTopbarHeight() {{
      var h = document.getElementById('topbar').offsetHeight;
      document.documentElement.style.setProperty('--tb-h', h + 'px');
    }}
    window.addEventListener('resize', syncTopbarHeight);

    function chartMargins(desktopMargin, mobileMargin) {{
      return isMobile() ? mobileMargin : desktopMargin;
    }}

    var VIEW_LABELS = {{
      situation: 'Situation Room',
      hour: 'By Hour', date: 'By Date', mismatch: 'Mismatches',
      leadtime: 'Lead Time', salvos: 'Salvos'
    }};

    function toggleNavDrawer() {{
      document.getElementById('nav-tabs').classList.toggle('open');
      syncTopbarHeight();
    }}

    // Close drawer when clicking outside
    document.addEventListener('click', function(e) {{
      var drawer = document.getElementById('nav-tabs');
      var btn    = document.getElementById('hamburger-btn');
      if (drawer.classList.contains('open') &&
          !drawer.contains(e.target) && !btn.contains(e.target)) {{
        drawer.classList.remove('open');
        syncTopbarHeight();
      }}
    }});

    // ── State ───────────────────────────────────────────────────────────────
    var isDark            = false;
    var currentView       = 'situation';
    var currentDateRange  = null;          // null = all dates
    var activeTypeMode    = 'missile_drone'; // 'pre' | 'missile_drone'
    var hourRegion        = '';
    var salvosRegion      = '';
    var salvosDateFrom    = '';
    var salvosDateTo      = '';

    // ── Mobile layout patches (mutate before first plot) ─────────────────────
    if (isMobile()) {{
      hourLayout.xaxis = Object.assign({{}}, hourLayout.xaxis, {{
        tickvals: [0, 4, 8, 12, 16, 20, 23],
        ticktext: ['0h', '4h', '8h', '12h', '16h', '20h', '23h'],
      }});
      hourLayout.margin = {{t: 45, b: 110, l: 45, r: 10}};
      hourLayout.legend = Object.assign({{}}, hourLayout.legend, {{
        orientation: 'h', x: 0.5, xanchor: 'center',
        y: -0.1, yanchor: 'top',
        font: {{size: 9, color: '#cccccc'}},
      }});
      hourLayout.title = Object.assign({{}}, hourLayout.title, {{
        font: {{size: 12, color: '#cccccc'}},
      }});

      // Date chart: shorten title (avoid rangeselector overlap), move annotation inside
      dateLayout.margin = {{t: 55, b: 110, l: 45, r: 10}};
      dateLayout.title  = Object.assign({{}}, dateLayout.title, {{
        text: 'Cumulative Alerts by Region',
        font: {{size: 13, color: '#cccccc'}},
      }});
      // Move partial-day annotation inside plot area (top-right)
      if (dateLayout.annotations && dateLayout.annotations.length) {{
        dateLayout.annotations = dateLayout.annotations.map(function(a) {{
          return Object.assign({{}}, a, {{
            xref: 'paper', x: 0.99, xanchor: 'right',
            y: 0.98, yanchor: 'top', yref: 'paper',
            font: {{size: 9, color: '#ffaa44'}},
          }});
        }});
      }}
      // Tight x-axis range (no Plotly 5% padding at right edge)
      var _allDates = [];
      dateData.forEach(function(t) {{ if (t.x) _allDates = _allDates.concat(t.x); }});
      _allDates.sort();
      if (_allDates.length > 1) {{
        dateLayout.xaxis = Object.assign({{}}, dateLayout.xaxis, {{
          range: [_allDates[0], _allDates[_allDates.length - 1]],
        }});
      }}
      dateLayout.legend = Object.assign({{}}, dateLayout.legend, {{
        orientation: 'h', x: 0.5, xanchor: 'center',
        y: -0.1, yanchor: 'top',
        font: {{size: 9, color: '#cccccc'}},
      }});
    }}

    // ── Init ─────────────────────────────────────────────────────────────────
    // Start on Situation Room; hide hour view by default
    document.getElementById('view-hour').style.display = 'none';
    document.getElementById('view-situation').style.display = 'block';
    // Defer initial chart renders so Plotly has correct dimensions
    setTimeout(function() {{
      buildSituationView();
      Plotly.newPlot('main-chart', hourData, hourLayout, {{responsive:true}});
      Plotly.relayout('main-chart', lightMain);
      updateHourChart();  // apply locked Y scale
      document.getElementById('main-chart').on('plotly_click', function(d) {{
        openSmallMultiples(d.points[0].data.name);
      }});
    }}, 0);
    syncTopbarHeight();

    // ── Populate By Hour date selectors ──────────────────────────────────────
    (function() {{
      var allDates = [...new Set(hourlyData.map(function(r) {{ return r.date_str; }}))].sort();
      var fromSel = document.getElementById('hour-date-from');
      var toSel   = document.getElementById('hour-date-to');
      var allOpt  = document.createElement('option');
      allOpt.value = ''; allOpt.textContent = 'All';
      fromSel.appendChild(allOpt.cloneNode(true));
      toSel.appendChild(allOpt);
      allDates.forEach(function(d) {{
        var of = document.createElement('option'); of.value = d; of.textContent = d; fromSel.appendChild(of);
        var ot = document.createElement('option'); ot.value = d; ot.textContent = d; toSel.appendChild(ot);
      }});
    }})();

    // ── Populate By Hour region selector ─────────────────────────────────────
    (function() {{
      var sel = document.getElementById('hour-region-select');
      var regions = [...new Set(hourlyData.map(function(r) {{ return r.group; }}))].sort();
      regions.forEach(function(g) {{
        var o = document.createElement('option'); o.value = g; o.textContent = g; sel.appendChild(o);
      }});
    }})();

    // ── Populate By Date date selectors ──────────────────────────────────────
    (function() {{
      var allDates = [...new Set(hourlyData.map(function(r) {{ return r.date_str; }}))].sort();
      var fromSel = document.getElementById('date-view-from');
      var toSel   = document.getElementById('date-view-to');
      var allOpt  = document.createElement('option');
      allOpt.value = ''; allOpt.textContent = 'All';
      fromSel.appendChild(allOpt.cloneNode(true));
      toSel.appendChild(allOpt);
      allDates.forEach(function(d) {{
        var of = document.createElement('option'); of.value = d; of.textContent = d; fromSel.appendChild(of);
        var ot = document.createElement('option'); ot.value = d; ot.textContent = d; toSel.appendChild(ot);
      }});
    }})();

    function onHourDateFrom(val) {{
      var toVal = document.getElementById('hour-date-to').value;
      currentDateRange = (val || toVal) ? [val || '', toVal || '9999'] : null;
      updateHourChart();
    }}
    function onHourDateTo(val) {{
      var fromVal = document.getElementById('hour-date-from').value;
      currentDateRange = (fromVal || val) ? [fromVal || '', val || '9999'] : null;
      updateHourChart();
    }}
    function onHourRegion(val) {{ hourRegion = val; updateHourChart(); }}

    function onDateViewFrom(val) {{
      var toVal = document.getElementById('date-view-to').value;
      if (val || toVal) {{
        Plotly.relayout('date-full-chart', {{'xaxis.range': [val || '1970-01-01', toVal || '2099-12-31'], 'xaxis.autorange': false}});
      }} else {{
        Plotly.relayout('date-full-chart', {{'xaxis.autorange': true}});
      }}
    }}
    function onDateViewTo(val) {{
      var fromVal = document.getElementById('date-view-from').value;
      if (fromVal || val) {{
        Plotly.relayout('date-full-chart', {{'xaxis.range': [fromVal || '1970-01-01', val || '2099-12-31'], 'xaxis.autorange': false}});
      }} else {{
        Plotly.relayout('date-full-chart', {{'xaxis.autorange': true}});
      }}
    }}

    function setTypeMode(mode) {{
      activeTypeMode = mode;
      document.getElementById('type-pre').classList.toggle('active', mode === 'pre');
      document.getElementById('type-md').classList.toggle('active',  mode === 'missile_drone');
      updateHourChart();
    }}

    // ── View toggle ─────────────────────────────────────────────────────────
    function setView(v) {{
      currentView = v;
      document.getElementById('view-situation').style.display  = v === 'situation' ? 'block' : 'none';
      document.getElementById('view-hour').style.display       = v === 'hour'      ? 'block' : 'none';
      document.getElementById('view-date').style.display       = v === 'date'      ? 'block' : 'none';
      document.getElementById('view-mismatch').style.display   = v === 'mismatch'  ? 'block' : 'none';
      document.getElementById('view-leadtime').style.display   = v === 'leadtime'  ? 'block' : 'none';
      document.getElementById('view-salvos').style.display     = v === 'salvos'    ? 'block' : 'none';
      document.getElementById('btn-situation').classList.toggle('active', v === 'situation');
      document.getElementById('btn-hour').classList.toggle('active',      v === 'hour');
      document.getElementById('btn-date').classList.toggle('active',      v === 'date');
      document.getElementById('btn-mismatch').classList.toggle('active',  v === 'mismatch');
      document.getElementById('btn-leadtime').classList.toggle('active',  v === 'leadtime');
      document.getElementById('btn-salvos').classList.toggle('active',    v === 'salvos');
      document.getElementById('hour-controls').style.display     = v === 'hour'     ? 'flex'  : 'none';
      document.getElementById('date-controls').style.display     = v === 'date'     ? 'flex'  : 'none';
      document.getElementById('mismatch-controls').style.display = v === 'mismatch' ? 'flex'  : 'none';
      document.getElementById('leadtime-controls').style.display = v === 'leadtime' ? 'flex'  : 'none';
      document.getElementById('salvos-controls').style.display   = v === 'salvos'   ? 'flex'  : 'none';
      // Close hamburger drawer and update its label
      document.getElementById('nav-tabs').classList.remove('open');
      var hamburgerLabel = document.getElementById('hamburger-label');
      if (hamburgerLabel) {{ hamburgerLabel.textContent = VIEW_LABELS[v] || v; }}
      syncTopbarHeight();
      // Force Plotly to redraw at the now-correct dimensions
      setTimeout(function() {{
        if (v === 'situation') {{
          buildSituationView();
        }} else if (v === 'date') {{
          Plotly.react('date-full-chart', dateData, dateLayout, {{responsive:true}});
          Plotly.relayout('date-full-chart', isDark ? darkMain : lightMain);
        }} else if (v === 'mismatch') {{
          buildMismatchCharts(mismatchRegion);
        }} else if (v === 'leadtime') {{
          buildLeadTimeChart(leadtimeRegion);
        }} else if (v === 'salvos') {{
          buildSalvosChart();
        }} else {{
          Plotly.Plots.resize('main-chart');
        }}
      }}, 30);
    }}

    // ── Recompute hour-chart traces from filtered data ────────────────────
    function updateHourChart() {{
      var filtered = hourlyData.filter(function(r) {{
        var dateOk = !currentDateRange ||
          (r.date_str >= currentDateRange[0] && r.date_str <= currentDateRange[1]);
        var typeOk = activeTypeMode === 'pre'
          ? r.alert_type === 'Pre-alert'
          : r.alert_type !== 'Pre-alert';
        var regionOk = !hourRegion || r.group === hourRegion;
        return dateOk && typeOk && regionOk;
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

      // Lock Y-axis scale so legend clicks don't rescale
      var maxY = 0;
      for (var h = 0; h < 24; h++) {{
        var s = 0; newTraces.forEach(function(t) {{ s += t.y[h] || 0; }}); maxY = Math.max(maxY, s);
      }}
      var lockedLayout = Object.assign({{}}, hourLayout, {{
        yaxis: Object.assign({{}}, hourLayout.yaxis, {{autorange: false, range: [0, maxY * 1.1 || 10]}})
      }});
      Plotly.react('main-chart', newTraces, lockedLayout);
    }}

    // ── Small-multiples modal ───────────────────────────────────────────────
    function openSmallMultiples(group) {{
      var rows = hourlyData.filter(function(r) {{
        var dateOk = !currentDateRange ||
          (r.date_str >= currentDateRange[0] && r.date_str <= currentDateRange[1]);
        var typeOk = activeTypeMode === 'pre'
          ? r.alert_type === 'Pre-alert'
          : r.alert_type !== 'Pre-alert';
        return r.group === group && dateOk && typeOk;
      }});
      if (!rows.length) return;

      var color = groupColors[group] || '#888';
      var days  = [...new Set(rows.map(function(r) {{ return r.date_str; }}))].sort();
      var COLS  = window.innerWidth < 500 ? 2 : (window.innerWidth < 900 ? 4 : 7);
      var ROWS  = Math.ceil(days.length / COLS);
      var cellH = window.innerWidth < 500 ? 100 : 140;
      var lineColor = isDark ? '#444' : '#ccc';

      var traces = [], layout = {{
        grid: {{ rows: ROWS, columns: COLS, pattern: 'independent', ygap: 0.28, xgap: 0.14 }},
        showlegend: false,
        margin: {{ t: 30, b: 10, l: 30, r: 10 }},
        height: ROWS * cellH + 40,
        paper_bgcolor: isDark ? '#12122a' : '#fff',
        plot_bgcolor:  isDark ? '#1a1a2e' : '#f8f8fc',
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

        var ax = {{ showgrid: false, zeroline: false,
                    showline: true, linecolor: lineColor, linewidth: 1, mirror: true,
                    color: isDark?'#888':'#555', tickfont:{{size:8}} }};
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

    function onSalvosRegion(val)   {{ salvosRegion  = val; buildSalvosChart(); }}
    function onSalvosDateFrom(val) {{ salvosDateFrom = val; buildSalvosChart(); }}
    function onSalvosDateTo(val)   {{ salvosDateTo   = val; buildSalvosChart(); }}

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

      var viewEl = document.getElementById('view-mismatch');
      var viewH  = viewEl.offsetHeight || (window.innerHeight - 74);
      var viewW  = viewEl.offsetWidth  || window.innerWidth;

      if (!dates.length) {{
        var empty = {{plot_bgcolor:theme.bg, paper_bgcolor:theme.paper,
                     font:{{color:theme.text}}, height:viewH, width:viewW,
                     title:{{text:'No data for selected region', x:0.5,
                             font:{{color:theme.text}}}}}};
        Plotly.react('mismatch-chart-bar', [], empty, {{responsive:true}});
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
      if (mismatchIsPct) {{
        barTraces.push({{
          type:'scatter', x:dates, y:rollRate, name:'7d mismatch %', yaxis:'y2',
          line:{{color:theme.roll, width:2, dash:'dot'}}, mode:'lines',
          hovertemplate:'7d mismatch rate: <b>%{{y:.1f}}%</b><extra></extra>',
        }});
      }}

      var regionLabel = region ? ' — ' + region : ' — All Regions';
      var barLayout = {{
        barmode:'stack',
        height: viewH,
        width:  viewW,
        title:{{text:'Mismatch by Day'+regionLabel+'<br><sup>15-min pairing · 7-day mismatch % (dotted) on right axis</sup>',
                x:0.5, font:{{size:isMobile()?11:14,color:theme.text}}}},
        yaxis:{{title:mismatchIsPct?'% of Events':'Event Count',
                showgrid:true, gridcolor:theme.grid, zeroline:false, color:theme.text}},
        yaxis2:{{title:'Mismatch %', overlaying:'y', side:'right',
                 showgrid:false, zeroline:false, color:theme.text, range:[0,100]}},
        plot_bgcolor:theme.bg, paper_bgcolor:theme.paper,
        font:{{family:'Arial, Helvetica, sans-serif', color:theme.text}},
        legend: isMobile()
          ? {{orientation:'h', x:0.5, xanchor:'center', y:-0.1, yanchor:'top',
              font:{{size:9,color:theme.text}}, bgcolor:theme.legendBg,
              bordercolor:theme.legendBorder, borderwidth:1}}
          : {{font:{{size:11,color:theme.text}},
              bgcolor:theme.legendBg, bordercolor:theme.legendBorder, borderwidth:1}},
        margin: isMobile()
          ? {{t:55, b:110, l:45, r:45}}
          : {{t:70, b:60,  l:70, r:70}},
        xaxis: {{
          showgrid: true, gridcolor: theme.grid, color: theme.text, zeroline: false,
          tickvals: dates,
          ticktext: dates.map(function(d) {{ return d.slice(5); }}),
          tickangle: -45, tickfont: {{size: 8}},
        }},
        annotations: partialDay ? [isMobile()
          ? {{xref:'paper', yref:'paper', x:0.99, xanchor:'right', y:0.98, yanchor:'top',
              text:'\u26a0 partial: '+partialDay,
              showarrow:false, font:{{size:9, color:'#ffaa44'}}}}
          : {{x:partialDay, y:1.06, xref:'x', yref:'paper',
              text:'\u26a0 '+partialDay+': partial day (data to '+partialHour+':xx)',
              showarrow:false, font:{{size:10, color:'#ffaa44'}}, xanchor:'center'}}
        ] : [],
      }};

      Plotly.react('mismatch-chart-bar', barTraces, barLayout, {{responsive:true}});
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

      // Pre-aggregate into explicit bins so hovertemplate can show exact edges.
      // Plotly histogram %{{x}} gives the bin centre, not the left edge, making
      // "start–end" range tooltips unreliable; a bar trace avoids that ambiguity.
      var BIN_START = 0, BIN_END = 15, BIN_SIZE = 0.5;
      function makeBins(vals) {{
        var nBins = Math.round((BIN_END - BIN_START) / BIN_SIZE);
        var counts = new Array(nBins).fill(0);
        vals.forEach(function(v) {{
          var idx = Math.floor((v - BIN_START) / BIN_SIZE);
          if (idx >= 0 && idx < nBins) counts[idx]++;
        }});
        var xs = [], ends = [], ys = [];
        for (var i = 0; i < nBins; i++) {{
          xs.push(parseFloat((BIN_START + i * BIN_SIZE).toFixed(1)));
          ends.push(parseFloat((BIN_START + (i + 1) * BIN_SIZE).toFixed(1)));
          ys.push(counts[i]);
        }}
        return {{x: xs, customdata: ends, y: ys}};
      }}

      var traces = [];
      if (missileMin.length) {{
        var mb = makeBins(missileMin);
        traces.push({{
          type: 'bar',
          x: mb.x,
          y: mb.y,
          customdata: mb.customdata,
          name: 'Paired (missile)',
          width: BIN_SIZE,
          offset: 0,
          marker: {{color: '#2ca02c', opacity: 0.75}},
          hovertemplate: '<b>Paired (missile)</b><br>%{{x:.1f}}–%{{customdata:.1f}} min: <b>%{{y:,}}</b><extra></extra>',
        }});
      }}
      if (droneMin.length) {{
        var db = makeBins(droneMin);
        traces.push({{
          type: 'bar',
          x: db.x,
          y: db.y,
          customdata: db.customdata,
          name: 'Paired (drone)',
          width: BIN_SIZE,
          offset: 0,
          marker: {{color: '#17becf', opacity: 0.75}},
          hovertemplate: '<b>Paired (drone)</b><br>%{{x:.1f}}–%{{customdata:.1f}} min: <b>%{{y:,}}</b><extra></extra>',
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
          x: 0.5, font: {{size: isMobile() ? 12 : 15, color: theme.text}},
        }},
        xaxis: Object.assign({{
          title: 'Minutes from Pre-alert to Paired Alert',
          dtick: 1,
          showgrid: true, gridcolor: theme.grid,
          zeroline: false, color: theme.text,
        }}, isMobile() ? {{}} : {{range: [0, 15]}}),
        yaxis: {{
          title: 'Number of Pre-alerts',
          showgrid: true, gridcolor: theme.grid,
          zeroline: false, color: theme.text,
        }},
        plot_bgcolor:  theme.bg,
        paper_bgcolor: theme.paper,
        font: {{family: 'Arial, Helvetica, sans-serif', color: theme.text}},
        legend: isMobile()
          ? {{orientation:'h', x:0.5, xanchor:'center', y:-0.1, yanchor:'top',
              font:{{size:9, color:theme.text}},
              bgcolor:theme.legendBg, bordercolor:theme.legendBorder, borderwidth:1}}
          : {{font:{{size:11, color:theme.text}},
              bgcolor:theme.legendBg, bordercolor:theme.legendBorder, borderwidth:1}},
        margin: chartMargins({{t:80,b:60,l:70,r:40}}, {{t:40,b:80,l:45,r:15}}),
      }};

      Plotly.react('leadtime-chart', traces, layout, {{responsive: true}});
    }}

    buildLeadTimeChart('');

    // ── Salvo activity by hour of day (one line per day) ────────────────────
    function buildSalvosChart() {{
      var textColor = isDark ? '#cccccc' : '#333333';
      var gridColor = isDark ? '#2a2a3e' : '#e0e0e0';
      var paperBg   = isDark ? '#0f0f1a' : '#fafafa';
      var plotBg    = isDark ? '#1a1a2e' : 'white';

      var filtered = allSalvoRecords.filter(function(r) {{
        return (!salvosRegion   || r.group    === salvosRegion)
            && (!salvosDateFrom || r.date_str >= salvosDateFrom)
            && (!salvosDateTo   || r.date_str <= salvosDateTo);
      }});

      if (!filtered.length) {{
        Plotly.react('salvos-chart', [], {{
          paper_bgcolor: paperBg, plot_bgcolor: plotBg, font: {{color: textColor}},
          title: {{text: 'No salvo data for selected filters', x: 0.5, font: {{color: textColor}}}},
        }}, {{responsive: true}});
        return;
      }}

      // Group by (date_str, hour), summing cluster_size → missiles per hour per day
      var byDateHour = {{}};
      var allDatesSet = {{}};
      filtered.forEach(function(r) {{
        var hour = 0;
        if (r.cluster_start) {{
          var ti = r.cluster_start.indexOf('T');
          if (ti !== -1) hour = parseInt(r.cluster_start.substring(ti + 1, ti + 3), 10);
        }}
        var key = r.date_str + '|' + hour;
        byDateHour[key] = (byDateHour[key] || 0) + r.cluster_size;
        allDatesSet[r.date_str] = true;
      }});

      var allDates = Object.keys(allDatesSet).sort();
      var palette  = ['#4455cc','#d62728','#2ca02c','#ff7f0e','#17becf',
                      '#9467bd','#e377c2','#f5c542','#98df8a','#56aeff','#ffbb78'];
      var hours24  = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23];

      var traces = allDates.map(function(date, idx) {{
        var ys    = hours24.map(function(h) {{ return byDateHour[date + '|' + h] || 0; }});
        var color = palette[idx % palette.length];
        return {{
          type: 'scatter', mode: 'lines+markers',
          name: date.slice(5),
          x: hours24, y: ys,
          line:   {{color: color, width: 1.8, shape: 'spline', smoothing: 1.2}},
          marker: {{size: 5, color: color}},
          hovertemplate: '<b>' + date + '</b><br>%{{x:02d}}:00 \u2014 <b>%{{y}}</b> missiles<extra></extra>',
        }};
      }});

      var regionNote = salvosRegion ? ' \u00b7 ' + salvosRegion : '';
      var viewEl = document.getElementById('view-salvos');
      var viewH  = viewEl.offsetHeight || (window.innerHeight - 74);
      var viewW  = viewEl.offsetWidth  || window.innerWidth;

      var layout = {{
        height: viewH, width: viewW,
        title: {{
          text: 'Missile Salvo Activity by Hour of Day' + regionNote +
                '<br><sup>Missiles per hour \u00b7 one line per day \u00b7 salvo\u202f=\u202f2+ missiles gap\u202f\u226430\u202fmin</sup>',
          x: 0.5, font: {{size: isMobile() ? 11 : 14, color: textColor}},
        }},
        xaxis: {{
          title: 'Hour of Day',
          tickmode: 'array',
          tickvals: [0,2,4,6,8,10,12,14,16,18,20,22],
          ticktext: ['0h','2h','4h','6h','8h','10h','12h','14h','16h','18h','20h','22h'],
          range: [-0.5, 23.5],
          showgrid: true, gridcolor: gridColor,
          zeroline: false, color: textColor,
        }},
        yaxis: {{
          title: 'Missiles in Salvos',
          showgrid: true, gridcolor: gridColor,
          zeroline: true, zerolinecolor: gridColor,
          color: textColor,
        }},
        plot_bgcolor:  plotBg,
        paper_bgcolor: paperBg,
        font: {{family: 'Arial, Helvetica, sans-serif', color: textColor}},
        legend: isMobile()
          ? {{orientation: 'h', x: 0.5, xanchor: 'center', y: -0.15, font: {{size: 8, color: textColor}}}}
          : {{font: {{size: 10, color: textColor}},
             bgcolor: isDark ? 'rgba(26,26,46,0.85)' : 'rgba(255,255,255,0.85)',
             bordercolor: isDark ? '#444' : '#ccc', borderwidth: 1}},
        margin: chartMargins({{t:80,b:60,l:70,r:40}}, {{t:55,b:100,l:45,r:15}}),
      }};

      Plotly.react('salvos-chart', traces, layout, {{responsive: true}});
    }}

    // ── Situation Room ───────────────────────────────────────────────────────
    function makeSVGSparkline(hourlyArr, color) {{
      var W = 120, H = 32, barW = Math.floor(W / 24);
      var maxV = Math.max.apply(null, hourlyArr.concat([1]));
      var bars = '';
      for (var h = 0; h < 24; h++) {{
        var bh   = Math.round(((hourlyArr[h] || 0) / maxV) * H);
        var isNight = (h >= 22 || h < 6);
        var fill = isNight ? color : color + '88';
        bars += '<rect x="' + (h * barW) + '" y="' + (H - bh) + '" width="' + (barW - 1) + '" height="' + bh + '" fill="' + fill + '"/>';
      }}
      return '<svg width="' + W + '" height="' + H + '" xmlns="http://www.w3.org/2000/svg" style="display:block;">' + bars + '</svg>';
    }}

    function buildSituationView() {{
      var el = document.getElementById('situation-content');
      if (!el) return;
      var titles = {{ last_night: 'What happened last night?', today: 'What\u2019s happening today?' }};
      // Format fetchedAt timestamp in Israel time
      var ILtz = 'Asia/Jerusalem';
      var fetchedStr = '';
      if (fetchedAt) {{
        try {{
          var fd = new Date(fetchedAt);
          fetchedStr = fd.toLocaleDateString('en-GB', {{day:'numeric',month:'short',year:'numeric',timeZone:ILtz}}) +
                       ' at ' + fd.toLocaleTimeString('en-GB', {{hour:'2-digit',minute:'2-digit',timeZone:ILtz}});
        }} catch(e) {{ fetchedStr = fetchedAt; }}
      }}
      // Compute next 06:00 UTC update
      var nextUpdateStr = '';
      try {{
        var now = new Date();
        var next = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), 6, 0, 0));
        if (next <= now) next.setUTCDate(next.getUTCDate() + 1);
        var diffH = (next - now) / 3600000;
        var hoursLeft = Math.floor(diffH);
        var minsLeft  = Math.round((diffH - hoursLeft) * 60);
        var nextIL = next.toLocaleTimeString('en-GB', {{hour:'2-digit',minute:'2-digit',timeZone:ILtz}});
        nextUpdateStr = 'Next update in ~' + hoursLeft + 'h ' + minsLeft + 'm (' + nextIL + ' Israel time)';
      }} catch(e) {{}}
      var html = fetchedStr
        ? '<div style="font-size:11px;color:#999;margin-bottom:18px;">Data last fetched: ' + fetchedStr + ' (Israel time)</div>'
        : '';

      ['last_night', 'today'].forEach(function(key) {{
        var d = situationData[key];
        if (!d) return;
        html += '<div class="sit-section">';
        html += '<div class="sit-section-title">' + titles[key] + '</div>';
        html += '<div class="sit-sublabel">' + (d.label || '') + '</div>';

        if (!d.total_missile && !d.total_pre && !d.total_drone) {{
          html += '<div class="sit-summary sit-quiet">Quiet \u2014 no alerts recorded for this period.</div>';
        }} else {{
          var parts = [];
          if (d.total_missile) parts.push(d.total_missile + ' missile alert' + (d.total_missile !== 1 ? 's' : ''));
          if (d.total_pre)     parts.push(d.total_pre     + ' pre-alert'     + (d.total_pre     !== 1 ? 's' : ''));
          if (d.total_drone)   parts.push(d.total_drone   + ' drone alert'   + (d.total_drone   !== 1 ? 's' : ''));
          var nr = d.regions.length;
          var rStr = nr <= 3 ? d.regions.join(', ') : d.regions.slice(0, 3).join(', ') + ' and ' + (nr - 3) + ' more';
          html += '<div class="sit-summary">' + parts.join(' and ') + ' across ' + nr + ' region' + (nr !== 1 ? 's' : '') + ' (' + rStr + ').</div>';

          var regionKeys = Object.keys(d.per_region_hourly || {{}}).sort();
          if (regionKeys.length) {{
            html += '<div class="sit-sparklines">';
            regionKeys.forEach(function(region) {{
              var color = groupColors[region] || '#888888';
              html += '<div class="sit-sparkline-cell">';
              html += makeSVGSparkline(d.per_region_hourly[region], color);
              html += '<div class="sit-sparkline-label" title="' + region + '">' + region + '</div>';
              html += '</div>';
            }});
            html += '</div>';
          }}
        }}
        html += '</div>';
      }});

      if (nextUpdateStr) {{
        html += '<div style="font-size:11px;color:#999;margin-top:8px;padding-top:12px;border-top:1px solid ' +
                (isDark ? '#2a2a3e' : '#e8e8e8') + ';">' + nextUpdateStr + '</div>';
      }}
      el.innerHTML = html;
    }}

    // ── Dark / light toggle ─────────────────────────────────────────────────
    function toggleTheme() {{
      isDark = !isDark;
      document.body.classList.toggle('dark',  isDark);
      document.body.classList.toggle('light', !isDark);
      document.getElementById('theme-btn').innerHTML =
        isDark ? '&#9728;&#65039;&nbsp;Light' : '&#127769;&nbsp;Dark';
      Plotly.relayout('main-chart', isDark ? darkMain : lightMain);
      var dateEl = document.getElementById('date-full-chart');
      if (dateEl && dateEl._fullLayout) {{
        Plotly.relayout('date-full-chart', isDark ? darkMain : lightMain);
      }}
      if (currentView === 'situation') buildSituationView();
      buildMismatchCharts(mismatchRegion);
      buildLeadTimeChart(leadtimeRegion);
      buildSalvosChart();
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

    # 6. Save processed data (enables fast style-only reruns via build_chart.py)
    from datetime import timezone as _tz
    fetched_at = datetime.now(_tz.utc).isoformat()
    save_processed(chart_df, mismatch_df, salvo_df, partial_day, partial_hour,
                   fetched_at=fetched_at)

    # 7. Situation Room summary (time-sensitive, computed fresh each run)
    situation_data = compute_situation(chart_df)

    # 8. Chart
    build_chart(chart_df, mismatch_df, salvo_df=salvo_df,
                partial_day=partial_day, partial_hour=partial_hour,
                situation_data=situation_data, fetched_at=fetched_at)
    print("\nDone.  Open output/index.html in your browser.")


if __name__ == "__main__":
    main()
