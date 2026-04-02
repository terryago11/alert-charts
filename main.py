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

GITHUB_CSV_URL      = "https://raw.githubusercontent.com/dleshem/israel-alerts-data/main/israel-alerts.csv"
GITHUB_CONTENTS_API = "https://api.github.com/repos/dleshem/israel-alerts-data/contents/israel-alerts.csv"
_cutoff_env = __import__("os").environ.get("ALERT_CUTOFF_DATE")
CUTOFF_DATE = pd.Timestamp(_cutoff_env) if _cutoff_env else pd.Timestamp("2026-02-28")
PROCESSED_SCHEMA_VERSION = 4
EVENT_CLUSTER_WINDOW = 90  # seconds — alerts within this window per zone = 1 event
SITE_URL = "https://terryago11.github.io/alert-charts"

ALERT_TRANSLATIONS = {
    "בדקות הקרובות צפויות להתקבל התרעות באזורך": "Pre-alert",
    "חדירת כלי טיס עוין":                         "Drone alert",
    "ירי רקטות וטילים":                            "Missile alert",
}

DATA_DIR    = Path("data")
OUTPUT_DIR  = Path("output")
PAIR_WINDOW       = timedelta(minutes=15)   # max gap between pre-alert and missile alert
SALVO_WINDOW      = timedelta(minutes=30)   # max consecutive gap within a salvo cluster
MISMATCH_LOOKBACK = timedelta(minutes=30)   # re-examine window to catch cross-boundary pairs


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

def fetch_csv_sha() -> Optional[str]:
    """Return the current git blob SHA of the source CSV via GitHub Contents API."""
    import os
    headers = {"Accept": "application/vnd.github.v3+json"}
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        headers["Authorization"] = f"token {token}"
    try:
        resp = requests.get(GITHUB_CONTENTS_API, timeout=10, headers=headers)
        resp.raise_for_status()
        return resp.json().get("sha")
    except Exception as exc:
        print(f"  Could not fetch CSV SHA: {exc}")
        return None


def load_processed() -> Optional[dict]:
    """Load data/processed.json if it exists and matches PROCESSED_SCHEMA_VERSION."""
    path = DATA_DIR / "processed.json"
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if payload.get("schema_version") != PROCESSED_SCHEMA_VERSION:
            print("  processed.json schema version mismatch — full rebuild.")
            return None
        return payload
    except Exception as exc:
        print(f"  Could not load processed.json: {exc}")
        return None


def fetch_github_csv(since_dt: Optional[pd.Timestamp] = None) -> Optional[pd.DataFrame]:
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
    raw = raw[raw["_dt"] >= CUTOFF_DATE]

    # Incremental filter: only rows newer than last full-build fetched_at
    if since_dt is not None:
        before = len(raw)
        # _dt is tz-naive (CSV has no timezone); since_dt may be tz-aware (UTC ISO string)
        _since_naive = since_dt.tz_convert(None) if since_dt.tzinfo is not None else since_dt
        raw = raw[raw["_dt"] > _since_naive]
        print(f"  Incremental filter (>{since_dt.isoformat()}): {len(raw):,} new rows (of {before:,} total)")

    raw = raw.drop(columns=["_dt"])

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

    _valid     = dts.notna()
    dt_floor   = dts.dt.floor("min").where(_valid, other=None).tolist()
    hours      = dts.dt.hour.where(_valid, other=pd.NA).astype(object).where(_valid, other=None).tolist()
    date_strs  = dts.dt.strftime("%Y-%m-%d").where(_valid, other=None).tolist()

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

def cluster_events(times: list) -> list:
    """Temporal clustering: group sorted timestamps into clusters where each new
    event starts a new cluster only if it is >EVENT_CLUSTER_WINDOW seconds after
    the current cluster's start.  Returns the representative (first) timestamp
    of each cluster."""
    if not times:
        return []
    sorted_ts = sorted(times)
    result = [sorted_ts[0]]
    cluster_start = sorted_ts[0]
    for t in sorted_ts[1:]:
        if (t - cluster_start).total_seconds() > EVENT_CLUSTER_WINDOW:
            result.append(t)
            cluster_start = t
    return result


def compute_global_incidents(zone_clustered: dict) -> "pd.DataFrame":
    """Cross-zone temporal clustering (same 90 s window).

    Takes the per-zone clustered events and collapses simultaneous alerts across
    all zones into single global incidents, per alert_type.  Two zone-level events
    of the same type within EVENT_CLUSTER_WINDOW seconds count as one incident.

    Returns DataFrame [date_str, hour, alert_type, count].
    """
    by_type: dict = defaultdict(list)
    for (_, alert_type), times in zone_clustered.items():
        by_type[alert_type].extend(times)

    rows: list = []
    for alert_type, times in by_type.items():
        for dt in cluster_events(times):
            rows.append({
                "date_str":   dt.strftime("%Y-%m-%d"),
                "hour":       dt.hour,
                "alert_type": alert_type,
            })

    if rows:
        return (
            pd.DataFrame(rows)
            .groupby(["date_str", "hour", "alert_type"])
            .size()
            .reset_index(name="count")
        )
    return pd.DataFrame(columns=["date_str", "hour", "alert_type", "count"])


def is_night(hour: Optional[int]) -> bool:
    if hour is None:
        return False
    return hour >= NIGHT_START or hour < NIGHT_END


def aggregate(
    df: pd.DataFrame, city_to_zone: dict
) -> Tuple[dict, dict, pd.DataFrame, pd.DataFrame]:
    """
    Deduplicate using 90-second temporal clustering per (zone, alert_type):
    alerts within EVENT_CLUSTER_WINDOW seconds to the same zone are treated as
    one alert event (e.g. the same missile spreading across nearby cities).

    Returns (zone_total, zone_night, chart_df, incident_df).
    chart_df has columns [date_str, hour, group, alert_type, count] —
    deduplicated event counts per (date, hour, group, alert_type) cell.
    incident_df has columns [date_str, hour, alert_type, count] —
    cross-zone deduplicated incident counts (same 90 s window across all zones).
    """
    # Phase 1: collect raw (zone, alert_type) -> [dt, ...] from all rows
    raw_events: dict = defaultdict(list)
    unmatched: set   = set()
    skipped_null_dt  = 0
    unknown_types: set = set()

    for row in df.itertuples(index=False):
        dt = row.dt
        if dt is None or pd.isna(dt):
            skipped_null_dt += 1
            continue
        alert_type = str(getattr(row, "alert_type", "Unknown") or "Unknown")
        if alert_type not in ("Pre-alert", "Missile alert", "Drone alert", "Unknown"):
            unknown_types.add(alert_type)
        cities = [c.strip() for c in re.split(r"[,،;|\n]+", str(row.city_raw)) if c.strip()]
        for city in cities:
            zone = city_to_zone.get(city)
            if zone:
                raw_events[(zone, alert_type)].append(dt)
            else:
                unmatched.add(city)

    if skipped_null_dt:
        print(f"  Warning: dropped {skipped_null_dt:,} row(s) with missing timestamp.")
    if unknown_types:
        print(f"  Warning: unrecognised alert type(s) encountered: {sorted(unknown_types)}")
    if unmatched:
        print(f"\n  Note: {len(unmatched)} unmatched city names "
              f"(first 15): {sorted(unmatched)[:15]}")

    # Phase 2: cluster per (zone, alert_type) — zone-level deduplication
    zone_clustered: dict = {}
    for (zone, alert_type), times in raw_events.items():
        zone_clustered[(zone, alert_type)] = cluster_events(times)

    # Global cross-zone deduplication
    incident_df = compute_global_incidents(zone_clustered)

    zone_total = defaultdict(int)
    zone_night = defaultdict(int)
    chart_rows: list = []

    for (zone, alert_type), clustered_times in zone_clustered.items():
        group = ZONE_GROUP.get(zone, "Other")
        for dt in clustered_times:
            zone_total[zone] += 1
            if is_night(dt.hour):
                zone_night[zone] += 1
            chart_rows.append({
                "date_str":   dt.strftime("%Y-%m-%d"),
                "hour":       dt.hour,
                "group":      group,
                "alert_type": alert_type,
            })

    if chart_rows:
        chart_df = (
            pd.DataFrame(chart_rows)
            .groupby(["date_str", "hour", "group", "alert_type"])
            .size()
            .reset_index(name="count")
        )
    else:
        chart_df = pd.DataFrame(columns=["date_str", "hour", "group", "alert_type", "count"])

    return dict(zone_total), dict(zone_night), chart_df, incident_df


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
    _skipped = 0
    for row in df.itertuples(index=False):
        raw        = str(row.city_raw).strip()
        dt         = row.dt
        alert_type = str(getattr(row, "alert_type", "Unknown") or "Unknown")
        if dt is None or pd.isna(dt):
            _skipped += 1
            continue
        if alert_type not in ("Pre-alert", "Missile alert", "Drone alert"):
            continue
        cities = [c.strip() for c in re.split(r"[,،;|\n]+", raw) if c.strip()]
        for city in cities:
            city_events[city].append((dt, alert_type))
    if _skipped:
        print(f"  compute_mismatches: dropped {_skipped:,} row(s) with missing timestamp.")

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
    Count all Missile alerts per (zone, date, hour), after deduplicating
    same-minute hits per zone (consistent with aggregate()).

    Returns DataFrame: [zone, group, date_str, cluster_start, cluster_size]
    where cluster_start is the ISO timestamp of the first missile in that
    zone-date-hour and cluster_size is the missile count.
    """
    zone_times: dict = defaultdict(list)
    for row in df.itertuples(index=False):
        if str(getattr(row, "alert_type", "") or "") != "Missile alert":
            continue
        dt = row.dt
        if dt is None or pd.isna(dt):
            continue
        raw    = str(row.city_raw).strip()
        cities = [c.strip() for c in re.split(r"[,،;|\n]+", raw) if c.strip()]
        for city in cities:
            zone = city_to_zone.get(city)
            if zone:
                zone_times[zone].append(dt)

    rows = []
    for zone, times in zone_times.items():
        group        = ZONE_GROUP.get(zone, "Other")
        deduped      = cluster_events(times)  # 90-second event clustering (consistent with aggregate)

        # Group by (date, hour) and count
        hour_counts: dict = defaultdict(list)
        for dt in deduped:
            key = (dt.strftime("%Y-%m-%d"), dt.hour)
            hour_counts[key].append(dt)

        for (date_str, _hour), hour_times in hour_counts.items():
            rows.append({
                "zone":          zone,
                "group":         group,
                "date_str":      date_str,
                "cluster_start": hour_times[0].isoformat(),
                "cluster_size":  len(hour_times),
            })

    if rows:
        return pd.DataFrame(rows)
    return pd.DataFrame(columns=["zone", "group", "date_str", "cluster_start", "cluster_size"])


# ── Incremental merge helpers ─────────────────────────────────────────────────

def build_gap_hist(mismatch_df: pd.DataFrame, event_type: str) -> dict:
    """Convert paired mismatch rows → {group: {gap_seconds_str: count}} histogram."""
    if mismatch_df is None or mismatch_df.empty:
        return {}
    paired = mismatch_df[
        (mismatch_df["event_type"] == event_type) & mismatch_df["gap_seconds"].notna()
    ].copy()
    if paired.empty:
        return {}
    paired["_key"] = paired["gap_seconds"].astype(int).astype(str)
    return {
        g: sub["_key"].value_counts().to_dict()
        for g, sub in paired.groupby("group")
    }


def merge_chart_df(existing_records: list, new_df: pd.DataFrame) -> pd.DataFrame:
    """Merge new chart_df rows into existing records, re-summing overlapping groups."""
    if not existing_records:
        return new_df
    existing_df = pd.DataFrame(existing_records)
    combined    = pd.concat([existing_df, new_df], ignore_index=True)
    return (
        combined
        .groupby(["date_str", "hour", "group", "alert_type"], as_index=False)["count"]
        .sum()
    )


def merge_incident_df(existing_records: list, new_df: pd.DataFrame) -> pd.DataFrame:
    """Merge new incident_df rows into existing records, re-summing overlapping cells."""
    if not existing_records:
        return new_df
    existing_df = pd.DataFrame(existing_records)
    combined    = pd.concat([existing_df, new_df], ignore_index=True)
    return (
        combined
        .groupby(["date_str", "hour", "alert_type"], as_index=False)["count"]
        .sum()
    )


def merge_mismatch(existing_agg: list, existing_gap_m: dict, existing_gap_d: dict,
                   new_mismatch_df: pd.DataFrame, lookback_dates=None) -> tuple:
    """Merge new mismatch rows into existing aggregated counts and gap histograms.

    lookback_dates: if provided, existing records for those date_str values are dropped
    before merging so re-examined boundary events replace rather than add to prior counts.
    """
    if new_mismatch_df is not None and not new_mismatch_df.empty:
        new_agg_df = (
            new_mismatch_df.groupby(["group", "date_str", "event_type"])
            .size().reset_index(name="count")
        )
        new_agg   = new_agg_df.to_dict(orient="records")
        new_gap_m = build_gap_hist(new_mismatch_df, "paired_missile")
        new_gap_d = build_gap_hist(new_mismatch_df, "paired_drone")
    else:
        new_agg, new_gap_m, new_gap_d = [], {}, {}

    # Strip existing records for the lookback window — they'll be replaced by new_agg.
    if lookback_dates:
        existing_agg = [r for r in existing_agg if r["date_str"] not in lookback_dates]

    # Merge agg: sum counts per (group, date_str, event_type)
    combined: dict = {}
    for rec in existing_agg + new_agg:
        key = (rec["group"], rec["date_str"], rec["event_type"])
        combined[key] = combined.get(key, 0) + rec["count"]
    merged_agg = [
        {"group": k[0], "date_str": k[1], "event_type": k[2], "count": v}
        for k, v in combined.items()
    ]

    def _merge_hist(old: dict, new: dict) -> dict:
        result = {g: dict(h) for g, h in old.items()}
        for g, h in new.items():
            if g not in result:
                result[g] = {}
            for gs, cnt in h.items():
                result[g][gs] = result[g].get(gs, 0) + cnt
        return result

    return merged_agg, _merge_hist(existing_gap_m, new_gap_m), _merge_hist(existing_gap_d, new_gap_d)


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
        if chart_df.empty:
            sub = chart_df
        else:
            _valid_rows = chart_df["date_str"].notna() & chart_df["hour"].notna()
            _dts = pd.to_datetime(
                chart_df.loc[_valid_rows, "date_str"]
            ) + pd.to_timedelta(chart_df.loc[_valid_rows, "hour"].astype(int), unit="h")
            _mask = pd.Series(False, index=chart_df.index)
            _mask.loc[_valid_rows] = (_dts >= pd.Timestamp(start_dt)) & (_dts < pd.Timestamp(end_dt))
            sub = chart_df[_mask]

        def type_sum(atype: str) -> int:
            return int(sub[sub["alert_type"] == atype]["count"].sum()) if not sub.empty else 0

        per_region_hourly: dict = {}
        for row in sub.itertuples(index=False):
            grp  = getattr(row, "group", None) or "Other"
            hour = int(getattr(row, "hour", 0) or 0)
            cnt  = int(getattr(row, "count", 0) or 0)
            if not (0 <= hour <= 23):
                continue
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
            f"{NIGHT_END:02d}:00 today \u2192 now"
        ),
    }


# ── Processed-data serialisation ──────────────────────────────────────────────

def save_processed(chart_df: pd.DataFrame,
                   mismatch_agg: list,
                   gap_missile_hist: dict,
                   gap_drone_hist: dict,
                   partial_day: Optional[str],
                   partial_hour: Optional[int],
                   incident_df: Optional[pd.DataFrame] = None,
                   fetched_at: Optional[str] = None,
                   csv_sha: Optional[str] = None) -> None:
    """Serialise aggregated data to data/processed.json for build_chart.py."""
    DATA_DIR.mkdir(exist_ok=True)
    payload = {
        "schema_version":  PROCESSED_SCHEMA_VERSION,
        "fetched_at":      fetched_at or datetime.now().isoformat(),
        "csv_sha":         csv_sha,
        "partial_day":     partial_day,
        "partial_hour":    partial_hour,
        "chart_df":        chart_df.to_dict(orient="records"),
        "incident_df":     incident_df.to_dict(orient="records") if incident_df is not None else [],
        "mismatch_agg":    mismatch_agg,
        "gap_missile_hist": gap_missile_hist,
        "gap_drone_hist":   gap_drone_hist,
    }
    path = DATA_DIR / "processed.json"
    path.write_text(json.dumps(payload, default=str), encoding="utf-8")
    size_kb = path.stat().st_size // 1024
    print(f"  Processed data saved → {path}  ({size_kb:,} KB)")


def save_situation_json(chart_df: pd.DataFrame, fetched_at: str) -> None:
    """Write output/situation.json with the last 48 h of chart_df rows.

    This small file is fetched client-side on page load so the Situation Room
    always shows data from the most recent build, even if the HTML itself is stale.
    """
    try:
        now_utc = datetime.fromisoformat(fetched_at.rstrip("Z"))
    except (ValueError, AttributeError):
        now_utc = datetime.utcnow()

    cutoff_date = (now_utc - timedelta(days=2)).strftime("%Y-%m-%d")
    recent = chart_df[chart_df["date_str"] >= cutoff_date] if not chart_df.empty else chart_df

    OUTPUT_DIR.mkdir(exist_ok=True)
    payload = {
        "fetched_at": fetched_at,
        "rows": recent.to_dict(orient="records"),
    }
    path = OUTPUT_DIR / "situation.json"
    path.write_text(json.dumps(payload, default=str), encoding="utf-8")
    print(f"  Situation data saved → {path}  ({len(recent):,} rows)")


# ── Chart ─────────────────────────────────────────────────────────────────────

def build_chart(chart_df: pd.DataFrame,
                mismatch_agg: Optional[list] = None,
                gap_missile_hist: Optional[dict] = None,
                gap_drone_hist: Optional[dict] = None,
                partial_day: Optional[str] = None, partial_hour: Optional[int] = None,
                situation_data: Optional[dict] = None,
                fetched_at: Optional[str] = None,
                incident_df: Optional[pd.DataFrame] = None) -> None:
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
            title="Alert Events", showgrid=True,
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
            text="IDF Homefront Command — Cumulative Alert Events by Region<br>"
                 "<sup>Deduplicated per zone (90 s window) · use date filters above to zoom</sup>",
            x=0.5, font=dict(size=15, color="#cccccc"),
        ),
        xaxis=dict(
            title="Date", showgrid=True, gridcolor="#2a2a3e",
            zeroline=False, color="#cccccc",
            type="date", dtick=86400000, tickformat="%b %d", tickangle=-45,
        ),
        yaxis=dict(
            title="Cumulative Alert Events", showgrid=True,
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

    # ── Mismatch / gap-hist serialisation ─────────────────────────────────────
    mismatch_records_js = json.dumps(mismatch_agg or [])
    gap_missile_js      = json.dumps(gap_missile_hist or {})
    gap_drone_js        = json.dumps(gap_drone_hist   or {})

    # ── Incident data (cross-zone deduplication) ──────────────────────────────
    incident_js = json.dumps(
        incident_df.to_dict(orient="records") if incident_df is not None and not incident_df.empty else []
    )

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
  <meta property="og:title"        content="IDF Alert Activity Dashboard">
  <meta property="og:description"  content="Live Israeli Homefront Command alert data — by hour, region, and type">
  <meta property="og:image"        content="{SITE_URL}/preview.png">
  <meta property="og:image:width"  content="1200">
  <meta property="og:image:height" content="630">
  <meta property="og:type"         content="website">
  <meta property="og:url"          content="{SITE_URL}">
  <meta name="twitter:card"        content="summary_large_image">
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
    .tb-btn:focus-visible {{ outline: 2px solid #4455cc; outline-offset: 2px; }}
    #modal-close:focus-visible {{ outline: 2px solid #4455cc; outline-offset: 2px; }}
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

    #sep {{ display: none; width: 1px; height: 22px; background: #ccc; margin: 0 4px; flex-shrink:0; }}
    body.dark #sep {{ background: #333; }}
    /* ── Chart area ── */
    #view-hour, #view-date, #view-mismatch, #view-leadtime, #view-salvos {{
      position: absolute; left:0; right:0;
      display: flex; flex-direction: column;
    }}
    #view-hour  {{ top: var(--tb-h); bottom: 28px; }}
    #view-date  {{ top: var(--tb-h); bottom: 28px; display: none; }}
    #view-mismatch {{ top: var(--tb-h); bottom: 28px; display: none; }}
    #view-leadtime {{ top: var(--tb-h); bottom: 28px; display: none; }}
    #view-salvos   {{ top: var(--tb-h); bottom: 28px; display: none; }}
    /* ── Situation Room ── */
    #view-situation {{ position: absolute; left:0; right:0; top: var(--tb-h); bottom: 28px; display: none; overflow-y: auto; }}
    .sit-section {{ margin-bottom: 28px; }}
    .sit-section-title {{ font-size: 15px; font-weight: 700; margin-bottom: 6px; color: #4455cc; }}
    body.dark .sit-section-title {{ color: #7788ee; }}
    .sit-sublabel {{ font-size: 11px; color: #888; margin-bottom: 8px; }}
    .sit-summary {{ font-size: 13px; color: #444; margin-bottom: 12px; line-height: 1.6; }}
    body.dark .sit-summary {{ color: #aaa; }}
    .sit-quiet {{ font-style: italic; color: #888; }}
    /* ── Situation Room timeline ── */
    .sit-timeline {{ display: flex; flex-direction: column; gap: 2px; margin-top: 6px; }}
    .sit-tl-row {{
      display: flex; align-items: center; gap: 10px;
      padding: 7px 10px; border-radius: 6px; cursor: pointer;
      transition: background 0.12s;
    }}
    .sit-tl-row:hover {{ background: rgba(68,85,204,0.07); }}
    body.dark .sit-tl-row:hover {{ background: rgba(68,85,204,0.15); }}
    .sit-tl-time {{ font-size: 13px; font-weight: 600; color: #444; min-width: 46px; font-variant-numeric: tabular-nums; flex-shrink: 0; }}
    body.dark .sit-tl-time {{ color: #bbb; }}
    .sit-tl-types {{ display: flex; gap: 5px; flex-wrap: wrap; flex: 1; }}
    .sit-badge {{ font-size: 11px; padding: 2px 8px; border-radius: 10px; font-weight: 600; white-space: nowrap; }}
    .sit-badge-missile {{ background: #fee2e2; color: #991b1b; }}
    .sit-badge-pre     {{ background: #fef3c7; color: #92400e; }}
    .sit-badge-drone   {{ background: #e0e7ff; color: #3730a3; }}
    body.dark .sit-badge-missile {{ background: #450a0a; color: #fca5a5; }}
    body.dark .sit-badge-pre     {{ background: #422006; color: #fde68a; }}
    body.dark .sit-badge-drone   {{ background: #1e1b4b; color: #a5b4fc; }}
    .sit-tl-regions {{ display: flex; gap: 4px; flex-wrap: wrap; flex-shrink: 0; align-items: center; }}
    .sit-tl-dot {{ width: 9px; height: 9px; border-radius: 50%; display: inline-block; flex-shrink: 0; }}
    #leadtime-chart {{ width:100%; flex:1; min-height:0; }}
    #salvos-chart {{ width:100%; flex:1; min-height:0; }}
    #main-chart, #date-full-chart {{ width:100%; flex:1; min-height:0; }}
    #mismatch-chart-bar {{ width:100%; flex:1; min-height:0; }}

    /* ── Global footer ── */
    #global-footer {{
      position: fixed; bottom: 0; left: 0; right: 0; z-index: 900;
      padding: 5px 16px; font-size: 11px; text-align: center;
      background: rgba(245,245,252,0.93); color: #888;
      border-top: 1px solid #e0e0e8; backdrop-filter: blur(4px);
    }}
    body.dark #global-footer {{
      background: rgba(10,10,28,0.93); color: #666; border-color: #2a2a3e;
    }}

    /* ── Per-tab explainer strip ── */
    .view-explainer {{
      flex-shrink: 0; padding: 5px 20px 6px;
      font-size: 11px; line-height: 1.5; border-top: 1px solid; text-align: center;
    }}
    body.light .view-explainer {{ color: #666; border-color: #e0e0e8; background: rgba(245,245,252,0.92); }}
    body.dark  .view-explainer {{ color: #888; border-color: #2a2a3e; background: rgba(10,10,28,0.92); }}
    .sit-explainer {{
      font-size: 11px; color: #888; text-align: center;
      padding: 12px 0 4px; border-top: 1px solid #e0e0e8; margin-top: 8px; line-height: 1.5;
    }}
    body.dark .sit-explainer {{ color: #666; border-color: #2a2a3e; }}

    /* ── RTL (Hebrew) ── */
    :root[dir=rtl] #nav-tabs {{ flex-direction: row; }}
    :root[dir=rtl] #nav-row  {{ flex-direction: row; }}

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
      #hamburger-btn {{ display: inline-flex; border-radius: 4px; border-color: transparent; background: transparent; color: #555; }}
      body.dark #hamburger-btn {{ color: #999; }}
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
        font-size: 13px;
        /* Drawer items: flat rows with left accent, not tab or pill */
        border-radius: 0; border: none;
        border-left: 3px solid transparent;
        top: 0; background: transparent; color: #555;
      }}
      #nav-tabs .tb-btn:hover {{
        background: rgba(68,85,204,0.07);
        border-left-color: #aab;
        color: #222;
      }}
      #nav-tabs .tb-btn.active {{
        background: rgba(68,85,204,0.10);
        border-left: 3px solid #4455cc;
        color: #4455cc;
      }}
      body.dark #nav-tabs .tb-btn {{ color: #888; background: transparent; border-left-color: transparent; }}
      body.dark #nav-tabs .tb-btn:hover {{ background: rgba(68,85,204,0.15); color: #ccc; border-left-color: #555; }}
      body.dark #nav-tabs .tb-btn.active {{ background: rgba(68,85,204,0.20); border-left-color: #7788ee; color: #7788ee; }}
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
      <button id="hamburger-btn" class="tb-btn" onclick="toggleNavDrawer()" aria-label="Open navigation menu" aria-expanded="false" aria-controls="nav-tabs">&#9776;&nbsp;<span id="hamburger-label">Situation Room</span></button>
      <div id="nav-tabs" role="tablist">
        <div id="sep"></div>
        <button class="tb-btn active" onclick="setView('situation')" id="btn-situation" role="tab" aria-selected="true"  aria-controls="view-situation">&#9889;&nbsp;Situation Room</button>
        <button class="tb-btn"        onclick="setView('hour')"      id="btn-hour"      role="tab" aria-selected="false" aria-controls="view-hour">&#9200;&nbsp;By Hour</button>
        <button class="tb-btn"        onclick="setView('date')"      id="btn-date"      role="tab" aria-selected="false" aria-controls="view-date">&#128197;&nbsp;By Date</button>
        <button class="tb-btn"        onclick="setView('mismatch')"  id="btn-mismatch"  role="tab" aria-selected="false" aria-controls="view-mismatch">&#9888;&#65039;&nbsp;Mismatches</button>
        <button class="tb-btn"        onclick="setView('leadtime')"  id="btn-leadtime"  role="tab" aria-selected="false" aria-controls="view-leadtime">&#9203;&nbsp;Lead Time</button>
        <button class="tb-btn"        onclick="setView('salvos')"    id="btn-salvos"    role="tab" aria-selected="false" aria-controls="view-salvos">&#128165;&nbsp;Salvos</button>
      </div>
      <div id="nav-spacer"></div>
      <button class="tb-btn" onclick="toggleLang()" id="lang-btn" title="עברית / English" aria-label="Toggle language between English and Hebrew">&#127760;&nbsp;EN</button>
      <button class="tb-btn" onclick="toggleTheme()" id="theme-btn" aria-label="Toggle dark/light theme">&#127769;&nbsp;Dark</button>
    </div>

    <!-- Row 2: view-specific filters -->
    <div id="filter-row">

      <!-- By Hour controls -->
      <div id="hour-controls" style="display:none;align-items:center;gap:6px;">
        <select id="hour-region-select" class="tb-region-select" onchange="onHourRegion(this.value)">
          <option value="" data-i18n="lbl_all_regions">All regions</option>
        </select>
        <div class="tb-sep"></div>
        <span style="font-size:12px;color:#555;white-space:nowrap;" data-i18n="lbl_from">From:</span>
        <select id="hour-date-from" class="tb-region-select" onchange="onHourDateFrom(this.value)"></select>
        <span style="font-size:12px;color:#555;" data-i18n="lbl_to">To:</span>
        <select id="hour-date-to" class="tb-region-select" onchange="onHourDateTo(this.value)"></select>
        <div class="tb-sep"></div>
        <div id="type-btns" style="display:flex;gap:6px;">
          <button class="tb-btn" id="type-pre" onclick="setTypeMode('pre')" data-i18n="btn_pre">Pre-alert</button>
          <button class="tb-btn active" id="type-md" onclick="setTypeMode('missile_drone')" data-i18n="btn_missile">Missile &amp; Drone</button>
        </div>
      </div>

      <!-- By Date controls -->
      <div id="date-controls" style="display:none;align-items:center;gap:6px;">
        <span style="font-size:12px;color:#555;white-space:nowrap;" data-i18n="lbl_from">From:</span>
        <select id="date-view-from" class="tb-region-select" onchange="onDateViewFrom(this.value)"></select>
        <span style="font-size:12px;color:#555;" data-i18n="lbl_to">To:</span>
        <select id="date-view-to" class="tb-region-select" onchange="onDateViewTo(this.value)"></select>
      </div>

      <!-- Mismatch controls -->
      <div id="mismatch-controls" style="display:none;align-items:center;gap:6px;">
        <select id="mismatch-region-select" class="tb-region-select" onchange="onMismatchRegion(this.value)">
          <option value="" data-i18n="lbl_all_regions">All regions</option>
        </select>
        <button class="tb-btn" onclick="toggleMismatchMode()" id="mismatch-mode-btn">%&nbsp;View</button>
      </div>

      <!-- Lead Time controls -->
      <div id="leadtime-controls" style="display:none;align-items:center;gap:6px;">
        <select id="leadtime-region-select" class="tb-region-select" onchange="onLeadtimeRegion(this.value)">
          <option value="" data-i18n="lbl_all_regions">All regions</option>
        </select>
      </div>

      <!-- Salvos controls -->
      <div id="salvos-controls" style="display:none;align-items:center;gap:6px;">
        <select id="salvos-region-select" class="tb-region-select" onchange="onSalvosRegion(this.value)">
          <option value="" data-i18n="lbl_all_regions">All regions</option>
        </select>
        <div class="tb-sep"></div>
        <span style="font-size:12px;color:#555;white-space:nowrap;" data-i18n="lbl_from">From:</span>
        <select id="salvos-date-from" class="tb-region-select" onchange="onSalvosDateFrom(this.value)"></select>
        <span style="font-size:12px;color:#555;" data-i18n="lbl_to">To:</span>
        <select id="salvos-date-to" class="tb-region-select" onchange="onSalvosDateTo(this.value)"></select>
      </div>

    </div><!-- /filter-row -->
  </div><!-- /topbar -->

  <!-- Situation Room view -->
  <div id="view-situation">
    <div id="situation-content" style="padding:20px 24px;box-sizing:border-box;"></div>
  </div>

  <!-- By-hour view -->
  <div id="view-hour" style="display:flex;">
    <div id="main-chart"></div>
    <div class="view-explainer" data-i18n-html="explainer_hour">Stacked by region &middot; deduplicated events (same zone within 90&thinsp;s&nbsp;=&nbsp;1 event) &middot; click a bar to drill down by day</div>
  </div>

  <!-- By-date cumulative view -->
  <div id="view-date">
    <div id="date-full-chart"></div>
    <div class="view-explainer" data-i18n-html="explainer_date">Cumulative deduplicated events per region since 28&nbsp;Feb&nbsp;2026 &middot; same zone within 90&thinsp;s&nbsp;=&nbsp;1 event</div>
  </div>

  <!-- Mismatch view -->
  <div id="view-mismatch">
    <div id="mismatch-chart-bar"></div>
    <div class="view-explainer" data-i18n-html="explainer_mismatch">A Pre-alert is <strong>paired</strong> if a Missile alert follows within 15&thinsp;min for the same city &middot; <strong>Pre-alert only</strong> = warning, no missile &middot; <strong>Missile only</strong> = missile, no prior warning &middot; Drone alerts excluded &middot; dotted line = 7-day rolling mismatch&nbsp;%</div>
  </div>

  <!-- Lead-time histogram view -->
  <div id="view-leadtime">
    <div id="leadtime-chart"></div>
    <div class="view-explainer" data-i18n-html="explainer_leadtime">Histogram of the gap (seconds) between a Pre-alert and the Missile alert that followed it for the same city (within 15&thinsp;min) &middot; taller bar = more common lead time</div>
  </div>

  <!-- Salvos view -->
  <div id="view-salvos">
    <div id="salvos-chart"></div>
    <div class="view-explainer" data-i18n-html="explainer_salvos">Each line = one day &middot; X-axis = hour of day &middot; counts deduplicated (90&thinsp;s window)</div>
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
    var incidentData = {incident_js};
    var groupColors  = {group_colors_js};
    var allGroups    = {groups_js};
    var allTypes     = {alert_types_js};

    var hourData            = {hour_data_js};
    var hourLayout          = {hour_layout_js};
    var dateData            = {date_data_js};
    var dateLayout          = {date_layout_js};
    var allMismatchRecords  = {mismatch_records_js};
    var gapMissileHist      = {gap_missile_js};
    var gapDroneHist        = {gap_drone_js};
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

    // ── i18n translations ────────────────────────────────────────────────────
    var TRANSLATIONS = {{
      en: {{
        tab_situation: 'Situation Room', tab_hour: 'By Hour', tab_date: 'By Date',
        tab_mismatch: 'Mismatches', tab_leadtime: 'Lead Time', tab_salvos: 'Salvos',
        btn_pre: 'Pre-alert', btn_missile: 'Missile \u0026 Drone',
        btn_dark: '\U0001F319\u00a0Dark', btn_light: '\u2600\ufe0f\u00a0Light',
        btn_lang: '\u05e2\u05d1',
        lbl_from: 'From:', lbl_to: 'To:', lbl_all_regions: 'All regions', lbl_all_dates: 'All',
        sit_lastnight_title: 'What happened last night?',
        sit_today_title: 'What\u2019s happening today?',
        sit_quiet: 'Quiet \u2014 no alerts recorded for this period.',
        title_hour: 'IDF Homefront Command \u2014 Alert Events by Hour<br><sup>Stacked by region \u00b7 deduplicated (90\u2009s window) \u00b7 click a bar to drill down</sup>',
        title_date: 'IDF Homefront Command \u2014 Cumulative Alert Events by Region<br><sup>Deduplicated per zone (90\u2009s window) \u00b7 use date filters above to zoom</sup>',
        explainer_hour: 'Stacked by region \u00b7 deduplicated events (same zone within 90\u2009s\u00a0=\u00a01 event) \u00b7 click a bar to drill down by day',
        explainer_date: 'Cumulative deduplicated events per region since 28\u00a0Feb\u00a02026 \u00b7 same zone within 90\u2009s\u00a0=\u00a01 event',
        explainer_mismatch: 'A Pre-alert is <strong>paired</strong> if a Missile alert follows within 15\u2009min for the same city \u00b7 <strong>Pre-alert only</strong> = warning, no missile \u00b7 <strong>Missile only</strong> = missile, no prior warning \u00b7 Drone alerts excluded \u00b7 dotted line = 7-day rolling mismatch\u00a0%',
        explainer_leadtime: 'Histogram of the gap (seconds) between a Pre-alert and the Missile alert that followed it for the same city (within 15\u2009min) \u00b7 taller bar = more common lead time',
        explainer_situation: 'Shows deduplicated alert events for <strong>last night</strong> (22:00\u201306:00) and <strong>today</strong> (06:00\u2013now), Israel time. Alerts to the same zone within 90\u2009seconds count as one event. Click any row for a regional breakdown.',
        explainer_salvos: 'Each line = one day \u00b7 X-axis = hour of day \u00b7 counts deduplicated (90\u2009s window)',
        title_mismatch_base: 'Mismatch by Day',
        title_mismatch_sub: '15-min pairing \u00b7 7-day mismatch\u00a0% (dotted) on right axis',
        title_leadtime_base: 'Warning Lead Time Distribution',
        title_salvos_base: 'Missile Alert Events by Hour of Day',
        title_salvos_sub: 'Deduplicated events per hour (90\u2009s window) \u00b7 one line per day',
        lbl_all_regions_title: 'All Regions',
        trace_paired_missile: 'Paired (missile)',
        trace_paired_drone:   'Paired (drone)',
        trace_pre_only:       'Pre-alert only',
        trace_missile_only:   'Missile only',
        trace_drone_only:     'Drone only',
        trace_7d_pct:         '7d mismatch %',
        hover_7d_rate:        '7d mismatch rate:',
        hover_alerts:         'alerts',
        hover_min:            'min',
        hover_missile_alert:  'Missile alert',
        hover_pre_alert:      'Pre-alert',
        hover_drone_alert:    'Drone alert',
        hover_missile_events: 'missile events',
        popup_daily_dist:     'Daily Alert Distribution',
        yaxis_alert_events:   'Alert Events',
        xaxis_hour:           'Hour of Day',
        yaxis_hour:           'Alert Events',
        legend_region:        'Region',
        xaxis_date:           'Date',
        yaxis_date_cumul:     'Cumulative Alert Events',
        hover_cumulative:     'Cumulative',
        leadtime_paired_only: 'Paired events only',
        leadtime_bins:        '30-second bins',
        leadtime_missile_pairs: 'missile pairs',
        leadtime_drone_pairs:   'drone pairs',
        xaxis_leadtime:       'Minutes from Pre-alert to Paired Alert',
        yaxis_prealerts:      'Number of Pre-alerts',
        btn_pct_view:         '% View',
        btn_abs_view:         'Abs View',
        yaxis_pct_events:     '% of Events',
        yaxis_event_count:    'Event Count',
        yaxis_mismatch_pct:   'Mismatch %',
        yaxis_missile_events_axis: 'Missile Alert Events',
      }},
      he: {{
        tab_situation: '\u05d7\u05d3\u05e8 \u05de\u05e6\u05d1',
        tab_hour: '\u05dc\u05e4\u05d9 \u05e9\u05e2\u05d4',
        tab_date: '\u05dc\u05e4\u05d9 \u05ea\u05d0\u05e8\u05d9\u05da',
        tab_mismatch: '\u05d0\u05d9-\u05d4\u05ea\u05d0\u05de\u05d5\u05ea',
        tab_leadtime: '\u05d6\u05de\u05df \u05d0\u05d6\u05d4\u05e8\u05d4',
        tab_salvos: '\u05de\u05d8\u05d7\u05d9\u05dd',
        btn_pre: '\u05d4\u05ea\u05e8\u05d0\u05d4 \u05de\u05d5\u05e7\u05d3\u05de\u05ea',
        btn_missile: '\u05d8\u05d9\u05dc \u05d5\u05e8\u05d7\u05e4\u05df',
        btn_dark: '\U0001F319\u00a0\u05db\u05d4\u05d4',
        btn_light: '\u2600\ufe0f\u00a0\u05d1\u05d4\u05d9\u05e8',
        btn_lang: 'EN',
        lbl_from: '\u05de:',
        lbl_to: '\u05e2\u05d3:',
        lbl_all_regions: '\u05db\u05dc \u05d4\u05d0\u05d6\u05d5\u05e8\u05d9\u05dd',
        lbl_all_dates: '\u05d4\u05db\u05dc',
        sit_lastnight_title: '\u05de\u05d4 \u05e7\u05e8\u05d4 \u05d0\u05de\u05e9 \u05d1\u05dc\u05d9\u05dc\u05d4?',
        sit_today_title: '\u05de\u05d4 \u05e7\u05d5\u05e8\u05d4 \u05d4\u05d9\u05d5\u05dd?',
        sit_quiet: '\u05e9\u05e7\u05d8 \u2014 \u05dc\u05d0 \u05e0\u05e8\u05e9\u05de\u05d5 \u05d4\u05ea\u05e8\u05d0\u05d5\u05ea \u05dc\u05ea\u05e7\u05d5\u05e4\u05d4 \u05d6\u05d5.',
        title_hour: '\u05e4\u05d9\u05e7\u05d5\u05d3 \u05d4\u05e2\u05d5\u05e8\u05e3 \u2014 \u05d0\u05d9\u05e8\u05d5\u05e2\u05d9 \u05d4\u05ea\u05e8\u05d0\u05d4 \u05dc\u05e4\u05d9 \u05e9\u05e2\u05d4<br><sup>\u05de\u05d5\u05e2\u05e8\u05dd \u05dc\u05e4\u05d9 \u05d0\u05d6\u05d5\u05e8 \u00b7 \u05de\u05d1\u05d5\u05d6\u05e0\u05d5\u05d2 (90\u05e9) \u00b7 \u05dc\u05d7\u05e5 \u05e2\u05de\u05d5\u05d3\u05d4 \u05dc\u05e4\u05d9\u05e8\u05d5\u05d8</sup>',
        title_date: '\u05e4\u05d9\u05e7\u05d5\u05d3 \u05d4\u05e2\u05d5\u05e8\u05e3 \u2014 \u05d0\u05d9\u05e8\u05d5\u05e2\u05d9 \u05d4\u05ea\u05e8\u05d0\u05d4 \u05de\u05e6\u05d8\u05d1\u05e8\u05d9\u05dd \u05dc\u05e4\u05d9 \u05d0\u05d6\u05d5\u05e8<br><sup>\u05de\u05d1\u05d5\u05d6\u05e0\u05d5\u05d2 \u05dc\u05d0\u05d6\u05d5\u05e8 (90\u05e9) \u00b7 \u05d4\u05e9\u05ea\u05de\u05e9 \u05d1\u05e4\u05d9\u05dc\u05d8\u05e8\u05d9 \u05d4\u05ea\u05d0\u05e8\u05d9\u05da \u05dc\u05d6\u05d5\u05dd</sup>',
        explainer_hour: '\u05de\u05d5\u05e2\u05e8\u05dd \u05dc\u05e4\u05d9 \u05d0\u05d6\u05d5\u05e8 \u00b7 \u05d0\u05d9\u05e8\u05d5\u05e2\u05d9\u05dd \u05de\u05d1\u05d5\u05d6\u05e0\u05d5\u05d2\u05d9\u05dd (\u05d0\u05d5\u05ea\u05d5 \u05d0\u05d6\u05d5\u05e8 \u05d1\u05ea\u05d5\u05da 90\u2009\u05e9 = \u05d0\u05d9\u05e8\u05d5\u05e2 \u05d0\u05d7\u05d3) \u00b7 \u05dc\u05d7\u05e5 \u05e2\u05dc \u05e2\u05de\u05d5\u05d3\u05d4 \u05dc\u05e4\u05d9\u05e8\u05d5\u05d8 \u05dc\u05e4\u05d9 \u05d9\u05d5\u05dd',
        explainer_date: '\u05d0\u05d9\u05e8\u05d5\u05e2\u05d9\u05dd \u05de\u05e6\u05d8\u05d1\u05e8\u05d9\u05dd \u05de\u05d1\u05d5\u05d6\u05e0\u05d5\u05d2\u05d9\u05dd \u05dc\u05e4\u05d9 \u05d0\u05d6\u05d5\u05e8 \u05de\u05d0\u05d6 28 \u05e4\u05d1\u05e8\u05d5\u05d0\u05e8 2026 \u00b7 \u05d0\u05d5\u05ea\u05d5 \u05d0\u05d6\u05d5\u05e8 \u05d1\u05ea\u05d5\u05da 90\u2009\u05e9 = \u05d0\u05d9\u05e8\u05d5\u05e2 \u05d0\u05d7\u05d3',
        explainer_mismatch: '\u05d4\u05ea\u05e8\u05d0\u05d4 \u05de\u05d5\u05e7\u05d3\u05de\u05ea <strong>\u05de\u05d5\u05ea\u05d0\u05de\u05ea</strong> = \u05d9\u05e8\u05d9 \u05d8\u05d9\u05dc\u05d9\u05dd \u05d1\u05ea\u05d5\u05da 15\u2009\u05d3\u05e7 \u05dc\u05d0\u05d5\u05ea\u05d4 \u05e2\u05d9\u05e8 \u00b7 \u05d4\u05ea\u05e8\u05d0\u05d4 \u05de\u05d5\u05e7\u05d3\u05de\u05ea \u05d1\u05dc\u05d1\u05d3 = \u05d0\u05d6\u05d4\u05e8\u05d4 \u05dc\u05dc\u05d0 \u05d8\u05d9\u05dc \u00b7 \u05d8\u05d9\u05dc \u05d1\u05dc\u05d1\u05d3 = \u05d8\u05d9\u05dc \u05dc\u05dc\u05d0 \u05d0\u05d6\u05d4\u05e8\u05d4 \u05de\u05d5\u05e7\u05d3\u05de\u05ea \u00b7 \u05e8\u05d7\u05e4\u05e0\u05d9\u05dd \u05dc\u05d0 \u05e0\u05db\u05dc\u05dc\u05d9\u05dd \u00b7 \u05e7\u05d5 \u05de\u05e7\u05d5\u05d5\u05e7\u05d5 = % \u05d0\u05d9-\u05d4\u05ea\u05d0\u05de\u05d4 \u05e9\u05d1\u05d5\u05e2\u05d9',
        explainer_leadtime: '\u05d4\u05d9\u05e1\u05d8\u05d5\u05d2\u05e8\u05de\u05d4 \u05e9\u05dc \u05e4\u05e8\u05e9 \u05d4\u05d6\u05de\u05df (\u05e9\u05e0\u05d9\u05d5\u05ea) \u05d1\u05d9\u05df \u05d4\u05ea\u05e8\u05d0\u05d4 \u05de\u05d5\u05e7\u05d3\u05de\u05ea \u05dc\u05d9\u05e8\u05d9 \u05d4\u05d8\u05d9\u05dc\u05d9\u05dd \u05e9\u05d4\u05d2\u05d9\u05e2 \u05d0\u05d7\u05e8\u05d9\u05d4 \u05dc\u05d0\u05d5\u05ea\u05d4 \u05e2\u05d9\u05e8 (15\u2009\u05d3\u05e7 \u05d4\u05e8\u05d0\u05e9\u05d5\u05e0\u05d5\u05ea) \u00b7 \u05e2\u05de\u05d5\u05d3\u05d4 \u05d2\u05d1\u05d5\u05d4\u05d4 \u05d9\u05d5\u05ea\u05e8 = \u05d6\u05de\u05df \u05d0\u05d6\u05d4\u05e8\u05d4 \u05e0\u05e4\u05d5\u05e5 \u05d9\u05d5\u05ea\u05e8',
        explainer_situation: '\u05de\u05e6\u05d9\u05d2 \u05d0\u05d9\u05e8\u05d5\u05e2\u05d9 \u05d4\u05ea\u05e8\u05d0\u05d4 \u05de\u05d1\u05d5\u05d6\u05e0\u05d5\u05d2\u05d9\u05dd \u05dc<strong>\u05d0\u05de\u05e9 \u05d1\u05dc\u05d9\u05dc\u05d4</strong> (22:00\u201306:00) \u05d5\u05dc<strong>\u05d4\u05d9\u05d5\u05dd</strong> (06:00\u2013\u05db\u05e2\u05ea), \u05d1\u05e9\u05e2\u05d5\u05df \u05d9\u05e9\u05e8\u05d0\u05dc. \u05d4\u05ea\u05e8\u05d0\u05d5\u05ea \u05dc\u05d0\u05d5\u05ea\u05d5 \u05d0\u05d6\u05d5\u05e8 \u05d1\u05ea\u05d5\u05da 90\u2009\u05e9\u05e0\u05d9\u05d5\u05ea \u05e0\u05e1\u05e4\u05e8\u05d5\u05ea \u05db\u05d0\u05d9\u05e8\u05d5\u05e2 \u05d0\u05d7\u05d3. \u05dc\u05d7\u05e5 \u05e2\u05dc \u05e9\u05d5\u05e8\u05d4 \u05dc\u05e4\u05d9\u05e8\u05d5\u05d8 \u05dc\u05e4\u05d9 \u05d0\u05d6\u05d5\u05e8.',
        explainer_salvos: '\u05db\u05dc \u05e7\u05d5 = \u05d9\u05d5\u05dd \u05d0\u05d7\u05d3 \u00b7 \u05e6\u05d9\u05e8 X = \u05e9\u05e2\u05d4 \u05d1\u05d9\u05d5\u05dd \u00b7 \u05e1\u05e4\u05d9\u05e8\u05d5\u05ea \u05de\u05d1\u05d5\u05d6\u05e0\u05d5\u05d2\u05d5\u05ea (\u05d7\u05dc\u05d5\u05df 90\u2009\u05e9)',
        title_mismatch_base: '\u05d0\u05d9-\u05d4\u05ea\u05d0\u05de\u05d5\u05ea \u05dc\u05e4\u05d9 \u05d9\u05d5\u05dd',
        title_mismatch_sub: '\u05d7\u05d9\u05d1\u05d5\u05e8 \u05d1\u05d8\u05d5\u05d5\u05d7 15\u2009\u05d3\u05e7 \u00b7 % \u05d0\u05d9-\u05d4\u05ea\u05d0\u05de\u05d4 \u05e9\u05d1\u05d5\u05e2\u05d9 (\u05e7\u05d5 \u05de\u05e7\u05d5\u05d5\u05e7\u05d5) \u05d1\u05e6\u05d9\u05e8 \u05d9\u05de\u05e0\u05d9',
        title_leadtime_base: '\u05d4\u05ea\u05e4\u05dc\u05d2\u05d5\u05ea \u05d6\u05de\u05df \u05d4\u05d0\u05d6\u05d4\u05e8\u05d4 \u05d4\u05de\u05d5\u05e7\u05d3\u05de\u05ea',
        title_salvos_base: '\u05d0\u05d9\u05e8\u05d5\u05e2\u05d9 \u05d9\u05e8\u05d9 \u05d8\u05d9\u05dc\u05d9\u05dd \u05dc\u05e4\u05d9 \u05e9\u05e2\u05d4 \u05d1\u05d9\u05d5\u05dd',
        title_salvos_sub: '\u05d0\u05d9\u05e8\u05d5\u05e2\u05d9\u05dd \u05de\u05d1\u05d5\u05d6\u05e0\u05d5\u05d2\u05d9\u05dd \u05dc\u05e9\u05e2\u05d4 (90\u2009\u05e9 \u05d7\u05dc\u05d5\u05df) \u00b7 \u05e7\u05d5 \u05d0\u05d7\u05d3 \u05dc\u05db\u05dc \u05d9\u05d5\u05dd',
        lbl_all_regions_title: '\u05d4\u05db\u05dc',
        trace_paired_missile: '\u05de\u05d5\u05ea\u05d0\u05dd (\u05d9\u05e8\u05d9 \u05e8\u05e7\u05d8\u05d5\u05ea)',
        trace_paired_drone:   '\u05de\u05d5\u05ea\u05d0\u05dd (\u05e8\u05d7\u05e4\u05df)',
        trace_pre_only:       '\u05d4\u05ea\u05e8\u05d0\u05d4 \u05de\u05d5\u05e7\u05d3\u05de\u05ea \u05d1\u05dc\u05d1\u05d3',
        trace_missile_only:   '\u05d9\u05e8\u05d9 \u05d8\u05d9\u05dc \u05d1\u05dc\u05d1\u05d3',
        trace_drone_only:     '\u05e8\u05d7\u05e4\u05df \u05d1\u05dc\u05d1\u05d3',
        trace_7d_pct:         '% \u05d0\u05d9-\u05d4\u05ea\u05d0\u05de\u05d4 7 \u05d9\u05de\u05d9\u05dd',
        hover_7d_rate:        '\u05e9\u05d9\u05e2\u05d5\u05e8 \u05d0\u05d9-\u05d4\u05ea\u05d0\u05de\u05d4 7 \u05d9\u05de\u05d9\u05dd:',
        hover_alerts:         '\u05d4\u05ea\u05e8\u05d0\u05d5\u05ea',
        hover_min:            '\u05d3\u05e7',
        hover_missile_alert:  '\u05d9\u05e8\u05d9 \u05e8\u05e7\u05d8\u05d5\u05ea \u05d5\u05d8\u05d9\u05dc\u05d9\u05dd',
        hover_pre_alert:      '\u05d4\u05ea\u05e8\u05d0\u05d4 \u05de\u05d5\u05e7\u05d3\u05de\u05ea',
        hover_drone_alert:    '\u05d7\u05d3\u05d9\u05e8\u05ea \u05e8\u05d7\u05e4\u05df',
        hover_missile_events: '\u05d0\u05d9\u05e8\u05d5\u05e2\u05d9 \u05d9\u05e8\u05d9',
        popup_daily_dist:     '\u05d4\u05ea\u05e4\u05dc\u05d2\u05d5\u05ea \u05d9\u05d5\u05de\u05d9\u05ea',
        yaxis_alert_events:   '\u05d0\u05d9\u05e8\u05d5\u05e2\u05d9 \u05d4\u05ea\u05e8\u05d0\u05d4',
        xaxis_hour:           '\u05e9\u05e2\u05d4 \u05d1\u05d9\u05d5\u05dd',
        yaxis_hour:           '\u05d0\u05d9\u05e8\u05d5\u05e2\u05d9 \u05d4\u05ea\u05e8\u05d0\u05d4',
        legend_region:        '\u05d0\u05d6\u05d5\u05e8',
        xaxis_date:           '\u05ea\u05d0\u05e8\u05d9\u05da',
        yaxis_date_cumul:     '\u05d0\u05d9\u05e8\u05d5\u05e2\u05d9\u05dd \u05de\u05e6\u05d8\u05d1\u05e8\u05d9\u05dd',
        hover_cumulative:     '\u05de\u05e6\u05d8\u05d1\u05e8',
        leadtime_paired_only: '\u05d0\u05d9\u05e8\u05d5\u05e2\u05d9\u05dd \u05de\u05d5\u05ea\u05d0\u05de\u05d9\u05dd \u05d1\u05dc\u05d1\u05d3',
        leadtime_bins:        '\u05e8\u05e6\u05d5\u05e2\u05d5\u05ea 30 \u05e9\u05e0\u05d9\u05d5\u05ea',
        leadtime_missile_pairs: '\u05d6\u05d5\u05d2\u05d5\u05ea \u05d8\u05d9\u05dc',
        leadtime_drone_pairs:   '\u05d6\u05d5\u05d2\u05d5\u05ea \u05e8\u05d7\u05e4\u05df',
        xaxis_leadtime:       '\u05d3\u05e7\u05d5\u05ea \u05de\u05d4\u05ea\u05e8\u05d0\u05d4 \u05de\u05d5\u05e7\u05d3\u05de\u05ea \u05e2\u05d3 \u05d0\u05d6\u05e2\u05e7\u05ea \u05d8\u05d9\u05dc',
        yaxis_prealerts:      '\u05de\u05e1\u05e4\u05e8 \u05d4\u05ea\u05e8\u05d0\u05d5\u05ea \u05de\u05d5\u05e7\u05d3\u05de\u05d5\u05ea',
        btn_pct_view:         '\u05ea\u05e6\u05d5\u05d2\u05ea %',
        btn_abs_view:         '\u05ea\u05e6\u05d5\u05d2\u05ea \u05de\u05d5\u05d7\u05dc\u05d8\u05ea',
        yaxis_pct_events:     '% \u05de\u05d0\u05d9\u05e8\u05d5\u05e2\u05d9\u05dd',
        yaxis_event_count:    '\u05e1\u05e4\u05d9\u05e8\u05ea \u05d0\u05d9\u05e8\u05d5\u05e2\u05d9\u05dd',
        yaxis_mismatch_pct:   '% \u05d0\u05d9-\u05d4\u05ea\u05d0\u05de\u05d4',
        yaxis_missile_events_axis: '\u05d0\u05d9\u05e8\u05d5\u05e2\u05d9 \u05d9\u05e8\u05d9 \u05d8\u05d9\u05dc\u05d9\u05dd',
      }},
    }};

    // Region name map English → Hebrew
    var REGION_HE = {{
      'Golan': '\u05d2\u05d5\u05dc\u05df',
      'Galilee': '\u05d2\u05dc\u05d9\u05dc',
      'Haifa Area': '\u05d0\u05d6\u05d5\u05e8 \u05d7\u05d9\u05e4\u05d4',
      'West Bank': '\u05d9\u05d4\u05d5\u05d3\u05d4 \u05d5\u05e9\u05d5\u05de\u05e8\u05d5\u05df',
      'Tel Aviv / Gush Dan': '\u05ea\u05dc \u05d0\u05d1\u05d9\u05d1 / \u05d2\u05d5\u05e9 \u05d3\u05df',
      'Sharon / Shephelah': '\u05e9\u05e8\u05d5\u05df / \u05e9\u05e4\u05dc\u05d4',
      'Jerusalem': '\u05d9\u05e8\u05d5\u05e9\u05dc\u05d9\u05dd',
      'Gaza Area': '\u05e2\u05d5\u05d8\u05e3 \u05e2\u05d6\u05d4',
      'Beer Sheva / Negev': '\u05d1\u05d0\u05e8 \u05e9\u05d1\u05e2 / \u05e0\u05d2\u05d1',
      'Arava': '\u05e2\u05e8\u05d1\u05d4',
      'Eilat': '\u05d0\u05d9\u05dc\u05ea',
    }};

    function toggleNavDrawer() {{
      var drawer = document.getElementById('nav-tabs');
      var btn    = document.getElementById('hamburger-btn');
      drawer.classList.toggle('open');
      btn.setAttribute('aria-expanded', drawer.classList.contains('open') ? 'true' : 'false');
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
    var currentLang       = 'en';
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
        text: 'Cumulative Alert Events by Region',
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
      initSituationRoom();
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
      document.getElementById('view-hour').style.display       = v === 'hour'      ? 'flex'  : 'none';
      document.getElementById('view-date').style.display       = v === 'date'      ? 'flex'  : 'none';
      document.getElementById('view-mismatch').style.display   = v === 'mismatch'  ? 'flex'  : 'none';
      document.getElementById('view-leadtime').style.display   = v === 'leadtime'  ? 'flex'  : 'none';
      document.getElementById('view-salvos').style.display     = v === 'salvos'    ? 'flex'  : 'none';
      ['situation','hour','date','mismatch','leadtime','salvos'].forEach(function(tab) {{
        var btn = document.getElementById('btn-' + tab);
        btn.classList.toggle('active', v === tab);
        btn.setAttribute('aria-selected', v === tab ? 'true' : 'false');
      }});
      var hbBtn = document.getElementById('hamburger-btn');
      if (hbBtn) {{ hbBtn.setAttribute('aria-expanded', 'false'); }}
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

      var _T = TRANSLATIONS[currentLang] || TRANSLATIONS.en;
      var _isHe = currentLang === 'he';
      var newTraces = allGroups.map(function(group) {{
        var displayName = _isHe ? (REGION_HE[group] || group) : group;
        var ys = Array.from({{length:24}}, function(_, h) {{ return agg[group+'|'+h] || 0; }});
        return {{
          type: 'bar',
          x: Array.from({{length:24}}, function(_, h) {{ return h; }}),
          y: ys, name: displayName,
          marker: {{ color: groupColors[group] || '#888' }},
          hovertemplate: '<b>' + displayName + '</b><br>%{{x:02d}}:00 \u2014 <b>%{{y:,}}</b> ' + _T.hover_alerts + '<extra></extra>',
        }};
      }});

      // Lock Y-axis scale from bar traces only (before adding scatter)
      var maxY = 0;
      for (var h = 0; h < 24; h++) {{
        var s = 0; newTraces.forEach(function(t) {{ s += t.y[h] || 0; }}); maxY = Math.max(maxY, s);
      }}

      // Add total-incidents dotted line (hidden when region filter is active)
      if (!hourRegion) {{
        var incFiltered = incidentData.filter(function(r) {{
          var dateOk = !currentDateRange ||
            (r.date_str >= currentDateRange[0] && r.date_str <= currentDateRange[1]);
          var typeOk = activeTypeMode === 'pre'
            ? r.alert_type === 'Pre-alert'
            : r.alert_type !== 'Pre-alert';
          return dateOk && typeOk;
        }});
        var incAgg = {{}};
        incFiltered.forEach(function(r) {{ incAgg[r.hour] = (incAgg[r.hour] || 0) + r.count; }});
        var incYs = Array.from({{length:24}}, function(_, h) {{ return incAgg[h] || 0; }});
        newTraces.push({{
          type: 'scatter',
          mode: 'lines+markers',
          x: Array.from({{length:24}}, function(_, h) {{ return h; }}),
          y: incYs,
          name: 'Total Incidents',
          line: {{ dash: 'dot', color: '#ffffff', width: 1.5 }},
          marker: {{ size: 4, color: '#ffffff', opacity: 0.8 }},
          opacity: 0.7,
          hovertemplate: 'Total Incidents<br>%{{x:02d}}:00 \u2014 <b>%{{y:,}}</b><extra></extra>',
        }});
      }}

      var lockedLayout = Object.assign({{}}, hourLayout, {{
        yaxis: Object.assign({{}}, hourLayout.yaxis, {{autorange: false, range: [0, maxY * 1.1 || 10]}})
      }});
      Plotly.react('main-chart', newTraces, lockedLayout);
    }}

    // ── Small-multiples modal ───────────────────────────────────────────────
    function openSmallMultiples(group) {{
      var _Tsm      = TRANSLATIONS[currentLang] || TRANSLATIONS.en;
      var _Tsm_isHe = currentLang === 'he';
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
          hovertemplate: '%{{x}}:00 \u2014 %{{y}} ' + (_Tsm.hover_alerts) + '<extra>' + day + '</extra>',
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

      document.getElementById('modal-title').textContent = (_Tsm_isHe ? (REGION_HE[group]||group) : group) + ' \u2014 ' + _Tsm.popup_daily_dist;
      document.getElementById('modal-backdrop').classList.add('open');
      Plotly.newPlot('modal-chart', traces, layout, {{responsive:true}});
    }}

    function closeModal(e) {{
      if (e && e.target !== document.getElementById('modal-backdrop') &&
          e.target !== document.getElementById('modal-close')) return;
      document.getElementById('modal-backdrop').classList.remove('open');
      Plotly.purge('modal-chart');
    }}
    document.addEventListener('keydown', function(e) {{
      if (e.key === 'Escape' && document.getElementById('modal-backdrop').classList.contains('open')) {{
        document.getElementById('modal-backdrop').classList.remove('open');
        Plotly.purge('modal-chart');
      }}
    }});

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
      var regions = Object.keys(Object.assign({{}}, gapMissileHist, gapDroneHist)).sort();
      regions.forEach(function(g) {{
        var o = document.createElement('option');
        o.value = g; o.textContent = g;
        sel.appendChild(o);
      }});
    }})();

    // Populate salvos dropdowns once (derived from hourlyData, Missile alert rows)
    (function() {{
      var missileRows = hourlyData.filter(function(r) {{ return r.alert_type === 'Missile alert'; }});
      var dates   = [...new Set(missileRows.map(function(r) {{ return r.date_str; }}))].sort();
      var regions = [...new Set(missileRows.map(function(r) {{ return r.group;    }}))].sort();

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
      var _Tm = TRANSLATIONS[currentLang] || TRANSLATIONS.en;
      document.getElementById('mismatch-mode-btn').textContent =
        mismatchIsPct ? _Tm.btn_abs_view : _Tm.btn_pct_view;
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

      var _T = TRANSLATIONS[currentLang] || TRANSLATIONS.en;
      var _isHe = currentLang === 'he';
      var evtLabels = {{
        paired_missile: _T.trace_paired_missile,
        paired_drone:   _T.trace_paired_drone,
        pre_only:       _T.trace_pre_only,
        missile_only:   _T.trace_missile_only,
        drone_only:     _T.trace_drone_only,
      }};
      // Bar traces
      var barTraces = EVT_ORDER.map(function(et) {{
        var ys = mismatchIsPct
          ? dates.map(function(_, i) {{
              return totals[i] > 0
                ? parseFloat((daily[et][i] / totals[i] * 100).toFixed(1)) : 0;
            }})
          : daily[et];
        return {{
          type:'bar', x:dates, y:ys, name:evtLabels[et],
          marker:{{color:EVT_COLORS[et]}},
          hovertemplate: mismatchIsPct
            ? '<b>'+evtLabels[et]+'</b><br>%{{x}}: <b>%{{y:.1f}}</b>%<extra></extra>'
            : '<b>'+evtLabels[et]+'</b><br>%{{x}}: <b>%{{y:,}}</b><extra></extra>',
        }};
      }});
      if (mismatchIsPct) {{
        barTraces.push({{
          type:'scatter', x:dates, y:rollRate, name:_T.trace_7d_pct, yaxis:'y2',
          line:{{color:theme.roll, width:2, dash:'dot'}}, mode:'lines',
          hovertemplate:_T.hover_7d_rate + ' <b>%{{y:.1f}}%</b><extra></extra>',
        }});
      }}

      var regionDisplay = region ? (_isHe ? (REGION_HE[region] || region) : region) : _T.lbl_all_regions_title;
      var regionLabel = ' \u2014 ' + regionDisplay;
      var barLayout = {{
        barmode:'stack',
        height: viewH,
        width:  viewW,
        title:{{text: _T.title_mismatch_base + regionLabel + '<br><sup>' + _T.title_mismatch_sub + '</sup>',
                x:0.5, font:{{size:isMobile()?11:14,color:theme.text}}}},
        yaxis:{{title:mismatchIsPct?_T.yaxis_pct_events:_T.yaxis_event_count,
                showgrid:true, gridcolor:theme.grid, zeroline:false, color:theme.text}},
        yaxis2:{{title:_T.yaxis_mismatch_pct, overlaying:'y', side:'right',
                 showgrid:false, zeroline:false, color:'#aaa', range:[0,100],
                 ticksuffix:'%', tickfont:{{size:10}}}},
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
      var _T    = TRANSLATIONS[currentLang] || TRANSLATIONS.en;
      var _isHe = currentLang === 'he';
      var theme = isDark
        ? {{bg:'#1a1a2e', paper:'#0f0f1a', grid:'#2a2a3e', text:'#cccccc', legendBg:'rgba(26,26,46,0.85)',    legendBorder:'#444'}}
        : {{bg:'white',   paper:'#fafafa', grid:'#e0e0e0', text:'#333333', legendBg:'rgba(255,255,255,0.85)', legendBorder:'#ccc'}};

      // Expand histogram → minutes array (only 16 unique values, fast)
      function histToMinutes(hist) {{
        var out = [];
        var src = region ? (hist[region] ? {{[region]: hist[region]}} : {{}}) : hist;
        Object.keys(src).forEach(function(g) {{
          Object.keys(src[g]).forEach(function(gs) {{
            var m = parseInt(gs, 10) / 60;
            for (var i = 0; i < src[g][gs]; i++) out.push(parseFloat(m.toFixed(2)));
          }});
        }});
        return out;
      }}

      var missileMin = histToMinutes(gapMissileHist);
      var droneMin   = histToMinutes(gapDroneHist);

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
          name: _T.trace_paired_missile,
          width: BIN_SIZE,
          offset: 0,
          marker: {{color: '#2ca02c', opacity: 0.75}},
          hovertemplate: '<b>' + _T.trace_paired_missile + '</b><br>%{{x:.1f}}\u2013%{{customdata:.1f}} ' + _T.hover_min + ': <b>%{{y:,}}</b><extra></extra>',
        }});
      }}
      if (droneMin.length) {{
        var db = makeBins(droneMin);
        traces.push({{
          type: 'bar',
          x: db.x,
          y: db.y,
          customdata: db.customdata,
          name: _T.trace_paired_drone,
          width: BIN_SIZE,
          offset: 0,
          marker: {{color: '#17becf', opacity: 0.75}},
          hovertemplate: '<b>' + _T.trace_paired_drone + '</b><br>%{{x:.1f}}\u2013%{{customdata:.1f}} ' + _T.hover_min + ': <b>%{{y:,}}</b><extra></extra>',
        }});
      }}
      if (!traces.length) {{
        traces = [{{type:'scatter', x:[], y:[], showlegend:false}}];
      }}

      var nMissile = missileMin.length;
      var nDrone   = droneMin.length;
      var regionDisplay = region ? (_isHe ? (REGION_HE[region] || region) : region) : '';
      var regionLabel = regionDisplay ? ' \u2014 ' + regionDisplay : '';
      var subtitle = _T.leadtime_paired_only + ' \u00b7 ' + _T.leadtime_bins + ' \u00b7 '
        + nMissile.toLocaleString() + ' ' + _T.leadtime_missile_pairs
        + (nDrone ? ', ' + nDrone.toLocaleString() + ' ' + _T.leadtime_drone_pairs : '');

      var viewEl = document.getElementById('view-leadtime');
      var viewH  = viewEl.offsetHeight || (window.innerHeight - 46);
      var viewW  = viewEl.offsetWidth  || window.innerWidth;

      var layout = {{
        barmode: 'overlay',
        height: viewH,
        width:  viewW,
        title: {{
          text: _T.title_leadtime_base + regionLabel + '<br><sup>' + subtitle + '</sup>',
          x: 0.5, font: {{size: isMobile() ? 12 : 15, color: theme.text}},
        }},
        xaxis: Object.assign({{
          title: _T.xaxis_leadtime,
          dtick: 1,
          showgrid: true, gridcolor: theme.grid,
          zeroline: false, color: theme.text,
        }}, isMobile() ? {{}} : {{range: [0, 15]}}),
        yaxis: {{
          title: _T.yaxis_prealerts,
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
      var _S      = TRANSLATIONS[currentLang] || TRANSLATIONS.en;
      var _S_isHe = currentLang === 'he';
      var textColor = isDark ? '#cccccc' : '#333333';
      var gridColor = isDark ? '#2a2a3e' : '#e0e0e0';
      var paperBg   = isDark ? '#0f0f1a' : '#fafafa';
      var plotBg    = isDark ? '#1a1a2e' : 'white';

      var filtered = hourlyData.filter(function(r) {{
        return r.alert_type === 'Missile alert'
            && (!salvosRegion   || r.group    === salvosRegion)
            && (!salvosDateFrom || r.date_str >= salvosDateFrom)
            && (!salvosDateTo   || r.date_str <= salvosDateTo);
      }});

      if (!filtered.length) {{
        Plotly.react('salvos-chart', [], {{
          paper_bgcolor: paperBg, plot_bgcolor: plotBg, font: {{color: textColor}},
          title: {{text: 'No missile data for selected filters', x: 0.5, font: {{color: textColor}}}},
        }}, {{responsive: true}});
        return;
      }}

      // Group by (date_str, hour), summing count → missiles per hour per day
      var byDateHour = {{}};
      var allDatesSet = {{}};
      filtered.forEach(function(r) {{
        var key = r.date_str + '|' + r.hour;
        byDateHour[key] = (byDateHour[key] || 0) + r.count;
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
          line:   {{color: color, width: 1.8, shape: 'linear'}},
          marker: {{size: 5, color: color}},
          hovertemplate: '<b>' + date + '</b><br>%{{x:02d}}:00 \u2014 <b>%{{y}}</b> ' + (_S.hover_missile_events) + '<extra></extra>',
        }};
      }});

      var _rDisplay = salvosRegion ? (_S_isHe ? (REGION_HE[salvosRegion] || salvosRegion) : salvosRegion) : '';
      var regionNote = _rDisplay ? ' \u00b7 ' + _rDisplay : '';
      var viewEl = document.getElementById('view-salvos');
      var viewH  = viewEl.offsetHeight || (window.innerHeight - 74);
      var viewW  = viewEl.offsetWidth  || window.innerWidth;

      var layout = {{
        height: viewH, width: viewW,
        title: {{
          text: _S.title_salvos_base + regionNote + '<br><sup>' + _S.title_salvos_sub + '</sup>',
          x: 0.5, font: {{size: isMobile() ? 11 : 14, color: textColor}},
        }},
        xaxis: {{
          title: _S.xaxis_hour,
          tickmode: 'array',
          tickvals: [0,2,4,6,8,10,12,14,16,18,20,22],
          ticktext: ['0h','2h','4h','6h','8h','10h','12h','14h','16h','18h','20h','22h'],
          range: [-0.5, 23.5],
          showgrid: true, gridcolor: gridColor,
          zeroline: false, color: textColor,
        }},
        yaxis: {{
          title: _S.yaxis_missile_events_axis,
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

    // Compute "last night" (22:00 yesterday → 06:00 today) and "today"
    // (06:00 today → now) time windows dynamically using current Israel time.
    function computeSituationWindows() {{
      var NIGHT_START = {NIGHT_START}, NIGHT_END = {NIGHT_END};
      var ILtz = 'Asia/Jerusalem';
      var now  = new Date();
      function isoDate(d) {{ return d.toLocaleDateString('en-CA', {{timeZone: ILtz}}); }}
      function pad2(n)    {{ return n < 10 ? '0'+n : ''+n; }}
      var todayStr  = isoDate(now);
      var yesterStr = isoDate(new Date(+now - 86400000));
      var rawH = now.toLocaleString('en-US', {{timeZone: ILtz, hour:'2-digit', hour12:false}});
      var h = parseInt(rawH); if (h === 24) h = 0;
      var m = now.getMinutes();
      return {{
        last_night: {{
          start_iso: yesterStr + 'T' + pad2(NIGHT_START) + ':00:00',
          end_iso:   todayStr  + 'T' + pad2(NIGHT_END)   + ':00:00',
          label: pad2(NIGHT_START) + ':00 yesterday \u2192 ' + pad2(NIGHT_END) + ':00 today',
        }},
        today: {{
          start_iso: todayStr + 'T' + pad2(NIGHT_END) + ':00:00',
          end_iso:   todayStr + 'T' + pad2(h) + ':' + pad2(m) + ':00',
          label: pad2(NIGHT_END) + ':00 today \u2192 now',
        }},
      }};
    }}

    // Merge fresh rows from situation.json into hourlyData, replacing any
    // existing rows for the same (date_str, hour) pairs.
    function mergeFreshRows(freshRows) {{
      if (!freshRows || !freshRows.length) return;
      var freshKeys = {{}};
      freshRows.forEach(function(r) {{ freshKeys[r.date_str + '|' + r.hour] = true; }});
      hourlyData = hourlyData.filter(function(r) {{ return !freshKeys[r.date_str + '|' + r.hour]; }});
      hourlyData = hourlyData.concat(freshRows);
    }}

    // Fetch situation.json (updated every 30 min), merge fresh rows, then render.
    function initSituationRoom() {{
      var done = function() {{ buildSituationView(); }};
      fetch('situation.json?t=' + Date.now())
        .then(function(r) {{ return r.ok ? r.json() : Promise.resolve(null); }})
        .then(function(fresh) {{
          if (fresh && fresh.rows) {{
            mergeFreshRows(fresh.rows);
            if (fresh.fetched_at) fetchedAt = fresh.fetched_at;
          }}
          done();
        }})
        .catch(function() {{ done(); }});
    }}

    // ── Situation Room timeline helpers ─────────────────────────────────────
    function buildTimelineHTML(d, sectionTitle) {{
      // Build (date_str, hour) pairs covered by this period
      var startDate = (d.start_iso || '').slice(0, 10);
      var startHour = parseInt((d.start_iso || '00').slice(11, 13)) || 0;
      var endDate   = (d.end_iso   || '').slice(0, 10);
      var endHour   = parseInt((d.end_iso   || '00').slice(11, 13)) || 0;

      var pairs = [];
      if (startDate === endDate) {{
        for (var h = startHour; h < endHour; h++) pairs.push({{ds: startDate, h: h}});
      }} else {{
        for (var h = startHour; h <= 23; h++) pairs.push({{ds: startDate, h: h}});
        for (var h2 = 0; h2 < endHour; h2++) pairs.push({{ds: endDate, h: h2}});
      }}

      // Badge counts from incidentData (globally deduplicated); dots from hourlyData
      var rows = [];
      pairs.forEach(function(pair) {{
        var missile = 0, pre = 0, drone = 0, regions = {{}};
        incidentData.forEach(function(r) {{
          if (r.date_str !== pair.ds || r.hour !== pair.h) return;
          if      (r.alert_type === 'Missile alert') missile += r.count;
          else if (r.alert_type === 'Pre-alert')     pre     += r.count;
          else if (r.alert_type === 'Drone alert')   drone   += r.count;
        }});
        hourlyData.forEach(function(r) {{
          if (r.date_str !== pair.ds || r.hour !== pair.h) return;
          regions[r.group] = (regions[r.group] || 0) + r.count;
        }});
        if (missile + pre + drone > 0)
          rows.push({{ds: pair.ds, h: pair.h, missile: missile, pre: pre, drone: drone, regions: regions}});
      }});

      if (!rows.length) return '<div class="sit-quiet">' + (TRANSLATIONS[currentLang] || TRANSLATIONS.en).sit_quiet + '</div>';

      var html = '<div class="sit-timeline">';
      rows.forEach(function(row) {{
        var timeStr = ('0' + row.h).slice(-2) + ':00';
        html += '<div class="sit-tl-row" data-ds="' + row.ds + '" data-h="' + row.h + '" data-sect="' + sectionTitle + '" onclick="openHourModal(this.dataset.ds,+this.dataset.h,this.dataset.sect)">';
        html += '<span class="sit-tl-time">' + timeStr + '</span>';
        html += '<div class="sit-tl-types">';
        if (row.pre)     html += '<span class="sit-badge sit-badge-pre"     title="pre-alert incidents">\u26A1\u202F'    + row.pre     + '</span>';
        if (row.missile) html += '<span class="sit-badge sit-badge-missile" title="missile incidents">🚀\u202F'           + row.missile + '</span>';
        if (row.drone)   html += '<span class="sit-badge sit-badge-drone"   title="drone incidents">🛩\u202F'             + row.drone   + '</span>';
        html += '</div>';
        html += '<div class="sit-tl-regions">';
        // Sort regions by count desc, show coloured dots
        Object.keys(row.regions).sort(function(a, b) {{ return row.regions[b] - row.regions[a]; }}).forEach(function(grp) {{
          var c = groupColors[grp] || '#888';
          html += '<span class="sit-tl-dot" style="background:' + c + '" title="' + grp + '"></span>';
        }});
        html += '</div></div>';
      }});
      html += '</div>';
      return html;
    }}

    function openHourModal(dateStr, hour, sectionLabel) {{
      var _Thm      = TRANSLATIONS[currentLang] || TRANSLATIONS.en;
      var _Thm_isHe = currentLang === 'he';
      var rows = hourlyData.filter(function(r) {{ return r.date_str === dateStr && r.hour === hour; }});
      var gd = {{}};
      rows.forEach(function(r) {{
        if (!gd[r.group]) gd[r.group] = {{missile:0, pre:0, drone:0}};
        if      (r.alert_type === 'Missile alert') gd[r.group].missile += r.count;
        else if (r.alert_type === 'Pre-alert')     gd[r.group].pre     += r.count;
        else if (r.alert_type === 'Drone alert')   gd[r.group].drone   += r.count;
      }});
      var groups = Object.keys(gd).sort(function(a, b) {{
        return (gd[b].missile + gd[b].pre + gd[b].drone) - (gd[a].missile + gd[a].pre + gd[a].drone);
      }});
      var groupsX = groups.map(function(g) {{ return _Thm_isHe ? (REGION_HE[g] || g) : g; }});

      // Cross-region overlap note
      var hourEvents = rows.reduce(function(s, r) {{ return s + (+r.count || 0); }}, 0);
      var hourIncidents = incidentData
        .filter(function(r) {{ return r.date_str === dateStr && r.hour === hour; }})
        .reduce(function(s, r) {{ return s + (+r.count || 0); }}, 0);
      var multiRegion = hourEvents - hourIncidents;
      var incLabel = hourIncidents === 1 ? 'incident' : 'incidents';
      var overlapNote = multiRegion > 0
        ? hourIncidents + ' ' + incLabel + ' \u00b7 ' + hourEvents + ' region events (' + multiRegion + ' cross-region)'
        : hourIncidents + ' ' + incLabel;

      var textColor = isDark ? '#cccccc' : '#333333';
      var plotBg    = isDark ? '#1a1a2e' : 'white';
      var paperBg   = isDark ? '#0f0f1a' : '#fafafa';
      var gridColor = isDark ? '#2a2a3e' : '#e0e0e0';

      var traces = [
        {{ name: _Thm.hover_pre_alert,     type:'bar', x:groupsX, y:groups.map(function(g){{return gd[g].pre;}}),     marker:{{color:'#ff7f0e'}} }},
        {{ name: _Thm.hover_missile_alert, type:'bar', x:groupsX, y:groups.map(function(g){{return gd[g].missile;}}), marker:{{color:'#d62728'}} }},
        {{ name: _Thm.hover_drone_alert,   type:'bar', x:groupsX, y:groups.map(function(g){{return gd[g].drone;}}),   marker:{{color:'#17becf'}} }},
      ];
      var layout = {{
        barmode: 'stack',
        title: {{ text: ('0'+hour).slice(-2)+':00 \u00b7 '+dateStr+'<br><sup>'+overlapNote+'</sup>', font:{{color:textColor, size:13}}, x:0.5 }},
        xaxis: {{ tickangle: -30, color: textColor, tickfont: {{size: 10}} }},
        yaxis: {{ title: _Thm.yaxis_alert_events, color: textColor, gridcolor: gridColor, zeroline: false }},
        plot_bgcolor: plotBg, paper_bgcolor: paperBg,
        font: {{ family: 'Arial, Helvetica, sans-serif', color: textColor }},
        margin: {{t:65, b:90, l:50, r:20}},
        legend: {{ font: {{color: textColor}} }},
      }};

      document.getElementById('modal-title').textContent = ('0'+hour).slice(-2)+':00 \u2014 '+sectionLabel;
      document.getElementById('modal-chart').style.height = '340px';
      document.getElementById('modal-backdrop').classList.add('open');
      Plotly.react('modal-chart', traces, layout, {{responsive: true}});
    }}

    // ── Incident count helper for a time window ────────────────────────────
    function periodCounts(w) {{
      var startDate = (w.start_iso || '').slice(0, 10);
      var startHour = parseInt((w.start_iso || '00').slice(11, 13)) || 0;
      var endDate   = (w.end_iso   || '').slice(0, 10);
      var endHour   = parseInt((w.end_iso   || '00').slice(11, 13)) || 0;
      function inWindow(ds, h) {{
        if (ds < startDate || ds > endDate) return false;
        if (ds === startDate && h < startHour) return false;
        if (ds === endDate   && h >= endHour)  return false;
        return true;
      }}
      // Sum events per (ds, h, atype) cell for cross-referencing
      var eventsByCell = {{}};
      hourlyData.forEach(function(r) {{
        if (!inWindow(r.date_str, +r.hour)) return;
        var k = r.date_str + '|' + r.hour + '|' + r.alert_type;
        eventsByCell[k] = (eventsByCell[k] || 0) + (+r.count || 0);
      }});
      var events = Object.keys(eventsByCell).reduce(function(s, k) {{ return s + eventsByCell[k]; }}, 0);

      var incidents = 0, multiRegionEvents = 0, multiRegionIncidents = 0;
      incidentData.forEach(function(r) {{
        if (!inWindow(r.date_str, +r.hour)) return;
        var cnt = +r.count || 0;
        incidents += cnt;
        var k = r.date_str + '|' + r.hour + '|' + r.alert_type;
        var evts = eventsByCell[k] || 0;
        if (evts > cnt) {{
          multiRegionEvents     += evts - cnt;  // extra events beyond 1-per-incident
          multiRegionIncidents  += cnt;          // all incidents in this cell are multi-region context
        }}
      }});
      return {{ events: events, incidents: incidents,
                multiRegionEvents: multiRegionEvents, multiRegionIncidents: multiRegionIncidents }};
    }}

    function buildSituationView() {{
      var el = document.getElementById('situation-content');
      if (!el) return;
      var T = TRANSLATIONS[currentLang] || TRANSLATIONS.en;
      var titles = {{ last_night: T.sit_lastnight_title, today: T.sit_today_title }};
      var ILtz = 'Asia/Jerusalem';
      var html = '';

      var windows = computeSituationWindows();
      ['last_night', 'today'].forEach(function(key) {{
        var w = windows[key];
        html += '<div class="sit-section">';
        html += '<div class="sit-section-title">' + titles[key] + '</div>';
        var sublabel = w.label;
        if (key === 'today') {{
          var nowIL = new Date().toLocaleTimeString('en-GB', {{hour:'2-digit',minute:'2-digit',timeZone:ILtz}});
          sublabel += ' (' + nowIL + ' Israel time)';
        }}
        html += '<div class="sit-sublabel">' + sublabel + '</div>';

        var pc = periodCounts(w);
        if (pc.events > 0) {{
          var evtStr  = pc.events.toLocaleString();
          var incStr  = pc.incidents.toLocaleString();
          var ePart   = pc.multiRegionEvents     > 0 ? evtStr + ' events (' + pc.multiRegionEvents.toLocaleString()     + ' multi-region)' : evtStr + ' events';
          var iPart   = pc.multiRegionIncidents  > 0 ? incStr + ' incidents (' + pc.multiRegionIncidents.toLocaleString() + ' multi-region)' : incStr + ' incidents';
          html += '<div class="sit-summary" style="margin-bottom:8px;font-size:12px;">' + ePart + ' \u00b7 ' + iPart + '</div>';
        }}

        html += buildTimelineHTML(w, titles[key]);
        html += '</div>';
      }});

      html += '<div class="sit-explainer">' + T.explainer_situation + '</div>';
      el.innerHTML = html;
    }}

    // ── Dark / light toggle ─────────────────────────────────────────────────
    function toggleTheme() {{
      isDark = !isDark;
      document.body.classList.toggle('dark',  isDark);
      document.body.classList.toggle('light', !isDark);
      var T = TRANSLATIONS[currentLang] || TRANSLATIONS.en;
      document.getElementById('theme-btn').textContent = isDark ? T.btn_light : T.btn_dark;
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

    // ── Language toggle ──────────────────────────────────────────────────────
    function toggleLang() {{
      setLang(currentLang === 'en' ? 'he' : 'en');
    }}

    function setLang(lang) {{
      currentLang = lang;
      try {{ localStorage.setItem('lang', lang); }} catch(e) {{}}
      var T = TRANSLATIONS[lang] || TRANSLATIONS.en;
      var isHe = lang === 'he';
      document.documentElement.setAttribute('dir',  isHe ? 'rtl' : 'ltr');
      document.documentElement.setAttribute('lang', lang);

      // RTL nav layout (explicit JS override — CSS :root[dir=rtl] may race on first paint)
      document.getElementById('nav-row').style.flexDirection  = isHe ? 'row' : '';
      document.getElementById('nav-tabs').style.flexDirection = isHe ? 'row' : '';
      var _fr = document.getElementById('filter-row');
      if (_fr) _fr.style.flexDirection = isHe ? 'row' : '';

      // Tab buttons (preserve leading icon)
      var tabIcons = {{ situation: '&#9889;', hour: '&#9200;', date: '&#128197;', mismatch: '&#9888;&#65039;', leadtime: '&#9203;', salvos: '&#128165;' }};
      document.getElementById('btn-situation').innerHTML = tabIcons.situation + '&nbsp;' + T.tab_situation;
      document.getElementById('btn-hour').innerHTML      = tabIcons.hour      + '&nbsp;' + T.tab_hour;
      document.getElementById('btn-date').innerHTML      = tabIcons.date      + '&nbsp;' + T.tab_date;
      document.getElementById('btn-mismatch').innerHTML  = tabIcons.mismatch  + '&nbsp;' + T.tab_mismatch;
      document.getElementById('btn-leadtime').innerHTML  = tabIcons.leadtime  + '&nbsp;' + T.tab_leadtime;
      document.getElementById('btn-salvos').innerHTML    = tabIcons.salvos    + '&nbsp;' + T.tab_salvos;

      // data-i18n text elements (From:, To:, Pre-alert, Missile & Drone, All regions options)
      document.querySelectorAll('[data-i18n]').forEach(function(el) {{
        var key = el.getAttribute('data-i18n');
        if (T[key] !== undefined) el.textContent = T[key];
      }});

      // data-i18n-html explainer divs
      document.querySelectorAll('[data-i18n-html]').forEach(function(el) {{
        var key = el.getAttribute('data-i18n-html');
        if (T[key] !== undefined) el.innerHTML = T[key];
      }});

      // Region selects: translate option labels (keep English values for filter logic)
      ['hour-region-select','mismatch-region-select','leadtime-region-select','salvos-region-select'].forEach(function(selId) {{
        var sel = document.getElementById(selId);
        if (!sel) return;
        Array.from(sel.options).forEach(function(opt) {{
          if (opt.value === '') {{
            opt.textContent = T.lbl_all_regions;
          }} else {{
            opt.textContent = isHe ? (REGION_HE[opt.value] || opt.value) : opt.value;
          }}
        }});
      }});

      // Theme and lang buttons
      document.getElementById('theme-btn').textContent = isDark ? T.btn_light : T.btn_dark;
      document.getElementById('lang-btn').textContent  = T.btn_lang;

      // Mismatch mode button (reflects current state)
      var _mmb = document.getElementById('mismatch-mode-btn');
      if (_mmb) _mmb.textContent = mismatchIsPct ? T.btn_abs_view : T.btn_pct_view;

      // Hamburger label
      var hl = document.getElementById('hamburger-label');
      if (hl) {{ hl.textContent = T['tab_' + currentView] || VIEW_LABELS[currentView]; }}

      // VIEW_LABELS sync (used by setView for hamburger label on tab switch)
      VIEW_LABELS.situation = T.tab_situation;
      VIEW_LABELS.hour      = T.tab_hour;
      VIEW_LABELS.date      = T.tab_date;
      VIEW_LABELS.mismatch  = T.tab_mismatch;
      VIEW_LABELS.leadtime  = T.tab_leadtime;
      VIEW_LABELS.salvos    = T.tab_salvos;

      // Translate By Hour axis labels + title (mutate hourLayout so updateHourChart and setView pick them up)
      if (hourLayout.title) hourLayout.title.text = T.title_hour;
      if (hourLayout.xaxis) hourLayout.xaxis = Object.assign({{}}, hourLayout.xaxis, {{title: T.xaxis_hour}});
      if (hourLayout.yaxis) hourLayout.yaxis = Object.assign({{}}, hourLayout.yaxis, {{title: T.yaxis_hour}});
      if (hourLayout.legend && hourLayout.legend.title) hourLayout.legend.title.text = T.legend_region;
      updateHourChart();

      // Translate By Date title, axis labels, hovertemplates, and end-of-line region labels
      // Mutate dateLayout so setView('date') re-renders with the correct values
      if (dateLayout.title) dateLayout.title.text = T.title_date;
      if (dateLayout.xaxis) dateLayout.xaxis = Object.assign({{}}, dateLayout.xaxis, {{title: T.xaxis_date}});
      if (dateLayout.yaxis) dateLayout.yaxis = Object.assign({{}}, dateLayout.yaxis, {{title: T.yaxis_date_cumul}});
      try {{
        var _dateEl = document.getElementById('date-full-chart');
        if (_dateEl && _dateEl.data && _dateEl.data.length) {{
          var _newHTs   = dateData.map(function(tr) {{
            var _disp = isHe ? (REGION_HE[tr.name] || tr.name) : tr.name;
            return '<b>' + _disp + '</b><br>%{{x}}<br>' + T.hover_cumulative + ': <b>%{{customdata[1]:,}}</b> (+%{{customdata[0]:,}})<extra></extra>';
          }});
          var _newTexts = dateData.map(function(tr) {{
            var _disp = isHe ? (REGION_HE[tr.name] || tr.name) : tr.name;
            var _n = (tr.x || []).length;
            return Array(_n > 1 ? _n - 1 : 0).fill('').concat(_n > 0 ? [_disp] : []);
          }});
          Plotly.restyle('date-full-chart', {{hovertemplate: _newHTs, text: _newTexts}});
          Plotly.relayout('date-full-chart', {{'title.text': T.title_date, 'xaxis.title': T.xaxis_date, 'yaxis.title': T.yaxis_date_cumul}});
        }}
      }} catch(e) {{}}

      // Translate "All" option in date From/To selectors
      ['hour-date-from','hour-date-to','date-view-from','date-view-to','salvos-date-from','salvos-date-to'].forEach(function(selId) {{
        var _ds = document.getElementById(selId);
        if (!_ds) return;
        var _allOpt = _ds.querySelector('option[value=""]');
        if (_allOpt) _allOpt.textContent = T.lbl_all_dates;
      }});

      // Rebuild dynamic views
      if (currentView === 'situation') buildSituationView();
      buildMismatchCharts(mismatchRegion);
      buildLeadTimeChart(leadtimeRegion);
      buildSalvosChart();
    }}

    // Init language from localStorage
    (function() {{
      var saved = 'en';
      try {{ saved = localStorage.getItem('lang') || 'en'; }} catch(e) {{}}
      if (saved === 'he') setLang('he');
    }})();
  </script>
  <footer id="global-footer">
    <span id="global-footer-text"></span>
  </footer>
  <script>
    (function() {{
      var el = document.getElementById('global-footer-text');
      if (!el) return;
      var ILtz = 'Asia/Jerusalem';
      var parts = [];
      try {{
        var now = new Date();
        var d    = new Date(fetchedAt);
        var dStr = d.toLocaleDateString('en-GB', {{day:'2-digit', month:'long', year:'numeric', timeZone:ILtz}});
        var tStr = d.toLocaleTimeString('en-GB', {{hour:'2-digit', minute:'2-digit', timeZone:ILtz}});
        var nextRefresh = new Date(Math.ceil(now.getTime() / 1800000) * 1800000);
        var nextIL = nextRefresh.toLocaleTimeString('en-GB', {{hour:'2-digit', minute:'2-digit', timeZone:ILtz}});
        parts.push('Data fetched ' + dStr + ' at ' + tStr + ' Israel time (next refresh at ' + nextIL + ' Israel time)');
      }} catch(e) {{}}
      parts.push('Built by Ira and Natan Skop with some help from Claude Code');
      el.textContent = parts.join(' \u00b7 ');
    }})();
  </script>
</body>
</html>"""

    OUTPUT_DIR.mkdir(exist_ok=True)

    # Generate OG preview image (By Hour chart snapshot for social sharing)
    try:
        preview_path = OUTPUT_DIR / "preview.png"
        hour_fig.write_image(str(preview_path), width=1200, height=630, scale=1)
        print(f"  Preview image saved → {preview_path}")
    except Exception as _e:
        print(f"  Preview image skipped ({_e})")

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
    from datetime import timezone as _tz
    situation_only = "--situation-only" in sys.argv

    # 1. City → zone mapping
    city_to_zone, _ = load_city_data()

    # 2. Load existing processed state (provides incremental cutoff fetched_at)
    existing = load_processed()

    # 3. Check if source CSV has changed since last full build
    print("\nChecking source CSV for changes …")
    current_sha = fetch_csv_sha()
    if existing and current_sha and current_sha == existing.get("csv_sha"):
        print("  CSV unchanged — skipping download.")
        # Rebuild HTML from cached data (situation data is time-sensitive, always fresh)
        fetched_at     = existing["fetched_at"]
        chart_df       = pd.DataFrame(existing["chart_df"])
        mismatch_agg   = existing.get("mismatch_agg", [])
        gap_missile_h  = existing.get("gap_missile_hist", {})
        gap_drone_h    = existing.get("gap_drone_hist", {})
        partial_day    = existing.get("partial_day")
        partial_hour   = existing.get("partial_hour")

        save_situation_json(chart_df, fetched_at)
        if situation_only:
            print("\nDone (situation-only, CSV unchanged).  output/situation.json updated.")
            return

        situation_data = compute_situation(chart_df)
        build_chart(chart_df, mismatch_agg, gap_missile_h, gap_drone_h,
                    partial_day=partial_day, partial_hour=partial_hour,
                    situation_data=situation_data, fetched_at=fetched_at)
        print("\nDone.  Open output/index.html in your browser.")
        return

    # 4. Download CSV — incremental if we have a prior full-build timestamp.
    # Fetch from (since_dt - MISMATCH_LOOKBACK) so that compute_mismatches() can
    # re-examine the boundary window and catch Pre-alert / Missile pairs that
    # straddled two consecutive runs.
    since_dt = pd.Timestamp(existing["fetched_at"]) if existing else None
    mismatch_since_dt = (since_dt - MISMATCH_LOOKBACK) if since_dt is not None else None
    raw = fetch_github_csv(since_dt=mismatch_since_dt)
    if raw is not None:
        df_extended = _normalise_df(raw)
        # Chart aggregation uses only strictly new rows (same behaviour as before)
        if since_dt is not None:
            _since_naive = since_dt.tz_convert(None) if since_dt.tzinfo else since_dt
            df = df_extended[df_extended["dt"] > _since_naive].copy()
        else:
            df = df_extended
    else:
        data_file = find_data_file()
        if data_file is None:
            print("\nNo data found. Place an .xlsx/.csv in data/ or check network.")
            sys.exit(1)
        df = load_alerts(data_file)
        df_extended = df

    # 5. Aggregate new rows
    print("\nAggregating alerts by zone …")
    zone_total, zone_night, new_chart_df, new_incident_df = aggregate(df, city_to_zone)
    total_alerts = sum(zone_total.values())
    total_night  = sum(zone_night.values())
    print(f"  Deduplicated alert events : {total_alerts:,}")
    print(f"  Of which at night         : {total_night:,}  "
          f"({round(total_night / total_alerts * 100, 1) if total_alerts else 0}%)")

    # Merge with historical chart_df from prior build
    chart_df    = merge_chart_df(existing["chart_df"] if existing else [], new_chart_df)
    incident_df = merge_incident_df(existing.get("incident_df", []) if existing else [], new_incident_df)

    # 5b. Detect partial day from the merged chart_df
    last_date    = chart_df["date_str"].dropna().max() if not chart_df.empty else None
    today_str    = date.today().isoformat()
    is_partial   = last_date == today_str
    partial_day  = last_date if is_partial else None
    try:
        from zoneinfo import ZoneInfo as _ZoneInfo
        _il_now = datetime.now(tz=_ZoneInfo("Asia/Jerusalem"))
    except ImportError:
        _il_now = datetime.now(_tz.utc)  # UTC fallback if zoneinfo unavailable
    partial_hour = _il_now.hour if is_partial else None
    if partial_day:
        print(f"  Partial day detected: {partial_day} — fetched at hour {partial_hour:02d}:xx")

    fetched_at = datetime.now(_tz.utc).isoformat()

    # In situation-only mode: save situation.json and exit without the full chart build.
    if situation_only:
        save_situation_json(chart_df, fetched_at)
        print("\nDone (situation-only).  output/situation.json updated.")
        return

    # 6. Summary table
    print_summary(zone_total, zone_night)

    # 7. Pre-alert / missile mismatch analysis.
    # Uses df_extended (lookback + new rows) so pairs that straddle two consecutive
    # incremental batches are correctly matched.
    print("\nComputing pre-alert / missile mismatches …")
    new_mismatch_df = compute_mismatches(df_extended, city_to_zone)
    if not new_mismatch_df.empty:
        counts = new_mismatch_df["event_type"].value_counts()
        for et in ALL_EVENT_TYPES:
            print(f"  {et:<16}: {counts.get(et, 0):,}")
        OUTPUT_DIR.mkdir(exist_ok=True)
        xlsx_path = OUTPUT_DIR / "mismatches.xlsx"
        new_mismatch_df.to_excel(xlsx_path, index=False)
        print(f"  Saved → {xlsx_path}")

    # Calendar dates covered by the lookback window; existing mismatch_agg records for
    # those dates are replaced rather than summed (avoids double-counting re-examined events).
    if since_dt is not None and mismatch_since_dt is not None:
        _lb_start = mismatch_since_dt.date() if hasattr(mismatch_since_dt, "date") else mismatch_since_dt.to_pydatetime().date()
        _lb_end   = since_dt.date()          if hasattr(since_dt, "date")          else since_dt.to_pydatetime().date()
        lookback_dates: set = set()
        _d = _lb_start
        while _d <= _lb_end:
            lookback_dates.add(_d.isoformat())
            _d += timedelta(days=1)
    else:
        lookback_dates = None

    mismatch_agg, gap_missile_h, gap_drone_h = merge_mismatch(
        existing.get("mismatch_agg", []) if existing else [],
        existing.get("gap_missile_hist", {}) if existing else {},
        existing.get("gap_drone_hist", {}) if existing else {},
        new_mismatch_df,
        lookback_dates=lookback_dates,
    )

    # 8. Save processed data (incremental)
    save_processed(chart_df, mismatch_agg, gap_missile_h, gap_drone_h,
                   partial_day, partial_hour,
                   incident_df=incident_df,
                   fetched_at=fetched_at, csv_sha=current_sha)

    # 9. Save situation.json (small file fetched client-side on every page load)
    save_situation_json(chart_df, fetched_at)

    # 10. Situation Room summary (time-sensitive, computed fresh each run)
    situation_data = compute_situation(chart_df)

    # 11. Build chart
    build_chart(chart_df, mismatch_agg, gap_missile_h, gap_drone_h,
                partial_day=partial_day, partial_hour=partial_hour,
                situation_data=situation_data, fetched_at=fetched_at,
                incident_df=incident_df)
    print("\nDone.  Open output/index.html in your browser.")


if __name__ == "__main__":
    main()
