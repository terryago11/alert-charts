"""Data loading and serialisation utilities.

Covers city-to-zone mapping, GitHub CSV fetching, local file loading,
DataFrame normalisation, and processed-data persistence (processed.json).
"""

import json
import re
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
import requests

# ── Constants ─────────────────────────────────────────────────────────────────

CITIES_JSON_URL = (
    "https://raw.githubusercontent.com/eladnava/pikud-haoref-api/master/cities.json"
)

GITHUB_CSV_URL      = "https://raw.githubusercontent.com/dleshem/israel-alerts-data/main/israel-alerts.csv"
GITHUB_CONTENTS_API = "https://api.github.com/repos/dleshem/israel-alerts-data/contents/israel-alerts.csv"
_cutoff_env = __import__("os").environ.get("ALERT_CUTOFF_DATE")
CUTOFF_DATE = pd.Timestamp(_cutoff_env) if _cutoff_env else pd.Timestamp("2026-02-28")
PROCESSED_SCHEMA_VERSION = 5

ALERT_TRANSLATIONS = {
    "בדקות הקרובות צפויות להתקבל התרעות באזורך": "Pre-alert",
    "חדירת כלי טיס עוין":                         "Drone alert",
    "ירי רקטות וטילים":                            "Missile alert",
    "האירוע הסתיים":                               "All clear",
    "ירי רקטות וטילים - האירוע הסתיים":           "All clear",
    "חדירת כלי טיס עוין - האירוע הסתיים":         "All clear",
}

DATA_DIR    = Path("data")

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

