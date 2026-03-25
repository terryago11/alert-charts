#!/usr/bin/env python3
"""
Israeli Homefront Command – Night Alert Bubble Chart
=====================================================
Fetches alert history from a Google Sheet (set SHEETS_URL below), maps each
city to its official Homefront Command zone, counts nighttime alerts
(NIGHT_START – NIGHT_END) per zone, and saves an interactive geographic
bubble chart to output/night_alerts.html.

Quick start
-----------
1. Share your Google Sheet:  File → Share → "Anyone with the link" → Viewer
2. pip install -r requirements.txt
3. python main.py
4. Open output/night_alerts.html

Fallback
--------
If the sheet is not publicly accessible, export it as .xlsx / .csv,
drop it in the data/ directory, and re-run.
"""

import re
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import requests

from regions import GROUP_COLORS, NIGHT_END, NIGHT_START, ZONE_GROUP

# ── Constants ─────────────────────────────────────────────────────────────────

CITIES_JSON_URL = (
    "https://raw.githubusercontent.com/eladnava/pikud-haoref-api/master/cities.json"
)

# Google Sheet ID – set this to your spreadsheet ID and the script will fetch
# the data automatically (requires the sheet to be shared "Anyone with link").
# Leave as None to fall back to a local file in data/.
SHEETS_ID = "14yX1jocodglfqioKvnRaR9oHJ1ppHcb2-8mMZxtKVN8"
SHEETS_GID = 0   # tab/sheet index (0 = first tab)

DATA_DIR   = Path("data")
OUTPUT_DIR = Path("output")


# ── City / Zone helpers ───────────────────────────────────────────────────────

def load_city_data() -> tuple[dict, dict]:
    """
    Fetch cities.json from pikud-haoref-api and return:
      city_to_zone  : {hebrew_city_name: zone_en}
      zone_centroid : {zone_en: (avg_lat, avg_lon)}
    """
    print("Fetching city→zone mapping from pikud-haoref-api …")
    resp = requests.get(CITIES_JSON_URL, timeout=30)
    resp.raise_for_status()
    cities = resp.json()

    city_to_zone: dict[str, str] = {}
    zone_coords: dict[str, list] = defaultdict(list)

    for entry in cities:
        name = (entry.get("name") or entry.get("value") or "").strip()
        zone_en = (entry.get("zone_en") or "").strip()
        lat = entry.get("lat")
        lng = entry.get("lng")

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

    mapped_zones = len(set(city_to_zone.values()))
    print(f"  {len(city_to_zone):,} cities mapped across {mapped_zones} zones.")
    return city_to_zone, zone_centroid


# ── Alert data loading ────────────────────────────────────────────────────────

def fetch_sheet() -> pd.DataFrame | None:
    """
    Try to download the configured Google Sheet as CSV.
    Returns a DataFrame on success, or None if the sheet is not publicly
    accessible (in which case we fall back to a local file).
    """
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
                "  → In Google Sheets: File → Share → 'Anyone with the link' → Viewer\n"
                "  → Or export as .xlsx and place in data/"
            )
            return None
        from io import StringIO
        df = pd.read_csv(StringIO(resp.text), dtype=str)
        print(f"  Loaded {len(df):,} rows from Google Sheet.")
        return df
    except requests.RequestException as exc:
        print(f"  Could not reach Google Sheet: {exc}")
        return None


def find_data_file() -> Path | None:
    """Return the first .xlsx / .xls / .csv file found in data/."""
    for pattern in ("*.xlsx", "*.xls", "*.csv"):
        for p in sorted(DATA_DIR.glob(pattern)):
            return p
    return None


def _parse_hour(value) -> int | None:
    """Best-effort extraction of an hour (0–23) from various date/time types."""
    if value is None:
        return None
    try:
        # pandas Timestamp / datetime
        return int(pd.Timestamp(value).hour)
    except Exception:
        pass
    s = str(value).strip()
    # "HH:MM" or "HH:MM:SS"
    m = re.match(r"^(\d{1,2}):", s)
    if m:
        return int(m.group(1))
    # ISO-ish "2023-10-07 06:29" or "2023-10-07T06:29"
    m = re.search(r"[T ](\d{2}):\d{2}", s)
    if m:
        return int(m.group(1))
    return None


def _detect_column(df: pd.DataFrame, keywords: list[str]) -> str | None:
    """Find the first column whose name contains any of the given keywords."""
    for col in df.columns:
        col_lower = str(col).lower()
        if any(kw in col_lower for kw in keywords):
            return col
    return None


def _normalise_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Given any raw alert DataFrame, detect the city and time columns and
    return a normalised DataFrame with just:
      'city_raw'  – raw city/area string (may contain multiple cities)
      'hour'      – integer hour (0–23) or None
    """
    print(f"  {len(df):,} rows  |  columns: {list(df.columns)}")

    # ── Detect city column ────────────────────────────────────────────────
    city_keywords = ["city", "cities", "area", "areas", "location",
                     "data", "ערים", "עיר", "אזור", "מיקום"]
    city_col = _detect_column(df, city_keywords)
    if city_col is None:
        for col in df.columns:
            if df[col].dtype == object:
                city_col = col
                break
    if city_col is None:
        raise ValueError("Could not detect a city/area column in the data.")
    print(f"  Using city column: '{city_col}'")

    # ── Detect datetime / time column ────────────────────────────────────
    dt_col   = _detect_column(df, ["datetime", "timestamp", "alertdate", "alert_date"])
    time_col = _detect_column(df, ["time", "שעה", "hour"]) if dt_col is None else None
    date_col = _detect_column(df, ["date", "תאריך"])       if dt_col is None else None

    if dt_col:
        print(f"  Using datetime column: '{dt_col}'")
        hours = [_parse_hour(v) for v in df[dt_col]]
    elif time_col:
        print(f"  Using time column: '{time_col}'")
        hours = [_parse_hour(v) for v in df[time_col]]
    elif date_col:
        print(f"  Using date column: '{date_col}' (no separate time column found)")
        hours = [_parse_hour(v) for v in df[date_col]]
    else:
        print("  WARNING: No date/time column detected – cannot filter by hour.")
        hours = [None] * len(df)

    return pd.DataFrame({"city_raw": df[city_col].astype(str), "hour": hours})


def load_alerts(filepath: Path) -> pd.DataFrame:
    """Load an alert export file (.xlsx / .xls / .csv) and normalise it."""
    print(f"Loading {filepath} …")
    if filepath.suffix.lower() == ".csv":
        raw = pd.read_csv(filepath, dtype=str)
    else:
        raw = pd.read_excel(filepath, dtype=str)
    return _normalise_df(raw)


# ── Aggregation ───────────────────────────────────────────────────────────────

def is_night(hour: int | None) -> bool:
    if hour is None:
        return False
    return hour >= NIGHT_START or hour < NIGHT_END


def aggregate(df: pd.DataFrame, city_to_zone: dict) -> tuple[dict, dict]:
    """
    Walk every row.  A cell may hold a single city name or several separated
    by commas / semicolons / pipes / newlines.

    Returns (zone_total, zone_night) – both dicts of {zone_en: count}.
    """
    zone_total: dict[str, int] = defaultdict(int)
    zone_night: dict[str, int] = defaultdict(int)
    unmatched: set[str] = set()

    for _, row in df.iterrows():
        raw  = str(row["city_raw"]).strip()
        hour = row["hour"]

        # Split on common separators
        cities = [c.strip() for c in re.split(r"[,،;|\n]+", raw) if c.strip()]

        for city in cities:
            zone = city_to_zone.get(city)
            if zone:
                zone_total[zone] += 1
                if is_night(hour):
                    zone_night[zone] += 1
            else:
                unmatched.add(city)

    if unmatched:
        sample = sorted(unmatched)[:15]
        print(
            f"\n  Note: {len(unmatched)} unmatched city names "
            f"(first 15): {sample}"
        )

    return dict(zone_total), dict(zone_night)


# ── Chart ─────────────────────────────────────────────────────────────────────

def build_chart(
    zone_total: dict,
    zone_night: dict,
    zone_centroid: dict,
) -> None:
    """Create and save an interactive Plotly geographic bubble chart."""

    # Build a flat table of all zones we have data for
    rows = []
    for zone, total in zone_total.items():
        night = zone_night.get(zone, 0)
        pct   = round(night / total * 100, 1) if total > 0 else 0.0
        group = ZONE_GROUP.get(zone, "Other")
        centroid = zone_centroid.get(zone, (31.5, 35.0))   # Israel centre fallback
        rows.append({
            "zone":  zone,
            "group": group,
            "lat":   centroid[0],
            "lon":   centroid[1],
            "total": total,
            "night": night,
            "pct":   pct,
        })

    if not rows:
        print("\nNo data to chart – check that your city column matched correctly.")
        return

    data = pd.DataFrame(rows)

    # Scale bubble sizes: largest bubble ≈ 90 px, minimum ≈ 10 px
    max_night = data["night"].max() or 1
    data["size"] = (data["night"] / max_night * 80 + 10).clip(lower=10)

    # One trace per group so the legend is clean
    traces = []
    for group, gdf in data.groupby("group"):
        color = GROUP_COLORS.get(group, "#888888")
        traces.append(
            go.Scatter(
                x=gdf["lon"],
                y=gdf["lat"],
                mode="markers+text",
                name=group,
                marker=dict(
                    size=gdf["size"],
                    color=color,
                    opacity=0.80,
                    line=dict(width=1.5, color="white"),
                    sizemode="diameter",
                ),
                text=gdf["zone"],
                textposition="top center",
                textfont=dict(size=10, color="#333"),
                customdata=gdf[["zone", "total", "night", "pct"]].values,
                hovertemplate=(
                    "<b>%{customdata[0]}</b><br>"
                    "Night alerts : <b>%{customdata[2]:,}</b><br>"
                    "Total alerts : %{customdata[1]:,}<br>"
                    "Night share  : %{customdata[3]:.1f}%"
                    "<extra></extra>"
                ),
            )
        )

    fig = go.Figure(traces)

    night_label = f"{NIGHT_START}:00 – {NIGHT_END:02d}:00"
    fig.update_layout(
        title=dict(
            text=(
                "Israeli Homefront Command — Zones Most Woken at Night<br>"
                f"<sup>Bubble size ∝ nighttime alert count ({night_label})"
                " · hover for details</sup>"
            ),
            x=0.5,
            font=dict(size=18),
        ),
        xaxis=dict(
            title="Longitude  (West ← → East)",
            showgrid=True,
            gridcolor="#ececec",
            zeroline=False,
        ),
        yaxis=dict(
            title="Latitude  (South ↑ North)",
            showgrid=True,
            gridcolor="#ececec",
            zeroline=False,
        ),
        plot_bgcolor="white",
        paper_bgcolor="#fafafa",
        legend=dict(
            title=dict(text="Region group"),
            itemsizing="constant",
            font=dict(size=12),
        ),
        font=dict(family="Arial, Helvetica, sans-serif"),
        width=1000,
        height=720,
        margin=dict(t=100, b=60, l=70, r=30),
    )

    OUTPUT_DIR.mkdir(exist_ok=True)
    outfile = OUTPUT_DIR / "night_alerts.html"
    fig.write_html(str(outfile), include_plotlyjs="cdn")
    print(f"\nChart saved → {outfile}")


# ── Console summary ───────────────────────────────────────────────────────────

def print_summary(zone_total: dict, zone_night: dict) -> None:
    rows = []
    for zone, total in zone_total.items():
        night = zone_night.get(zone, 0)
        pct   = round(night / total * 100, 1) if total > 0 else 0.0
        group = ZONE_GROUP.get(zone, "Other")
        rows.append((group, zone, total, night, pct))

    rows.sort(key=lambda r: r[3], reverse=True)   # sort by night count

    print(f"\n{'Group':<22} {'Zone':<26} {'Total':>8} {'Night':>8} {'%Night':>8}")
    print("─" * 76)
    for group, zone, total, night, pct in rows:
        if total > 0:
            print(f"{group:<22} {zone:<26} {total:>8,} {night:>8,} {pct:>7.1f}%")


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    # 1. City → zone mapping (fetched from GitHub)
    city_to_zone, zone_centroid = load_city_data()

    # 2. Alert data — Google Sheet first, then local file fallback
    raw_df = fetch_sheet()
    if raw_df is not None:
        # Sheet was fetched successfully; wrap in the same normalisation step
        df = _normalise_df(raw_df)
    else:
        data_file = find_data_file()
        if data_file is None:
            print(
                "\nNo data found.\n"
                "Either:\n"
                "  a) Share your Google Sheet publicly and re-run, or\n"
                "  b) Export it as .xlsx / .csv and place in data/"
            )
            sys.exit(1)
        df = load_alerts(data_file)

    # 3. Aggregate by zone
    print("\nAggregating alerts by zone …")
    zone_total, zone_night = aggregate(df, city_to_zone)

    total_alerts = sum(zone_total.values())
    total_night  = sum(zone_night.values())
    print(f"  Total alert–city entries : {total_alerts:,}")
    print(f"  Of which at night        : {total_night:,}  "
          f"({round(total_night/total_alerts*100,1) if total_alerts else 0}%)")

    # 4. Print summary table
    print_summary(zone_total, zone_night)

    # 5. Build chart
    build_chart(zone_total, zone_night, zone_centroid)
    print("\nDone.  Open output/night_alerts.html in your browser.")


if __name__ == "__main__":
    main()
