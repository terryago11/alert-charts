#!/usr/bin/env python3
"""
Israeli Homefront Command – Alert Bubble Chart
===============================================
Loads alert history, maps each city to its Homefront Command zone, deduplicates
per-zone per-minute, then saves a full-screen interactive chart to
output/index.html with a dark/light mode toggle.

Quick start
-----------
1. pip install -r requirements.txt
2. python main.py
3. Open output/index.html
"""

import sys
from datetime import date, datetime, timedelta

import pandas as pd

from aggregator import (
    ALL_EVENT_TYPES,
    INCIDENT_LOOKBACK,
    aggregate,
    compute_mismatches,
    compute_situation,
    merge_chart_df,
    merge_incident_df,
    merge_mismatch,
)
from chart_builder import OUTPUT_DIR, build_chart, save_situation_json
from data_loader import (
    fetch_csv_sha,
    fetch_github_csv,
    find_data_file,
    load_alerts,
    load_city_data,
    load_processed,
    save_processed,
    _normalise_df,
)
from regions import ZONE_GROUP


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
    # Fetch from (since_dt - INCIDENT_LOOKBACK) so that build_incidents() can
    # close incidents whose alerts landed in a prior run but whose "All clear"
    # arrives in this run.
    since_dt = pd.Timestamp(existing["fetched_at"]) if existing else None
    incident_since_dt = (since_dt - INCIDENT_LOOKBACK) if since_dt is not None else None
    raw = fetch_github_csv(since_dt=incident_since_dt)
    if raw is not None:
        df_extended = _normalise_df(raw)
    else:
        data_file = find_data_file()
        if data_file is None:
            print("\nNo data found. Place an .xlsx/.csv in data/ or check network.")
            sys.exit(1)
        df_extended = load_alerts(data_file)

    # 5. Aggregate — always over the full lookback window so incident boundaries
    # are correctly detected even when they straddle two consecutive runs.
    print("\nAggregating alerts by zone …")
    zone_total, zone_night, new_chart_df, new_incident_df = aggregate(df_extended, city_to_zone)
    total_alerts = sum(zone_total.values())
    total_night  = sum(zone_night.values())
    print(f"  Deduplicated alert events : {total_alerts:,}")
    print(f"  Of which at night         : {total_night:,}  "
          f"({round(total_night / total_alerts * 100, 1) if total_alerts else 0}%)")

    # Calendar dates in the lookback window are replaced rather than summed in
    # the merge to prevent double-counting re-examined incidents.
    if since_dt is not None and incident_since_dt is not None:
        _lb_start = incident_since_dt.date() if hasattr(incident_since_dt, "date") else incident_since_dt.to_pydatetime().date()
        _lb_end   = since_dt.date() if hasattr(since_dt, "date") else since_dt.to_pydatetime().date()
        lookback_dates: set = set()
        _d = _lb_start
        while _d <= _lb_end:
            lookback_dates.add(_d.isoformat())
            _d += timedelta(days=1)
    else:
        lookback_dates = None

    # Merge with historical chart_df from prior build (dropping lookback dates first)
    chart_df    = merge_chart_df(existing["chart_df"] if existing else [], new_chart_df,
                                 drop_dates=lookback_dates)
    incident_df = merge_incident_df(existing.get("incident_df", []) if existing else [], new_incident_df,
                                    drop_dates=lookback_dates)

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

    # lookback_dates already computed above; reused here to replace mismatch records
    # for the same window (consistent with chart_df / incident_df drop-replace).
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
