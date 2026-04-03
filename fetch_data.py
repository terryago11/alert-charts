#!/usr/bin/env python3
"""
Fetch and process alert data, saving results to data/processed.json.

Run this once per day (or whenever new alert data arrives):

    python fetch_data.py

Afterwards, regenerate the HTML without re-fetching:

    python build_chart.py
"""

import sys
from datetime import date, datetime
from pathlib import Path

from main import (
    ALL_EVENT_TYPES,
    INCIDENT_LOOKBACK,
    OUTPUT_DIR,
    aggregate,
    build_gap_hist,
    compute_mismatches,
    fetch_github_csv,
    find_data_file,
    load_alerts,
    load_city_data,
    load_processed,
    merge_mismatch,
    _normalise_df,
    print_summary,
    save_processed,
    save_situation_json,
)


def main() -> None:
    from datetime import timezone as _tz

    # 1. City → zone mapping
    city_to_zone, _ = load_city_data()

    # 2. Load existing state for incremental merge
    existing = load_processed()

    # 3. Alert data — GitHub CSV (incremental if prior state exists).
    # Fetch from (since_dt - INCIDENT_LOOKBACK) so build_incidents() can close
    # incidents whose alerts landed in a prior run but whose "All clear" arrives now.
    import pandas as pd
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

    # 4. Detect partial day
    last_date    = df_extended["date_str"].dropna().max()
    today_str    = date.today().isoformat()
    is_partial   = last_date == today_str
    partial_day  = last_date if is_partial else None
    partial_hour = datetime.now().hour if is_partial else None
    if partial_day:
        print(f"  Partial day detected: {partial_day} — fetched at hour {partial_hour:02d}:xx")

    # 5. Aggregate over the full lookback window and merge with existing chart_df.
    print("\nAggregating alerts by zone …")
    from main import merge_chart_df, merge_incident_df
    from datetime import timedelta
    zone_total, zone_night, new_chart_df, new_incident_df = aggregate(df_extended, city_to_zone)

    total_alerts = sum(zone_total.values())
    total_night  = sum(zone_night.values())
    print(f"  Deduplicated alert events : {total_alerts:,}")
    print(f"  Of which at night         : {total_night:,}  "
          f"({round(total_night / total_alerts * 100, 1) if total_alerts else 0}%)")

    # Dates in the lookback window are replaced rather than summed.
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

    chart_df    = merge_chart_df(existing["chart_df"] if existing else [], new_chart_df,
                                 drop_dates=lookback_dates)
    incident_df = merge_incident_df(existing.get("incident_df", []) if existing else [], new_incident_df,
                                    drop_dates=lookback_dates)

    # 6. Summary table
    print_summary(zone_total, zone_night)

    # 7. Pre-alert / missile mismatch analysis
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

    mismatch_agg, gap_missile_h, gap_drone_h = merge_mismatch(
        existing.get("mismatch_agg", []) if existing else [],
        existing.get("gap_missile_hist", {}) if existing else {},
        existing.get("gap_drone_hist", {}) if existing else {},
        new_mismatch_df,
        lookback_dates=lookback_dates,
    )

    # 8. Save processed data
    fetched_at = datetime.now(_tz.utc).isoformat()
    save_processed(chart_df, mismatch_agg, gap_missile_h, gap_drone_h,
                   partial_day, partial_hour, incident_df=incident_df, fetched_at=fetched_at)
    save_situation_json(chart_df, fetched_at)
    print("\nDone.  Run 'python build_chart.py' to regenerate the HTML.")


if __name__ == "__main__":
    main()
