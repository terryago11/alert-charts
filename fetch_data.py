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
    OUTPUT_DIR,
    aggregate,
    compute_mismatches,
    compute_salvos,
    fetch_github_csv,
    find_data_file,
    load_alerts,
    load_city_data,
    _normalise_df,
    print_summary,
    save_processed,
)


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

    # 2b. Detect partial day
    last_date    = df["date_str"].dropna().max()
    today_str    = date.today().isoformat()
    is_partial   = last_date == today_str
    partial_day  = last_date if is_partial else None
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

    # 6. Save processed data
    save_processed(chart_df, mismatch_df, salvo_df, partial_day, partial_hour)
    print("\nDone.  Run 'python build_chart.py' to regenerate the HTML.")


if __name__ == "__main__":
    main()
