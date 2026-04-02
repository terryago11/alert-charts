#!/usr/bin/env python3
"""
Read data/processed.json and regenerate output/index.html.

Run this whenever you want to iterate on styles or templates without
re-fetching data (fast — no network, no pandas aggregation):

    python build_chart.py

To refresh the underlying data first, run:

    python main.py
"""

import json
import sys
from pathlib import Path

import pandas as pd

from main import build_chart, compute_situation

DATA_FILE = Path("data/processed.json")


def main() -> None:
    if not DATA_FILE.exists():
        print(f"No processed data at {DATA_FILE}. Run 'python main.py' first.")
        sys.exit(1)

    payload          = json.loads(DATA_FILE.read_text(encoding="utf-8"))
    chart_df         = pd.DataFrame(payload["chart_df"])
    incident_df      = pd.DataFrame(payload.get("incident_df", []))
    mismatch_agg     = payload.get("mismatch_agg", [])
    gap_missile_hist = payload.get("gap_missile_hist", {})
    gap_drone_hist   = payload.get("gap_drone_hist", {})
    partial_day      = payload.get("partial_day")
    partial_hour     = payload.get("partial_hour")
    fetched_at       = payload.get("fetched_at")

    situation_data = compute_situation(chart_df)

    build_chart(
        chart_df,
        mismatch_agg,
        gap_missile_hist,
        gap_drone_hist,
        partial_day    = partial_day,
        partial_hour   = partial_hour,
        situation_data = situation_data,
        fetched_at     = fetched_at,
        incident_df    = incident_df,
    )


if __name__ == "__main__":
    main()
