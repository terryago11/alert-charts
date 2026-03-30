#!/usr/bin/env python3
"""
Read data/processed.json and regenerate output/index.html.

Run this whenever you want to iterate on styles or templates without
re-fetching data (fast — no network, no pandas aggregation):

    python build_chart.py

To refresh the underlying data first, run:

    python fetch_data.py
"""

import json
import sys
from pathlib import Path

import pandas as pd

from main import build_chart, compute_situation

DATA_FILE = Path("data/processed.json")


def main() -> None:
    if not DATA_FILE.exists():
        print(f"No processed data at {DATA_FILE}. Run 'python fetch_data.py' first.")
        sys.exit(1)

    payload      = json.loads(DATA_FILE.read_text(encoding="utf-8"))
    chart_df     = pd.DataFrame(payload["chart_df"])
    mismatch_df  = pd.DataFrame(payload["mismatch_df"]) if payload.get("mismatch_df") else pd.DataFrame()
    salvo_df     = pd.DataFrame(payload["salvo_df"])    if payload.get("salvo_df")    else pd.DataFrame()
    partial_day  = payload.get("partial_day")
    partial_hour = payload.get("partial_hour")

    situation_data = compute_situation(chart_df)

    build_chart(
        chart_df,
        mismatch_df  if not mismatch_df.empty  else None,
        salvo_df      = salvo_df if not salvo_df.empty else None,
        partial_day   = partial_day,
        partial_hour  = partial_hour,
        situation_data = situation_data,
    )


if __name__ == "__main__":
    main()
