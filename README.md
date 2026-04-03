# Roaring Lion - Homefront Command Alert Dashboard

## About

An open-source interactive dashboard for analysing Israeli Homefront Command (oref.org.il) alert history during Operation Roaring Lion. Tracks rocket, drone, and pre-warning alerts across all HFC zones — visualising patterns by time of day, region, salvo intensity, and pre-alert effectiveness.

Data is fetched automatically from a public archive and refreshed 4× daily. No server required — the output is a single self-contained HTML file.

## What it shows

### ⚡ Situation Room *(default view)*
At-a-glance timeline of recent activity, refreshed 4× daily (06:00 / 12:00 / 18:00 / 00:00 Israel time).

- **What happened last night?** — 22:00 yesterday → 06:00 today
- **What's happening today?** — 06:00 → now (Israel time)

Each section shows a scrollable list of active hours. Every row displays:
- **Time** (e.g. 05:00)
- **Alert event counts** as emoji badges — 🚀 missile, ⚡ pre-alert, 🛩 drone
- **Coloured dots** for each affected region

Hover a badge to see the exact definition (e.g. *"missile alert events — one incident per zone per all-clear signal"*).

Click any row to open a **region breakdown popup** — a stacked bar chart showing how alert events were distributed across regions for that hour.

The footer shows when situation data and full chart data were last fetched and when each is next scheduled to refresh (Israel time).

### By Hour
Stacked bar chart of **alert events** per hour of day (0–23), broken down by region.
A date-range slider lets you focus on any period; alert-type toggles filter by Pre-alert / Missile alert / Drone alert.
Click any bar to open a day-by-day small-multiples view for that region.

### By Date
Cumulative **alert events** per region over time.
Range selector buttons (1w / 2w / All) and a drag slider for quick navigation.

### Mismatches
Stacked bar chart showing, for each calendar day, how many incidents were:

| Category | Meaning |
|----------|---------|
| **Paired** | Incident containing both a pre-alert and a missile alert |
| **Pre-alert only** | Incident with a pre-alert but no missile alert |
| **Missile only** | Incident with a missile alert but no preceding pre-alert |

Toggle between absolute counts and percentage (ratio) view.

### Lead Time
Histogram of the gap (in seconds) between the first pre-alert and the first missile alert in the same incident.
Filter by region to compare warning times across the country.

### Salvos
Overlaid line chart — one line per day — showing **missile alert events** by hour of day (0–23).

- X axis = hour of day; Y axis = missile alert events that hour (flat count, not cumulative)
- Each day is a separate coloured line, making it easy to compare attack timing patterns across dates
- Filter by **region** and **date range**

## Alert types

| Type | Description |
|------|-------------|
| `Pre-alert` | Early warning — typically 1–3 minutes before expected impact |
| `Missile alert` | Active siren / rocket alert |
| `Drone alert` | UAV / unmanned aircraft intrusion |

### Counting methodology

All counts are **incidents**, not individual city notifications.

An incident **opens** when the first alert (pre-alert, missile, or drone) is received for a zone, and **closes** when an "all clear" record (האירוע הסתיים) is received for any city in that zone. The source data from the Homefront Command API includes explicit all-clear signals, so incident boundaries are signal-driven rather than time-window heuristics.

A 90-second window is still applied across zones to collapse simultaneous multi-zone events into single global incident counts.

## Quick start

```bash
# Install dependencies
pip install -r requirements.txt

# Run (fetches latest data from GitHub or uses local file in data/)
python3 main.py

# Open the dashboard
open output/index.html
```

For faster style-only rebuilds (no network, reads cached `data/processed.json`):

```bash
python3 build_chart.py
open output/index.html
```

To override the monitoring start date without editing code:

```bash
ALERT_CUTOFF_DATE=2026-01-01 python3 main.py
```

## Data sources

The dashboard loads alert data from:
- **GitHub** — fetched automatically from [dleshem/israel-alerts-data](https://github.com/dleshem/israel-alerts-data); no configuration needed

City → zone mapping is fetched automatically from
[pikud-haoref-api](https://github.com/eladnava/pikud-haoref-api).

## Location hierarchy

```
~1,450 cities  →  33 HFC zones  →  11 display regions
```

Zones come directly from the official Homefront Command taxonomy.
Display regions (e.g. "Galilee", "Jerusalem", "Gaza Area") are used for chart colouring only.

## Recent improvements

### Signal-based incident detection
- **Replaced 90-second heuristic clustering** with signal-based incident boundaries: incidents now open on the first alert to a zone and close when an "All clear" record arrives for that zone
- **`build_incidents()`** — new core function preserving city membership for future city-level drill-down
- **Mismatch analysis** updated to use incident membership for pairing (pre-alert + missile in same incident = paired), removing the 15-minute time window
- **Incremental builds** use a 6-hour lookback window so all-clear signals that arrive in a later run still correctly close their incident
- All chart text (English + Hebrew) updated to reflect the new model

### Security & Accessibility
- **Color contrast** — three region colors that failed WCAG AA on white backgrounds have
  been darkened: Galilee, Tel Aviv / Gush Dan, and Sharon / Shephelah. Sublabel and footer
  text lightened from `#888` to `#666` for improved readability at small sizes
- **XSS hardening** — tab button labels are now set via `createTextNode` rather than
  `innerHTML`, ensuring translation strings can never inject HTML markup

### Performance
- Replaced all `iterrows()` loops with `itertuples()` in the core processing pipeline
  (`aggregate`, `compute_mismatches`, `compute_salvos`) — approximately 10× faster on
  large DataFrames
- Vectorized `build_gap_hist()`, `_normalise_df()` datetime parsing, and the
  `compute_situation()` period filter using pandas boolean indexing

### Data integrity
- `CUTOFF_DATE` is now overridable via `ALERT_CUTOFF_DATE` environment variable
- `partial_hour` annotation now uses `Asia/Jerusalem` timezone (`zoneinfo`) instead of
  the local system clock
- Missing-timestamp rows in `aggregate()` and `compute_mismatches()` now emit a warning
  with a count rather than silently disappearing
- Unrecognised alert type names now surface a console warning
- Hour-bounds guard (`0–23`) prevents an `IndexError` crash in `compute_situation()`

### Charts & UI
- **Salvos** chart: lines are now straight segments (`shape: 'linear'`) — spline
  smoothing was implying continuous values between discrete hourly data points
- **Mismatches** % view: right-axis (7-day rolling %) now has grey ticks and a `%`
  suffix to visually distinguish it from the left-side event count axis
- Nav tab buttons now carry `role="tab"`, `aria-selected`, and `aria-controls` ARIA
  attributes; `aria-expanded` on the hamburger button is kept in sync by JavaScript
- `:focus-visible` CSS rings added to all toolbar buttons and the modal close button
  for keyboard navigation
- ESC key now closes any open drill-down modal

## Requirements

- Python 3.9+
- `pandas`, `plotly`, `requests`, `openpyxl`

See `requirements.txt` for pinned versions.
