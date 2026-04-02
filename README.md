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

Hover a badge to see the exact definition (e.g. *"missile alert events — same zone within 90 s = 1 event"*).

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
Stacked bar chart showing, for each calendar day, how many events were:

| Category | Meaning |
|----------|---------|
| **Paired** | Pre-alert followed by a missile alert within 15 minutes (same city) |
| **Pre-alert only** | Pre-alert with no missile alert following within 15 minutes |
| **Missile only** | Missile alert with no preceding pre-alert within 15 minutes |

Toggle between absolute counts and percentage (ratio) view.

### Lead Time
Histogram of the gap (in minutes) between a pre-alert and its paired missile alert.
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

All counts are **deduplicated alert events**, not individual city notifications.
Alerts fired to the same zone within **90 seconds** of each other are treated as a single event (one missile/drone spreading its alert across nearby cities). This prevents inflating counts when a single threat triggers rapid sequential alerts to multiple neighbouring cities in the same zone.

## Quick start

```bash
# Install dependencies
pip install -r requirements.txt

# Run (fetches latest data from GitHub or uses local file in data/)
python main.py

# Open the dashboard
open output/index.html
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

## Requirements

- Python 3.9+
- `pandas`, `plotly`, `requests`, `openpyxl`

See `requirements.txt` for pinned versions.
