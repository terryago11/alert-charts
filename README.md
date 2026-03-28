# Ira-alert — IDF Homefront Command Alert Dashboard

An interactive dashboard for analysing Israeli Homefront Command (oref.org.il) alert history — rockets, drones, and pre-warnings — by time of day, region, and pre-alert / missile pairing.

## What it shows

### By Hour
Stacked bar chart of alert counts per hour of day (0–23), broken down by region.
A date-range slider lets you focus on any period; alert-type toggles filter by Pre-alert / Missile alert / Drone alert.
Click any bar to open a day-by-day small-multiples view for that region.

### By Date
Cumulative alert counts per region over time.
Range selector buttons (1w / 2w / All) and a drag slider for quick navigation.

### Mismatches
Stacked bar chart showing, for each calendar day, how many events were:

| Category | Meaning |
|----------|---------|
| **Paired** | Pre-alert followed by a missile alert within 15 minutes (same city) |
| **Pre-alert only** | Pre-alert with no missile alert following within 15 minutes |
| **Missile only** | Missile alert with no preceding pre-alert within 15 minutes |

Toggle between absolute counts and percentage (ratio) view.

## Alert types

| Type | Description |
|------|-------------|
| `Pre-alert` | Early warning — typically 1–3 minutes before expected impact |
| `Missile alert` | Active siren / rocket alert |
| `Drone alert` | UAV / unmanned aircraft intrusion |

## Quick start

```bash
# Install dependencies
pip install -r requirements.txt

# Run (fetches latest data from GitHub or uses local file in data/)
python main.py

# Open the dashboard
open output/ira_alerts.html
```

## Data sources

The dashboard loads alert data from one of:
1. **GitHub** — fetched automatically from [dleshem/israel-alerts-data](https://github.com/dleshem/israel-alerts-data); no configuration needed
2. **Local file fallback** — place any `.xlsx`, `.xls`, or `.csv` export in the `data/` folder

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
