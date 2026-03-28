# CLAUDE.md — Developer Guide

## What this project does

Fetches Israeli Homefront Command (IDF/oref.org.il) alert history, maps each
alert city to its official HFC zone, and generates a self-contained interactive
HTML dashboard at `output/ira_alerts.html`.

## Stack

- **Python 3** — data loading, aggregation, chart construction
- **pandas** — data wrangling
- **plotly** (Python + JS) — chart rendering; Python builds the figure objects,
  serialises them to JSON, and inlines them into the HTML template
- **requests** — fetching cities.json and Google Sheets export
- Output is a single static HTML file (no server needed)

## Quick start

```bash
pip install -r requirements.txt
python main.py
open output/ira_alerts.html
```

## Data sources

| Source | How to configure |
|--------|-----------------|
| Google Sheets (live) | Set `SHEETS_ID` / `SHEETS_GID` in `main.py`; sheet must be publicly accessible |
| Local file (fallback) | Place any `.xlsx`, `.xls`, or `.csv` in `data/` |

Alert data must have columns for city/location, date+time, and alert type.
Column detection is keyword-based — see `_detect_column()` and `_normalise_df()`.

## Key concepts

### Alert types
Three types (must appear verbatim in the `alert_type` column):
- `Pre-alert` — early warning, typically 1–3 minutes before impact
- `Missile alert` — active siren / rocket alert
- `Drone alert` — UAV intrusion

### Location hierarchy
```
city (~1,450 names)  →  zone (33 HFC zones)  →  region group (11 display groups)
```
- City → zone mapping comes from `pikud-haoref-api/cities.json` (fetched at runtime; cached locally in `data/cities.json` if downloaded manually)
- Zone → region group mapping is in `regions.py` (`ZONE_GROUP`)
- Region group colours are in `regions.py` (`GROUP_COLORS`)

### Deduplication
`aggregate()` deduplicates per `(zone, datetime-minute)`: if multiple cities in
the same zone fire at the same minute, that counts as **one event**.

### Mismatch analysis
`compute_mismatches()` works at **city level** with a 15-minute pairing window:
- A `Pre-alert` is `paired` if a `Missile alert` follows within 15 min for the same city
- A `Pre-alert` is `pre_only` if no missile follows within 15 min
- A `Missile alert` is `missile_only` if no pre-alert preceded it within 15 min
- Drone alerts are excluded from pairing

## File map

| File | Purpose |
|------|---------|
| `main.py` | Entry point; all data loading, aggregation, mismatch analysis, chart HTML |
| `regions.py` | `ZONE_GROUP`, `GROUP_COLORS`, `NIGHT_START`/`NIGHT_END` constants |
| `data/cities.json` | City → zone mapping from pikud-haoref-api (not committed) |
| `data/city_region_mapping.csv` | Pre-computed city → zone → region export |
| `output/ira_alerts.html` | Generated dashboard (not committed) |

## Dashboard tabs

1. **By Hour** — stacked bar, X=hour 0–23, Y=alert count; date-range slider + alert-type toggles
2. **By Date** — cumulative line chart per region; range selector buttons
3. **Mismatches** — stacked bar per day: paired / pre-alert only / missile only; toggle Abs / % view

## Conventions

- Night = hour ≥ 22 OR hour < 6 (see `NIGHT_START`/`NIGHT_END` in `regions.py`)
- All theme (dark/light) logic lives in the inline JavaScript in `build_chart()`
- Adding a new chart view: add a tab button + view div in the HTML template,
  initialise a Plotly chart, and handle it in `setView()` and `toggleTheme()`
