# CLAUDE.md — Developer Guide

## What this project does

Fetches Israeli Homefront Command (IDF/oref.org.il) alert history, maps each
alert city to its official HFC zone, and generates a self-contained interactive
HTML dashboard at `output/index.html`.

## Stack

- **Python 3** — data loading, aggregation, chart construction
- **pandas** — data wrangling
- **plotly** (Python + JS) — chart rendering; Python builds the figure objects,
  serialises them to JSON, and inlines them into the HTML template
- **requests** — fetching cities.json and GitHub CSV
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
| GitHub CSV (live) | Fetched automatically from `dleshem/israel-alerts-data`; set `ALERT_CUTOFF_DATE` env var (or fall back to hardcoded `CUTOFF_DATE` in `main.py`) to adjust the start of the monitoring period |
| Local file (fallback) | Place any `.xlsx`, `.xls`, or `.csv` in `data/` |

Alert data must have columns for city/location, date+time, and alert type.
Column detection is keyword-based — see `_detect_column()` and `_normalise_df()`.

## Key concepts

### Alert types
Four types recognised in `ALERT_TRANSLATIONS` (mapped from Hebrew `category_desc`):
- `Pre-alert` — early warning, typically 1–3 minutes before impact
- `Missile alert` — active siren / rocket alert
- `Drone alert` — UAV intrusion
- `All clear` — incident ended (3 Hebrew variants: generic, missile-specific, drone-specific)

### Location hierarchy
```
city (~1,450 names)  →  zone (33 HFC zones)  →  region group (11 display groups)
```
- City → zone mapping comes from `pikud-haoref-api/cities.json` (fetched at runtime; cached locally in `data/cities.json` if downloaded manually)
- Zone → region group mapping is in `regions.py` (`ZONE_GROUP`)
- Region group colours are in `regions.py` (`GROUP_COLORS`)

### Incident detection
`aggregate()` uses **signal-based incident detection** via `build_incidents()`:
- An incident **opens** on the first Pre-alert / Missile alert / Drone alert to a zone
- An incident **closes** when an "All clear" record maps to the same zone
- Incidents with no closing all-clear are marked `closed=False` (still counted)
- Each incident stores `alert_cities` (cities that received alerts) and `cities`
  (full set incl. all-clear cities) for future drill-down

Two-phase approach:
1. Collect per-zone `(dt, alert_type, city)` tuples for all 4 types
2. For each zone, sort by time and call `build_incidents()` → list of incident dicts

`compute_global_incidents()` then applies the same 90-second `cluster_events()`
window **across zones** to collapse simultaneous multi-zone events into single
global incident counts (used only for `incident_df`).

All counts throughout the dashboard represent **incidents**, not individual
city-level alerts.

### Mismatch analysis
`compute_mismatches()` derives pairing from **incident membership**, not a time window:
- `paired_missile` — incident has ≥1 pre-alert AND ≥1 missile alert
- `paired_drone` — incident has ≥1 pre-alert AND ≥1 drone alert (no missile)
- `pre_only` — incident has pre-alert(s) but no missile or drone
- `missile_only` — incident has missile alert(s) but no pre-alert
- `drone_only` — incident has drone alert(s) but no pre-alert
- Emits one row per city (`alert_cities`) per incident for city-level drill-down

### Salvo analysis
`compute_salvos()` is **dead code** — never called. The Salvos chart renders
client-side in JS directly from `chart_df` (`hourlyData`), filtering for
`alert_type === 'Missile alert'` and grouping by hour. No Python changes needed.

### Situation Room
`compute_situation(chart_df)` computes time-bounded summaries for the Situation Room tab:
- **Last night**: 22:00 yesterday → 06:00 today (uses `NIGHT_START`/`NIGHT_END`)
- **Today**: 06:00 today → now
- For each period: totals by alert type and list of affected regions
- Called fresh on every run (not cached in `processed.json`) since it depends on `datetime.now()`
- `build_chart.py` also calls it after loading `chart_df` from `processed.json`
- Times displayed in `Asia/Jerusalem` (Israel time) via JS `toLocaleString`
- `fetched_at` is stored as a UTC ISO string in `processed.json` and converted to Israel time in JS

#### Timeline list (JS `buildTimelineHTML`)
- Iterates every `(date_str, hour)` pair in the window, filters `hourlyData`, aggregates counts
- Renders one `.sit-tl-row` per active hour: time label | emoji-badge counts | coloured region dots
- Each row stores its data in `data-ds`, `data-h`, `data-sect` attributes (avoids Python f-string backslash-escaping issues with inline onclick strings)
- Clicking opens `openHourModal()` — a Plotly stacked-bar popup breaking down alerts by region for that hour; reuses the existing `#modal-backdrop` / `#modal-chart` infrastructure

## File map

| File | Purpose |
|------|---------|
| `main.py` | Entry point; all data loading, aggregation, mismatch analysis, chart HTML |
| `build_chart.py` | Fast style-only rebuild from `data/processed.json` (no network) |
| `regions.py` | `ZONE_GROUP`, `GROUP_COLORS`, `NIGHT_START`/`NIGHT_END` constants |
| `data/cities.json` | City → zone mapping from pikud-haoref-api (not committed) |
| `data/city_region_mapping.csv` | City → zone → region mapping (CSV only; xlsx removed) |
| `output/index.html` | Generated dashboard (not committed) |

## Dashboard tabs

1. **Situation Room** *(default)* — per-hour timeline list for last night and today; emoji badges (🚀 missile, ⚡ pre-alert, 🛩 drone) + coloured region dots per row; click any row for a region-breakdown popup bar chart; built by `compute_situation()` + `buildSituationView()` / `buildTimelineHTML()` / `openHourModal()` JS
2. **By Hour** — stacked bar, X=hour 0–23, Y=alert events; date-range slider + alert-type toggles
3. **By Date** — cumulative alert events per region over time; range selector buttons
4. **Mismatches** — stacked bar per day: paired / pre-alert only / missile only; toggle Abs / % view
5. **Lead Time** — histogram of pre-alert → missile gap (seconds); region filter
6. **Salvos** — overlaid line chart, one line per day; X=hour 0–23, Y=missile alert events per hour (flat, non-cumulative); filters: region, date range

## Conventions

- Night = hour ≥ 22 OR hour < 6 (see `NIGHT_START`/`NIGHT_END` in `regions.py`)
- All theme (dark/light) logic lives in the inline JavaScript in `build_chart()`
- Adding a new chart view: add a tab button + view div in the HTML template,
  initialise a Plotly chart, and handle it in `setView()` and `toggleTheme()`
- Charts rendered into hidden `display:none` views (Lead Time, Salvos) must call their
  build function from `setView()` rather than relying on `Plotly.Plots.resize()`, because
  Plotly skips rendering into zero-size elements. Both also pass explicit `height`/`width`
  from `offsetHeight`/`offsetWidth` to fill the window correctly.
- Nav tab style: `#nav-tabs .tb-btn` overrides the base `.tb-btn` pill style with browser-tab
  appearance (transparent background, 2px bottom-border highlight on active). The hamburger
  and theme-toggle buttons are outside `#nav-tabs` and are unaffected.
- The Situation Room view is non-Plotly (pure HTML), so it does not need the deferred
  resize trick — `buildSituationView()` can be called directly.
- Avoid inline onclick string literals that embed JS-escaped quotes inside a Python f-string
  template: Python collapses `\'` → `'`, producing adjacent string literals and a JS syntax
  error. Use `data-*` attributes on the element and read them via `this.dataset.*` in the
  onclick handler instead.
- Use `itertuples(index=False)` instead of `iterrows()` for all row-level loops over large
  DataFrames (`aggregate`, `compute_mismatches`, `compute_salvos`, `compute_situation`).
  `iterrows()` creates a full copy of each row as a Series; `itertuples()` is ~10× faster.
- `partial_hour` must be derived from `Asia/Jerusalem` time (via `zoneinfo.ZoneInfo`),
  not the local clock, so the partial-day annotation matches the Israel-time display in JS.

## Roadmap

Planned improvements, grouped by PR. Each can be a standalone session.

### ~~PR: Security & Accessibility~~ ✅ Done
- **XSS hardening** — tab buttons now set the icon via `innerHTML` (safe hardcoded entity
  only) and append the translated label via `createTextNode`; `T.*` values can no longer
  inject HTML. Remaining intentional `innerHTML` uses are explicitly commented.
- **Color contrast** — three region colors that failed WCAG AA on white backgrounds darkened:
  Galilee `#98df8a`→`#3d8b37`, Tel Aviv `#56aeff`→`#1a6bc9`, Sharon `#aec7e8`→`#4a7bb5`.
  Sublabel/footer text `#888`→`#666` across `.sit-sublabel`, `.sit-quiet`, `#global-footer`,
  `.sit-explainer`.

### ~~PR: Signal-Based Incident Detection~~ ✅ Done
- **`build_incidents()`** — new core function; groups per-zone events into incidents
  delimited by "All clear" signals instead of a fixed 90-second time window.
- **`aggregate()` rewrite** — uses `build_incidents()` per zone; city membership
  preserved in `alert_cities` and `cities` sets for future drill-down.
- **`compute_mismatches()` simplified** — pairing via incident membership, removing
  the 15-minute `PAIR_WINDOW` heuristic.
- **`INCIDENT_LOOKBACK = 6 h`** replaces `MISMATCH_LOOKBACK = 30 min`; wider window
  ensures all-clear signals that arrive in a later incremental run still close their
  incident correctly.
- **`merge_chart_df()` / `merge_incident_df()`** — `drop_dates` param added to
  replace rather than double-count dates in the lookback window.
- **Schema version bumped** to 5 for automatic one-time full reprocess.
- **All chart text updated** (EN + HE) to reflect the new incident model.

### PR: New Visualizations
- **Day-of-week heatmap** — rows = Mon–Sun, columns = regions (or alert type), cells =
  average event count. Reveals weekly attack-timing patterns not visible in the current charts.
- **Daily new + 7-day rolling average** — a "daily new alert events" line chart (non-cumulative
  version of By Date) with a 7-day rolling average overlay to show trend direction clearly.
- **Per-region lead time** — the Lead Time histogram currently aggregates all regions.
  Add a small-multiples or faceted view breaking it down per region.
- **Alert-type breakdown** — a donut or treemap showing the proportional split of Missile /
  Pre-alert / Drone across the full dataset.
- **Salvo size distribution** — `compute_salvos()` already produces `cluster_size` (events per
  cluster). Add a histogram showing how often salvos have 2 vs 5 vs 10+ events.

### PR: Refactor
- **Split `main.py`** (~2,900 lines) into focused modules:
  - `data_loader.py` — `load_alerts()`, `_normalise_df()`, `load_city_data()`, `fetch_github_csv()`
  - `aggregator.py` — `aggregate()`, `compute_mismatches()`, `compute_salvos()`, `compute_situation()`
  - `chart_builder.py` — the `build_chart()` function (~2,000 lines of HTML/JS template)
  - Keep `main.py` as a thin entry point that orchestrates the above.

### PR: Tests
- **`cluster_events()`** — edge cases: empty list, single item, all within window, gap exactly
  equal to `EVENT_CLUSTER_WINDOW`, gap just over it.
- **`compute_mismatches()`** — pairing boundary conditions: pre-alert at t=0 / missile at
  t=15:00 (should pair), t=15:01 (should not); city with only pre-alerts; only missiles.
- **`compute_salvos()`** — zone with a single event; events spanning midnight (date boundary).
- **`_normalise_df()`** — column detection with Hebrew headers, missing datetime column,
  mixed-format dates.
- Use `pytest`; add a `tests/` directory with small fixture DataFrames (no network calls).
