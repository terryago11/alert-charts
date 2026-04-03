"""Alert aggregation, incident detection, mismatch analysis, and merge helpers."""

import re
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Optional, Tuple

import pandas as pd

from regions import GROUP_COLORS, NIGHT_END, NIGHT_START, ZONE_GROUP

# ── Constants ─────────────────────────────────────────────────────────────────

EVENT_CLUSTER_WINDOW = 90  # seconds — alerts within this window per zone = 1 event
SALVO_WINDOW      = timedelta(minutes=30)   # max consecutive gap within a salvo cluster
INCIDENT_LOOKBACK = timedelta(hours=6)      # re-examine window to close cross-boundary incidents

def cluster_events(times: list) -> list:
    """Temporal clustering: group sorted timestamps into clusters where each new
    event starts a new cluster only if it is >EVENT_CLUSTER_WINDOW seconds after
    the current cluster's start.  Returns the representative (first) timestamp
    of each cluster."""
    if not times:
        return []
    sorted_ts = sorted(times)
    result = [sorted_ts[0]]
    cluster_start = sorted_ts[0]
    for t in sorted_ts[1:]:
        if (t - cluster_start).total_seconds() > EVENT_CLUSTER_WINDOW:
            result.append(t)
            cluster_start = t
    return result


def build_incidents(zone_events: list) -> list:
    """Group sorted (dt, alert_type, city) events for one zone into incidents.

    An incident opens on the first Pre-alert / Missile alert / Drone alert and
    closes when an "All clear" record is received for any city in that zone.
    Incidents still open at the end of the list are marked closed=False.

    Each incident dict contains:
      start_dt    – timestamp of the first alert
      end_dt      – timestamp of the closing "All clear" (or None if still open)
      closed      – bool
      cities      – set of all city names seen in this incident (alerts + all-clear)
      pre_dts     – list of pre-alert timestamps
      missile_dts – list of missile alert timestamps
      drone_dts   – list of drone alert timestamps
    """
    incidents: list = []
    current: Optional[dict] = None
    for dt, alert_type, city in zone_events:
        if alert_type == "All clear":
            if current is not None:
                current["end_dt"] = dt
                current["closed"] = True
                current["cities"].add(city)
                incidents.append(current)
                current = None
            # spurious all-clear with no open incident — skip
        else:
            if current is None:
                current = {
                    "start_dt":     dt,
                    "end_dt":       None,
                    "closed":       False,
                    "cities":       {city},   # all cities: alerts + all-clear (full drill-down)
                    "alert_cities": {city},   # only cities that received alert records
                    "pre_dts":      [],
                    "missile_dts":  [],
                    "drone_dts":    [],
                }
            else:
                current["cities"].add(city)
                current["alert_cities"].add(city)
            _key = {"Pre-alert": "pre", "Missile alert": "missile", "Drone alert": "drone"}.get(alert_type)
            if _key:
                current[f"{_key}_dts"].append(dt)
    if current is not None:
        current["closed"] = False
        incidents.append(current)
    return incidents


def compute_global_incidents(zone_clustered: dict) -> "pd.DataFrame":
    """Cross-zone temporal clustering (same 90 s window).

    Takes the per-zone clustered events and collapses simultaneous alerts across
    all zones into single global incidents, per alert_type.  Two zone-level events
    of the same type within EVENT_CLUSTER_WINDOW seconds count as one incident.

    Returns DataFrame [date_str, hour, alert_type, count].
    """
    by_type: dict = defaultdict(list)
    for (_, alert_type), times in zone_clustered.items():
        by_type[alert_type].extend(times)

    rows: list = []
    for alert_type, times in by_type.items():
        for dt in cluster_events(times):
            rows.append({
                "date_str":   dt.strftime("%Y-%m-%d"),
                "hour":       dt.hour,
                "alert_type": alert_type,
            })

    if rows:
        return (
            pd.DataFrame(rows)
            .groupby(["date_str", "hour", "alert_type"])
            .size()
            .reset_index(name="count")
        )
    return pd.DataFrame(columns=["date_str", "hour", "alert_type", "count"])


def is_night(hour: Optional[int]) -> bool:
    if hour is None:
        return False
    return hour >= NIGHT_START or hour < NIGHT_END


def aggregate(
    df: pd.DataFrame, city_to_zone: dict
) -> Tuple[dict, dict, pd.DataFrame, pd.DataFrame]:
    """
    Signal-based incident detection: incidents are bounded by "All clear" records.

    An incident opens on the first Pre-alert / Missile alert / Drone alert to a
    zone and closes when an "All clear" record maps to the same zone.  City
    membership is preserved in each incident for future drill-down.

    Returns (zone_total, zone_night, chart_df, incident_df).
    chart_df has columns [date_str, hour, group, alert_type, count] —
    one count per alert type present in each incident, keyed to start_dt.
    incident_df has columns [date_str, hour, alert_type, count] —
    cross-zone deduplicated incident counts (90 s window across all zones).
    """
    # Phase 1: collect per-zone (dt, alert_type, city) tuples for all 4 types
    zone_events: dict  = defaultdict(list)  # zone -> [(dt, alert_type, city)]
    unmatched: set     = set()
    skipped_null_dt    = 0

    for row in df.itertuples(index=False):
        dt = row.dt
        if dt is None or pd.isna(dt):
            skipped_null_dt += 1
            continue
        alert_type = str(getattr(row, "alert_type", "Unknown") or "Unknown")
        cities = [c.strip() for c in re.split(r"[,،;|\n]+", str(row.city_raw)) if c.strip()]
        for city in cities:
            zone = city_to_zone.get(city)
            if zone:
                zone_events[zone].append((dt, alert_type, city))
            else:
                unmatched.add(city)

    if skipped_null_dt:
        print(f"  Warning: dropped {skipped_null_dt:,} row(s) with missing timestamp.")
    if unmatched:
        print(f"\n  Note: {len(unmatched)} unmatched city names "
              f"(first 15): {sorted(unmatched)[:15]}")

    # Phase 2: build incidents per zone
    zone_incidents: dict = {}  # zone -> [incident_dict]
    for zone, events in zone_events.items():
        events_sorted = sorted(events, key=lambda x: x[0])
        zone_incidents[zone] = build_incidents(events_sorted)

    # Phase 3: build zone_clustered for compute_global_incidents (first alert per incident)
    zone_clustered: dict = defaultdict(list)
    for zone, incidents in zone_incidents.items():
        for inc in incidents:
            for atype, dts in (("Pre-alert", inc["pre_dts"]),
                               ("Missile alert", inc["missile_dts"]),
                               ("Drone alert", inc["drone_dts"])):
                if dts:
                    zone_clustered[(zone, atype)].append(inc["start_dt"])

    incident_df = compute_global_incidents(dict(zone_clustered))

    # Phase 4: derive chart_df, zone_total, zone_night from incidents
    zone_total = defaultdict(int)
    zone_night = defaultdict(int)
    chart_rows: list = []

    for zone, incidents in zone_incidents.items():
        group = ZONE_GROUP.get(zone, "Other")
        for inc in incidents:
            dt = inc["start_dt"]
            zone_total[zone] += 1
            if is_night(dt.hour):
                zone_night[zone] += 1
            date_str = dt.strftime("%Y-%m-%d")
            hour     = dt.hour
            for atype, dts in (("Pre-alert", inc["pre_dts"]),
                               ("Missile alert", inc["missile_dts"]),
                               ("Drone alert", inc["drone_dts"])):
                if dts:
                    chart_rows.append({
                        "date_str":   date_str,
                        "hour":       hour,
                        "group":      group,
                        "alert_type": atype,
                    })

    if chart_rows:
        chart_df = (
            pd.DataFrame(chart_rows)
            .groupby(["date_str", "hour", "group", "alert_type"])
            .size()
            .reset_index(name="count")
        )
    else:
        chart_df = pd.DataFrame(columns=["date_str", "hour", "group", "alert_type", "count"])

    open_count = sum(
        1 for incs in zone_incidents.values() for inc in incs if not inc["closed"]
    )
    if open_count:
        print(f"  Note: {open_count} incident(s) still open (no all-clear received yet).")

    return dict(zone_total), dict(zone_night), chart_df, incident_df


# ── Mismatch analysis ─────────────────────────────────────────────────────────

def compute_mismatches(df: pd.DataFrame, city_to_zone: dict) -> pd.DataFrame:
    """
    Derive Pre-alert / Missile / Drone pairing from incident membership.

    Pairing is determined by what alert types appear within the same incident
    (delimited by "All clear" signals), not by a fixed time window.

    event_type values:
      paired_missile – incident has pre-alert(s) AND missile alert(s)
      paired_drone   – incident has pre-alert(s) AND drone alert(s), no missile
      pre_only       – incident has pre-alert(s) but no missile or drone
      missile_only   – incident has missile alert(s) but no pre-alert
      drone_only     – incident has drone alert(s) but no pre-alert

    gap_seconds for paired events: time from first pre-alert to first missile/drone.

    Returns DataFrame: [city, zone, group, date_str, event_type, gap_seconds]
    One row per city per incident (cities preserved for future drill-down).
    """
    # Build per-zone event lists (all 4 types including "All clear")
    zone_events: dict = defaultdict(list)
    _skipped = 0
    for row in df.itertuples(index=False):
        dt         = row.dt
        alert_type = str(getattr(row, "alert_type", "Unknown") or "Unknown")
        if dt is None or pd.isna(dt):
            _skipped += 1
            continue
        if alert_type not in ("Pre-alert", "Missile alert", "Drone alert", "All clear"):
            continue
        cities = [c.strip() for c in re.split(r"[,،;|\n]+", str(row.city_raw)) if c.strip()]
        for city in cities:
            zone = city_to_zone.get(city)
            if zone:
                zone_events[zone].append((dt, alert_type, city))
    if _skipped:
        print(f"  compute_mismatches: dropped {_skipped:,} row(s) with missing timestamp.")

    rows = []
    for zone, events in zone_events.items():
        group    = ZONE_GROUP.get(zone, "Other")
        incidents = build_incidents(sorted(events, key=lambda x: x[0]))

        for inc in incidents:
            has_pre     = bool(inc["pre_dts"])
            has_missile = bool(inc["missile_dts"])
            has_drone   = bool(inc["drone_dts"])

            if has_pre and has_missile:
                evt = "paired_missile"
                gap: Optional[float] = (
                    min(inc["missile_dts"]) - inc["pre_dts"][0]
                ).total_seconds()
            elif has_pre and has_drone:
                evt = "paired_drone"
                gap = (min(inc["drone_dts"]) - inc["pre_dts"][0]).total_seconds()
            elif has_pre:
                evt = "pre_only"
                gap = None
            elif has_missile:
                evt = "missile_only"
                gap = None
            elif has_drone:
                evt = "drone_only"
                gap = None
            else:
                continue  # all-clear with no alerts — skip

            date_str = inc["start_dt"].strftime("%Y-%m-%d")
            for city in inc["alert_cities"]:
                rows.append({
                    "city":        city,
                    "zone":        zone,
                    "group":       group,
                    "date_str":    date_str,
                    "event_type":  evt,
                    "gap_seconds": gap,
                })

    if rows:
        return pd.DataFrame(rows)
    return pd.DataFrame(columns=["city", "zone", "group", "date_str", "event_type", "gap_seconds"])


ALL_EVENT_TYPES = ("paired_missile", "paired_drone", "pre_only", "missile_only", "drone_only")


def mismatch_daily_data(mismatch_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate mismatch events per calendar day.
    Returns DataFrame: [date_str, paired_missile, paired_drone, pre_only, missile_only, drone_only].
    """
    if mismatch_df.empty:
        return pd.DataFrame(columns=["date_str", *ALL_EVENT_TYPES])

    counts = (
        mismatch_df.groupby(["date_str", "event_type"])
        .size()
        .reset_index(name="count")
        .pivot(index="date_str", columns="event_type", values="count")
        .fillna(0)
        .astype(int)
        .reset_index()
    )
    for col in ALL_EVENT_TYPES:
        if col not in counts.columns:
            counts[col] = 0

    return counts[["date_str", *ALL_EVENT_TYPES]].sort_values("date_str")


def compute_salvos(df: pd.DataFrame, city_to_zone: dict) -> pd.DataFrame:
    """
    Count all Missile alerts per (zone, date, hour), after deduplicating
    same-minute hits per zone (consistent with aggregate()).

    Returns DataFrame: [zone, group, date_str, cluster_start, cluster_size]
    where cluster_start is the ISO timestamp of the first missile in that
    zone-date-hour and cluster_size is the missile count.
    """
    zone_times: dict = defaultdict(list)
    for row in df.itertuples(index=False):
        if str(getattr(row, "alert_type", "") or "") != "Missile alert":
            continue
        dt = row.dt
        if dt is None or pd.isna(dt):
            continue
        raw    = str(row.city_raw).strip()
        cities = [c.strip() for c in re.split(r"[,،;|\n]+", raw) if c.strip()]
        for city in cities:
            zone = city_to_zone.get(city)
            if zone:
                zone_times[zone].append(dt)

    rows = []
    for zone, times in zone_times.items():
        group        = ZONE_GROUP.get(zone, "Other")
        deduped      = cluster_events(times)  # 90-second event clustering (consistent with aggregate)

        # Group by (date, hour) and count
        hour_counts: dict = defaultdict(list)
        for dt in deduped:
            key = (dt.strftime("%Y-%m-%d"), dt.hour)
            hour_counts[key].append(dt)

        for (date_str, _hour), hour_times in hour_counts.items():
            rows.append({
                "zone":          zone,
                "group":         group,
                "date_str":      date_str,
                "cluster_start": hour_times[0].isoformat(),
                "cluster_size":  len(hour_times),
            })

    if rows:
        return pd.DataFrame(rows)
    return pd.DataFrame(columns=["zone", "group", "date_str", "cluster_start", "cluster_size"])


# ── Incremental merge helpers ─────────────────────────────────────────────────

def build_gap_hist(mismatch_df: pd.DataFrame, event_type: str) -> dict:
    """Convert paired mismatch rows → {group: {gap_seconds_str: count}} histogram."""
    if mismatch_df is None or mismatch_df.empty:
        return {}
    paired = mismatch_df[
        (mismatch_df["event_type"] == event_type) & mismatch_df["gap_seconds"].notna()
    ].copy()
    if paired.empty:
        return {}
    paired["_key"] = paired["gap_seconds"].astype(int).astype(str)
    return {
        g: sub["_key"].value_counts().to_dict()
        for g, sub in paired.groupby("group")
    }


def merge_chart_df(existing_records: list, new_df: pd.DataFrame,
                   drop_dates: Optional[set] = None) -> pd.DataFrame:
    """Merge new chart_df rows into existing records, re-summing overlapping groups.

    drop_dates: if provided, existing records for those date_str values are removed
    before merging so re-examined incident windows replace rather than double-count.
    """
    if not existing_records:
        return new_df
    existing_df = pd.DataFrame(existing_records)
    if drop_dates:
        existing_df = existing_df[~existing_df["date_str"].isin(drop_dates)]
    combined = pd.concat([existing_df, new_df], ignore_index=True)
    return (
        combined
        .groupby(["date_str", "hour", "group", "alert_type"], as_index=False)["count"]
        .sum()
    )


def merge_incident_df(existing_records: list, new_df: pd.DataFrame,
                      drop_dates: Optional[set] = None) -> pd.DataFrame:
    """Merge new incident_df rows into existing records, re-summing overlapping cells.

    drop_dates: if provided, existing records for those date_str values are removed
    before merging so re-examined incident windows replace rather than double-count.
    """
    if not existing_records:
        return new_df
    existing_df = pd.DataFrame(existing_records)
    if drop_dates:
        existing_df = existing_df[~existing_df["date_str"].isin(drop_dates)]
    combined = pd.concat([existing_df, new_df], ignore_index=True)
    return (
        combined
        .groupby(["date_str", "hour", "alert_type"], as_index=False)["count"]
        .sum()
    )


def merge_mismatch(existing_agg: list, existing_gap_m: dict, existing_gap_d: dict,
                   new_mismatch_df: pd.DataFrame, lookback_dates=None) -> tuple:
    """Merge new mismatch rows into existing aggregated counts and gap histograms.

    lookback_dates: if provided, existing records for those date_str values are dropped
    before merging so re-examined boundary events replace rather than add to prior counts.
    """
    if new_mismatch_df is not None and not new_mismatch_df.empty:
        new_agg_df = (
            new_mismatch_df.groupby(["group", "date_str", "event_type"])
            .size().reset_index(name="count")
        )
        new_agg   = new_agg_df.to_dict(orient="records")
        new_gap_m = build_gap_hist(new_mismatch_df, "paired_missile")
        new_gap_d = build_gap_hist(new_mismatch_df, "paired_drone")
    else:
        new_agg, new_gap_m, new_gap_d = [], {}, {}

    # Strip existing records for the lookback window — they'll be replaced by new_agg.
    if lookback_dates:
        existing_agg = [r for r in existing_agg if r["date_str"] not in lookback_dates]

    # Merge agg: sum counts per (group, date_str, event_type)
    combined: dict = {}
    for rec in existing_agg + new_agg:
        key = (rec["group"], rec["date_str"], rec["event_type"])
        combined[key] = combined.get(key, 0) + rec["count"]
    merged_agg = [
        {"group": k[0], "date_str": k[1], "event_type": k[2], "count": v}
        for k, v in combined.items()
    ]

    def _merge_hist(old: dict, new: dict) -> dict:
        result = {g: dict(h) for g, h in old.items()}
        for g, h in new.items():
            if g not in result:
                result[g] = {}
            for gs, cnt in h.items():
                result[g][gs] = result[g].get(gs, 0) + cnt
        return result

    return merged_agg, _merge_hist(existing_gap_m, new_gap_m), _merge_hist(existing_gap_d, new_gap_d)


# ── Situation Room ─────────────────────────────────────────────────────────────

def compute_situation(chart_df: pd.DataFrame) -> dict:
    """Return last-night and today alert summaries for the Situation Room tab."""
    from datetime import timezone
    now   = datetime.now(timezone.utc)
    today = now.date()
    yest  = today - timedelta(days=1)

    ln_start = datetime(yest.year,  yest.month,  yest.day,  NIGHT_START)
    ln_end   = datetime(today.year, today.month, today.day, NIGHT_END)
    td_start = ln_end
    td_end   = datetime(today.year, today.month, today.day, now.hour, now.minute)

    def period_stats(start_dt: datetime, end_dt: datetime, label: str) -> dict:
        if chart_df.empty:
            sub = chart_df
        else:
            _valid_rows = chart_df["date_str"].notna() & chart_df["hour"].notna()
            _dts = pd.to_datetime(
                chart_df.loc[_valid_rows, "date_str"]
            ) + pd.to_timedelta(chart_df.loc[_valid_rows, "hour"].astype(int), unit="h")
            _mask = pd.Series(False, index=chart_df.index)
            _mask.loc[_valid_rows] = (_dts >= pd.Timestamp(start_dt)) & (_dts < pd.Timestamp(end_dt))
            sub = chart_df[_mask]

        def type_sum(atype: str) -> int:
            return int(sub[sub["alert_type"] == atype]["count"].sum()) if not sub.empty else 0

        per_region_hourly: dict = {}
        for row in sub.itertuples(index=False):
            grp  = getattr(row, "group", None) or "Other"
            hour = int(getattr(row, "hour", 0) or 0)
            cnt  = int(getattr(row, "count", 0) or 0)
            if not (0 <= hour <= 23):
                continue
            if grp not in per_region_hourly:
                per_region_hourly[grp] = [0] * 24
            per_region_hourly[grp][hour] += cnt

        return {
            "label":             label,
            "start_iso":         start_dt.isoformat(),
            "end_iso":           end_dt.isoformat(),
            "total_missile":     type_sum("Missile alert"),
            "total_pre":         type_sum("Pre-alert"),
            "total_drone":       type_sum("Drone alert"),
            "regions":           sorted(per_region_hourly.keys()),
            "per_region_hourly": per_region_hourly,
        }

    return {
        "last_night": period_stats(
            ln_start, ln_end,
            f"{NIGHT_START}:00 yesterday \u2192 {NIGHT_END:02d}:00 today"
        ),
        "today": period_stats(
            td_start, td_end,
            f"{NIGHT_END:02d}:00 today \u2192 now"
        ),
    }
