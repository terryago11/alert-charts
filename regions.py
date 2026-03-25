"""
Region grouping and color configuration for the night alert chart.

All zone names come directly from the Israeli Homefront Command (oref.org.il)
via the pikud-haoref-api cities.json.  This file only defines:
  - ZONE_GROUP   : maps each oref zone_en string → broad display group
  - GROUP_COLORS : color per display group
  - NIGHT_START / NIGHT_END : local-time hours that define "night"
"""

# Hours that count as "night" (24-hour clock, inclusive start/exclusive end)
NIGHT_START = 22   # 10 PM
NIGHT_END   = 6    # 6 AM

# ── Broad groupings used purely for bubble colour ──────────────────────────
# All unique zone_en values from pikud-haoref-api/cities.json:
#   Northern Golan, Golan South, Confrontation Line,
#   Upper Galilee, Center Galilee, Lower Galilee, Beit She'an Valley,
#   HaAmakim, Bika'a,
#   HaMifratz (The Bay), HaCarmel, Menashe, Wadi Ara,
#   Dan, Yarkon, Sharon, HaShfela, Lachish, Jerusalem, Judea Foothills,
#   Judea, Yehuda, Shomron,
#   Gaza Envelope, West Negev,
#   Center Negev, Southern Negev,
#   Aravah, Dead Sea,
#   Eilat

ZONE_GROUP: dict[str, str] = {
    # ── Golan ──────────────────────────────────────────────────────────────
    "Northern Golan":      "Golan",
    "Golan South":         "Golan",
    "Confrontation Line":  "Golan",

    # ── Galilee ────────────────────────────────────────────────────────────
    "Upper Galilee":       "Galilee",
    "Center Galilee":      "Galilee",
    "Lower Galilee":       "Galilee",
    "Beit She'an Valley":  "Galilee",
    "HaAmakim":            "Galilee",
    "Bika'a":              "Galilee",

    # ── Haifa / Bay Area ───────────────────────────────────────────────────
    "HaMifratz (The Bay)": "Haifa Area",
    "HaCarmel":            "Haifa Area",
    "Menashe":             "Haifa Area",
    "Wadi Ara":            "Haifa Area",

    # ── West Bank (Judea & Samaria) ────────────────────────────────────────
    "Judea Foothills":     "West Bank",
    "Judea":               "West Bank",
    "Yehuda":              "West Bank",
    "Shomron":             "West Bank",

    # ── Central Israel ─────────────────────────────────────────────────────
    "Dan":                 "Central",
    "Yarkon":              "Central",
    "Sharon":              "Central",
    "HaShfela":            "Central",
    "Lachish":             "Central",
    "Jerusalem":           "Central",

    # ── Gaza Envelope / Western Negev ──────────────────────────────────────
    "Gaza Envelope":       "Gaza Area",
    "West Negev":          "Gaza Area",

    # ── Negev / Beer Sheva ─────────────────────────────────────────────────
    "Center Negev":        "Beer Sheva / Negev",
    "Southern Negev":      "Beer Sheva / Negev",

    # ── Arava / Dead Sea ───────────────────────────────────────────────────
    "Aravah":              "Arava",
    "Dead Sea":            "Arava",

    # ── Eilat ──────────────────────────────────────────────────────────────
    "Eilat":               "Eilat",
}

GROUP_COLORS: dict[str, str] = {
    "Golan":              "#2ca02c",   # dark green
    "Galilee":            "#98df8a",   # light green
    "Haifa Area":         "#17becf",   # teal
    "West Bank":          "#9467bd",   # purple
    "Central":            "#1f77b4",   # blue
    "Gaza Area":          "#d62728",   # red
    "Beer Sheva / Negev": "#ff7f0e",   # orange
    "Arava":              "#ffbb78",   # pale orange
    "Eilat":              "#bcbd22",   # yellow-green
}
