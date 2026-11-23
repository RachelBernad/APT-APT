# Harvesting every area filter (reproducible)

[← back to index](./README.md)

**Question:** can we enumerate *every* location filter Yad2 supports (all regions/areas/cities/hoods
in Israel), and script it? **Answer: yes** — proven live. Autocomplete has no enumeration and caps at
~5 results, so we don't ask it for "everything" — we **sweep** it, and we **tile the feed** for hoods.

## Method 1 — Autocomplete prefix sweep (cities / areas / regions, nationwide)

Autocomplete ranks the top ~5 matches for a `text` query. Sweep a space of short Hebrew prefixes and
**union the results by id**; overlap is removed by the id keys.

### Verified constraints & yields

- **Minimum query length = 2.** Single-letter queries return **empty** (tested all 22 letters → 0).
- **2-letter prefixes are productive.** A batch of **52** two-letter prefixes (13 initials × 4 second
  letters) returned **151 distinct cities, 24 areas, 135 hoods, 0 empty, 0 errors**.
- Extrapolation: the **full 2-letter space = 22×22 = 484 prefixes** → captures the large majority of
  Israel's real-estate localities. Add a **targeted 3-letter** pass for the long tail (prefixes whose
  2-letter parent hit the 5-result cap, i.e. likely truncated).

### Algorithm

```
letters = 22 Hebrew base letters (א..ת)
seen_city = {}, seen_area = {}, seen_region = {}
for p in all 2-letter combos (and 3-letter for truncated parents):
    d = GET /address-autocomplete/realestate/v2?text=urlencode(p)   # warmed session
    for c in d.cities:  seen_city[c.cityId]   = c            # full id chain incl. region/area
    for a in d.areas:   seen_area[a.areaId]   = a
    for r in d.regions: seen_region[r.regionId] = r
    pace(~40ms)
# result: complete {cityId → {names, regionId, areaId, topAreaId}} etc.
```

- **~484 cheap GETs** (plus a few hundred 3-letter for completeness). Paced, this is a couple of
  minutes and safe.
- Each item carries the **full ancestor id chain**, so the sweep also yields the region/area tree.

## Method 2 — Feed bBox tiling (complete hood list for a city)

Autocomplete surfaces *some* hoods but caps; the **complete** hood set for a city comes from the
[map feed](./listings-map-feed.md). Tile `bBox` boxes over the city and collect distinct
`neighborhood.text` where `city.text == "<city>"`.

- Verified for Tel Aviv-Yafo: **10 tiles → 2,000 markers → 56 distinct hoods** (after filtering out
  spillover from Ramat Gan / Holon / Bat Yam / Givatayim / Azor).
- Limitation: only hoods with **active listings** at harvest time appear → re-harvest periodically.
- Hoods have **no id** on markers → key them by **normalized name** (the bot's local match key).

```
for tile in grid(city_bbox):
    d = GET /realestate-feed/rent/map?region=<r>&city=<c>&bBox=<tile>&zoom=14
    for m in d.data.markers:
        if m.address.city.text == CITY: hoods.add(m.address.neighborhood.text)
```

## Method 3 — Streets: on-demand only

Streets number in the hundreds of thousands nationwide and picking one is exclusive in the UI. Do
**not** pre-harvest — resolve via autocomplete when a user actually types a street.

## Reproducibility & where it runs

Both methods are **pure gateway calls** and need a **warmed session** (valid Radware cookie + browser
headers — see [anti-bot](./anti-bot-radware.md)). Run them either:

- **In-process** via the bot's cookie'd `aiohttp` session (the same one the scraper uses), or
- **Offline** via the CDP-real-browser method (fetch from the `www.yad2.co.il` origin), which is how
  this reference was produced.

### Implemented artifact

`scripts/harvest_locations.py` **exists and runs both methods**: Method 1 (2-letter + 3-letter
prefix sweep) writes `data/il_locations.json` (regions/areas/cities with id chains); Method 2
(`bBox` tiling, hard-coded to the 10 Tel Aviv tiles) writes the hood list printed for
`data/hoods/5000.json` (the quarter grouping inside it stays hand-curated — the script prints hood
names for review, it does not overwrite the curated file automatically).

Run `python scripts/harvest_locations.py` (full sweep + Tel Aviv hoods) or
`--skip-sweep` (Tel Aviv hoods only, faster). It hard-codes a `REGION_NAMES` map of all
**8 region ids** (autocomplete rarely echoes `regionHeb` for every region on its own), which is the
source-of-truth region table reflected in [location-taxonomy.md](./location-taxonomy.md#regions-מחוזות).

The bot ships the resulting bundled catalog:

- `data/il_locations.json` — regions/areas/cities with id chains (from the sweep).
- `data/hoods/<cityId>.json` — per-city hood catalogs (from tiling); **only Tel Aviv (5000) is
  bundled today**.

`locations.py` uses the bundled catalog for **tap-to-browse** where available, falls back to a
**live one-request hood fetch** off the map feed for any other city (see [README](./README.md#how-the-bot-uses-this)),
and falls back to **live autocomplete** for anything not resolvable that way (and for streets), so
coverage is never blocked on the catalog being complete.
