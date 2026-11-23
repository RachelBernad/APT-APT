# Location taxonomy

[← back to index](./README.md)

Yad2 models Israeli geography as a **5-level hierarchy**. Every level has a numeric id
(except that inside listing markers only `region` carries an id — see
[listings-map-feed](./listings-map-feed.md)).

## The five levels

```
region (מחוז / district)          ← required on the listings feed
└─ topArea                        ← coarse grouping, not accepted as a feed filter
   └─ area (אזור / metro area)
      └─ city (עיר)
         └─ neighborhood (שכונה)  ← "hood"; also where sub-neighborhoods live
                                    (street (רחוב) hangs off city, parallel to hood)
```

- **region** — top-level district. Required param on the map feed.
- **topArea** — an intermediate grouping id returned by autocomplete; **not** honored as a feed filter.
- **area** — metro/sub-region (e.g. "אזור תל אביב יפו").
- **city** — a settlement.
- **neighborhood (hood)** — the finest level. Tel Aviv's hoods include **sub-neighborhoods**
  (e.g. `הצפון הישן - צפון` / `הצפון הישן - דרום`) — see [tel-aviv-neighborhoods](./tel-aviv-neighborhoods.md).
- **street** — hangs off a city; in the UI, picking a street is **exclusive** (disables other selections).

## Autocomplete result groups ↔ taxonomy

The location search dropdown shows up to four groups, which map 1:1 to the taxonomy and to the
[autocomplete](./address-autocomplete.md) JSON arrays:

| UI label | JSON array | Level |
|----------|-----------|-------|
| שכונה | `hoods` | neighborhood |
| עיר | `cities` | city |
| אזור | `areas` | area |
| רחוב | `streets` | street |
| (region shown as context) | `regions` | region |

## Verified id table

Captured live from autocomplete + feed responses:

| Level | id | Hebrew |
|-------|----|--------|
| region | 3 | תל אביב והסביבה |
| topArea | 2 | (Tel Aviv/Center super-group) |
| area | 1 | אזור תל אביב יפו |
| city | 5000 | תל אביב יפו |
| hood | 312 | תל ברוך |
| hood | 1483 | הצפון הישן - צפון |
| hood | 1461 | הצפון הישן - דרום |
| region | 1 | מרכז והשרון |
| city | 8300 | ראשון לציון (area 9) |
| region | 5 | מישור החוף הצפוני |
| area | 6 | נשר והקריות |
| city | 9500 | קרית ביאליק (hood 103) |

## Regions (מחוזות)

**8 region ids the bot's code knows about**, per the bundled catalog (`data/il_locations.json`, built
by `scripts/harvest_locations.py`'s nationwide autocomplete sweep) and its `REGION_NAMES` map. Regions
1–7 were all confirmed by the sweep tying at least one live city/area to that `regionId`; region 8 is
a name-only fallback (see note below) — not independently confirmed:

| region id | Hebrew |
|-----------|--------|
| 1 | מרכז והשרון |
| 2 | דרום |
| 3 | תל אביב והסביבה |
| 4 | יהודה, שומרון ובקעת הירדן |
| 5 | מישור החוף הצפוני |
| 6 | ירושלים והסביבה |
| 7 | צפון והעמקים |
| 8 | ירושלים |

> **Region 8 (`ירושלים`) is unconfirmed live** — it only appears in the bundled catalog because
> `scripts/harvest_locations.py` hard-codes a `REGION_NAMES` fallback map (used to name any region id
> the sweep observed without a `regionHeb`) and unconditionally seeds all 8 of its entries into the
> catalog. **Zero cities or areas in the sweep actually resolved to region 8** — the real city of
> Jerusalem (`cityId=3000`) sits under **region 6** (`ירושלים והסביבה`, `areaId=7`) instead. Until an
> autocomplete/feed response is caught with a live `regionId=8`, treat region 8 as a name-only
> placeholder, not a confirmed usable district id.

## Practical notes for the bot

- To resolve any place → ids, use [autocomplete](./address-autocomplete.md) and read the ids off
  the matching item (`regionId`, `areaId`, `cityId`, `hoodId`, `topAreaId`).
- On the feed, **`region` is mandatory**; `area`/`city`/`neighborhood` narrow it. `topArea` is ignored.
- Markers only echo `region.id`; city/area/hood come back as **text**, so the bot matches hoods
  **locally by normalized name** rather than by id.
- Name normalization (`models.normalize_name`) strips whitespace, casefolds, and rewrites
  **`קריית` → `קרית`** before comparing — Yad2 spells the same city/hood both ways in different
  places (e.g. "קריית ביאליק" vs. "קרית ביאליק"), so without this the two spellings would never match.
