# Address autocomplete

[← back to index](./README.md)

The location search used by the Yad2 filter box. This is the **only** location-lookup endpoint
the site exposes — there is no "list all hoods" endpoint, so this is a **ranked search box, not an
enumerator** (see [Caps](#caps)).

## Request

```
GET https://gw.yad2.co.il/address-autocomplete/realestate/v2?text=<url-encoded Hebrew>
```

- **`text`** — the query. **Hebrew only**; Latin/English input returns empty arrays.
- Matching is **fuzzy + token-based** over the place's full title (e.g. `צפון ישן` → both Old-North
  halves; `רמת` → places containing "רמת").

## Response

```jsonc
{
  "hoods":    [ { "fullTitleText": "הצפון הישן - צפון, תל אביב יפו",
                  "cityId": "5000", "hoodId": "1483",
                  "areaId": "1", "topAreaId": "2", "regionId": "3",
                  "regionHeb": "תל אביב והסביבה" } ],
  "cities":   [ { "fullTitleText": "תל אביב יפו",
                  "cityId": "5000", "areaId": "1", "topAreaId": "2",
                  "regionId": "3", "regionHeb": "תל אביב והסביבה" } ],
  "areas":    [ { "fullTitleText": "אזור תל אביב יפו",
                  "areaId": "1", "topAreaId": "2", "regionId": "3",
                  "regionHeb": "תל אביב והסביבה" } ],
  "topAreas": [],
  "streets":  [ { "fullTitleText": "תל גיבורים, תל אביב יפו",
                  "streetId": "2173", "cityId": "5000", "areaId": "1",
                  "topAreaId": "2", "regionId": "3",
                  "regionHeb": "תל אביב והסביבה" } ],
  "regions":  [ { "fullTitleText": "תל אביב והסביבה",
                  "regionId": "3", "regionHeb": "תל אביב והסביבה" } ]
}
```

Field notes:

- All ids are **strings**. Each item carries the **full ancestor chain** of ids, so one pick gives
  you everything you need for the [feed](./listings-map-feed.md).
- `fullTitleText` is `"<place>, <city>"` (or `"<place>, <city-area>"`); the leading segment before
  the first comma is the place's own name.
- `streets` items include a `streetId`; UI rule: **choosing a street is exclusive** (no other
  location filters alongside it).

## Caps

**The server caps each category at ~5 results** and ignores paging params:

| Query | hoods | cities | areas | streets |
|-------|-------|--------|-------|---------|
| `רמת` | 2 | 4 | 3 | 0 |
| `נווה` | 4 | 4 | 0 | 1 |
| `שכונת` | 5 | 0 | 0 | 4 |

`&limit=50`, `&size=50`, `&maxResults=50` **have no effect** — identical results. So:

- To **find a place**, type more specifically (narrower text → the place you want ranks in the top 5).
- To **enumerate all hoods of a city** (for a browse UI), autocomplete cannot do it — harvest hood
  names from the [map feed](./listings-map-feed.md) instead (see
  [tel-aviv-neighborhoods](./tel-aviv-neighborhoods.md)).

## Bot usage

- Normalize the user's text first (English-alias table + `rapidfuzz`) → Hebrew, then query
  (`locations.resolve_candidates`), ranking hits by fuzzy match score + a level bonus (city/hood
  ranked above area/street/region) so the most useful pick surfaces first.
- Present candidates grouped by category (hood / city / area / street) with `fullTitleText`.
- Keep the full candidate object (all ids) server-side; reference it by index in callback data.
- `locations.batch_autocomplete(terms)` unions candidates across several queries (dedup by
  level+ids) to beat the ~5-per-category cap — implemented and available, but currently unused:
  no handler calls it, and `scripts/harvest_locations.py`'s own sweep hits the endpoint directly
  rather than going through it.
- Results are cached in-process (`_AC_CACHE`, keyed by the normalized Hebrew query text) since
  location text → candidates is stable. Capped at 2000 entries with no eviction — once full, new
  queries simply stop being cached (old ones are never displaced).
