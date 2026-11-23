# Listings feed (rent/map)

[← back to index](./README.md)

The endpoint that returns actual rental listings for a location + filter set. This is what the
site's results **map** uses; the bot uses it as its primary scraper.

## Request

```
GET https://gw.yad2.co.il/realestate-feed/rent/map?region=<id>&<narrowers>&<filters>
```

### Location params

| Param | Required | Notes |
|-------|----------|-------|
| `region` | **yes** | District id. Omitting it → **HTTP 400** (re-verified 2026-11-23). |
| `area` | no | Metro area id. |
| `city` | no | City id. |
| `neighborhood` | no | **Single hood id only.** A comma list (`1483,1461`) returns HTTP 200 but **0 markers** → to cover several hoods, request the city and match hoods locally, or issue one request per hood. |
| `bBox` | no | `lat1,lng1,lat2,lng2` bounding box (map/pan mode). |
| `zoom` | no | Map zoom level; pairs with `bBox`. |

### Filter params (server-side, verified honored)

**Ranges & property:** `minPrice`, `maxPrice`, `minRooms`, `maxRooms`, `minSquaremeter`,
`maxSquaremeter`, `minFloor`, `maxFloor`, `property` (property-type id; comma list = OR),
`propertyCondition` (id).

> **How the bot actually uses `property` vs. `propertyCondition`:** the bot pushes `property`
> server-side (unioned across a scrape group's watchers — see [README](./README.md#how-the-bot-uses-this)),
> but **never sends `propertyCondition` to the gateway** — `yad2_gateway._build_params()` has no
> handling for it and `engine.union_filters()` never populates `GatewayFilters.property_condition`.
> Condition is instead matched **locally** against each marker's `additionalDetails.propertyCondition.id`
> (`engine.matches_non_amenity()`). The param is real and gateway-honored (confirmed live), the bot
> just doesn't happen to use it server-side today.

**Property-type ids** (`property=<id>`, comma list = OR):

| id | Hebrew |
|----|--------|
| 1  | דירה |
| 3  | דירת גן |
| 4  | סטודיו/לופט |
| 5  | בית פרטי/קוטג' |
| 6  | גג/פנטהאוז |
| 7  | דופלקס |
| 11 | יחידת דיור |
| 39 | דו משפחתי |
| 49 | מרתף/פרטר |
| 51 | טריפלקס |

**Property-condition ids** (`propertyCondition=<id>`; ids observed on markers are 1, 2, 3, 5, 6 — note 4 is skipped).
Re-verified 2026-11-23 by reading "מצב הנכס" off real item pages for markers of each id, and against the
site's own filter checkboxes (`realestate-property-condition-*` testids). **An earlier version of this table
(and of `models.PROPERTY_CONDITIONS`) had 2/3/5/6 scrambled** — fixed.

| id | Hebrew |
|----|--------|
| 1 | חדש מקבלן (לא גרו בנכס) |
| 6 | חדש (עד 10 שנים, גרו בנכס) |
| 2 | משופץ |
| 3 | במצב שמור |
| 5 | דרוש שיפוץ |

(Both tables are `models.PROPERTY_TYPES` / `models.PROPERTY_CONDITIONS` verbatim.)

**Boolean features (`=1`)** — read straight off the site's own filter URL:

| Param | Feature | Param | Feature |
|-------|---------|-------|---------|
| `elevator` | מעלית | `bars` | סורגים |
| `parking` | חניה | `warehouse` | מחסן |
| `balcony` | מרפסת | `accessibility` | גישה לנכים |
| `airConditioner` | מיזוג | `assetExclusive` | בבלעדיות |
| `renovated` | משופצת | `pets` | חיות מחמד |
| `furniture` | מרוהטת | `imageOnly` | עם תמונה (ad) |
| `shelter` | **ממ״ד** (apartment safe room) | `priceOnly` | עם מחיר (ad) |
| `floorShelter` | **ממ״ק** (floor safe room) | | |
| `buildingShelter` | מקלט בבניין | | |

Verified by result-count drops (baseline 118 → `elevator` 52, `shelter` 32, `floorShelter` 5, …)
and confirmed against the live filter URL. Re-verified 2026-11-23 (Florentin, hood 205: base 144 →
`elevator` 112, `shelter` 64, `floorShelter` 50, `warehouse` 12, …), and every range filter's returned
markers were checked to actually satisfy it.

**More filters (found on the site 2026-11-23, accepted by the feed, not used by the bot yet):**

| Param | Meaning | Check (Florentin base 144) |
|-------|---------|------|
| `priceDropped=1` | נכסים שמחירם ירד (price dropped) | 30 |
| `settlements=1` | רק מושבים וקיבוצים | 0 in TLV (as expected) |
| `minSquareMeterBuild` / `maxSquareMeterBuild` | built area (separate from `minSquaremeter`) | `min…=60` → 93 |
| `text=<he>` | free-text search (חיפוש חופשי) | `text=גינה` → 6 |

The site also has a "3D tour" checkbox and an entry-date picker ("תאריך כניסה" + "כניסה קרובה") whose
feed param names were **not** found (`is3D`, `entranceDate`, `immediate`… all 400). `sort` and `page` are
accepted but have no effect on the map feed.

### Param-name validation

The server **validates param names and returns HTTP 400 for unknown ones** — e.g. `mamad`, `saferoom`,
`petsAllowed`, `furnished`, `exclusive` all 400. The **safe room is `shelter=1`, not `mamad`**; pets is
`pets`, not `petsAllowed`; furnished is `furniture`; exclusive is `assetExclusive`. Use the exact
names above.

### Notes on features vs. markers

Feature filters work server-side, but markers carry **no** feature flags, so a listing is only known
to have the features you filtered on. This is why the bot makes feature requirements part of the
**scrape signature** (pushed server-side) rather than matching them locally. `topArea` is not a feed
filter (it's a taxonomy id only).

## Response envelope

```jsonc
{
  "data": {
    "markers":          [ /* the listings — see schema below */ ],
    "yad1Markers":      [ ],
    "clusters":         [ ],
    "yad1Promotions":   [ ],
    "agencyPromotions": [ ],
    "grayMarkers":      [ ]
  },
  "message": "..."
}
```

Use `data.markers`. There is **no pagination and no total-count field**.

The side lists (re-checked 2026-11-23, Florentin):
- `agencyPromotions` — **real rental listings** in the same marker shape (agency "early access" ads,
  ≤3 per response). ~60% of them are **not** also in `markers` (9 of 15 across 5 TLV hoods) → the bot,
  which reads only `markers`, misses them.
- `yad1Markers` — Yad1 new-project rentals (`projectId`, `price: null`, `metaData.projectName`) — not listings.
- `grayMarkers` — project pins (`projectName`, `developerName`), no price — not listings.

## Marker schema

```jsonc
{
  "address": {
    "region":       { "text": "תל אביב והסביבה", "id": 3 },   // only region has an id
    "city":         { "text": "תל אביב יפו" },                 // text only
    "area":         { "text": "אזור תל אביב יפו" },            // text only
    "neighborhood": { "text": "הדר יוסף" },                    // text only → match locally by name
    "street":       { "text": "קיציס" },
    "house":        { "number": 20, "floor": 1 },
    "coords":       { "lon": 34.820894, "lat": 32.107646 }
  },
  "subcategoryId": 2,
  "categoryId": 2,
  "adType": "private",              // or "agency"/"platinum" etc.
  "price": 8000,                     // number (coerce; can be missing on some ads)
  "token": "fbwyx63a",              // the listing id → item URL
  "additionalDetails": {
    "property":          { "text": "דירה" },
    "roomsCount": 4,
    "squareMeter": 88,
    "propertyCondition": { "id": 2 }
  },
  "metaData": {
    "coverImage": "https://img.yad2.co.il/...jpeg",
    "images": [ "https://img.yad2.co.il/...jpeg" ],
    "squareMeterBuild": 88
  },
  "tags": [ { "name": "נוף פתוח לים", "id": 1004, "priority": 1 } ],   // ad highlight chips
  "orderId": 57103736,
  "priority": 1
}
```

> **Changed since the first capture (seen 2026-11-23):** `address.city` and `address.neighborhood` now
> also carry an **`id`** (`{"id": 5000, "text": "תל אביב יפו"}`, `{"id": 205, "text": "פלורנטין"}`; hood id
> present on ~92% of markers). The city id here is an **unpadded int** (`70` for Ashdod, whose real id is
> `"0070"`) — never compare it to a target's string id without padding. Markers also gained a **`tags`**
> list. The bot still matches hoods by normalized name, which keeps working.

- **`token`** is the listing id. Item page: `https://www.yad2.co.il/realestate/item/<token>`
  (Radware-blocked — do **not** rely on it for enrichment; see [anti-bot](./anti-bot-radware.md)).
- Markers carry **no tags/description and no `mamad`/`elevator` flags** — those live only on the
  (blocked) item page.

## Caveats

### 200-marker cap
The feed returns **at most 200 markers**, with no pagination and no total. A tight filter
(`minPrice=8000&maxPrice=9000&minRooms=3`) still returned exactly 200 → **silent truncation** when a
location has more matches. Mitigations:

- **Price-band bisection** — split `[minPrice,maxPrice]` recursively until each band returns < 200.
  The bot (`yad2_gateway.fetch_map`) bisects until the band is done, hits `config.MAP_MAX_REQUESTS`
  (default 16 requests per scrape group, to bound load), or narrows below `config.MAP_MIN_PRICE_BAND`
  (default ₪250) — whichever comes first. Hitting either bound logs a warning that the scope may still
  be truncated ("tighten the search filters").
- **`bBox` tiling** — pan a grid of bounding boxes over the area (used to harvest the
  [Tel Aviv hood catalog](./tel-aviv-neighborhoods.md) — 4–7 tiles yielded 1,400 markers / 97 hoods).
  Note: the bot's live scrape path (`fetch_map`) only does price bisection, never `bBox` tiling —
  tiling is a harvest-time technique (`scripts/harvest_locations.py`), not part of the regular scrape.

### Hoods have no id on markers
Only `region` echoes an id. City/area/hood are text. → The bot scrapes by `region`(+`city`) and
**matches hoods locally** on normalized `neighborhood.text`. This is also why "monitor many hoods"
needs no server support: fetch the city once, filter hoods in code.

### The site's results list (not reachable from the bot)
`GET /realestate-feed/rent/list` returns **404** (re-checked 2026-11-23). The site's results *list* is
**server-rendered** into `www.yad2.co.il/realestate/rent/...` (`__NEXT_DATA__` →
`dehydratedState` query `realestate-rent-feed`): 40 items/page grouped by ad tier (`yad2`, `platinum`,
`kingOfTheHar`, `trio`, `booster`, `leadingBroker`) plus **`pagination: {total, totalPages}`**, and each item
has **`dates.approvedAt`** (a real publish timestamp) and `inProperty`. That HTML is behind the Radware JS
challenge, so the bot can't use it; the map feed stays the scrape path.

## Bot usage

- Scrape **once per distinct location signature** across all users, with the **loosest union** of
  members' filters, then match each user's exact filters locally.
- For a saved search that targets **many locations**, batch one feed request per location target and
  union the markers (dedup by `token`). Not bound by any browser multi-select cap.
- Apply price-band bisection whenever a response hits 200.
