# Yad2 API Reference (unofficial)

Reverse-engineered reference for the Yad2 real-estate **gateway** API used by the
APT-APT apartment bot. Every fact here was **verified live** against the production
gateway (see [Provenance](#provenance)), not guessed.

> Scope: **rent** real-estate feed (`category=2`, `subcategory=2`). Sale/other verticals
> share the gateway shape but are out of scope for this bot.

---

## Table of contents

1. [Base URL & conventions](#base-url--conventions)
2. [Endpoint index](#endpoint-index)
3. [Quick reference](#quick-reference)
4. [Detailed docs](#detailed-docs)
5. [How the bot uses this](#how-the-bot-uses-this)
6. [Provenance](#provenance)

Detailed per-topic pages:

| Page | What's in it |
|------|--------------|
| [`location-taxonomy.md`](./location-taxonomy.md) | The 5-level region→hood hierarchy + verified id table |
| [`address-autocomplete.md`](./address-autocomplete.md) | The location search endpoint (type → candidates) |
| [`listings-map-feed.md`](./listings-map-feed.md) | The listings feed, filters, 200-cap, marker schema |
| [`tel-aviv-neighborhoods.md`](./tel-aviv-neighborhoods.md) | Harvested 56-hood Tel Aviv catalog + quarters |
| [`harvesting-all-areas.md`](./harvesting-all-areas.md) | Reproducible way to enumerate every area filter in Israel |
| [`anti-bot-radware.md`](./anti-bot-radware.md) | Radware behavior & how to actually reach the API |

---

## Base URL & conventions

- **Gateway base:** `https://gw.yad2.co.il`
- **Site base:** `https://www.yad2.co.il` (HTML pages — Radware-protected, see [anti-bot](./anti-bot-radware.md))
- All gateway endpoints return **JSON**. Text params are **URL-encoded Hebrew**.
- Requests must be sent with **full browser headers** + a valid session/Radware cookie
  when made programmatically (see [anti-bot](./anti-bot-radware.md)).

---

## Endpoint index

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/address-autocomplete/realestate/v2?text=<he>` | Fuzzy location search → region/area/city/hood/street ids |
| GET | `/realestate-feed/rent/map?region=…&<filters>` | Listings feed (markers) for a location + filters |
| GET | `/realestate-feed/rent/list?…` | Paginated list variant (site uses `map`; see notes) |

Everything location-related on the Yad2 site is powered by just the **autocomplete**
(to pick a place) and the **map feed** (to fetch listings). **There is no
"list all neighborhoods of a city" endpoint** — the site is search-driven.

---

## Quick reference

```
# Search for a place (Hebrew only)
GET https://gw.yad2.co.il/address-autocomplete/realestate/v2?text=פלורנטין

# Fetch rent listings for Tel Aviv-Yafo, 3-4 rooms, ₪6000-9000, with elevator
GET https://gw.yad2.co.il/realestate-feed/rent/map?region=3&city=5000&minRooms=3&maxRooms=4&minPrice=6000&maxPrice=9000&elevator=1
```

- **Region is required** on the map feed. `area`/`city`/`neighborhood` are optional narrowers.
- **`neighborhood=` takes a single id** (comma-lists return empty).
- **Feed is capped at 200 markers**, no pagination, no total → bisect price bands or tile by `bBox`.
- Markers give hood **names, not ids** → match hoods **locally by name**.

---

## Detailed docs

See the per-topic pages linked in the [table of contents](#table-of-contents).

---

## How the bot uses this

- **Location picking** → [`address-autocomplete`](./address-autocomplete.md): user types Hebrew
  (with English-alias + rapidfuzz pre-normalization), we surface candidates with their ids.
- **Browsing Tel Aviv sub-neighborhoods** → we can't enumerate hoods from the API, so the bot
  ships a **curated catalog** ([`tel-aviv-neighborhoods.md`](./tel-aviv-neighborhoods.md)) grouped
  into quarters for tap-to-browse; free-text search still uses autocomplete.
- **Browsing any other city's neighborhoods** → no bundled catalog exists beyond Tel Aviv
  (`data/hoods/5000.json` is the only per-city file shipped), so tapping a city with no bundled
  quarters falls back to a **live one-request hood fetch** (`locations.fetch_city_hoods` →
  unfiltered `map` feed for that city, capped at 200 markers, distinct `neighborhood.text`
  collected as a flat un-curated list — no quarter grouping).
- **Scraping** → [`listings-map-feed`](./listings-map-feed.md): scrape once per location signature
  with the loosest union of filters, then match each listing per-user locally (hoods by name).

---

## Provenance

Captured **2026-07-15** via the Chrome DevTools Protocol against a real logged-in Chrome
(remote-debugging port 9222): `fetch()` was executed from the `https://www.yad2.co.il`
page origin (so requests carried the live Radware/session cookie), and responses were read
back over CDP `Runtime.evaluate`. Marker/response schemas are copied from actual 200 responses.
