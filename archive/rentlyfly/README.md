# rentlyfly.ai client (archived)

`client.py` is the former top-level `rentlyfly.py`, moved here unchanged on
2026-11-23 because **rentlyfly.ai is offline**: every URL, including
`/api/listings`, returns `HTTP 402 Payment required — DEPLOYMENT_DISABLED`
(Vercel suspended the deployment).

## What was known about the API (last verified 2026-07)

- `GET https://rentlyfly.ai/api/listings?page=&limit=&minPrice=&maxPrice=&minRooms=&maxRooms=&minFloor=&maxFloor=`
  → `{data: [...], pagination: {hasMore}}`, newest first.
- Covered **Tel Aviv-Yafo only** (~5,400 listings aggregated from Facebook groups).
- Each item: `id`, `price`, `roomsAvailable`, `description`, `photos`, `url`,
  `group_name`, nested `location {city, area, hood, street, latitude, longitude}`,
  inline amenity flags (`isMamad`, `isElevator`, `isBalcony`, `isAirConditioner`,
  `isRenovated`, `isFurnished`, `isParking`, `isPetsAllowed`), `isSharedApartment`, `isSublet`.
- Behind Cloudflare: force `Accept-Encoding: gzip, deflate` (Brotli responses
  failed to decode in the container).

## How it was wired in (to revive)

1. Move `client.py` to `aptbot/scrapers/rentlyfly/client.py`; fix imports
   (`from aptbot import config`, `from aptbot.models import GatewayFilters`) and
   re-add the `RENTLYFLY_*` settings to `aptbot/config.py`
   (`RENTLYFLY_API_URL`, `RENTLYFLY_PAGE_LIMIT=50`, `RENTLYFLY_MAX_PAGES=4`, `ENABLE_RENTLYFLY`).
2. In `aptbot.engine.scrape_group`, after the Yad2 markers:
   ```python
   if config.ENABLE_RENTLYFLY and sig.city_id == config.TEL_AVIV_CITY_ID:
       for listing in await rentlyfly.fetch_tel_aviv(http, f):
           listings.setdefault(listing["uid"], listing)
   ```
   (uids are prefixed `rentlyfly:` so they never collide with Yad2's.)
3. Move the tests back under `tests/unit/` and `tests/live/` and fix their imports.

The old per-search `source_mode` opt-out (`auto` / `yad2` / `yad2+rentlyfly`) was
removed with it; re-add it only if per-user source choice is wanted again.
