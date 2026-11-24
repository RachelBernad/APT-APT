# Transport: HTTP, auth, anti-bot, limits

Last verified: 2026-11-24 (curl and Python urllib/aiohttp from macOS, no browser state)

## Auth
- **None.** No token, API key, cookie, Referer, or Origin is needed for MapServer `query`
  or the `/metadata/*.json` files. A bare `curl` works.

## Edge / anti-bot
- The server sits behind **F5 Distributed Cloud** (`x-volterra-location`, `x-envoy-upstream-service-time`).
  It sets `f5xctlvlbcookie` and a `TS01…` cookie (BIG-IP ASM style). Neither is needed on
  later requests. There is no challenge page and no JS check.
- 12 parallel `returnCountOnly` queries all returned 200 in 0.4–1.1 s. **Unverified:** a
  sustained-rate limit may exist; stay polite (bounded concurrency).

## Responses
- `content-type: application/json; charset=UTF-8`.
- **Gzip quirk:** some endpoints (e.g. the folder listing `/rest/services/WM?f=json`) return
  `content-encoding: gzip` **even when the request has no `Accept-Encoding`**. Clients must
  honor `Content-Encoding` (aiohttp does). Naive readers that ignore the header get binary.
- `/metadata/*.json` files start with a **UTF-8 BOM**. Decode with `utf-8-sig`.
- `cache-control: max-age=3600, s-maxage=3600, public` plus `x-cache-status` / `x-proxy-cache`
  headers. In practice (2026-11-24) repeated **identical** query URLs, error responses included, all came back
  `x-cache-status: MISS`, so the edge is not caching query results. **Unverified** for other paths
  (e.g. `/metadata/*.json`). Don't rely on the cache either way: a recovered layer is visible immediately.

## Errors
- A bad statistics field (e.g. `onStatisticField=OBJECTID` on a layer whose OID field has
  another name) returns **HTTP 200** with body
  `{"error":{"code":400,"extendedCode":-2147467259,"message":"Unable to complete operation.","details":[]}}`.
  Always check for an `error` key; the HTTP status alone is not enough.
- An unknown field in `where` and a layer outage return the **same** body
  (`"Failed to execute query."`). See [operations/query.md](../operations/query.md#the-same-error-body-means-bad-query-and-layer-down).

## Outages
A single layer can go down while everything around it looks healthy. Observed 2026-11-24 on
528 (town plans) and 683 (city-wide plans), which share one database table:
- `…/MapServer/<id>?f=json` (layer metadata) still answers normally, so a metadata-only health
  check misses it. In this case the field list had also changed (a field was removed), so the
  layer was probably republished against a broken or changed source. **Unverified** cause.
- Every `query` on the layer fails, including `where=1=1&returnCountOnly=true`, `returnIdsOnly`,
  `objectIds=…` and `returnExtentOnly`, with `{"error":{"code":400,"message":"Failed to execute query."}}`.
- `identify` and `find` return `{"results":[]}` (no error), and `export` renders the layer as an
  empty image. The web viewer silently shows nothing.
- The viewer's own cache files built from that table (`/metadata/tabaot0.json`…`tabaot2.json`) contain
  the same error body instead of data, so the viewer has no fallback either.
- The same layer ids on **every** service fail identically (`IView2WM`, `IView2`, `IView2WM____test`,
  `IView2____test`, `Test/IView_Internet[WM]`): they all read one source. Switching service does not help.
- All other layers on the same service keep working.

How to handle it:
- Detect it with a `1=1` count probe (see above), and report it as an outage, not as "no results".
  `catalog.py` prints `UNAVAILABLE` and exits 2.
- Don't retry in a tight loop: this outage lasted at least the whole hour we watched it, not seconds. Callers should degrade
  (skip that layer, flag "unknown") and try again on a later run.
- When it recovers, re-run `catalog.py`: compare the field list with the layer page, because a
  republish can rename or drop fields.

## Data freshness
- Most construction layers carry a `date_import` string (`DD/MM/YYYY HH:MM:SS`, Israel time).
  On 2026-11-23 layers 772/499/479/858/433/852 all showed that day's early-morning import,
  i.e. **nightly refresh**. The light-rail layers (760–763, 633) and metro (954) were last
  imported 2022–2025.
