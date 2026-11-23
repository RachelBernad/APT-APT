# Transport: HTTP, auth, anti-bot, limits

Last verified: 2026-11-23 (curl and Python urllib from macOS, no browser state)

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
- `cache-control: max-age=3600, s-maxage=3600, public` plus `x-cache-status` / `x-proxy-cache`:
  query results may be up to 1 h stale through the CDN.

## Errors
- A bad statistics field (e.g. `onStatisticField=OBJECTID` on a layer whose OID field has
  another name) returns **HTTP 200** with body
  `{"error":{"code":400,"extendedCode":-2147467259,"message":"Unable to complete operation.","details":[]}}`.
  Always check for an `error` key; the HTTP status alone is not enough.

## Data freshness
- Most construction layers carry a `date_import` string (`DD/MM/YYYY HH:MM:SS`, Israel time).
  On 2026-11-23 layers 772/499/479/858/433/852 all showed that day's early-morning import,
  i.e. **nightly refresh**. The light-rail layers (760–763, 633) and metro (954) were last
  imported 2022–2025.
