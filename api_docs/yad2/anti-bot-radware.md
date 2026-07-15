# Anti-bot (Radware) & how to reach the API

[← back to index](./README.md)

Yad2 fronts its site with **Radware Bot Manager** ("Verifying your browser before proceeding…").
Understanding what is and isn't protected is what makes the scraper reliable.

## What's protected vs. open

| Surface | Protection | Usable? |
|---------|-----------|---------|
| `gw.yad2.co.il/*` gateway JSON (autocomplete, feed) | Passes with a valid session cookie + real browser headers | ✅ **Yes** — the bot's data source |
| `www.yad2.co.il` HTML pages (search, **item/detail**) | Radware JS challenge | ⚠️ Blocks automation-flagged clients; item pages unreliable even for a real browser |

Consequence: **item-page enrichment is not viable.** Per-listing detail (description, tags, and
per-marker verification of amenities) **cannot be fetched**. But this does **not** mean those filters
are unavailable — every feature, **including ממ״ד (`shelter=1`)**, is filterable **server-side on the
feed** (see [listings-map-feed](./listings-map-feed.md)). The bot therefore:

- applies **all feature filters server-side** (elevator, balcony, ממ״ד/`shelter`, parking, …), making
  them part of the scrape signature since markers don't echo the flags back,
- does **not** try to re-verify a feature per marker or read item-page-only fields,
- formats messages from **marker fields only** (plus the features it filtered on), omitting unknowns.

## Making gateway calls succeed programmatically

The gateway rejects "naked" requests. To succeed:

1. Send a **full browser header set** — `User-Agent` (current Chrome), `Accept: application/json`,
   `Accept-Language`, `Sec-Ch-Ua*`, `Sec-Fetch-*`, `Origin: https://www.yad2.co.il`,
   `Referer: https://www.yad2.co.il/`.
2. Carry cookies from a **warmed session** (a browser that has passed the Radware challenge). Reuse a
   shared cookie jar across requests.
3. Accept **Brotli/gzip** (`Accept-Encoding: br, gzip`) and decode.
4. **Detect the challenge**: if a response is HTML / redirects to `validate.perfdrive.com` (or a
   `Verifying your browser` body), treat it as a challenge, refresh the session, and retry with backoff.

The bot's `yad2_gateway.gateway_headers()` implements (1); `_is_challenge()` (checks for
`perfdrive`/`__uzdbm`/`captcha`/an HTML doctype in a non-JSON response, or HTTP 403) plus
`_get_markers()`'s retry loop implement (4): **up to 4 attempts**, backing off ~1s × attempt number
on a detected challenge/403, exponential (`2**attempt`, capped at 3s) on a 5xx, and exponential
(uncapped) on a network/JSON-decode error — giving up and returning an empty result only after the
4th attempt.

## How this reference was captured

The MCP's own bundled Chrome fails the Radware JS challenge (automation fingerprint). Working method
used for this doc set:

1. Launch **real Chrome** with remote debugging:
   `Google Chrome --remote-debugging-port=9222 --user-data-dir=<profile>` and surf to Yad2 once so
   the profile holds a valid Radware cookie.
2. Find the tab target via `http://127.0.0.1:9222/json`.
3. Drive it over CDP (`Runtime.evaluate`, `awaitPromise`) and run `fetch(...)` **from the
   `www.yad2.co.il` page origin**, so calls inherit the live cookie. (Node 24's built-in `WebSocket`
   speaks CDP with no extra deps.)

This is the reliable way to re-verify endpoints when the API shape changes.
