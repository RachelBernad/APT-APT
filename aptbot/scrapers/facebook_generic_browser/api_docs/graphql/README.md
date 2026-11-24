# `/api/graphql/`: how Facebook's frontend talks to its backend

Last verified: 2026-11-24 (captures `20261124-094403_joined`, `-094608_group`, `-094808_harvest`, `-094937_deep`)

The Comet (React + Relay) frontend fetches nearly all data from `POST https://www.facebook.com/api/graphql/`
(form-encoded). **We never send these requests ourselves.** We read the responses the page fetches anyway
(`context.on("response")`, `explore/capture.py`). This page explains how to recognise and parse them.

## Request (form body)
| Field | Meaning | Use for us |
|---|---|---|
| `fb_api_req_friendly_name` | Relay operation name, e.g. `GroupsCometFeedRegularStoriesPaginationQuery` | **the routing key** for parsers |
| `doc_id` | persisted-query id; **changes on frontend deploys** | log only, never key on it |
| `variables` | JSON: cursor, count, group id, render location… | cursor/count/`id` for bookkeeping |
| `fb_api_caller_class` | `RelayModern` | n/a |
| `server_timestamps` | `true` | n/a |
| `fb_dtsg`, `lsd`, `jazoest`, `__user`, `av`, `__a`, `__req`, `__hs`, `__dyn`, `__csr`, `__rev`, `__s`, `__hsi`, `__spin_*`, `__comet_req`, `__ccg`, `dpr`, `__aaid`, `__crn`, `__hblp`, `__hsdp`, `__sjsp` | session/CSRF tokens + build fingerprint | **never stored** (capture.py keeps only the key names) |

Observed `variables` keys include many `__relay_internal__pv__*relayprovider` booleans (feature flags baked into the
query). They're noise for us.

## Response
- Optionally prefixed with `for (;;);` (anti-JSON-hijacking). Strip it.
- **One or more JSON objects separated by newlines.** The first is `{"data": …, "extensions": …}`. Later ones are Relay
  `@stream`/`@defer` chunks: `{"label": "...$stream$...", "path": [...], "data": …}` (e.g. one per streamed feed edge)
  and `{"label": "...$defer$...page_info", ...}` carrying `page_info` (`end_cursor`, `has_next_page`).
- Parse every line. Don't assume a single object. (`capture.split_chunks`)

## Initial page data is in the HTML, not graphql
A full page load (typed URL) returns HTML with ~100–170 `<script type="application/json" data-sjs>` islands that hold
the preloaded Relay store: the group header, privacy and member count, and the **first feed page**. In-app (SPA)
navigation fetches the same via graphql instead. See `../navigation/routing-spa.md`. `capture.html_json_blobs()` extracts them.

## Query catalogue (observed)
| friendly name | when | page |
|---|---|---|
| `GroupsCometFeedRegularStoriesPaginationQuery` | scrolling a group feed | [queries/GroupsCometFeedRegularStoriesPaginationQuery.md](queries/GroupsCometFeedRegularStoriesPaginationQuery.md) |
| `GroupsCometCrossGroupFeedContainerQuery` | `/groups/feed/` (all your groups) | [queries/GroupsCometCrossGroupFeedContainerQuery.md](queries/GroupsCometCrossGroupFeedContainerQuery.md) |
| `CometNewsFeedPaginationQuery` | home feed scroll | same Story schema as above |
| `GroupsCometLeftRailContainerQuery` | groups left rail (your groups list) | `../groups/joined-groups-list.md` |
| `SearchCometResultsInitialResultsQuery` / `…ParallelFetchQuery` / `…PaginatedResultsQuery` | search results | [queries/SearchCometResults.md](queries/SearchCometResults.md) |
| `CometSearchKeywordDataSourceQuery` | typeahead, once per typed character burst (11× for one query) | n/a |
| `CometHovercardQueryRendererQuery` | pointer hovers a name/avatar ≥ ~0.5 s | proof our pointer behaves like a real one |
| `useGroupsCometVisitMutation` | entering a group | Facebook logs the visit (normal) |
| background noise | every full page load | [queries/background-noise.md](queries/background-noise.md) |

Sample/`shape` tooling: `/tmp`-style helpers aren't committed. Use `explore/capture.py` outputs in `~/.fbgb/captures/`.
