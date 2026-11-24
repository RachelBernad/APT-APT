# SearchCometResults* (global search)

Last verified: 2026-11-24 (capture `20261124-094937_deep`)

Flow when a user types in the top search box and presses Enter:
1. `CometSearchKeywordDataSourceQuery`: typeahead, several times while typing.
2. Enter → `/search/top?q=<q>` → `SearchCometResultsInitialResultsQuery` + `…ParallelFetchQuery` (ads in parallel).
3. Clicking the **Groups** filter (`a[href*="/search/groups"]`) → `/search/groups/?q=<q>` → initial results again,
   then `SearchCometResultsPaginatedResultsQuery` per scroll.

## Variables
`args.callsite = "COMET_GLOBAL_SEARCH"`, `args.text = <query>`, `args.context.{bsid,tsid}` (per-search session ids),
`args.experience.type` (vertical, e.g. groups), `cursor`, `count`.

## Group result fields (`data.serpResponse.results.edges[]`)
| Field | Path |
|---|---|
| kind | `node.role` = `ENTITY_GROUPS` |
| group id / name / url | `rendering_strategy.view_model.profile.{id,name,url}` |
| summary line | `rendering_strategy.view_model.primary_snippet_text_with_entities.text`, e.g. `Public · 43K members · 30+ posts a day` (privacy · size · activity, localized) |
| next page | `data.serpResponse.results.page_info.has_next_page` (+ `end_cursor`) |
| filters | `results.filters[].filters[].main_filter.{name,text}`, e.g. `filter_groups_location` / "City" |

The summary line is the cheapest way to tell public from private and to rank groups by activity before joining.
