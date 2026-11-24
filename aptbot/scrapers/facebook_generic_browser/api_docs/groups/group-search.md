# Finding groups (search) and searching inside a group

Last verified: 2026-11-24 (capture `-094937_deep`, query "דירה להשכרה")

Human flow (what `explore/session.py deep` does): click `input[type="search"]` in the banner → type the query with
per-key jitter → Enter → `/search/top?q=…` → click the **Groups** filter link (`a[href*="/search/groups"]`) →
`/search/groups/?q=…` → scroll for more.

Data: see `../graphql/queries/SearchCometResults.md`. Each result gives the id, name, url and a summary line
`"<Public|Private> · <N> members · <activity>"`, so you can pick groups to join without opening them.
The Groups vertical offers a **City** filter (`filter_groups_location`). **Unverified** how to drive it.

## Inside one group (`/groups/<gid>/search/?q=<q>`)
Last verified: 2026-11-24 ("פלורנטין" → 6/6 matching posts, 10.9 s). A full load. The first ~5 results are
embedded as Story nodes in the HTML, and more arrive as `SearchCometResultsPaginatedResultsQuery` while scrolling (same
family as global search, Story nodes inside). Results are **relevance-ordered**, not by date. Code: `jobs.search_in_group`.

## Global search timing note
After Enter, `/search/top` is an **in-app navigation (no document)**. Wait for `SearchCometResults*` instead of a page load
(fixed 2026-11-24: waiting for a document wasted 15 s).
