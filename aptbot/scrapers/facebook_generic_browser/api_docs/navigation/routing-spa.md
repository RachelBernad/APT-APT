# Routing: full loads vs. in-app (SPA) navigation

Last verified: 2026-11-24

Facebook web ("Comet") is a single-page app.
- **Full load** (typed URL / `page.goto`): a new HTML document (1.2–3 MB, 100–170 `data-sjs` JSON islands) plus about
  10 background graphql queries (`../graphql/queries/background-noise.md`). The first feed page and group metadata come
  **inside the HTML**, not as graphql.
- **In-app click** (e.g. the left-nav Groups link → `/groups/feed/`): no document request. The route's data arrives as
  graphql (`GroupsCometCrossGroupFeedContainerQuery`). URL changes via `history.pushState`.

Implications:
- Parse both sources: HTML islands for whatever the first paint shows, graphql for everything after.
- Prefer clicking links once a session is running (cheaper, more natural). One typed URL per job is fine
  (bookmarks/typing are human), and much faster than hunting for a link.
- `page.wait_for_load_state("load")` is meaningful only for full loads. For SPA navigation, wait for the
  expected graphql response or a landmark selector.
