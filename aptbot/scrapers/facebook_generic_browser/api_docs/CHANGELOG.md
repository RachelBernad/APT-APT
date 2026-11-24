# api_docs changelog

## 2026-11-24 (afternoon: library verification, 3 live runs)
- Verified: permalink page embeds the post + ~10 comments (`posts/post-permalink.md`, `posts/comments.md`). In-group
  search works (`groups/group-search.md`). `/groups/joins/` alone gives the rail + full list (`groups/joined-groups-list.md`).
  Hidden tabs receive responses and feed pagination only fires in front (`feed/multi-tab.md`). Request-aware scrolling ≈ 76 posts/min.
- Found + fixed: scrolling a permalink loads an unrelated feed (~7 MB). Global search is an in-app navigation (no document).
  The rail/full-list order varies between loads (merge bug).

## 2026-11-24
- Documented the Groups hub (`groups/groups-hub.md`) from the existing capture: `GroupsCometLeftRailContainerQuery` gives
  `last_post_time` per member group, the basis for skipping unchanged groups. Hub search/Discover/See-all not yet explored.
- Initial exploration on the logged-in account (5 sessions): home, `/groups/feed/`, `/groups/joins/`, a public apartment
  group (default + CHRONOLOGICAL), a private group (pending membership), and search ("דירה להשכרה" → Groups).
- Found: the group feed is virtualized and `role=article` marks skeletons only. The feed graphql carries full post text
  (no See-more needed). `/groups/joins/` includes pending requests. The UI auto-translates posts.
- Not yet observed: permalink page, comments dialog, a private group as a member, hidden-tab Facebook behavior.

## 2026-11-23
- Framework validation (no Facebook): Patchright + real Chrome + `IGNORED_DEFAULT_ARGS` + `focus_control` + sandbox
  passes rebrowser, sannysoft, BrowserScan and CreepJS. Vanilla Playwright flagged as Robot. See `stealth/browser-setup.md`.
