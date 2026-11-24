# api_docs changelog

## 2026-11-24 (host-neutral wording)
- `stealth/pi-headless.md` renamed `stealth/no-gui-container.md`. Docs, comments and `deploy/fbgb-headless/` no longer
  target a specific device: the image is for any host without a GUI. No behavior or measured result changed.

## 2026-11-24 (no-GUI validation in Docker, linux/arm64)
- `stealth/pi-headless.md` (now `stealth/no-gui-container.md`) rewritten with measured results: the custom seccomp profile is needed for the sandbox,
  Mesa WebGL flags, openbox work area, PulseAudio sink, maximized window. Detectors all green except the accepted
  "Chromium" brand. CreepJS like-headless 38% (Mac 31%). New tool `explore/tools/envprobe.py`. Facebook not used.

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
- 2026-11-24: assisted login (`fbgb login --stdin`) verified in the Docker image: login form `input[name=email|pass]`,
  Enter → `/two_step_verification/authentication/` for ~5 s, then home with `c_user`+`xs`. No checkpoint.
