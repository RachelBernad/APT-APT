# Post permalink page

Last verified: 2026-11-24 (debug captures `20261124-103532`, `-1040xx`; `jobs.get_post`)

URL: `/groups/<gid>/posts/<post_id>/` (canonical, from `permalink_url`). `/groups/<gid>/permalink/<post_id>/` is the older form.

- **The post is embedded whole in the page HTML** (`data-sjs` islands): the full Story node with text, photos and counts.
  No graphql is needed for it. Cost: 1 full load, ~3–8 s.
- The HTML also embeds a couple of **other** Story nodes (related posts). Select by `post_id`.
- **Comments:** the first ~10 comments come embedded with the post (verified: 10 of 14). The rest are behind a
  "View more comments" button. Scrolling does **not** load them (no comment query fired in 3 scroll bursts).
  Clicking that button is **Unverified/not implemented**.
- ⚠️ **Below the post the page shows an unrelated feed.** Scrolling into it fired 3× `CometNewsFeedPaginationQuery`
  (~7 MB of other posts). Only scroll on a permalink when comments are missing, and stop when none arrive (`jobs.get_post`).
