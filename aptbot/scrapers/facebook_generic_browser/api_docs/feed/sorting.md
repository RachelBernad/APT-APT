# Feed sorting

Last verified: 2026-11-24

- `?sorting_setting=CHRONOLOGICAL` on the group URL → the graphql variable `sortingSetting: "CHRONOLOGICAL"` → posts strictly
  newest → oldest (30 posts verified). **Use this for scraping.** It makes incremental runs trivial (stop at the
  watermark).
- Default ("Most relevant") is algorithmic and interleaves old popular posts.
- The UI sort control is the first `[role="feed"]` child (button with `aria-haspopup`). Other values (e.g. "New activity",
  `RECENT_ACTIVITY`) are **Unverified**.
