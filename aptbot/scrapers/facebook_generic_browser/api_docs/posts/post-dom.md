# A post in the DOM (fallback only)

Last verified: 2026-11-24 (snapshot `-094608_group/0033_snap_group_scrolled`)

Prefer graphql. The DOM is for **clicking** (finding things to click) and for health checks, not for data.

Inside a content-bearing post slot (`[role="feed"] > div[aria-labelledby]`):
- author: `a[href*="/groups/<gid>/user/<uid>/"]` (appears 3×: avatar, name, hover target)
- relative time ("38 minutes ago") + audience icon. The timestamp link's real href appears only on hover (lazy).
  Use graphql `creation_time` / `permalink_url` instead.
- body: truncated around 9 lines with a **"See more"** `[role="button"]` (Hebrew UI: "ראה עוד"). Not needed: graphql has the full text.
- auto-translation: when the post language ≠ UI language, the DOM shows the **translation** with
  "See original · Rate this translation". graphql has both.
- photos: `a[href*="/photo/?fbid="][href*="set=pcb.<post_id>"]` with `<img>`
- action bar: Like / Comment / Share buttons, then a "Comment as <account name>" textbox (`[role="textbox"]`).
  **Never focus or type in it.**
- about 56–59 `[role="button"]` and 11–12 links per post. Class names are generated (`x1lziwa`…), never use them.
