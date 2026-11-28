# Facebook in oxide

[← index](README.md) · related: [input-and-scrolling](input-and-scrolling.md), [memory](memory.md), [caveats](caveats.md), [security](security.md)

The production Chrome scraper's FB docs are in `~/TomerShit/APT-APT/aptbot/scrapers/facebook_generic_browser/api_docs/`
(feed, posts, comments, multi-tab, stealth). They still apply. This page covers what is **different or extra in oxide**.

## Rules

- Use a logged-in session from `profiles/` (how it was made: [security](security.md#session)). Never automate the login
  through Arkose: a human solved it once.
- Human pacing, wheel scrolling, low volume. Never focus or type in a comment box. Never like, comment or share.
- On a checkpoint, "temporarily blocked", or a login wall: **stop and tell the user. Never retry** ([checkpoints](#checkpoints)).
- Don't run the Playwright/Chrome session on the same account while oxide runs.

## What works

The logged-in home feed, group feeds (infinite scroll), "See more", post dialogs (comments), permalink pages, and
`/groups/<gid>/posts/<post_id>/`. The fixes that made FB boot are in [engine-changes](engine-changes.md)
(IndexedDB, `setAttributeNS`, IntersectionObserver, scroll, `compareDocumentPosition`, focus).

## Feed structure as seen in oxide

- The feed is `[role=feed]`. Its children are **post slots**, but in oxide the slots have **no `aria-labelledby`**
  (Chrome's do). Select slots by content instead:
  ```js
  [...document.querySelectorAll('[role=feed] > div')].filter(d => d.querySelector('[aria-label^="Comment on "]'))
  ```
- **Virtualized.** Only about 3 slots near the viewport keep their content; the others are emptied (height kept,
  `textContent` ''). Collect post data **while a slot is near the viewport**, or (better) from graphql.
- **Stable post key** (no DOM marking; survives reloads and "See more"): FB tags post parts with
  `data-ad-rendering-role`. Use `profile_name` text + the first 60 chars of `story_message` + the first 40 of `title`
  (listing posts have a title and no message). **Don't** use `description` (it holds "listed 3h ago" and location, and changes
  between loads), raw `textContent` (relative times, reaction counts), or the first `/user/` link (it can be a
  commenter). Verify any key change with `tools/keydiff.py GROUP_URL` (collects keys, reloads, diffs; prints
  hashes only).
- The real post id appears in `location.pathname` (`/groups/<gid>/permalink/<id>/`) while its dialog is open. Use it
  to check dedup.
- Controls inside a slot (aria-labels): `Actions for this post by <name>`, `React with Like to <name>'s post`,
  `Comment on <name>'s post`, `Share <name>'s post`. "See more" / "See original" are `[role=button]` with that text.
  Listing (marketplace) posts link to `/commerce/listing/<id>/`.
- The timestamp link's href is lazy (filled in on hover); take post ids from graphql instead.

## Getting post data

Feed pages arrive as `GroupsCometFeedRegularStoriesPaginationQuery` (3 posts each, full text), and a dialog as
`CometSinglePostDialogContentQuery` (about 270 KB with comments). `research/fb_capture.js` counts `"post_id":"…"` from
XHR/fetch responses. It's an **init script that wraps XHR/fetch, so pages can see it** (research only). A production
scraper should tap responses engine-side (oxide's network layer), not by patching page globals.

## "See more"

```js
const more = [...slot.querySelectorAll('[role=button]')].find(x => /^(See more|ראה עוד)$/.test(x.textContent.trim()));
H.input.click(more)
```
It expands in place; read 1.5–4 s.

## Opening a post (comments dialog)

Click `[aria-label^="Comment on "]` inside the slot. FB `pushState`s `/groups/<gid>/permalink/<id>/`, mounts a modal
layer with 2 `[role=dialog]` nodes, and loads `CometSinglePostDialogContentQuery`. Close it with `press('Escape')`
(FB calls `history.go(-1)`).

Two oxide-specific problems and what to do about them:
1. **The scroll position is lost on close.** FB leaves the feed at `scrollY=0`; Chrome restores it.
   **Workaround (in fb_deep_session.py):** remember `scrollY` before the click. After Escape, if it dropped,
   `H.viewportScroll(0, saved)`. That's a programmatic scroll, the same thing FB's own `scrollTo` does in Chrome.
   Root cause analysis: [caveats](caveats.md#post-dialog-scroll-reset).
2. **Escape is sometimes swallowed.** If `[role=dialog]` is still there, click the last `[role=dialog] [aria-label=Close]`.

Memory: every opened dialog leaves ~16 MB live heap in oxide (~6 MB in Chrome: FB's SPA leaks). Plan for page
recycling ([memory](memory.md#long-sessions)).

## Permalink pages / extra tabs

`/groups/<gid>/posts/<post_id>/` embeds the post and the first ~10 comments in the HTML. A second `Browser()` can load it
as a background "tab", but **each tab is a full FB app: +250 MB**, so one feed tab plus one permalink tab
(~650 MB) is over the Pi budget. Use permalinks *instead of* the feed (sequentially, one Browser), not alongside it.
Numbers: [memory](memory.md#tabs).

## Checkpoints

`fb_deep_session.py` `blocked()` stops when `location.pathname` matches `/checkpoint|login/` or the body text has
"temporarily blocked", "confirm your identity" or "we suspended". Check it after every navigation and after each post.
Then stop the session and hand it to the user.

## Layout differences that matter

- On the home page the left nav is stacked above the feed, so the first posts load only after the first scroll.
- Group pages: the first post slot is at y≈400 after the initial auto-scroll; slots are ~2300 px tall (oxide's layout
  is taller than Chrome's), so a "screen" holds less than in Chrome. Pace by slots, not by pixel counts copied from Chrome.
