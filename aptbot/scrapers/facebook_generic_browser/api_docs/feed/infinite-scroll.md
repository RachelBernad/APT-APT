# Feed infinite scroll & virtualization

Last verified: 2026-11-24 (captures `-094608_group`, `-094808_harvest`)

## DOM
`[role="feed"]` has one child `div` per feed slot:
- a header slot (sort control),
- **post slots**: `div` with `aria-labelledby="<13 generated ids>"` (useful as "is a post slot"),
- trailing loader slots containing `[role="article"]` **skeletons** (`[role="status"]`, empty text).

**Virtualization:** only about 3 post slots near the viewport have content. Slots that scrolled away are emptied and get
`data-virtualized`, keeping only their height. In the snapshot after 6 scrolls: 19 children, 3 with content.
⇒ `role="article"` no longer marks posts (it marks loading placeholders), and you **cannot** collect posts from the DOM
at the end of a scroll. Read graphql (`../graphql/queries/GroupsCometFeedRegularStoriesPaginationQuery.md`).

## Pagination trigger
The next `GroupsCometFeedRegularStoriesPaginationQuery` fires when the bottom loader enters the viewport. Measured with
900–1600 px wheel scrolls: about one request per 2 scrolls, 3 posts per request.

## Fast, event-driven loop (validated: 30 posts in 49.6 s, no checkpoint)
```
loop:
  wheel(900..1600 px in uneven ticks)
  wait until the next feed response arrives OR timeout 2.5–3.5 s   # never a fixed sleep
  micro()                                                       # 40–350 ms
  stop when: wanted N posts | a post older than the watermark | 4 stalls in a row (end of feed)
```
Half of the scrolls in the measured run timed out (no new request needed yet). Tuning to scroll on immediately when the
loader isn't near (e.g. a shorter 1.0–1.5 s timeout) should reach about 60 posts/min. **Unverified.**

## Request-aware loop (verified 2026-11-24: 15 posts in 11.9 s ≈ 76/min, 1 tab)
After each burst, look 0.35–0.7 s for an outgoing `GroupsCometFeed…` **request** (`context.on("request")`). If one
went out, wait for its response (timeout 8 s). If none did, the sentinel isn't in view yet, so scroll again
immediately. End of feed = 6 consecutive bursts without new posts. Code: `jobs.collect_scrolling`.
