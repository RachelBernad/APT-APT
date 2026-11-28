# aptbot integration: oxide behind fbgb

```
bot ── fb_engine.fb_loop (8-12 min, quiet 01-07) ── FacebookAPI(browser=FB_BROWSER)
                                                     ├─ chrome: dispatcher/tabs/jobs (unchanged)
                                                     └─ oxide:  oxide/client.py OxideBackend
                                                                  │ line-JSON over stdin/stdout
                                                                  ▼
                                                        oxide/worker.py (subprocess) → driver.py + jobs.py
```

- **Files:** `aptbot/scrapers/facebook_generic_browser/oxide/`, with `events.py` (tap records → `capture.Event`), `health.py`, `driver.py`, `jobs.py`, `worker.py` and `client.py`.
- **Reused unchanged:** parse.py, store.py (watermarks and diffing), and models.
- **Why a subprocess:** `Browser` calls block, so they stay off the bot's event loop. A V8 OOM kills only the worker.
- **Data source:** the engine network tap ([engine-changes.md#network-tap](engine-changes.md#network-tap)). No page-visible init script.
- **Worker lifetime:** recycled after any call that leaves it above 460 MB, killed above 650 MB (watchdog), and always closed at the end of `new_posts`.
- **Errors:** `account_at_risk` writes HALTED and the bot stops for good. `not_member` becomes `NotMember`. Timeout or kill becomes `RequestTimeout` for that group, with no retry inside the cycle.
- **Session:** the worker loads `BROWSER_OXIDE_COOKIE_JAR` and saves it on shutdown. To move a Chrome session over, use `tools/chrome_to_jar.py`. Use only one browser per session.
- **Config:**
  - `FB_BROWSER`
  - `FB_OXIDE_JAR`
  - `FB_POLL_MIN/MAX_SECONDS`
  - `FB_QUIET_HOURS`
  - `FBGB_OXIDE_RSS_LIMIT_MB`
- **Deploy:** `deploy/aptbot-oxide/` (multi-stage image, no Xvfb).
- **Not on oxide yet:** search_groups, search_group, cross_group_feed (they raise `FBGBError`), and streaming (`stream_posts` yields after a single call).

## Measured (Mac, test account, 2026-11-28)
- **One worker, 3 groups × 15 posts:** about 85 s per group. Worker peak 521 → 556 → 584 MB, which is why the worker is now recycled.
- **list_groups:** 14 groups in 31 s.
- **Full cycle, 6 groups:** 60 posts in 919 s, peak 545 MB.
- **Second cycle right after (diffing):** 2 new posts in 146 s, peak 529 MB. The fast pass works because groups stop scrolling at the stored watermark.
- **Photos:** 12 of 12 URLs return image/jpeg without cookies, so Telegram can fetch them by URL.
  - **Open:** the feed gives at most 5 photos per post, even when `photo_count` is 6–12. `get_post` doesn't fill in the rest yet.
- **Comments:** `get_post` on the permalink parsed 3 of 3 comments, with the same text as the feed.
  - Replies and "View more comments" remain untested: no test post had enough comments.
- **Album fill (one group, 10 posts):** albums with 6–12 photos were completed. The extra photos are full size (900–1280 px wide) and their URLs are fetchable. 294 s, peak 486 MB.
- **Container (`deploy/aptbot-oxide`, linux/arm64, OrbStack, `--memory=1g`):**
  - list_groups 31 s.
  - One group: 15 posts and 81 photos, albums filled, in 335 s. Worker peak 506 MB.
  - It needs the glibc TLS preload ([caveats.md#linux-tls](caveats.md#linux-tls)).
  - Image size: 482 MB.
