# API reference: `FacebookAPI`

Last verified: 2026-11-24 (live runs on the Mac; numbers below are measured)

```python
from aptbot.scrapers.facebook_generic_browser import FacebookAPI

async with FacebookAPI(max_tabs=3) as fb:
    ...
```
- Every method is a **coroutine, safe to call concurrently** from any number of tasks. Browser work is scheduled
  over up to `max_tabs` tabs of one account ([concurrency.md](concurrency.md)).
- **"Cost"** = navigations charged to the account-wide budget (default 8/min, burst 3, 120/hour). Scrolling is free.
- **Cache**: results are stored in SQLite (`$FBGB_HOME/fbgb.db`). Methods with `max_age` answer from the cache when
  the data is younger than that. Pass `max_age=0` to force a fresh read.
- **Times are UTC.** Every returned datetime is timezone-aware UTC. `since=` accepts an aware datetime or unix
  seconds; pass aware datetimes (`datetime.now(timezone.utc) - timedelta(hours=3)`), not naive local times.
- Every browser-backed method accepts `timeout=` (seconds, default 240). On timeout it raises `RequestTimeout`
  and the tab is freed.
- Errors common to every browser-backed method: `Halted` (account-safety stop), `NotLoggedIn`, `BudgetExhausted`,
  `RequestTimeout`, `SchemaDrift`. See [errors-and-safety.md](errors-and-safety.md).
- Chrome starts lazily on the first browser-backed call and closes after `idle_close_after` idle seconds.

## Constructor
```python
FacebookAPI(*, home=None, profile_dir=None, max_tabs=3, limiter=None, idle_close_after=600,
            default_timeout=240, debug_dump=False)
```
| Param | Meaning |
|---|---|
| `home` | state dir: cache DB, `HALTED` marker, debug captures. Default `$FBGB_HOME` or `~/.fbgb` |
| `profile_dir` | Chrome profile holding the logged-in session. Default `$FBGB_PROFILE_DIR` or `<home>/profile` |
| `max_tabs` | parallel tabs (3 on a Mac, 2 on a low-CPU host). Light load opens only 1 |
| `limiter` | `Limiter(rate_per_min=8, burst=3, hourly_cap=120)`, shared by all tabs |
| `idle_close_after` | seconds of inactivity before Chrome is closed (`None` = keep open) |
| `default_timeout` | per-call timeout for browser-backed calls |
| `debug_dump` | write raw payloads to `<home>/captures/` (they contain real posts; debugging only) |

---

## `status() -> Status`
Cost 0. Never starts Chrome. `Status{state: "stopped"|"ok"|"halted", logged_in, halt_reason, tabs_busy, max_tabs,
queued, budget_left, started_at}`.

## `resume() -> None`
Clears the circuit breaker (`HALTED` marker). Call it only after a human has checked the account in a normal browser.

## `list_groups(*, max_age=600) -> list[Group]`
Every group of the account: members **and pending join requests** (check `join_state`).
Cost 1 (`/groups/joins/`, one full page load, ~2 s). `last_post_time` is filled for the 10 most recently active
member groups (Facebook's left rail). The rest have `None` (unknown, not "no posts").
```
305724686290054  MEMBER     last post 2026-11-24 08:39  דירות להשכרה בתל אביב
2065942027026008 REQUESTED  last post ?                  דירות מציאה מפייס לאוזן בתל אביב המאגר המלא
```

## `get_group(group_id, *, max_age=3600) -> Group`
One group's metadata: `name, url, privacy ("public"/"private"), join_state, member_count ("139.8K members"),
description`. Cost 1 (the group page). `group_id` is the numeric id (a vanity slug works for navigation too).

## `latest_posts(group_id, *, limit=30, since=None, max_age=60, priority=INTERACTIVE) -> list[Post]`
Newest-first posts of one group with **full text** (no "See more" needed), original + translated text, author,
photos (full-size URLs), comment/reaction counts and **inline comment previews**. `since` (datetime or unix) keeps
only posts created after it and stops scrolling once whole batches are older.
Cost 1 + scrolling. Measured: **15 posts in 11.9 s (~76/min)** in one tab. Facebook delivers 3 posts per page.
Raises `NotMember` for a private group the account can't read (e.g. a pending request).

## `stream_posts(group_id, *, limit=100, since=None) -> AsyncIterator[Post]`
Same as `latest_posts`, but yields each post as soon as its batch arrives (and caches it). `break` at any time:
the scrolling is cancelled and the tab freed.
```python
async for post in fb.stream_posts(gid, since=last_seen):
    handle(post)
```

## `new_posts(group_ids=None, *, per_group_limit=50, first_run_limit=10) -> list[Post]`
Incremental polling across groups (default: every `MEMBER` group). Each call returns only posts created after
the previous call (per-group watermark in the cache). Groups whose `last_post_time` hasn't moved past their
watermark are **skipped without being opened**. The changed groups are read **concurrently**. The first call for a
group returns its newest `first_run_limit` posts. Cost: 1 (`list_groups`) + 1 per changed group.

## `search_groups(query, *, limit=20) -> list[GroupHit]`
Facebook search → Groups filter, typed like a person (top search box, Enter, "Groups"). `GroupHit{id, name, url,
summary ("Public · 43K members · 30+ posts a day"), privacy}`. Cost 1–2.

## `search_group(group_id, query, *, limit=30) -> list[Post]`
Facebook's search inside one group (`/groups/<id>/search/?q=`). Results are **relevance-ordered**, not by date.
Verified: "פלורנטין" → 6/6 matching posts in 10.9 s. Cost 1 + scrolling.

## `get_post(group_id, post_id, *, with_comments=True) -> Post`
One post from its permalink page (the post is embedded whole in the page: cost 1, ~3–8 s). With comments: the
comments embedded on the page (verified: **10 of 14**). Facebook hides the rest behind "View more comments",
which isn't clicked yet (**Unverified/not implemented**, see SPEC open questions). `post.comments` is a list of
`Comment{id, text, created_at, author_id, author_name, reply_to}`.

## `get_comments(group_id, post_id) -> list[Comment]`
Shortcut for `(await get_post(..., with_comments=True)).comments`.

## `cross_group_feed(*, limit=30) -> list[Post]`
"Your feed" (`/groups/feed/`): recent posts across all joined groups, in **Facebook's ranking** (not chronological).
Cost 1 + scrolling.

## `filter_posts(*, group_ids=None, include=None, exclude=None, since=None, has_photos=None, author_id=None, min_price=None, max_price=None, min_rooms=None, max_rooms=None, limit=100) -> list[Post]`
**Cache only, never touches Facebook.** Newest first. `include`: every word must appear (original or translated
text, case-insensitive). `exclude`: none may. Price/rooms use `post.hints` (best-effort text extraction). Posts
without a hint are dropped when a price/rooms bound is given.

## `close()`
Closes Chrome cleanly (flushes cookies) and the cache. `async with` does this for you.

---
Priorities (`from aptbot.scrapers.facebook_generic_browser import Priority`): `INTERACTIVE` (default) >
`POLL` (used by `new_posts`) > `BACKGROUND`. Higher priorities jump the queue. Running jobs aren't preempted.
