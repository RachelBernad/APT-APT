# Concurrency: many callers, several tabs, one account

Last verified: 2026-11-24 (offline tests `tests/unit/test_fbgb_dispatcher.py`, `test_fbgb_api.py`. Live: `examples/concurrent.py`)

## The model
```
callers (any number of asyncio tasks)
   │  await fb.latest_posts(g1)   await fb.search_groups(q)   async for p in fb.stream_posts(g2)
   ▼
Dispatcher (dispatcher.py)
   • single-flight: identical concurrent calls share ONE job (same op + args)
   • per-group exclusion: two jobs on the same group never run at the same time
   • priority queue: INTERACTIVE > POLL > BACKGROUND, FIFO within a priority
   • timeouts / cancellation: when every caller gave up, the job is dropped or cancelled and its tab freed
   ▼
max_tabs tab workers: each owns ONE tab and runs one job at a time. Tabs open lazily (light load = 1 tab)
   │
   ├─ Attention (tabs.TabPool.foreground): a FIFO lock held only for short INPUT bursts (1–3 wheel flicks, a
   │  click, typing). Taking it brings the tab to the front. A hidden tab never gets input.
   ├─ Between bursts a job waits for the NETWORK without holding attention, so the other tabs take their turn.
   │  Network waits overlap; input is serialized, like one person switching tabs quickly.
   ├─ Limiter: one account-wide navigation budget for all tabs (8/min, burst 3, 120/hour)
   └─ Health: AccountAtRisk in any tab → breaker HALTED → every queued/running call fails with Halted
```

## Measured (live, Mac, 2026-11-24)
| Scenario | Result |
|---|---|
| 2 groups' `latest_posts` (12 each) + `search_groups`, 3 tabs | wall 76.6 s vs 157.2 s sequential → **2.05× faster**, 11 tab switches, 0 warnings |
| single `latest_posts` (15), request-aware waits | 11.9 s (~76 posts/min), 1 switch |
| hidden tabs | keep receiving responses (15–30 of 44–64 graphql arrived while hidden). Feed pagination fires only while the feed is scrolled (i.e. in front) |

(That first run predates the search wait fix: search took 76.6 s of it. Expect a lower wall time now.)

## What callers can rely on
- Calls never interfere: each gets its own tab, or waits in the queue.
- Calling the same thing twice at once costs Facebook once.
- Two different groups really do run in parallel. The same group is serialized (it's one feed).
- `asyncio.wait_for(fb.x(...), t)` or `timeout=t` frees the tab when it fires.
- After a `Halted`, nothing touches Facebook until `resume()`, even across process restarts.

## What they can't
- Parallelism isn't free: input is serialized, so N tabs give less than N× speed (~2× with 3 tabs above).
- `max_tabs` doesn't raise the account budget. More tabs = the same number of navigations, just overlapped.
- Order of results across different calls isn't guaranteed (priority + FIFO only decide who starts first).

## Tuning
`FacebookAPI(max_tabs=…)`: 3 on a Mac, 2 on a low-CPU host (CPU-bound). `jobs.Env` holds the scroll timing knobs:
`request_wait` (0.35–0.7 s look for a pagination request after a burst), `response_timeout` (8 s),
`max_stalls` (6 bursts without new posts = end of feed).
