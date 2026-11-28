# Caveats (and what to do)

[← index](README.md) · related: [input-and-scrolling](input-and-scrolling.md), [facebook](facebook.md), [memory](memory.md), [engine-changes](engine-changes.md)

Each entry has a **status**: ✅ fixed in the engine · 🛠 workaround (use it) · ⚠️ open.

## Interaction

### post-dialog-scroll-reset
🛠 After closing an FB post dialog, the feed is left at `scrollY=0` (Chrome restores it).
**Do:** save `scrollY` before opening, and after close call `H.viewportScroll(0, saved)` if it dropped
(`fb_deep_session.py`). This matches what FB's own `scrollTo` does in Chrome; normal scroll events fire.
**Why (partly solved):** FB's `BaseDocumentScrollView` keeps one active scroll layer. A new layer that comes later in
document order makes the earlier one "detached" (it saves its scroll and renders as `#scrollview`, `position:fixed`). Two causes were
oxide bugs and are fixed: `compareDocumentPosition` always said FOLLOWING ([engine-changes](engine-changes.md#cdp)),
and `focus()` ignored `preventScroll` ([engine-changes](engine-changes.md#focus)). It still differs: in oxide the
*dialog's* layer detaches instead of the feed's, because at mount it is briefly in the hidden set. Suspects, in order:
no Web Animations `finish`/`playState`, no CSS `transitionend`/`animationend` events, rAF vs timer ordering.
Tools: `tools/scrolltrace.js`, `cdp2.js`, `rdt.js`.

### escape-swallowed
🛠 `press('Escape')` sometimes leaves the dialog open. **Do:** if `[role=dialog]` is still present, click the last
`[role=dialog] [aria-label=Close]`.

### inner-scrollers
⚠️ Element scroll containers aren't modelled: `el.scrollTop` is stored but doesn't move children, and
`scrollHeight`/`clientHeight` = `offsetHeight`. `position:fixed; height:100%` boxes get their content height (or 0 with
inline styles). **Do:** scroll the window (`wheel`). Don't rely on scrolling inside FB modal or comment-list scrollers. Expect
"View more comments" style lazy lists inside dialogs not to trigger by scrolling; click their buttons instead.

### scrollintoview
✅ `scrollIntoView` was a no-op stub, so `click()` on an off-screen element clicked "through" at the wrong coordinates.
Now it really scrolls the viewport (start/center/end/nearest).

### layout-stacking
⚠️ Oxide's layout stacks some multi-column pages (FB home: the left nav sits above the feed). The first feed sentinel is
below the fold, so posts load only after the first scroll. Post slots are ~2300 px tall. **Do:** always do one `wheel` after load;
pace by "new slots appeared", not by Chrome pixel counts.

### select-elements
- Don't mark nodes with attributes to find them later (page-visible). Re-find them by content each time.
- `document.getElementsByTagName('*').length` returns 0 ⚠️. Use `querySelectorAll('*')`.
- FB post slots have no `aria-labelledby` in oxide. Select them by the `Comment on …` button ([facebook](facebook.md#feed-structure-as-seen-in-oxide)).

## Memory

### per-dialog-leak
🛠 Every opened FB post dialog leaves ~16 MB of live heap (Chrome: ~6 MB; FB's SPA keeps event handles and data).
**Do:** recycle, i.e. reload the page when RSS goes over budget (`RECYCLE_RSS=500`) ([memory](memory.md#long-sessions)).

### tabs-cost
⚠️ A second `Browser()` is a whole FB app: +250 MB. One feed tab + one permalink tab ≈ 650 MB. **Do:** one Browser at a time on the Pi.

### rss-not-returned
⚠️ macOS malloc doesn't return freed pages promptly. After closing a tab, RSS stays ~190 MB above where it started. Expect
better on Linux/glibc (untested). Measure on the Pi.

### heap-limit-death
⚠️ Going over `BROWSER_OXIDE_HEAP_MAX_MB` is a hard crash ("Fatal JavaScript out of memory"), not an exception.
Keep a margin, and recycle before it.

## Engine

### teardown
✅ Dropping a `Browser` used to abort the process ("thread local panicked on drop"; iframe isolates were dropped by
thread-local destructors after the main isolate). Fixed: frames are disposed when the engine thread exits.
Tabs can be closed mid-session. `os._exit(0)` at the very end is still a cheap way to skip interpreter teardown.

### console-empty
⚠️ The `console` verb returns nothing on FB. **Do:** hook `console.error/warn` in an init script (`tools/contrace.js`).

### idb-swallows
⚠️ Oxide's IndexedDB swallows exceptions thrown inside handlers. **Do:** wrap handlers in an init script to see them.

### net-log-slow
⚠️ `BO_NET_LOG=1` slows FB runs enough to hit timeouts. Use it only for short probes.

### navigation-api
⚠️ `window.navigation` (the Navigation API, in Chrome since v102) is **missing**. That's a fingerprint gap for a Chrome-158
profile. Not implemented yet.

### web-animations
⚠️ `Element.animate()` returns an object with `finished`, but `playState`/`ready`/`onfinish` are missing, and CSS
`transitionend`/`animationstart`/`animationend` never fire. Pages that wait for them can stall ([post-dialog-scroll-reset](#post-dialog-scroll-reset)).

### double-dialog-query
✅ (likely) Opening one FB dialog sent `CometSinglePostDialogContentQuery` twice in oxide. Root cause found 2026-11-28: XHR `on*`
handlers ran twice ([engine-changes](engine-changes.md#double-handlers)). Re-check on FB with the tap: one query per dialog.

### xhr-upload-events
⚠️ `xhr.upload` never fires `loadstart`/`progress`/`load`/`loadend`. Chrome does when an upload listener is attached before
`send()` and there's a body. And `abort()` after `send()` fires only `abort`, where Chrome fires `readystatechange`(4) → `abort` →
`loadend`. FB doesn't depend on either as far as we've seen.

### weakref-job
ℹ️ WeakRef targets live until the end of the current job. Leak tests must span several `evaluate` calls.

### guard-buffering
✅ `guard.py` used to collect the child's stdout and stderr and only read them at exit. A long run showed nothing (a
40-minute 200-post run produced an empty log), and oxide's heavy stderr could fill the 64 KB pipe buffer and **block the
child forever**. Fixed: reader threads drain both pipes continuously, and the CLI forwards lines live
(`watch(..., stream=True)`). Callers of `watch()` as a library still get the full `stdout`/`stderr` strings.

### stdout-flush
ℹ️ `os._exit()` skips the stdout flush: use `print(..., flush=True)`.

### feed-depth
**Scrolling deep costs memory on its own, even with no posts opened.** FB virtualizes the DOM, but its Relay store
(JS) keeps every post it loaded. `tools/depth_probe.py` (scroll only, best V8 flags, mimalloc):

| Gestures | Posts loaded | scrollY | RSS | Peak |
|---|---|---|---|---|
| 10 | 13 | 15k | 399 | 501 |
| 40 | 30 | 59k | 420 | 501 |
| 70 | 48 | 103k | 471 | 574 |
| 100 | 63 | 147k | 527 | 661 |
| 120 | 76 | 177k | 612 | 661 |

That's about **+2.8 MB per post past roughly 30 posts**. Reloading fixes memory but you then have to re-scroll to where you
were, so with recycling the time grows quadratically (posts 10→20 took 16 min in a 100-post run).
**What to do:** for the Pi bot, read only the newest posts (≤ ~30 deep, sorted by new) on each poll and reload between
polls. That stays under 600 MB. Don't try to read 100+ posts in one document. If you need depth, collect post ids from
graphql while scrolling, then visit `/groups/<gid>/posts/<id>/` one at a time after a reload ([facebook](facebook.md#permalink-pages--extra-tabs)).

## linux-tls
**Linux (glibc) only.** `import browser_oxide` fails with "cannot allocate memory in static TLS block". V8 uses initial-exec TLS, and a dlopen'd module can't get enough of it.
- **Fix:** start the process that uses the engine with `LD_PRELOAD=<site-packages>/browser_oxide/_native.abi3.so`, and set it on that process only. A global `ENV LD_PRELOAD` breaks `sh`, which lacks the `Py*` symbols.
- **What doesn't work:** `GLIBC_TUNABLES=glibc.rtld.optional_static_tls=…`. The module loads, but V8 then crashes with `Check failed: IsOnCentralStack()` or SIGSEGV on the first navigate.
- **In the bot:** `oxide/client.py` `worker_env` sets the preload for the worker automatically. Verified in `deploy/aptbot-oxide` (linux/arm64, 2026-11-28).

## h2-enhance-your-calm
- **Seen on the Pi, 2026-11-28, ~25 min FB session:** the worker logged `HTTP/2 connection error: connection error detected: detected excessive load generating behavior`.
- **Cause:** the http2 crate's local DoS guard. After 1024 resets on one connection that the library itself issues (oxide cancels aborted XHR/fetch and superseded long-polls), it sends GOAWAY ENHANCE_YOUR_CALM and drops the connection. Chrome never does that, so it's a wire tell, and in-flight requests are lost.
- **Fix:** `net/h2_client.rs` sets `max_local_error_reset_streams(Some(1 << 20))`.
