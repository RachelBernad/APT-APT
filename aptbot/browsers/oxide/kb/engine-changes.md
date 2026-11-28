# Engine changes (changelog)

[← index](README.md) · related: [caveats](caveats.md), [memory](memory.md), [input-and-scrolling](input-and-scrolling.md)

Fork: `crates/browser_oxide`, branch `poc`, **uncommitted**; the pre-work snapshot is `git stash@{0}`. Paths below
are relative to `src/`. JS files are compiled in, so rebuild after any change ([quickstart](quickstart.md#build)).

## Header parity (Chrome 158), earlier work
`net/blink_order.rs` (Blink WTF hash-map header order, rapidhash) and `net/headers.rs` (`nav_headers_chrome158`,
`fetch_headers_chrome158`). 60/71 first-party request kinds match Chrome's wire order. Harness: `hdrpar/` (see
memory note `browseroxide-header-parity`).

## Making FB boot

| File | Change | Why |
|---|---|---|
| `js_runtime/js/window_bootstrap.js`, `shared_apis_bootstrap.js` | IndexedDB: `DOMStringList` (`contains`, `item`, indexed, iterator) for `objectStoreNames`/`indexNames` (getters, sorted snapshots); `IDBTransaction.objectStoreNames`; `deleteObjectStore` throws NotFoundError; `open()` events on `setTimeout` (was a microtask); `upgradeneeded` listeners dispatched; events are real `Event`s with `target`/`currentTarget`/`srcElement` | FB AsyncStorage: "objectStoreNames.contains is not a function", "NotFoundError: store data"; FB reads `e.currentTarget.result` after an await |
| `js_runtime/js/dom_bootstrap.js` | `setAttributeNS/getAttributeNS/hasAttributeNS/removeAttributeNS` (xlink/xml/xmlns prefixes) | React SVG threw and FB showed its error screen |

## Scrolling and visibility

| File | Change | Why |
|---|---|---|
| `dom_bootstrap.js` | Viewport scroll state `_vpX/_vpY`, `_vpSet` (clamped, one coalesced trusted `scroll` event per 16 ms); `getBoundingClientRect` offset by the scroll; `documentElement.scrollTop/Left` map to the viewport; hook gets `viewportScroll`/`viewportPos` | Scrolling was fake; rects never moved |
| `window_bootstrap.js` | `scrollX/Y`, `pageX/YOffset`, `scrollTo`, `scrollBy` use the viewport | Same |
| `window_bootstrap.js` | IntersectionObserver rewritten to be layout-based (thresholds, rootMargin, root, validation, getters, `takeRecords`) with 16 ms re-checks on mutation/scroll/resize | It reported everything visible, so FB paginated 10 pages with no scrolling: +400 MB and a bot tell |
| `event_bootstrap.js` | `input.wheel(px)`: trusted `WheelEvent` ticks + viewport scroll | Human-like scrolling |
| `dom_bootstrap.js` | Real `Element.scrollIntoView` (block/inline modes) | It was a no-op, and `click()` relied on it |

## Correctness fixes found through FB dialogs (2026-11-27)

### cdp
`dom_bootstrap.js` `compareDocumentPosition`: implemented per spec (common ancestor + sibling order;
DISCONNECTED|IMPLEMENTATION_SPECIFIC with a stable direction; attributes). It used to return FOLLOWING for any non-ancestor
pair, so `a.cDP(b)` and `b.cDP(a)` were both 4. FB's layer manager uses it. Matches Chrome on `tools/cdp_selftest.py`.

### focus
`dom_bootstrap.js` `_focusElement(el, options)`: WebIDL `FocusOptions` conversion (`focusVisible` then
`preventScroll`, TypeError for non-objects), and scroll into view (`nearest`) unless `preventScroll`. FB's FocusManager
feature-detects `preventScroll` with a getter. Without it, FB fell back to saving and restoring every scroll
position around each focus (a fingerprint and a behaviour difference).

## Memory leaks (2026-11-27), see [memory](memory.md#leaks-fixed)

| File | Change |
|---|---|
| `dom_bootstrap.js` | Focus is held through a **WeakRef** + Chrome's focus fixup (a removed element stops being active). A strong `_activeEl` pinned every closed FB dialog (~70% of the leaked objects) |
| `event_bootstrap.js` | `_nodeListeners` (strong Map by nodeId, never pruned) is now swept lazily, deterministically, when it doubles past 4096 entries. Listener maps of **disconnected** nodes move to a WeakMap keyed by the wrapper, so they live as long as page code can reach the node and come back on re-insertion. The wrapper lookup is resolved lazily from the private hook (at bootstrap time `__browser_oxide._wrapNode` doesn't exist yet) |
| `dom_bootstrap.js` | `_nodeCache` (nodeId → WeakRef) entries are pruned by a `FinalizationRegistry` once the wrapper is collected (it was ~100 B per node, forever) |

## DOM reaper: freeing removed nodes on the Rust side (2026-11-27)

**Problem:** `removeChild` (`op_dom_remove_child`) only *detached* nodes. `innerHTML=` and `textContent=` dropped the old
children's own slots but left their descendants allocated and unreachable. The Rust arena (`dom/arena.rs`) therefore
held every node FB ever removed: elements, attribute strings and text, for the life of the document. On FB that was
the 300→440 MB "native" part of RSS ([memory](memory.md#where-the-rss-is-bostats-needs-bo_debug_heap1)).

**Fix:**
- `Dom::remove`, and `op_dom_remove_child`, detach and **queue** the node (`reap_pending`).
- New ops:
  - `op_dom_reap_take`: returns the queue.
  - `op_dom_reap_info(id)`: returns the subtree of the node's detached root, root first. Walks out of shadow trees via the host. Returns empty if the root is a Document, or the subtree has an iframe, frame, object or embed.
  - `op_dom_reap_free(ids)`: re-validates the subtree, then frees the slots.
  - `op_dom_detached_root(id)`.
- JS `_reapRun`/`_reapCheck` (dom_bootstrap) runs on a timer, never inside page code holding raw ids.
  - **Trigger:** about 128 removals, or wrapper finalizations while candidates are pending.
  - **Phase 1:** sweep the listener maps.
  - **Phase 2, in the next task:** free each candidate subtree in which **no node has a live wrapper**, then drop its `_nodeCache`, `_scrollState` and listener entries (`hook.forgetNodes`).
- Slots are still never reused, so stale ids can't alias new nodes.
- **Tree grouping** (event_bootstrap `_pinInGroup`): when the sweep parks a detached node's listeners, it pins
  that node's wrapper in a group object for its detached tree, and every live wrapper of the tree points at the group
  (a WeakMap). Holding any node of a removed tree keeps all its listeners and wrapper identities, like Blink's
  object grouping. Holding none lets the group be collected, after which the reaper frees the nodes. This also fixed a bug
  in the earlier sweep: listeners of a held-but-removed subtree's children were lost.
- Debug hooks: `hook.reapStats()` → `{runs, freed, kept, pending}`, and `hook.reap()` runs a check now.

**Tests:** `tools/reap_selftest.py` (example.com) churns 56k nodes and then:
- RSS goes 227 → 109 MB, with 55,278 nodes freed.
- Held subtrees keep their children, attributes and text; a held deep child keeps its parent and siblings; a listener on a held child still fires after re-insertion.

## mimalloc as the Rust allocator (2026-11-27)
`browser_oxide_py/Cargo.toml`: `mimalloc` optional dep, default feature, `#[global_allocator]` in `src/lib.rs`. The system
malloc zone was 55% fragmented on FB (188 MB dirty for 85 MB allocated). With mimalloc the Rust allocations leave that
zone, and total dirty memory dropped about 27 MB on the same 6-post session. Only Rust allocations are affected: V8's own C++
allocations and Python still use the system malloc. It builds for Linux/arm64 too (C source, no prebuilt needed).

## Network tap (2026-11-28) {#network-tap}
`net/tap.rs` (new) + call sites in `js_runtime/extensions/fetch_ext.rs` `op_fetch` (fetch() and XHR, with the request
body) and the 5 document loads in `page.rs`. Binding: `Browser.tap(patterns, max_kb=4096)`, `Browser.take_responses()`
→ `(records, dropped)`.
- **Why:** the research scrapers read graphql through an init script that wrapped `XMLHttpRequest`/`fetch` (`research/fb_capture.js`),
  and a page can detect that. The tap is the CDP `Network.getResponseBody` equivalent: Rust-side, invisible to page JS.
- Process-global, off by default (one atomic load per response). Plain-substring URL match. Bodies capped and flagged
  `truncated` (cut on a UTF-8 boundary). The queue holds 512 records and drops the oldest (counted in `dropped`).
- Test: `tools/tap_selftest.py` (local HTTPS server with a throwaway cert; document + XHR + fetch, non-matching URL
  ignored, page globals unchanged, off = nothing recorded). Rust unit test in `net/tap.rs`.

## XHR / OfflineAudioContext handlers fired twice (2026-11-28) {#double-handlers}
`window_bootstrap.js` (XHR `open`, `send`'s `fireEvent`, `abort`) and `canvas_bootstrap.js` (`OfflineAudioContext`
complete) called `dispatchEvent(ev)` **and then** `this.on<type>(ev)`, but the generic dispatcher
(`event_bootstrap.js` `_fireListeners`) already runs the `on*` handler. So every `xhr.onload`/`onreadystatechange`/
`onloadend`/`onabort` and `oncomplete` ran **twice**. Code that does work in `onload` (FB's request layer) did it twice:
the "dialog query sent twice" caveat, and the tap selftest saw a `fetch()` issued from `onload` reach the server twice.
Now the handler is called directly only when there's no `dispatchEvent`. Probe result, which matches Chrome:
`rs2,rs3,rs4,onload,L:load,onloadend`, `audio1`.

## Teardown
`frames.rs` `pub fn shutdown()` (disposes every frame isolate, entered), called from `host/mod.rs` `engine_loop` after the
command loop, before dropping the page. Fixes the "thread local panicked on drop" abort when a `Browser` is closed.

## Debug and ops hooks
- `js_runtime/runtime.rs`: `BROWSER_OXIDE_V8_FLAGS` → `V8::set_flags_from_string` (once, before heap limits).
- `js_runtime/mod.rs`: `write_heap_snapshot(path)`. `page.rs` `evaluate`: with `BO_DEBUG_HEAP=1`, the `#bo:gc` / `#bo:snap:PATH` prefixes.
- `net/mod.rs`: the POST header log is gated on `BO_NET_LOG`, with cookie/authorization/`x-fb-*`/token values redacted
  (it used to print cookies unconditionally). `page.rs`: the 403/429/498 reload-headers dump redacts `set-cookie`.

## Tried and reverted
- V8 pointer compression via the crate feature `v8_enable_pointer_compression` (macOS target table). deno_core needs
  `simdutf`, and rusty_v8 v149.4.0 ships **no** `_ptrcomp_simdutf` prebuilt lib for any target (`_ptrcomp` alone exists only for
  macOS arm64). It needs a V8 source build ([memory](memory.md#pointer-compression)).
