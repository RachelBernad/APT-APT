# Memory (target: peak RSS ≤ 600 MB on the Pi)

[← index](README.md) · related: [env-vars](env-vars.md), [debugging](debugging.md#heap), [caveats](caveats.md#memory), [engine-changes](engine-changes.md)

## How to measure

- RSS of the Python process (oxide runs in-process): `ps -o rss= -p PID`, sampled every 0.5 s by a thread
  (`fb_deep_session.py` `Sampler`). `guard.py` prints the peak for the whole tree.
- Live heap vs garbage: `BO_DEBUG_HEAP=1` + `b.evaluate("#bo:gc")` after each step. Growth that survives `#bo:gc` is a
  leak; growth that doesn't is GC lag.
- The forced GC and heap snapshots themselves inflate RSS: snapshot runs give useless RSS numbers.
- Chrome baseline: CDP `HeapProfiler.collectGarbage` + `Runtime.getHeapUsage` ([debugging](debugging.md#compare-with-real-chrome)).

## Settings (always)

`BROWSER_OXIDE_HEAP_INITIAL_MB=16 BROWSER_OXIDE_HEAP_MAX_MB=384 BROWSER_OXIDE_V8_FLAGS="--max-semi-space-size=2 --optimize-for-size"`
([why](env-vars.md#memory--v8-use-these-for-every-fb-run)). With the defaults (1 GB initial heap) the same pages peaked >1 GB.

## Results (macOS arm64, 2026-11-27)

| Scenario | Peak RSS | Notes |
|---|---|---|
| Chrome, same group feed | ~750 MB (all processes) | JS heap 60 MB, 2,683 elements |
| Oxide home + 6 wheel scrolls (×3 runs) | 510–553 MB | live heap 143–153 MB, after the IntersectionObserver fix (before: 399 MB heap, OOM at a 512 cap) |
| Group, 12 scrolls | 525 MB | ends at 360 MB, heap 116 MB |
| Group, 30 scrolls | 545 MB | RSS creeps 285→418 MB, no runaway |
| Group, opening posts, no recycling | +~17 MB RSS per opened post | 9 posts → 647 MB. Live heap +14–16 MB/post |
| Group, opening posts, `RECYCLE_RSS=500` | 616 MB over 11 posts | each reload drops ~100 MB |
| Long session (200 posts) | see [below](#long-sessions) | |

## Leaks fixed

Found with `heaproot.py` (shortest GC-root paths to new objects across two snapshots):
1. **`_activeEl`** (focus) strongly held a removed element inside each closed dialog → its handlers → the whole React
   tree. About 70% of the per-dialog leak at that point. Fix: WeakRef + focus fixup.
2. **`_nodeListeners`** never shrank: every listener ever added to a removed node, plus its closure (fibers), lived forever.
   Fix: lazy sweep into a wrapper-keyed WeakMap.
3. **`_nodeCache`** kept an id → WeakRef entry for every node ever wrapped. Fix: FinalizationRegistry pruning.

Synthetic check (`tools/leak_selftest.py`, 20k nodes in batches): never-attached nodes with listeners went from +27 MB to ≈0;
attach/remove nodes with listeners +4.7 MB (bounded by the sweep threshold); plain create +1.1 MB.

## What's left: FB's own per-dialog leak

After those fixes, an opened dialog still costs ~16 MB live heap in oxide. **Chrome leaks too: 68 → 104 MB over 6 dialogs,
~6 MB each** (FB's `document.__reactHandles$*` Set grows 28→31→40→52 and never shrinks on close). DOM wrappers are
negligible (0.1 MB); the growth is FB objects (fibers, Relay records). Why oxide is ~2.7× bigger:
- No V8 pointer compression (8-byte vs Chrome's 4-byte tagged pointers, so every object is larger). See below.
- The dialog query is sent twice in oxide ([caveat](caveats.md#double-dialog-query)), and the layer mis-detach may keep extra trees.

## Long sessions

The rule: **you can't open hundreds of posts in one FB document, in any browser.** Recycle:
- `RECYCLE_RSS=500`: after a post, if RSS > 500 MB, reload the group (like a user refreshing). Already-opened posts are
  skipped by key, and the script flicks past them with bigger wheels.
- Post ids must be accumulated **outside** the page (the page-side capture resets on reload).
- Cost: re-scrolling past handled posts grows with the session (feed position isn't kept across reloads).

Measured over 49 opened dialogs with `RECYCLE_RSS=500` (`out/deep/stats2.log`):
- After each reload: V8 used ≈ 115 MB, RSS ≈ 400–460 MB. **Reloads fully reset the JS heap.** Nothing builds up
  across reloads.
- Typical RSS 450–550 MB. Spikes to 600–640 MB happen on single dialog opens. The worst seen in other runs was 816 MB.
- 3–5 dialogs per reload.

### Where the RSS is (`#bo:stats`, needs BO_DEBUG_HEAP=1)

| Part | Size |
|---|---|
| V8 heap, committed (`total`) | ≈ used + 4 MB. V8 does **not** hoard pages |
| V8 malloced + external (ArrayBuffers) | 1–2 MB + 2 MB |
| **Everything else (Rust side)** | **≈ 300 MB after a reload, growing to 350–440 MB as dialogs open** |

So the spikes are **Rust-side native memory per document**: oxide's DOM, style and layout data, and network buffers. It's freed on
reload. It is **not** allocator retention: `malloc_zone_pressure_relief` (macOS) released 0 MB every time
(`fb_deep_session.py BO_MALLOC_RELIEF=1`). Next step: find which Rust structure grows (DOM nodes removed by FB but not
freed by oxide is the prime suspect).

### Dedup across reloads

Recycling only works if already-handled posts are recognized after the reload. The first keys (textContent slice,
then author uid + listing description) changed between loads, so the same 8 posts were opened 49 times. The stable
key and how to verify it are in [facebook](facebook.md#feed-structure-as-seen-in-oxide) (`tools/keydiff.py`).

### What sets the peak (`tools/spike_probe.py`, `ru_maxrss` around single actions)

| Action | Process peak change |
|---|---|
| navigate + FB boot | 25 → **538 MB** (settles at 471) |
| wheel, forced layout, See more, open dialog, Escape | **+0–6 MB each** |

So the peak is **the boot of the FB app**, plus the in-document climb of about 16 MB per opened dialog when you wait too
long to reload. Single actions don't spike. Keep `RECYCLE_RSS` about 120 MB below the budget (480 for 600).

### Native memory breakdown (`BO_VMMAP=1`, macOS vmmap at end of a 6-post run)

| Region (dirty) | System malloc | With mimalloc |
|---|---|---|
| V8 heap (Memory Tag 255) | 172 MB | 191 MB (also holds mimalloc's pages now) |
| Default malloc zone | **188 MB, 85 MB allocated, 55% fragmentation** | 64 MB (22 MB allocated: V8's C++ and Python) |
| Page tables | 26 MB | 27 MB |
| Total dirty | 403 MB | 376 MB |

The oxide `.so` code is about 200 MB resident, but it's file-backed and clean (evictable). The DOM arena itself is small:
37k live nodes, of which 30k are removed ones, ≈ 10 MB ([reaper](engine-changes.md#dom-reaper-freeing-removed-nodes-on-the-rust-side-2026-11-27)).

### V8 flag A/B (6 posts, same feed, `tools/ab_flags.sh`)

| Flags (on top of `--max-semi-space-size=2 --optimize-for-size`) | Mean | p90 | Max |
|---|---|---|---|
| none | 431 | 521 | 584 |
| `--lite-mode` | **390** | 488 | 569 |
| `--no-opt --no-maglev --max-lazy --bytecode-old-age=2 --memory-reducer-for-small-heaps --heap-growing-percent=20` | 402 | 492 | **552** |

There was no visible slowdown at human pacing. None of these change JS semantics. Pages could only notice them through timing.

100-post run: _pending._

## Tabs

`research/fb_tabs_probe.py`, one process:

| Step | RSS |
|---|---|
| Feed tab after 3 scrolls | 396 MB |
| + 1 permalink tab open | 645–653 MB (+250 MB: a full FB app) |
| tab closed | 583–598 MB (macOS keeps freed pages) |

On the Pi that means **one Browser at a time**. Load permalinks sequentially in the same Browser if you need them.

## Pointer compression

The biggest untried lever: Chrome's V8 uses 4-byte compressed pointers, ours uses 8-byte ones. Expect roughly 30–45% less JS heap.
The crate feature exists (`v8_enable_pointer_compression`), but deno_core also enables `simdutf`, and rusty_v8 v149.4.0
publishes no `_ptrcomp_simdutf` prebuilt for any target (`_ptrcomp` only for macOS arm64). So it needs
`V8_FROM_SOURCE=1` builds: on the Mac for testing, and for `aarch64-unknown-linux-gnu` (doable in an arm64 Linux Docker
container on Apple Silicon; expect hours and ~30 GB disk). Pointer compression also limits the heap to a 4 GB cage, as in Chrome.
