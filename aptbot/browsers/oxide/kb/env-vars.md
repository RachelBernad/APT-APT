# Environment variables

[← index](README.md) · related: [memory](memory.md), [debugging](debugging.md)

## Memory / V8 (use these for every FB run)

```sh
BROWSER_OXIDE_HEAP_INITIAL_MB=16 \
BROWSER_OXIDE_HEAP_MAX_MB=384 \
BROWSER_OXIDE_V8_FLAGS="--max-semi-space-size=2 --optimize-for-size"
```

| Var | Default | Meaning |
|---|---|---|
| `BROWSER_OXIDE_HEAP_INITIAL_MB` | **1024** | Initial V8 old-space size. The default lets the heap grow to 1 GB before V8 bothers to collect, so RSS balloons. 16 is fine |
| `BROWSER_OXIDE_HEAP_MAX_MB` | 4096 | Hard V8 heap cap. Going over it = "Fatal JavaScript out of memory" (the process dies). 384 holds a feed session, 512 is safer for debugging. Pages can't see it: `performance.memory.jsHeapSizeLimit` is hardcoded to Chrome's 4395630592 |
| `BROWSER_OXIDE_V8_FLAGS` | — | **Ours.** Passed to `V8::set_flags_from_string` once, before the first isolate. Invalid flags make V8 print noise or misbehave (e.g. `--trace-gc-nvp=false` turned tracing on and the run timed out) |

## Session / network

| Var | Meaning |
|---|---|
| `BROWSER_OXIDE_COOKIE_JAR=path` | Load the cookie jar from a JSON file and persist it. Shared by every `Browser()` in the process. **Work on a copy** of `profiles/fbtest_jar.json` (e.g. `/tmp/fbjar_run.json`, chmod 600) so a bad run doesn't corrupt the master |
| `BROWSER_OXIDE_CA_BUNDLE=pem` | Extra CA (header-parity harness only: `hdrpar/server.pem`) |
| `BROWSER_OXIDE_DUMP_POST_DIR=dir` | Dumps every POST **with headers (cookies!)** and body to disk. Debug only; delete the files afterwards ([security](security.md)) |
| `BO_NET_LOG=1` | Request logging. Since our fix, POST header logging is gated on this and cookie/token values are redacted. It slows FB runs a lot (they can time out) |

## Our harness

| Var | Used by | Meaning |
|---|---|---|
| `BO_MEM_MB`, `BO_TIMEOUT` | guard.py | RSS limit (MB, the whole process tree) and wall-clock limit (s) |
| `BO_INIT=file.js` | drive.py | Run the file as an init script on every `nav` (tracing, see [debugging](debugging.md#init-scripts)) |
| `BO_DEBUG_HEAP=1` | oxide (ours) | Enables the driver-only `evaluate` prefixes `#bo:gc` (full GC, returns live heap bytes), `#bo:stats` (JSON: V8 used/total/physical/malloced/external/contexts; RSS minus these = Rust side) and `#bo:snap:PATH` (heap snapshot). Off = those strings go to the page as normal JS |
| `BO_POST_STATS=1`, `BO_MALLOC_RELIEF=1`, `TARGET_MODE=opened` | fb_deep_session.py | Per-dialog cost line (DOM size, media, network KB, RSS change) / ask malloc to release free pages after each post (diagnostic) / stop after N opened posts instead of N graphql ids |
| `BO_LIVE_DIR` | drive.py `live` | Where `live.png` / `view.html` go (default `/tmp/oxlive`) |
| `RECYCLE_RSS`, `RECYCLE_EVERY` | fb_deep_session.py | Reload the page when RSS (MB) after a post exceeds this / every N opened posts |
| `BO_SNAP_AT=3,7` | fb_deep_session.py | Write heap snapshots after the Nth opened post (needs `BO_DEBUG_HEAP=1`). A snapshot costs about +900 MB RSS, so don't trust RSS readings after one |
| `BO_DEBUG_SLOTS=1` | fb_deep_session.py | Print the post slots seen on each loop |
