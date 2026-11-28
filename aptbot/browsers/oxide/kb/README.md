# BrowserOxide knowledge base

How to use our BrowserOxide ("oxide") fork to build scrapers, and what we changed in it.
Oxide is a Rust headless browser: real HTML/DOM/CSS layout, V8 through deno_core, its own TLS/HTTP2 stack
(Chrome-158 fingerprint), and no Chromium or CDP. We target it to replace Chrome for the Facebook scraper on the
Raspberry Pi, where **peak RSS must stay ≤ 600 MB**.

Start with [quickstart](quickstart.md). Before you write a scraper, read [caveats](caveats.md).

## Map

| Page | What's in it |
|---|---|
| [quickstart.md](quickstart.md) | Build, the `guard.py` rule, `drive.py` verbs, the Python API, a minimal scraper loop |
| [env-vars.md](env-vars.md) | Every environment variable we use (memory, V8 flags, cookie jar, debug hooks) |
| [input-and-scrolling.md](input-and-scrolling.md) | The trusted-input hook (`click`/`type`/`press`/`wheel`), the scroll model, focus |
| [facebook.md](facebook.md) | Facebook recipes: login session, feed structure, post slots, "See more", post dialogs, graphql capture, checkpoints |
| [memory.md](memory.md) | RAM budget, tuned settings, measurements (home, groups, 200-post session, tabs), the leaks we fixed, what's left |
| [caveats.md](caveats.md) | Every known trap, with its fix or workaround (anchor per caveat) |
| [debugging.md](debugging.md) | Init-script tracing, heap snapshots + analyzers, live PNG view, React fiber walk, comparing with real Chrome |
| [engine-changes.md](engine-changes.md) | Changelog of every edit we made to oxide: file, what, why |
| [security.md](security.md) | Session files, secrets in logs, account-safety rules |

## Where things are

Two places. The **code** is in the public repo, and the **private workdir** never leaves the machine.

```
aptbot/browsers/oxide/            ($OXIDE, in the repo: vendored fork, see VENDORED.md)
  crates/browser_oxide/           the engine
    src/js_runtime/js/*.js          the JS "browser" (DOM, events, window APIs)  ← most fixes live here
    src/host/mod.rs                 engine thread behind the Python `Browser`
    src/frames.rs                   iframes (each has its own V8 isolate)
    src/net/                        HTTP/TLS, cookies
  crates/browser_oxide_py/        pyo3 binding (`browser_oxide._native`, mimalloc allocator)
  python/browser_oxide/           Python package (`Browser`, `Profile`)
  build.sh                        build + install the wheel (target dir outside the repo)
  tools/                          guard.py, drive.py, selftests, probes, trace scripts, heap analyzers
  research/                       scraper prototypes (fb_deep_session.py = reference loop, fb_capture.js, fb_tabs_probe.py)
  kb/                             this knowledge base

~/aptbot-browser-research/browseroxide/   ($OXIDE_WORK, private, never committed)
  venv/                           python with maturin + the installed wheel
  profiles/                       logged-in session files (secret, chmod 600)  → security.md
  out/                            run logs and CSVs (contain real posts)
  hdrpar/                         header-parity harness (holds a test TLS key)
  oxide/                          the original fork checkout (upstream git history, `git stash@{0}` = pre-work snapshot)
```

The production scraper that uses oxide lives in `aptbot/scrapers/facebook_generic_browser/oxide/`
([aptbot-integration](aptbot-integration.md)).
Security validation (deps, unsafe, secrets, how to rerun): [security](security.md).

`research/`:
- `fb_deep_session.py`: long group session. It scrolls, opens every post (See more + comments dialog), recycles the
  page under memory pressure, and logs RSS as CSV. This is the reference scraper loop.
- `fb_capture.js`: init script that counts post ids seen in graphql responses (research only; production uses the
  engine-side network tap).
- `fb_tabs_probe.py`: extra `Browser()` instances ("tabs") in one process; per-tab RSS.

`tools/`: `guard.py`, `drive.py`, selftests (`reap_`, `cdp_`, `leak_selftest.py`), memory probes (`depth_probe.py`,
`spike_probe.py`, `ab_flags.sh`), trace init scripts (`scrolltrace.js`, `cdp2.js`, `rdt.js`, …) and heap analyzers
(`heaproot.py`, …), see [debugging](debugging.md).

## Status (2026-11-27)

- Logged-in FB home, group feeds, post dialogs and permalinks all render and work in oxide.
- Feed scrolling: 30 scrolls with the tuned env peak at about 545 MB. A long session that opens posts deeply needs page
  recycling (FB leaks per opened post in Chrome too); see [memory](memory.md#long-sessions).
- Not yet verified on the Pi (Linux arm64). The biggest untried lever is V8 pointer compression, which needs V8 built
  from source ([memory](memory.md#pointer-compression)).
