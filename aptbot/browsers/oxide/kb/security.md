# Security validation (2026-11-28)

Rerun this before any release, and after every dependency or engine bump.
- **Tools:** cargo-audit, gitleaks, pip-audit, bandit.
- **Shortcut:** the commands below, run from the repo root.

```sh
(cd aptbot/browsers/oxide && cargo audit) ; (cd aptbot/browsers/oxide/crates/browser_oxide_py && cargo audit)
gitleaks git --redact .                       # all history; the repo is PUBLIC
pip-audit --disable-pip --no-deps -r <(docker run --rm aptbot-oxide pip freeze | grep -v browser.oxide)
bandit -q -r aptbot -x aptbot/browsers/oxide/crates -ll
```

## Rust dependencies
- **Found:** 20 vulnerabilities in the workspace lock, and 22 in the wheel's lock (`crates/browser_oxide_py/Cargo.lock`, the one maturin builds with).
- **Affected crates:** quinn-proto/udp (QUIC), rustls, rustls-webpki, webpki-roots and root-certs (distrusted intermediates), url, zstd/zstd-safe/zstd-sys, pyo3 and anyhow.
- **Fixed:** semver-compatible `cargo update` of each crate. rustls is pinned `--precise 0.23.46`. pyo3 0.28 → 0.29 (RUSTSEC-2026-0176/0177) needed no code change.
- **Left (no fix exists):** unmaintained-only warnings for bincode, chrono, paste, rustybuzz, ttf-parser and unsafe-libyaml. They are transitive, via deno_core/v8, the fonts and the yaml profiles. Re-check on the next deno_core bump.

## `unsafe` in the engine
- **64 sites in total.** 52 are in `canvas/webgl_render.rs`, which sits behind the `webgl-render` feature, off and not compiled.
- **Compiled: 12.** All have SAFETY notes:
  - `html_parser/tree_sink.rs` ×5: UnsafeCell, single-threaded parser.
  - `net/tls.rs` ×2: BoringSSL FFI, read-only buffers.
  - `js_runtime/mod.rs` ×2: isolate enter/exit guard.
  - `page.rs` ×1: `ptr::read` under ManuallyDrop.
  - `frames.rs` ×2: our iframe change. It had no SAFETY note; one is now added.
- **The pyo3 binding has 0.**

## Script injection (engine-built JS)
- **The bug:** the engine interpolated URLs, selectors and typed text into scripts, and escaped only `\` and `'` or `"`. A newline or U+2028 in a URL or selector could break out of the string.
- **The fix:** every site now uses `crate::js_string_literal` (JSON encoding).
- **Sites fixed:** `location.href` ×7, `has_element`, `text_of`, `human_click`, `human_type`.
- **Rule:** never `format!` raw text into a script.

## Secrets and personal data
- **gitleaks, full history:** 55 hits, all FB `deduplication_key`/`mediaset_token` in the anonymized test fixtures. They aren't credentials. The fixture authors are "Person NN", and they hold no signed CDN URLs.
- **Never committed:** `.env`, `codedb.snapshot`, `.playwright-mcp/` (all git-ignored).
- **No personal paths or emails** in tracked files.
- **Worker env is an allowlist** (`oxide/client.py` `worker_env`). The page-JS process never gets the bot's Telegram/AI tokens.
- **Debug switches never forwarded:** `BO_DEBUG_HEAP`, `BO_NET_LOG`, `BROWSER_OXIDE_DUMP_POST_DIR`.
- **Error and crash text is redacted** (`redact`): cookie/token pairs and URL query strings.
- **Worker umask 077:** the engine rewrites the cookie jar, which was found at 644 on the Pi, now 600. `~/.fbgb` is set to 700.
- **Pi bot log:** 0 token-like strings.

## Python
- **pip-audit, image packages:** Brotli 1.1.0 had 4 advisories, so it's bumped to 1.2.1. fbgb requirements: clean.
- **bandit (medium and up):** 12 hits, all reviewed, none real:
  - SQL: placeholders, plus column names from an allowlist.
  - sha1/md5: cache keys.
  - urlopen: only in research tools, on https URLs.
  - /tmp: a research tool, dir 700.

## Open
- The builder stage installs rustup via `curl | sh`, and the rusty_v8 static lib is downloaded at build time. Both come over TLS from the official hosts and run only in the build stage. Pin a rustup checksum if this matters.
- The upstream engine has no fuzzing. It parses hostile HTML/CSS/JS from facebook.com only, inside a worker process that holds no secrets other than the FB session.
