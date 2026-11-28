# aptbot/browsers

Browser engines the scrapers run on.

| Dir | What | Used by |
|---|---|---|
| [oxide/](oxide/) | BrowserOxide fork: a Rust headless browser (V8, own TLS/HTTP2 stack with a Chrome fingerprint, no Chromium). ~450–560 MB on a logged-in FB group, so it fits the Pi | `scrapers/facebook_generic_browser` with `FB_BROWSER=oxide` |

Chrome (Patchright) is not vendored: fbgb uses the installed Google Chrome or Chromium (`FB_BROWSER=chrome`, the default).

Start at [oxide/kb/README.md](oxide/kb/README.md).
