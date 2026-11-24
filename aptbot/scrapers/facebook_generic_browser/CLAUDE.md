# facebook_generic_browser: instructions for Claude

A standalone, browser-driven scraper over a **real, logged-in Facebook account** (Patchright + real
Chrome, headful). `aptbot` never imports this package and this package never imports `aptbot`.

- `SPEC.md`: **our** contract: the finalized, validated architecture. The source of truth for building it.
- `api_docs/`: **their** frontend, as observed live (URLs, DOM landmarks, graphql queries, quirks,
  detection and account-health behavior).
- `docs/`: **how to use and extend the library** (API reference, concurrency, errors, operations).
- `docs/exploration-playbook.md`: the generic, site-agnostic exploration method + `explore/tools/`.
  **Follow it whenever you explore this or any other site, and add any new technique you use to it.**

## Account safety (mandatory)
- Never store, log or commit credentials. Login: by hand, or assisted with credentials the user gives at runtime,
  passed only via stdin (`fbgb login --stdin`; never argv/env/files). Stop on code/checkpoint pages; no auto-retry.
- Never commit or print cookies, `fb_dtsg`/`lsd`/`jazoest` or other session tokens. The profile and raw captures
  live in `~/.fbgb/` (outside the repo).
- Every browsing session goes through `browser.py` (validated config), `human.py` (input),
  `tabs.py` (attention lock, shared limiter) and `health.py` (stop on checkpoint). Prefer `FacebookAPI`.
  No `page.goto` loops, no `scrollTo`, no headless mode, no custom UA or headers, no proxies.
- On any checkpoint, "temporarily blocked", or login wall: **stop the session and tell the user**. Never retry.
- Keep volume low: short sessions, and stay inside the budgets in `SPEC.md`.

## Keep `api_docs/` in sync with reality (mandatory)
When Facebook behaves differently from what `api_docs/` says (a renamed graphql `fb_api_req_friendly_name`,
a changed response path, new DOM landmarks, new interstitials, a different scroll or pagination behavior),
in the same session:
1. Update the affected page(s) and set their `Last verified:` line to today's date.
2. Add a dated entry to `api_docs/CHANGELOG.md`: what changed, how it was observed, and the impact on code.
3. If `SPEC.md` depends on it, update `SPEC.md` too, or record it under its "Open questions".

Rules:
- Document only verified behavior. Mark anything inferred as **Unverified** and say why.
- Samples are trimmed and anonymized: no real names, profile IDs, phone numbers, photos or tokens.
  Replace them with placeholders like `<user_id>`, `<group_id>`, `<name>`.
- One page per graphql query at `api_docs/graphql/queries/<FriendlyName>.md`. Update `api_docs/README.md`
  when you add or remove pages.
- After a `patchright` upgrade, rerun `explore/validate.py` and update `api_docs/stealth/browser-setup.md`.
  `IGNORED_DEFAULT_ARGS` in `browser.py` is matched by exact string and can silently drift.
- Public API changes: update `docs/api-reference.md` (a test checks every public method is listed) + docstrings.

## Conventions agents must keep
- **Times are UTC everywhere** (models, cache, CLI output with a `UTC` suffix, `since=` inputs). Convert only for
  display. Naive datetimes are a bug. When reporting times to the user, say they're UTC or convert to Israel time.

## Code conventions
- Python 3.13, asyncio. Its own venv: `~/.fbgb/venv` (`requirements.txt` here, NOT the repo root's).
- Run from the repo root: `~/.fbgb/venv/bin/python -m aptbot.scrapers.facebook_generic_browser <cmd>` (CLI) or `…explore.<mod>`.
- Tests: `~/.fbgb/venv/bin/python -m pytest tests/unit/test_fbgb_*.py` (also pass in the bot venv: no patchright import at load).
- Absolute imports inside the package (`from aptbot.scrapers.facebook_generic_browser import human`).
