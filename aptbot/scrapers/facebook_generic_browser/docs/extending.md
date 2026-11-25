# Extending the library

## Layering (who may call whom)
```
api.py (FacebookAPI: public methods, cache policy, breaker)
  └─ dispatcher.py (queue, tabs, single-flight)     ← browser-agnostic, unit-tested with fake tabs
       └─ jobs.py (one coroutine per operation, runs in ONE tab)
            ├─ tabs.py (attention lock, pace)  ├─ human.py (input)  ├─ health.py (stop screens)
            ├─ capture.py (CaptureHub: per-tab response events)
            └─ parse.py / extract.py (pure: payload → models)        ← unit-tested on anonymized fixtures
store.py (SQLite cache)   models.py (dataclasses)   errors.py   browser.py (validated launch config)
explore/ (prototypes, validation, login, anonymizer; not used at runtime)
```

## Adding a function (e.g. "group members")
1. **Explore** with `explore/session.py`-style code or `FacebookAPI(debug_dump=True)`: one short, slow session.
   Find which page and which `fb_api_req_friendly_name` carries the data (`~/.fbgb/captures/<run>/index.jsonl`).
2. **Document** it in `../api_docs/` (a `graphql/queries/<Name>.md` page with the field map, `Last verified:`, and
   anything inferred marked **Unverified**). Add a `CHANGELOG.md` entry.
3. **Fixture**: `~/.fbgb/venv/bin/python -m aptbot.scrapers.facebook_generic_browser.explore.anonymize <capture.json>
   tests/unit/fixtures/fbgb/<name>.json` (`--html page.html out.json <ids…>` for page islands). It replaces people,
   phones, CDN urls and **all post/comment bodies** with synthetic data. Eyeball it, then grep it for real names and
   phone numbers before committing.
4. **Parser**: a pure function in `parse.py` (find nodes by type/key, never by full path or `doc_id`) + tests.
5. **Job** in `jobs.py`: subscribe to the tab's events → `navigate()` (costs a token, health-checked) or click →
   wait on events (never fixed sleeps) → `scroll_burst()` for more → `health.check()` → return models.
   Input only inside `env.pool.foreground(page)`, and keep it short.
6. **API method** in `api.py`: `self._run(job, key=<single-flight key>, group=<group id or None>, priority=…)`,
   cache the result in `store`, add `timeout=`. Full docstring: args, return, cost, errors.
7. **Docs**: `docs/api-reference.md` (+ `data-model.md` for new types) and a recipe in `usage-guide.md`.
   `tests/unit/test_fbgb_docs.py` fails if a public method is missing from the reference.

## When Facebook changes something (drift)
Symptoms: `SchemaDrift`, empty results, `NotMember` on a group you're in, parse tests passing but live results empty.
1. `FacebookAPI(debug_dump=True)` → reproduce once → diff the new payload against the fixture/docs.
2. Update `parse.py` (prefer making the key search more tolerant over hard-coding a new path), regenerate the
   fixture, update `api_docs/` + CHANGELOG with what changed and how it was observed.

## Rules that keep the account safe
- No `page.route`, `expose_function`, `add_init_script`, headless, `scrollTo`, custom UA/headers.
- Never send graphql yourself or replay `doc_id`s. Only read what the page fetches.
- Every navigation goes through `TabPool.goto` (attention + pace) and is followed by `health.check`.
- Never write credentials, cookies or tokens anywhere. Raw captures stay in `~/.fbgb/`.
