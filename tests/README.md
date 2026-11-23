# APT-APT test suite

Two layers:

- **`tests/unit/`**: fully offline. Every HTTP call is monkeypatched and SQLite
  uses temp-file DBs, so these pass with no internet. This is the default run.
- **`tests/live/`**: opt-in smoke tests that hit the **real** Yad2 gateway. They
  check the real API's behavior (padded city ids, end-to-end `fetch_map`,
  autocomplete) and **skip gracefully** on a network failure or a bot challenge
  instead of failing. Marked `@pytest.mark.live`.

## Setup (venv)

```bash
python -m venv /tmp/aptvenv
/tmp/aptvenv/bin/pip install -r requirements-dev.txt   # includes requirements.txt
```

## Running

Always run from the **repo root**, so `import aptbot` resolves (`pythonpath = .`
in `pytest.ini`).

```bash
/tmp/aptvenv/bin/python -m pytest -q            # offline unit tests (live excluded by addopts)
/tmp/aptvenv/bin/python -m pytest -m live -q    # live smoke tests (real network)
```

## What each file covers

| File | Module | Key invariants locked in |
|------|--------|--------------------------|
| `unit/test_models.py`       | `aptbot.models` | feature→param map, `normalize_name` (space/case, `קריית`→`קרית`), string city id on `LocationTarget`, **property-condition ids match Yad2** |
| `unit/test_locations.py`    | `aptbot.scrapers.yad2.locations` | **#1** string city ids (`_str_id`, `popular_cities`, `city_name("0070")`, `_flatten` keeps `"0070"`/`"103P"`), English→Hebrew query normalization, `resolve_candidates`/`batch_autocomplete` (HTTP monkeypatched) |
| `unit/test_yad2_gateway.py` | `aptbot.scrapers.yad2.gateway` | `_build_params` keeps `city` a string, `_is_challenge`, `normalize_marker`, **#4** 200-cap price **bisection** (fake `_get_markers`), including the request cap and the narrow-band stop |
| `unit/test_db.py`           | `aptbot.db` | **#1** city id round-trips as `"0070"`/`"103P"`; seen/priming persistence; users; **#6** a DB from another schema version is moved aside (`.old-<ts>`) and a fresh one is created |
| `unit/test_engine.py`       | `aptbot.engine` | `scrape_sig`/`group_searches`/`union_filters`, `matches_*`, `scrape_group`, **#5** capped backfill / only-new / price change / send-failure retry / prime-race guard / chat dedup, `run_cycle` logging |
| `unit/test_activity.py`     | `aptbot.bot.activity` | one log line per user action, new-user tagging, the observer never raises |
| `live/test_yad2_live.py`    | Yad2 (real) | **#2** `city=0070` returns markers, `city=70` returns none; `fetch_map` end to end |
| `live/test_locations_live.py` | autocomplete (real) | Tel Aviv resolves to string id `"5000"`; live `fetch_city_hoods` |

Shared builders and fixtures (`make_listing`, `FakeSender`, the temp-DB `db` fixture)
live in `tests/conftest.py`. The archived rentlyfly tests are in `archive/rentlyfly/`
and are not collected.
