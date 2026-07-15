# APT-APT test suite

Two layers:

- **`tests/unit/`** — fully offline. Every HTTP call is monkeypatched and SQLite
  uses temp-file DBs, so these pass with no internet. This is the default run.
- **`tests/live/`** — opt-in smoke tests that hit the **real** Yad2 gateway and
  rentlyfly.ai. They verify reality (the padded-city-id feed behaviour, rentlyfly's
  gzip negotiation, autocomplete) and **skip gracefully** on a network failure or
  bot-challenge rather than failing. Marked `@pytest.mark.live`.

## Setup (venv)

```bash
python -m venv /tmp/aptvenv
/tmp/aptvenv/bin/pip install -r requirements-dev.txt
```

The app's own `requirements.txt` is intentionally kept free of test deps.

## Running

Always run from the **repo root** (so `import config`, `import engine`, … resolve;
`pythonpath = .` in `pytest.ini` handles this).

```bash
# Offline unit tests (default — live is excluded via addopts `-m "not live"`)
/tmp/aptvenv/bin/python -m pytest tests/unit -q
/tmp/aptvenv/bin/python -m pytest -q            # same set; live deselected

# Live smoke tests (real network; TLV is the only rentlyfly-covered city)
/tmp/aptvenv/bin/python -m pytest -m live -q
```

## What each file covers

| File | Scraper / component | Key invariants locked in |
|------|--------------------|--------------------------|
| `unit/test_models.py`      | `models` | feature→param map, `normalize_name` (space/case, `קריית`→`קרית`), string city id on `LocationTarget` |
| `unit/test_locations.py`   | locations / autocomplete | **#1** string city ids (`_str_id`, `popular_cities`, `city_name("0070")`, `_flatten` keeps `"0070"`/`"103P"`), English→Hebrew query normalization, `resolve`/`batch` (HTTP monkeypatched) |
| `unit/test_yad2_gateway.py`| Yad2 map scraper | `_build_params` keeps `city` a string, `_is_challenge`, `normalize_marker`, **#4** 200-cap price-**bisection** (fake `_get_markers`) incl. request-cap + narrow-band stop |
| `unit/test_rentlyfly.py`   | rentlyfly scraper | **#3** forced `gzip, deflate` (no `br`), `rentlyfly:` uid prefix, price/floor/feature normalization, pagination (HTTP monkeypatched) |
| `unit/test_db.py`          | persistence + migrations | **#1** city id round-trips as `"0070"`/`"103P"`; seen/priming persistence; **#6** v1→v2, seen-FK self-heal, city_id INTEGER→TEXT |
| `unit/test_engine.py`      | bot's use of the scrapers | `scrape_sig`/`group_searches`/`union_filters`, `matches_*`, **#7** TLV-only rentlyfly merge, **#5** capped backfill / only-new / price-change / send-failure retry / prime-race guard / chat dedup, `run_cycle` |
| `live/test_yad2_live.py`   | Yad2 (real) | **#2** `city=0070` returns markers, `city=70` returns none; `fetch_map` end-to-end |
| `live/test_rentlyfly_live.py` | rentlyfly (real) | **#3** gzip returns listings, `rentlyfly:` uids |
| `live/test_locations_live.py` | autocomplete (real) | Tel Aviv resolves to string id `"5000"`; live `fetch_city_hoods` |

Shared builders/fixtures (`make_listing`, `FakeSender`, the temp-DB `db` fixture)
live in `tests/conftest.py`.
