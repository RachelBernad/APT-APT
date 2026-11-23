# APT-APT

A multi-user Telegram bot that watches **Yad2** rental listings. Each chat creates
**monitors**: one filter set (price, rooms, size, floor, property type and condition,
and features such as elevator or ממ״ד) over any number of areas, picked by fuzzy
Hebrew/English search or by browsing a city's neighborhoods. The bot scrapes each
distinct area once per cycle, matches every listing against every monitor locally,
and sends only **new** listings and **price changes**, each with a photo preview.

## Layout

```
aptbot/                     the bot (run: python -m aptbot)
  __main__.py               entry point: Telegram polling + periodic scrape cycle
  config.py                 every setting, env-overridable
  models.py                 shared dataclasses, feature/property/condition tables
  db.py                     async SQLite (users, monitors, seen listings)
  engine.py                 scrape once per area → match per monitor → route
  bot/                      Telegram-facing: handlers, activity log, formatting, HTML report
  scrapers/
    yad2/                   the Yad2 source, self-contained:
      gateway.py              listings map feed (200-cap price bisection)
      locations.py            area search (autocomplete) + bundled catalog browse
      harvest_locations.py    rebuilds data/ (python -m aptbot.scrapers.yad2.harvest_locations)
      data/                   bundled location catalog
      docs/                   reverse-engineered API reference (start at docs/README.md)
deploy/                     Dockerfile, compose, build/run/stop scripts (see deploy/README.md)
tests/                      unit (offline) + live smoke tests (see tests/README.md)
archive/                    retired scrapers: Facebook Marketplace, rentlyfly.ai (not imported)
```

To add a source, create `aptbot/scrapers/<name>/` that returns the shared listing
dict (`aptbot/models.py`, `LISTING_KEYS`) and call it from `aptbot.engine.scrape_group`.

## Run locally

```bash
python -m venv .venv && .venv/bin/pip install -r requirements.txt
cp .env.example .env            # set TELEGRAM_BOT_TOKEN
set -a; . ./.env; set +a
.venv/bin/python -m aptbot      # writes out/bot.db and out/bot.log
```

## Deploy

```bash
deploy/build-docker.sh && deploy/run-docker.sh
```

Details, including the Raspberry Pi and the fresh-start DB policy, are in
[`deploy/README.md`](deploy/README.md).

## Test

```bash
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/python -m pytest -q          # offline
.venv/bin/python -m pytest -m live -q  # real Yad2 gateway
```

## Bot commands

`/start`, `/mysearches`, `/report` (an HTML page of current matches), `/mute`,
`/unmute`, `/help`, plus the persistent tap bar (Add monitor / My searches / Daily report / Help).
