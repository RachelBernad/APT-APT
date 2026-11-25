# APT-APT

A Telegram bot that watches **Tel Aviv apartment rentals** on two sources and posts what's new:

- **Yad2**: rentals in לב העיר (the whole center quarter), הצפון הישן and נווה צדק. New listings and price
  changes, each with a photo. The neighborhoods are `YAD2_HOODS` in `aptbot/config.py` (overridable in `.env`).
- **Facebook groups**: the Tel Aviv rental groups, read through a logged-in browser. Each new post arrives
  with its text and photos, **once**, even if it's cross-posted to several groups. Dedup uses a hash of the
  post text with leading/trailing whitespace stripped.
- **AI summaries (optional, `AI_ENABLED=1`)**: before a Facebook post is sent, ChatGPT reads it and the bot puts
  a short Hebrew summary above the raw text: kind (rent, roommate, sublet, sale, seeking, or 🚫 not a listing), address,
  price, rooms, m², floor, ממ״ד, elevator and more. It uses your ChatGPT subscription through OAuth (no API key) and
  15 posts per request. Only the kinds in `FB_POST_KINDS` are sent (default `rent`; sofas, movers, roommates, sublets
  and sales are skipped). If the AI is off, failing or at its usage limit, posts are sent as before. See
  [`aptbot/listing_ai/SPEC.md`](aptbot/listing_ai/SPEC.md).

Send `/monitor` in the chat to start; no filters for now. **Only the chats in `ALLOWED_CHAT_IDS`** (set in
`.env`) can use the bot or receive anything from it. Every other chat is ignored, and its id is logged.

## Layout

```
aptbot/                     the bot (run: python -m aptbot)
  __main__.py               entry point: Telegram polling + periodic scrape cycle
  config.py                 every setting, env-overridable
  models.py                 shared dataclasses, feature/property/condition tables
  db.py                     async SQLite (users, monitors, seen Yad2 listings, seen FB posts)
  engine.py                 Yad2: scrape once per area → match per monitor → route
  fb_engine.py              Facebook: poll the groups → dedup by content hash → AI facts → route
  listing_ai/               AI Listing Facts for posts over ChatGPT OAuth (SPEC.md; login: python -m aptbot.listing_ai login)
  bot/                      Telegram-facing: simple.py (live commands), gate.py (allowlist),
                            formatting, activity log; handlers.py = parked filters wizard
  scrapers/
    yad2/                   the Yad2 source, self-contained:
      gateway.py              listings map feed (200-cap price bisection)
      locations.py            area search (autocomplete) + bundled catalog browse
      harvest_locations.py    rebuilds data/ (python -m aptbot.scrapers.yad2.harvest_locations)
      data/                   bundled location catalog
      docs/                   reverse-engineered API reference (start at docs/README.md)
    facebook_generic_browser/  Facebook groups reader via a logged-in Chrome (see its README.md)
    tel_aviv_gis/           municipal GIS: construction/plans near a point (library; not wired in yet)
deploy/                     Yad2-only bot image, compose, scripts (see deploy/README.md)
  aptbot-fb/                the bot with Yad2 + Facebook in one no-GUI container (see its README.md)
  fbgb-headless/            its base: headful Chromium under Xvfb (see its README.md)
tests/                      unit (offline) + live smoke tests (see tests/README.md)
archive/                    retired scrapers: old Facebook Marketplace, rentlyfly.ai (not imported)
```

To add a source, create `aptbot/scrapers/<name>/` that returns the shared listing
dict (`aptbot/models.py`, `LISTING_KEYS`) and call it from `aptbot.engine.scrape_group`.

## Run (Mac or any Docker host)

```bash
cp .env.example .env            # set TELEGRAM_BOT_TOKEN and ALLOWED_CHAT_IDS
deploy/aptbot-fb/build.sh && deploy/aptbot-fb/run.sh
```

This needs a logged-in Facebook profile in the `fbgb-home` Docker volume, which is a one-time login (see
[`deploy/aptbot-fb/README.md`](deploy/aptbot-fb/README.md)). For Yad2 only, set `FB_ENABLED=0` and use the
lighter image (`deploy/build-docker.sh && deploy/run-docker.sh`), or run it directly with
`python -m aptbot` after `pip install -r requirements.txt`. Details on the Raspberry Pi and the fresh-start DB policy are
in [`deploy/README.md`](deploy/README.md).

Domain terms are in [`GLOSSARY.md`](GLOSSARY.md) and decisions in [`docs/adr/`](docs/adr/).

## Test

```bash
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/python -m pytest -q          # offline
.venv/bin/python -m pytest -m live -q  # real Yad2 gateway
```

## Bot commands

`/monitor` (start watching Tel Aviv on both sources; you first get a short taster of what's there now),
`/stop`, `/status` (last check of each source, the AI summaries' state) and `/help`. In a group, use them
as commands (`/monitor@YourBot` also works), since bots in groups only see commands.
