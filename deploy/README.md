# Deploy (Docker)

Everything deployment-related lives in this folder. The scripts `cd` to the repo
root themselves, so run them from anywhere. **`.env`, `out/` and `.dockerignore`
stay at the repo root**: the build context is the root, and `.dockerignore` has to
sit at the context root, since the Pi's older Docker has no BuildKit and so no
per-Dockerfile ignore files.

## Image

A multi-stage build on `python:3.12-slim` (Debian 12). Dependencies (`requirements.txt`:
`python-telegram-bot[job-queue]`, `aiohttp`, `aiosqlite`, `rapidfuzz`, `Brotli`) are
installed in a builder stage and copied into the runtime stage, so no build toolchain
ships. All of them have prebuilt wheels for `linux/amd64` and `linux/arm64`.

Only the `aptbot/` package is copied into the image, including its bundled Yad2
location catalog at `aptbot/scrapers/yad2/data`. Entrypoint: `python -m aptbot`.

## Build

```bash
deploy/build-docker.sh          # = docker build -f deploy/Dockerfile -t apt-apt-telegram-bot:latest .
docker images apt-apt-telegram-bot:latest
```

## Run

Create `.env` at the repo root from `.env.example` and set `TELEGRAM_BOT_TOKEN`
(optional: `CHECK_INTERVAL_SECONDS`, `DB_PATH`, `BACKFILL_CAP`; every other setting
in `aptbot/config.py` can be overridden the same way). Then:

```bash
deploy/run-docker.sh            # (re)creates the container, tails its logs
deploy/stop-docker.sh
```

The container writes the SQLite DB (`out/bot.db`) and rotating logs (`out/bot.log`)
to the repo-root `out/` volume.

**Fresh-start DB policy:** there are no in-place migrations. If `out/bot.db` comes
from a different schema version, the bot renames it to `out/bot.db.old-<timestamp>`
on startup and creates a clean one, so users have to re-create their monitors.

Alternative to the scripts:

```bash
docker compose -f deploy/docker-compose.yml up -d --build
```

## Raspberry Pi

The `python:3.12-slim` base and all wheels support the Pi (arm64), so you can
build directly on it:

```bash
git pull && deploy/build-docker.sh && deploy/run-docker.sh
```

## Export / Import / Cross-build

```bash
docker save apt-apt-telegram-bot:latest -o apt-apt-telegram-bot.tar
docker load -i apt-apt-telegram-bot.tar
docker buildx build --platform linux/arm64 -f deploy/Dockerfile -t apt-apt-telegram-bot:pi --load .
```
