# Docker

Multi-user apartment bot. Each Telegram chat adds its own **saved searches**
(any Yad2 location, picked via fuzzy Hebrew search, with per-search price / rooms
/ size / mamad / elevator filters) and is notified only about **new** listings.
For Tel Aviv searches, `rentlyfly.ai` (Facebook Groups) is layered on top of Yad2.

State (users, searches, seen listings, enrichment cache) is stored in a SQLite
database at `out/bot.db`, persisted via the `./out` volume.

## Image

A multi-stage build on `python:3.12-slim` (Debian 12). Dependencies are installed
in a builder stage and copied into the runtime stage, so no build toolchain ships
in the final image. All dependencies (`python-telegram-bot`, `aiohttp`,
`aiosqlite`, `rapidfuzz`, `beautifulsoup4`, `Brotli`, APScheduler) ship prebuilt
wheels for `linux/amd64` and `linux/arm64`, so builds need no compiler.

Check the built size:

```bash
docker images apt-apt-telegram-bot:latest
```

## Build

```bash
./build-docker.sh
```

## Run

Create `.env` from `.env.example` and set `TELEGRAM_BOT_TOKEN` (see `.env.example`
for optional `CHECK_INTERVAL_SECONDS`, `DB_PATH`, `ENABLE_RENTLYFLY`), then:

```bash
./run-docker.sh
```

The container writes the SQLite DB and logs to `./out`. Stop with:

```bash
./stop-docker.sh
```

`docker-compose.yml` is provided as an alternative to the shell scripts.

## Raspberry Pi

The `python:3.12-slim` base and all wheels support Pi (arm64), so building on the
Pi works directly:

```bash
./build-docker.sh && ./run-docker.sh
```

## Export / Import / Cross-build

```bash
docker save apt-apt-telegram-bot:latest -o apt-apt-telegram-bot.tar
docker load -i apt-apt-telegram-bot.tar
docker buildx build --platform linux/arm64 -t apt-apt-telegram-bot:pi --load .
```
