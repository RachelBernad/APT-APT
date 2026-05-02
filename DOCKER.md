# Docker

The bot image is Yad2-only and uses the current active Krayot locations from `shared_scrapers_config.py`.

Active filters:
- Max price: `5800`
- Rooms: `3.5` to `4.0`
- Minimum size for Yad2: `65`
- Mamad required: `True`
- Elevator required: `True`

## Build On This Machine

```bash
./build-docker.sh
```

## Run

Create `.env` from `.env.example` and set `TELEGRAM_BOT_TOKEN`, then run:

```bash
./run-docker.sh
```

The container writes persistent state and logs to `./out`.

## Build On Raspberry Pi

Copy this project folder to the Raspberry Pi and run:

```bash
./build-docker.sh
./run-docker.sh
```

The `python:3.12-slim` base image supports Raspberry Pi architectures, so building directly on the Pi is the simplest path.

To stop the running bot:

```bash
./stop-docker.sh
```

`docker-compose.yml` is included for machines that have Docker Compose installed, but the shell scripts above only require plain Docker.

## Export / Import An Image

If you build on a Raspberry Pi and want to move the image elsewhere:

```bash
docker save apt-apt-telegram-bot:latest -o apt-apt-telegram-bot.tar
docker load -i apt-apt-telegram-bot.tar
```

For cross-building from another machine, use Docker Buildx with the Pi platform, for example:

```bash
docker buildx build --platform linux/arm64 -t apt-apt-telegram-bot:pi --load .
```
