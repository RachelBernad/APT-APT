#!/bin/sh
set -eu

mkdir -p out

# Run as the host user that owns ./out so the bind-mounted volume is always
# writable, regardless of the image's build-time uid. (The app only writes to
# /app/out; the rest of /app is world-readable.)
docker rm -f apt-apt-telegram-bot >/dev/null 2>&1 || true
docker run -d \
  --name apt-apt-telegram-bot \
  --restart unless-stopped \
  --user "$(id -u):$(id -g)" \
  --env-file .env \
  -v "$(pwd)/out:/app/out" \
  apt-apt-telegram-bot:latest

docker logs --tail 50 apt-apt-telegram-bot
