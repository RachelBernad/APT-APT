#!/bin/sh
set -eu

mkdir -p out

docker rm -f apt-apt-telegram-bot >/dev/null 2>&1 || true
docker run -d \
  --name apt-apt-telegram-bot \
  --restart unless-stopped \
  --env-file .env \
  -v "$(pwd)/out:/app/out" \
  apt-apt-telegram-bot:latest

docker logs --tail 50 apt-apt-telegram-bot
