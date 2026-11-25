#!/bin/sh
# Run the bot (Yad2 + Facebook) in the background. Needs ./.env (TELEGRAM_BOT_TOKEN,
# ALLOWED_CHAT_IDS) and a logged-in Facebook profile in the `fbgb-home` volume
# (one-time login: deploy/fbgb-headless/README.md). DB + log go to ./out. AI summaries
# (AI_ENABLED=1) need the ChatGPT token in the `aptbot-ai` volume: see README.md here.
set -eu
cd "$(dirname "$0")/../.."
[ -f .env ] || { echo "missing .env (copy .env.example)"; exit 1; }
mkdir -p out
docker rm -f aptbot-fb >/dev/null 2>&1 || true
docker run -d --name aptbot-fb --restart unless-stopped \
  --shm-size=1g --security-opt seccomp=deploy/fbgb-headless/seccomp-chromium.json \
  -v fbgb-home:/home/fbgb/.fbgb -v aptbot-ai:/home/fbgb/.aptbot -v "$PWD/out:/app/out" \
  --env-file .env aptbot-fb:latest
echo "started; follow the log with: docker logs -f aptbot-fb"
