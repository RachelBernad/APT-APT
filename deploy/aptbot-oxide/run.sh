#!/bin/sh
# Run the bot with FB_BROWSER=oxide. Needs ./.env and the oxide session jar in the `fbgb-home` volume
# (oxide_jar.json, made once with aptbot/browsers/oxide/tools/chrome_to_jar.py: see README.md here).
# NEVER run this and the Chrome image (aptbot-fb) at the same time: one browser per Facebook session.
set -eu
cd "$(dirname "$0")/../.."
[ -f .env ] || { echo "missing .env (copy .env.example)"; exit 1; }
mkdir -p out && chmod 700 out
docker rm -f aptbot-fb aptbot-oxide >/dev/null 2>&1 || true
docker run -d --name aptbot-oxide --restart unless-stopped --memory=1g \
  -v fbgb-home:/home/fbgb/.fbgb -v aptbot-ai:/home/fbgb/.aptbot -v "$PWD/out:/app/out" \
  --env-file .env -e FB_BROWSER=oxide aptbot-oxide:latest
echo "started; follow the log with: docker logs -f aptbot-oxide"
