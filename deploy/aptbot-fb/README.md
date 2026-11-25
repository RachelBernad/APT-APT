# aptbot-fb: the bot with Yad2 + Facebook groups, in one container with no GUI

The bot image built on top of [`../fbgb-headless`](../fbgb-headless/README.md), which provides a headful
Chromium under Xvfb with the validated sandbox and fingerprint settings. Use this image when Facebook is on.
(`deploy/Dockerfile` is the Yad2-only image with no browser.)

```bash
deploy/aptbot-fb/build.sh     # builds fbgb-headless:latest, then aptbot-fb:latest (no BuildKit needed)
deploy/aptbot-fb/run.sh       # starts container "aptbot-fb" (restart unless-stopped)
docker logs -f aptbot-fb      # or out/bot.log
deploy/aptbot-fb/stop.sh      # clean stop: Chrome closes and flushes cookies
```

## Needs
- **`.env` at the repo root** (gitignored): `TELEGRAM_BOT_TOKEN` and `ALLOWED_CHAT_IDS` (see `.env.example`).
  The repo is public, so these never go in a committed file.
- **A logged-in Facebook profile in the `fbgb-home` volume.** It's a one-time login, described in
  [`../fbgb-headless/README.md`](../fbgb-headless/README.md#one-time-login). If the session is missing or Facebook shows a
  warning, the bot stops reading Facebook, tells the allowed chat once, and keeps Yad2 running.
- **Optional: AI summaries** (`AI_ENABLED=1` in `.env`). They need a ChatGPT login stored in the `aptbot-ai`
  volume. The token never touches the repo folder. Log in once with a device code, which works headless: it prints
  a URL and a code to enter in any browser.
  ```bash
  docker run --rm -it --entrypoint python -v aptbot-ai:/home/fbgb/.aptbot aptbot-fb:latest -m aptbot.listing_ai login
  docker run --rm --entrypoint python -v aptbot-ai:/home/fbgb/.aptbot aptbot-fb:latest -m aptbot.listing_ai status
  ```
  Without the login, or when the ChatGPT usage limit is reached, posts are sent as before with no summary.
  See [`aptbot/listing_ai/SPEC.md`](../../aptbot/listing_ai/SPEC.md).
- **One bot instance per token.** A second poller gets `Conflict: terminated by other getUpdates request`.

## Run options (the same as fbgb-headless)
`--shm-size=1g`, `--security-opt seccomp=deploy/fbgb-headless/seccomp-chromium.json`, the `fbgb-home` volume on
`/home/fbgb/.fbgb`, and `./out` on `/app/out` (bot DB + log). No ports are published.
