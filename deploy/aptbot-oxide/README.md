# aptbot-oxide: the bot with Facebook on BrowserOxide

The same bot as `deploy/aptbot-fb`, but Facebook is read by a BrowserOxide worker (`FB_BROWSER=oxide`) instead of
Chromium under Xvfb. No display, no Chromium, no seccomp profile. The FB worker peaks at ~545-585 MB and is restarted
between groups, so it fits a 1 GB Pi. Design: `aptbot/browsers/oxide/kb/aptbot-integration.md`.

## Build (on the Mac) and move to the Pi
```sh
deploy/aptbot-oxide/build.sh                         # ~15-25 min the first time (Rust + V8), ~6 GB RAM
docker save aptbot-oxide:latest | gzip | ssh pi-hermes 'gunzip | docker load'
```

## One-time: move the session from Chrome to oxide (on the Pi)
One browser per Facebook session: from here on, Chrome must not run on this profile again.
```sh
docker stop aptbot-fb                                # Chrome closes cleanly and flushes its cookies
docker run --rm -v fbgb-home:/home/fbgb/.fbgb aptbot-fb:latest \
  python /app/aptbot/browsers/oxide/tools/chrome_to_jar.py --profile /home/fbgb/.fbgb/profile /home/fbgb/.fbgb/oxide_jar.json
```
The converter prints cookie **names** only and refuses to write a jar without `c_user`/`xs`. (The `aptbot-fb` image
needs this repo version so the tool is in it; otherwise `docker cp` the script in.)

## Run
```sh
deploy/aptbot-oxide/run.sh                           # stops aptbot-fb and aptbot-oxide first; --memory=1g
docker logs -f aptbot-oxide
```
Cadence: `FB_POLL_MIN/MAX_SECONDS` (default 8-12 min), `FB_QUIET_HOURS` (default 01-07 Israel time).
Worker limits: `FBGB_OXIDE_RSS_LIMIT_MB` (kill, default 650).

## If Facebook stops the account
The bot halts (`<FBGB_HOME>/HALTED`) on any checkpoint, login wall or block and doesn't retry. A human checks the
account in a normal browser, then clears the halt. Never just restart into it.

## Going back to Chrome
`docker stop aptbot-oxide && deploy/aptbot-fb/run.sh`. Chrome's profile still holds the old session, and it may have
been rotated by oxide in the meantime. If FB asks to log in, log in by hand (deploy/fbgb-headless/README.md).
