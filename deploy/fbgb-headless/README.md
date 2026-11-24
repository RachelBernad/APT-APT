# fbgb-headless: Facebook browser library in a container with no real display

Test image for running `facebook_generic_browser` on any host without a GUI, validated on the Mac's
Docker (linux/arm64). It's separate from the bot image. Findings: `aptbot/scrapers/facebook_generic_browser/api_docs/stealth/no-gui-container.md`.

```bash
# build (from the repo root)
docker build -f deploy/fbgb-headless/Dockerfile -t fbgb-headless:test .

RUN="docker run --rm --shm-size=1g --security-opt seccomp=deploy/fbgb-headless/seccomp-chromium.json"
# environment check (sandbox, WebGL, desktop signals): no site involved
$RUN fbgb-headless:test python -m aptbot.scrapers.facebook_generic_browser.explore.tools.envprobe
# detector A/B (rebrowser, sannysoft, CreepJS, BrowserScan); results on the host in /tmp/fbgb_docker_val
$RUN -v /tmp/fbgb_docker_val:/home/fbgb/.fbgb/validation fbgb-headless:test \
     python -m aptbot.scrapers.facebook_generic_browser.explore.validate patchright
```
(In zsh, use the full command instead of `$RUN`, or a function.)

## Required run options
| Option | Why |
|---|---|
| `--security-opt seccomp=deploy/fbgb-headless/seccomp-chromium.json` | Chromium's sandbox needs namespaces. This is Docker's default profile + exactly one rule (`clone`/`unshare`/`setns`). Never `--no-sandbox`, never `--privileged`/`CAP_SYS_ADMIN` |
| `--shm-size=1g` | Chromium needs more than Docker's 64 MB `/dev/shm` |
| volume on `/home/fbgb/.fbgb` | profile (= the logged-in account) + cache. Keep it outside the image |
| no `-p` except VNC on `127.0.0.1` during login | Xvfb has no TCP listener. Nothing else listens |

## One-time login
Assisted (no VNC needed; credentials only on stdin, never stored):
```bash
docker run --rm -i --shm-size=1g --security-opt seccomp=deploy/fbgb-headless/seccomp-chromium.json \
  -v fbgb-home:/home/fbgb/.fbgb fbgb-headless:test \
  python -m aptbot.scrapers.facebook_generic_browser login --stdin     # then type/paste: email⏎ password⏎ Ctrl-D
```
If it stops at a code/checkpoint page, finish by hand over VNC:
```bash
docker run --rm -it --shm-size=1g --security-opt seccomp=deploy/fbgb-headless/seccomp-chromium.json \
  -e VNC=1 -p 127.0.0.1:5900:5900 -v fbgb-home:/home/fbgb/.fbgb fbgb-headless:test \
  python -m aptbot.scrapers.facebook_generic_browser login
# password (random per container start): docker exec <container> cat /tmp/vncpass.txt
# then open vnc://localhost:5900 (macOS Screen Sharing), log in by hand, close the browser window
```
This is a **new device** for the Facebook account (Linux/Chromium). Facebook may ask "confirm it's you" once.
Verified 2026-11-24: login in this container → two-step check page passed → home feed, then `groups` (14, same as the
Mac) and `posts 305724686290054 --limit 12` (15 s incl. browser start), state OK. The session lives in the
`fbgb-home` volume, so later containers start logged in.

**Don't copy the Mac profile instead:** Chrome on macOS encrypts cookies with a Keychain key (`v11` values), which
Linux Chromium can't decrypt, and the Mac profile is from a newer Chrome than Debian's Chromium. Moving the session
cookies themselves would look like cookie theft (same session, different OS/fingerprint). Log in fresh.

The entrypoint deletes Chromium's `Singleton*` lock files at start: a killed container leaves them pointing at its old
hostname, and Chromium then hangs on the profile.

## What the entrypoint starts
Xvfb `:99` (1920×1080×24, `-nolisten tcp`) → openbox (32 px top work-area margin, like a desktop taskbar) →
PulseAudio with a null "Speakers" sink → optional x11vnc → your command.
`FBGB_CHROME_ARGS` (baked in): `--use-gl=angle --use-angle=gl --ignore-gpu-blocklist --start-maximized` (Mesa WebGL,
maximized window).
