# Operations

## Install (Mac)
```bash
uv venv ~/.fbgb/venv --python 3.13
uv pip install --python ~/.fbgb/venv/bin/python -r aptbot/scrapers/facebook_generic_browser/requirements.txt
# uses the installed Google Chrome; without it: ~/.fbgb/venv/bin/patchright install chrome
fbgb() { ~/.fbgb/venv/bin/python -m aptbot.scrapers.facebook_generic_browser "$@"; }   # from the repo root
```
The bot's own venv/image is untouched. The library only needs to be importable where it runs.

## Files (all outside the repo)
| Path | What | Sensitive |
|---|---|---|
| `~/.fbgb/profile/` | Chrome profile with the logged-in session | **yes**: it is the account. Never copy it into the repo or logs |
| `~/.fbgb/fbgb.db` | cache (groups, posts, watermarks) | contains real posts |
| `~/.fbgb/HALTED` | circuit-breaker marker (delete only via `fbgb resume`) | no |
| `~/.fbgb/captures/` | raw payloads (only with `debug_dump=True` / explore scripts) | contains real posts |
Override with `FBGB_HOME`, `FBGB_PROFILE_DIR`, `FBGB_BROWSER` (explicit Chrome/Chromium binary).
`FBGB_NAV_TIMEOUT` (seconds, default 45) is the page-load timeout; slow hosts such as a Raspberry Pi 3 need about 120.

## Log in (once per machine)
Two ways, both print only `logged in: yes/no` (by cookie name):
- `fbgb login`: Chrome opens on the profile. Log in **by hand** (2FA, "trust this device"), then close the window.
- `fbgb login --stdin`: **assisted**. Email and password come from stdin (2 lines) or a no-echo prompt and are typed
  with human timing. They're held in memory only: never in files, env vars, argv, logs or the repo. In Docker:
  `docker exec -i <container> python -m aptbot.scrapers.facebook_generic_browser login --stdin` and pipe/paste
  the two lines. If Facebook asks for a code or a "confirm it's you" step, it stops and says where. Finish that step
  with `fbgb login` (over VNC on a no-GUI box).
Verified 2026-11-24 in the Docker image: assisted login → a short two-step page that passed by itself → home feed.

## Validate the stealth setup
`fbgb validate` runs the detection pages (rebrowser, sannysoft, CreepJS, BrowserScan) with a throwaway profile, with
no Facebook involved. Do it after every patchright or Chrome upgrade and compare with
`../api_docs/stealth/browser-setup.md`.

## Health routine
- `fbgb status`: `state` must be `ok`/`stopped`.
- After a `Halted`: open the account by hand, resolve it, wait, then `fbgb resume`.
- Logged out (`NotLoggedIn`): `fbgb login` again.

## Upgrading patchright
`launch.IGNORED_DEFAULT_ARGS` (in `browser.py`) mirrors Playwright's default switches **by exact string**. After an
upgrade, run `fbgb validate`. If `storagePartitioning` turns red, copy the new `disabledFeatures` list from
`patchright/driver/package/lib/coreBundle.js` (search `chromiumSwitches`) into `_PW_DISABLED_FEATURES`.

## No GUI (Docker container)
Validated in Docker (linux/arm64) on the Mac: `deploy/fbgb-headless/README.md` (build, run options, VNC login) and
`../api_docs/stealth/no-gui-container.md` (what each setting fixes, measured). Use `max_tabs=2` on a low-CPU host. Check any new
machine with `python -m aptbot.scrapers.facebook_generic_browser.explore.tools.envprobe`. The future in-container
service with authentication is designed in SPEC §12.
