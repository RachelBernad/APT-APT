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

## Log in (once per machine)
`fbgb login` opens Chrome on the profile. Log in **by hand** (2FA, "trust this device"), then close the window.
It prints `logged in: yes` once the session cookies exist. Facebook may ask "confirm it's you" on a new device:
answer it by hand.

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

## Raspberry Pi (no GUI): planned, not yet run
See `../api_docs/stealth/pi-headless.md`: Debian Chromium headful under Xvfb + openbox, `x11vnc -localhost` over an
SSH tunnel for the one-time manual login, `FBGB_BROWSER=/usr/bin/chromium`, `max_tabs=2`. The future
in-container service with authentication is designed in SPEC §12.
