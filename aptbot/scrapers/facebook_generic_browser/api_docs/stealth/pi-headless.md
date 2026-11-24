# Raspberry Pi (no GUI): headful Chromium under Xvfb

Last verified: n/a. **Unverified: designed, not yet run on the Pi.** Validate with `explore/validate.py patchright`
on the Pi before the first Facebook session there, and update this page.

## Why not headless
Headless Chrome, even `--headless=new`, is the most widely fingerprinted automation mode (GPU/plugins/UA-CH
differences, rendering timing). Instead we run the normal headful browser on a **virtual X display**.

## Setup (Raspberry Pi OS / Debian bookworm, arm64)
```bash
sudo apt install -y chromium xvfb openbox x11vnc fonts-noto fonts-noto-color-emoji fonts-culmus  # Hebrew fonts!
Xvfb :99 -screen 0 1920x1080x24 -nolisten tcp &       # virtual screen with a realistic resolution
DISPLAY=:99 openbox &                                 # a window manager: real window decorations, focus, sizes
DISPLAY=:99 x11vnc -display :99 -localhost -forever -nopw -quiet &   # only reachable through SSH
export DISPLAY=:99 FBGB_BROWSER=/usr/bin/chromium
```
On the Mac: `ssh -L 5900:localhost:5900 pi` and then open `vnc://localhost:5900`. You see the Pi's Chromium
and can **log in by hand** (`open_profile.py`), watch sessions, and solve a checkpoint yourself.

- Google Chrome has no linux-arm64 build, so this uses Debian's Chromium (`executable_path`). Its UA says `Chromium` in
  client hints and `X11; Linux aarch64`: an unusual but **consistent** device. Never spoof it to look like a Mac.
- A Mac profile **can't be copied** to the Pi (cookies are encrypted per OS). The Pi gets its own profile and its own
  manual login, and Facebook sees it as a new device once.
- Missing fonts are a fingerprint (and break Hebrew rendering). Install the Noto + Culmus families.

## Known tells to measure on the Pi
| Tell | Expectation | Mitigation if it matters |
|---|---|---|
| WebGL renderer under Xvfb | `llvmpipe` / `SwiftShader` (software GL: a VM/bot signal) | try a headless Wayland compositor (`labwc`/`cage` with `WLR_BACKENDS=headless`) using the Pi's V3D GPU, or `xserver-xorg-video-dummy` with glamor |
| Screen = window size, no taskbar | `screen.availHeight == screen.height` | openbox + a thin panel (`tint2`), or leave as is (Unverified whether FB checks) |
| Low `hardwareConcurrency` / memory | 4 cores, 4–8 GB: matches a real Pi, fine | none (don't spoof) |
| Speed | Pi 4/5 Chromium on Facebook is slow; fewer tabs (max 2) | `TabPool(max_tabs=2)` |

## Running as a service
Use a systemd user unit that starts Xvfb, openbox and the scraper (`After=network-online.target`), separate from
the aptbot Docker container. In a container: `--shm-size=1g`, Xvfb inside it, and the profile on a volume.
