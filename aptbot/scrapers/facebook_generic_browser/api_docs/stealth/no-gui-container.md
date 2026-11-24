# No real GUI (a container or a display-less Linux host): headful Chromium under Xvfb

Last verified: 2026-11-24 in **Docker on the Mac (linux/arm64)**, image `deploy/fbgb-headless/`
(Debian 13, Chromium 156, patchright 1.64.4). Not yet run on another host. Facebook was NOT used in this
validation (detector pages only).

## Why not headless
Headless Chrome (even `--headless=new`) is the most fingerprinted automation mode. We run the normal headful
browser on a **virtual X display** (Xvfb) with a window manager (openbox), so the page sees a desktop.

## Verified setup (all in `deploy/fbgb-headless/`)
| Piece | Setting | Why (measured) |
|---|---|---|
| Browser | Debian `chromium` through its `/usr/bin/chromium` wrapper | Google Chrome has no linux-arm64 build. The wrapper's flags (`--enable-gpu-rasterization`, `--disable-pings`, …) are what every Debian Chromium user has |
| Display | `Xvfb :99 -screen 0 1920x1080x24 -nolisten tcp` + openbox | headful without a monitor |
| Sandbox | **custom seccomp** `seccomp-chromium.json` = Docker's default + allow `clone`/`unshare`/`setns` | Docker's default blocks namespaces → Chromium **fails to start** ("Failed to move to new namespace: EPERM"). The setuid helper (`chromium-sandbox`, `--disable-namespace-sandbox`) fails the same way. `--no-sandbox` is never used. No `CAP_SYS_ADMIN`. Result: `chrome://sandbox` "adequately sandboxed" |
| WebGL | `FBGB_CHROME_ARGS="--use-gl=angle --use-angle=gl --ignore-gpu-blocklist"` | without it: **no WebGL at all** (Chrome ≥137 has no software fallback). `--enable-unsafe-swiftshader` gives "SwiftShader" (a known automation tell). With Mesa through Xvfb's GLX: `ANGLE (Mesa, llvmpipe …)`, i.e. a Linux desktop without GPU drivers. WebGL2 also works |
| Window | `--start-maximized` | like a real user (otherwise 937×913 on a 1920×1080 screen) |
| Work area | openbox `rc.xml` margin top 32 px | without it `screen.availHeight == screen.height` (no taskbar: headless/kiosk signal) |
| Audio | PulseAudio + null sink "Speakers" | without it `enumerateDevices()` is empty. A real desktop has an audio output |
| Locale/time | `TZ=Asia/Jerusalem`, `LANG=en_US.UTF-8`, Hebrew fonts (culmus, Noto) | must match the Israeli home IP and render Hebrew |
| Shared memory | `--shm-size=1g` | Chromium crashes on Docker's 64 MB default under load |

## Results (detector run `explore/validate.py patchright`; Mac = real Chrome for comparison)
| Check | Mac | Docker, no GUI |
|---|---|---|
| rebrowser | all green | all green **except `useragent`** (see below) |
| bot.sannysoft | 0 failed | 0 failed |
| BrowserScan | Normal | Normal |
| CreepJS headless / stealth | 0% / 0% | 0% / 0% |
| CreepJS "like headless" (heuristic) | 31% | 38% (was 50% before the work-area/audio fixes, 44% without WebGL) |
| hidden-tab throttling | 3 ticks/3 s | 3 ticks/3 s |
| UA ↔ platform ↔ client hints ↔ worker | consistent | consistent (`X11; Linux x86_64` everywhere: Chrome's frozen Linux UA, also on ARM) |

**Accepted tell, `useragent`:** `userAgentData.brands` says "Chromium", not "Google Chrome". rebrowser warns that this
*might* be Chrome for Testing. Unavoidable on Linux ARM (Chromium is the stock browser of most ARM Linux distros, so
common for real users there).
**Remaining differences (normal for a Linux desktop):** no `navigator.share`, DPR 1, 24-bit color, no mic/camera,
software WebGL.

## Re-check anywhere
`python -m aptbot.scrapers.facebook_generic_browser.explore.tools.envprobe`: sandbox, WebGL, UA/brands, tz, screen
and work area, media devices, fonts. Run it on every new machine and compare with this page.

## On a new host (still to do per host)
- Same image (or the same packages natively). A distro's own Chromium build differs: re-run `envprobe` + `validate`.
- WebGL will still be llvmpipe under Xvfb. A GPU-backed renderer would need a real X/Wayland session on the host's
  GPU driver (e.g. `labwc`/`cage` headless backend, or Xorg dummy + glamor). Unverified and optional.
- Login: run the container with `VNC=1 -p 127.0.0.1:5900:5900` (on a remote host: `ssh -L 5900:localhost:5900 <host>`), open
  `vnc://localhost:5900`, `fbgb login`, log in by hand. Or skip VNC: `fbgb login --stdin` (verified in Docker). A new device for the account. Expect "confirm it's you" once.
