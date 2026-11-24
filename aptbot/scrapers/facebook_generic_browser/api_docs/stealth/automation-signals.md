# Automation signals: what gives a controlled browser away

Last verified: 2026-11-23 (signals marked ✅ were measured in `explore/validate.py`; the rest are **Unverified**,
from public research, with the source named)

Detection works in layers. Each anomaly adds to a risk score. For a logged-in Facebook account the
**behavior and account layers matter most** (see `behavior-model.md`, `rate-limits-and-checkpoints.md`),
but the browser layer has to be clean first, or nothing else helps.

## 1. Browser / protocol layer
| Signal | How a page sees it | Status with our config |
|---|---|---|
| `navigator.webdriver === true` | direct read | ✅ false (`--disable-blink-features=AutomationControlled`) |
| `--enable-automation` infobar/flags | UI + some APIs | ✅ removed by Patchright |
| CDP `Runtime.enable` | V8 serializes console args: a getter on an Error passed to `console.debug` fires | ✅ not sent by Patchright (rebrowser `runtimeEnableLeak` 🟢) |
| `Console.enable` | similar | ✅ patched by Patchright |
| Evaluated-script stack traces (`UtilityScript.`, `__playwright_evaluation_script__`, `pptr:`) | page hooks a DOM getter and reads `new Error().stack` | ✅ our evaluates run in an isolated world (the hook never fires) |
| Main-world globals (`__pwInitScripts`, `__playwright__binding__`, cdc_ vars) | `Object.keys(window)` | ✅ none (no init scripts / bindings) |
| 3rd-party storage partitioning disabled | cross-site iframe reads localStorage | ✅ fixed via `IGNORED_DEFAULT_ARGS` |
| IPC flooding protection / popup blocking disabled | rate of `history.pushState`, `window.open` without a gesture | ✅ normal |
| Hidden tabs not throttled | timer accuracy while `visibilityState=hidden` | ✅ throttled like Chrome (3 ticks/3 s) |
| All tabs focused + visible at once | `document.hasFocus()`, `visibilitychange` events | ✅ fixed with `focus_control=True` |
| Headless (`HeadlessChrome` UA, missing plugins, GPU = SwiftShader) | UA/userAgentData, `navigator.plugins`, WebGL renderer | ✅ headful on the Mac. **No-GUI container: WebGL under Xvfb = llvmpipe/SwiftShader**, see `no-gui-container.md` |
| Spoofed-fingerprint inconsistencies (UA vs. client hints vs. platform, patched natives' `toString`) | cross-checks; CreepJS "lies" | ✅ nothing spoofed, so nothing inconsistent |
| Emulated viewport (1280×720 defaults) | `innerWidth/innerHeight` vs `outerWidth/Height` | ✅ `no_viewport=True` |
| Request interception (`page.route`) | Service Worker / timing / header oddities | ✅ not used |

## 2. Input layer (Unverified: Meta doesn't publish its signals; these are common industry signals)
- `isTrusted` events: CDP `Input.dispatch*` produces trusted events, the same as a real device. ✅ OK.
- No `mousemove` before a `click`, teleporting pointers, clicks at the exact element center: avoided by `human.click`
  (Bezier path, jittered point, press-release 50–130 ms).
- `window.scrollTo` / `element.scrollIntoView` without wheel events: avoided by `human.wheel` (wheel ticks only).
- Typing at constant speed or via `fill()`: avoided by `human.type_text` (per-key log-normal gaps).
- Input to a hidden tab: avoided by `TabPool.foreground`.

## 3. Network / account layer (Unverified, from scraping-industry reports, see SPEC "Sources")
- IP changes inside a session, datacenter or proxy ASNs → checkpoint. Use the home connection, no proxy.
- A fresh login followed by a burst of page loads → checkpoint. Log in by hand, let the profile age, and start slow.
- A new device or browser fingerprint for an existing account → "confirm it's you". Moving to the Pi = a new device,
  so log in there by hand, once.
- Volume and regularity: requests at fixed intervals, 24/7 activity, opening hundreds of posts per hour.

## Sources
- rebrowser bot-detector tests: https://bot-detector.rebrowser.net/ (live), https://github.com/rebrowser/rebrowser-patches
- DataDome on CDP detection: https://datadome.co/threat-research/how-new-headless-chrome-the-cdp-signal-are-impacting-bot-detection/
- Patchright: https://github.com/Kaliiiiiiiiii-Vinyzu/patchright-python
