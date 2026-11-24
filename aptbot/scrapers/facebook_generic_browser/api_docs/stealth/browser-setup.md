# Browser setup: the validated launch config

Last verified: 2026-11-23 (macOS 26.5.2 arm64, Google Chrome 158.0.8127.37, patchright 1.64.4)

Code: `explore/launch.py` (`open_context`). Validation harness: `explore/validate.py`. Raw results are in
`~/.fbgb/validation/<engine>/summary.json` + screenshots.

## Config
```python
pw.chromium.launch_persistent_context(
    user_data_dir,                 # ~/.fbgb/profile, outside the repo
    channel="chrome",              # installed Google Chrome (Linux: executable_path=/usr/bin/chromium)
    headless=False,                # never headless (on the Pi: headful under Xvfb)
    no_viewport=True,              # window size = real window size, no emulated viewport
    chromium_sandbox=True,         # Playwright's default adds --no-sandbox
    focus_control=True,            # disables Playwright's focus emulation (see below)
    ignore_default_args=IGNORED_DEFAULT_ARGS,
)
```
No `user_agent`, `locale`, `timezone_id`, `extra_http_headers`, `viewport`, init scripts or fingerprint
injection. Everything is inherited from the real OS, so it stays consistent.

### `IGNORED_DEFAULT_ARGS`: Playwright defaults removed
| Default switch | Why it's removed |
|---|---|
| `--disable-features=…ThirdPartyStoragePartitioning…` (the whole list) | an iframe can detect that 3rd-party storage partitioning is off (rebrowser `storagePartitioning` 🔴) |
| `--disable-background-timer-throttling`, `--disable-backgrounding-occluded-windows`, `--disable-renderer-backgrounding` | with these, hidden tabs run timers at full speed (30 ticks/3 s vs. 3 in a real Chrome) |
| `--force-color-profile=srgb` | a real Mac reports its display's color profile |

Kept on purpose: `--password-store=basic --use-mock-keychain`. The profile's cookies are encrypted with that
key, so removing them later logs the profile out. They aren't visible to pages.

### Focus emulation (`focus_control=True`)
By default Playwright sends `Emulation.setFocusEmulationEnabled` to every page. Then **every tab reports
`document.hasFocus() === true` and `visibilityState === "visible"` at the same time**, which no real browser does.
Measured with `/tmp` probe (2 tabs + a Cmd+click background tab):

| | default | `focus_control=True` |
|---|---|---|
| background tab via `new_page()` | `visible`, focused | `hidden`, not focused |
| Cmd+click background tab | `visible`, focused | `hidden`, not focused, 0×0 until shown |
| after `bring_to_front()` | n/a | front tab `visible`+focused, the others `hidden` |

Consequence: **input must only go to the front tab** (`tabs.TabPool.foreground`).

### Final Chrome command line (from `chrome://version`)
```
--disable-field-trial-config --disable-background-networking --disable-breakpad --no-default-browser-check
--disable-dev-shm-usage --disable-edgeupdater --enable-features=CDPScreenshotNewSurface --disable-hang-monitor
--disable-prompt-on-repost --disable-updater-scheduler --no-first-run --password-store=basic --use-mock-keychain
--no-service-autorun --export-tagged-pdf --disable-search-engine-choice-screen --edge-skip-compat-layer-relaunch
--disable-infobars --disable-sync --disable-blink-features=AutomationControlled --user-data-dir=… --remote-debugging-pipe
```
`--remote-debugging-pipe` means no TCP debug port for a page or a local scan to find.

## Validation results (2026-11-23, headful, real Chrome, same machine)

| Check | vanilla Playwright | zendriver 0.18 | **Patchright (final config)** |
|---|---|---|---|
| rebrowser `runtimeEnableLeak` | 🔴 | 🟢 | 🟢 |
| rebrowser `navigatorWebdriver` | 🔴 (`webdriver=true`) | 🟢 | 🟢 |
| rebrowser `sourceUrlLeak` (after an `evaluate` probe) | 🔴 `UtilityScript.` in the stack | 🟢 (but ran in the **main world**) | ⚪ not triggered: evaluate runs in an **isolated world**, the page can't see it |
| rebrowser `storagePartitioning` | 🔴 | 🟢 | 🟢 (🔴 before `IGNORED_DEFAULT_ARGS`) |
| rebrowser `ipcFloodingProtection` / `popupBlocking` | 🔴 / 🟠 | 🟢 / 🟢 | 🟢 / 🟢 |
| other rebrowser tests (pwInitScripts, cdpBindingGlobals, headless, nativeTampering, workerConsistency, useragent, bypassCsp, platformMismatch, viewport) | 🟢 | 🟢 | 🟢 |
| bot.sannysoft (failed rows) | 1 (WebDriver present) | 0 | 0 |
| BrowserScan bot detection | **Robot** | Normal | **Normal** |
| CreepJS headless / stealth | 33% / 0% | 0% / 0% | 0% / 0% |
| CreepJS "like headless" | 31% | 25% | 31% **Unverified** baseline: not yet compared with a hand-opened Chrome on this Mac |
| hidden-tab timer ticks in 3 s (100 ms interval) | 30 | n/a | **3** (real Chrome throttling) |

Fingerprint (matches the host hardware and OS; nothing spoofed): UA `Chrome/158.0.0.0 … Mac OS X 10_15_7`, platform
`MacIntel`, 10 cores, 16 GB, 5 plugins, `window.chrome` present, TZ `Asia/Jerusalem`, WebGL
`ANGLE (Apple, ANGLE Metal Renderer: Apple M4)`.

**Decision: Patchright with this config.** zendriver also passes, but its `evaluate` runs in the page's main
world (observable through hooked DOM methods), it's AGPL, and network capture is lower-level.

## Rules that follow from this
- Use `locator.*` / `page.evaluate` freely for *reading*. Patchright runs them in an isolated world.
- Never `page.expose_function` / `expose_binding` / `add_init_script` (they create main-world artifacts;
  rebrowser `exposeFunctionLeak` / `pwInitScripts`).
- Never `page.route` / request interception (rebrowser `documentInterception`, and it changes network timing).
  Read responses with `context.on("response")` only.
- Re-run `validate.py patchright` after any patchright or Chrome upgrade.
