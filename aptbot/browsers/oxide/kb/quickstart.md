# Quickstart

[← index](README.md) · related: [env-vars](env-vars.md), [input-and-scrolling](input-and-scrolling.md), [caveats](caveats.md)

## Build

Commands below run from the private workdir, with `$OXIDE` = the repo's `aptbot/browsers/oxide` ([where things are](README.md#where-things-are)):
```sh
cd ~/aptbot-browser-research/browseroxide; OXIDE=~/TomerShit/APT-APT/aptbot/browsers/oxide
BO_MEM_MB=6000 BO_TIMEOUT=1800 venv/bin/python $OXIDE/tools/guard.py sh $OXIDE/build.sh
```
`build.sh` runs maturin in release mode. The cargo target dir is `$OXIDE_TARGET` (default `~/.cache/aptbot-oxide-target`, never
inside the repo) and the log is `$OXIDE_TARGET/build.log`. It reinstalls the wheel into `$OXIDE_PY` (default the workdir `venv/`).
A first build from a new target dir compiles everything (~15 min). The Pi image builds in Docker ([aptbot-integration](aptbot-integration.md#build)). An incremental build after a JS change takes about 1–2 min. The `js_runtime/js/*.js` files are compiled in
with `include_str!`, so **any JS edit needs a rebuild**. Syntax-check JS first:
`node -e "new Function(require('fs').readFileSync('dom_bootstrap.js','utf8'))"`.

## Always run under guard.py (`tools/guard.py`)

`guard.py` kills the process tree when RSS goes over `BO_MEM_MB` or wall time goes over `BO_TIMEOUT`, and prints `guard: peak N MB`.
It forwards the child's stdout and stderr line by line as they come, so you can pipe it into `grep --line-buffered … > log`
for long runs ([caveat](caveats.md#guard-buffering)). It counts child processes too, so the `live` verb's headless Chrome counts toward the limit (use `BO_MEM_MB=5000`
for that).

```sh
BO_MEM_MB=800 BO_TIMEOUT=300 venv/bin/python $OXIDE/tools/guard.py venv/bin/python $OXIDE/tools/drive.py STEP STEP ...
```

## drive.py (quick experiments)

Each argv item is one step. The first word is the verb.

| Verb | Meaning |
|---|---|
| `nav URL` | Navigate (new document; cookies are kept in the process-wide jar). With `BO_INIT=file.js` the file runs as an init script before page JS |
| `pump MS` | Run the page's event loop (timers, network, frames) for up to MS. **Nothing happens between steps unless you pump** |
| `js CODE` / `js @file.js` | Evaluate and print the result (strings print raw; return `JSON.stringify(...)` for objects) |
| `click SELECTOR` | Trusted pointer path + mousedown/up/click on the element ([details](input-and-scrolling.md#click)) |
| `type TEXT` / `type @stdin:N` | Trusted keystrokes into the focused element. `@stdin:N` = line N of stdin (for secrets) |
| `press KEY` | One trusted key (Enter, Escape, Tab, ArrowDown, …) |
| `wheel PX` | Trusted wheel scroll in uneven ticks (negative = up) ([details](input-and-scrolling.md#wheel)) |
| `follow` | Load a navigation the page started (link, form submit, `location=`) |
| `navjs CODE` | Navigate to the URL the expression returns |
| `console` / `errors` | Page console lines / uncaught errors (console capture is empty on FB, see [caveats](caveats.md#console-empty)) |
| `frames` / `fjs N CODE` | Count child frames / evaluate inside frame N |
| `cookies URL` | Names of cookies sent to URL (values never printed) |
| `savecookies PATH` | Write the jar (it contains a session; chmod 600) |
| `snap` | URL, title, first 600 chars of visible text |
| `live SECS [EVERY]` | Render the page to `/tmp/oxlive/live.png` every EVERY s; open `/tmp/oxlive/view.html` to watch ([debugging](debugging.md#live-view)) |
| `wait S` | Sleep without pumping |

Example: logged-in group feed, one scroll, list the feed's children.
```sh
cp profiles/fbtest_jar.json /tmp/fbjar_run.json && chmod 600 /tmp/fbjar_run.json
BROWSER_OXIDE_COOKIE_JAR=/tmp/fbjar_run.json BO_MEM_MB=800 BO_TIMEOUT=300 \
  venv/bin/python $OXIDE/tools/guard.py venv/bin/python $OXIDE/tools/drive.py \
  "nav https://www.facebook.com/groups/240748264581079/" "pump 6000" "wheel 1500" "pump 3000" \
  "js JSON.stringify([...document.querySelector('[role=feed]').children].map(d => (d.textContent||'').length))"
```
⚠️ Read only **stdout** lines that start with `## `. Oxide writes debug output to stderr, and before our fix some of
it included cookies ([security](security.md#logs)). Use `2>/dev/null`, or grep stderr only for specific markers.

## Python API (for real scrapers)

```python
from browser_oxide import Browser, Profile

HOOK = "globalThis[Symbol.for('__browser_oxide_script_hook__')]"   # private hook, invisible to pages

with Browser(profile=Profile.chrome()) as b:
    b.navigate(url, max_iterations=10, init_scripts=[...])  # init_scripts: JS source strings, optional
    b.pump(6000)                                            # let the SPA boot
    b.evaluate(f"{HOOK}.input.wheel(1300)")                 # queue trusted input
    while b.evaluate(f"{HOOK}.input.busy()") == "true":     # drain the input queue
        b.pump(40)
    b.evaluate(f"{HOOK}.input.takeLog()")                   # what the input layer did
    data = b.evaluate("JSON.stringify(...)")                # evaluate() returns a string
    os._exit(0)   # optional: skip interpreter teardown (see caveats#teardown)
```

- `evaluate` always returns a **string**. Return `JSON.stringify(...)` and parse it in Python.
- Input calls (`click`, `wheel`, `press`, `type`) only **queue** events. They run while you `pump`. Always drain the queue
  with `busy()` before reading results. See [input-and-scrolling](input-and-scrolling.md).
- `print(..., flush=True)` if you end with `os._exit` (it skips the flush).
- A second `Browser()` in the same process is a second "tab": its own engine thread and V8 isolate, the same
  process-wide cookie jar. Closing one mid-session works since our fix ([engine-changes](engine-changes.md#teardown)).

## Minimal scraper loop

`research/fb_deep_session.py` is the reference. Its shape:
1. `navigate` + `pump(4–6 s)`.
2. Check for a block page and stop if there is one ([facebook](facebook.md#checkpoints)).
3. Find unhandled post slots near the viewport, then for each one: bring it to the top with `wheel`, click "See more",
   read (pump 1.5–4 s), open the comments dialog, read, press Escape, then restore scroll if FB reset it
   ([caveat](caveats.md#post-dialog-scroll-reset)).
4. `wheel` 900–1600 px, then `pump` 2.5–4.5 s.
5. If RSS is over budget, reload the page ([memory](memory.md#long-sessions)).
