"""PoC driver: run a sequence of steps against one BrowserOxide session. Always run under guard.py.

Usage:
    BO_MEM_MB=400 BO_TIMEOUT=120 venv/bin/python guard.py drive.py STEP [STEP ...]

Steps (one argv item each, verb first):
    nav URL             navigate (new document, cookies kept in the process-wide jar)
    pump MS             keep the page (and its frames) running for up to MS
    js CODE|@file.js    evaluate in the page, print the result
    console             print and clear the page's console lines
    errors              print and clear uncaught page errors
    frames              number of live child frames
    fjs N CODE|@file    evaluate in child frame N
    click SELECTOR      trusted pointer + mouse sequence on the element, default actions included
    type TEXT           trusted key/input events into the focused element (TEXT may be @stdin:N for line N of stdin)
    press KEY           one trusted key (Enter, Tab, Backspace, Escape, ArrowDown, ...)
    wheel PX            trusted mouse-wheel scroll by PX (negative = up), in uneven ~100 px ticks
    navjs CODE          navigate to the URL the page expression CODE returns (path printed only)
    follow              if the page started a navigation (link, form submit, location=...), load it
    cookies URL         names of the cookies the jar sends to URL (values never printed)
    savecookies PATH    write the cookie jar to PATH (JSON, chmod it yourself; it holds a session)
    wait SECONDS        plain sleep (no pumping)
    snap                print url, title and the first 600 chars of visible text
    live SECS [EVERY]   keep the page running for SECS, every EVERY s (default 3) render it to
                        $BO_LIVE_DIR/live.png (default /tmp/oxlive); open view.html there to watch

Secrets: `type @stdin:N` reads line N (0-based) of stdin so credentials never appear in argv or logs.
"""
import json
import os
import subprocess
import sys
import tempfile
import time

from browser_oxide import Browser, Profile

_stdin_lines = None


def _arg_text(s):
    global _stdin_lines
    if s.startswith("@stdin:"):
        if _stdin_lines is None:
            _stdin_lines = sys.stdin.read().splitlines()
        return _stdin_lines[int(s.split(":", 1)[1])]
    if s.startswith("@"):
        return open(s[1:]).read()
    return s


def out(tag, val):
    s = val if isinstance(val, str) else json.dumps(val, ensure_ascii=False)
    print(f"## {tag}: " + s[:4000].replace("\n", " "), flush=True)


HOOK = "globalThis[Symbol.for('__browser_oxide_script_hook__')].input"


def drive(b, call, timeout=60):
    """Start a driver action, then keep the page running until the driver's queue is empty."""
    first = b.evaluate(f"{HOOK}.{call}")
    t0 = time.time()
    while time.time() - t0 < timeout:
        b.pump(40)
        if b.evaluate(f"{HOOK}.busy()") == "false":
            break
    log = json.loads(b.evaluate(f"JSON.stringify({HOOK}.takeLog())") or "[]")
    return f"{first}; " + "; ".join(log) + f" ({time.time() - t0:.1f}s)"


CHROME = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"

# Serialize the live DOM (scripts removed and blocked by CSP, CSSOM-only <style> rules inlined, form
# values copied into attributes) so a JS-disabled Chrome can draw it.
DUMP_JS = r"""(() => {
  const root = document.documentElement.cloneNode(true);
  const live = [...document.querySelectorAll('style')];
  const copy = [...root.querySelectorAll('style')];
  live.forEach((st, i) => {
    try {
      if (!st.textContent && st.sheet && st.sheet.cssRules && copy[i])
        copy[i].textContent = [...st.sheet.cssRules].map(r => r.cssText).join('\n');
    } catch (_) {}
  });
  root.querySelectorAll('script, noscript, iframe, link[rel=preload], link[rel=modulepreload]').forEach(e => e.remove());
  const head = root.querySelector('head') || root;
  head.insertAdjacentHTML('afterbegin',
    '<meta charset="utf-8"><meta http-equiv="Content-Security-Policy" content="script-src \'none\'; connect-src \'none\'">' +
    '<base href="' + location.href.replace(/"/g, '&quot;') + '">');
  return '<!DOCTYPE html>' + root.outerHTML;
})()"""

VIEW_HTML = """<!doctype html><meta charset=utf-8><title>oxide live</title>
<style>body{margin:0;background:#222;color:#ccc;font:12px sans-serif}img{display:block;max-width:100%}</style>
<div id=s>waiting…</div><img id=i>
<script>
setInterval(() => { const i = document.getElementById('i'); const n = new Image();
  n.onload = () => { i.src = n.src; document.getElementById('s').textContent = 'oxide live · ' + new Date().toLocaleTimeString(); };
  n.src = 'live.png?' + Date.now(); }, 1500);
</script>"""


def live(b, secs, every=3.0):
    d = os.environ.get("BO_LIVE_DIR", "/tmp/oxlive")
    os.makedirs(d, mode=0o700, exist_ok=True)
    with open(os.path.join(d, "view.html"), "w") as f:
        f.write(VIEW_HTML)
    prof = tempfile.mkdtemp(prefix="oxlive-chrome-")
    html = os.path.join(d, "page.html")
    frames, t0, nxt = 0, time.time(), 0.0
    while time.time() - t0 < secs:
        if time.time() >= nxt:
            nxt = time.time() + every
            fd = os.open(html, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(b.evaluate(DUMP_JS))
            tmp = os.path.join(d, "next.png")
            subprocess.run([CHROME, "--headless", f"--user-data-dir={prof}", "--no-first-run",
                            "--hide-scrollbars",
                            "--window-size=1280,1600",
                            f"--screenshot={tmp}", "file://" + html],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=60)
            if os.path.exists(tmp):
                os.replace(tmp, os.path.join(d, "live.png"))
                frames += 1
        b.pump(200)
    return f"{frames} frames -> {d}/live.png (watch {d}/view.html)"


def main(steps):
    import os
    prof = Profile.from_file(os.environ["BO_PROFILE"]) if os.environ.get("BO_PROFILE") else Profile.chrome()
    with Browser(profile=prof) as b:
        pg = None
        for raw in steps:
            verb, _, rest = raw.partition(" ")
            t0 = time.time()
            try:
                if verb == "nav":
                    init = [open(os.environ["BO_INIT"]).read()] if os.environ.get("BO_INIT") else None
                    pg = b.navigate(rest, max_iterations=10, init_scripts=init)
                    out("nav", f"{pg.url} | {pg.title!r} | {pg.verdict} | {time.time() - t0:.1f}s")
                elif verb == "navjs":
                    # Navigate to the URL a page expression returns (the URL never
                    # goes through argv); only the path is printed.
                    target = b.evaluate(_arg_text(rest))
                    if not target or not str(target).startswith("http"):
                        out("navjs", f"no url ({target!r})")
                    else:
                        init = [open(os.environ["BO_INIT"]).read()] if os.environ.get("BO_INIT") else None
                        pg = b.navigate(str(target), max_iterations=10, init_scripts=init)
                        from urllib.parse import urlsplit
                        out("navjs", f"{urlsplit(pg.url).path} | {pg.title!r} | {pg.verdict} | {time.time() - t0:.1f}s")
                elif verb == "pump":
                    out("pump", b.pump(int(rest or 1000)))
                elif verb == "js":
                    out("js", b.evaluate(_arg_text(rest)))
                elif verb == "console":
                    for line in b.console():
                        print("## console: " + str(line)[:400].replace("\n", " | "))
                elif verb == "errors":
                    for line in b.page_errors():
                        print("## error: " + str(line)[:400].replace("\n", " | "))
                elif verb == "frames":
                    out("frames", str(b.frame_count()))
                elif verb == "fjs":
                    idx, _, code = rest.partition(" ")
                    out(f"fjs {idx}", b.frame_evaluate(int(idx), _arg_text(code)))
                elif verb == "click":
                    out("click", drive(b, f"click({json.dumps(rest)})"))
                elif verb == "type":
                    text = _arg_text(rest)
                    res = drive(b, f"type({json.dumps(text)})")
                    out("type", res if not rest.startswith("@stdin") else f"{len(text)} chars (hidden) -> {res}")
                elif verb == "press":
                    out("press", drive(b, f"press({json.dumps(rest)})"))
                elif verb == "wheel":
                    out("wheel", drive(b, f"wheel({float(rest)})"))
                elif verb == "follow":
                    snap = b.follow()
                    out("follow", "no navigation pending" if snap is None else
                        f"{snap.url} | {snap.title!r} | {snap.verdict} | {time.time() - t0:.1f}s")
                elif verb == "cookies":
                    jar = b.cookies(rest) or ""
                    out("cookies", sorted({c.split("=", 1)[0].strip() for c in jar.split(";") if c.strip()}))
                elif verb == "savecookies":
                    b.save_cookies(rest)
                    out("savecookies", rest)
                elif verb == "wait":
                    time.sleep(float(rest))
                elif verb == "live":
                    a = rest.split()
                    out("live", live(b, float(a[0]) if a else 30, float(a[1]) if len(a) > 1 else 3.0))
                elif verb == "snap":
                    out("snap", b.evaluate("JSON.stringify({u: location.href, t: document.title, "
                                           "x: (document.body && document.body.innerText || '')"
                                           ".replace(/\\s+/g, ' ').slice(0, 600)})"))
                else:
                    out("bad step", raw)
            except Exception as e:  # keep going; the engine reports most failures as exceptions
                out(f"{verb} FAILED", repr(e)[:600])


if __name__ == "__main__":
    main(sys.argv[1:])
