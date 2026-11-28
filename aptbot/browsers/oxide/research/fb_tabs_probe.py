"""Probe: one feed Browser + extra Browser "tabs" in the same process. RSS per tab, and whether closing a tab
(dropping its Browser) is survivable and frees memory.  Usage: guard.py venv/bin/python research/fb_tabs_probe.py GROUP_URL"""
import json, os, subprocess, sys, time
from browser_oxide import Browser, Profile

CAP = "globalThis[Symbol.for('__research_capture__')]"
def rss():
    return int(subprocess.run(["ps", "-o", "rss=", "-p", str(os.getpid())], capture_output=True, text=True).stdout.strip()) // 1024
def log(*a): print(f"[{rss():4d} MB]", *a, flush=True)

url = sys.argv[1]; gid = url.rstrip("/").split("/")[-1]
init = [open(os.path.join(os.path.dirname(__file__), "fb_capture.js")).read()]
log("start")
feed = Browser(profile=Profile.chrome()); feed.__enter__()
feed.navigate(url, max_iterations=10, init_scripts=init); feed.pump(6000)
H = "globalThis[Symbol.for('__browser_oxide_script_hook__')].input"
for _ in range(3):
    feed.evaluate(f"{H}.wheel(1300)")
    for _ in range(60):
        feed.pump(50)
        if feed.evaluate(f"{H}.busy()") == "false": break
    feed.pump(3000)
ids = json.loads(feed.evaluate(f"JSON.stringify([...{CAP}.ids])"))
log("feed tab ready, post ids:", len(ids))
mode = sys.argv[2] if len(sys.argv) > 2 else "seq"
tabs = []
for n, pid in enumerate(ids[:3]):
    t = Browser(profile=Profile.chrome()); t.__enter__()
    t.navigate(f"https://www.facebook.com/groups/{gid}/posts/{pid}/", max_iterations=10)
    t.pump(5000)
    info = t.evaluate("JSON.stringify({path: location.pathname.replace(/\\d{6,}/g,'N'), dialogs: document.querySelectorAll('[role=dialog]').length, articles: document.querySelectorAll('[role=article]').length, text: (document.body.innerText||document.body.textContent||'').length, comments: document.querySelectorAll('[aria-label^=\"Comment by\"]').length})")
    log(f"tab {n+1} open:", info)
    if mode == "seq":
        t.__exit__(None, None, None); del t
        feed.pump(500)
        log(f"tab {n+1} closed; feed alive:", feed.evaluate("document.querySelectorAll('[role=feed] > div').length"))
    else:
        tabs.append(t)
if mode != "seq":
    log("all tabs open together")
    for i, t in enumerate(tabs):
        t.__exit__(None, None, None)
    tabs.clear(); feed.pump(500)
    log("all extra tabs closed; feed alive:", feed.evaluate("document.querySelectorAll('[role=feed] > div').length"))
os._exit(0)
