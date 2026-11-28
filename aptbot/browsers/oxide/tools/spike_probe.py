"""Which single action makes the RSS spike? Uses ru_maxrss (monotonic process peak) around each step."""
import json, os, random, resource, subprocess, sys, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "research"))
import fb_deep_session as m
from browser_oxide import Browser, Profile
peak = lambda: resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1048576   # macOS: bytes
rss = m.rss_mb
H = m.HOOK.rsplit('.', 1)[0]
def step(label, fn):
    p0, r0 = peak(), rss(); t = time.time(); fn(); dt = time.time() - t
    print(f"{label:28s} rss {r0:4d}->{rss():4d}  peak {p0:4d}->{peak():4d}  (+{peak() - p0:3d})  {dt:4.1f}s", flush=True)
url = sys.argv[1]
with Browser(profile=Profile.chrome()) as b:
    step("navigate+boot", lambda: (b.navigate(url, max_iterations=10), b.pump(6000)))
    for i in range(5):
        step(f"wheel {i}", lambda: (m.act(b, f"wheel({random.randint(1200, 1500)})"), b.pump(3000)))
        step(f"  forced layout {i}", lambda: b.evaluate("(() => { const d = document.createElement('div'); document.body.appendChild(d); const t = d.getBoundingClientRect().top; d.remove(); return t; })()"))
        keys = json.loads(b.evaluate(f"JSON.stringify({m.SLOTS}.filter(d => (d.textContent||'').length > 80).filter(d => {{ const r = d.getBoundingClientRect(); return r.bottom > 0 && r.top < innerHeight; }}).map({m.KEYOF}))"))
        if not keys: continue
        k = keys[0]
        step(f"  see more {i}", lambda: (b.evaluate(f"(() => {{ const d = {m.slot_expr(k)}; const x = d && [...d.querySelectorAll('[role=button]')].find(x => /^(See more|ראה עוד)$/.test((x.textContent||'').trim())); return x ? {m.HOOK}.click(x) : 'none'; }})()"), m.act(b, "busy()"), b.pump(2000)))
        step(f"  open dialog {i}", lambda: (b.evaluate(f"(() => {{ const d = {m.slot_expr(k)}; const c = d && d.querySelector({json.dumps(m.COMMENT_SEL)}); return c ? {m.HOOK}.click(c) : 'none'; }})()"), m.act(b, "busy()"), b.pump(3000)))
        step(f"  escape {i}", lambda: (m.act(b, "press('Escape')"), b.pump(1000)))
        if m.blocked(b): print("STOP: block page"); break
    os._exit(0)
