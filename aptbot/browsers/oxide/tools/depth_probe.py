"""RSS vs feed depth, scrolling only (no dialogs). One line per 5 wheel gestures."""
import json, os, random, sys, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "research"))
import fb_deep_session as m
from browser_oxide import Browser, Profile
url, gestures = sys.argv[1], int(sys.argv[2])
s = m.Sampler(); s.start()
init = [open(os.path.join(os.path.dirname(__file__), "..", "research", "fb_capture.js")).read()]
with Browser(profile=Profile.chrome()) as b:
    b.navigate(url, max_iterations=10, init_scripts=init); b.pump(6000)
    t0 = time.time()
    for g in range(1, gestures + 1):
        m.act(b, f"wheel({random.randint(1200, 1700)})"); b.pump(random.randint(2000, 3200))
        if g % 5 == 0:
            ids = len(json.loads(b.evaluate(f"JSON.stringify([...{m.CAP}.ids])")))
            print(f"g={g:3d} ids={ids:3d} scrollY={b.evaluate('Math.round(scrollY)'):>6} rss={m.rss_mb()} peak={s.peak} t={time.time() - t0:.0f}s", flush=True)
            if m.blocked(b): print("STOP: block page", flush=True); break
    os._exit(0)
