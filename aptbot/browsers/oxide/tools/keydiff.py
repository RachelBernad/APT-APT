"""Do fb_deep_session's post keys survive a reload? Load the group, scroll, collect keys; reload; repeat; diff.
Prints only key *shapes* (field lengths + a short hash), never post text."""
import hashlib, json, os, random, sys, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "research"))
import fb_deep_session as m
from browser_oxide import Browser, Profile

FIELDS = ("(d => { const t = s => { const e = d.querySelector(s); return e ? (e.textContent || '').replace(/\\s+/g, ' ').trim() : ''; };"
          " return { name: t('[data-ad-rendering-role=profile_name]'), msg: t('[data-ad-rendering-role=story_message]'),"
          " title: t('[data-ad-rendering-role=title]'), desc: t('[data-ad-rendering-role=description]'),"
          " n_name: d.querySelectorAll('[data-ad-rendering-role=profile_name]').length, n_msg: d.querySelectorAll('[data-ad-rendering-role=story_message]').length }; })")
h = lambda s: hashlib.sha1(s.encode()).hexdigest()[:6]

def collect(b):
    out = []
    for _ in range(4):
        rows = json.loads(b.evaluate(f"JSON.stringify({m.SLOTS}.filter(d => (d.textContent||'').length > 80).map(d => [{m.KEYOF}(d), {FIELDS}(d)]))"))
        for k, f in rows:
            if k not in [x[0] for x in out]:
                out.append((k, f))
        m.act(b, f"wheel({random.randint(1200, 1600)})"); b.pump(3000)
    return out

url = sys.argv[1]
with Browser(profile=Profile.chrome()) as b:
    b.navigate(url, max_iterations=10); b.pump(6000)
    a = collect(b)
    b.navigate(url, max_iterations=10); b.pump(6000)
    c = collect(b)
    ka = {k for k, _ in a}
    for tag, rows in (("A", a), ("B", c)):
        for k, f in rows:
            print(f"{tag} key={h(k)} in_A={k in ka} name={len(f['name'])}/{h(f['name'])} msg={len(f['msg'])}/{h(f['msg'][:60])} "
                  f"title={len(f['title'])} desc={len(f['desc'])} n_name={f['n_name']} n_msg={f['n_msg']}", flush=True)
    os._exit(0)
