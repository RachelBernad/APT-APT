"""DOM reaper self-test on example.com: memory freed + removed-but-held nodes keep working."""
import json, os, resource, subprocess, sys
from browser_oxide import Browser, Profile
H = "globalThis[Symbol.for('__browser_oxide_script_hook__')]"
rss = lambda: int(subprocess.run(["ps", "-o", "rss=", "-p", str(os.getpid())], capture_output=True, text=True).stdout) // 1024
with Browser(profile=Profile.chrome()) as b:
    b.navigate("https://example.com/", max_iterations=5); b.pump(1500)
    st = lambda: json.loads(b.evaluate("#bo:stats"))
    pick = lambda d: {k: d[k] for k in ("dom_slots", "dom_live", "dom_connected", "dom_orphan_kb")}
    b.evaluate("""globalThis.__mk = (n) => { const root = document.createElement('div');
        for (let i = 0; i < n; i++) { const d = document.createElement('div'); d.className = 'x'.repeat(200) + i;
          d.setAttribute('data-k', 'v'.repeat(300)); d.appendChild(document.createTextNode('t'.repeat(200)));
          d.addEventListener('click', () => { globalThis.__hits = (globalThis.__hits || 0) + 1; }); root.appendChild(d); }
        return root; }; 1""")
    print("start", rss(), "MB", pick(st()), flush=True)
    # 1. churn: 40 rounds x 500 nodes appended to the document then removed, nothing kept
    for r in range(40):
        b.evaluate("(() => { const r = __mk(500); document.body.appendChild(r); r.remove(); })()")
        b.pump(30)
    # 2. innerHTML replacement churn
    for r in range(20):
        b.evaluate("document.body.lastElementChild.innerHTML = '<p>' + 'y'.repeat(400) + '</p>'.repeat(1) + Array.from({length: 400}, (_, i) => '<span class=\"' + 'z'.repeat(100) + i + '\">' + i + '</span>').join('')")
        b.pump(30)
    # 3. kept: a removed subtree held from JS, plus a deep child held alone
    b.evaluate("(() => { globalThis.__keep = __mk(50); document.body.appendChild(__keep); __keep.remove(); const t = __mk(50); document.body.appendChild(t); globalThis.__deep = t.children[7]; t.remove(); })()")
    print("after churn (before gc)", rss(), "MB", pick(st()), "reaper", b.evaluate(f"JSON.stringify({H}.reapStats())"), flush=True)
    for _ in range(3):
        b.evaluate("#bo:gc"); b.pump(200)
        b.evaluate(f"{H}.sweepListeners && {H}.sweepListeners(), 1"); b.pump(50)
        b.evaluate(f"JSON.stringify({H}.reap())"); b.pump(50)
    print("after gc+reap", rss(), "MB", pick(st()), "reaper", b.evaluate(f"JSON.stringify({H}.reapStats())"), flush=True)
    # correctness
    ok = b.evaluate("""JSON.stringify((() => {
        const r = {};
        r.keep_children = __keep.children.length;
        r.keep_attr = __keep.children[3].getAttribute('data-k').length;
        r.keep_text = __keep.children[3].textContent.length;
        document.body.appendChild(__keep); globalThis.__hits = 0;
        __keep.children[5].dispatchEvent(new MouseEvent('click', {bubbles: true}));
        r.keep_listener = __hits;
        r.keep_connected = __keep.isConnected;
        r.deep_parent_ok = !!__deep.parentNode && __deep.parentNode.children.length === 50;
        r.deep_sibling = __deep.nextElementSibling ? __deep.nextElementSibling.className.slice(-1) : null;
        r.body_ok = document.body.querySelectorAll('div').length;
        return r; })())""")
    print("correctness", ok, flush=True)
    os._exit(0)
