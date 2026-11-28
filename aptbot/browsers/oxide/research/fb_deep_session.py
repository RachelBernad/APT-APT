"""Long FB group session in oxide: scroll, open every post deeply, sample RAM.

    BROWSER_OXIDE_COOKIE_JAR=... BO_MEM_MB=... BO_TIMEOUT=... \\
        venv/bin/python guard.py venv/bin/python research/fb_deep_session.py GROUP_URL TARGET_POSTS OUT.csv

Per post slot that comes into view: wheel-scroll it near the top, click "See more" if present, read 1.5-4 s,
open the "N comments" dialog if present, read 2-4 s, Escape. Then wheel 900-1600 px. Stops at TARGET unique
post ids (from graphql), 10 scrolls without new posts, or any checkpoint/block page.
Prints progress; writes OUT.csv (t, rss_mb, posts, opened). See kb/ for the techniques.
"""
import ctypes, json, os, random, subprocess, sys, threading, time

from browser_oxide import Browser, Profile

HOOK = "globalThis[Symbol.for('__browser_oxide_script_hook__')].input"
CAP = "globalThis[Symbol.for('__research_capture__')]"
# Post slots: feed children holding a "Comment on …'s post" button (oxide's DOM has no aria-labelledby on
# slots, unlike Chrome's). Slots are virtualized: content is emptied once they scroll far away.
COMMENT_SEL = '[aria-label^="Comment on "]'
SLOTS = "[...document.querySelectorAll('[role=feed] > div')].filter(d => d.querySelector(" + json.dumps(COMMENT_SEL) + "))"
# Slot text starts with identical header text on every post, so key on the comment button's label (names the
# author) plus a slice from the body.
# Stable per-post key that survives reloads (relative times and reaction counts change between loads,
# so raw textContent does not) and "See more" expansion (only a short message prefix is used).
# FB marks post parts with data-ad-rendering-role. Not used: the listing 'description' (holds "listed 3h ago"/location,
# differs per load) and author links (the first /user/ link may be a commenter). Checked by tools/keydiff.py.
KEYOF = ("(d => { const t = s => { const e = d.querySelector(s); return e ? (e.textContent || '').replace(/\\s+/g, ' ').trim() : ''; };"
         " const k = [t('[data-ad-rendering-role=profile_name]'), t('[data-ad-rendering-role=story_message]').slice(0, 60),"
         " t('[data-ad-rendering-role=title]').slice(0, 40)].join('|');"
         " if (k.replace(/\\|/g, '')) return k; const c = d.querySelector(" + json.dumps(COMMENT_SEL) + "); return (c ? c.getAttribute('aria-label') : '') + '|' + (d.textContent || '').slice(150, 330); })")


_libc = ctypes.CDLL(None)


def release_malloc():
    """Ask the system allocator to hand free pages back to the OS (diagnostic, BO_MALLOC_RELIEF=1)."""
    if sys.platform == "darwin":
        _libc.malloc_zone_pressure_relief.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
        _libc.malloc_zone_pressure_relief(None, 0)
    else:
        _libc.malloc_trim(0)


def rss_mb():
    out = subprocess.run(["ps", "-o", "rss=", "-p", str(os.getpid())], capture_output=True, text=True).stdout
    return int(out.strip() or 0) // 1024


class Sampler(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True); self.rows = []; self.peak = 0; self.posts = 0; self.opened = 0; self.t0 = time.time()
    def run(self):
        while True:
            r = rss_mb(); self.peak = max(self.peak, r)
            self.rows.append((round(time.time() - self.t0, 1), r, self.posts, self.opened)); time.sleep(0.5)


def act(b, call, timeout=40):
    """Run an input action and pump until the driver queue drains."""
    first = b.evaluate(f"{HOOK}.{call}")
    t0 = time.time()
    while time.time() - t0 < timeout:
        b.pump(40)
        if b.evaluate(f"{HOOK}.busy()") == "false":
            break
    b.evaluate(f"{HOOK}.takeLog()")
    return first


def slot_expr(key):
    return f"{SLOTS}.find(d => {KEYOF}(d) === {json.dumps(key)})"


def blocked(b):
    return b.evaluate("(/checkpoint|login/.test(location.pathname) || /temporarily blocked|confirm your identity|we suspended/i.test(document.body ? document.body.textContent.slice(0, 20000) : '')) ? 'yes' : 'no'") == "yes"


def main(url, target, out_csv):
    s = Sampler(); s.start()
    init = [open(os.path.join(os.path.dirname(__file__), "fb_capture.js")).read()]
    with Browser(profile=Profile.chrome()) as b:
        b.navigate(url, max_iterations=10, init_scripts=init)
        b.pump(6000)
        opened, stalls, last_n = set(), 0, 0
        seen_pids = {}           # real post ids from the dialog's permalink URL: checks the dedup key
        all_ids = set()          # page-side capture resets on every (re)load
        recycle_every = int(os.environ.get("RECYCLE_EVERY", "0"))
        recycle_rss = int(os.environ.get("RECYCLE_RSS", "0"))   # reload when RSS (MB) after a post exceeds this
        want_recycle = False
        last_sy = 0.0
        opened_at_load = 0
        t_start = time.time()
        while True:
            if blocked(b):
                print("STOP: checkpoint/block page", flush=True); break
            if want_recycle or (recycle_every and len(opened) - opened_at_load >= recycle_every):
                want_recycle = False
                # FB's SPA keeps ~6 MB (Chrome) / ~16 MB (oxide) per closed post dialog. Reload the group like a
                # user refreshing a sluggish tab; already-opened posts are skipped by key. See kb/memory.md.
                before = rss_mb()
                b.navigate(url, max_iterations=10, init_scripts=init)
                b.pump(random.randint(4000, 6000))
                opened_at_load = len(opened); stalls = 0; last_sy = 0.0
                print(f"   RECYCLE: reloaded group, rss {before} -> {rss_mb()} MB", flush=True)
            all_ids.update(json.loads(b.evaluate(f"JSON.stringify([...{CAP}.ids])")))
            s.posts = n = len(all_ids)
            # TARGET_MODE=opened: stop after `target` opened posts instead of `target` graphql post ids.
            if (len(opened) if os.environ.get("TARGET_MODE") == "opened" else n) >= target:
                break
            sy = float(b.evaluate("scrollY"))
            progressed = n > last_n or sy > last_sy + 500
            stalls = 0 if progressed else stalls + 1
            last_n, last_sy = n, max(last_sy, sy)
            if stalls >= 12:
                print("STOP: feed not growing (no new posts, no scroll progress)", flush=True); break
            # Posts with content that are on or near the screen.
            keys = json.loads(b.evaluate(f"JSON.stringify({SLOTS}.filter(d => (d.textContent || '').length > 80).filter(d => {{ const r = d.getBoundingClientRect(); return r.bottom > -200 && r.top < innerHeight + 400; }}).map({KEYOF}))"))
            if os.environ.get("BO_DEBUG_SLOTS") == "1":
                print("   slots:", b.evaluate(f"JSON.stringify({SLOTS}.map(d => [Math.round(d.getBoundingClientRect().top), Math.round(d.getBoundingClientRect().height), (d.textContent||'').length]))"), "new:", sum(k not in opened for k in keys), flush=True)
            fresh = [k for k in keys if k not in opened]
            for key in keys:
                if key in opened:
                    continue
                opened.add(key); s.opened = len(opened)
                top = float(b.evaluate(f"(() => {{ const d = {slot_expr(key)}; return d ? d.getBoundingClientRect().top : 0; }})()") or 0)
                if abs(top - 120) > 60:
                    act(b, f"wheel({round(top - random.uniform(80, 180))})")
                b.pump(random.randint(300, 800))
                more = b.evaluate(f"(() => {{ const d = {slot_expr(key)}; const m = d && [...d.querySelectorAll('[role=button]')].find(x => /^(See more|ראה עוד)$/.test((x.textContent || '').trim())); return m ? {HOOK}.click(m) : 'none'; }})()")
                if more != "none":
                    act(b, "busy()")  # drain the queued click
                b.pump(random.randint(1500, 4000))
                com = b.evaluate(f"(() => {{ const d = {slot_expr(key)}; const c = d && d.querySelector({json.dumps(COMMENT_SEL)}); return c ? {HOOK}.click(c) : 'none'; }})()")
                if com != "none":
                    y_before = float(b.evaluate("scrollY"))
                    rss0 = rss_mb()
                    bytes0 = json.loads(b.evaluate(f"JSON.stringify({CAP}.bytes)"))
                    act(b, "busy()")
                    b.pump(random.randint(2000, 4000))
                    dlg = b.evaluate("document.querySelectorAll('[role=dialog]').length")
                    pid = b.evaluate("(/\\/(?:permalink|posts)\\/(\\d+)/.exec(location.pathname) || [])[1] || ''")
                    if pid:
                        seen_pids[pid] = seen_pids.get(pid, 0) + 1
                    if os.environ.get("BO_POST_STATS") == "1":
                        # What does an open dialog cost? DOM size, media, network bytes by query, RSS delta.
                        st = json.loads(b.evaluate("JSON.stringify((() => { const d = [...document.querySelectorAll('[role=dialog]')].pop(); if (!d) return {}; return { els: d.querySelectorAll('*').length, imgs: d.querySelectorAll('img').length, vids: d.querySelectorAll('video').length, svgs: d.querySelectorAll('svg').length, txt: (d.textContent||'').length, doc: document.querySelectorAll('*').length }; })())"))
                        bytes1 = json.loads(b.evaluate(f"JSON.stringify({CAP}.bytes)"))
                        nb = {k: (v - bytes0.get(k, 0)) // 1024 for k, v in bytes1.items() if v - bytes0.get(k, 0) > 20480}
                        print(f"   open: rss {rss0}->{rss_mb()} MB dialog={st} new_KB={nb}", flush=True)
                    act(b, "press('Escape')")
                    b.pump(random.randint(600, 1200))
                    still = b.evaluate("document.querySelectorAll('[role=dialog]').length")
                    # oxide caveat (kb/caveats.md#post-dialog-scroll-reset): FB leaves the feed at scrollY 0 after the
                    # dialog closes; Chrome restores it programmatically, so do the same (fires normal scroll events).
                    if float(b.evaluate("scrollY")) < y_before - 400:
                        b.evaluate(f"{HOOK.rsplit('.', 1)[0]}.viewportScroll(0, {y_before})")
                        b.pump(random.randint(300, 600))
                    if still != "0":
                        # Escape was swallowed (focus inside a nested layer): use the dialog's own Close button.
                        cl = b.evaluate(f"(() => {{ const c = [...document.querySelectorAll('[role=dialog] [aria-label=Close]')].pop(); return c ? {HOOK}.click(c) : 'none'; }})()")
                        if cl != "none":
                            act(b, "busy()")
                            b.pump(random.randint(600, 1200))
                        still = still + "->" + b.evaluate("document.querySelectorAll('[role=dialog]').length")
                    heap = ""
                    snap_at = os.environ.get("BO_SNAP_AT", "")
                    if str(len(opened)) in snap_at.split(","):
                        b.evaluate(f"#bo:snap:/tmp/deep_snap_{len(opened)}.heapsnapshot")
                    if os.environ.get("BO_DEBUG_HEAP") == "1":
                        heap = f" heap={int(b.evaluate('#bo:gc')) // 1048576}MB rss_after_gc={rss_mb()}MB"
                        st = json.loads(b.evaluate("#bo:stats"))
                        heap += " v8[" + " ".join(f"{k}={v // 1048576 if k in ('used', 'total', 'physical', 'malloced', 'peak_malloced', 'external') else v}" for k, v in st.items()) + "]"
                    if os.environ.get("BO_MALLOC_RELIEF") == "1":
                        r0 = rss_mb(); release_malloc(); time.sleep(0.3)
                        heap += f" malloc_relief {r0}->{rss_mb()}MB"
                    print(f"  post {len(opened)}: see_more={more != 'none'} comments_dialog={dlg}->{still} uniq={len(seen_pids)} dup={pid and seen_pids.get(pid, 0) > 1}{heap}", flush=True)
                else:
                    print(f"  post {len(opened)}: see_more={more != 'none'} no comments button", flush=True)
                if recycle_rss and rss_mb() > recycle_rss:
                    want_recycle = True
                if blocked(b) or want_recycle:
                    break
            if want_recycle:
                continue
            if fresh:
                act(b, f"wheel({random.randint(900, 1600)})")
                b.pump(random.randint(2500, 4500))
            else:
                # Nothing new on screen (e.g. re-scrolling after a reload): flick past it like a person would.
                act(b, f"wheel({random.randint(2000, 3400)})")
                b.pump(random.randint(1200, 2200))
            el = time.time() - t_start
            print(f"[{el:6.0f}s] posts={s.posts} opened={len(opened)} rss={rss_mb()} MB peak={s.peak} MB scrollY={b.evaluate('Math.round(scrollY)')} reaper={b.evaluate(f"JSON.stringify(({HOOK.rsplit('.', 1)[0]}.reapStats || (() => null))())")}", flush=True)
            if len(opened) and len(opened) % 50 < len(keys) and os.environ.get("BO_DEBUG_HEAP") == "1":
                print(f"   live heap {int(b.evaluate('#bo:gc')) // 1048576} MB", flush=True)
        print(f"DONE unique_dialog_posts={len(seen_pids)} reopened={sum(v - 1 for v in seen_pids.values())} posts={s.posts} opened={len(opened)} peak={s.peak} MB last={rss_mb()} MB in {time.time() - t_start:.0f}s", flush=True)
        if os.environ.get("BO_VMMAP") == "1":
            # Where is the non-V8 memory? Region summary of this process (macOS).
            out = subprocess.run(["vmmap", "-summary", str(os.getpid())], capture_output=True, text=True).stdout
            keep = False
            for line in out.splitlines():
                if line.startswith("REGION TYPE") or line.startswith("MALLOC ZONE"):
                    keep = True
                if keep and line.strip():
                    print("   vmmap:", line[:150], flush=True)
        with open(out_csv, "w") as f:
            f.write("t,rss_mb,posts,opened\n" + "\n".join(",".join(map(str, r)) for r in s.rows))
        os._exit(0)  # skip the V8 teardown abort


if __name__ == "__main__":
    main(sys.argv[1], int(sys.argv[2]), sys.argv[3])
