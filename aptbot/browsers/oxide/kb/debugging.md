# Debugging techniques

[← index](README.md) · related: [env-vars](env-vars.md), [memory](memory.md), [caveats](caveats.md)

Tools live in `tools/`. Each technique below has paid off at least once.

## Init scripts

`BO_INIT=file.js` (drive.py) or `init_scripts=[src]` (Python) run before page JS. Use them to wrap APIs and record
calls into a global, then read it back with `js JSON.stringify(globalThis.__x)`. **Page-visible: debugging only.**

| Script | Records |
|---|---|
| `contrace.js` | `console.error/warn` → `__con` (oxide's own console capture is empty on FB) |
| `gqltrace.js` | graphql `fb_api_req_friendly_name` counts → `__gql` |
| `idbtrace.js` | IndexedDB calls + handler exceptions (oxide's IDB swallows exceptions thrown in handlers) |
| `iotrace.js` | IntersectionObserver observe/callback activity |
| `scrolltrace.js` | `scrollTo/scrollBy/scrollTop=` and `history.pushState/go` + popstate, with stack frames |
| `cdp2.js` | `compareDocumentPosition` calls as tree paths (`1.158.0.0.0.3`) + the result |
| `dlgresp.js` | XHR lifecycle (readyState/progress/load, sizes) for chosen graphql queries |
| `restrace.js` | `<script>`/`<link>` inserted after load, and whether they fired load/error |
| `rdt.js` | Minimal `__REACT_DEVTOOLS_GLOBAL_HOOK__`: collects fiber roots; `__walkHidden(el)` lists ancestor components and their hidden/backgrounded props. FB's React keeps fibers in an internal map, not in `__reactFiber$` keys, so this is the only way in. Very detectable |

Stack traces show FB module positions like `FB:L0v8bS-vU588:337:2750`. To read that code, fetch the script from inside the page
and slice the line:
```
js globalThis.__src='pending'; const s=[...document.scripts].map(x=>x.src).find(u=>u.includes('L0v8bS-vU588')); fetch(s).then(r=>r.text()).then(t=>{__src=t.split('\n')[336].slice(0,4000)}); 1
pump 3000
js __src
```
Each module starts with `__d("ModuleName", …)`, so you get the name too (e.g. `BaseDocumentScrollView.react`).

## Compare with real Chrome

The account's real Chrome session (Playwright MCP, the same profile the user logged in with) is the ground truth. Run the same
probe in both (`browser_run_code_unsafe` with `page.evaluate`) and diff. Don't run both on the account at the same time.
Examples that settled questions: `#scrollview` contents while a dialog is open; the size of `document.__reactHandles$*`;
per-dialog heap with CDP `HeapProfiler.collectGarbage` + `Runtime.getHeapUsage`.

## Heap

With `BO_DEBUG_HEAP=1`:
- `b.evaluate("#bo:gc")` runs a full GC and returns live heap bytes. Use it to tell a real leak from GC lag.
- `b.evaluate("#bo:snap:/tmp/x.heapsnapshot")` writes a snapshot (~170–280 MB file, about +900 MB RSS spike).

Analyzers (python3, standard library only; they read whole snapshots, which takes ~1–3 min each):

| Tool | Question it answers |
|---|---|
| `heapdiff.py A B` | Which constructors grew between two snapshots (count, self size) |
| `heapnew.py A B` | Objects new in B (node ids are stable within one process), grouped by retainer chain |
| `heaproot.py A B` | **Shortest path from the GC roots** to a sample of the new objects. This is the one that finds leaks (it found `_activeEl` and `_nodeListeners`) |
| `heapdom.py S` | Who retains DOM wrapper objects |
| `heapclo.py S NAME` | Who retains closures named NAME |
| `heaptypes.py S…` | Counts of DOM-ish objects (wrappers, CSSStyleDeclaration, …) |

Synthetic leak self-test: `tools/leak_selftest.py`. It creates/removes 20k nodes with listeners in
**separate jobs**, then prints heap deltas. Note: within one job, WeakRef targets are kept alive until the job ends
(spec "KeepDuringJob"), so single-`evaluate` tests show fake leaks.

## Live view

`drive.py ... "live 60 3"` renders the DOM every 3 s. It clones the DOM, inlines CSSOM rules, strips scripts, adds a CSP and a `<base>`,
then has headless Chrome take a screenshot to `/tmp/oxlive/live.png`. `view.html` auto-reloads. Don't use `--virtual-time-budget`
(Chrome then writes no image). The output holds private page content: keep it local.

## Unit-style probes on a neutral page

Navigate to `https://example.com/`, then build the DOM with `innerHTML` and test one API
(`cdp_selftest.py` = compareDocumentPosition vs Chrome's values). `data:` URLs don't work (`no host in URL`).
