from browser_oxide import Browser, Profile
import os
with Browser(profile=Profile.chrome()) as b:
    b.navigate("https://example.com/", max_iterations=3); b.evaluate("document.body.innerHTML = \"<div id=a><p id=a1></p><p id=a2></p></div><div id=b></div>\"")
    js = """(() => { const g = id => document.getElementById(id); const a=g('a'),a1=g('a1'),a2=g('a2'),bb=g('b');
      const d=document.createElement('div'); document.body.appendChild(d); const det=document.createElement('i');
      return JSON.stringify({a_b: a.compareDocumentPosition(bb), b_a: bb.compareDocumentPosition(a), a_a1: a.compareDocumentPosition(a1), a1_a: a1.compareDocumentPosition(a),
        a1_a2: a1.compareDocumentPosition(a2), a2_b: a2.compareDocumentPosition(bb), self: a.compareDocumentPosition(a), new_last: a.compareDocumentPosition(d), detached: a.compareDocumentPosition(det) & 1, contains: a.contains(a1)}); })()"""
    print(b.evaluate(js), flush=True)
    print("chrome: a_b 4, b_a 2, a_a1 20, a1_a 10, a1_a2 4, a2_b 4, self 0, new_last 4, detached 1", flush=True)
    os._exit(0)
