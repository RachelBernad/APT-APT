"""Server-rendered data inside an HTML page: extract JSON islands, find where a value lives (playbook §4).

    python -m aptbot.scrapers.facebook_generic_browser.explore.tools.islands PAGE.html --find TEXT [--context 300]
    python -m aptbot.scrapers.facebook_generic_browser.explore.tools.islands PAGE.html --keys REGEX

--find: raw-text search with context (works even when the value is inside escaped JSON strings).
--keys: every key matching REGEX across all parsed islands, with example values and counts.
Islands = <script type="application/json"> blocks (Facebook: data-sjs), __NEXT_DATA__, window.__APOLLO_STATE__
style assignments are also tried. Many SPAs ship the first screen's data this way, not via XHR.
"""
import collections
import json
import re
import sys

from aptbot.scrapers.facebook_generic_browser import parse

_SCRIPT_JSON = re.compile(r'<script[^>]*type="application/(?:json|ld\+json)"[^>]*>(.*?)</script>', re.S)
_ASSIGN = re.compile(r'window\.(__[A-Z_]+__)\s*=\s*(\{.*?\});\s*</script>', re.S)


def islands(html: str) -> list:
    out = []
    for m in _SCRIPT_JSON.finditer(html):
        try:
            out.append(json.loads(m.group(1)))
        except json.JSONDecodeError:
            pass
    for m in _ASSIGN.finditer(html):
        try:
            out.append({m.group(1): json.loads(m.group(2))})
        except json.JSONDecodeError:
            pass
    return out


def main(argv):
    html = open(argv[0]).read()
    if "--find" in argv:
        needle = argv[argv.index("--find") + 1]
        ctx = int(argv[argv.index("--context") + 1]) if "--context" in argv else 300
        for m in list(re.finditer(re.escape(needle), html))[:10]:
            print("==", m.start(), html[max(0, m.start() - ctx):m.end() + ctx].replace("\n", " "), "\n")
        return
    rx = re.compile(argv[argv.index("--keys") + 1])
    isl = islands(html)
    seen = collections.Counter()
    ex = {}
    for d in parse.walk(isl):
        for k, v in d.items():
            if rx.search(k):
                seen[k] += 1
                ex.setdefault(k, str(v)[:100])
    print(f"{len(isl)} islands")
    for k, n in seen.most_common():
        print(f"{n:5}  {k}: {ex[k]}")


if __name__ == "__main__":
    main(sys.argv[1:])
