"""Timeline of a capture session: documents, requests by name, seconds since start (playbook §6).

    python -m aptbot.scrapers.facebook_generic_browser.explore.tools.timeline CAPTURE_DIR [--only REGEX] [--skip REGEX]

Answers "what did the page fetch, when, from which tab" and exposes surprises (e.g. a feed loading under a
permalink while we scrolled for comments). Reads CAPTURE_DIR/index.jsonl written by capture.Capture.
"""
import json
import re
import sys
from pathlib import Path


def main(argv):
    d = Path(argv[0])
    only = re.compile(argv[argv.index("--only") + 1]) if "--only" in argv else None
    skip = re.compile(argv[argv.index("--skip") + 1]) if "--skip" in argv else None
    rows = [json.loads(l) for l in (d / "index.jsonl").open()]
    t0 = rows[0]["t"] if rows else 0
    counts: dict[str, int] = {}
    for r in rows:
        label = r.get("name") or r.get("label") or r.get("url", "")
        if only and not only.search(label) and r.get("kind") != "document":
            continue
        if skip and skip.search(label):
            continue
        counts[label] = counts.get(label, 0) + 1
        size = f"{r['bytes'] // 1024}KB" if r.get("bytes") else ""
        print(f"{r['t'] - t0:7.1f}s  {r.get('kind', ''):9} {label[:70]:70} {size:>8}  {(r.get('page') or '')[25:80]}")
    print("\nby name:", json.dumps(dict(sorted(counts.items(), key=lambda kv: -kv[1])[:25]), ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main(sys.argv[1:])
