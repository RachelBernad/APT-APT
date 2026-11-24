"""Print the key-path "shape" of a captured JSON payload: every distinct path with its type and a short example.

    python -m aptbot.scrapers.facebook_generic_browser.explore.tools.shape FILE.json [chunk_index|all] [max_depth] [--grep REGEX]

Works on any JSON (graphql chunks from capture.py, REST bodies, HTML islands dumped as JSON). Lists collapse to
their first element (`[]`). This is the fastest way to find where a field lives in an unknown API (playbook §5).
"""
import json
import re
import sys


def shape(o, path="", out=None, depth=0, max_depth=40):
    out = {} if out is None else out
    if depth > max_depth:
        return out
    if isinstance(o, dict):
        for k, v in o.items():
            shape(v, f"{path}.{k}", out, depth + 1, max_depth)
    elif isinstance(o, list):
        if o:
            shape(o[0], f"{path}[]", out, depth + 1, max_depth)
    elif path not in out:
        out[path] = (type(o).__name__, str(o)[:60])
    return out


def main(argv):
    grep = None
    if "--grep" in argv:
        i = argv.index("--grep"); grep = re.compile(argv[i + 1]); argv = argv[:i] + argv[i + 2:]
    data = json.load(open(argv[0]))
    which = argv[1] if len(argv) > 1 else "all"
    max_depth = int(argv[2]) if len(argv) > 2 else 40
    roots = data["chunks"] if isinstance(data, dict) and "chunks" in data else [data]
    out = {}
    for i, c in enumerate(roots):
        if which == "all" or str(i) == which:
            shape(c, f"c{i}", out, 0, max_depth)
    for p, (t, v) in out.items():
        if grep is None or grep.search(p) or grep.search(v):
            print(f"{p}  <{t}> {v}")


if __name__ == "__main__":
    main(sys.argv[1:])
