"""Turn a real capture into a committable test fixture (docs/extending.md).

    ~/.fbgb/venv/bin/python -m aptbot.scrapers.facebook_generic_browser.explore.anonymize IN.json OUT.json
    ~/.fbgb/venv/bin/python -m aptbot.scrapers.facebook_generic_browser.explore.anonymize --html IN.html OUT.json [ids…]

- people (actors/authors/owners/commenters): ids → 9000000000NN, names → "Person NN", profile urls → fake
- phone numbers → 050-0000000; CDN urls → https://scontent.example/…; tracking/logging blobs dropped
- --html: keep only the JSON-island dicts relevant to parsers (group objects with the given ids, join states,
  the left rail, Story nodes), so a 2 MB page becomes a few KB.
Group names/ids stay (they're public). ALWAYS eyeball the output before committing it.
"""
from __future__ import annotations

import json
import re
import sys

from aptbot.scrapers.facebook_generic_browser import capture, parse

DROP_KEY = re.compile(r"tracking|logging|encrypted|^__module|^__dr$|cix_screen|debug_info|sponsored|^cache_id$|"
                      r"^accessibility_caption$|^profile_picture|^image_low|^blurred", re.I)
PERSON_KEYS = ("actors", "author", "owning_profile", "actor", "user")
PHONE = re.compile(r"(?<!\d)0(?:5\d|[2-489]|7\d)[-\s]?\d{3}[-\s]?\d{4}(?!\d)")
CDN = re.compile(r"https:(?:\\/|/){2}scontent[^\"\s]*")


def _is_user(d: dict) -> bool:
    return "User" in (d.get("__typename"), d.get("__isActor"), d.get("__isProfile"), d.get("__isNode")) or \
        d.get("__typename") in ("GroupAnonAuthorProfile", "Viewer")


def _people(o, out: dict) -> None:
    for d in parse.walk(o):
        if _is_user(d) and d.get("id") and isinstance(d.get("name"), str):
            out.setdefault(d["id"], d["name"])
        for k in PERSON_KEYS:
            v = d.get(k)
            for p in (v if isinstance(v, list) else [v]):
                if isinstance(p, dict) and p.get("id") and p.get("__typename") not in ("Group",) and p.get("name"):
                    out.setdefault(p["id"], p["name"])


def _strip(o):
    if isinstance(o, dict):
        return {k: _strip(v) for k, v in o.items() if not DROP_KEY.search(k)}
    if isinstance(o, list):
        return [_strip(v) for v in o]
    return o


SYNTH_POST = "Synthetic post {n}: להשכרה דירת {r} חדרים ברחוב הדוגמה {n}, {p:,} ₪ לחודש, כניסה מיידית."
SYNTH_COMMENT = "Synthetic comment {n}"


def _synthesize_texts(o) -> None:
    """Replace every post/comment body with synthetic text (real bodies can name people)."""
    n = 0
    for d in parse.walk(o):
        for key, tpl in (("message", SYNTH_POST), ("body", SYNTH_COMMENT)):
            v = d.get(key)
            if isinstance(v, dict) and isinstance(v.get("text"), str) and v["text"]:
                n += 1
                v["text"] = tpl.format(n=n, r=2 + n % 4, p=4000 + 250 * n)
                v.pop("ranges", None)
                v.pop("aggregated_ranges", None)


def anonymize(obj):
    _synthesize_texts(obj)
    people: dict[str, str] = {}
    _people(obj, people)
    s = json.dumps(_strip(obj), ensure_ascii=False)
    for n, (pid, name) in enumerate(sorted(people.items(), key=lambda kv: -len(kv[1])), 1):
        fake_id = f"9000000000{n:02d}"
        s = s.replace(f'"{pid}"', f'"{fake_id}"').replace(f"/{pid}/", f"/{fake_id}/").replace(f"={pid}", f"={fake_id}")
        if len(name) > 2:
            s = s.replace(json.dumps(name, ensure_ascii=False)[1:-1], f"Person {n:02d}")
    s = re.sub(r"profile\.php\?id=\d+", "profile.php?id=900000000000", s)
    s = PHONE.sub("050-0000000", s)
    s = CDN.sub("https://scontent.example/photo.jpg", s)
    s = re.sub(r"https:(\\/|/){2}www\.facebook\.com(\\/|/)(?!groups|photo|media|permalink|search)[A-Za-z0-9.]+(\\/|/)?\"",
               'https://www.facebook.com/person"', s)
    return json.loads(s)


def html_relevant(html: str, ids: list[str]) -> list:
    blobs = capture.html_json_blobs(html)
    keep = []
    for d in parse.walk(blobs):
        if (d.get("id") in ids or d.get("viewer_join_state") or "tab_groups_list" in d
                or (d.get("__typename") == "Story" and d.get("post_id"))):
            keep.append(d)
    return keep


def main(argv: list[str]) -> None:
    if argv[0] == "--html":
        src, dst, ids = argv[1], argv[2], argv[3:]
        data = html_relevant(open(src).read(), ids)
    else:
        src, dst = argv[0], argv[1]
        data = json.load(open(src))
        data = {"params": {k: v for k, v in data.get("params", {}).items() if k != "variables"} |
                {"variables": {k: v for k, v in (data.get("params", {}).get("variables") or {}).items()
                               if "relayprovider" not in k}},
                "chunks": data["chunks"]}
    out = anonymize(data)
    with open(dst, "w") as f:
        json.dump(out, f, ensure_ascii=False, separators=(",", ":"))
    print(f"{dst}: {len(json.dumps(out)) // 1024} KB")


if __name__ == "__main__":
    main(sys.argv[1:])
