"""Album page probe: photos of a feed post via /media/set/?set=pcb.<post_id>. Counts + query names only."""
import sys
from aptbot.scrapers.facebook_generic_browser.oxide import jobs
from aptbot.scrapers.facebook_generic_browser.oxide.driver import Driver, rss_mb
from aptbot.scrapers.facebook_generic_browser import parse

from collections import Counter
TYPES = Counter()
def photo_ids(o):
    ids = set()
    for d in parse.walk(o):
        img = d.get("image") or d.get("viewer_image") or d.get("photo_image")
        if isinstance(img, dict) and "fbcdn" in str(img.get("uri", "")) and d.get("id"):
            TYPES[d.get("__typename")] += 1
            ids.add(d["id"])
    return ids

gid = sys.argv[1]
d = Driver()
meta, posts = jobs.group_feed(d, gid, limit=8, since=None)
p = next(p for p in posts if (p.photo_count or 0) > len(p.photos))
print(f"post: count={p.photo_count} feed={len(p.photos)} rss={rss_mb()}", flush=True)
jobs.think()
evs = d.goto(f"https://www.facebook.com/media/set/?set=pcb.{p.post_id}&type=1")
names = sorted({e.name for e in evs if e.kind == "graphql"})
def paths(o, target, path=()):
    if isinstance(o, dict):
        for k, v in o.items():
            yield from paths(v, target, path + (k,))
    elif isinstance(o, list):
        for i, v in enumerate(o[:50]):
            yield from paths(v, target, path + ("[]",))
    elif o == target:
        yield path
fid = p.photos[0].id
for e in evs:
    for pa in list(paths(e.chunks, fid))[:3]:
        print(" path", e.kind, "/".join(pa[-9:]), flush=True)
def nodes_at(o):
    for d in parse.walk(o):
        n = d.get("node")
        if isinstance(n, dict) and n.get("__typename") in ("Photo", "Video") or (isinstance(n, dict) and "id" in n and any(k in n for k in ("image", "thumbnail", "photo_image", "uri"))):
            yield n
ks = Counter()
for e in evs:
    for n in nodes_at(e.chunks):
        ks[(n.get("__typename"), tuple(sorted(n)[:8]))] += 1
print(" node shapes", ks.most_common(3), flush=True)
got = set()
for e in evs:
    got |= photo_ids(e.chunks)
print(f"media/set: url_ok={'/media/set' in d.url()} photos={len(got)} contains_feed={len({x.id for x in p.photos} & got)} queries={names} rss={rss_mb()}", flush=True)
for i in range(4):
    if len(got) >= p.photo_count: break
    d.scroll_burst(); evs = d.pump(2500)
    for e in evs: got |= photo_ids(e.chunks)
    print(f" burst {i}: photos={len(got)} queries={sorted({e.name for e in evs if e.kind=='graphql'})}", flush=True)
print("typenames", dict(TYPES), "dom_imgs", d.js("String(Array.from(document.images).filter(i=>/fbcdn/.test(i.src)&&i.naturalWidth!==1).length)"),
      "links", d.js("String(document.querySelectorAll('a[href*=\"/photo\"]').length)"), flush=True)
d.check_health()
