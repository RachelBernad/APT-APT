"""latest_posts on oxide with album completion: photo_count vs URLs, URL fetchable, sizes. Counts only."""
import asyncio, sys, tempfile, time, urllib.request
from aptbot.scrapers.facebook_generic_browser import FacebookAPI

def ok(u):
    try:
        with urllib.request.urlopen(urllib.request.Request(u, headers={"Range": "bytes=0-1023"}), timeout=15) as r:
            return r.status in (200, 206)
    except Exception:
        return False

async def main(jar, gid):
    async with FacebookAPI(browser="oxide", home=tempfile.mkdtemp(), oxide_jar=jar, default_timeout=600) as fb:
        t = time.time()
        posts = await fb.latest_posts(gid, limit=10, max_age=0)
        print(f"{len(posts)} posts in {time.time()-t:.0f}s, peak {fb._oxide.peak_rss_mb} MB", flush=True)
        print("count/urls:", [(p.photo_count, len(p.photos)) for p in posts if p.photos], flush=True)
        extra = [ph for p in posts for ph in p.photos[5:]]
        print(f"extra photos {len(extra)}: fetchable {sum(ok(x.url) for x in extra[:10])}/{min(10, len(extra))}, "
              f"widths {sorted({x.width for x in extra})[:5]}", flush=True)

asyncio.run(main(sys.argv[1], sys.argv[2]))
