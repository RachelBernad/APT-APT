"""Two new_posts() cycles over the bot's groups (oxide), then photo-URL and comment checks. Counts only.
    python fbgb_cycle.py JAR_COPY"""
import asyncio, sys, tempfile, time, urllib.request
from aptbot import config
from aptbot.scrapers.facebook_generic_browser import FacebookAPI


def head_ok(url):
    try:
        req = urllib.request.Request(url, method="GET", headers={"Range": "bytes=0-1023", "User-Agent": "TelegramBot (like TwitterBot)"})
        with urllib.request.urlopen(req, timeout=15) as r:
            return r.status in (200, 206), r.headers.get("content-type", "")
    except Exception as e:
        return False, type(e).__name__


async def main(jar):
    gids = config.FB_GROUP_IDS
    async with FacebookAPI(browser="oxide", home=tempfile.mkdtemp(prefix="fbgb-cycle-"), oxide_jar=jar,
                           default_timeout=400) as fb:
        posts = []
        for cyc in (1, 2):
            t = time.time(); fb._oxide.peak_rss_mb = 0
            new = await fb.new_posts(gids)
            print(f"cycle {cyc}: {len(new)} new posts from {len({p.group_id for p in new})} groups in {time.time() - t:.0f}s, "
                  f"worker peak {fb._oxide.peak_rss_mb} MB", flush=True)
            posts = posts or new
        # G2 photos: the URLs the bot hands to Telegram
        urls = [ph.url for p in posts for ph in p.photos if ph.url][:12]
        res = [head_ok(u) for u in urls]
        print(f"photos: {sum(ok for ok, _ in res)}/{len(res)} fetchable without cookies, types={sorted({c.split(';')[0] for _, c in res})}, "
              f"sizes={sorted({(ph.width, ph.height) for p in posts for ph in p.photos if ph.width})[:4]}", flush=True)
        print(f"photo_count vs urls: {[(p.photo_count, len(p.photos)) for p in posts if (p.photo_count or 0) > len(p.photos)][:6]}", flush=True)
        # G3 comments: the post with the most comments, via its permalink
        best = max(posts, key=lambda p: p.comment_count or 0, default=None)
        if best and (best.comment_count or 0) > 0:
            t = time.time()
            full = await fb.get_post(best.group_id, best.post_id, with_comments=True)
            print(f"get_post: counter={full.comment_count} parsed={len(full.comments or [])} "
                  f"replies={sum(bool(getattr(c, 'reply_to', None)) for c in full.comments or [])} "
                  f"text_same={full.text == best.text} photos {len(best.photos)}->{len(full.photos)} in {time.time() - t:.0f}s", flush=True)
        print(f"DONE peak {fb._oxide.peak_rss_mb} MB", flush=True)

asyncio.run(main(sys.argv[1]))
