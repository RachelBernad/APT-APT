"""Live check of fbgb on oxide: list groups, then latest posts of the given groups through FacebookAPI.
Prints counts/timings only (no names, no texts). Usage (repo root on PYTHONPATH):
    python fbgb_live.py JAR_COPY GROUP_ID [GROUP_ID ...]"""
import asyncio, os, sys, tempfile, time
from aptbot.scrapers.facebook_generic_browser import FacebookAPI


async def main(jar, gids):
    home = tempfile.mkdtemp(prefix="fbgb-oxide-")
    async with FacebookAPI(browser="oxide", home=home, oxide_jar=jar, default_timeout=400) as fb:
        t = time.time()
        groups = await fb.list_groups(max_age=0)
        mem = {g.id: g.join_state for g in groups}
        print(f"list_groups: {len(groups)} groups, member={sum(s == 'MEMBER' for s in mem.values())}, "
              f"with last_post_time={sum(bool(g.last_post_time) for g in groups)} in {time.time() - t:.0f}s "
              f"| watched member: {sum(mem.get(g) == 'MEMBER' for g in gids)}/{len(gids)}", flush=True)
        for gid in gids:
            t = time.time()
            try:
                posts = await fb.latest_posts(gid, limit=15, max_age=0)
            except Exception as e:
                print(f"group {gid}: {type(e).__name__}: {str(e)[:120]}", flush=True)
                if "alt" in type(e).__name__ or "Halted" in type(e).__name__:
                    break
                continue
            ph = sum(len(p.photos) for p in posts)
            print(f"group {gid}: {len(posts)} posts, text={sum(bool(p.text) for p in posts)}, photos={ph}, "
                  f"with_time={sum(bool(p.created_at) for p in posts)}, comments_cnt={sum(p.comment_count or 0 for p in posts)} "
                  f"in {time.time() - t:.0f}s, worker peak {fb._oxide.peak_rss_mb} MB", flush=True)
        print(f"DONE worker peak {fb._oxide.peak_rss_mb} MB", flush=True)

asyncio.run(main(sys.argv[1], sys.argv[2:]))
