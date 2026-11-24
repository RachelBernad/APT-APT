"""Concurrency demo + measurement: two groups' latest posts and a group search, all at once, over 3 tabs.

    ~/.fbgb/venv/bin/python -m aptbot.scrapers.facebook_generic_browser.examples.concurrent [gid1 gid2 "query"]

Prints per-call durations, total wall time (vs. the sum = what one tab would take), tab switches, and how many
feed responses arrived while their tab was hidden (docs/concurrency.md).
"""
import asyncio
import sys
import time

from aptbot.scrapers.facebook_generic_browser import FacebookAPI


async def timed(name, coro):
    t = time.monotonic()
    res = await coro
    return name, time.monotonic() - t, res


async def main(g1="305724686290054", g2="101875683484689", query="דירה להשכרה"):
    async with FacebookAPI(max_tabs=3) as fb:
        t0 = time.monotonic()
        results = await asyncio.gather(
            timed(f"latest_posts({g1})", fb.latest_posts(g1, limit=12, max_age=0)),
            timed(f"latest_posts({g2})", fb.latest_posts(g2, limit=12, max_age=0)),
            timed(f"search_groups({query!r})", fb.search_groups(query, limit=10)),
        )
        wall = time.monotonic() - t0
        for name, dt, res in results:
            print(f"{name:40} {dt:6.1f}s  {len(res)} results")
        total = sum(dt for _, dt, _ in results)
        print(f"wall {wall:.1f}s vs sum {total:.1f}s → {total / wall:.2f}x parallel speed-up")
        print("tab switches:", fb._pool.switches, "| capture stats:", fb._env.hub.stats)
        print("status:", (await fb.status()).to_dict())


if __name__ == "__main__":
    asyncio.run(main(*sys.argv[1:]))
