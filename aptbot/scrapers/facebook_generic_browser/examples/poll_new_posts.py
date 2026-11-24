"""Incremental polling: every run prints only posts created since the previous run (watermarks in the cache).

    ~/.fbgb/venv/bin/python -m aptbot.scrapers.facebook_generic_browser.examples.poll_new_posts [group_id ...]

Groups whose newest post is older than their watermark are skipped without being opened, and changed
groups are read concurrently. Run it from a scheduler at most every 20-30 minutes (SPEC §7).
"""
import asyncio
import sys

from aptbot.scrapers.facebook_generic_browser import FacebookAPI, Halted


async def main(group_ids):
    async with FacebookAPI() as fb:
        try:
            new = await fb.new_posts(group_ids or None)
        except Halted as e:
            print("STOPPED for account safety:", e.reason, "- check the account by hand, then run `resume`.")
            return
        for p in new:
            print(p.created_at, p.group_name, "|", p.text[:100].replace("\n", " "))
        print(len(new), "new posts")


asyncio.run(main(sys.argv[1:]))
