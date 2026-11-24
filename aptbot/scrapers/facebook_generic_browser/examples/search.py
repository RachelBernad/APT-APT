"""Find groups by keyword, and search inside one group.

    ~/.fbgb/venv/bin/python -m aptbot.scrapers.facebook_generic_browser.examples.search "דירה להשכרה" [group_id "פלורנטין"]
"""
import asyncio
import sys

from aptbot.scrapers.facebook_generic_browser import FacebookAPI


async def main(query: str, group_id: str | None = None, in_group: str | None = None):
    async with FacebookAPI() as fb:
        if group_id and in_group:   # both run at the same time, in two tabs
            hits, posts = await asyncio.gather(fb.search_groups(query, limit=10),
                                               fb.search_group(group_id, in_group, limit=10))
        else:
            hits, posts = await fb.search_groups(query, limit=10), []
        for h in hits:
            print(f"{h.id:>18}  {h.summary or '':45}  {h.name}")
        for p in posts:
            print(p.created_at, p.url, "|", p.text[:80].replace("\n", " "))


asyncio.run(main(*sys.argv[1:]))
