"""List the account's groups, then pick one by id.

    ~/.fbgb/venv/bin/python -m aptbot.scrapers.facebook_generic_browser.examples.list_groups
"""
import asyncio

from aptbot.scrapers.facebook_generic_browser import FacebookAPI


async def main():
    async with FacebookAPI() as fb:
        groups = await fb.list_groups()                       # 1 navigation (cached for 10 min)
        for g in sorted(groups, key=lambda g: g.last_post_time.timestamp() if g.last_post_time else 0, reverse=True):
            print(f"{g.id:>18}  {g.join_state:9}  last post {g.last_post_time or '?'}  {g.name}")
        readable = [g for g in groups if g.join_state == "MEMBER"]
        chosen = await fb.get_group(readable[0].id)           # metadata: privacy, members, description
        print("\nchosen:", chosen.name, "|", chosen.privacy, "|", chosen.member_count)


asyncio.run(main())
