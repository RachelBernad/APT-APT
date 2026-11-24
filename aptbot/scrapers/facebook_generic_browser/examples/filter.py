"""Filter what's already cached (zero Facebook traffic): price, rooms, keywords, photos, time.

    ~/.fbgb/venv/bin/python -m aptbot.scrapers.facebook_generic_browser.examples.filter
"""
import asyncio
from datetime import datetime, timedelta, timezone

from aptbot.scrapers.facebook_generic_browser import FacebookAPI


async def main():
    async with FacebookAPI() as fb:
        posts = await fb.filter_posts(max_price=7000, min_rooms=2.5, exclude=["תיווך", "sublet", "סאבלט"],
                                      has_photos=True, since=datetime.now(timezone.utc) - timedelta(days=2))
        for p in posts:
            print(p.created_at, p.hints.prices, p.hints.rooms, p.url)


asyncio.run(main())
