"""Latest posts of one group, newest first; then the same posts streamed as they arrive.

    ~/.fbgb/venv/bin/python -m aptbot.scrapers.facebook_generic_browser.examples.latest_posts <group_id>
"""
import asyncio
import sys
from datetime import datetime, timedelta, timezone

from aptbot.scrapers.facebook_generic_browser import FacebookAPI


async def main(group_id: str):
    async with FacebookAPI() as fb:
        posts = await fb.latest_posts(group_id, limit=10)
        for p in posts:
            print(p.created_at, p.author_name, p.hints.prices, p.hints.rooms, p.photo_count, p.url)
            print("   ", p.text[:120].replace("\n", " "))

        # Only the last 6 hours, delivered post by post (3 per Facebook page); stop early whenever you like.
        since = datetime.now(timezone.utc) - timedelta(hours=6)
        async for p in fb.stream_posts(group_id, since=since, limit=50):
            print("streamed:", p.post_id, p.created_at)


asyncio.run(main(sys.argv[1]))
