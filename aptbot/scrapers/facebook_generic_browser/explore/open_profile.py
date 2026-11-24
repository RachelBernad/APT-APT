"""Open the persistent profile so a HUMAN can log in (or just look around). No automation input.

    ~/.fbgb/venv/bin/python -m aptbot.scrapers.facebook_generic_browser.explore.open_profile

Opens facebook.com in the profile at ~/.fbgb/profile. Log in by hand (email, password, 2FA,
"trust this device" = yes), then simply close the Chrome window. The script reports whether a
session cookie exists. It never reads, prints or stores the credentials or cookie values.
"""
from __future__ import annotations

import asyncio

from aptbot.scrapers.facebook_generic_browser.browser import PROFILE_DIR, open_context


async def logged_in(ctx) -> bool:
    names = {c["name"] for c in await ctx.cookies("https://www.facebook.com")}
    return {"c_user", "xs"} <= names


async def main() -> None:
    async with open_context(PROFILE_DIR) as ctx:
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()
        if page.url in ("", "about:blank"):
            await page.goto("https://www.facebook.com/")
        print(f"profile: {PROFILE_DIR}")
        print("Log in by hand in the Chrome window, then close the window.", flush=True)
        closed = asyncio.Event()
        ctx.on("close", lambda *_: closed.set())
        while not closed.is_set():
            ok = await logged_in(ctx) if ctx.pages else None
            if ok is not None:
                print(f"\rlogged in: {'yes' if ok else 'no '}", end="", flush=True)
            try:
                await asyncio.wait_for(closed.wait(), timeout=3)
            except asyncio.TimeoutError:
                if not ctx.pages:
                    break
    print()


if __name__ == "__main__":
    asyncio.run(main())
