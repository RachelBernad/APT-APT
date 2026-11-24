"""Log the persistent profile into Facebook, either by a human or assisted (credentials at runtime).

    fbgb login            # opens facebook.com; a HUMAN logs in, then closes the window
    fbgb login --stdin    # reads email and password from stdin (2 lines) or prompts without echo, types them

Both modes report only whether the session cookies exist (by name). Credentials are held in memory for the
typing only: never written to disk, logs, env vars, argv or the repo. If Facebook shows a checkpoint or a 2FA
code page that needs input, the assisted mode stops and says so: finish that step by hand (VNC on a no-GUI box).
"""
from __future__ import annotations

import asyncio
import getpass
import sys
from urllib.parse import urlparse

from aptbot.scrapers.facebook_generic_browser import human
from aptbot.scrapers.facebook_generic_browser.browser import PROFILE_DIR, open_context


async def logged_in(ctx) -> bool:
    names = {c["name"] for c in await ctx.cookies("https://www.facebook.com")}
    return {"c_user", "xs"} <= names


def read_credentials() -> tuple[str, str]:
    """Two lines from stdin (piped), or interactive prompts without echo."""
    if sys.stdin.isatty():
        return input("Facebook email: ").strip(), getpass.getpass("Facebook password: ")
    lines = sys.stdin.read().splitlines()
    if len(lines) < 2:
        raise SystemExit("expected 2 lines on stdin: email, password")
    return lines[0].strip(), lines[1]


async def assisted(timeout_s: float = 60) -> bool:
    """Type the credentials into the login form like a person, then wait for the session cookies."""
    email, password = read_credentials()
    async with open_context(PROFILE_DIR) as ctx:
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()
        await page.goto("https://www.facebook.com/", wait_until="domcontentloaded")
        await page.wait_for_timeout(2500)
        if await logged_in(ctx):
            print("logged in: yes (already)")
            return True
        email_box, pass_box = page.locator('input[name="email"]'), page.locator('input[name="pass"]')
        if not (await email_box.count() and await pass_box.count()):
            print(f"no login form at {urlparse(page.url).path}: finish by hand (`fbgb login`)")
            return False
        await human.think(0.8, 1.6)
        await human.click(page, email_box.first)
        await human.type_text(page, email)
        await human.think(0.3, 0.8)
        await human.click(page, pass_box.first)
        await human.type_text(page, password)
        email = password = ""  # drop references as soon as typing is done
        await human.think(0.3, 0.7)
        await page.keyboard.press("Enter")
        path = ""
        for _ in range(int(timeout_s / 2)):
            await page.wait_for_timeout(2000)
            path = urlparse(page.url).path
            if await logged_in(ctx) and not path.startswith(("/checkpoint", "/two_step_verification", "/login")):
                await page.wait_for_timeout(2000)  # let the home shell settle before closing
                print("logged in: yes")
                return True
        print(f"logged in: no (stuck at {path}): Facebook wants a step a human must do. Run `fbgb login` "
              "(VNC on a no-GUI box), complete it by hand, close the window.")
        return False


async def manual() -> None:
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


async def main(stdin: bool = False) -> int:
    if stdin:
        return 0 if await assisted() else 1
    await manual()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main("--stdin" in sys.argv)))
