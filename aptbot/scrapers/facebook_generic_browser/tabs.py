"""Tabs of ONE account, used concurrently the way one person would (docs/concurrency.md).

- Attention (foreground) is a FIFO lock held only for short input bursts (a few wheel flicks, a click, typing).
  Whoever holds it gets its tab brought to the front first. A hidden tab never receives input.
- Between bursts, jobs wait on the network WITHOUT holding attention, so other tabs take turns. Network waits
  overlap; input is serialized.
- Every attention grab (input burst, navigation, new tab) is preceded by `pace()`: a 10-30 ms random gap.
"""
from __future__ import annotations

import asyncio
import os
import random
from contextlib import asynccontextmanager

from aptbot.scrapers.facebook_generic_browser import human

_rng = random.SystemRandom()

# Page-load timeout for typed navigations, in seconds. 45 s suits a desktop; a slow host
# (a Raspberry Pi 3 swapping while Chromium starts) needs more: set FBGB_NAV_TIMEOUT=120.
NAV_TIMEOUT_MS = float(os.environ.get("FBGB_NAV_TIMEOUT", "45")) * 1000


async def pace() -> None:
    """A tiny random gap (10-30 ms) before every operation, so two operations never fire back-to-back.

    Facebook publishes no limit; this is our choice. Human-looking timing comes from human.py (micro/think);
    this only breaks zero-gap hand-offs between tabs and costs well under a second per poll."""
    await asyncio.sleep(_rng.uniform(0.01, 0.03))


class TabPool:
    """Attention shared by every tab of the context: one tab acts at a time."""

    def __init__(self, ctx, *, max_tabs: int = 3):
        self.ctx = ctx
        self.max_tabs = max_tabs
        self._fg = asyncio.Lock()           # asyncio.Lock wakes waiters in FIFO order → fair turns
        self.front: int | None = None       # id() of the tab currently in front
        self.holder: int | None = None      # id() of the tab holding attention (for invariants/tests)
        self.switches = 0
        self._slots = asyncio.Semaphore(max_tabs)
        self._held: set[int] = set()

    @asynccontextmanager
    async def _attention(self):
        """Take the FIFO attention lock, then wait the small jittered gap."""
        async with self._fg:
            await pace()
            yield

    @asynccontextmanager
    async def foreground(self, page):
        """Exclusive input focus on `page` for a short burst. Brings it to the front if needed."""
        async with self._attention():
            self.holder = id(page)
            try:
                if self.front != id(page):
                    await page.bring_to_front()
                    self.front = id(page)
                    self.switches += 1
                    await human.think(0.2, 0.6)  # eyes landing on the switched tab
                yield page
            finally:
                self.holder = None

    async def new_tab(self):
        """Open a tab (Chrome focuses new tabs, so it becomes the front tab)."""
        async with self._attention():
            page = await self.ctx.new_page()
            self.front = id(page)
            return page

    async def goto(self, page, url: str, *, wait: str = "domcontentloaded",
                   timeout: float | None = None) -> None:
        """Typed-URL navigation. Attention is held only while 'typing'; the load happens after."""
        timeout = NAV_TIMEOUT_MS if timeout is None else timeout
        async with self.foreground(page):
            await human.micro()
            await page.goto(url, wait_until="commit", timeout=timeout)
        if wait != "commit":
            await page.wait_for_load_state(wait, timeout=timeout)

    async def open_in_background(self, page, link):
        """Cmd/Ctrl+click `link` in `page` → new background tab (counts against max_tabs)."""
        await self._slots.acquire()
        try:
            async with self.foreground(page):
                async with self.ctx.expect_page() as info:
                    await human.click(page, link, modifiers=[human.OPEN_IN_BG_MODIFIER])
                new = await info.value
            self._held.add(id(new))
            new.once("close", lambda *_: self._release(new))
            return new
        except BaseException:
            self._slots.release()
            raise

    async def open_url_in_background(self, opener, url: str):
        """Open `url` in a new tab, then give the front back to `opener` (the new tab loads hidden)."""
        await self._slots.acquire()
        try:
            async with self._attention():
                new = await self.ctx.new_page()
                await new.goto(url, wait_until="commit")
                await human.micro()
                await opener.bring_to_front()
                self.front = id(opener)
            self._held.add(id(new))
            new.once("close", lambda *_: self._release(new))
            return new
        except BaseException:
            self._slots.release()
            raise

    def _release(self, page) -> None:
        if id(page) in self._held:
            self._held.discard(id(page))
            self._slots.release()

    async def close(self, page) -> None:
        await human.micro()
        await page.close()
