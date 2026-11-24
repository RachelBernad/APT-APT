"""Tabs of ONE account, used concurrently the way one person would (docs/concurrency.md).

- Attention (foreground) is a FIFO lock held only for short input bursts (a few wheel flicks, a click, typing).
  Whoever holds it gets its tab brought to the front first. A hidden tab never receives input.
- Between bursts, jobs wait on the network WITHOUT holding attention, so other tabs take turns. Network waits
  overlap; input is serialized.
- One account-wide Limiter: every navigation / opened post costs a token, whatever tab it happens in.
"""
from __future__ import annotations

import asyncio
import collections
import random
import time
from contextlib import asynccontextmanager

from aptbot.scrapers.facebook_generic_browser import human
from aptbot.scrapers.facebook_generic_browser.errors import BudgetExhausted

_rng = random.SystemRandom()


class Limiter:
    """Token bucket (`rate_per_min`, `burst`) plus a rolling `hourly_cap`. Waiting for the next token is short and
    jittered; exceeding the hourly cap raises BudgetExhausted(retry_after) immediately instead of blocking."""

    def __init__(self, rate_per_min: float = 8, burst: int = 3, hourly_cap: int = 120, *, session_cap: int | None = None):
        self.rate = rate_per_min / 60
        self.burst = burst
        self.hourly_cap = session_cap if session_cap is not None else hourly_cap
        self.tokens = float(burst)
        self.t = time.monotonic()
        self._spent: collections.deque[float] = collections.deque()
        self._lock = asyncio.Lock()

    def left(self) -> int:
        self._expire(time.monotonic())
        return max(0, self.hourly_cap - len(self._spent))

    def _expire(self, now: float) -> None:
        while self._spent and now - self._spent[0] > 3600:
            self._spent.popleft()

    async def take(self) -> None:
        async with self._lock:
            now = time.monotonic()
            self._expire(now)
            if len(self._spent) >= self.hourly_cap:
                raise BudgetExhausted(3600 - (now - self._spent[0]))
            while True:
                now = time.monotonic()
                self.tokens = min(self.burst, self.tokens + (now - self.t) * self.rate)
                self.t = now
                if self.tokens >= 1:
                    self.tokens -= 1
                    self._spent.append(now)
                    return
                await asyncio.sleep((1 - self.tokens) / self.rate * _rng.uniform(1.0, 1.3))


class TabPool:
    """Attention + limiter shared by every tab of the context."""

    def __init__(self, ctx, *, max_tabs: int = 3, limiter: Limiter | None = None):
        self.ctx = ctx
        self.max_tabs = max_tabs
        self.limiter = limiter or Limiter()
        self._fg = asyncio.Lock()           # asyncio.Lock wakes waiters in FIFO order → fair turns
        self.front: int | None = None       # id() of the tab currently in front
        self.holder: int | None = None      # id() of the tab holding attention (for invariants/tests)
        self.switches = 0
        self._slots = asyncio.Semaphore(max_tabs)
        self._held: set[int] = set()

    @asynccontextmanager
    async def foreground(self, page):
        """Exclusive input focus on `page` for a short burst. Brings it to the front if needed."""
        async with self._fg:
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
        async with self._fg:
            page = await self.ctx.new_page()
            self.front = id(page)
            return page

    async def goto(self, page, url: str, *, wait: str = "domcontentloaded", timeout: float = 45_000) -> None:
        """Typed-URL navigation (1 limiter token). Attention is held only while 'typing'; the load happens after."""
        await self.limiter.take()
        async with self.foreground(page):
            await human.micro()
            await page.goto(url, wait_until="commit", timeout=timeout)
        if wait != "commit":
            await page.wait_for_load_state(wait, timeout=timeout)

    async def open_in_background(self, page, link):
        """Cmd/Ctrl+click `link` in `page` → new background tab (counts against max_tabs and the limiter)."""
        await self._slots.acquire()
        try:
            await self.limiter.take()
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
            await self.limiter.take()
            async with self._fg:
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
