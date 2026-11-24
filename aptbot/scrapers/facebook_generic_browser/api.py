"""FacebookAPI: the public, async, concurrency-safe interface of this library.

    async with FacebookAPI() as fb:
        groups = await fb.list_groups()
        posts  = await fb.latest_posts(groups[0].id, limit=20)

Every method is a coroutine that may be called concurrently from many tasks. Browser work is scheduled
over up to `max_tabs` tabs of ONE logged-in account (docs/concurrency.md); cache reads never touch the
browser. Full reference: docs/api-reference.md. Errors: docs/errors-and-safety.md.
"""
from __future__ import annotations

import asyncio
import contextlib
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncIterator, Iterable

from aptbot.scrapers.facebook_generic_browser import jobs
from aptbot.scrapers.facebook_generic_browser.browser import HOME, PROFILE_DIR
from aptbot.scrapers.facebook_generic_browser.dispatcher import Dispatcher, Priority
from aptbot.scrapers.facebook_generic_browser.errors import (AccountAtRisk, FBGBError, Halted, NotLoggedIn)
from aptbot.scrapers.facebook_generic_browser.models import Group, GroupHit, Post, State, Status
from aptbot.scrapers.facebook_generic_browser.store import Store
from aptbot.scrapers.facebook_generic_browser.tabs import Limiter, TabPool

_END = object()


def _unix(v: datetime | float | int | None) -> float | None:
    if v is None:
        return None
    return v.timestamp() if isinstance(v, datetime) else float(v)


class FacebookAPI:
    def __init__(self, *, home: Path | None = None, profile_dir: Path | None = None, max_tabs: int = 3,
                 limiter: Limiter | None = None, idle_close_after: float | None = 600,
                 default_timeout: float = 240, debug_dump: bool = False):
        """
        home:             state dir (cache db, halt marker). Default $FBGB_HOME or ~/.fbgb.
        profile_dir:      Chrome profile with the logged-in session. Default $FBGB_PROFILE_DIR or <home>/profile.
        max_tabs:         tabs used in parallel (3 on a Mac, 2 on a low-CPU host).
        limiter:          account-wide navigation budget (default 8/min, burst 3, 120/hour).
        idle_close_after: close Chrome after this many idle seconds (None = keep it open). It reopens on demand.
        default_timeout:  per-call timeout in seconds for browser-backed calls.
        debug_dump:       write raw captured payloads to <home>/captures/ (contain real posts; debugging only).
        """
        self.home = Path(home) if home else HOME
        self.profile_dir = Path(profile_dir) if profile_dir else (self.home / "profile" if home else PROFILE_DIR)
        self.max_tabs = max_tabs
        self.limiter = limiter or Limiter()
        self.idle_close_after = idle_close_after
        self.default_timeout = default_timeout
        self.debug_dump = debug_dump
        self.store = Store(self.home / "fbgb.db")
        self._halt_file = self.home / "HALTED"
        self._stack: contextlib.AsyncExitStack | None = None
        self._ctx = None
        self._pool: TabPool | None = None
        self._dispatcher: Dispatcher | None = None
        self._env: jobs.Env | None = None
        self._start_lock = asyncio.Lock()
        self._started_at: datetime | None = None
        self._last_activity = time.monotonic()
        self._idle_task: asyncio.Task | None = None
        self._logged_in: bool | None = None

    # ------------------------------------------------------------------ lifecycle
    async def __aenter__(self) -> "FacebookAPI":
        await self.store.open()
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close()

    async def close(self) -> None:
        """Close Chrome (cleanly, so cookies are flushed) and the cache."""
        await self._close_browser()
        await self.store.close()

    async def _ensure_browser(self) -> Dispatcher:
        if self._halt_file.exists():
            raise Halted(self._halt_file.read_text().strip() or "halted")
        async with self._start_lock:
            if self._dispatcher:
                return self._dispatcher
            from aptbot.scrapers.facebook_generic_browser.browser import open_context
            from aptbot.scrapers.facebook_generic_browser.capture import CaptureHub
            stack = contextlib.AsyncExitStack()
            ctx = await stack.enter_async_context(open_context(self.profile_dir))
            names = {c["name"] for c in await ctx.cookies("https://www.facebook.com")}
            self._logged_in = {"c_user", "xs"} <= names
            if not self._logged_in:
                await stack.aclose()
                raise NotLoggedIn("no Facebook session in the profile; run the login command")
            self._stack, self._ctx = stack, ctx
            self._pool = TabPool(ctx, max_tabs=self.max_tabs, limiter=self.limiter)
            hub = CaptureHub(ctx, self.home / "captures" / time.strftime("%Y%m%d-%H%M%S") if self.debug_dump else None)
            hub.is_front = lambda page: self._pool.front == id(page) if self._pool else None
            self._env = jobs.Env(pool=self._pool, hub=hub)
            initial = list(ctx.pages)

            async def tab_factory(i: int, old):
                if old is None and i == 0 and initial and not initial[0].is_closed():
                    return initial[0]
                return await self._pool.new_tab()

            self._dispatcher = Dispatcher(self.max_tabs, tab_factory)
            self._dispatcher.start()
            self._started_at = datetime.now(timezone.utc)
            if self.idle_close_after:
                self._idle_task = asyncio.create_task(self._idle_watch())
            return self._dispatcher

    async def _close_browser(self) -> None:
        if self._idle_task and self._idle_task is not asyncio.current_task():
            self._idle_task.cancel()
        self._idle_task = None
        if self._dispatcher:
            await self._dispatcher.stop()
        self._dispatcher = None
        if self._stack:
            with contextlib.suppress(Exception):
                await self._stack.aclose()
        self._stack = self._ctx = self._pool = self._env = None
        self._started_at = None

    async def _idle_watch(self) -> None:
        while True:
            await asyncio.sleep(15)
            d = self._dispatcher
            if d and not d.busy and not d.queued and time.monotonic() - self._last_activity > self.idle_close_after:
                await self._close_browser()
                return

    # ------------------------------------------------------------------ running jobs
    def _halt(self, reason: str) -> None:
        self.home.mkdir(parents=True, exist_ok=True)
        self._halt_file.write_text(f"{datetime.now(timezone.utc).isoformat()} {reason}\n")
        if self._dispatcher:
            self._dispatcher.halt(Halted(reason))

    async def _run(self, fn, *, key: str | None, group: str | None = None, priority: int = Priority.INTERACTIVE,
                   timeout: float | None = None):
        d = await self._ensure_browser()
        self._last_activity = time.monotonic()

        async def guarded(page):
            try:
                return await fn(self._env, page)
            except AccountAtRisk as e:
                # halt AFTER this task finishes raising (halt cancels running tasks, this one included)
                asyncio.get_running_loop().call_soon(self._halt, str(e))
                raise Halted(str(e)) from e

        try:
            return await d.run(guarded, key=key, group=group, priority=priority,
                               timeout=timeout if timeout is not None else self.default_timeout)
        finally:
            self._last_activity = time.monotonic()

    # ------------------------------------------------------------------ public API
    async def status(self) -> Status:
        """Current state. Never touches Facebook and never starts Chrome."""
        halted = self._halt_file.read_text().strip() if self._halt_file.exists() else None
        d = self._dispatcher
        return Status(state=State.HALTED if halted else (State.OK if d else State.STOPPED),
                      logged_in=self._logged_in, halt_reason=halted, tabs_busy=d.busy if d else 0,
                      max_tabs=self.max_tabs, queued=d.queued if d else 0, budget_left=self.limiter.left(),
                      started_at=self._started_at)

    async def resume(self) -> None:
        """Clear the circuit breaker after a human checked the account in a normal browser."""
        self._halt_file.unlink(missing_ok=True)
        if self._dispatcher:
            self._dispatcher.resume()

    async def list_groups(self, *, max_age: float = 600, timeout: float | None = None) -> list[Group]:
        """Groups of the account: members AND pending requests (check `join_state`). 1 navigation when stale."""
        if await self.store.is_fresh("groups", max_age):
            return await self.store.groups()
        groups = await self._run(lambda env, page: jobs.list_groups(env, page), key="groups", timeout=timeout)
        await self.store.upsert_groups(groups)
        await self.store.mark_fetched("groups")
        cached = {g.id: g for g in await self.store.groups()}
        return [cached.get(g.id, g) for g in groups]

    async def get_group(self, group_id: str, *, max_age: float = 3600, timeout: float | None = None) -> Group:
        """One group's metadata (privacy, members, description, join_state). 1 navigation when stale."""
        cached = await self.store.group(group_id)
        if cached and cached.privacy and await self.store.is_fresh(f"group:{group_id}", max_age):
            return cached
        g = await self._run(lambda env, page: jobs.get_group(env, page, group_id),
                            key=f"group:{group_id}", group=group_id, timeout=timeout)
        await self.store.upsert_groups([g])
        await self.store.mark_fetched(f"group:{group_id}")
        return await self.store.group(group_id) or g

    async def latest_posts(self, group_id: str, *, limit: int = 30, since: datetime | float | None = None,
                           max_age: float = 60, priority: int = Priority.INTERACTIVE,
                           timeout: float | None = None) -> list[Post]:
        """Newest-first posts of a group (full text, photos, counts, comment previews).
        `since`: only posts created after it. Served from cache if the group was read < max_age s ago."""
        since_ts = _unix(since)
        if max_age and await self.store.is_fresh(f"feed:{group_id}", max_age):
            return await self.store.posts([group_id], since=datetime.fromtimestamp(since_ts, timezone.utc)
                                          if since_ts else None, limit=limit)
        meta, posts = await self._run(
            lambda env, page: jobs.group_feed(env, page, group_id, limit=limit, since=since_ts),
            key=f"feed:{group_id}:{limit}:{since_ts}", group=group_id, priority=priority, timeout=timeout)
        await self.store.upsert_groups([meta])
        await self.store.upsert_posts(posts)
        await self.store.mark_fetched(f"feed:{group_id}")
        return posts

    async def stream_posts(self, group_id: str, *, limit: int = 100, since: datetime | float | None = None,
                           timeout: float | None = None) -> AsyncIterator[Post]:
        """Like latest_posts, but yields each post as soon as Facebook delivers it (3 per pagination).
        Breaking out of the loop cancels the scrolling and frees the tab."""
        since_ts = _unix(since)
        q: asyncio.Queue = asyncio.Queue()

        async def sink(batch: list[Post]) -> None:
            await self.store.upsert_posts(batch)
            for p in batch:
                q.put_nowait(p)

        async def job(env, page):
            try:
                return await jobs.group_feed(env, page, group_id, limit=limit, since=since_ts, sink=sink)
            finally:
                q.put_nowait(_END)

        runner = asyncio.create_task(self._run(job, key=None, group=group_id, timeout=timeout))
        try:
            while True:
                getter = asyncio.create_task(q.get())
                done, _ = await asyncio.wait({getter, runner}, return_when=asyncio.FIRST_COMPLETED)
                if getter in done:
                    item = getter.result()
                    if item is _END:
                        break
                    yield item
                else:
                    getter.cancel()
                    while not q.empty():
                        item = q.get_nowait()
                        if item is not _END:
                            yield item
                    break
            meta, _ = await runner
            await self.store.upsert_groups([meta])
            await self.store.mark_fetched(f"feed:{group_id}")
        finally:
            if not runner.done():
                runner.cancel()
                with contextlib.suppress(BaseException):
                    await runner

    async def new_posts(self, group_ids: Iterable[str] | None = None, *, per_group_limit: int = 50,
                        first_run_limit: int = 10, timeout: float | None = None) -> list[Post]:
        """Posts created since the previous new_posts() call, across groups (default: all MEMBER groups).
        Cheapest polling: groups whose last_post_time didn't move are skipped without being opened.
        Groups are read concurrently (one tab each). The first call per group returns its newest
        `first_run_limit` posts and sets the watermark."""
        groups = await self.list_groups(max_age=0, timeout=timeout)
        wanted = set(group_ids) if group_ids else None
        todo: list[tuple[Group, float | None]] = []
        for g in groups:
            if g.join_state != "MEMBER" or (wanted and g.id not in wanted):
                continue
            wm = await self.store.watermark(g.id)
            if wm is not None and g.last_post_time and g.last_post_time.timestamp() <= wm:
                continue   # nothing new: skip without opening the group
            todo.append((g, wm))

        async def one(g: Group, wm: float | None) -> list[Post]:
            limit = per_group_limit if wm is not None else first_run_limit
            posts = await self.latest_posts(g.id, limit=limit, since=wm, max_age=0, priority=Priority.POLL,
                                            timeout=timeout)
            if posts:
                await self.store.set_watermark(g.id, max(p.created_at.timestamp() for p in posts if p.created_at))
            elif wm is None:
                await self.store.set_watermark(g.id, time.time())
            return posts

        results = await asyncio.gather(*(one(g, wm) for g, wm in todo), return_exceptions=True)
        out: list[Post] = []
        for r in results:
            if isinstance(r, Halted):
                raise r
            if isinstance(r, list):
                out += r
        return sorted(out, key=lambda p: p.created_at.timestamp() if p.created_at else 0, reverse=True)

    async def search_groups(self, query: str, *, limit: int = 20, timeout: float | None = None) -> list[GroupHit]:
        """Find groups by keyword (Facebook search → Groups). Each hit says privacy, size and activity."""
        return await self._run(lambda env, page: jobs.search_groups(env, page, query, limit=limit),
                               key=f"search_groups:{query}:{limit}", timeout=timeout)

    async def search_group(self, group_id: str, query: str, *, limit: int = 30,
                           timeout: float | None = None) -> list[Post]:
        """Keyword search inside one group (Facebook's in-group search; relevance order, not by date)."""
        posts = await self._run(lambda env, page: jobs.search_in_group(env, page, group_id, query, limit=limit),
                                key=f"search_group:{group_id}:{query}:{limit}", group=group_id, timeout=timeout)
        await self.store.upsert_posts(posts)
        return posts

    async def get_post(self, group_id: str, post_id: str, *, with_comments: bool = True,
                       timeout: float | None = None) -> Post:
        """One post from its permalink page (1 navigation), optionally with its comments (comment loading beyond
        the embedded ones is still marked Unverified in docs/api-reference.md)."""
        post = await self._run(lambda env, page: jobs.get_post(env, page, group_id, post_id,
                                                                with_comments=with_comments),
                               key=f"post:{group_id}:{post_id}:{with_comments}", timeout=timeout)
        await self.store.upsert_posts([post])
        return post

    async def get_comments(self, group_id: str, post_id: str, *, timeout: float | None = None):
        """EXPERIMENTAL: the comments of one post (via get_post)."""
        return (await self.get_post(group_id, post_id, with_comments=True, timeout=timeout)).comments or []

    async def cross_group_feed(self, *, limit: int = 30, timeout: float | None = None) -> list[Post]:
        """"Your feed": recent posts across all joined groups (Facebook's ranking, not chronological)."""
        posts = await self._run(lambda env, page: jobs.cross_group_feed(env, page, limit=limit),
                                key=f"cross_feed:{limit}", timeout=timeout)
        await self.store.upsert_posts(posts)
        return posts

    async def filter_posts(self, *, group_ids: list[str] | None = None, include: list[str] | None = None,
                           exclude: list[str] | None = None, since: datetime | float | None = None,
                           has_photos: bool | None = None, author_id: str | None = None,
                           min_price: int | None = None, max_price: int | None = None,
                           min_rooms: float | None = None, max_rooms: float | None = None,
                           limit: int = 100) -> list[Post]:
        """Filter CACHED posts (zero Facebook traffic). Keywords match the original and translated text,
        case-insensitive; `include` = all must match, `exclude` = none may. Price/rooms use the text hints
        (posts without a hint are dropped when a price/rooms bound is given)."""
        since_ts = _unix(since)
        rows = await self.store.posts(group_ids, since=datetime.fromtimestamp(since_ts, timezone.utc)
                                      if since_ts else None, limit=100_000)
        inc = [s.lower() for s in include or []]
        exc = [s.lower() for s in exclude or []]
        out = []
        for p in rows:
            text = f"{p.text}\n{p.text_translated or ''}".lower()
            if inc and not all(s in text for s in inc):
                continue
            if exc and any(s in text for s in exc):
                continue
            if has_photos is not None and (p.photo_count > 0) != has_photos:
                continue
            if author_id and p.author_id != author_id:
                continue
            if min_price is not None or max_price is not None:
                ok = [v for v in p.hints.prices if (min_price is None or v >= min_price)
                      and (max_price is None or v <= max_price)]
                if not ok:
                    continue
            if min_rooms is not None or max_rooms is not None:
                r = p.hints.rooms
                if r is None or (min_rooms is not None and r < min_rooms) or (max_rooms is not None and r > max_rooms):
                    continue
            out.append(p)
            if len(out) >= limit:
                break
        return out


__all__ = ["FacebookAPI", "Priority", "FBGBError"]
