"""Concurrent request scheduling over N tab workers (docs/concurrency.md). Browser-agnostic.

Guarantees:
- At most `n_workers` jobs run at once, each in its own tab (a tab runs one job at a time).
- Single-flight: concurrent calls with the same `key` share ONE job and get the same result.
- Per-group exclusion: two jobs with the same `group` never run at the same time (they'd fight over the same feed).
- Priority: lower number first (INTERACTIVE < POLL < BACKGROUND); FIFO within a priority.
- Timeout/cancellation: when every caller of a job has given up, the job is dequeued or its task cancelled,
  and its tab is freed.
- Halt: `halt(exc)` fails every queued and running job with `exc` and rejects new ones until `resume()`.
Tabs are created lazily: a worker opens its tab on its first job, so light load uses one tab only.
"""
from __future__ import annotations

import asyncio
import itertools
from dataclasses import dataclass
from enum import IntEnum
from typing import Any, Awaitable, Callable

from aptbot.scrapers.facebook_generic_browser.errors import RequestTimeout


class Priority(IntEnum):
    INTERACTIVE = 0      # a caller is waiting (bot user asked)
    POLL = 1             # periodic polling (new_posts)
    BACKGROUND = 2       # prefetch / maintenance


JobFn = Callable[[Any], Awaitable[Any]]   # receives the worker's tab


@dataclass(eq=False)  # identity equality/hash: jobs live in sets; ordering via _order()
class Job:
    priority: int
    seq: int
    key: str | None = None
    group: str | None = None
    fn: JobFn | None = None
    future: asyncio.Future | None = None
    task: asyncio.Task | None = None
    waiters: int = 0


def _order(j: Job) -> tuple[int, int]:
    return (j.priority, j.seq)


def _consume(f: asyncio.Future) -> None:   # avoid "exception was never retrieved" for abandoned jobs
    if not f.cancelled():
        f.exception()


class Dispatcher:
    def __init__(self, n_workers: int, tab_factory: Callable[[int, Any], Awaitable[Any]]):
        """tab_factory(worker_index, old_tab_or_None) → a usable tab (create or recreate)."""
        self.n_workers = n_workers
        self._tab_factory = tab_factory
        self._pending: list[Job] = []
        self._inflight: dict[str, Job] = {}
        self._running: set[Job] = set()
        self._busy_groups: set[str] = set()
        self._cond = asyncio.Condition()
        self._seq = itertools.count()
        self._workers: list[asyncio.Task] = []
        self.tabs: list[Any] = [None] * n_workers
        self.halted: BaseException | None = None

    # ---- lifecycle ----
    def start(self) -> None:
        if not self._workers:
            self._workers = [asyncio.create_task(self._worker(i), name=f"fbgb-tab-{i}") for i in range(self.n_workers)]

    async def stop(self) -> None:
        for w in self._workers:
            w.cancel()
        await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers = []
        for j in self._pending:
            if not j.future.done():
                j.future.cancel()
        self._pending.clear()

    # ---- introspection ----
    @property
    def queued(self) -> int:
        return len(self._pending)

    @property
    def busy(self) -> int:
        return len(self._running)

    # ---- submit / run ----
    def submit(self, fn: JobFn, *, key: str | None = None, group: str | None = None,
               priority: int = Priority.INTERACTIVE) -> Job:
        if self.halted:
            raise self.halted
        if key and key in self._inflight:
            job = self._inflight[key]
            job.priority = min(job.priority, priority)
            job.waiters += 1
            self._pending.sort(key=_order)
            return job
        fut = asyncio.get_running_loop().create_future()
        fut.add_done_callback(_consume)
        job = Job(priority, next(self._seq), key, group, fn, fut, waiters=1)
        if key:
            self._inflight[key] = job
            fut.add_done_callback(lambda _f, k=key, j=job: self._inflight.pop(k, None) if self._inflight.get(k) is j else None)
        self._pending.append(job)
        self._pending.sort(key=_order)
        asyncio.get_running_loop().create_task(self._notify())
        return job

    async def run(self, fn: JobFn, *, key: str | None = None, group: str | None = None,
                  priority: int = Priority.INTERACTIVE, timeout: float | None = None) -> Any:
        job = self.submit(fn, key=key, group=group, priority=priority)
        try:
            return await asyncio.wait_for(asyncio.shield(job.future), timeout)
        except asyncio.TimeoutError:
            self.abandon(job)
            raise RequestTimeout(f"request timed out after {timeout}s") from None
        except asyncio.CancelledError:
            self.abandon(job)
            raise

    def abandon(self, job: Job) -> None:
        """A caller gave up. If nobody else waits for the job, drop it from the queue or cancel it."""
        job.waiters -= 1
        if job.waiters > 0 or job.future.done():
            return
        if job in self._pending:
            self._pending.remove(job)
            job.future.cancel()
        elif job.task:
            job.task.cancel()

    # ---- halt / resume ----
    def halt(self, exc: BaseException) -> None:
        self.halted = exc
        for j in list(self._pending):
            if not j.future.done():
                j.future.set_exception(exc)
        self._pending.clear()
        for j in list(self._running):
            if j.task and not j.task.done():
                j.task.cancel(msg="halted")

    def resume(self) -> None:
        self.halted = None

    # ---- workers ----
    async def _notify(self) -> None:
        async with self._cond:
            self._cond.notify_all()

    def _next_runnable(self) -> Job | None:
        for j in self._pending:
            if j.group is None or j.group not in self._busy_groups:
                return j
        return None

    async def _worker(self, i: int) -> None:
        while True:
            async with self._cond:
                job = self._next_runnable()
                while job is None:
                    await self._cond.wait()
                    job = self._next_runnable()
                self._pending.remove(job)
                self._running.add(job)
                if job.group:
                    self._busy_groups.add(job.group)
            try:
                tab = self.tabs[i]
                if tab is None or getattr(tab, "is_closed", lambda: False)():
                    tab = self.tabs[i] = await self._tab_factory(i, tab)
                job.task = asyncio.create_task(job.fn(tab))
                try:
                    result = await job.task
                    if not job.future.done():
                        job.future.set_result(result)
                except asyncio.CancelledError:
                    if not job.future.done():
                        if self.halted:
                            job.future.set_exception(self.halted)
                        else:
                            job.future.cancel()
                    if asyncio.current_task().cancelling():   # the worker itself is being stopped
                        raise
                except BaseException as e:
                    if not job.future.done():
                        job.future.set_exception(e)
            except asyncio.CancelledError:
                raise
            except BaseException as e:  # tab factory failed
                if not job.future.done():
                    job.future.set_exception(e)
            finally:
                self._running.discard(job)
                if job.group:
                    self._busy_groups.discard(job.group)
                await self._notify()
