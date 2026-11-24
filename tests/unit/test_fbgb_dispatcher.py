"""facebook_generic_browser.dispatcher + tabs.TabPool: concurrency guarantees, with fake tabs (offline).

INVARIANTS (docs/concurrency.md):
- at most n_workers jobs run at once; tabs open lazily;
- identical keys share one job; same-group jobs never overlap;
- attention (foreground) is never held by two tabs; a switch brings the tab to the front first;
- the limiter is shared; halt fails queued + running jobs and rejects new ones; cancellation frees the tab.
"""
import asyncio

import pytest

from aptbot.scrapers.facebook_generic_browser import human, tabs
from aptbot.scrapers.facebook_generic_browser.dispatcher import Dispatcher, Priority
from aptbot.scrapers.facebook_generic_browser.errors import BudgetExhausted, Halted, RequestTimeout


@pytest.fixture(autouse=True)
def fast_human(monkeypatch):
    async def nop(*a, **k):
        return None
    monkeypatch.setattr(human, "think", nop)
    monkeypatch.setattr(human, "micro", nop)


class FakeTab:
    def __init__(self, i):
        self.i, self.closed, self.fronted = i, False, 0

    async def bring_to_front(self):
        self.fronted += 1

    def is_closed(self):
        return self.closed


def make(n=3):
    opened = []

    async def factory(i, old):
        t = FakeTab(i)
        opened.append(t)
        return t
    d = Dispatcher(n, factory)
    d.start()
    return d, opened


async def test_parallelism_is_bounded_and_tabs_are_lazy():
    d, opened = make(3)
    running = peak = 0

    async def job(tab):
        nonlocal running, peak
        running += 1
        peak = max(peak, running)
        await asyncio.sleep(0.02)
        running -= 1
        return tab.i

    one = await d.run(job)
    assert len(opened) == 1 and one == 0          # light load → one tab
    res = await asyncio.gather(*(d.run(job) for _ in range(10)))
    assert peak == 3 and len(opened) == 3 and set(res) <= {0, 1, 2}
    await d.stop()


async def test_single_flight_shares_one_job():
    d, _ = make(3)
    calls = 0

    async def job(tab):
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.02)
        return "posts"

    res = await asyncio.gather(*(d.run(job, key="latest:g1") for _ in range(5)))
    assert res == ["posts"] * 5 and calls == 1
    await d.stop()


async def test_same_group_jobs_never_overlap_but_other_groups_do():
    d, _ = make(3)
    active: dict[str, int] = {}
    overlap = {"g1": 0, "max_total": 0}

    def job_for(g):
        async def job(tab):
            active[g] = active.get(g, 0) + 1
            overlap[g] = max(overlap.get(g, 0), active[g])
            overlap["max_total"] = max(overlap["max_total"], sum(active.values()))
            await asyncio.sleep(0.02)
            active[g] -= 1
        return job

    await asyncio.gather(*(d.run(job_for("g1"), group="g1") for _ in range(3)),
                         d.run(job_for("g2"), group="g2"))
    assert overlap["g1"] == 1 and overlap["max_total"] >= 2
    await d.stop()


async def test_priority_order():
    d, _ = make(1)
    order = []
    gate = asyncio.Event()

    async def blocker(tab):
        await gate.wait()

    def rec(name):
        async def job(tab):
            order.append(name)
        return job

    b = asyncio.create_task(d.run(blocker))
    await asyncio.sleep(0.01)
    tasks = [asyncio.create_task(d.run(rec("bg"), priority=Priority.BACKGROUND)),
             asyncio.create_task(d.run(rec("poll"), priority=Priority.POLL)),
             asyncio.create_task(d.run(rec("now"), priority=Priority.INTERACTIVE))]
    await asyncio.sleep(0.01)
    gate.set()
    await asyncio.gather(b, *tasks)
    assert order == ["now", "poll", "bg"]
    await d.stop()


async def test_timeout_cancels_job_and_frees_tab():
    d, _ = make(1)
    cancelled = asyncio.Event()

    async def slow(tab):
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    with pytest.raises(RequestTimeout):
        await d.run(slow, timeout=0.05)
    await asyncio.wait_for(cancelled.wait(), 1)

    async def quick(tab):
        return "ok"
    assert await asyncio.wait_for(d.run(quick), 1) == "ok"   # the single tab is free again
    await d.stop()


async def test_halt_fails_everything_and_rejects_new():
    d, _ = make(1)

    async def slow(tab):
        await asyncio.sleep(10)

    running = asyncio.create_task(d.run(slow))
    queued = asyncio.create_task(d.run(slow))
    await asyncio.sleep(0.02)
    d.halt(Halted("checkpoint"))
    for t in (running, queued):
        with pytest.raises(Halted):
            await asyncio.wait_for(t, 1)
    with pytest.raises(Halted):
        await d.run(slow)
    d.resume()

    async def quick(tab):
        return 1
    assert await asyncio.wait_for(d.run(quick), 1) == 1
    await d.stop()


async def test_crashed_tab_is_recreated():
    d, opened = make(1)

    async def crash(tab):
        tab.closed = True
        raise RuntimeError("target closed")

    with pytest.raises(RuntimeError):
        await d.run(crash)

    async def ok(tab):
        return tab
    tab = await d.run(ok)
    assert len(opened) == 2 and tab is opened[1]
    await d.stop()


async def test_attention_is_exclusive_and_switches_bring_to_front():
    pool = tabs.TabPool(ctx=None, max_tabs=3)
    a, b = FakeTab(0), FakeTab(1)
    holders = []

    async def burst(t):
        for _ in range(5):
            async with pool.foreground(t):
                holders.append(pool.holder)
                assert pool.holder == id(t) and pool.front == id(t)
                await asyncio.sleep(0.001)
            await asyncio.sleep(0.001)   # "network wait" outside the lock

    await asyncio.gather(burst(a), burst(b))
    assert len(holders) == 10 and pool.holder is None
    assert a.fronted + b.fronted == pool.switches >= 2


async def test_limiter_is_shared_and_hourly_cap_raises():
    lim = tabs.Limiter(rate_per_min=6000, burst=2, hourly_cap=3)
    await asyncio.gather(lim.take(), lim.take(), lim.take())
    assert lim.left() == 0
    with pytest.raises(BudgetExhausted) as e:
        await lim.take()
    assert 0 < e.value.retry_after <= 3600
