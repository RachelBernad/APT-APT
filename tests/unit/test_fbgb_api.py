"""facebook_generic_browser.FacebookAPI + store: cache policy, single-flight, new_posts, breaker, filter, stream.

The browser is faked (jobs.* replaced); the dispatcher, cache, breaker and API logic are real (offline).
INVARIANTS (docs/api-reference.md, docs/errors-and-safety.md):
- fresh cache answers without a browser job; filter_posts never runs a job;
- identical concurrent calls → one job; new_posts skips groups whose last_post_time ≤ watermark;
- AccountAtRisk anywhere → Halted for every caller, persisted across restarts until resume();
- stream_posts yields as batches arrive and frees the tab when the caller stops early.
"""
import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from aptbot.scrapers.facebook_generic_browser import api as api_mod
from aptbot.scrapers.facebook_generic_browser import human, jobs
from aptbot.scrapers.facebook_generic_browser.dispatcher import Dispatcher
from aptbot.scrapers.facebook_generic_browser.errors import AccountAtRisk, Halted
from aptbot.scrapers.facebook_generic_browser.models import Group, Hints, Post, State

NOW = datetime(2026, 11, 24, 12, tzinfo=timezone.utc)


def post(gid, pid, minutes_ago, text="דירה", prices=(), rooms=None, photos=0):
    return Post(group_id=gid, post_id=str(pid), url=f"https://www.facebook.com/groups/{gid}/posts/{pid}/",
                created_at=NOW - timedelta(minutes=minutes_ago), text=text, photo_count=photos,
                hints=Hints(prices=list(prices), rooms=rooms))


class FakeTab:
    def __init__(self):
        self.closed = False

    def is_closed(self):
        return self.closed


@pytest.fixture
async def fb(tmp_path, monkeypatch):
    async def nop(*a, **k):
        return None
    monkeypatch.setattr(human, "think", nop)
    monkeypatch.setattr(human, "micro", nop)
    calls = {"feed": 0, "groups": 0}

    async def fake_ensure(self):
        if (self.home / "HALTED").exists():
            raise Halted((self.home / "HALTED").read_text())
        if not self._dispatcher:
            async def factory(i, old):
                return FakeTab()
            self._dispatcher = Dispatcher(self.max_tabs, factory)
            self._dispatcher.start()
            self._env = object()
            self._logged_in = True
        return self._dispatcher

    monkeypatch.setattr(api_mod.FacebookAPI, "_ensure_browser", fake_ensure)
    inst = api_mod.FacebookAPI(home=tmp_path, max_tabs=3, idle_close_after=None)
    inst.calls = calls
    async with inst:
        yield inst


def fake_feed(monkeypatch, fb, data: dict[str, list[Post]], delay=0.02, risk_for=None):
    async def group_feed(env, page, group_id, *, limit, since, sink=None):
        fb.calls["feed"] += 1
        await asyncio.sleep(delay)
        if group_id == risk_for:
            raise AccountAtRisk("redirected to /checkpoint/")
        posts = [p for p in data.get(group_id, []) if since is None or p.created_at.timestamp() > since][:limit]
        for i in range(0, len(posts), 3):          # 3 per "pagination", like Facebook
            if sink:
                await sink(posts[i:i + 3])
            await asyncio.sleep(delay)
        return Group(id=group_id, name=f"g{group_id}", url="", join_state="MEMBER"), posts
    monkeypatch.setattr(jobs, "group_feed", group_feed)


def fake_groups(monkeypatch, fb, groups):
    async def list_groups(env, page):
        fb.calls["groups"] += 1
        return [Group(**g.__dict__) for g in groups]
    monkeypatch.setattr(jobs, "list_groups", list_groups)


async def test_latest_posts_cache_and_single_flight(fb, monkeypatch):
    fake_feed(monkeypatch, fb, {"1": [post("1", i, i) for i in range(1, 7)]})
    a, b = await asyncio.gather(fb.latest_posts("1", limit=5), fb.latest_posts("1", limit=5))
    assert fb.calls["feed"] == 1 and [p.post_id for p in a] == [p.post_id for p in b]
    again = await fb.latest_posts("1", limit=5)               # fresh (max_age=60) → cache
    assert fb.calls["feed"] == 1 and len(again) == 5
    await fb.latest_posts("1", limit=5, max_age=0)            # forced refresh
    assert fb.calls["feed"] == 2


async def test_different_groups_run_in_parallel(fb, monkeypatch):
    fake_feed(monkeypatch, fb, {g: [post(g, 1, 1)] for g in "123"}, delay=0.1)
    t0 = asyncio.get_running_loop().time()
    await asyncio.gather(*(fb.latest_posts(g, max_age=0) for g in "123"))
    assert asyncio.get_running_loop().time() - t0 < 0.25       # 3 tabs → ~1x, not 3x
    assert fb.calls["feed"] == 3


async def test_new_posts_uses_last_post_time_and_watermarks(fb, monkeypatch):
    data = {"1": [post("1", i, i) for i in range(1, 15)], "2": [post("2", 100, 30)]}
    fake_feed(monkeypatch, fb, data)
    groups = [Group("1", "a", "", last_post_time=NOW - timedelta(minutes=1), join_state="MEMBER"),
              Group("2", "b", "", last_post_time=NOW - timedelta(minutes=30), join_state="MEMBER"),
              Group("3", "pending", "", join_state="REQUESTED")]
    fake_groups(monkeypatch, fb, groups)

    first = await fb.new_posts(first_run_limit=10)
    assert len([p for p in first if p.group_id == "1"]) == 10 and fb.calls["feed"] == 2   # pending group skipped
    fb.calls["feed"] = 0
    again = await fb.new_posts()                              # nothing moved → no group opened
    assert again == [] and fb.calls["feed"] == 0

    data["1"].insert(0, post("1", 999, 0))                    # a new post in group 1 only
    groups[0].last_post_time = NOW
    newer = await fb.new_posts()
    assert [p.post_id for p in newer] == ["999"] and fb.calls["feed"] == 1


async def test_account_at_risk_halts_everyone_and_persists(fb, monkeypatch, tmp_path):
    fake_feed(monkeypatch, fb, {"ok": [post("ok", 1, 1)]}, delay=0.1, risk_for="bad")
    results = await asyncio.gather(fb.latest_posts("bad", max_age=0), fb.latest_posts("ok", max_age=0),
                                   return_exceptions=True)
    assert isinstance(results[0], Halted)
    assert isinstance(results[1], (Halted, list))              # may have finished before the halt
    st = await fb.status()
    assert st.state == State.HALTED and "checkpoint" in st.halt_reason
    with pytest.raises(Halted):
        await fb.latest_posts("ok", max_age=0)
    restarted = api_mod.FacebookAPI(home=tmp_path, idle_close_after=None)
    async with restarted:
        assert (await restarted.status()).state == State.HALTED   # survives a restart
        await restarted.resume()
        assert (await restarted.status()).state != State.HALTED


async def test_filter_posts_is_cache_only(fb, monkeypatch):
    fake_feed(monkeypatch, fb, {"1": [
        post("1", 1, 1, "דירת 3 חדרים בפלורנטין", prices=[5200], rooms=3, photos=4),
        post("1", 2, 2, "חדר בדירת שותפים", prices=[2800], rooms=None),
        post("1", 3, 3, "4 חדרים עם מעלית, תיווך", prices=[9000], rooms=4, photos=2),
    ]})
    await fb.latest_posts("1")
    fb.calls["feed"] = 0
    assert [p.post_id for p in await fb.filter_posts(max_price=6000, min_rooms=3)] == ["1"]
    assert [p.post_id for p in await fb.filter_posts(include=["חדרים"], exclude=["תיווך"])] == ["1"]
    assert {p.post_id for p in await fb.filter_posts(has_photos=True)} == {"1", "3"}
    assert fb.calls["feed"] == 0


async def test_stream_posts_yields_incrementally_and_stops_early(fb, monkeypatch):
    fake_feed(monkeypatch, fb, {"1": [post("1", i, i) for i in range(1, 10)]}, delay=0.05)
    got = []
    async for p in fb.stream_posts("1", limit=9):
        got.append(p.post_id)
        if len(got) == 4:
            break
    assert len(got) == 4
    await asyncio.sleep(0.05)
    assert fb._dispatcher.busy == 0                            # tab freed after the early break
    cached = await fb.store.posts(["1"])
    assert len(cached) >= 3                                    # streamed batches were cached as they came


async def test_store_detects_edits(tmp_path):
    from aptbot.scrapers.facebook_generic_browser.store import Store
    s = await Store(tmp_path / "x.db").open()
    p = post("1", 1, 1, "v1")
    new, edited = await s.upsert_posts([p])
    assert len(new) == 1 and not edited
    p.text = "v2"
    new, edited = await s.upsert_posts([p])
    assert not new and len(edited) == 1
    await s.set_watermark("1", 100)
    await s.set_watermark("1", 50)                             # never moves backwards
    assert await s.watermark("1") == 100
    await s.close()


async def test_new_posts_commit_false_returns_the_same_posts_until_committed(fb, monkeypatch):
    data = {"1": [post("1", 5, 5)]}
    fake_feed(monkeypatch, fb, data)
    groups = [Group("1", "a", "", last_post_time=NOW, join_state="MEMBER")]
    fake_groups(monkeypatch, fb, groups)
    first = await fb.new_posts(commit=False)
    assert [p.post_id for p in first] == ["5"]
    again = await fb.new_posts(commit=False)                  # not delivered → same posts again
    assert [p.post_id for p in again] == ["5"]
    await fb.commit_watermarks()
    assert await fb.new_posts() == []
