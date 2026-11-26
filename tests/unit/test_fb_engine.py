"""Facebook source: content-hash dedup, priming/taster, only-new, safety stops.

Offline: a fake FacebookAPI stands in for the browser library. Chat ids are fake.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from aptbot import config, fb_engine
from aptbot.models import LocationTarget, SavedSearch
from aptbot.scrapers.facebook_generic_browser import Halted, NotLoggedIn, Photo, Post

CHAT = 1001
T0 = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)


def post(pid, text, minutes=0, group="g1", photos=()):
    return Post(group_id=group, post_id=pid, url=f"https://fb.test/{group}/{pid}",
                created_at=T0 + timedelta(minutes=minutes), text=text, group_name=f"Group {group}",
                photos=[Photo(id=i, url=f"https://cdn.test/{i}.jpg") for i in photos],
                photo_count=len(photos))


class FakeFB:
    def __init__(self, polls=(), cached=(), exc=None):
        self.polls = list(polls)       # one list of posts per new_posts() call
        self.cached = list(cached)
        self.exc = exc
        self.new_calls = 0
        self.filter_calls = 0

    async def new_posts(self, group_ids=None, **kw):
        self.new_calls += 1
        if self.exc:
            raise self.exc
        return self.polls.pop(0) if self.polls else []

    async def filter_posts(self, **kw):
        self.filter_calls += 1
        return list(self.cached)


class Sent:
    def __init__(self, fail=False):
        self.posts, self.texts, self.fail = [], [], fail

    async def post(self, chat_id, p, facts=None, search=None):
        self.posts.append((chat_id, p.post_id))
        self.facts = getattr(self, "facts", []) + [facts]
        return not self.fail

    async def text(self, chat_id, text, photo=None):
        self.texts.append((chat_id, text))
        return True


@pytest.fixture(autouse=True)
def _fast(monkeypatch):
    async def no_pace():
        return None
    monkeypatch.setattr(fb_engine, "_pace", no_pace)
    monkeypatch.setattr(config, "ALLOWED_CHAT_IDS", frozenset({CHAT}))
    monkeypatch.setattr(config, "FB_BACKFILL_CAP", 2)


async def _monitor(db, chat=CHAT):
    await db.upsert_user(chat, "u")
    tlv = LocationTarget(level="city", region_id=3, city_id="5000", display_name="תל אביב יפו")
    return await db.add_search(SavedSearch(id=0, chat_id=chat, label="תל אביב"), [tlv])


# --- content hash ----------------------------------------------------------

def test_hash_ignores_leading_and_trailing_whitespace():
    a = post("1", "דירה 3 חדרים\nבצפון")
    b = post("2", "  \n דירה 3 חדרים\nבצפון \n\t", group="g2")
    assert fb_engine.content_hash(a) == fb_engine.content_hash(b)


def test_hash_keeps_inner_differences():
    assert fb_engine.content_hash(post("1", "a b")) != fb_engine.content_hash(post("2", "a  b"))


def test_hash_photo_only_and_empty_posts():
    p1 = post("1", "  ", photos=("b", "a"))
    p2 = post("2", "", photos=("a", "b"))
    assert fb_engine.content_hash(p1) == fb_engine.content_hash(p2)
    assert fb_engine.content_hash(post("9", "")) == "post:g1:9"


def test_unique_by_content_keeps_newest_copy():
    old, new = post("1", "same", minutes=0), post("2", "same", minutes=5, group="g2")
    assert [p.post_id for p in fb_engine.unique_by_content([old, new])] == ["2"]


# --- routing ---------------------------------------------------------------

async def test_first_poll_primes_and_sends_capped_taster(db):
    sid = await _monitor(db)
    fb = FakeFB(polls=[[post("1", "a", 1), post("2", "b", 2)]],
                cached=[post("3", "c", 3), post("2", "b", 2)])
    out = Sent()
    await fb_engine.run_fb_cycle(fb, db, out.post, out.text, fb_engine.FbState())
    assert [pid for _, pid in out.posts] == ["3", "2"]          # newest first, cap 2
    assert len(out.texts) == 1 and "newest" in out.texts[0][1]      # the intro
    s = await db.get_search(sid)
    assert s.fb_primed is True
    assert len(await db.get_fb_seen(sid)) == 3                   # everything recorded


async def test_after_priming_only_new_content_is_sent_oldest_first(db):
    await _monitor(db)
    fb = FakeFB(polls=[[post("1", "a")],
                       [post("1", "a"), post("5", "e", 9), post("4", "d", 8)]])
    out = Sent()
    state = fb_engine.FbState()
    await fb_engine.run_fb_cycle(fb, db, out.post, out.text, state)
    out.posts.clear()
    await fb_engine.run_fb_cycle(fb, db, out.post, out.text, state)
    assert [pid for _, pid in out.posts] == ["4", "5"]
    assert state.last_new == 3 and state.last_sent == 2


async def test_cross_posted_duplicate_is_sent_once(db):
    await _monitor(db)
    fb = FakeFB(polls=[[], [post("1", "same apt", 1, "g1"), post("2", "\nsame apt  ", 2, "g2")],
                       [post("3", "same apt", 3, "g3")]])
    out = Sent()
    state = fb_engine.FbState()
    for _ in range(3):
        await fb_engine.run_fb_cycle(fb, db, out.post, out.text, state)
    assert len(out.posts) == 1


async def test_failed_send_is_retried_next_poll(db):
    sid = await _monitor(db)
    await db.mark_fb_primed(sid)
    p = post("1", "a")
    fb = FakeFB(polls=[[p], [p]])
    state = fb_engine.FbState()
    bad = Sent(fail=True)
    await fb_engine.run_fb_cycle(fb, db, bad.post, bad.text, state)
    assert await db.get_fb_seen(sid) == set()
    good = Sent()
    await fb_engine.run_fb_cycle(fb, db, good.post, good.text, state)
    assert len(good.posts) == 1 and len(await db.get_fb_seen(sid)) == 1


async def test_reprime_sends_a_new_taster_without_repeats(db):
    sid = await _monitor(db)
    fb = FakeFB(polls=[[post("1", "a", 1)], []], cached=[post("1", "a", 1), post("2", "b", 2)])
    out = Sent()
    state = fb_engine.FbState()
    await fb_engine.run_fb_cycle(fb, db, out.post, out.text, state)
    await db.reprime_search(sid)
    out.posts.clear()
    await fb_engine.run_fb_cycle(fb, db, out.post, out.text, state)
    assert out.posts == []        # everything in the pool was already delivered or recorded


async def test_no_active_allowed_monitor_means_no_facebook_traffic(db, monkeypatch):
    await _monitor(db, chat=2002)                # a monitor, but not in the allowlist
    fb = FakeFB(polls=[[post("1", "a")]])
    out = Sent()
    await fb_engine.run_fb_cycle(fb, db, out.post, out.text, fb_engine.FbState())
    assert fb.new_calls == 0 and out.posts == []


async def test_paused_monitor_gets_nothing(db):
    sid = await _monitor(db)
    await db.set_search_active(sid, False)
    fb = FakeFB(polls=[[post("1", "a")]])
    out = Sent()
    await fb_engine.run_fb_cycle(fb, db, out.post, out.text, fb_engine.FbState())
    assert fb.new_calls == 0


# --- account safety --------------------------------------------------------

@pytest.mark.parametrize("exc", [Halted("checkpoint page"), NotLoggedIn("no session")])
async def test_safety_stop_notifies_once_and_stops_the_loop(db, exc):
    await _monitor(db)
    fb = FakeFB(exc=exc)
    out = Sent()
    state = fb_engine.FbState()
    await fb_engine.fb_loop(fb, db, out.post, out.text, state, first_delay=0)   # returns on stop
    assert state.stopped_reason
    assert fb.new_calls == 1
    assert len(out.texts) == 1 and "stopped" in out.texts[0][1]

