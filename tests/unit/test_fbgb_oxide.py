"""fbgb oxide backend, offline: tap records → events, health snapshot, scroll loop on a fake driver, worker client."""
import asyncio
import json
import sys
import textwrap

import pytest

from aptbot.scrapers.facebook_generic_browser.errors import NotMember, RequestTimeout
from aptbot.scrapers.facebook_generic_browser.health import AccountAtRisk
from aptbot.scrapers.facebook_generic_browser.oxide import jobs
from aptbot.scrapers.facebook_generic_browser.oxide.client import OxideBackend, error_from, worker_env
from aptbot.scrapers.facebook_generic_browser.oxide.events import to_events
from aptbot.scrapers.facebook_generic_browser.oxide.health import check_snapshot


def story(pid, ts, gid="111"):
    return {"__typename": "Story", "post_id": pid, "creation_time": ts, "id": f"S{pid}",
            "message": {"text": f"apartment {pid}"}, "to": {"__typename": "Group", "id": gid, "name": "G"}}


def gql(name, *stories):
    return {"kind": "fetch", "method": "POST", "url": "https://www.facebook.com/api/graphql/", "status": 200,
            "request_body": f"fb_api_req_friendly_name={name}&variables=%7B%22a%22%3A1%7D",
            "body": "for (;;);" + json.dumps({"data": {"node": {"edges": [{"node": s} for s in stories]}}})}


def test_to_events_graphql_document_and_filtering():
    doc = {"kind": "document", "method": "GET", "url": "https://www.facebook.com/groups/111/", "status": 200,
           "body": '<script type="application/json" data-sjs>{"x":1}</script>'}
    other = {"kind": "fetch", "method": "POST", "url": "https://evil.example/api/graphql/", "status": 200, "body": "{}"}
    evs = to_events([doc, gql("GroupsCometFeedRegularStoriesPaginationQuery", story("1", 100)), other])
    assert [e.kind for e in evs] == ["document", "graphql"]
    assert evs[0].chunks == [{"x": 1}]
    assert evs[1].name == "GroupsCometFeedRegularStoriesPaginationQuery" and evs[1].variables == {"a": 1}


@pytest.mark.parametrize("snap", [
    {"url": "https://www.facebook.com/checkpoint/123/"},
    {"url": "https://www.facebook.com/groups/1/", "login_form": True},
    {"url": "https://www.facebook.com/", "dialogs": ["You're Temporarily Blocked"]},
    {"url": "https://www.facebook.com/", "body": "Confirm it's you"},
])
def test_health_snapshot_stops(snap):
    with pytest.raises(AccountAtRisk):
        check_snapshot(snap)


def test_health_snapshot_ok():
    check_snapshot({"url": "https://www.facebook.com/groups/1/", "dialogs": [], "body": None})


class FakeDriver:
    """Feeds one batch of 3 posts per burst, newest first, created at 1000, 990, 980, ..."""

    def __init__(self, n_posts=12, feed=True):
        self.ts = [1000 - 10 * i for i in range(n_posts)]
        self.i, self.bursts, self.health, self.feed = 0, 0, 0, feed

    def _batch(self):
        b = self.ts[self.i:self.i + 3]
        self.i += 3
        return to_events([gql("GroupsCometFeedRegularStoriesPaginationQuery",
                              *(story(str(t), t) for t in b))]) if b else []

    def goto(self, url):
        return self._batch()

    def scroll_burst(self):
        self.bursts += 1

    def pump(self, ms):
        return self._batch()

    def check_health(self):
        self.health += 1

    def count(self, sel):
        return 1 if self.feed else 0


def test_group_feed_stops_at_watermark(monkeypatch):
    monkeypatch.setattr(jobs, "think", lambda *a: None)
    d = FakeDriver(30)
    meta, posts = jobs.group_feed(d, "111", limit=50, since=955)
    assert [p.post_id for p in posts] == ["1000", "990", "980", "970", "960"]
    assert d.bursts <= 5 and d.health >= 1


def test_group_feed_depth_cap():
    d = FakeDriver(90)
    _, posts = jobs.group_feed(d, "111", limit=80, since=None, depth_cap=12)
    assert len(posts) == 12


def test_group_feed_not_member():
    with pytest.raises(NotMember):
        jobs.group_feed(FakeDriver(3, feed=False), "111", limit=5, since=None)


def test_worker_env_drops_debug_and_sets_tuning(monkeypatch):
    monkeypatch.setenv("BO_DEBUG_HEAP", "1")
    env = worker_env("/x/jar.json")
    assert "BO_DEBUG_HEAP" not in env and env["BROWSER_OXIDE_COOKIE_JAR"] == "/x/jar.json"
    assert env["BROWSER_OXIDE_HEAP_MAX_MB"] == "384"


def test_error_mapping():
    assert isinstance(error_from({"error": "account_at_risk", "message": "x"}), AccountAtRisk)
    e = error_from({"error": "not_member", "group_id": "1", "join_state": "REQUESTED"})
    assert isinstance(e, NotMember) and e.join_state == "REQUESTED"


FAKE_WORKER = textwrap.dedent("""
    import json, sys, time
    for line in sys.stdin:
        r = json.loads(line)
        if r["op"] == "shutdown":
            print(json.dumps({"id": r["id"], "ok": True}), flush=True); break
        if r["op"] == "hello":
            print(json.dumps({"id": r["id"], "ok": {"logged_in": True}}), flush=True)
        elif r["op"] == "slow":
            time.sleep(5)
        elif r["op"] == "risk":
            print(json.dumps({"id": r["id"], "error": "account_at_risk", "message": "checkpoint"}), flush=True)
        else:
            print(json.dumps({"id": r["id"], "ok": r["args"]}), flush=True)
""")


def test_client_roundtrip_errors_timeout_and_restart(tmp_path):
    w = tmp_path / "w.py"
    w.write_text(FAKE_WORKER)

    async def run():
        b = OxideBackend(cmd=[sys.executable, str(w)])
        assert await b.call("echo", {"a": 1}) == {"a": 1}
        with pytest.raises(AccountAtRisk):
            await b.call("risk")
        with pytest.raises(RequestTimeout):
            await b.call("slow", timeout=0.5)
        assert not b.running
        assert await b.call("echo", {"b": 2}) == {"b": 2}      # a fresh worker
        await b.close()
        assert not b.running

    asyncio.run(run())


def test_quiet_delay_window():
    from datetime import datetime, timezone
    from aptbot.fb_engine import quiet_delay
    # 02:30 Israel (UTC+2 in winter) = 00:30 UTC → inside 01-07, resumes at 07:00 (+0..15 min jitter)
    d = quiet_delay(datetime(2026, 11, 28, 0, 30, tzinfo=timezone.utc), "01-07")
    assert 4.5 * 3600 <= d <= 4.5 * 3600 + 900
    assert quiet_delay(datetime(2026, 11, 28, 10, 0, tzinfo=timezone.utc), "01-07") == 0
    assert quiet_delay(datetime(2026, 11, 28, 21, 30, tzinfo=timezone.utc), "23-06") > 0     # wraps midnight
    assert quiet_delay(datetime(2026, 11, 28, 0, 30, tzinfo=timezone.utc), "") == 0


def test_fill_albums_appends_missing_photos(monkeypatch):
    from aptbot.scrapers.facebook_generic_browser.models import Photo
    from aptbot.scrapers.facebook_generic_browser.capture import Event
    monkeypatch.setattr(jobs, "think", lambda *a: None)
    _, posts = jobs.group_feed(FakeDriver(3), "111", limit=3, since=None)
    p = posts[0]
    p.photos, p.photo_count = [Photo(id="p1", url="https://scontent.example/1.jpg")], 3
    album = {"data": {"album": {"media": {"edges": [
        {"node": {"__typename": "Photo", "id": f"p{i}", "image": {"uri": f"https://scontent.example/{i}.jpg",
                                                                   "width": 720, "height": 960}}} for i in (1, 2, 3)]}}}}

    class AlbumDriver(FakeDriver):
        def goto(self, url):
            assert "/media/set/?set=pcb." + p.post_id in url
            return [Event("document", url, "", [album])]

    d = AlbumDriver(0)
    assert jobs.fill_albums(d, posts) == 1
    assert [x.id for x in p.photos] == ["p1", "p2", "p3"] and p.photos[2].width == 720
    assert jobs.fill_albums(d, posts) == 0          # already complete: no page load


def test_worker_env_is_an_allowlist_and_errors_are_redacted(monkeypatch):
    from aptbot.scrapers.facebook_generic_browser.oxide.client import redact
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "123:fake")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-fake")
    monkeypatch.setenv("FBGB_HOME", "/h")
    env = worker_env(None)
    assert "TELEGRAM_BOT_TOKEN" not in env and "OPENAI_API_KEY" not in env and env["FBGB_HOME"] == "/h"
    r = redact("Error: GET https://www.facebook.com/x?fb_dtsg=AAA&lsd=BBB failed; cookie: c_user=111")
    assert "AAA" not in r and "BBB" not in r and "111" not in r and "facebook.com/x" in r
    e = error_from({"error": "engine", "message": "xs=secret1 at https://h.example/p?token=t2"})
    assert "secret1" not in str(e) and "t2" not in str(e)


def test_fill_albums_cap_counts_every_attempt(monkeypatch):
    monkeypatch.setattr(jobs, "think", lambda *a: None)
    _, posts = jobs.group_feed(FakeDriver(12), "111", limit=12, since=None)
    for p in posts:
        p.photo_count = 9                                  # every post looks cut short
    loads = []
    monkeypatch.setattr(jobs, "album_photos", lambda d, pid, want: loads.append(pid) or [])
    assert jobs.fill_albums(FakeDriver(0), posts, max_albums=3) == 0
    assert len(loads) == 3                                 # empty albums still count toward the cap
