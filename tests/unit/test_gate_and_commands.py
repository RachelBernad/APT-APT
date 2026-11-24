"""Chat allowlist gate, the minimal commands, and Facebook post formatting. Fake ids only."""
from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from telegram.ext import ApplicationHandlerStop

from aptbot.bot import formatting
from aptbot.bot.gate import ChatGate
from aptbot.bot.simple import SimpleCommands
from aptbot.scrapers.facebook_generic_browser import Photo, Post
from aptbot.scrapers.yad2 import locations

ALLOWED = -1000000001


def _update(chat_id, chat_type="group", title="קבוצת בדיקה", migrate_to=None):
    replies = []

    async def reply_text(text, **kw):
        replies.append(text)

    msg = SimpleNamespace(migrate_to_chat_id=migrate_to, reply_text=reply_text)
    chat = SimpleNamespace(id=chat_id, type=chat_type, title=title, username=None, first_name=None)
    user = SimpleNamespace(username="alice_test")
    return SimpleNamespace(effective_chat=chat, effective_message=msg, effective_user=user), replies


# --- gate ------------------------------------------------------------------

async def test_gate_lets_the_allowed_chat_through():
    gate = ChatGate({ALLOWED})
    upd, _ = _update(ALLOWED)
    assert await gate(upd, None) is None


async def test_gate_stops_any_other_chat():
    gate = ChatGate({ALLOWED})
    upd, _ = _update(222222222, chat_type="private")
    with pytest.raises(ApplicationHandlerStop):
        await gate(upd, None)


async def test_empty_allowlist_blocks_everyone():
    gate = ChatGate(set())
    upd, _ = _update(ALLOWED)
    with pytest.raises(ApplicationHandlerStop):
        await gate(upd, None)


async def test_gate_follows_a_supergroup_migration():
    gate = ChatGate({ALLOWED})
    upd, _ = _update(ALLOWED, migrate_to=-1009999999999)
    await gate(upd, None)
    assert gate.is_allowed(-1009999999999)


# --- commands --------------------------------------------------------------

def test_city_target_for_tel_aviv_comes_from_the_catalog():
    t = locations.city_target("5000")
    assert t.level == "city" and t.city_id == "5000" and t.region_id > 0 and t.display_name


async def test_monitor_creates_one_tel_aviv_search_then_reports_already_on(db):
    started = []

    async def on_started(sid):
        started.append(sid)

    cmds = SimpleCommands(db, on_started=on_started)
    upd, replies = _update(ALLOWED)
    await cmds.monitor(upd, None)
    await cmds.monitor(upd, None)
    searches = await db.list_searches(ALLOWED)
    assert len(searches) == 1
    s = searches[0]
    assert s.is_active and s.locations[0].city_id == "5000" and s.min_price is None
    assert "Already watching" in replies[-1]
    import asyncio
    await asyncio.sleep(0)
    assert started == [s.id]


async def test_stop_then_monitor_resumes_and_reprimes(db):
    async def on_started(sid):
        return None

    cmds = SimpleCommands(db, on_started=on_started)
    upd, replies = _update(ALLOWED)
    await cmds.monitor(upd, None)
    sid = (await db.list_searches(ALLOWED))[0].id
    await db.mark_primed(sid)
    await db.mark_fb_primed(sid)
    await cmds.stop(upd, None)
    assert (await db.get_search(sid)).is_active is False
    await cmds.monitor(upd, None)
    s = await db.get_search(sid)
    assert s.is_active and not s.is_primed and not s.fb_primed
    assert len(await db.list_searches(ALLOWED)) == 1


# --- db --------------------------------------------------------------------

async def test_fb_seen_round_trip_and_reprime_resets_fb_flag(db):
    await db.upsert_user(1, "u")
    from aptbot.models import SavedSearch
    sid = await db.add_search(SavedSearch(id=0, chat_id=1, label="x"), [])
    await db.add_fb_seen(sid, [("h1", "https://fb.test/1"), ("h1", None), ("h2", None)])
    assert await db.get_fb_seen(sid) == {"h1", "h2"}
    await db.mark_fb_primed(sid)
    assert (await db.get_search(sid)).fb_primed is True
    await db.reprime_user_searches(1)
    assert (await db.get_search(sid)).fb_primed is False


# --- formatting --------------------------------------------------------------

def _post(text, photos=0):
    return Post(group_id="g", post_id="p", url="https://fb.test/p?a=1&b=2",
                created_at=datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc), text=text,
                group_name="דירות <בדיקה>",
                photos=[Photo(id=str(i), url=f"https://cdn.test/{i}") for i in range(photos)])


def test_fb_post_has_group_israel_time_text_and_escaped_link():
    out = formatting.format_fb_post(_post("  3 חדרים & מרפסת \n"))
    assert "דירות &lt;בדיקה&gt;" in out
    assert "01/01 12:00" in out                    # UTC 10:00 = Israel 12:00 in winter
    assert "3 חדרים &amp; מרפסת" in out and not out.endswith("\n")
    assert "a=1&amp;b=2" in out


def test_long_post_is_cut_to_the_limit_without_breaking_entities():
    out = formatting.format_fb_post(_post("&" * 5000))
    assert len(out) <= formatting.MESSAGE_LIMIT
    assert "&amp" in out and "&am…" not in out and out.count("…") == 1
    assert not formatting.fb_post_fits_caption(_post("x" * 2000))
    assert formatting.fb_post_fits_caption(_post("short"))
    short = formatting.format_fb_short_caption(_post("x" * 2000))
    assert len(short) < formatting.CAPTION_LIMIT and "fb.test" in short


# --- Yad2 area list --------------------------------------------------------

def test_default_yad2_hoods_all_exist_in_the_catalog():
    from aptbot import config
    targets = locations.monitor_targets("5000", config.YAD2_HOODS)
    assert len(targets) == len(config.YAD2_HOODS) > 0
    assert all(t.level == "hood" and t.city_id == "5000" and t.match_name for t in targets)


def test_old_north_uses_the_listing_names_not_the_autocomplete_ones():
    # Listings say "הצפון הישן - החלק הצפוני"; the short autocomplete name matched 0 of them.
    names = {h for q in locations.city_quarters("5000") for h in q["hoods"]}
    assert {"הצפון הישן - החלק הצפוני", "הצפון הישן - החלק הדרומי"} <= names
    assert "הצפון הישן - צפון" not in names


def test_unknown_hood_name_is_an_error_not_silence():
    with pytest.raises(KeyError):
        locations.monitor_targets("5000", ["אין שכונה כזאת"])


def test_empty_hood_list_means_the_whole_city():
    (t,) = locations.monitor_targets("5000", [])
    assert t.level == "city"


async def test_monitor_on_an_active_search_applies_changed_areas(db, monkeypatch):
    from aptbot import config

    async def on_started(sid):
        return None

    cmds = SimpleCommands(db, on_started=on_started)
    upd, replies = _update(ALLOWED)
    monkeypatch.setattr(config, "YAD2_HOODS", ["נווה צדק"])
    await cmds.monitor(upd, None)
    monkeypatch.setattr(config, "YAD2_HOODS", ["נווה צדק", "כרם התימנים"])
    await cmds.monitor(upd, None)
    (s,) = await db.list_searches(ALLOWED)
    assert {t.display_name.split(",")[0] for t in s.locations} == {"נווה צדק", "כרם התימנים"}
    assert "areas updated" in replies[-1]


async def test_startup_sync_moves_an_old_whole_city_monitor_to_the_areas(db, monkeypatch):
    from aptbot import config
    from aptbot.models import SavedSearch
    await db.upsert_user(ALLOWED, "u")
    sid = await db.add_search(SavedSearch(id=0, chat_id=ALLOWED, label="x"),
                              [locations.city_target("5000")])
    monkeypatch.setattr(config, "YAD2_HOODS", ["נווה צדק"])

    async def on_started(sid):
        return None

    cmds = SimpleCommands(db, on_started=on_started)
    assert await cmds.sync_areas() == 1
    assert [t.level for t in (await db.get_search(sid)).locations] == ["hood"]
    assert await cmds.sync_areas() == 0
