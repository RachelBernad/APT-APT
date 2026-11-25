"""Telegram rendering of Listing Facts and the post layout with a summary (synthetic data)."""
from __future__ import annotations

from datetime import datetime, timezone

from aptbot import fb_engine
from aptbot.bot import formatting
from aptbot.listing_ai import ListingFacts
from aptbot.scrapers.facebook_generic_browser import Post

NULLS = {f: None for f in ListingFacts.model_fields if f not in ("ref", "post_kind")}


def facts(kind="rent", **kw):
    return ListingFacts(ref=0, post_kind=kind, **{**NULLS, **kw})


def post(text="דירה להשכרה"):
    return Post(group_id="g", post_id="1", url="https://fb.test/g/1",
                created_at=datetime(2026, 11, 24, 8, 30, tzinfo=timezone.utc), text=text,
                group_name="Group g")


def test_full_summary_lines():
    s = formatting.format_fb_summary(facts(
        city_area_or_address="ארלוזורוב 83, הצפון הישן", has_accurate_address=True, price_ils=7800,
        arnona_ils=450, is_broker=False, entry_date="immediate", rooms_count=3.0, square_meters=70.0,
        floor=2.0, elevator=False, has_mamad=True, renovated=True, balcony=True, balcony_type="מרפסת שמש"))
    assert s.splitlines() == [
        "🏠 השכרה · ארלוזורוב 83, הצפון הישן 📍",
        "💰 7,800 ₪ · ארנונה 450 · ללא תיווך · כניסה: מיידית",
        "🛏 3 חד׳ · 70 מ״ר · קומה 2",
        "✅ ממ״ד · משופצת · מרפסת שמש  ❌ מעלית",
    ]


def test_nulls_are_hidden_and_half_rooms_kept():
    s = formatting.format_fb_summary(facts("roommate", rooms_count=2.5))
    assert s.splitlines() == ["👥 שותפים", "🛏 2.5 חד׳"]


def test_not_listing_and_seeking_are_one_line():
    assert formatting.format_fb_summary(facts("not_listing")) == "🚫 לא מודעת דירה"
    assert formatting.format_fb_summary(facts("seeking")) == "🔎 מחפשים דירה"


def test_special_floors_dates_broker_fee_and_escaping():
    s = formatting.format_fb_summary(facts(
        floor=0.0, entry_date="2027-01-01", is_broker=True, broker_fee="חודש <שכירות>",
        city_area_or_address="A & B", bills_included=True))
    assert "קומה קרקע" in s and "כניסה: 01/01/2027" in s
    assert "תיווך (חודש &lt;שכירות&gt;)" in s and "A &amp; B" in s and "כולל חשבונות" in s
    assert "קומה מרתף" in formatting.format_fb_summary(facts(floor=-1.0))
    assert "כניסה: 12/2026" in formatting.format_fb_summary(facts(entry_date="2026-12"))


def test_post_layout_summary_between_header_and_text():
    summary = formatting.format_fb_summary(facts(price_ils=5000))
    msg = formatting.format_fb_post(post("הטקסט המקורי"), summary=summary)
    head, rest = msg.split("\n", 1)
    assert head.startswith("📘 <b>Group g</b>")
    assert (rest.index("💰 5,000 ₪") < rest.index("לפוסט בפייסבוק")
            < rest.index(formatting.SUMMARY_SEPARATOR) < rest.index("הטקסט המקורי"))
    assert rest.count("לפוסט בפייסבוק") == 1 and not rest.rstrip().endswith("</a>")
    assert formatting.format_fb_post(post("x")) == formatting.format_fb_post(post("x"), summary=None)


def test_caption_fit_counts_the_summary_and_short_caption_keeps_it():
    summary = formatting.format_fb_summary(facts(price_ils=5000, rooms_count=3.0))
    p = post("א" * 900)
    assert formatting.fb_post_fits_caption(p)
    assert not formatting.fb_post_fits_caption(p, summary + "\n" + "ב" * 200)
    short = formatting.format_fb_short_caption(p, summary)
    assert "💰 5,000 ₪" in short and "א" not in short


FACTS1 = facts(price_ils=1)


async def _primed_monitor(db, monkeypatch, primed=True):
    from aptbot import config
    from aptbot.models import LocationTarget, SavedSearch

    async def no_pace():
        return None
    monkeypatch.setattr(fb_engine, "_pace", no_pace)
    monkeypatch.setattr(config, "ALLOWED_CHAT_IDS", frozenset({1001}))
    await db.upsert_user(1001, "u")
    tlv = LocationTarget(level="city", region_id=3, city_id="5000", display_name="תל אביב יפו")
    sid = await db.add_search(SavedSearch(id=0, chat_id=1001, label="תל אביב"), [tlv])
    if primed:
        await db.mark_fb_primed(sid)
    return sid


def _posts(*kinds_texts):
    return [Post(group_id="g", post_id=str(i), url=f"https://fb.test/{i}",
                 created_at=datetime(2026, 11, 24, 8, i, tzinfo=timezone.utc), text=t)
            for i, t in enumerate(kinds_texts)]


class _Recorder:
    def __init__(self):
        self.sent = []

    async def post(self, chat, p, f):
        self.sent.append(p.text)
        return True

    async def text(self, *a, **k):
        return True


def _analyze_by_text(kind_of):
    async def analyze(posts):
        return {fb_engine.content_hash(p): facts(kind_of[p.text]) for p in posts if kind_of.get(p.text)}
    return analyze


async def test_only_wanted_kinds_are_sent_and_the_rest_marked_seen(db, monkeypatch):
    from aptbot import config
    monkeypatch.setattr(config, "FB_POST_KINDS", frozenset({"rent"}))
    sid = await _primed_monitor(db, monkeypatch)
    posts = _posts("rent-a", "sofa", "room", "unknown")
    kinds = {"rent-a": "rent", "sofa": "not_listing", "room": "roommate"}     # "unknown": no facts
    out = _Recorder()
    s = await db.get_search(sid)
    await fb_engine.route_fb_posts(out.post, out.text, db, s, posts, [], _analyze_by_text(kinds))
    assert out.sent == ["rent-a", "unknown"]          # oldest first; unanalyzed still goes out
    assert len(await db.get_fb_seen(sid)) == 4        # skipped ones are seen: never re-analyzed


async def test_empty_kind_setting_sends_every_kind(db, monkeypatch):
    from aptbot import config
    monkeypatch.setattr(config, "FB_POST_KINDS", frozenset())
    sid = await _primed_monitor(db, monkeypatch)
    out = _Recorder()
    await fb_engine.route_fb_posts(out.post, out.text, db, await db.get_search(sid), _posts("sofa"), [],
                                   _analyze_by_text({"sofa": "not_listing"}))
    assert out.sent == ["sofa"]


async def test_taster_fills_its_cap_with_wanted_kinds(db, monkeypatch):
    from aptbot import config
    monkeypatch.setattr(config, "FB_POST_KINDS", frozenset({"rent"}))
    monkeypatch.setattr(config, "FB_BACKFILL_CAP", 2)
    sid = await _primed_monitor(db, monkeypatch, primed=False)
    posts = _posts("r1", "ad1", "ad2", "r2", "r3")           # newest last
    kinds = {"r1": "rent", "r2": "rent", "r3": "rent", "ad1": "not_listing", "ad2": "sale"}
    out = _Recorder()
    await fb_engine.route_fb_posts(out.post, out.text, db, await db.get_search(sid), posts, [],
                                   _analyze_by_text(kinds))
    assert out.sent == ["r3", "r2"]                          # newest wanted ones, cap 2


async def test_without_ai_the_kind_setting_changes_nothing(db, monkeypatch):
    from aptbot import config
    monkeypatch.setattr(config, "FB_POST_KINDS", frozenset({"rent"}))
    sid = await _primed_monitor(db, monkeypatch)
    out = _Recorder()
    await fb_engine.route_fb_posts(out.post, out.text, db, await db.get_search(sid), _posts("a", "b"), [], None)
    assert out.sent == ["a", "b"]


async def test_route_passes_facts_per_post(db, monkeypatch):
    from aptbot import config
    from aptbot.models import LocationTarget, SavedSearch

    async def no_pace():
        return None
    monkeypatch.setattr(fb_engine, "_pace", no_pace)
    monkeypatch.setattr(config, "ALLOWED_CHAT_IDS", frozenset({1001}))
    await db.upsert_user(1001, "u")
    tlv = LocationTarget(level="city", region_id=3, city_id="5000", display_name="תל אביב יפו")
    sid = await db.add_search(SavedSearch(id=0, chat_id=1001, label="תל אביב"), [tlv])
    await db.mark_fb_primed(sid)

    p1, p2 = post("first"), Post(group_id="g", post_id="2", url=None, created_at=None, text="second")
    seen_inputs, sent = [], []

    async def analyze(posts):
        seen_inputs.extend(p.text for p in posts)
        return {fb_engine.content_hash(p1): FACTS1}

    async def send_post(chat, p, f):
        sent.append((p.text, f))
        return True

    async def send_text(*a, **k):
        return True

    s = await db.get_search(sid)
    n = await fb_engine.route_fb_posts(send_post, send_text, db, s, [p1, p2], [], analyze)
    assert n == 2 and sorted(seen_inputs) == ["first", "second"]
    assert dict(sent) == {"first": FACTS1, "second": None}
    # already delivered -> not analyzed again
    seen_inputs.clear()
    await fb_engine.route_fb_posts(send_post, send_text, db, await db.get_search(sid), [p1, p2], [], analyze)
    assert seen_inputs == []


async def test_make_analyze_never_raises():
    class Boom:
        async def analyze(self, items):
            raise RuntimeError("x")
    assert await fb_engine.make_analyze(Boom())([post()]) == {}
    assert fb_engine.make_analyze(None) is None
