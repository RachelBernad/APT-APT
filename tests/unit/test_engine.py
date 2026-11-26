"""engine.py: grouping, union filters, local matching, scrape_group, and the
routing state-machine (capped backfill / only-new / price-change / prime-race).

Regression guards for real invariants:
  #5 backfill: unprimed sends <= BACKFILL_CAP newest, records ALL as seen, primes.
"""

from aptbot import config
from aptbot import engine
from aptbot.scrapers.yad2 import gateway as yad2_gateway
from aptbot.models import GatewayFilters, LocationSignature, LocationTarget, SavedSearch
from tests.conftest import FakeSender, make_listing


# --- builders -------------------------------------------------------------

def _search(sid=1, chat_id=100, city_id="5000", level="city", **kw):
    loc = LocationTarget(level=level, region_id=int(kw.pop("region_id", 3)),
                         city_id=city_id, area_id=kw.pop("area_id", None),
                         match_name=kw.pop("match_name", ""),
                         hood_id=kw.pop("hood_id", None))
    return SavedSearch(id=sid, chat_id=chat_id, label=kw.pop("label", f"s{sid}"),
                       locations=[loc], **kw)


# --- scrape_sig -----------------------------------------------------------

def test_scrape_sig_prefers_most_specific_scope():
    assert engine.scrape_sig(LocationTarget("city", 3, city_id="5000")) == \
        LocationSignature(3, None, "5000")
    assert engine.scrape_sig(LocationTarget("area", 3, area_id=42)) == \
        LocationSignature(3, 42, None)
    assert engine.scrape_sig(LocationTarget("region", 3)) == \
        LocationSignature(3, None, None)
    # a hood target still scrapes at its city scope (hood is matched locally)
    assert engine.scrape_sig(LocationTarget("hood", 3, city_id="5000", match_name="x")) == \
        LocationSignature(3, None, "5000")


# --- group_searches -------------------------------------------------------

def test_group_searches_groups_by_scope_and_unions_features():
    s1 = _search(sid=1, city_id="5000")
    s2 = _search(sid=2, city_id="5000", features=["mamad"])
    s3 = _search(sid=3, city_id="5000", features=["elevator"])
    s4 = _search(sid=4, city_id="7000")
    groups = engine.group_searches([s1, s2, s3, s4])
    # features don't split a scope (they only tag); the key carries their union to tag
    tlv = groups[(LocationSignature(3, None, "5000"), frozenset({"mamad", "elevator"}))]
    assert {s.id for s, _ in tlv} == {1, 2, 3}
    assert {s.id for s, _ in groups[(LocationSignature(3, None, "7000"), frozenset())]} == {4}


def test_group_searches_merges_targets_of_one_search():
    loc_a = LocationTarget("city", 3, city_id="5000")
    loc_b = LocationTarget("hood", 3, city_id="5000", match_name="florentin")
    s = SavedSearch(id=1, chat_id=100, label="s", locations=[loc_a, loc_b])
    groups = engine.group_searches([s])
    (members,) = groups.values()
    (member,) = members
    assert member[0].id == 1 and len(member[1]) == 2   # both targets in one member


# --- union_filters --------------------------------------------------------

def test_union_filters_loosest_bounds():
    s1 = _search(sid=1, min_price=4000, max_price=8000, min_rooms=3)
    s2 = _search(sid=2, min_price=5000, max_price=9000, min_rooms=2)
    members = [(s1, s1.locations), (s2, s2.locations)]
    f = engine.union_filters(members, frozenset())
    assert f.min_price == 4000 and f.max_price == 9000   # widest bounds
    assert f.min_rooms == 2                                # loosest min


def test_union_filters_none_makes_bound_open():
    s1 = _search(sid=1, max_price=8000)
    s2 = _search(sid=2, max_price=None)     # one watcher has no cap -> group has none
    members = [(s1, s1.locations), (s2, s2.locations)]
    f = engine.union_filters(members, frozenset())
    assert f.max_price is None


def test_union_filters_property_ids_only_when_all_constrain():
    s1 = _search(sid=1, property_types=[1])
    s2 = _search(sid=2, property_types=[6])
    members = [(s1, s1.locations), (s2, s2.locations)]
    assert engine.union_filters(members, frozenset()).property_ids == [1, 6]
    # if any watcher wants all types, the group must not constrain
    s3 = _search(sid=3, property_types=None)
    members2 = [(s1, s1.locations), (s3, s3.locations)]
    assert engine.union_filters(members2, frozenset()).property_ids is None


def test_union_filters_carries_group_features():
    s1 = _search(sid=1, features=["elevator"])
    f = engine.union_filters([(s1, s1.locations)], frozenset({"elevator"}))
    assert f.features == ["elevator"]
    # several features come out sorted (stable request order)
    f2 = engine.union_filters([(s1, s1.locations)], frozenset({"mamad", "elevator"}))
    assert f2.features == ["elevator", "mamad"]


# --- matching -------------------------------------------------------------

def test_matches_location_region_area_city_always_true():
    listing = make_listing("yad2:1", hood="פלורנטין")
    assert engine.matches_location(listing, [LocationTarget("city", 3, city_id="5000")])


def test_matches_location_hood_by_normalized_name():
    listing = make_listing("yad2:1", hood="פלורנטין")
    yes = [LocationTarget("hood", 3, city_id="5000", match_name="פלורנטין")]
    no = [LocationTarget("hood", 3, city_id="5000", match_name="הצפוןהישן")]
    assert engine.matches_location(listing, yes)
    assert not engine.matches_location(listing, no)


def test_matches_non_amenity_price_and_rooms():
    s = _search(min_price=5000, max_price=8000, min_rooms=3, max_rooms=4)
    assert engine.matches_non_amenity(make_listing("yad2:1", price=6000, rooms="3"), s)
    assert not engine.matches_non_amenity(make_listing("yad2:2", price=9000, rooms="3"), s)
    assert not engine.matches_non_amenity(make_listing("yad2:3", price=6000, rooms="2"), s)
    # missing price / rooms -> KEPT (a listing is never dropped for a field it doesn't report)
    assert engine.matches_non_amenity(make_listing("yad2:4", price=None, rooms="3"), s)
    assert engine.matches_non_amenity(make_listing("yad2:5", price=6000, rooms=""), s)
    # max-only and min-only bounds
    assert not engine.matches_non_amenity(make_listing("yad2:6", price=8001), _search(max_price=8000))
    assert engine.matches_non_amenity(make_listing("yad2:7", price=100), _search(max_price=8000))


def test_matches_non_amenity_size_floor_only_when_reported():
    s = _search(min_sqm=60, min_floor=2)
    # size/floor unknown -> NOT rejected
    assert engine.matches_non_amenity(make_listing("r:1", size="", floor=""), s)
    # size/floor reported and below threshold -> rejected
    assert not engine.matches_non_amenity(make_listing("y:1", size="50", floor="3"), s)
    assert not engine.matches_non_amenity(make_listing("y:2", size="70", floor="1"), s)


def test_matches_non_amenity_property_type_and_condition_only_when_known():
    s = _search(property_types=[1, 6], property_condition=3)
    ok = make_listing("y:1", property_type_id=1, property_condition=3)
    assert engine.matches_non_amenity(ok, s)
    bad_type = make_listing("y:2", property_type_id=7, property_condition=3)
    assert not engine.matches_non_amenity(bad_type, s)
    # unknown (None) type/condition passes rather than being rejected
    unknown = make_listing("y:3", property_type_id=None, property_condition=None)
    assert engine.matches_non_amenity(unknown, s)


def test_features_never_drop_a_yad2_listing():
    s = _search(features=["elevator", "mamad"])
    untagged = make_listing("y:2", features=[])
    assert engine.matches_full(untagged, s, s.locations)


# --- scrape_group --------------------------------------------------------

async def test_scrape_group_normalizes_and_dedups_yad2_markers(monkeypatch):
    marker = {"token": "t1", "price": 6000, "address": {}, "additionalDetails": {},
              "metaData": {}}

    async def fake_fetch_map(http, sig, f):
        return [marker, dict(marker)]          # same token twice → one listing

    monkeypatch.setattr(yad2_gateway, "fetch_map", fake_fetch_map)
    s = _search(city_id="0070", region_id=2)
    sig = LocationSignature(2, None, "0070")
    listings = await engine.scrape_group(None, sig, GatewayFilters(), [(s, s.locations)])
    assert set(listings) == {"yad2:t1"}


def _marker(token, price=6000):
    return {"token": token, "price": price, "address": {}, "additionalDetails": {}, "metaData": {}}


async def test_main_fetch_is_unfiltered_and_tagging_fetches_mark_features(monkeypatch):
    calls = []

    async def fake_fetch_map(http, sig, f):
        calls.append(f)
        if not f.features:                       # main fetch: everything, incl. no price
            return [_marker("a"), _marker("b"), _marker("np", price=None)]
        return {"mamad": [_marker("a")], "elevator": [_marker("a"), _marker("b"), _marker("zz")]}[f.features[0]]

    monkeypatch.setattr(yad2_gateway, "fetch_map", fake_fetch_map)
    s = _search(max_price=7000, features=["mamad", "elevator"])
    sig = engine.scrape_sig(s.locations[0])
    f = engine.union_filters([(s, s.locations)], frozenset(s.features))
    listings = await engine.scrape_group(None, sig, f, [(s, s.locations)])

    main = calls[0]
    assert main.features == [] and main.max_price is None and main.min_rooms is None
    # tagging fetches: one feature each, narrowed by the group's ranges
    assert [(c.features, c.max_price) for c in calls[1:]] == [(["elevator"], 7000), (["mamad"], 7000)]
    assert sorted(listings["yad2:a"]["features"]) == ["elevator", "mamad"]
    assert listings["yad2:a"]["is_mamad"] is True
    assert listings["yad2:b"]["features"] == ["elevator"] and listings["yad2:b"]["is_mamad"] is None
    assert "yad2:zz" not in listings             # tagging never adds listings
    assert listings["yad2:np"]["features"] == []  # no price: still here, untagged
    assert all(engine.matches_full(l, s, s.locations) for l in listings.values())


async def test_failed_tagging_fetch_keeps_listings_untagged(monkeypatch):
    async def fake_fetch_map(http, sig, f):
        if f.features:
            raise RuntimeError("radware")
        return [_marker("a")]

    monkeypatch.setattr(yad2_gateway, "fetch_map", fake_fetch_map)
    s = _search(features=["mamad"])
    f = engine.union_filters([(s, s.locations)], frozenset(s.features))
    listings = await engine.scrape_group(None, engine.scrape_sig(s.locations[0]), f, [(s, s.locations)])
    assert set(listings) == {"yad2:a"} and listings["yad2:a"]["features"] == []


# --- routing: backfill / only-new / price-change --------------------------

async def _seed_search(db, chat_id=100, primed=False, **kw):
    from aptbot.models import SavedSearch as SS
    await db.upsert_user(chat_id, "t")
    loc = LocationTarget("city", 3, city_id="5000", display_name="תל אביב")
    s = SS(id=0, chat_id=chat_id, label=kw.pop("label", "s"), locations=[loc], **kw)
    sid = await db.add_search(s, s.locations)
    if primed:
        await db.mark_primed(sid)
    return await db.get_search(sid)


async def test_backfill_caps_and_records_all_as_seen(db, monkeypatch):
    monkeypatch.setattr(config, "BACKFILL_CAP", 10)
    s = await _seed_search(db)          # unprimed
    finals = [make_listing(f"yad2:{i}", price=5000 + i, order_id=i) for i in range(15)]
    sender = FakeSender()

    await engine.route_notifications(sender, db, s, finals, {})

    # exactly one intro + 10 apartment cards (the newest by order_id)
    assert sender.backfill_intros == 1
    assert sender.apartments == 10
    # ALL 15 recorded as seen (not just the 10 sent) so they never resend
    seen = await db.get_seen_map(s.id)
    assert len(seen) == 15
    # primed now
    assert (await db.get_search(s.id)).is_primed is True
    # the 10 sent are the highest order_ids (14..5)
    sent_urls = [t for t in sender.texts() if "Apartment Found" in t]
    assert any("/yad2:14" in t for t in sent_urls)
    assert all("/yad2:4" not in t for t in sent_urls)   # #4 was below the cap


async def test_backfill_empty_sends_started_empty(db):
    s = await _seed_search(db)
    sender = FakeSender()
    await engine.route_notifications(sender, db, s, [], {})
    assert sender.count("Monitor started") == 1
    assert sender.apartments == 0
    assert (await db.get_search(s.id)).is_primed is True


async def test_primed_sends_only_new(db):
    s = await _seed_search(db, primed=True)
    await db.bulk_upsert_seen(s.id, [("yad2:old", 5000)])
    finals = [make_listing("yad2:old", price=5000, order_id=1),
              make_listing("yad2:new", price=6000, order_id=2)]
    sender = FakeSender()
    await engine.route_notifications(sender, db, s, finals, {})
    assert sender.backfill_intros == 0
    assert sender.apartments == 1                 # only the new one
    assert "/yad2:new" in "".join(sender.texts())
    assert "yad2:new" in await db.get_seen_map(s.id)


async def test_primed_price_change_notifies_and_updates(db):
    s = await _seed_search(db, primed=True)
    await db.bulk_upsert_seen(s.id, [("yad2:a", 6000)])
    finals = [make_listing("yad2:a", price=5500, order_id=1)]
    sender = FakeSender()
    await engine.route_notifications(sender, db, s, finals, {})
    assert sender.price_changes == 1 and sender.apartments == 0
    assert (await db.get_seen_map(s.id))["yad2:a"] == 5500


async def test_send_failure_is_not_recorded_and_retries(db):
    s = await _seed_search(db, primed=True)
    finals = [make_listing("yad2:x", price=5000, order_id=1)]

    failing = FakeSender(fail=True)
    await engine.route_notifications(failing, db, s, finals, {})
    assert "yad2:x" not in await db.get_seen_map(s.id)   # not recorded on failure

    ok = FakeSender()
    await engine.route_notifications(ok, db, s, finals, {})
    assert ok.apartments == 1
    assert "yad2:x" in await db.get_seen_map(s.id)       # retried and recorded


async def test_prime_race_guard_no_double_backfill(db):
    """The router re-reads the freshest priming state; if a concurrent path already
    primed the search, it must fall through to only-new (no second backfill)."""
    s = await _seed_search(db)      # unprimed
    finals = [make_listing(f"yad2:{i}", price=5000, order_id=i) for i in range(3)]
    sender = FakeSender()

    # first call primes + backfills
    await engine.route_notifications(sender, db, s, finals, {})
    assert sender.backfill_intros == 1

    # second call holds a STALE (is_primed=False) object, but the DB now says primed
    stale = s               # s.is_primed is still False in this in-memory object
    assert stale.is_primed is False
    sender2 = FakeSender()
    await engine.route_notifications(sender2, db, stale, finals, {})
    assert sender2.backfill_intros == 0     # no second backfill
    assert sender2.apartments == 0          # everything already seen


async def test_chat_seen_dedup_across_searches(db):
    """Two searches for the same chat matching the same listing send it once."""
    s1 = await _seed_search(db, primed=True, label="s1")
    s2 = await _seed_search(db, primed=True, label="s2")
    listing = make_listing("yad2:shared", price=5000, order_id=1)
    sender = FakeSender()
    notified = {}
    await engine.route_notifications(sender, db, s1, [listing], notified)
    await engine.route_notifications(sender, db, s2, [listing], notified)
    assert sender.apartments == 1   # sent once across both searches
    # but both searches record it as seen
    assert "yad2:shared" in await db.get_seen_map(s1.id)
    assert "yad2:shared" in await db.get_seen_map(s2.id)


# --- run_cycle: end-to-end wiring -----------------------------------------

async def test_run_cycle_routes_only_new(db, monkeypatch):
    s = await _seed_search(db, primed=True)
    await db.bulk_upsert_seen(s.id, [("yad2:old", 5000)])

    async def fake_fetch_map(http, sig, f):
        return [{"token": "old", "price": 5000, "address": {"city": {"text": "תל אביב"}},
                 "additionalDetails": {"roomsCount": 3}, "metaData": {}, "orderId": 1},
                {"token": "fresh", "price": 6000, "address": {"city": {"text": "תל אביב"}},
                 "additionalDetails": {"roomsCount": 3}, "metaData": {}, "orderId": 2}]

    monkeypatch.setattr(yad2_gateway, "fetch_map", fake_fetch_map)

    sender = FakeSender()
    await engine.run_cycle(sender, db, http=None)
    assert sender.apartments == 1
    joined = "".join(sender.texts())
    assert "/item/fresh" in joined and "/item/old" not in joined


async def test_run_cycle_empty_scrape_does_not_prime(db, monkeypatch):
    """An empty/failed scrape must not prime an unprimed search (else it floods
    next cycle). No backfill is sent and the search stays unprimed."""
    s = await _seed_search(db)   # unprimed

    async def empty_fetch_map(http, sig, f):
        return []

    monkeypatch.setattr(yad2_gateway, "fetch_map", empty_fetch_map)

    sender = FakeSender()
    await engine.run_cycle(sender, db, http=None)
    assert sender.sent == []
    assert (await db.get_search(s.id)).is_primed is False


# --- run_cycle logging: quiet unless it delivered or broke ------------------

def _engine_info(caplog):
    return [r.getMessage() for r in caplog.records
            if r.name == "aptbot.engine" and r.levelname == "INFO"]


async def test_uneventful_cycle_logs_nothing_at_info(db, monkeypatch, caplog):
    """The whole point of the change: a healthy cycle with nothing new must not
    write a single INFO line (it used to write ~4 every 15 minutes)."""
    s = await _seed_search(db, primed=True)
    await db.bulk_upsert_seen(s.id, [("yad2:old", 5000)])

    async def fake_fetch_map(http, sig, f):
        return [{"token": "old", "price": 5000, "address": {"city": {"text": "תל אביב"}},
                 "additionalDetails": {"roomsCount": 3}, "metaData": {}, "orderId": 1}]

    monkeypatch.setattr(yad2_gateway, "fetch_map", fake_fetch_map)

    sender = FakeSender()
    with caplog.at_level("DEBUG", logger="aptbot.engine"):
        await engine.run_cycle(sender, db, http=None)
    assert sender.sent == []          # nothing new to send
    assert _engine_info(caplog) == []  # ...so nothing said about it
    # the work still happened, just at DEBUG
    assert any("Scraped" in r.getMessage() for r in caplog.records)


async def test_cycle_that_sends_logs_one_summary(db, monkeypatch, caplog):
    s = await _seed_search(db, primed=True)
    await db.bulk_upsert_seen(s.id, [("yad2:old", 5000)])

    async def fake_fetch_map(http, sig, f):
        return [{"token": "fresh", "price": 6000, "address": {"city": {"text": "תל אביב"}},
                 "additionalDetails": {"roomsCount": 3}, "metaData": {}, "orderId": 2}]

    monkeypatch.setattr(yad2_gateway, "fetch_map", fake_fetch_map)

    sender = FakeSender()
    with caplog.at_level("INFO", logger="aptbot.engine"):
        await engine.run_cycle(sender, db, http=None)
    summaries = [m for m in _engine_info(caplog) if m.startswith("Cycle:")]
    assert len(summaries) == 1
    assert "sent 1 message(s) to 1 chat(s)" in summaries[0]


async def test_failed_cycle_still_reports(db, monkeypatch, caplog):
    """'Only when there was a problem' — a broken scrape must stay visible."""
    await _seed_search(db, primed=True)

    async def empty_fetch_map(http, sig, f):
        return []

    monkeypatch.setattr(yad2_gateway, "fetch_map", empty_fetch_map)

    with caplog.at_level("INFO", logger="aptbot.engine"):
        await engine.run_cycle(FakeSender(), db, http=None)
    assert any("bad scrape" in m for m in _engine_info(caplog))
    assert any(r.levelname == "WARNING" for r in caplog.records)


# --- message labels --------------------------------------------------------

def test_listing_message_marks_wanted_features_and_unknown_filtered_fields():
    from aptbot.bot.formatting import format_apartment_message
    s = _search(max_price=7000, min_rooms=2, features=["mamad", "elevator"])
    text = format_apartment_message(make_listing("y:1", price=None, rooms="", features=["mamad"]), s)
    assert "<b>Price:</b> ❔ not listed" in text
    assert "<b>Rooms:</b> ❔ not listed" in text
    assert "<b>Features:</b> ✅ Safe room · ❔ Elevator" in text
    assert "Size" not in text                     # no size filter, no size: nothing shown
    plain = format_apartment_message(make_listing("y:2", features=["mamad"]))
    assert "🛡 Safe room" in plain and "❔" not in plain
