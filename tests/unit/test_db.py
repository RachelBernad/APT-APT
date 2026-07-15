"""db.py: string city-id round-trip, seen/priming persistence, and the three
in-place migrations (v1->v2, seen_listings FK self-heal, city_id INTEGER->TEXT).

Regression guards for real bugs:
  INVARIANT #1: a LocationTarget(city_id="0070") round-trips as "0070", not 70.
  INVARIANT #6: all migrations run cleanly and preserve rows.
"""
import sqlite3

import pytest

from db import Database, _SCHEMA
from models import LocationTarget, SavedSearch


# --- helpers to seed a user + search --------------------------------------

async def _add_user_and_search(db, chat_id=100, city_id="0070", **search_kw):
    await db.upsert_user(chat_id, "tester")
    s = SavedSearch(id=0, chat_id=chat_id, label=search_kw.pop("label", "s1"), **search_kw)
    loc = LocationTarget(level="city", region_id=2, city_id=city_id,
                         display_name="אשדוד")
    sid = await db.add_search(s, [loc])
    return sid


# --- city-id round-trip ---------------------------------------------------

@pytest.mark.parametrize("city_id", ["0070", "5000", "103P"])
async def test_city_id_round_trips_as_string(db, city_id):
    sid = await _add_user_and_search(db, city_id=city_id)
    reread = await db.get_search(sid)
    assert reread is not None
    assert reread.locations[0].city_id == city_id
    assert isinstance(reread.locations[0].city_id, str)


async def test_search_filter_serialization_round_trip(db):
    sid = await _add_user_and_search(
        db, min_price=4000, max_price=9000, min_rooms=2.5,
        property_types=[1, 6], features=["elevator", "mamad"],
        source_mode="yad2+rentlyfly",
    )
    s = await db.get_search(sid)
    assert s.min_price == 4000 and s.max_price == 9000 and s.min_rooms == 2.5
    assert s.property_types == [1, 6]
    assert s.features == ["elevator", "mamad"]
    assert s.source_mode == "yad2+rentlyfly"


# --- seen listings + priming ----------------------------------------------

async def test_seen_upsert_and_price_update(db):
    sid = await _add_user_and_search(db)
    await db.bulk_upsert_seen(sid, [("yad2:a", 5000), ("yad2:b", 6000)])
    seen = await db.get_seen_map(sid)
    assert seen == {"yad2:a": 5000, "yad2:b": 6000}

    await db.update_seen_price(sid, "yad2:a", 4500)
    assert (await db.get_seen_map(sid))["yad2:a"] == 4500

    # upsert on conflict updates price, doesn't duplicate
    await db.bulk_upsert_seen(sid, [("yad2:a", 4000)])
    seen = await db.get_seen_map(sid)
    assert seen["yad2:a"] == 4000 and len(seen) == 2


async def test_priming_flags(db):
    sid = await _add_user_and_search(db)
    assert (await db.get_search(sid)).is_primed is False
    await db.mark_primed(sid)
    assert (await db.get_search(sid)).is_primed is True
    await db.reprime_search(sid)
    assert (await db.get_search(sid)).is_primed is False


async def test_delete_search_cascades_seen(db):
    sid = await _add_user_and_search(db)
    await db.bulk_upsert_seen(sid, [("yad2:a", 1)])
    await db.delete_search(sid)
    assert await db.get_search(sid) is None
    # seen rows are gone via ON DELETE CASCADE
    assert await db.get_seen_map(sid) == {}


# --- migration: city_id INTEGER -> TEXT -----------------------------------

async def test_migrate_city_id_integer_to_text(tmp_path):
    path = str(tmp_path / "legacy_int.db")
    con = sqlite3.connect(path)
    con.executescript(_SCHEMA)                    # gives correct TEXT schema...
    # ...then clobber search_locations with an old INTEGER-affinity city_id column.
    con.executescript(
        "DROP TABLE search_locations;"
        "CREATE TABLE search_locations ("
        " id INTEGER PRIMARY KEY AUTOINCREMENT, search_id INTEGER, level TEXT,"
        " region_id INTEGER NOT NULL, area_id INTEGER, city_id INTEGER,"
        " hood_id INTEGER, street_id INTEGER, display_name TEXT, match_name TEXT);"
    )
    # '0070' was silently coerced to the integer 70 by the old column.
    con.execute("INSERT INTO search_locations(search_id, level, region_id, city_id) "
                "VALUES (1, 'city', 2, 70)")
    con.execute("INSERT INTO search_locations(search_id, level, region_id, city_id) "
                "VALUES (2, 'region', 3, NULL)")
    con.commit()
    con.close()

    db = await Database(path).connect()
    try:
        # column is now TEXT and 70 was re-padded to '0070'
        locs1 = await db.get_search_locations(1)
        assert locs1[0].city_id == "0070" and isinstance(locs1[0].city_id, str)
        locs2 = await db.get_search_locations(2)
        assert locs2[0].city_id is None    # NULL preserved
        # idempotent: connecting again does not error / re-migrate
    finally:
        await db.close()

    db2 = await Database(path).connect()
    try:
        locs = await db2.get_search_locations(1)
        assert locs[0].city_id == "0070"   # idempotent: re-migrate is a no-op
    finally:
        await db2.close()


# --- migration: seen_listings dangling FK self-heal -----------------------

async def test_repair_seen_listings_dangling_fk(tmp_path):
    path = str(tmp_path / "broken_fk.db")
    con = sqlite3.connect(path)
    con.executescript(_SCHEMA)
    # Simulate the post-v1-migration corruption: seen_listings FK points at the
    # dropped saved_searches_v1 table.
    con.executescript(
        "DROP TABLE seen_listings;"
        "CREATE TABLE seen_listings ("
        " search_id INTEGER NOT NULL REFERENCES saved_searches_v1(id) ON DELETE CASCADE,"
        " listing_uid TEXT NOT NULL, last_price INTEGER, first_seen_at TEXT NOT NULL,"
        " last_notified_at TEXT, PRIMARY KEY (search_id, listing_uid));"
    )
    con.execute("INSERT INTO users(chat_id, username, is_active, created_at) "
                "VALUES (100, 't', 1, '2020-01-01T00:00:00')")
    con.execute("INSERT INTO saved_searches(id, chat_id, source_mode, is_active, "
                "is_primed, created_at, updated_at) "
                "VALUES (1, 100, 'auto', 1, 0, '2020-01-01', '2020-01-01')")
    con.execute("INSERT INTO seen_listings(search_id, listing_uid, last_price, first_seen_at) "
                "VALUES (1, 'yad2:old', 5000, '2020-01-01')")
    con.commit()
    con.close()

    db = await Database(path).connect()
    try:
        # the pre-existing row is preserved through the rebuild
        assert (await db.get_seen_map(1)) == {"yad2:old": 5000}
        # and writing new seen rows works again (the bug made this fail with
        # "no such table: saved_searches_v1")
        await db.bulk_upsert_seen(1, [("yad2:new", 6000)])
        assert "yad2:new" in await db.get_seen_map(1)
        # the FK no longer references the dead table
        raw = sqlite3.connect(path)
        sql = raw.execute(
            "SELECT sql FROM sqlite_master WHERE name='seen_listings'").fetchone()[0]
        raw.close()
        assert "saved_searches_v1" not in sql
    finally:
        await db.close()


# --- migration: v1 (single-location) -> v2 --------------------------------

async def test_migrate_v1_to_v2(tmp_path):
    path = str(tmp_path / "v1.db")
    con = sqlite3.connect(path)
    con.executescript(
        "CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);"
        "CREATE TABLE users (chat_id INTEGER PRIMARY KEY, username TEXT,"
        " is_active INTEGER NOT NULL DEFAULT 1, created_at TEXT NOT NULL);"
        # v1 single-location saved_searches (note: has region_id, no search_locations)
        "CREATE TABLE saved_searches ("
        " id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER, label TEXT,"
        " min_price INTEGER, max_price INTEGER, min_rooms REAL, max_rooms REAL,"
        " min_sqm INTEGER, require_elevator INTEGER, match_level TEXT,"
        " region_id INTEGER, area_id INTEGER, city_id TEXT, hood_id INTEGER,"
        " location_display TEXT, hood_name TEXT, source_mode TEXT DEFAULT 'auto',"
        " is_active INTEGER DEFAULT 1, is_primed INTEGER DEFAULT 0,"
        " created_at TEXT, updated_at TEXT);"
        # FK-less seen table so the RENAME doesn't rewrite it; carries a legacy uid
        "CREATE TABLE seen_listings ("
        " search_id INTEGER NOT NULL, listing_uid TEXT NOT NULL, last_price INTEGER,"
        " first_seen_at TEXT NOT NULL, last_notified_at TEXT,"
        " PRIMARY KEY (search_id, listing_uid));"
    )
    con.execute("INSERT INTO users(chat_id, username, is_active, created_at) "
                "VALUES (100, 't', 1, '2020-01-01')")
    con.execute(
        "INSERT INTO saved_searches(id, chat_id, label, min_price, require_elevator, "
        "match_level, region_id, area_id, city_id, location_display, hood_name, "
        "source_mode, is_active, is_primed, created_at, updated_at) "
        "VALUES (1, 100, 'old search', 3000, 1, 'city', 2, 21, '0070', 'אשדוד', '', "
        "'auto', 1, 1, '2020-01-01', '2020-01-01')"
    )
    con.execute("INSERT INTO seen_listings(search_id, listing_uid, last_price, first_seen_at) "
                "VALUES (1, 'fbgroups:999', 5000, '2020-01-01')")
    con.commit()
    con.close()

    db = await Database(path).connect()
    try:
        s = await db.get_search(1)
        assert s is not None
        assert s.label == "old search" and s.min_price == 3000
        # require_elevator -> features=['elevator']
        assert s.features == ["elevator"]
        # single location migrated into search_locations, city_id kept as string
        assert len(s.locations) == 1
        loc = s.locations[0]
        assert loc.level == "city" and loc.region_id == 2 and loc.city_id == "0070"
        # legacy fbgroups uid relabeled to rentlyfly:
        seen = await db.get_seen_map(1)
        assert "rentlyfly:999" in seen and "fbgroups:999" not in seen
        # schema version stamped
        assert await db.get_meta("schema_version") == "2"
    finally:
        await db.close()
