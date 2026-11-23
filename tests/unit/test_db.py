"""db.py: string city-id round-trip, seen/priming persistence, users, and the
fresh-start policy (a DB from another schema version is moved aside, not migrated).

Regression guards for real bugs:
  INVARIANT #1: a LocationTarget(city_id="0070") round-trips as "0070", not 70.
  INVARIANT #6: an old/foreign DB file never breaks startup; it is set aside intact.
"""
import sqlite3

import pytest

from db import SCHEMA_VERSION, Database
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
    )
    s = await db.get_search(sid)
    assert s.min_price == 4000 and s.max_price == 9000 and s.min_rooms == 2.5
    assert s.property_types == [1, 6]
    assert s.features == ["elevator", "mamad"]


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


# --- users: new-user detection + name capture -----------------------------

async def test_upsert_user_reports_first_contact_only_once(db):
    assert await db.upsert_user(1234, "handle", "Netneal") is True
    assert await db.upsert_user(1234, "handle", "Netneal") is False


async def test_upsert_user_stores_names(db):
    await db.upsert_user(1234, "handle", "Bob", "Example")
    cur = await db._conn.execute(
        "SELECT username, first_name, last_name FROM users WHERE chat_id=1234")
    row = await cur.fetchone()
    assert (row["username"], row["first_name"], row["last_name"]) == \
        ("handle", "Bob", "Example")


async def test_two_arg_upsert_does_not_wipe_a_known_name(db):
    """Legacy call sites pass only a username; the COALESCE must keep the name."""
    await db.upsert_user(1234, "handle", "Bob", "Example")
    await db.upsert_user(1234, "handle")          # old-style call
    cur = await db._conn.execute(
        "SELECT first_name, last_name FROM users WHERE chat_id=1234")
    row = await cur.fetchone()
    assert (row["first_name"], row["last_name"]) == ("Bob", "Example")


# --- fresh-start policy: foreign DBs are moved aside, never migrated -------

def _old_files(tmp_path):
    return sorted(p.name for p in tmp_path.iterdir() if ".old-" in p.name)


async def test_reconnect_same_version_keeps_data(tmp_path):
    path = str(tmp_path / "bot.db")
    db = await Database(path).connect()
    sid = await _add_user_and_search(db)
    await db.close()
    db = await Database(path).connect()
    try:
        assert (await db.get_search(sid)) is not None
        assert _old_files(tmp_path) == []
    finally:
        await db.close()


@pytest.mark.parametrize("seed", [
    # a pre-v3 DB (has meta, older version)
    "CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);"
    "INSERT INTO meta VALUES ('schema_version', '2');"
    "CREATE TABLE saved_searches (id INTEGER PRIMARY KEY, source_mode TEXT);",
    # an ancient DB with no meta table at all
    "CREATE TABLE saved_searches (id INTEGER PRIMARY KEY, region_id INTEGER);",
])
async def test_foreign_db_is_moved_aside_and_fresh_one_created(tmp_path, seed):
    path = tmp_path / "bot.db"
    con = sqlite3.connect(str(path)); con.executescript(seed); con.commit(); con.close()

    db = await Database(str(path)).connect()
    try:
        sid = await _add_user_and_search(db)          # fresh schema works
        assert (await db.get_search(sid)) is not None
    finally:
        await db.close()

    old = _old_files(tmp_path)
    assert len(old) == 1 and old[0].startswith("bot.db.old-")
    # the old data is preserved untouched
    con = sqlite3.connect(str(tmp_path / old[0]))
    assert con.execute("SELECT name FROM sqlite_master WHERE name='saved_searches'").fetchone()
    con.close()
    # and the new file records the current version
    con = sqlite3.connect(str(path))
    assert con.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()[0] == SCHEMA_VERSION
    con.close()
