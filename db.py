# db.py
"""Async SQLite persistence layer (aiosqlite).

Schema v2: a saved search = one filter set + a *list* of area targets
(``search_locations``), so one monitor can watch many areas. Per-search
``seen_listings`` drives only-new + capped backfill + price-change with per-user
isolation. Migrates a v1 (single-location) database in place.
"""
from __future__ import annotations

import asyncio
import datetime
import logging
from typing import Dict, List, Optional, Tuple

import aiosqlite

from models import LocationTarget, SavedSearch, normalize_name as _normalize_name

logger = logging.getLogger(__name__)

SCHEMA_VERSION = "2"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    chat_id    INTEGER PRIMARY KEY,
    username   TEXT,
    first_name TEXT,
    last_name  TEXT,
    is_active  INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS saved_searches (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id            INTEGER NOT NULL REFERENCES users(chat_id) ON DELETE CASCADE,
    label              TEXT,
    min_price          INTEGER,
    max_price          INTEGER,
    min_rooms          REAL,
    max_rooms          REAL,
    min_sqm            INTEGER,
    max_sqm            INTEGER,
    min_floor          INTEGER,
    max_floor          INTEGER,
    property_types     TEXT,
    property_condition INTEGER,
    features           TEXT,
    source_mode        TEXT NOT NULL DEFAULT 'auto',
    is_active          INTEGER NOT NULL DEFAULT 1,
    is_primed          INTEGER NOT NULL DEFAULT 0,
    created_at         TEXT NOT NULL,
    updated_at         TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_searches_active ON saved_searches(is_active);
CREATE INDEX IF NOT EXISTS idx_searches_chat   ON saved_searches(chat_id);

CREATE TABLE IF NOT EXISTS search_locations (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    search_id    INTEGER NOT NULL REFERENCES saved_searches(id) ON DELETE CASCADE,
    level        TEXT NOT NULL,
    region_id    INTEGER NOT NULL,
    area_id      INTEGER,
    city_id      TEXT,       -- Yad2 city ids are 4-char zero-padded strings ("0070")
    hood_id      INTEGER,
    street_id    INTEGER,
    display_name TEXT,
    match_name   TEXT
);
CREATE INDEX IF NOT EXISTS idx_loc_search ON search_locations(search_id);

CREATE TABLE IF NOT EXISTS seen_listings (
    search_id        INTEGER NOT NULL REFERENCES saved_searches(id) ON DELETE CASCADE,
    listing_uid      TEXT    NOT NULL,
    last_price       INTEGER,
    first_seen_at    TEXT    NOT NULL,
    last_notified_at TEXT,
    PRIMARY KEY (search_id, listing_uid)
);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);
"""

_SEARCH_COLUMNS = (
    "id, chat_id, label, min_price, max_price, min_rooms, max_rooms, min_sqm, "
    "max_sqm, min_floor, max_floor, property_types, property_condition, features, "
    "source_mode, is_active, is_primed"
)

_FILTER_FIELDS = (
    "label", "min_price", "max_price", "min_rooms", "max_rooms", "min_sqm",
    "max_sqm", "min_floor", "max_floor", "property_types", "property_condition",
    "features", "source_mode",
)


def _now() -> str:
    return datetime.datetime.utcnow().isoformat(timespec="seconds")


def _opt_bool(value) -> Optional[bool]:
    return None if value is None else bool(value)


def _to_db_bool(value) -> Optional[int]:
    return None if value is None else int(bool(value))


def _types_to_db(types: Optional[List[int]]) -> Optional[str]:
    if not types:
        return None
    return ",".join(str(int(t)) for t in types)


def _types_from_db(raw: Optional[str]) -> Optional[List[int]]:
    if not raw:
        return None
    out = [int(p) for p in raw.split(",") if p.strip().isdigit()]
    return out or None


def _features_to_db(features: Optional[List[str]]) -> Optional[str]:
    if not features:
        return None
    return ",".join(f for f in features if f)


def _features_from_db(raw: Optional[str]) -> List[str]:
    if not raw:
        return []
    return [p for p in raw.split(",") if p]


def _row_to_search(row: aiosqlite.Row) -> SavedSearch:
    return SavedSearch(
        id=row["id"],
        chat_id=row["chat_id"],
        label=row["label"],
        min_price=row["min_price"],
        max_price=row["max_price"],
        min_rooms=row["min_rooms"],
        max_rooms=row["max_rooms"],
        min_sqm=row["min_sqm"],
        max_sqm=row["max_sqm"],
        min_floor=row["min_floor"],
        max_floor=row["max_floor"],
        property_types=_types_from_db(row["property_types"]),
        property_condition=row["property_condition"],
        features=_features_from_db(row["features"]),
        source_mode=row["source_mode"],
        is_active=bool(row["is_active"]),
        is_primed=bool(row["is_primed"]),
        locations=[],
    )


def _row_to_location(row: aiosqlite.Row) -> LocationTarget:
    city_id = row["city_id"]
    return LocationTarget(
        level=row["level"],
        region_id=row["region_id"],
        area_id=row["area_id"],
        city_id=None if city_id is None else str(city_id),  # keep '0070' padding
        hood_id=row["hood_id"],
        street_id=row["street_id"],
        display_name=row["display_name"] or "",
        match_name=row["match_name"] or "",
    )


class Database:
    def __init__(self, path: str):
        self.path = path
        self._conn: Optional[aiosqlite.Connection] = None
        self._write_lock = asyncio.Lock()

    async def connect(self) -> "Database":
        self._conn = await aiosqlite.connect(self.path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA journal_mode=WAL;")
        await self._conn.execute("PRAGMA foreign_keys=ON;")
        await self._conn.commit()
        await self.init_schema()
        logger.info("Database connected at %s", self.path)
        return self

    async def _table_exists(self, name: str) -> bool:
        cur = await self._conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
        )
        return await cur.fetchone() is not None

    async def _columns(self, table: str) -> set:
        cur = await self._conn.execute(f"PRAGMA table_info({table})")
        return {r["name"] for r in await cur.fetchall()}

    async def init_schema(self) -> None:
        assert self._conn is not None
        await self._conn.executescript(
            "CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);"
        )
        await self._maybe_migrate_v1()
        await self._conn.executescript(_SCHEMA)
        await self._repair_seen_fk()
        await self._migrate_city_id_text()
        await self._migrate_user_names()
        await self._conn.execute(
            "INSERT OR REPLACE INTO meta(key, value) VALUES ('schema_version', ?)",
            (SCHEMA_VERSION,),
        )
        await self._conn.commit()

    async def _repair_seen_fk(self) -> None:
        """Self-heal a broken ``seen_listings`` foreign key.

        A subtle v1→v2 migration bug: modern SQLite's ``ALTER TABLE ... RENAME``
        rewrote the pre-existing ``seen_listings`` FK from ``saved_searches`` to
        ``saved_searches_v1``; that table was then dropped, leaving a dangling FK so
        every ``bulk_upsert_seen`` fails with "no such table: saved_searches_v1".
        Detect it and rebuild the table with the correct FK, preserving rows.
        """
        cur = await self._conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='seen_listings'"
        )
        row = await cur.fetchone()
        if not row or not row["sql"] or "saved_searches_v1" not in row["sql"]:
            return
        logger.warning("Repairing seen_listings FK (dangling saved_searches_v1)…")
        await self._conn.commit()
        await self._conn.execute("PRAGMA foreign_keys=OFF")
        await self._conn.executescript(
            "CREATE TABLE seen_listings_fix ("
            " search_id INTEGER NOT NULL REFERENCES saved_searches(id) ON DELETE CASCADE,"
            " listing_uid TEXT NOT NULL, last_price INTEGER, first_seen_at TEXT NOT NULL,"
            " last_notified_at TEXT, PRIMARY KEY (search_id, listing_uid));"
            "INSERT INTO seen_listings_fix "
            " SELECT search_id, listing_uid, last_price, first_seen_at, last_notified_at"
            " FROM seen_listings;"
            "DROP TABLE seen_listings;"
            "ALTER TABLE seen_listings_fix RENAME TO seen_listings;"
        )
        await self._conn.commit()
        await self._conn.execute("PRAGMA foreign_keys=ON")
        await self._conn.commit()
        logger.warning("seen_listings FK repaired.")

    async def _migrate_city_id_text(self) -> None:
        """Rebuild search_locations if city_id is an INTEGER column (older schema).

        Yad2 city ids are 4-char zero-padded strings ('0070'); an INTEGER-affinity
        column silently drops leading zeros on insert ('0070' -> 70), which breaks
        the feed query and catalog lookup for every leading-zero city (≈half of
        them). Convert the column to TEXT, recovering padding for any numeric rows.
        """
        if not await self._table_exists("search_locations"):
            return
        cur = await self._conn.execute("PRAGMA table_info(search_locations)")
        info = {r["name"]: (r["type"] or "") for r in await cur.fetchall()}
        if info.get("city_id", "").upper() == "TEXT":
            return
        logger.warning("Migrating search_locations.city_id INTEGER -> TEXT …")
        await self._conn.commit()
        await self._conn.execute("PRAGMA foreign_keys=OFF")
        await self._conn.executescript(
            "CREATE TABLE search_locations_fix ("
            " id INTEGER PRIMARY KEY AUTOINCREMENT,"
            " search_id INTEGER NOT NULL REFERENCES saved_searches(id) ON DELETE CASCADE,"
            " level TEXT NOT NULL, region_id INTEGER NOT NULL, area_id INTEGER,"
            " city_id TEXT, hood_id INTEGER, street_id INTEGER,"
            " display_name TEXT, match_name TEXT);"
            "INSERT INTO search_locations_fix"
            " (id, search_id, level, region_id, area_id, city_id, hood_id, street_id,"
            "  display_name, match_name)"
            " SELECT id, search_id, level, region_id, area_id,"
            "  CASE WHEN city_id IS NULL THEN NULL"
            "       WHEN typeof(city_id)='integer' THEN printf('%04d', city_id)"
            "       ELSE city_id END,"
            "  hood_id, street_id, display_name, match_name FROM search_locations;"
            "DROP TABLE search_locations;"
            "ALTER TABLE search_locations_fix RENAME TO search_locations;"
            "CREATE INDEX IF NOT EXISTS idx_loc_search ON search_locations(search_id);"
        )
        await self._conn.commit()
        await self._conn.execute("PRAGMA foreign_keys=ON")
        await self._conn.commit()
        logger.warning("search_locations.city_id migrated to TEXT.")

    async def _migrate_user_names(self) -> None:
        """Add ``first_name``/``last_name`` to an older ``users`` table.

        Telegram only gives a ``username`` when the user set one (most don't), so
        without these the DB can't say who is actually using the bot. Additive
        ALTERs: existing rows stay valid and backfill themselves as users interact.
        """
        if not await self._table_exists("users"):
            return
        cols = await self._columns("users")
        for col in ("first_name", "last_name"):
            if col not in cols:
                await self._conn.execute(f"ALTER TABLE users ADD COLUMN {col} TEXT")
                logger.warning("Added users.%s column.", col)

    async def _maybe_migrate_v1(self) -> None:
        """Migrate a v1 (single-location) saved_searches into v2 in place."""
        if not await self._table_exists("saved_searches"):
            return
        cols = await self._columns("saved_searches")
        if "region_id" not in cols or await self._table_exists("search_locations"):
            return  # already v2 (or not a v1 table)

        logger.warning("Migrating database schema v1 -> v2 ...")
        async with self._write_lock:
            await self._conn.execute("ALTER TABLE saved_searches RENAME TO saved_searches_v1")
            await self._conn.executescript(_SCHEMA)
            cur = await self._conn.execute("SELECT * FROM saved_searches_v1")
            rows = await cur.fetchall()
            for r in rows:
                keys = r.keys()

                def g(k, default=None):
                    return r[k] if k in keys else default

                features = "elevator" if g("require_elevator") else None
                await self._conn.execute(
                    "INSERT INTO saved_searches(id, chat_id, label, min_price, max_price, "
                    "min_rooms, max_rooms, min_sqm, source_mode, is_active, is_primed, "
                    "features, created_at, updated_at) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        g("id"), g("chat_id"), g("label"), g("min_price"), g("max_price"),
                        g("min_rooms"), g("max_rooms"), g("min_sqm"),
                        g("source_mode", "auto"), g("is_active", 1), g("is_primed", 0),
                        features, g("created_at", _now()), g("updated_at", _now()),
                    ),
                )
                await self._conn.execute(
                    "INSERT INTO search_locations(search_id, level, region_id, area_id, "
                    "city_id, hood_id, display_name, match_name) VALUES (?,?,?,?,?,?,?,?)",
                    (
                        g("id"), g("match_level", "city"), g("region_id"), g("area_id"),
                        g("city_id"), g("hood_id"), g("location_display", ""),
                        _normalize_name(g("hood_name") or ""),
                    ),
                )
            # relabel legacy fbgroups: uids -> rentlyfly:
            await self._conn.execute(
                "UPDATE seen_listings SET listing_uid = 'rentlyfly:' || substr(listing_uid, 10) "
                "WHERE listing_uid LIKE 'fbgroups:%'"
            )
            await self._conn.execute("DROP TABLE saved_searches_v1")
            await self._conn.commit()
        logger.warning("Migration v1 -> v2 complete (%d searches).", len(rows))

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    # --- users ---
    async def upsert_user(self, chat_id: int, username: Optional[str],
                         first_name: Optional[str] = None,
                         last_name: Optional[str] = None) -> bool:
        """Record/refresh a user. Returns True if this chat was seen for the FIRST
        time (used to log "a new user started a chat").

        Names are COALESCE-d so a caller that only knows the username can't wipe a
        name we already captured.
        """
        async with self._write_lock:
            cur = await self._conn.execute(
                "SELECT 1 FROM users WHERE chat_id=?", (chat_id,))
            is_new = await cur.fetchone() is None
            await self._conn.execute(
                "INSERT INTO users(chat_id, username, first_name, last_name, is_active, created_at) "
                "VALUES (?, ?, ?, ?, 1, ?) "
                "ON CONFLICT(chat_id) DO UPDATE SET "
                "  username=excluded.username, "
                "  first_name=COALESCE(excluded.first_name, users.first_name), "
                "  last_name=COALESCE(excluded.last_name, users.last_name)",
                (chat_id, username, first_name, last_name, _now()),
            )
            await self._conn.commit()
            return is_new

    async def set_user_active(self, chat_id: int, active: bool) -> None:
        async with self._write_lock:
            await self._conn.execute(
                "UPDATE users SET is_active=? WHERE chat_id=?",
                (1 if active else 0, chat_id),
            )
            await self._conn.commit()

    async def is_user_active(self, chat_id: int) -> bool:
        cur = await self._conn.execute(
            "SELECT is_active FROM users WHERE chat_id=?", (chat_id,)
        )
        row = await cur.fetchone()
        return bool(row["is_active"]) if row else False

    # --- saved searches ---
    async def add_search(self, s: SavedSearch, locations: List[LocationTarget]) -> int:
        now = _now()
        async with self._write_lock:
            cur = await self._conn.execute(
                "INSERT INTO saved_searches("
                "chat_id, label, min_price, max_price, min_rooms, max_rooms, min_sqm, "
                "max_sqm, min_floor, max_floor, property_types, property_condition, "
                "features, source_mode, is_active, is_primed, created_at, updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    s.chat_id, s.label, s.min_price, s.max_price, s.min_rooms, s.max_rooms,
                    s.min_sqm, s.max_sqm, s.min_floor, s.max_floor,
                    _types_to_db(s.property_types), s.property_condition,
                    _features_to_db(s.features),
                    s.source_mode, 1 if s.is_active else 0, 1 if s.is_primed else 0,
                    now, now,
                ),
            )
            search_id = cur.lastrowid
            await self._insert_locations(search_id, locations)
            await self._conn.commit()
            return search_id

    async def _insert_locations(self, search_id: int, locations: List[LocationTarget]) -> None:
        for loc in locations:
            await self._conn.execute(
                "INSERT INTO search_locations(search_id, level, region_id, area_id, "
                "city_id, hood_id, street_id, display_name, match_name) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    search_id, loc.level, loc.region_id, loc.area_id, loc.city_id,
                    loc.hood_id, loc.street_id, loc.display_name, loc.match_name,
                ),
            )

    async def set_search_locations(self, search_id: int, locations: List[LocationTarget]) -> None:
        async with self._write_lock:
            await self._conn.execute(
                "DELETE FROM search_locations WHERE search_id=?", (search_id,)
            )
            await self._insert_locations(search_id, locations)
            await self._conn.execute(
                "UPDATE saved_searches SET updated_at=? WHERE id=?", (_now(), search_id)
            )
            await self._conn.commit()

    async def get_search_locations(self, search_id: int) -> List[LocationTarget]:
        cur = await self._conn.execute(
            "SELECT * FROM search_locations WHERE search_id=? ORDER BY id", (search_id,)
        )
        return [_row_to_location(r) for r in await cur.fetchall()]

    async def _attach_locations(self, searches: List[SavedSearch]) -> List[SavedSearch]:
        if not searches:
            return searches
        by_id = {s.id: s for s in searches}
        placeholders = ",".join("?" for _ in by_id)
        cur = await self._conn.execute(
            f"SELECT * FROM search_locations WHERE search_id IN ({placeholders}) ORDER BY id",
            tuple(by_id.keys()),
        )
        for r in await cur.fetchall():
            by_id[r["search_id"]].locations.append(_row_to_location(r))
        return searches

    async def get_search(self, search_id: int) -> Optional[SavedSearch]:
        cur = await self._conn.execute(
            f"SELECT {_SEARCH_COLUMNS} FROM saved_searches WHERE id=?", (search_id,)
        )
        row = await cur.fetchone()
        if not row:
            return None
        (search,) = await self._attach_locations([_row_to_search(row)])
        return search

    async def list_searches(self, chat_id: int) -> List[SavedSearch]:
        cur = await self._conn.execute(
            f"SELECT {_SEARCH_COLUMNS} FROM saved_searches WHERE chat_id=? ORDER BY id",
            (chat_id,),
        )
        searches = [_row_to_search(r) for r in await cur.fetchall()]
        return await self._attach_locations(searches)

    async def get_active_searches(self) -> List[SavedSearch]:
        cur = await self._conn.execute(
            f"SELECT {', '.join('s.' + c for c in _SEARCH_COLUMNS.split(', '))} "
            f"FROM saved_searches s JOIN users u ON u.chat_id = s.chat_id "
            f"WHERE s.is_active=1 AND u.is_active=1"
        )
        searches = [_row_to_search(r) for r in await cur.fetchall()]
        return await self._attach_locations(searches)

    async def update_search_filters(self, search_id: int, **fields) -> None:
        sets, values = [], []
        for key, value in fields.items():
            if key not in _FILTER_FIELDS:
                continue
            if key == "property_types":
                value = _types_to_db(value)
            elif key == "features":
                value = _features_to_db(value)
            sets.append(f"{key}=?")
            values.append(value)
        if not sets:
            return
        sets.append("updated_at=?")
        values.append(_now())
        values.append(search_id)
        async with self._write_lock:
            await self._conn.execute(
                f"UPDATE saved_searches SET {', '.join(sets)} WHERE id=?", values
            )
            await self._conn.commit()

    async def set_search_active(self, search_id: int, active: bool) -> None:
        async with self._write_lock:
            await self._conn.execute(
                "UPDATE saved_searches SET is_active=?, updated_at=? WHERE id=?",
                (1 if active else 0, _now(), search_id),
            )
            await self._conn.commit()

    async def delete_search(self, search_id: int) -> None:
        async with self._write_lock:
            await self._conn.execute(
                "DELETE FROM saved_searches WHERE id=?", (search_id,)
            )
            await self._conn.commit()

    async def mark_primed(self, search_id: int) -> None:
        async with self._write_lock:
            await self._conn.execute(
                "UPDATE saved_searches SET is_primed=1 WHERE id=?", (search_id,)
            )
            await self._conn.commit()

    async def reprime_user_searches(self, chat_id: int) -> None:
        """Reset priming so searches resume with a capped backfill (used on unmute)."""
        async with self._write_lock:
            await self._conn.execute(
                "UPDATE saved_searches SET is_primed=0 WHERE chat_id=?", (chat_id,)
            )
            await self._conn.commit()

    async def reprime_search(self, search_id: int) -> None:
        """Reset priming for one search (after an edit / re-enable) so it re-backfills."""
        async with self._write_lock:
            await self._conn.execute(
                "UPDATE saved_searches SET is_primed=0 WHERE id=?", (search_id,)
            )
            await self._conn.commit()

    # --- seen listings ---
    async def get_seen_map(self, search_id: int) -> Dict[str, Optional[int]]:
        cur = await self._conn.execute(
            "SELECT listing_uid, last_price FROM seen_listings WHERE search_id=?",
            (search_id,),
        )
        return {r["listing_uid"]: r["last_price"] for r in await cur.fetchall()}

    async def bulk_upsert_seen(
        self, search_id: int, rows: List[Tuple[str, Optional[int]]]
    ) -> None:
        if not rows:
            return
        now = _now()
        payload = [(search_id, uid, price, now) for uid, price in rows]
        async with self._write_lock:
            await self._conn.executemany(
                "INSERT INTO seen_listings(search_id, listing_uid, last_price, first_seen_at) "
                "VALUES (?,?,?,?) "
                "ON CONFLICT(search_id, listing_uid) DO UPDATE SET last_price=excluded.last_price",
                payload,
            )
            await self._conn.commit()

    async def update_seen_price(
        self, search_id: int, uid: str, price: Optional[int]
    ) -> None:
        async with self._write_lock:
            await self._conn.execute(
                "UPDATE seen_listings SET last_price=?, last_notified_at=? "
                "WHERE search_id=? AND listing_uid=?",
                (price, _now(), search_id, uid),
            )
            await self._conn.commit()

    # --- meta ---
    async def get_meta(self, key: str) -> Optional[str]:
        cur = await self._conn.execute("SELECT value FROM meta WHERE key=?", (key,))
        row = await cur.fetchone()
        return row["value"] if row else None

    async def set_meta(self, key: str, value: str) -> None:
        async with self._write_lock:
            await self._conn.execute(
                "INSERT INTO meta(key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )
            await self._conn.commit()
