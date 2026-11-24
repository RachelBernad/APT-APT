"""SQLite cache: groups, posts, watermarks. Makes repeated reads free and filter_posts() Facebook-free.

One file ($FBGB_HOME/fbgb.db). Rows hold the model as JSON plus a few indexed columns. Single connection,
WAL mode; all access goes through this class (safe for concurrent asyncio callers).
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Iterable

import aiosqlite

from aptbot.scrapers.facebook_generic_browser.models import Comment, Group, Hints, Photo, Post

SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS groups (
  id TEXT PRIMARY KEY, data TEXT NOT NULL, fetched_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS posts (
  group_id TEXT NOT NULL, post_id TEXT NOT NULL, created_at REAL, text_hash TEXT,
  data TEXT NOT NULL, first_seen REAL NOT NULL, last_seen REAL NOT NULL,
  PRIMARY KEY (group_id, post_id));
CREATE INDEX IF NOT EXISTS posts_created ON posts (group_id, created_at DESC);
CREATE TABLE IF NOT EXISTS fetches (           -- when a feed/listing was last read from Facebook
  key TEXT PRIMARY KEY, fetched_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS watermarks (
  group_id TEXT PRIMARY KEY, ts REAL NOT NULL);
"""


def _dt(v):
    return datetime.fromisoformat(v) if v else None


def post_from_dict(d: dict) -> Post:
    d = dict(d)
    d["created_at"] = _dt(d.get("created_at"))
    d["photos"] = [Photo(**p) for p in d.get("photos") or []]
    d["hints"] = Hints(**(d.get("hints") or {}))
    if d.get("comments") is not None:
        d["comments"] = [Comment(**{**c, "created_at": _dt(c.get("created_at"))}) for c in d["comments"]]
    return Post(**d)


def group_from_dict(d: dict) -> Group:
    d = dict(d)
    d["last_post_time"] = _dt(d.get("last_post_time"))
    return Group(**d)


def _hash(text: str) -> str:
    return hashlib.sha1(text.encode()).hexdigest()[:16]


class Store:
    def __init__(self, path: Path):
        self.path = path
        self.db: aiosqlite.Connection | None = None
        self._write = asyncio.Lock()   # read-modify-write sequences must not interleave across awaits

    async def open(self) -> "Store":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.db = await aiosqlite.connect(self.path)
        await self.db.executescript(SCHEMA)
        await self.db.commit()
        return self

    async def close(self) -> None:
        if self.db:
            await self.db.close()
            self.db = None

    # ---- fetch freshness ----
    async def fetched_at(self, key: str) -> float | None:
        async with self.db.execute("SELECT fetched_at FROM fetches WHERE key=?", (key,)) as cur:
            row = await cur.fetchone()
        return row[0] if row else None

    async def mark_fetched(self, key: str) -> None:
        await self.db.execute("INSERT OR REPLACE INTO fetches VALUES (?, ?)", (key, time.time()))
        await self.db.commit()

    async def is_fresh(self, key: str, max_age: float) -> bool:
        t = await self.fetched_at(key)
        return t is not None and time.time() - t <= max_age

    # ---- groups ----
    async def upsert_groups(self, groups: Iterable[Group]) -> None:
        """Store groups; a field missing in the new record (None/"") keeps its previously known value."""
        now = time.time()
        async with self._write:
            for g in groups:
                async with self.db.execute("SELECT data FROM groups WHERE id=?", (g.id,)) as cur:
                    row = await cur.fetchone()
                merged = g.to_dict()
                for k, v in (json.loads(row[0]) if row else {}).items():
                    if merged.get(k) in (None, "") and v not in (None, ""):
                        merged[k] = v
                await self.db.execute("INSERT OR REPLACE INTO groups VALUES (?, ?, ?)", (g.id, json.dumps(merged), now))
            await self.db.commit()

    async def groups(self) -> list[Group]:
        async with self.db.execute("SELECT data FROM groups") as cur:
            rows = await cur.fetchall()
        return [group_from_dict(json.loads(r[0])) for r in rows]

    async def group(self, group_id: str) -> Group | None:
        async with self.db.execute("SELECT data FROM groups WHERE id=?", (group_id,)) as cur:
            row = await cur.fetchone()
        return group_from_dict(json.loads(row[0])) if row else None

    # ---- posts ----
    async def upsert_posts(self, posts: Iterable[Post]) -> tuple[list[Post], list[Post]]:
        """Store posts. Returns (new, edited): posts never seen before, and seen posts whose text changed."""
        new, edited = [], []
        now = time.time()
        async with self._write:
            await self._upsert_posts(posts, new, edited, now)
        return new, edited

    async def _upsert_posts(self, posts, new, edited, now) -> None:
        for p in posts:
            gid = p.group_id or ""
            async with self.db.execute("SELECT text_hash, data FROM posts WHERE group_id=? AND post_id=?",
                                       (gid, p.post_id)) as cur:
                row = await cur.fetchone()
            h = _hash(p.text)
            d = p.to_dict()
            if row:
                old = json.loads(row[1])
                if d.get("comments") is None and old.get("comments") is not None:
                    d["comments"] = old["comments"]          # don't lose fetched comments
                if row[0] != h:
                    edited.append(p)
                await self.db.execute("UPDATE posts SET text_hash=?, data=?, last_seen=? WHERE group_id=? AND post_id=?",
                                      (h, json.dumps(d), now, gid, p.post_id))
            else:
                new.append(p)
                await self.db.execute("INSERT INTO posts VALUES (?, ?, ?, ?, ?, ?, ?)",
                                      (gid, p.post_id, p.created_at.timestamp() if p.created_at else None, h,
                                       json.dumps(d), now, now))
        await self.db.commit()

    async def posts(self, group_ids: list[str] | None = None, since: datetime | None = None,
                    limit: int = 1000) -> list[Post]:
        """Cached posts, newest first."""
        q, args = "SELECT data FROM posts WHERE 1=1", []
        if group_ids:
            q += f" AND group_id IN ({','.join('?' * len(group_ids))})"
            args += group_ids
        if since:
            q += " AND created_at > ?"
            args.append(since.timestamp())
        q += " ORDER BY created_at DESC LIMIT ?"
        args.append(limit)
        async with self.db.execute(q, args) as cur:
            rows = await cur.fetchall()
        return [post_from_dict(json.loads(r[0])) for r in rows]

    async def post(self, group_id: str, post_id: str) -> Post | None:
        async with self.db.execute("SELECT data FROM posts WHERE group_id=? AND post_id=?", (group_id, post_id)) as cur:
            row = await cur.fetchone()
        return post_from_dict(json.loads(row[0])) if row else None

    # ---- watermarks (newest created_at already delivered by new_posts) ----
    async def watermark(self, group_id: str) -> float | None:
        async with self.db.execute("SELECT ts FROM watermarks WHERE group_id=?", (group_id,)) as cur:
            row = await cur.fetchone()
        return row[0] if row else None

    async def set_watermark(self, group_id: str, ts: float) -> None:
        await self.db.execute("INSERT INTO watermarks VALUES (?, ?) ON CONFLICT(group_id) DO UPDATE SET ts=max(ts, excluded.ts)",
                              (group_id, ts))
        await self.db.commit()
