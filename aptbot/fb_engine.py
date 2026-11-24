"""Facebook groups source: poll the watched groups, dedup by content, route to monitors.

Separate from the Yad2 ``engine`` on purpose: Facebook is read through a real,
logged-in browser (``aptbot.scrapers.facebook_generic_browser``) with an
account-wide budget, so it polls on its own slower, jittered schedule and stops
for good on any account-safety signal instead of retrying.

Dedup key = :func:`content_hash` (the post text with leading/trailing whitespace
stripped), so the same apartment cross-posted to several groups arrives once.
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Awaitable, Callable, Iterable, List, Optional

from aptbot import config
from aptbot.bot.formatting import format_fb_backfill_intro, format_fb_stopped
from aptbot.db import Database
from aptbot.engine import _pace
from aptbot.models import SavedSearch
from aptbot.scrapers.facebook_generic_browser import AccountAtRisk, BudgetExhausted, Halted, Post

logger = logging.getLogger(__name__)

PostSender = Callable[[int, Post], Awaitable[bool]]
TextSender = Callable[..., Awaitable[bool]]

# Cached posts older than this aren't used for the start-up taster: their signed photo
# URLs expire after ~4 days, and a stale "newest" post isn't news.
TASTER_MAX_AGE = timedelta(hours=48)


def content_hash(post: Post) -> str:
    """Dedup key: sha256 of the text with leading/trailing spaces/newlines stripped.
    A photo-only post hashes its photo ids; a post with neither falls back to its id."""
    text = (post.text or "").strip()
    if text:
        return hashlib.sha256(text.encode("utf-8")).hexdigest()
    ids = sorted(p.id for p in post.photos if p.id)
    if ids:
        return hashlib.sha256(("photos:" + ",".join(ids)).encode("utf-8")).hexdigest()
    return f"post:{post.group_id}:{post.post_id}"


def unique_by_content(posts: Iterable[Post]) -> List[Post]:
    """Newest first, one post per content hash (the newest copy wins)."""
    ordered = sorted(posts, key=lambda p: p.created_at.timestamp() if p.created_at else 0, reverse=True)
    seen, out = set(), []
    for p in ordered:
        h = content_hash(p)
        if h not in seen:
            seen.add(h)
            out.append(p)
    return out


@dataclass
class FbState:
    """What /status reports about the Facebook side."""
    last_cycle_at: Optional[datetime] = None
    last_new: int = 0              # posts returned by the last poll
    last_sent: int = 0             # messages delivered by the last poll
    stopped_reason: Optional[str] = None
    errors: int = 0
    wake: asyncio.Event = field(default_factory=asyncio.Event)


async def route_fb_posts(send_post: PostSender, send_text: TextSender, db: Database,
                         s: SavedSearch, posts: List[Post], taster: List[Post]) -> int:
    """Deliver posts to one monitor. Returns how many were sent.

    - **not FB-primed**: record every post (this poll + the taster pool) as seen and
      mark primed FIRST (so a concurrent path can't also backfill), then send the
      newest ``FB_BACKFILL_CAP`` as a taster.
    - **primed**: send each post whose content hash is new, oldest first; record it
      only after a successful send, so a failed send retries next poll.
    """
    fresh = await db.get_search(s.id)
    if fresh is None or not fresh.is_active:
        return 0
    seen = await db.get_fb_seen(s.id)
    sent = 0

    if not fresh.fb_primed:
        pool = unique_by_content([*posts, *taster])
        await db.add_fb_seen(s.id, [(content_hash(p), p.url) for p in pool])
        await db.mark_fb_primed(s.id)
        sample = [p for p in pool if content_hash(p) not in seen][: config.FB_BACKFILL_CAP]
        if sample:
            groups = len(config.FB_GROUP_IDS)
            await send_text(s.chat_id, format_fb_backfill_intro(len(sample), groups))
            await _pace()
            for p in sample:
                if await send_post(s.chat_id, p):
                    sent += 1
                    await _pace()
        logger.info("FB primed search #%s: taster %d of %d known posts", s.id, sent, len(pool))
        return sent

    for p in reversed(unique_by_content(posts)):      # oldest first reads naturally
        h = content_hash(p)
        if h in seen:
            continue
        if await send_post(s.chat_id, p):
            await db.add_fb_seen(s.id, [(h, p.url)])
            seen.add(h)
            sent += 1
            await _pace()
    return sent


def _allowed(searches: List[SavedSearch]) -> List[SavedSearch]:
    return [s for s in searches if s.chat_id in config.ALLOWED_CHAT_IDS]


async def run_fb_cycle(fb, db: Database, send_post: PostSender, send_text: TextSender,
                       state: FbState) -> Optional[float]:
    """One poll. Returns a forced delay in seconds (budget exhausted) or None.
    Sets ``state.stopped_reason`` on an account-safety stop (the caller stops polling)."""
    searches = _allowed(await db.get_active_searches())
    if not searches:
        logger.debug("FB: no active monitors; not touching Facebook.")
        return None
    try:
        posts = await fb.new_posts(config.FB_GROUP_IDS)
        taster: List[Post] = []
        if any(not s.fb_primed for s in searches):
            since = datetime.now(timezone.utc) - TASTER_MAX_AGE
            taster = await fb.filter_posts(group_ids=config.FB_GROUP_IDS, since=since,
                                           limit=max(50, config.FB_BACKFILL_CAP * 4))
    except (Halted, AccountAtRisk) as exc:
        reason = getattr(exc, "reason", None) or str(exc) or type(exc).__name__
        state.stopped_reason = reason
        logger.error("FB stopped for account safety: %s", reason)
        for chat in sorted({s.chat_id for s in searches}):
            await send_text(chat, format_fb_stopped(reason))
        return None
    except BudgetExhausted as exc:
        logger.warning("FB budget exhausted; next poll in %.0fs", exc.retry_after)
        return float(exc.retry_after)

    state.last_cycle_at = datetime.now(timezone.utc)
    state.last_new = len(posts)
    sent = 0
    for s in searches:
        try:
            sent += await route_fb_posts(send_post, send_text, db, s, posts, taster)
        except Exception as exc:
            state.errors += 1
            logger.error("FB routing failed for search #%s: %s", s.id, exc)
    state.last_sent = sent
    if sent or posts:
        logger.info("FB cycle: %d new post(s) from the groups, %d message(s) sent.", len(posts), sent)
    return None


async def fb_loop(fb, db: Database, send_post: PostSender, send_text: TextSender,
                  state: FbState, *, first_delay: float = 10.0) -> None:
    """Poll forever on a jittered 20-35 min schedule (or sooner when ``state.wake``
    is set by /monitor). Exits for good once Facebook reports an account-safety stop."""
    delay = first_delay
    while True:
        try:
            await asyncio.wait_for(state.wake.wait(), timeout=delay)
        except asyncio.TimeoutError:
            pass
        state.wake.clear()
        forced = None
        started = time.monotonic()
        try:
            forced = await run_fb_cycle(fb, db, send_post, send_text, state)
        except Exception as exc:              # parse drift, timeouts, browser crash: try next time
            state.errors += 1
            logger.error("FB cycle failed: %s: %s", type(exc).__name__, exc)
        if state.stopped_reason:
            logger.error("FB polling stopped until restart: %s", state.stopped_reason)
            return
        delay = forced if forced is not None else random.uniform(
            config.FB_POLL_MIN_SECONDS, config.FB_POLL_MAX_SECONDS)
        logger.debug("FB cycle took %.0fs; next in %.0fs", time.monotonic() - started, delay)
