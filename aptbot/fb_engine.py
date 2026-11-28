"""Facebook groups source: poll the watched groups, dedup by content, route to monitors.

Separate from the Yad2 ``engine`` on purpose: Facebook is read through a real,
logged-in browser (``aptbot.scrapers.facebook_generic_browser``), one tab acting
at a time, so it polls on its own slower, jittered schedule and stops
for good on any account-safety signal instead of retrying.

Dedup key = :func:`content_hash` (the post text with leading/trailing whitespace
stripped), so the same apartment cross-posted to several groups arrives once.

Optional AI analysis (``aptbot.listing_ai``): just before a monitor's posts are sent,
the ones about to go out are analyzed (cached per content hash, so every monitor
reuses one analysis) and each is sent with its Listing Facts, or without them when
the AI is off, paused or failed.
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Awaitable, Callable, Dict, Iterable, List, Optional

from aptbot import config
from aptbot.bot.formatting import format_fb_backfill_intro, format_fb_stopped, format_fb_taster_empty
from aptbot.db import Database
from aptbot.engine import _pace
from aptbot.models import FEATURE_FACT_FIELD, SavedSearch
from aptbot.scrapers.facebook_generic_browser import AccountAtRisk, Halted, Post

logger = logging.getLogger(__name__)

PostSender = Callable[..., Awaitable[bool]]   # (chat, post, facts, search=SavedSearch) -> bool
TextSender = Callable[..., Awaitable[bool]]
Analyze = Callable[[List[Post]], Awaitable[Dict[str, object]]]          # posts -> {content_hash: facts}

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


def analyzer_inputs(posts: Iterable[Post]):
    """fbgb posts -> listing_ai inputs keyed by :func:`content_hash`."""
    from aptbot.listing_ai import PostInput
    return [PostInput(key=content_hash(p), text=p.text or "", posted_at=p.created_at,
                      group_name=p.group_name, photo_count=p.photo_count or len(p.photos))
            for p in posts]


def make_analyze(analyzer) -> Optional[Analyze]:
    """Wrap a ``ListingAnalyzer`` (or None) as the ``analyze`` hook. Never raises."""
    if analyzer is None:
        return None

    async def analyze(posts: List[Post]) -> Dict[str, object]:
        try:
            return await analyzer.analyze(analyzer_inputs(posts))
        except Exception as exc:                       # the AI must never block delivery
            logger.error("AI analysis failed: %s: %s", type(exc).__name__, exc)
            return {}
    analyze.is_paused = lambda: bool(getattr(analyzer, "paused", False))
    return analyze


async def _facts_for(analyze: Optional[Analyze], posts: List[Post]) -> Dict[str, object]:
    return await analyze(posts) if analyze is not None and posts else {}


def wanted(facts) -> bool:
    """Send this post? Posts without facts (AI off/failed/paused) always go out; analyzed
    ones only if their Post kind is in ``FB_POST_KINDS`` (empty = every kind)."""
    if facts is None or not config.FB_POST_KINDS:
        return True
    kind = getattr(facts, "post_kind", None)
    return getattr(kind, "value", kind) in config.FB_POST_KINDS


def matches_facts(facts, s: SavedSearch) -> bool:
    """The monitor's filters on a post's AI facts. Lenient like Yad2: drop only on a value
    the post STATES outside the filter (price above max, "אין ממ״ד"); not mentioned (None),
    no facts at all (AI off/failed) and features the AI doesn't extract all pass."""
    if facts is None:
        return True
    for fact, lo, hi in (("price_ils", s.min_price, s.max_price),
                         ("rooms_count", s.min_rooms, s.max_rooms),
                         ("square_meters", s.min_sqm, s.max_sqm),
                         ("floor", s.min_floor, s.max_floor)):
        v = getattr(facts, fact, None)
        if v is not None and ((lo is not None and v < lo) or (hi is not None and v > hi)):
            return False
    for key in s.features or []:
        fact = FEATURE_FACT_FIELD.get(key)
        if fact and getattr(facts, fact, None) is False:
            return False
    return True


async def route_fb_posts(send_post: PostSender, send_text: TextSender, db: Database,
                         s: SavedSearch, posts: List[Post], taster: List[Post],
                         analyze: Optional[Analyze] = None, failed: Optional[list] = None) -> int:
    """Deliver posts to one monitor. Returns how many were sent.

    - **not FB-primed**: record every post (this poll + the taster pool) as seen and
      mark primed FIRST (so a concurrent path can't also backfill), then send the
      newest ``FB_BACKFILL_CAP`` as a taster.
    - **primed**: send each post whose content hash is new, oldest first; record it
      only after a successful send, so a failed send retries next poll.

    With an ``analyze`` hook, posts whose Post kind isn't in ``FB_POST_KINDS`` or that miss
    the monitor's filters (:func:`matches_facts`) are recorded as seen and not sent (the
    taster analyzes a few extra to fill its cap).
    """
    fresh = await db.get_search(s.id)
    if fresh is None or not fresh.is_active:
        return 0
    s = fresh                                      # the latest /filters edit applies

    def keep(p: Post) -> bool:
        f = facts.get(content_hash(p))
        return wanted(f) and matches_facts(f, s)
    seen = await db.get_fb_seen(s.id)
    sent = 0

    if not fresh.fb_primed:
        pool = unique_by_content([*posts, *taster])
        await db.add_fb_seen(s.id, [(content_hash(p), p.url) for p in pool])
        await db.mark_fb_primed(s.id)
        cap = config.FB_BACKFILL_CAP
        sample = [p for p in pool if content_hash(p) not in seen][: cap * (3 if analyze else 1)]
        facts = await _facts_for(analyze, sample)
        sample = [p for p in sample if keep(p)][:cap]
        if sample:
            groups = len(config.FB_GROUP_IDS)
            await send_text(s.chat_id, format_fb_backfill_intro(len(sample), groups))
            await _pace()
            for p in sample:
                if await send_post(s.chat_id, p, facts.get(content_hash(p)), search=s):
                    sent += 1
                    await _pace()
        else:
            await send_text(s.chat_id, format_fb_taster_empty())
        logger.info("FB primed search #%s: taster %d of %d known posts", s.id, sent, len(pool))
        return sent

    pending = [p for p in reversed(unique_by_content(posts))   # oldest first reads naturally
               if content_hash(p) not in seen]
    facts = await _facts_for(analyze, pending)
    skipped = [p for p in pending if not keep(p)]
    if skipped:
        await db.add_fb_seen(s.id, [(content_hash(p), p.url) for p in skipped])
        seen.update(content_hash(p) for p in skipped)
        logger.info("FB search #%s: %d post(s) skipped by kind (FB_POST_KINDS=%s) or filters",
                    s.id, len(skipped), ",".join(sorted(config.FB_POST_KINDS)))
    for p in pending:
        h = content_hash(p)
        if h in seen:
            continue
        if await send_post(s.chat_id, p, facts.get(h), search=s):
            await db.add_fb_seen(s.id, [(h, p.url)])
            seen.add(h)
            sent += 1
            await _pace()
        elif failed is not None:
            failed.append(h)
    return sent


# The chats the bot may send to. ``__main__`` points this at the gate's live set, so a
# group that became a supergroup (new id) keeps getting Facebook posts.
allowed_chats = set(config.ALLOWED_CHAT_IDS)


def _allowed(searches: List[SavedSearch]) -> List[SavedSearch]:
    return [s for s in searches if s.chat_id in allowed_chats or s.chat_id in config.ALLOWED_CHAT_IDS]


async def run_fb_cycle(fb, db: Database, send_post: PostSender, send_text: TextSender,
                       state: FbState, analyze: Optional[Analyze] = None) -> None:
    """One poll.
    Sets ``state.stopped_reason`` on an account-safety stop (the caller stops polling)."""
    searches = _allowed(await db.get_active_searches())
    if not searches:
        logger.debug("FB: no active monitors; not touching Facebook.")
        return None
    if analyze is not None and getattr(analyze, "is_paused", lambda: False)():
        # Without AI facts every post would pass the filters (sale, seeking...). Don't read
        # Facebook now: the watermarks stay put, so these posts come next cycle with facts.
        logger.info("FB: AI analysis paused (usage limit); skipping this poll.")
        return None
    deferred = hasattr(fb, "commit_watermarks")
    try:
        posts = await fb.new_posts(config.FB_GROUP_IDS, **({"commit": False} if deferred else {}))
        state.errors += getattr(fb, "last_group_errors", 0) or 0
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

    state.last_cycle_at = datetime.now(timezone.utc)
    state.last_new = len(posts)
    sent = 0
    failed: list = []
    for s in searches:
        try:
            sent += await route_fb_posts(send_post, send_text, db, s, posts, taster, analyze, failed)
        except Exception as exc:
            state.errors += 1
            failed.append(s.id)
            logger.error("FB routing failed for search #%s: %s", s.id, exc)
    if deferred:
        if failed:
            # Watermarks stay: the same posts come back next poll (fb_seen skips the delivered ones).
            logger.warning("FB: %d delivery failure(s); these posts will be retried next poll.", len(failed))
        else:
            await fb.commit_watermarks()
    state.last_sent = sent
    if sent or posts:
        logger.info("FB cycle: %d new post(s) from the groups, %d message(s) sent.", len(posts), sent)
    return None


def quiet_delay(now: Optional[datetime] = None, spec: Optional[str] = None) -> float:
    """Seconds until polling may resume when ``now`` falls in the quiet window (``FB_QUIET_HOURS``, "HH-HH" in
    Israel time; may wrap midnight), plus 0-15 min jitter. 0 outside it or when unset/malformed."""
    from zoneinfo import ZoneInfo
    spec = config.FB_QUIET_HOURS if spec is None else spec
    try:
        lo, hi = (int(x) for x in spec.split("-"))
    except (ValueError, AttributeError):
        return 0.0
    if lo == hi:
        return 0.0
    tz = ZoneInfo("Asia/Jerusalem")
    local = (now or datetime.now(timezone.utc)).astimezone(tz)
    inside = lo <= local.hour < hi if lo < hi else (local.hour >= lo or local.hour < hi)
    if not inside:
        return 0.0
    end = local.replace(hour=hi, minute=0, second=0, microsecond=0)
    if end <= local:
        end += timedelta(days=1)
    return (end - local).total_seconds() + random.uniform(0, 15 * 60)


async def fb_loop(fb, db: Database, send_post: PostSender, send_text: TextSender,
                  state: FbState, *, first_delay: float = 10.0,
                  analyze: Optional[Analyze] = None) -> None:
    """Poll forever on a jittered FB_POLL_MIN/MAX schedule (8-12 min), paused in FB_QUIET_HOURS (or sooner when ``state.wake``
    is set by /monitor). Exits for good once Facebook reports an account-safety stop."""
    delay = first_delay + quiet_delay(datetime.now(timezone.utc) + timedelta(seconds=first_delay))
    while True:
        try:
            await asyncio.wait_for(state.wake.wait(), timeout=delay)
        except asyncio.TimeoutError:
            pass
        state.wake.clear()
        quiet = quiet_delay()
        if quiet:                             # a /monitor or /filters wake inside the quiet hours
            logger.info("FB: woken inside the quiet hours; waiting %.0f min.", quiet / 60)
            await asyncio.sleep(quiet)
        started = time.monotonic()
        try:
            await run_fb_cycle(fb, db, send_post, send_text, state, analyze)
        except Exception as exc:              # parse drift, timeouts, browser crash: try next time
            state.errors += 1
            logger.error("FB cycle failed: %s: %s", type(exc).__name__, exc)
        if state.stopped_reason:
            logger.error("FB polling stopped until restart: %s", state.stopped_reason)
            return
        delay = random.uniform(config.FB_POLL_MIN_SECONDS, config.FB_POLL_MAX_SECONDS)
        # Never start a cycle inside the quiet hours: the next one waits for the window's end.
        delay = max(delay, quiet_delay(datetime.now(timezone.utc) + timedelta(seconds=delay)) + delay)
        logger.debug("FB cycle took %.0fs; next in %.0fs", time.monotonic() - started, delay)
