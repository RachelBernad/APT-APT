# engine.py
"""Scrape-once-per-location, match-per-user, route pipeline.

A saved search holds many area targets + one filter set. We collect the distinct
(scrape-scope, feature-set) signatures across every target of every active search,
scrape each once with the loosest union of its members' filters, then match each
listing against every watcher's exact filters/targets locally and route with
per-search dedup, capped backfill, only-new and price-change semantics.

Boolean features (elevator, ממ״ד, balcony, ...) are the only filters markers can't
carry, so they are part of the scrape signature (pushed server-side); everything
else is matched locally.
"""
from __future__ import annotations

import asyncio
import logging
import random
from collections import defaultdict
from typing import Awaitable, Callable, Dict, FrozenSet, List, Optional, Tuple

import aiohttp

import config
import rentlyfly
import yad2_gateway
from db import Database
from formatting import (format_apartment_message, format_backfill_intro,
                       format_monitor_started_empty, format_price_change)
from models import (GatewayFilters, LocationSignature, LocationTarget,
                   SavedSearch, normalize_name)

logger = logging.getLogger(__name__)

Sender = Callable[[int, str], Awaitable[bool]]

# group key = (scrape signature, required-feature set)
GroupKey = Tuple[LocationSignature, FrozenSet[str]]
# a member of a group: the search + which of its targets fall in this signature
Member = Tuple[SavedSearch, List[LocationTarget]]


def _to_float(value) -> Optional[float]:
    try:
        return float(value) if value not in ("", None) else None
    except (TypeError, ValueError):
        return None


def _uses_rentlyfly(s: SavedSearch) -> bool:
    return s.source_mode in ("auto", "yad2+rentlyfly")


# --- grouping & union filters ---

def scrape_sig(t: LocationTarget) -> LocationSignature:
    """Most-specific scrape scope for a target (region is always required)."""
    if t.city_id:
        return LocationSignature(t.region_id, None, t.city_id)
    if t.area_id:
        return LocationSignature(t.region_id, t.area_id, None)
    return LocationSignature(t.region_id, None, None)


def group_searches(searches: List[SavedSearch]) -> Dict[GroupKey, List[Member]]:
    """Group (search, target) pairs by (scrape signature, feature set)."""
    groups: Dict[GroupKey, Dict[int, Member]] = defaultdict(dict)
    for s in searches:
        feats = frozenset(s.features or [])
        for t in s.locations:
            key = (scrape_sig(t), feats)
            member = groups[key].get(s.id)
            if member is None:
                groups[key][s.id] = (s, [t])
            else:
                member[1].append(t)
    return {k: list(v.values()) for k, v in groups.items()}


def _loosest_min(values: List[Optional[float]]) -> Optional[float]:
    return None if any(v is None for v in values) else min(values)


def _loosest_max(values: List[Optional[float]]) -> Optional[float]:
    return None if any(v is None for v in values) else max(values)


def union_filters(members: List[Member], features: FrozenSet[str]) -> GatewayFilters:
    searches = [s for s, _ in members]
    # property-type ids: union (server does OR) — unless any watcher wants all types.
    property_ids: Optional[List[int]] = None
    if all(s.property_types for s in searches):
        ids = set()
        for s in searches:
            ids.update(s.property_types or [])
        property_ids = sorted(ids)
    return GatewayFilters(
        min_price=_int_or_none(_loosest_min([s.min_price for s in searches])),
        max_price=_int_or_none(_loosest_max([s.max_price for s in searches])),
        min_rooms=_loosest_min([s.min_rooms for s in searches]),
        max_rooms=_loosest_max([s.max_rooms for s in searches]),
        min_sqm=_int_or_none(_loosest_min([s.min_sqm for s in searches])),
        max_sqm=_int_or_none(_loosest_max([s.max_sqm for s in searches])),
        min_floor=_int_or_none(_loosest_min([s.min_floor for s in searches])),
        max_floor=_int_or_none(_loosest_max([s.max_floor for s in searches])),
        property_ids=property_ids,
        features=list(features),
    )


def _int_or_none(v: Optional[float]) -> Optional[int]:
    return None if v is None else int(v)


# --- matching ---

def matches_source(listing: Dict, s: SavedSearch) -> bool:
    if listing.get("source") == "rentlyfly" and not _uses_rentlyfly(s):
        return False
    return True


def matches_location(listing: Dict, targets: List[LocationTarget]) -> bool:
    """A listing matches if any target covers it.

    region/area/city targets are already guaranteed by the scrape scope; hood/
    street targets are checked locally by normalized name.
    """
    for t in targets:
        if t.level in ("region", "area", "city"):
            return True
        if t.level == "hood" and t.match_name and normalize_name(listing.get("hood", "")) == t.match_name:
            return True
        if t.level == "street" and t.match_name and normalize_name(listing.get("street", "")) == t.match_name:
            return True
    return False


def matches_non_amenity(listing: Dict, s: SavedSearch) -> bool:
    price = _to_float(listing.get("price"))
    if s.min_price is not None and (price is None or price < s.min_price):
        return False
    if s.max_price is not None and (price is None or price > s.max_price):
        return False

    rooms = _to_float(listing.get("rooms"))
    if s.min_rooms is not None and (rooms is None or rooms < s.min_rooms):
        return False
    if s.max_rooms is not None and (rooms is None or rooms > s.max_rooms):
        return False

    # Size / floor are only known for some sources — only reject when reported.
    size = _to_float(listing.get("size"))
    if s.min_sqm is not None and size is not None and size < s.min_sqm:
        return False
    if s.max_sqm is not None and size is not None and size > s.max_sqm:
        return False

    floor = _to_float(listing.get("floor"))
    if s.min_floor is not None and floor is not None and floor < s.min_floor:
        return False
    if s.max_floor is not None and floor is not None and floor > s.max_floor:
        return False

    # Property type / condition come off the marker (id); reject only when known.
    if s.property_types:
        pid = listing.get("property_type_id")
        if pid is not None and pid not in s.property_types:
            return False
    if s.property_condition is not None:
        cond = listing.get("property_condition")
        if cond is not None and cond != s.property_condition:
            return False
    return True


def matches_features(listing: Dict, s: SavedSearch) -> bool:
    # Every required feature must be present. Yad2 listings carry exactly the
    # features filtered server-side for their group; rentlyfly reports inline.
    return set(s.features or []).issubset(set(listing.get("features") or []))


def matches_full(listing: Dict, s: SavedSearch, targets: List[LocationTarget]) -> bool:
    return (
        matches_source(listing, s)
        and matches_location(listing, targets)
        and matches_non_amenity(listing, s)
        and matches_features(listing, s)
    )


# --- scraping ---

async def scrape_group(
    http: aiohttp.ClientSession, sig: LocationSignature, f: GatewayFilters,
    members: List[Member],
) -> Dict[str, Dict]:
    listings: Dict[str, Dict] = {}
    for marker in await yad2_gateway.fetch_map(http, sig, f):
        listing = yad2_gateway.normalize_marker(marker, f)
        listings[listing["uid"]] = listing

    if (
        config.ENABLE_RENTLYFLY
        and sig.city_id == config.TEL_AVIV_CITY_ID
        and any(_uses_rentlyfly(s) for s, _ in members)
    ):
        for listing in await rentlyfly.fetch_tel_aviv(http, f):
            listings.setdefault(listing["uid"], listing)

    logger.info(
        "Scraped %d listings for region=%s area=%s city=%s features=%s (watchers=%d)",
        len(listings), sig.region_id, sig.area_id, sig.city_id, f.features, len(members),
    )
    return listings


# --- routing ---

async def _pace() -> None:
    await asyncio.sleep(
        random.uniform(config.MIN_MESSAGE_DELAY_SECONDS, config.MAX_MESSAGE_DELAY_SECONDS)
    )


def _newest_first(listings: List[Dict]) -> List[Dict]:
    return sorted(listings, key=lambda l: (l.get("order_id") or 0), reverse=True)


async def route_notifications(
    send: Sender, db: Database, s: SavedSearch, finals: List[Dict],
    notified: Dict[int, set],
) -> None:
    """Deliver matches for one search.

    - **unprimed** (new / after edit|unmute|re-enable): send a capped *newest*
      backfill sample with a clear explanation, then record every current match as
      seen and mark primed. This is the "here's a taster, now I'll watch" moment.
    - **primed**: send only genuinely-new listings and price changes; a listing is
      recorded as seen only once its send actually succeeds (so a transient failure
      retries), and never sent to the same chat twice in one cycle.
    """
    # Re-read the freshest priming state: the immediate scan and the periodic cycle
    # can both reach a brand-new search — whichever primes first wins, the other
    # falls through to only-new (prevents a double backfill / spam).
    fresh = await db.get_search(s.id)
    if fresh is None:
        return
    s = fresh
    seen = await db.get_seen_map(s.id)
    chat_seen = notified.setdefault(s.chat_id, set())
    interval_min = max(1, config.CHECK_INTERVAL_SECONDS // 60)

    if not s.is_primed:
        # Record seen + mark primed FIRST so a concurrent path can't also backfill;
        # then send the sample. (If the DB write fails we abort without sending,
        # so we never spam without recording.)
        to_insert = [(l["uid"], l.get("price")) for l in finals]
        if to_insert:
            await db.bulk_upsert_seen(s.id, to_insert)
        await db.mark_primed(s.id)

        # Describe the whole monitor (not just one area) so it reads as one thing.
        areas = [t.display_name for t in s.locations]
        summary = ", ".join(areas[:2]) + (f" +{len(areas) - 2} more" if len(areas) > 2 else "")
        summary = summary or s.label

        sample = _newest_first(finals)[: config.BACKFILL_CAP]
        if sample:
            await send(s.chat_id, format_backfill_intro(summary, len(sample), interval_min))
            await _pace()
            for listing in sample:
                uid = listing["uid"]
                if uid in chat_seen:
                    continue
                if await send(s.chat_id, format_apartment_message(listing)):
                    chat_seen.add(uid)
                    await _pace()
        else:
            await send(s.chat_id, format_monitor_started_empty(summary, interval_min))
        logger.info("Primed search #%s (%s): backfilled %d of %d current matches",
                    s.id, s.label, len(sample), len(finals))
        return

    to_insert: List[tuple] = []
    for listing in finals:
        uid = listing["uid"]
        price = listing.get("price")
        if uid not in seen:
            if uid in chat_seen:
                to_insert.append((uid, price))
            elif await send(s.chat_id, format_apartment_message(listing)):
                chat_seen.add(uid)
                to_insert.append((uid, price))
                await _pace()
            # send failed -> do NOT record, retry next cycle
        elif seen[uid] != price and price is not None:
            if uid in chat_seen:
                await db.update_seen_price(s.id, uid, price)
            elif await send(s.chat_id, format_price_change(listing, seen[uid])):
                chat_seen.add(uid)
                await db.update_seen_price(s.id, uid, price)
                await _pace()
    if to_insert:
        await db.bulk_upsert_seen(s.id, to_insert)


async def collect_user_matches(
    db: Database, http: aiohttp.ClientSession, chat_id: int
) -> List[Dict]:
    """Scrape all of one user's active monitors and return the union of their
    current matches (deduped by uid). Used by the on-demand daily report."""
    searches = [s for s in await db.list_searches(chat_id) if s.is_active]
    if not searches:
        return []
    groups = group_searches(searches)
    sem = asyncio.Semaphore(max(1, config.SCRAPE_CONCURRENCY))

    async def one(key, members):
        sig, feats = key
        async with sem:
            try:
                return members, await scrape_group(http, sig, union_filters(members, feats), members)
            except Exception as exc:
                logger.error("Report scrape failed for %s: %s", sig, exc)
                return members, None

    results = await asyncio.gather(*(one(k, m) for k, m in groups.items()))
    union: Dict[str, Dict] = {}
    for members, listings in results:
        if not listings:
            continue
        for s, targets in members:
            for l in listings.values():
                if l["uid"] not in union and matches_full(l, s, targets):
                    union[l["uid"]] = l
    return list(union.values())


async def scan_search(send: Sender, db: Database, http: aiohttp.ClientSession,
                     search_id: int) -> None:
    """Scrape + route a single search immediately (used right after it is saved so
    the user gets their backfill sample within seconds, not on the next cycle)."""
    s = await db.get_search(search_id)
    if not s or not s.is_active or not await db.is_user_active(s.chat_id):
        return
    groups = group_searches([s])
    finals: Dict[str, Dict] = {}
    failed = False
    for (sig, feats), members in groups.items():
        try:
            listings = await scrape_group(http, sig, union_filters(members, feats), members)
        except Exception as exc:
            logger.error("Immediate scan scrape failed for #%s: %s", search_id, exc)
            failed = True
            continue
        if not listings:
            failed = True
            continue
        for s2, targets in members:
            for l in listings.values():
                if l["uid"] not in finals and matches_full(l, s2, targets):
                    finals[l["uid"]] = l
    if failed and not finals:
        logger.warning("Immediate scan for #%s got nothing (scrape failed?) — leaving for next cycle.",
                      search_id)
        return
    await route_notifications(send, db, s, list(finals.values()), {})


async def run_cycle(send: Sender, db: Database, http: aiohttp.ClientSession) -> None:
    searches = await db.get_active_searches()
    if not searches:
        logger.info("No active searches; skipping cycle.")
        return

    groups = group_searches(searches)
    logger.info("Running cycle: %d active searches in %d scrape groups",
                len(searches), len(groups))

    # Scrape groups CONCURRENTLY (bounded), then accumulate each search's matches
    # across all the groups it spans — a search with targets in several cities/
    # feature-sets participates in several groups but must be routed exactly ONCE.
    search_obj: Dict[int, SavedSearch] = {}
    search_finals: Dict[int, Dict[str, Dict]] = defaultdict(dict)  # sid -> uid -> listing
    failed: set = set()  # searches whose scrape (any group) failed — skip to avoid flood

    sem = asyncio.Semaphore(max(1, config.SCRAPE_CONCURRENCY))

    async def scrape_one(key, members):
        sig, feats = key
        async with sem:
            try:
                listings = await scrape_group(http, sig, union_filters(members, feats), members)
            except Exception as exc:
                logger.error("Scrape failed for group %s/%s: %s", sig, list(feats), exc)
                return key, members, None
            return key, members, listings

    results = await asyncio.gather(*(scrape_one(k, m) for k, m in groups.items()))

    for (sig, feats), members, listings in results:
        if not listings:
            # Empty/None almost always means a transient failure (bot-challenge /
            # network), not a genuinely empty scope. Mark its searches failed so we
            # don't prime on a partial view and flood on the next successful cycle.
            logger.warning("Empty scrape for region=%s area=%s city=%s features=%s.",
                          sig.region_id, sig.area_id, sig.city_id, list(feats))
            for s, _ in members:
                failed.add(s.id)
            continue
        for s, targets in members:
            search_obj[s.id] = s
            bucket = search_finals[s.id]
            for l in listings.values():
                if l["uid"] not in bucket and matches_full(l, s, targets):
                    bucket[l["uid"]] = l

    notified: Dict[int, set] = {}  # chat_id -> uids already sent this cycle
    for sid, s in search_obj.items():
        if sid in failed:
            logger.warning("Skipping search #%s this cycle — a scrape group failed.", sid)
            continue
        try:
            await route_notifications(send, db, s, list(search_finals[sid].values()), notified)
        except Exception as exc:
            logger.error("Routing failed for search #%s: %s", s.id, exc)
