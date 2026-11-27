"""Scrape-once-per-location, match-per-user, route pipeline.

A saved search holds many area targets + one filter set. We collect the distinct
scrape scopes across every target of every active search, scrape each once
UNFILTERED, then match each listing against every watcher's filters/targets locally
and route with per-search dedup, capped backfill, only-new and price-change semantics.

Filters are lenient: a listing is dropped only for a value it REPORTS outside the
filter; a missing value always passes (and is shown as ❔). Server-side ranges would
silently drop listings with no price, so none are sent on the main fetch.

Boolean features (elevator, ממ״ד, balcony, ...) aren't on markers. They never drop a
Yad2 listing: one extra ticked-only fetch per wanted feature tags the listings that
have it (✅) and marks the rest it covered as checked (❌: the box isn't ticked, so
most likely it doesn't have it). A listing the fetch couldn't cover (it failed, or the
listing lacks a value the fetch was narrowed by) stays unknown (❔).
"""
from __future__ import annotations

import asyncio
import logging
import random
from collections import defaultdict
from dataclasses import replace
from typing import Awaitable, Callable, Dict, FrozenSet, List, Optional, Tuple

import aiohttp

from aptbot import config
from aptbot.scrapers.yad2 import gateway as yad2_gateway
from aptbot.db import Database
from aptbot.bot.formatting import (format_apartment_message, format_backfill_intro,
                       format_monitor_started_empty, format_price_change,
                       main_image)
from aptbot.models import (GatewayFilters, LocationSignature, LocationTarget,
                   SavedSearch, normalize_name)

logger = logging.getLogger(__name__)

Sender = Callable[..., Awaitable[bool]]   # (chat_id, text, photo=None) -> bool

# group key = (scrape signature, union of the members' wanted features to tag)
GroupKey = Tuple[LocationSignature, FrozenSet[str]]
# a member of a group: the search + which of its targets fall in this signature
Member = Tuple[SavedSearch, List[LocationTarget]]


def _to_float(value) -> Optional[float]:
    try:
        return float(value) if value not in ("", None) else None
    except (TypeError, ValueError):
        return None


# --- grouping & union filters ---

def scrape_sig(t: LocationTarget) -> LocationSignature:
    """Most-specific scrape scope for a target (region is always required)."""
    if t.city_id:
        return LocationSignature(t.region_id, None, t.city_id)
    if t.area_id:
        return LocationSignature(t.region_id, t.area_id, None)
    return LocationSignature(t.region_id, None, None)


def group_searches(searches: List[SavedSearch]) -> Dict[GroupKey, List[Member]]:
    """Group (search, target) pairs by scrape signature. The key also carries the
    union of the members' wanted features (what the tagging fetches must cover)."""
    by_sig: Dict[LocationSignature, Dict[int, Member]] = defaultdict(dict)
    for s in searches:
        for t in s.locations:
            members = by_sig[scrape_sig(t)]
            member = members.get(s.id)
            if member is None:
                members[s.id] = (s, [t])
            else:
                member[1].append(t)
    out: Dict[GroupKey, List[Member]] = {}
    for sig, members in by_sig.items():
        feats = frozenset(f for s, _ in members.values() for f in (s.features or []))
        out[(sig, feats)] = list(members.values())
    return out


def _loosest_min(values: List[Optional[float]]) -> Optional[float]:
    return None if any(v is None for v in values) else min(values)


def _loosest_max(values: List[Optional[float]]) -> Optional[float]:
    return None if any(v is None for v in values) else max(values)


def union_filters(members: List[Member], features: FrozenSet[str]) -> GatewayFilters:
    """The loosest union of the group's filters, with the group's features to tag.

    Used only to narrow the per-feature TAGGING fetches (the main fetch is unfiltered):
    a listing outside every member's range is dropped anyway, and one missing a value
    simply stays ❔.
    """
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
        features=sorted(features),
    )


def _int_or_none(v: Optional[float]) -> Optional[int]:
    return None if v is None else int(v)


# --- matching ---

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


def _in_server_range(listing: Dict, f: GatewayFilters) -> bool:
    """Would a tagging fetch narrowed by ``f`` have returned this listing had its box been
    ticked? Only then does "not returned" mean "not ticked". A missing value under a
    narrowed range is not covered (the server drops it), so its feature stays ❔."""
    for key, lo, hi in (("price", f.min_price, f.max_price), ("rooms", f.min_rooms, f.max_rooms),
                        ("size", f.min_sqm, f.max_sqm), ("floor", f.min_floor, f.max_floor)):
        if lo is None and hi is None:
            continue
        v = _to_float(listing.get(key))
        if v is None or _outside(v, lo, hi):
            return False
    if f.property_ids and listing.get("property_type_id") not in f.property_ids:
        return False
    return True


def _outside(value: Optional[float], lo, hi) -> bool:
    """True only for a REPORTED value outside [lo, hi]; a missing value never is."""
    if value is None:
        return False
    return (lo is not None and value < lo) or (hi is not None and value > hi)


def matches_non_amenity(listing: Dict, s: SavedSearch) -> bool:
    """Lenient: reject only on a value the listing reports outside the filter."""
    if _outside(_to_float(listing.get("price")), s.min_price, s.max_price):
        return False
    if _outside(_to_float(listing.get("rooms")), s.min_rooms, s.max_rooms):
        return False
    if _outside(_to_float(listing.get("size")), s.min_sqm, s.max_sqm):
        return False
    if _outside(_to_float(listing.get("floor")), s.min_floor, s.max_floor):
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


def matches_full(listing: Dict, s: SavedSearch, targets: List[LocationTarget]) -> bool:
    # Features never drop a Yad2 listing (they only tag it ✅ / ❔), so they aren't here.
    return matches_location(listing, targets) and matches_non_amenity(listing, s)


# --- scraping ---

async def scrape_group(
    http: aiohttp.ClientSession, sig: LocationSignature, f: GatewayFilters,
    members: List[Member],
) -> Dict[str, Dict]:
    """Main fetch with no filters (nothing silently dropped), then one ticked-only
    fetch per wanted feature, narrowed by ``f``'s ranges, to tag who has it. A failed
    tagging fetch only leaves its feature unknown (❔) on this cycle's listings.

    ``features_checked`` lists the features a listing was covered for: ticked → in
    ``features`` (✅), else not ticked (❌)."""
    listings: Dict[str, Dict] = {}
    for marker in await yad2_gateway.fetch_map(http, sig, GatewayFilters()):
        listing = yad2_gateway.normalize_marker(marker)
        listing["features_checked"] = []
        listings[listing["uid"]] = listing

    if listings:
        covered = [l for l in listings.values() if _in_server_range(l, f)]
        for feat in f.features:
            try:
                markers = await yad2_gateway.fetch_map(http, sig, replace(f, features=[feat]))
            except Exception as exc:
                logger.warning("Yad2 tagging fetch for %r failed (%s); it stays unknown.", feat, exc)
                continue
            for m in markers:
                listing = listings.get(f"yad2:{m.get('token', '')}")
                if listing is not None and feat not in listing["features"]:
                    listing["features"].append(feat)
                    if feat == "mamad":
                        listing["is_mamad"] = True
            for listing in covered:
                listing["features_checked"].append(feat)

    # Routine per-cycle bookkeeping: DEBUG, so a healthy cycle stays silent.
    logger.debug(
        "Scraped %d listings for region=%s area=%s city=%s, tagged features=%s (watchers=%d)",
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
                if await send(s.chat_id, format_apartment_message(listing, s), main_image(listing)):
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
            elif await send(s.chat_id, format_apartment_message(listing, s), main_image(listing)):
                chat_seen.add(uid)
                to_insert.append((uid, price))
                await _pace()
            # send failed -> do NOT record, retry next cycle
        elif seen[uid] != price and price is not None:
            if uid in chat_seen:
                await db.update_seen_price(s.id, uid, price)
            elif await send(s.chat_id, format_price_change(listing, seen[uid]), main_image(listing)):
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
        logger.debug("No active searches; skipping cycle.")
        return

    groups = group_searches(searches)
    logger.debug("Running cycle: %d active searches in %d scrape groups",
                 len(searches), len(groups))

    # Scrape groups CONCURRENTLY (bounded), then accumulate each search's matches
    # across all the groups it spans — a search with targets in several cities/
    # feature-sets participates in several groups but must be routed exactly ONCE.
    search_obj: Dict[int, SavedSearch] = {}
    search_finals: Dict[int, Dict[str, Dict]] = defaultdict(dict)  # sid -> uid -> listing
    failed: set = set()  # searches whose scrape (any group) failed — skip to avoid flood

    # Count what actually reached a user, so a cycle that did nothing logs nothing.
    # Wrapping the sender keeps route_notifications untouched.
    sent = 0
    chats: set = set()
    bad_groups = 0
    route_errors = 0

    async def counting_send(chat_id, text, photo=None) -> bool:
        nonlocal sent
        ok = await send(chat_id, text, photo)
        if ok:
            sent += 1
            chats.add(chat_id)
        return ok

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
            bad_groups += 1
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
            await route_notifications(counting_send, db, s, list(search_finals[sid].values()), notified)
        except Exception as exc:
            route_errors += 1
            logger.error("Routing failed for search #%s: %s", s.id, exc)

    # Only speak up when the cycle delivered something or hit trouble; a healthy,
    # uneventful cycle is silent at INFO (it used to log ~4 lines every 15 min).
    if sent or bad_groups or failed or route_errors:
        logger.info(
            "Cycle: sent %d message(s) to %d chat(s) across %d searches/%d groups"
            " — %d bad scrape(s), %d search(es) skipped, %d routing error(s).",
            sent, len(chats), len(searches), len(groups),
            bad_groups, len(failed), route_errors)
    else:
        logger.debug("Cycle clean: nothing new (%d searches, %d groups).",
                     len(searches), len(groups))
