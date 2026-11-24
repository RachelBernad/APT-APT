"""Browser jobs: one coroutine per API operation. Each runs in ONE tab, chosen by the dispatcher.

Every job follows the same pattern:
  subscribe to its tab's captured responses → navigate (limiter token, short attention) → health.check →
  parse the page's JSON islands → (scroll in short attention bursts, waiting for Facebook's responses
  OUTSIDE the attention lock, so other tabs get their turn) → parse → return models.
Data always comes from Facebook's own responses (parse.py). The DOM is only used to find things to click.
"""
from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass
from typing import Awaitable, Callable
from urllib.parse import quote

from aptbot.scrapers.facebook_generic_browser import health, human, parse
from aptbot.scrapers.facebook_generic_browser.capture import CaptureHub, Event
from aptbot.scrapers.facebook_generic_browser.errors import NotMember, SchemaDrift
from aptbot.scrapers.facebook_generic_browser.models import Group, GroupHit, Post
from aptbot.scrapers.facebook_generic_browser.tabs import TabPool

_rng = random.SystemRandom()
FB = "https://www.facebook.com"
FEED_QUERIES = ("GroupsCometFeed", "CometNewsFeed", "GroupsCometCrossGroupFeed")
SEARCH_QUERIES = ("SearchCometResults",)


@dataclass
class Env:
    pool: TabPool
    hub: CaptureHub
    request_wait: tuple[float, float] = (0.35, 0.7)   # after a burst: did the feed send a pagination request?
    response_timeout: float = 8.0                     # if it did: wait for that response (no fixed sleep)
    stall_timeout: tuple[float, float] = (1.2, 2.8)   # search pages (no request tracking)
    max_stalls: int = 6                               # consecutive bursts with no new posts (~12k px) → end of feed
    health_every: int = 6                             # health.check every N bursts


Sink = Callable[[list[Post]], Awaitable[None]]


# ---------------------------------------------------------------- helpers

def _drain(q: asyncio.Queue) -> list[Event]:
    out = []
    while not q.empty():
        out.append(q.get_nowait())
    return out


async def _wait_event(q: asyncio.Queue, pred, timeout: float) -> list[Event]:
    """Collect events until one satisfies pred or timeout. Returns everything collected."""
    got: list[Event] = []
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while True:
        left = end - loop.time()
        if left <= 0:
            return got
        try:
            ev = await asyncio.wait_for(q.get(), left)
        except asyncio.TimeoutError:
            return got
        got.append(ev)
        if pred(ev):
            return got + _drain(q)


def _is_doc(ev: Event) -> bool:
    return ev.kind == "document"


async def navigate(env: Env, page, url: str, q: asyncio.Queue, *, doc_timeout: float = 20) -> list[Event]:
    """Typed-URL navigation. Returns the events seen up to (and including) the main document."""
    await env.pool.goto(page, url)
    events = _drain(q)
    if not any(_is_doc(e) for e in events):
        events += await _wait_event(q, _is_doc, doc_timeout)
    await health.check(page)
    return events


async def scroll_burst(env: Env, page) -> None:
    """1–3 quick wheel flicks under attention, then let go (the burst is what a person does before glancing)."""
    async with env.pool.foreground(page):
        for _ in range(_rng.choice((1, 2, 2, 3))):
            await human.wheel(page, _rng.uniform(700, 1400))
            await human.micro()


async def collect_scrolling(env: Env, page, q: asyncio.Queue, *, keep: Callable[[Post], bool], limit: int,
                            stop_when_older_than: float | None = None, sink: Sink | None = None,
                            initial: list[Post] = (), query_prefixes=FEED_QUERIES) -> list[Post]:
    """Scroll a feed, parsing every Story that arrives. Stops at `limit`, at a fully-old batch, or after
    `max_stalls` bursts with no new posts. Posts are passed to `sink` as soon as they arrive."""
    seen: dict[str, Post] = {}

    async def take(posts: list[Post]) -> bool:
        fresh = [p for p in posts if keep(p) and p.post_id not in seen]
        for p in fresh:
            seen[p.post_id] = p
        if sink and fresh:
            await sink(fresh)
        return bool(fresh)

    await take(list(initial))
    stalls = bursts = 0
    old_batches = 0
    def is_req(e: Event) -> bool:
        return e.kind == "request" and e.name.startswith(query_prefixes)

    def is_resp(e: Event) -> bool:
        return e.kind == "graphql" and e.name.startswith(query_prefixes)

    while len(seen) < limit and stalls < env.max_stalls:
        await scroll_burst(env, page)
        bursts += 1
        # Event-driven: a short look for an outgoing pagination request; only if one went out, wait for
        # its response. No request → the sentinel isn't in view yet → scroll on immediately.
        events = await _wait_event(q, lambda e: is_req(e) or is_resp(e), _rng.uniform(*env.request_wait))
        if any(is_req(e) for e in events) and not any(is_resp(e) for e in events):
            events += await _wait_event(q, is_resp, env.response_timeout)
        batch = [p for e in events if e.kind == "graphql" for p in parse.posts(e.chunks)]
        got = await take(batch)
        stalls = 0 if got else stalls + 1
        if stop_when_older_than is not None and batch:
            newest = max((p.created_at.timestamp() for p in batch if p.created_at), default=0)
            old_batches = old_batches + 1 if newest and newest <= stop_when_older_than else 0
            if old_batches >= 2:   # one slack batch for posts approved late by admins
                break
        if bursts % env.health_every == 0:
            await health.check(page)
        if bursts >= 8 and not seen and await page.locator('[role="feed"] > div[aria-labelledby]').count():
            raise SchemaDrift("feed shows posts but no Story could be parsed from Facebook's responses")
    await health.check(page)
    return sorted(seen.values(), key=lambda p: p.created_at.timestamp() if p.created_at else 0, reverse=True)[:limit]


def _blobs(events: list[Event]) -> list:
    return [e.chunks for e in events if e.kind == "document" and e.chunks]


# ---------------------------------------------------------------- jobs

async def list_groups(env: Env, page) -> list[Group]:
    """/groups/joins/ (1 navigation): every joined group + pending requests, with last_post_time for the
    first 10 member groups (the embedded left rail)."""
    async with env.hub.subscribe(page) as q:
        events = await navigate(env, page, f"{FB}/groups/joins/", q)
        await human.think()
    blobs = _blobs(events)
    rail, _ = parse.left_rail_groups(blobs)
    by_id = {g.id: g for g in rail}
    for gid, state in parse.join_states(blobs).items():
        meta = parse.group_meta(blobs, gid)
        g = by_id.get(gid) or Group(id=gid, name="", url=f"{FB}/groups/{gid}/")
        g.join_state = state
        if meta:
            g.name = g.name or meta.name
            g.url = meta.url or g.url
            g.privacy = g.privacy or meta.privacy
        by_id[gid] = g
    if not by_id:
        raise SchemaDrift("/groups/joins/ returned no groups")
    return list(by_id.values())


async def open_group(env: Env, page, group_id: str, q: asyncio.Queue) -> tuple[Group, list[Post], list[Event]]:
    """Group page sorted newest-first. Returns (meta, posts embedded in the page, events)."""
    events = await navigate(env, page, f"{FB}/groups/{group_id}/?sorting_setting=CHRONOLOGICAL", q)
    blobs = _blobs(events)
    meta = parse.group_meta(blobs, group_id) or Group(id=group_id, name="", url=f"{FB}/groups/{group_id}/")
    posts = parse.posts(blobs) + [p for e in events if e.kind == "graphql" for p in parse.posts(e.chunks)]
    return meta, posts, events


async def get_group(env: Env, page, group_id: str) -> Group:
    async with env.hub.subscribe(page) as q:
        meta, _, _ = await open_group(env, page, group_id, q)
        await human.think()
    return meta


async def group_feed(env: Env, page, group_id: str, *, limit: int, since: float | None,
                     sink: Sink | None = None) -> tuple[Group, list[Post]]:
    """Newest-first posts of one group, down to `since` (unix) or `limit`."""
    async with env.hub.subscribe(page) as q:
        meta, first, _ = await open_group(env, page, group_id, q)
        has_feed = await page.locator('[role="feed"]').count() > 0
        if not has_feed and meta.join_state != "MEMBER":
            raise NotMember(group_id, meta.join_state)
        num_id = meta.id

        def keep(p: Post) -> bool:
            in_group = p.group_id in (None, num_id, group_id)
            return in_group and (since is None or (p.created_at is not None and p.created_at.timestamp() > since))

        posts = await collect_scrolling(env, page, q, keep=keep, limit=limit, stop_when_older_than=since,
                                        sink=sink, initial=first)
    return meta, posts


async def cross_group_feed(env: Env, page, *, limit: int) -> list[Post]:
    """/groups/feed/ ("Your feed" across all joined groups; algorithmic order)."""
    async with env.hub.subscribe(page) as q:
        events = await navigate(env, page, f"{FB}/groups/feed/", q)
        first = parse.posts(_blobs(events)) + [p for e in events if e.kind == "graphql" for p in parse.posts(e.chunks)]
        return await collect_scrolling(env, page, q, keep=lambda p: True, limit=limit, initial=first)


async def search_groups(env: Env, page, query: str, *, limit: int) -> list[GroupHit]:
    """Type the query into the top search box, Enter, then the Groups filter; scroll for more results."""
    async with env.hub.subscribe(page) as q:
        if not page.url.startswith(FB):
            await navigate(env, page, f"{FB}/", q)
        box = page.locator('[role="banner"] input[type="search"], input[type="search"]').first
        await env.pool.limiter.take()
        async with env.pool.foreground(page):
            await human.click(page, box)
            await human.type_text(page, query)
            await human.micro()
            await page.keyboard.press("Enter")
        # /search/top is an in-app navigation (no document); results arrive as SearchCometResults* graphql
        await _wait_event(q, lambda e: _is_doc(e) or (e.kind == "graphql" and e.name.startswith(SEARCH_QUERIES)), 15)
        await health.check(page)
        link = page.locator('a[href*="/search/groups"]').first
        hits: dict[str, GroupHit] = {}
        if await link.count():
            async with env.pool.foreground(page):
                await human.click(page, link)
        else:  # fallback: the Groups vertical by URL
            await navigate(env, page, f"{FB}/search/groups/?q={quote(query)}", q)
        events = await _wait_event(q, lambda e: e.kind == "graphql" and e.name.startswith(SEARCH_QUERIES), 12)
        for e in events:
            for h in parse.search_group_hits(e.chunks or []):
                hits.setdefault(h.id, h)
        stalls = 0
        while len(hits) < limit and stalls < 3:
            await scroll_burst(env, page)
            evs = await _wait_event(q, lambda e: e.kind == "graphql" and e.name.startswith(SEARCH_QUERIES),
                                    _rng.uniform(*env.stall_timeout))
            before = len(hits)
            for e in evs:
                for h in parse.search_group_hits(e.chunks or []):
                    hits.setdefault(h.id, h)
            stalls = 0 if len(hits) > before else stalls + 1
        await health.check(page)
    return list(hits.values())[:limit]


async def search_in_group(env: Env, page, group_id: str, query: str, *, limit: int) -> list[Post]:
    """The group's own search page (verified 2026-11-24): first results are embedded in the page HTML, more
    arrive as SearchCometResultsPaginatedResultsQuery while scrolling. Results are relevance-ordered."""
    async with env.hub.subscribe(page) as q:
        events = await navigate(env, page, f"{FB}/groups/{group_id}/search/?q={quote(query)}", q)
        first = parse.posts(_blobs(events)) + [p for e in events if e.kind == "graphql" for p in parse.posts(e.chunks)]
        # Search results may carry the group under a vanity slug or omit it, so keep every Story they deliver.
        return await collect_scrolling(env, page, q, keep=lambda p: True, limit=limit, initial=first,
                                       query_prefixes=("Search", "Groups"))


def _target_story(sources: list, post_id: str) -> dict | None:
    """The richest Story node for post_id across payloads (the permalink HTML embeds it whole)."""
    best = None
    for s in sources:
        for d in parse.walk(s):
            if d.get("__typename") == "Story" and str(d.get("post_id")) == post_id:
                if best is None or len(str(d)) > len(str(best)):
                    best = d
    return best


async def get_post(env: Env, page, group_id: str, post_id: str, *, with_comments: bool) -> Post:
    """One post from its permalink page. Verified 2026-11-24: the post is embedded whole in the page HTML
    (no extra request). Comments: the ones embedded with the post, plus, only if the post has more than
    that, a few scroll bursts that stop as soon as no new comments arrive. Never scrolls into the
    unrelated feed below the post. (Comment pagination query names are still Unverified.)"""
    async with env.hub.subscribe(page) as q:
        events = await navigate(env, page, f"{FB}/groups/{group_id}/posts/{post_id}/", q)
        sources = _blobs(events) + [e.chunks for e in events if e.kind == "graphql"]
        story = _target_story(sources, post_id)
        if story is None:
            raise SchemaDrift(f"permalink {group_id}/{post_id} delivered no matching Story")
        post = parse.story_to_post(story)
        comments = {c.id: c for c in parse.comments(story)}
        if with_comments and (post.comment_count or 0) > len(comments):
            for _ in range(3):
                await scroll_burst(env, page)
                evs = await _wait_event(q, lambda e: e.kind == "graphql" and "Comment" in e.name, 2.0)
                new = {c.id: c for e in evs if e.kind == "graphql" and "Comment" in e.name
                       for c in parse.comments(e.chunks)}
                if not set(new) - set(comments):
                    break
                comments.update(new)
                if len(comments) >= (post.comment_count or 0):
                    break
        await health.check(page)
    post.comments = sorted(comments.values(), key=lambda c: c.created_at.timestamp() if c.created_at else 0) \
        if with_comments else post.comments
    return post
