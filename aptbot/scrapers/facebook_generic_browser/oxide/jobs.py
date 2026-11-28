"""Oxide jobs: the same operations and stop rules as ..jobs (Chrome), synchronous, on one Driver.

Data comes only from Facebook's own responses (tap → Events → parse.py). The DOM is used only to find things to
click. Every navigation is followed by a health check (Driver.goto). Feed depth is capped: in oxide every post
scrolled past costs ~2.8 MB (kb/caveats.md#feed-depth).
"""
from __future__ import annotations

import random
from typing import Callable

from aptbot.scrapers.facebook_generic_browser import parse
from aptbot.scrapers.facebook_generic_browser.capture import Event
from aptbot.scrapers.facebook_generic_browser.errors import NotMember, SchemaDrift
from aptbot.scrapers.facebook_generic_browser.jobs import _target_story
from aptbot.scrapers.facebook_generic_browser.models import Group, Photo, Post
from aptbot.scrapers.facebook_generic_browser.oxide.driver import FB, Driver, think
from aptbot.scrapers.facebook_generic_browser.oxide.events import FEED_QUERIES

_rng = random.SystemRandom()
MAX_STALLS = 8          # bursts with no new posts → end (responses can land one burst late: nothing is lost)
HEALTH_EVERY = 6
DEPTH_CAP = 40          # posts loaded in one document before we stop scrolling (memory)


def _blobs(events: list[Event]) -> list:
    return [e.chunks for e in events if e.kind == "document" and e.chunks]


def _graphql_posts(events: list[Event], prefixes=FEED_QUERIES) -> list[Post]:
    return [p for e in events if e.kind == "graphql" and e.name.startswith(prefixes) for p in parse.posts(e.chunks)]


def _newest_first(posts) -> list[Post]:
    return sorted(posts, key=lambda p: p.created_at.timestamp() if p.created_at else 0, reverse=True)


def collect_scrolling(d: Driver, *, keep: Callable[[Post], bool], limit: int, stop_when_older_than: float | None,
                      initial: list[Post], depth_cap: int = DEPTH_CAP) -> list[Post]:
    seen: dict[str, Post] = {}
    loaded: set[str] = set()

    def take(posts: list[Post]) -> bool:
        loaded.update(p.post_id for p in posts)
        fresh = [p for p in posts if keep(p) and p.post_id not in seen]
        for p in fresh:
            seen[p.post_id] = p
        return bool(fresh)

    take(list(initial))
    stalls = bursts = old_batches = 0
    while len(seen) < limit and stalls < MAX_STALLS and len(loaded) < depth_cap:
        d.scroll_burst()
        bursts += 1
        batch = _graphql_posts(d.pump(_rng.uniform(700, 1500)))
        stalls = 0 if take(batch) else stalls + 1
        if stop_when_older_than is not None and batch:
            newest = max((p.created_at.timestamp() for p in batch if p.created_at), default=0)
            old_batches = old_batches + 1 if newest and newest <= stop_when_older_than else 0
            if old_batches >= 2:            # one slack batch for posts approved late by admins
                break
        if bursts % HEALTH_EVERY == 0:
            d.check_health()
        if bursts >= 8 and not loaded and d.count('[role="feed"] [aria-label^="Comment on "]'):
            raise SchemaDrift("feed shows posts but no Story could be parsed from Facebook's responses")
    d.check_health()
    return _newest_first(seen.values())[:limit]


def groups_from_blobs(blobs: list) -> list[Group]:
    """/groups/joins/ islands → groups (same merge as ..jobs.list_groups)."""
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
    return list(by_id.values())


# ---------------------------------------------------------------- jobs (names = worker ops)

def list_groups(d: Driver) -> list[Group]:
    events = d.goto(f"{FB}/groups/joins/")
    think()
    groups = groups_from_blobs(_blobs(events))
    if not groups:
        raise SchemaDrift("/groups/joins/ returned no groups")
    return groups


def open_group(d: Driver, group_id: str) -> tuple[Group, list[Post], list[Event]]:
    events = d.goto(f"{FB}/groups/{group_id}/?sorting_setting=CHRONOLOGICAL")
    blobs = _blobs(events)
    meta = parse.group_meta(blobs, group_id) or Group(id=group_id, name="", url=f"{FB}/groups/{group_id}/")
    posts = parse.posts(blobs) + _graphql_posts(events, prefixes=("",))
    return meta, posts, events


def get_group(d: Driver, group_id: str) -> Group:
    meta, _, _ = open_group(d, group_id)
    think()
    return meta


ALBUM_FILL_MAX = 6      # albums completed per group_feed call (one page load each)


def album_photos(d: Driver, post_id: str, want: int) -> list[Photo]:
    """All photos of a post's album: /media/set/?set=pcb.<post_id> embeds them in its document
    (`data.album.media.edges[].node`, Photo with `image`). The feed and the permalink deliver only the first ~5
    (api_docs/posts/media.md). Pages further with a few scroll bursts for big albums."""
    found: dict[str, Photo] = {}

    def take(events: list[Event]) -> None:
        for e in events:
            for o in parse.walk(e.chunks):
                n = o.get("node") if isinstance(o.get("node"), dict) else None
                if not n or n.get("__typename") != "Photo" or n.get("is_playable"):
                    continue
                img = n.get("viewer_image") or n.get("image") or {}
                if n.get("id") and img.get("uri") and n["id"] not in found:
                    found[n["id"]] = Photo(id=n["id"], url=img.get("uri"), width=img.get("width"),
                                           height=img.get("height"), page_url=f"{FB}/photo/?fbid={n['id']}")

    take(d.goto(f"{FB}/media/set/?set=pcb.{post_id}&type=1"))
    for _ in range(4):
        if len(found) >= want:
            break
        d.scroll_burst()
        before = len(found)
        take(d.pump(_rng.uniform(1500, 2500)))
        if len(found) == before:
            break
    return list(found.values())


def fill_albums(d: Driver, posts: list[Post], *, max_albums: int = ALBUM_FILL_MAX) -> int:
    """Complete albums the feed cut short, newest posts first. Keeps the feed's (full-size) photos and appends the
    rest in album order. Returns how many posts were completed."""
    done = 0
    for p in posts:
        if done >= max_albums:
            break
        if (p.photo_count or 0) <= len(p.photos):
            continue
        think()
        album = album_photos(d, p.post_id, p.photo_count)
        have = {x.id for x in p.photos}
        extra = [x for x in album if x.id not in have]
        if extra:
            p.photos = p.photos + extra
            done += 1
    if done:
        d.check_health()
    return done


def group_feed(d: Driver, group_id: str, *, limit: int, since: float | None,
               depth_cap: int = DEPTH_CAP, albums: bool = False) -> tuple[Group, list[Post]]:
    meta, first, _ = open_group(d, group_id)
    if not d.count('[role="feed"]') and meta.join_state != "MEMBER":
        raise NotMember(group_id, meta.join_state)
    num_id = meta.id

    def keep(p: Post) -> bool:
        in_group = p.group_id in (None, num_id, group_id)
        return in_group and (since is None or (p.created_at is not None and p.created_at.timestamp() > since))

    posts = collect_scrolling(d, keep=keep, limit=limit, stop_when_older_than=since, initial=first,
                              depth_cap=depth_cap)
    if albums:
        fill_albums(d, posts)
    return meta, posts


def get_post(d: Driver, group_id: str, post_id: str, *, with_comments: bool) -> Post:
    """Permalink page: the post is embedded whole; comments beyond the embedded ones via a few bursts."""
    events = d.goto(f"{FB}/groups/{group_id}/posts/{post_id}/")
    sources = _blobs(events) + [e.chunks for e in events if e.kind == "graphql"]
    story = _target_story(sources, post_id)
    if story is None:
        raise SchemaDrift(f"permalink {group_id}/{post_id} delivered no matching Story")
    post = parse.story_to_post(story)
    comments = {c.id: c for c in parse.comments(story)}
    if with_comments and (post.comment_count or 0) > len(comments):
        for _ in range(3):
            d.scroll_burst()
            evs = d.pump(_rng.uniform(1500, 2500))
            new = {c.id: c for e in evs if e.kind == "graphql" and "Comment" in e.name for c in parse.comments(e.chunks)}
            if not set(new) - set(comments):
                break
            comments.update(new)
            if len(comments) >= (post.comment_count or 0):
                break
    d.check_health()
    if with_comments:
        post.comments = sorted(comments.values(), key=lambda c: c.created_at.timestamp() if c.created_at else 0)
    return post
