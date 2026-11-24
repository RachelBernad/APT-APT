"""Pure parsers: Facebook payloads → models. No I/O, no browser. Unit-tested on anonymized fixtures.

Inputs are "chunks": the list of JSON objects from one graphql response (capture.split_chunks), or the
list of JSON islands from a full page load (capture.html_json_blobs). Both are walked the same way.

Rule (api_docs/graphql/queries/GroupsCometFeedRegularStoriesPaginationQuery.md): find nodes by type and pick
fields by KEY, tolerating path changes between Facebook deploys. Never key on doc_id.
"""
from __future__ import annotations

import re
from typing import Any, Iterator

from aptbot.scrapers.facebook_generic_browser import extract
from aptbot.scrapers.facebook_generic_browser.models import Comment, Group, GroupHit, Photo, Post, ts

_TRACKING = re.compile(r"[?&](__cft__\[\d+\]|__tn__|comment_id|notif_id|ref)=[^&#]*")


def clean_url(url: str | None) -> str | None:
    if not url:
        return url
    u = _TRACKING.sub("", url)
    return u.replace("/?&", "/?").rstrip("?&")


# ---------- generic walking ----------

def walk(o: Any) -> Iterator[dict]:
    """Every dict in a JSON tree, depth-first."""
    stack = [o]
    while stack:
        x = stack.pop()
        if isinstance(x, dict):
            yield x
            stack.extend(reversed(list(x.values())))
        elif isinstance(x, list):
            stack.extend(reversed(x))


def first(o: Any, key: str, pred=lambda v: v is not None) -> Any:
    """First value of `key` anywhere under `o` that satisfies pred."""
    for d in walk(o):
        if key in d and pred(d[key]):
            return d[key]
    return None


def dig(o: Any, *path: str) -> Any:
    for k in path:
        if not isinstance(o, dict):
            return None
        o = o.get(k)
    return o


def page_info(chunks: Any) -> dict:
    """Merged {'end_cursor', 'has_next_page'} from any page_info in the payload."""
    out: dict = {}
    for d in walk(chunks):
        pi = d.get("page_info")
        if isinstance(pi, dict):
            out.update({k: v for k, v in pi.items() if k in ("end_cursor", "has_next_page") and v is not None})
    return out


# ---------- stories / posts ----------

def _is_story(d: dict) -> bool:
    return d.get("__typename") == "Story" and bool(d.get("post_id"))


def _text(story: dict) -> tuple[str, str | None]:
    content = dig(story, "comet_sections", "content", "story") or story
    msg = dig(content, "message", "text")
    tr = first(story, "translation", lambda v: isinstance(v, dict) and bool(dig(v, "message", "text")))
    translated = dig(tr, "message", "text") if tr else None
    if not msg:  # fall back to the longest message.text that isn't the translation
        cands = [dig(d, "message", "text") for d in walk(story) if isinstance(d.get("message"), dict)]
        cands = [c for c in cands if c and c != translated]
        msg = max(cands, key=len) if cands else ""
    return msg or "", translated if translated and translated != msg else None


def _group(story: dict) -> tuple[str | None, str | None]:
    title_to = dig(story, "comet_sections", "context_layout", "story", "comet_sections", "title", "story", "to")
    for to in [title_to] + [d.get("to") for d in walk(story) if isinstance(d.get("to"), dict)]:
        if isinstance(to, dict) and "Group" in (to.get("__typename"), to.get("__isActor"), to.get("__isEntity")):
            return to.get("id"), to.get("name")
    ag = first(story, "associated_group", lambda v: isinstance(v, dict) and v.get("id"))
    return (ag.get("id"), ag.get("name")) if ag else (None, None)


def _photos(story: dict) -> tuple[list[Photo], int]:
    photos: dict[str, Photo] = {}
    count = 0
    for d in walk(story):
        sub = d.get("all_subattachments")
        if isinstance(sub, dict):
            count = max(count, sub.get("count") or 0)
        media = d.get("media")
        if isinstance(media, dict) and (media.get("viewer_image") or media.get("photo_image") or media.get("image")):
            if media.get("__typename") not in (None, "Photo") or media.get("is_playable"):
                continue
            img = media.get("viewer_image") or media.get("photo_image") or media.get("image") or {}
            pid = media.get("id") or img.get("uri")
            if pid and pid not in photos:
                photos[pid] = Photo(id=media.get("id"), url=img.get("uri"), width=img.get("width"),
                                    height=img.get("height"), page_url=clean_url(d.get("url")))
    return list(photos.values()), max(count, len(photos))


def _count(story: dict) -> tuple[int | None, int | None]:
    comments = first(story, "comments", lambda v: isinstance(v, dict) and isinstance(v.get("total_count"), int))
    reaction = first(story, "reaction_count", lambda v: isinstance(v, dict) and isinstance(v.get("count"), int))
    return (comments or {}).get("total_count"), (reaction or {}).get("count")


def comments(o: Any) -> list[Comment]:
    """Comment nodes anywhere under `o` (inline feed previews, post dialog, permalink page)."""
    out: dict[str, Comment] = {}
    for d in walk(o):
        body = d.get("body")
        if isinstance(body, dict) and "text" in body and d.get("created_time") and isinstance(d.get("author"), dict):
            cid = d.get("legacy_fbid") or d.get("id")
            if not cid or cid in out:
                continue
            a = d["author"]
            parent = dig(d, "comment_parent", "id") or dig(d, "parent_comment", "id")
            out[cid] = Comment(id=cid, text=body.get("text") or "", created_at=ts(d.get("created_time")),
                               author_id=a.get("id"), author_name=a.get("name"), reply_to=parent)
    return list(out.values())


def story_to_post(story: dict) -> Post:
    content = dig(story, "comet_sections", "content", "story") or {}
    actor = (content.get("actors") or first(story, "actors", lambda v: isinstance(v, list) and v) or [{}])[0]
    text, translated = _text(story)
    gid, gname = _group(story)
    photos, n_photos = _photos(story)
    n_comments, n_reactions = _count(story)
    preview = comments(first(story, "interesting_top_level_comments") or [])
    return Post(
        group_id=gid, group_name=gname, post_id=str(story["post_id"]),
        url=clean_url(first(story, "permalink_url") or content.get("wwwURL")),
        created_at=ts(story.get("creation_time") or first(story, "creation_time")),
        text=text, text_translated=translated,
        author_id=actor.get("id"), author_name=actor.get("name"), author_url=clean_url(actor.get("url")),
        photos=photos, photo_count=n_photos, comment_count=n_comments, reaction_count=n_reactions,
        hints=extract.hints(text), comments=preview or None,
    )


def posts(chunks: Any) -> list[Post]:
    """All distinct posts (Story nodes with post_id) in a payload, in payload order."""
    out: dict[str, Post] = {}
    for d in walk(chunks):
        if _is_story(d) and str(d["post_id"]) not in out:
            out[str(d["post_id"])] = story_to_post(d)
    return list(out.values())


# ---------- groups ----------

def left_rail_groups(chunks: Any) -> tuple[list[Group], dict]:
    """GroupsCometLeftRailContainerQuery → member groups (+ page_info of the member list)."""
    groups: dict[str, Group] = {}
    pi: dict = {}
    for d in walk(chunks):
        lst = d.get("tab_groups_list")
        if not isinstance(lst, dict):
            continue
        for e in lst.get("edges") or []:
            n = e.get("node") or {}
            if not n.get("id"):
                continue
            g = groups.get(n["id"])
            if g is None:
                groups[n["id"]] = Group(id=n["id"], name=n.get("name") or "", url=clean_url(n.get("url")) or "",
                                        last_post_time=ts(n.get("last_post_time")), join_state="MEMBER")
            else:   # the same group appears in several lists (rail with times, full list without): merge
                g.last_post_time = g.last_post_time or ts(n.get("last_post_time"))
                g.name = g.name or n.get("name") or ""
                g.url = g.url or clean_url(n.get("url")) or ""
        if lst.get("edges"):
            pi = lst.get("page_info") or pi
    return list(groups.values()), pi


def _privacy_from_text(s: str | None) -> str | None:
    if not s:
        return None
    s = s.lower()
    if s.startswith(("public", "ציבורי")) or "public group" in s or "קבוצה ציבורית" in s:
        return "public"
    if s.startswith(("private", "פרטי")) or "private group" in s or "קבוצה פרטית" in s:
        return "private"
    return None


def group_meta(blobs: Any, group_id: str) -> Group | None:
    """Merge every object with id == group_id from a group page's JSON islands into one Group."""
    g: dict[str, Any] = {}
    for d in walk(blobs):
        if d.get("id") != group_id:
            continue
        if d.get("name") and "name" not in g:
            g["name"] = d["name"]
        if d.get("url") and "url" not in g:
            g["url"] = d["url"]
        if d.get("visibility"):
            g["privacy"] = {"OPEN": "public", "CLOSED": "private", "SECRET": "private"}.get(d["visibility"], g.get("privacy"))
        if d.get("viewer_join_state"):
            g["join_state"] = d["viewer_join_state"]
        if dig(d, "privacy_info", "title", "text"):
            g.setdefault("privacy", _privacy_from_text(dig(d, "privacy_info", "title", "text")))
        if dig(d, "group_member_profiles", "formatted_count_text"):
            g["member_count"] = dig(d, "group_member_profiles", "formatted_count_text")
        desc = dig(d, "description_with_entities", "text") or dig(d, "group_description", "text")
        if desc:
            g["description"] = desc
    if not g:
        return None
    return Group(id=group_id, name=g.get("name", ""), url=clean_url(g.get("url")) or
                 f"https://www.facebook.com/groups/{group_id}/", privacy=g.get("privacy"),
                 join_state=g.get("join_state"), member_count=g.get("member_count"), description=g.get("description"))


def join_states(blobs: Any) -> dict[str, str]:
    """{group_id: viewer_join_state} from any payload (e.g. /groups/joins/ islands)."""
    out: dict[str, str] = {}
    for d in walk(blobs):
        if d.get("viewer_join_state") and d.get("id"):
            out.setdefault(d["id"], d["viewer_join_state"])
    return out


def search_group_hits(chunks: Any) -> list[GroupHit]:
    """SearchCometResults* (Groups vertical) → GroupHit list."""
    out: dict[str, GroupHit] = {}
    for d in walk(chunks):
        edges = dig(d, "results", "edges")
        if not isinstance(edges, list):
            continue
        for e in edges:
            if dig(e, "node", "role") != "ENTITY_GROUPS":
                continue
            vm = dig(e, "rendering_strategy", "view_model") or {}
            prof = vm.get("profile") or {}
            if not prof.get("id") or prof["id"] in out:
                continue
            summary = dig(vm, "primary_snippet_text_with_entities", "text")
            out[prof["id"]] = GroupHit(id=prof["id"], name=prof.get("name") or "", url=clean_url(prof.get("url")) or "",
                                       summary=summary, privacy=_privacy_from_text((summary or "").split("·")[0].strip()))
    return list(out.values())
