"""Command line for every FacebookAPI function (docs/usage-guide.md).

    # from the repo root (zsh/bash):
    fbgb() { ~/.fbgb/venv/bin/python -m aptbot.scrapers.facebook_generic_browser "$@"; }
    fbgb login [--stdin]                # by hand; or --stdin: email+password from stdin/prompt, typed for you
    fbgb status | resume | validate
    fbgb groups                         # list_groups
    fbgb group <group_id>               # get_group
    fbgb posts <group_id> [--limit N] [--since ISO] [--refresh]
    fbgb stream <group_id> [--limit N]  # prints posts as they arrive
    fbgb new [<group_id> ...]           # new_posts (incremental polling)
    fbgb search-groups "<query>" [--limit N]
    fbgb search <group_id> "<query>"    # EXPERIMENTAL
    fbgb post <group_id> <post_id> [--no-comments]   # EXPERIMENTAL
    fbgb feed [--limit N]               # cross_group_feed
    fbgb filter [--group ID ...] [--include W ...] [--exclude W ...] [--min-price N] [--max-price N]
               [--min-rooms R] [--max-rooms R] [--photos | --no-photos] [--since ISO] [--limit N]
Add --json for one JSON object per line. Add --tabs N to change parallelism.
All times are UTC (Israel = UTC+2 in winter, UTC+3 in summer). --since takes ISO-8601; without an offset it's UTC.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import datetime, timezone

from aptbot.scrapers.facebook_generic_browser.api import FacebookAPI
from aptbot.scrapers.facebook_generic_browser.errors import FBGBError


def _since(s: str | None):
    """ISO-8601; a value without an offset is taken as UTC (like every time this library returns)."""
    if not s:
        return None
    dt = datetime.fromisoformat(s)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _show(obj, as_json: bool) -> None:
    items = obj if isinstance(obj, list) else [obj]
    for it in items:
        d = it.to_dict() if hasattr(it, "to_dict") else it
        if as_json:
            print(json.dumps(d, ensure_ascii=False))
            continue
        if "post_id" in d:
            t = (d.get("created_at") or "")[:16].replace("T", " ") + " UTC"
            hints = d.get("hints") or {}
            extra = " ".join(x for x in [f"₪{hints['prices'][0]:,}" if hints.get("prices") else "",
                                         f"{hints['rooms']}r" if hints.get("rooms") else "",
                                         f"📷{d['photo_count']}" if d.get("photo_count") else "",
                                         f"💬{d['comment_count']}" if d.get("comment_count") else ""] if x)
            print(f"{t}  {d.get('group_id')}/{d['post_id']}  {extra}\n    {d['text'][:160]!r}\n    {d.get('url')}")
            for c in d.get("comments") or []:
                print(f"      💬 {(c.get('created_at') or '')[:16].replace('T', ' ')} UTC {c['text'][:100]!r}")
        elif "join_state" in d or "last_post_time" in d:
            lp = (d.get("last_post_time") or "")[:16].replace("T", " ")
            print(f"{d['id']:>18}  {d.get('join_state') or '':9} {d.get('privacy') or '':7} last post {(lp + ' UTC') if lp else '?':20}  {d['name']}")
        elif "summary" in d:
            print(f"{d['id']:>18}  {d.get('summary') or '':45}  {d['name']}")
        else:
            print(json.dumps(d, ensure_ascii=False, indent=1))


async def _run(a) -> int:
    if a.cmd == "login":
        from aptbot.scrapers.facebook_generic_browser.explore import open_profile
        return await open_profile.main(stdin=a.stdin)
    if a.cmd == "validate":
        from aptbot.scrapers.facebook_generic_browser.explore import validate
        await validate.main(["patchright"])
        return 0
    async with FacebookAPI(max_tabs=a.tabs, idle_close_after=None, browser=a.browser) as fb:
        c = a.cmd
        if c == "status":
            out = await fb.status()
        elif c == "resume":
            await fb.resume()
            out = await fb.status()
        elif c == "groups":
            out = await fb.list_groups(max_age=0 if a.refresh else 600)
        elif c == "group":
            out = await fb.get_group(a.group_id, max_age=0 if a.refresh else 3600)
        elif c == "posts":
            out = await fb.latest_posts(a.group_id, limit=a.limit, since=_since(a.since), max_age=0 if a.refresh else 60)
        elif c == "stream":
            async for p in fb.stream_posts(a.group_id, limit=a.limit, since=_since(a.since)):
                _show(p, a.json)
            return 0
        elif c == "new":
            out = await fb.new_posts(a.group_ids or None)
        elif c == "search-groups":
            out = await fb.search_groups(a.query, limit=a.limit)
        elif c == "search":
            out = await fb.search_group(a.group_id, a.query, limit=a.limit)
        elif c == "post":
            out = await fb.get_post(a.group_id, a.post_id, with_comments=not a.no_comments)
        elif c == "feed":
            out = await fb.cross_group_feed(limit=a.limit)
        elif c == "filter":
            out = await fb.filter_posts(group_ids=a.group or None, include=a.include, exclude=a.exclude,
                                        since=_since(a.since), has_photos=a.photos, min_price=a.min_price,
                                        max_price=a.max_price, min_rooms=a.min_rooms, max_rooms=a.max_rooms,
                                        limit=a.limit)
        else:
            raise SystemExit(f"unknown command {c}")
        _show(out, a.json)
    return 0


def parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="facebook_generic_browser", description=__doc__.split("\n\n")[0])
    ap.add_argument("--json", action="store_true", help="one JSON object per line")
    ap.add_argument("--tabs", type=int, default=3, help="parallel tabs (default 3; use 2 on a low-CPU host)")
    ap.add_argument("--browser", choices=("chrome", "oxide"), default=None,
                    help="browser backend (default $FBGB_BROWSER or chrome)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("login", help="log the profile in (by hand, or --stdin: email+password typed for you)")
    s.add_argument("--stdin", action="store_true", help="read email and password (2 lines) from stdin, or prompt")
    for name in ("validate", "status", "resume"):
        sub.add_parser(name)
    s = sub.add_parser("groups"); s.add_argument("--refresh", action="store_true")
    s = sub.add_parser("group"); s.add_argument("group_id"); s.add_argument("--refresh", action="store_true")
    for name in ("posts", "stream"):
        s = sub.add_parser(name); s.add_argument("group_id"); s.add_argument("--limit", type=int, default=20)
        s.add_argument("--since"); s.add_argument("--refresh", action="store_true")
    s = sub.add_parser("new"); s.add_argument("group_ids", nargs="*")
    s = sub.add_parser("search-groups"); s.add_argument("query"); s.add_argument("--limit", type=int, default=20)
    s = sub.add_parser("search"); s.add_argument("group_id"); s.add_argument("query")
    s.add_argument("--limit", type=int, default=30)
    s = sub.add_parser("post"); s.add_argument("group_id"); s.add_argument("post_id")
    s.add_argument("--no-comments", action="store_true")
    s = sub.add_parser("feed"); s.add_argument("--limit", type=int, default=30)
    s = sub.add_parser("filter")
    s.add_argument("--group", action="append"); s.add_argument("--include", action="append")
    s.add_argument("--exclude", action="append"); s.add_argument("--since")
    s.add_argument("--min-price", type=int); s.add_argument("--max-price", type=int)
    s.add_argument("--min-rooms", type=float); s.add_argument("--max-rooms", type=float)
    s.add_argument("--photos", dest="photos", action="store_true", default=None)
    s.add_argument("--no-photos", dest="photos", action="store_false")
    s.add_argument("--limit", type=int, default=50)
    return ap


def main() -> None:
    a = parser().parse_args()
    try:
        sys.exit(asyncio.run(_run(a)))
    except FBGBError as e:
        print(f"error: {type(e).__name__}: {e}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
