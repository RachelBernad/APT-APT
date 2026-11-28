"""Oxide worker process: `python -m aptbot.scrapers.facebook_generic_browser.oxide.worker`.

Protocol (one JSON object per line):
  request  {"id": 1, "op": "group_feed", "args": {...}}
  reply    {"id": 1, "ok": <result>}  |  {"id": 1, "error": "<code>", "message": "...", ...}
  ops      hello, list_groups, get_group, group_feed, get_post, stats, shutdown
  errors   account_at_risk, not_logged_in, not_member (+group_id, join_state), schema_drift, bad_request, engine

Replies go to a private dup of the original stdout. fd 1 itself is pointed at stderr, so nothing the engine
prints can corrupt the protocol. The parent drops stderr (it can hold URLs) except for crash lines.
"""
from __future__ import annotations

import json
import os
import sys
import traceback

from aptbot.scrapers.facebook_generic_browser.errors import NotMember, SchemaDrift
from aptbot.scrapers.facebook_generic_browser.health import AccountAtRisk

OPS = ("hello", "list_groups", "get_group", "group_feed", "get_post", "stats", "shutdown")


def _result(op: str, d, args: dict):
    from aptbot.scrapers.facebook_generic_browser.oxide import jobs
    from aptbot.scrapers.facebook_generic_browser.oxide.driver import rss_mb
    if op == "hello":
        names = d.cookie_names()
        return {"logged_in": {"c_user", "xs"} <= names, "rss_mb": rss_mb()}
    if op == "stats":
        return {"rss_mb": rss_mb(), "tap_dropped": d.dropped}
    if op == "list_groups":
        return [g.to_dict() for g in jobs.list_groups(d)]
    if op == "get_group":
        return jobs.get_group(d, args["group_id"]).to_dict()
    if op == "group_feed":
        meta, posts = jobs.group_feed(d, args["group_id"], limit=int(args.get("limit", 30)), since=args.get("since"),
                                     albums=bool(args.get("albums", False)))
        return {"group": meta.to_dict(), "posts": [p.to_dict() for p in posts]}
    if op == "get_post":
        post = jobs.get_post(d, args["group_id"], args["post_id"], with_comments=bool(args.get("with_comments", True)))
        jobs.fill_albums(d, [post], max_albums=1)
        return post.to_dict()
    raise ValueError(op)


def _error(e: BaseException) -> dict:
    if isinstance(e, AccountAtRisk):
        return {"error": "account_at_risk", "message": str(e)}
    if isinstance(e, NotMember):
        return {"error": "not_member", "message": str(e), "group_id": e.group_id, "join_state": e.join_state}
    if isinstance(e, SchemaDrift):
        return {"error": "schema_drift", "message": str(e)}
    if isinstance(e, (KeyError, ValueError, TypeError)):
        return {"error": "bad_request", "message": f"{type(e).__name__}: {e}"}
    return {"error": "engine", "message": f"{type(e).__name__}: {e}"[:500]}


def main() -> int:
    proto = os.fdopen(os.dup(1), "w", buffering=1)
    os.dup2(2, 1)                                   # stray prints → stderr
    sys.stdout = sys.stderr
    driver = None
    for line in sys.stdin:
        try:
            req = json.loads(line)
            rid, op, args = req.get("id"), req.get("op"), req.get("args") or {}
        except json.JSONDecodeError:
            continue
        if op == "shutdown":
            jar = os.environ.get("BROWSER_OXIDE_COOKIE_JAR")
            if driver is not None and jar:
                try:
                    driver.save_cookies(jar)
                except Exception:
                    pass
            proto.write(json.dumps({"id": rid, "ok": True}) + "\n")
            break
        try:
            if op not in OPS:
                raise ValueError(f"unknown op {op!r}")
            if driver is None:
                from aptbot.scrapers.facebook_generic_browser.oxide.driver import Driver
                driver = Driver()
            reply = {"id": rid, "ok": _result(op, driver, args)}
        except BaseException as e:                  # noqa: BLE001 - every failure becomes a reply
            if isinstance(e, KeyboardInterrupt):
                raise
            traceback.print_exc(file=sys.stderr)
            reply = {"id": rid, **_error(e)}
        proto.write(json.dumps(reply, ensure_ascii=False) + "\n")
    proto.flush()
    os._exit(0)                                     # skip V8 teardown: the process ends here anyway


if __name__ == "__main__":
    sys.exit(main())
