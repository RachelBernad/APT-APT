"""CLI: python -m aptbot.listing_ai login [--browser] | status | try POSTS.json

- login: one-time ChatGPT login into the token store (AI_AUTH_PATH, default
  ~/.aptbot/chatgpt-auth.json, outside the repo). Device code by default (headless).
- status: plan + token expiry (never prints a token).
- try: analyze a JSON list of {text, posted_at?, group_name?, photo_count?} and print
  the facts plus the Telegram summary. Keep real posts outside the repo.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import datetime

from aptbot.listing_ai.llm import auth


def _try(path: str) -> int:
    from aptbot.bot.formatting import format_fb_summary
    from aptbot.listing_ai import ListingAnalyzer, PostInput

    raw = json.load(open(path, encoding="utf-8"))
    posts = [PostInput(key=str(i), text=p["text"], group_name=p.get("group_name"),
                       photo_count=int(p.get("photo_count") or 0),
                       posted_at=datetime.fromisoformat(p["posted_at"]) if p.get("posted_at") else None)
             for i, p in enumerate(raw)]
    analyzer = ListingAnalyzer.from_config()
    facts = asyncio.run(analyzer.analyze(posts))
    for p in posts:
        f = facts.get(p.key)
        print(f"===== {p.key}: {p.text[:70]!r}")
        print(format_fb_summary(f) if f else "(no facts)")
    st = analyzer.state
    print(f"\nanalyzed={st.analyzed} failed={st.failed} last_batch={st.last_batch_seconds} "
          f"paused_until={st.paused_until} last_error={st.last_error}")
    return 0 if not st.failed else 1


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="python -m aptbot.listing_ai")
    sub = ap.add_subparsers(dest="cmd", required=True)
    lg = sub.add_parser("login")
    lg.add_argument("--browser", action="store_true", help="loopback browser flow instead of device code")
    sub.add_parser("status")
    tr = sub.add_parser("try")
    tr.add_argument("posts")
    a = ap.parse_args(argv)
    if a.cmd == "login":
        (auth.browser_login if a.browser else auth.device_login)()
        print(auth.status())
    elif a.cmd == "status":
        print(auth.status())
    else:
        return _try(a.posts)
    return 0


if __name__ == "__main__":
    sys.exit(main())
