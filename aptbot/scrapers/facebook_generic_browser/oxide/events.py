"""Network-tap records (`Browser.take_responses()`) → the `capture.Event`s the parsers and jobs already use."""
from __future__ import annotations

import json
from urllib.parse import parse_qsl, urlparse

from aptbot.scrapers.facebook_generic_browser.capture import Event, html_json_blobs, split_chunks

# What the worker asks the engine to record. Graphql (XHR POSTs) and documents under /groups/. Nothing else is
# copied out of the engine, which keeps the tap's memory small.
TAP_PATTERNS = ["facebook.com/api/graphql/", "www.facebook.com/groups/", "www.facebook.com/permalink", "www.facebook.com/media/set"]
FEED_QUERIES = ("GroupsCometFeed", "CometNewsFeed", "GroupsCometCrossGroupFeed")


def _is_fb(url: str) -> bool:
    return urlparse(url).netloc.endswith("facebook.com")


def to_events(records: list[dict]) -> list[Event]:
    """Graphql POST responses → Event("graphql", friendly_name, variables, chunks); top-level documents →
    Event("document", url, html, chunks=JSON islands). Anything else is dropped."""
    out: list[Event] = []
    for r in records:
        url = r.get("url") or ""
        if not _is_fb(url):
            continue
        status = int(r.get("status") or 0)
        if r.get("kind") == "fetch" and "/api/graphql" in url and r.get("method") == "POST":
            form = dict(parse_qsl(r.get("request_body") or "", keep_blank_values=True))
            try:
                variables = json.loads(form.get("variables") or "null")
            except json.JSONDecodeError:
                variables = None
            out.append(Event("graphql", form.get("fb_api_req_friendly_name", "unknown"), variables,
                             split_chunks(r.get("body") or ""), status=status))
        elif r.get("kind") == "document":
            html = (r.get("body") or "") if status < 300 else ""
            out.append(Event("document", url, html=html, chunks=html_json_blobs(html), status=status))
    return out
