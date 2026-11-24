"""Passive capture of what Facebook's own frontend fetches. Adds zero requests.

Writes to ~/.fbgb/captures/<session>/ (outside the repo: these files contain real people's posts):
  index.jsonl                       one line per captured response
  NNNN_<friendly_name>.json         graphql: redacted request params + every JSON chunk of the response
  NNNN_document_<slug>.html         main-frame HTML (first feed page and post data are embedded in it)

Session tokens (fb_dtsg, lsd, jazoest, __user, ...) are never written; only their key names are.
"""
from __future__ import annotations

import json
import re
import time
from pathlib import Path
from urllib.parse import parse_qsl, urlparse

from aptbot.scrapers.facebook_generic_browser.browser import CAPTURE_DIR

KEEP_PARAMS = {"fb_api_caller_class", "fb_api_req_friendly_name", "doc_id", "variables", "server_timestamps"}
_SJS = re.compile(r'<script type="application/json"[^>]*data-sjs[^>]*>(.*?)</script>', re.S)


def split_chunks(text: str) -> list:
    """FB graphql bodies are one or more JSON objects separated by newlines (deferred/streamed
    @defer chunks), sometimes prefixed with the anti-JSON-hijacking guard `for (;;);`."""
    text = text.removeprefix("for (;;);")
    out = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            out.append({"__unparsed__": line[:500]})
    return out


def html_json_blobs(html: str) -> list:
    """The JSON islands (`<script type="application/json" data-sjs>`) the Comet app boots from."""
    blobs = []
    for m in _SJS.finditer(html):
        try:
            blobs.append(json.loads(m.group(1)))
        except json.JSONDecodeError:
            pass
    return blobs


def _slug(url: str) -> str:
    p = urlparse(url).path.strip("/") or "root"
    return re.sub(r"[^A-Za-z0-9]+", "-", p)[:60]


class Capture:
    def __init__(self, ctx, session: str | None = None, *, save_html: bool = True):
        self.dir = CAPTURE_DIR / (session or time.strftime("%Y%m%d-%H%M%S"))
        self.dir.mkdir(parents=True, exist_ok=True)
        self.seq = 0
        self.save_html = save_html
        self.ctx = ctx
        self.friendly_names: dict[str, int] = {}
        self.listeners: list = []  # callables(name, variables, chunks, page) for live parsing
        if ctx is not None:
            self.attach(ctx)

    def attach(self, ctx) -> None:
        self.ctx = ctx
        ctx.on("response", self._on_response)

    async def _on_response(self, resp) -> None:
        try:
            req = resp.request
            url = resp.url
            host = urlparse(url).netloc
            if not host.endswith("facebook.com"):
                return
            if "/api/graphql" in url and req.method == "POST":
                await self._graphql(resp, req)
            elif self.save_html and req.resource_type == "document" and req.frame.parent_frame is None:
                await self._document(resp)
        except Exception as e:  # never let capture break browsing
            self._index({"error": repr(e), "url": resp.url[:200]})

    async def _graphql(self, resp, req) -> None:
        form = dict(parse_qsl(req.post_data or "", keep_blank_values=True))
        name = form.get("fb_api_req_friendly_name", "unknown")
        params = {k: v for k, v in form.items() if k in KEEP_PARAMS}
        if "variables" in params:
            try:
                params["variables"] = json.loads(params["variables"])
            except json.JSONDecodeError:
                pass
        chunks = split_chunks(await resp.text())
        self.seq += 1
        self.friendly_names[name] = self.friendly_names.get(name, 0) + 1
        path = self.dir / f"{self.seq:04d}_{name}.json"
        path.write_text(json.dumps({"status": resp.status, "params": params,
                                    "param_keys": sorted(form), "chunks": chunks},
                                   ensure_ascii=False, indent=1))
        self._index({"kind": "graphql", "name": name, "doc_id": form.get("doc_id"),
                     "chunks": len(chunks), "bytes": path.stat().st_size, "file": path.name,
                     "page": req.frame.page.url[:200] if req.frame else None})
        for fn in self.listeners:
            fn(name, params.get("variables"), chunks, req.frame.page if req.frame else None)

    async def _document(self, resp) -> None:
        if resp.status >= 300:
            self._index({"kind": "document", "status": resp.status, "url": resp.url[:200]})
            return
        html = await resp.text()
        self.seq += 1
        path = self.dir / f"{self.seq:04d}_document_{_slug(resp.url)}.html"
        path.write_text(html)
        self._index({"kind": "document", "url": resp.url[:200], "bytes": len(html), "file": path.name,
                     "sjs_blobs": len(html_json_blobs(html))})

    def _index(self, rec: dict) -> None:
        rec["t"] = round(time.time(), 3)
        with (self.dir / "index.jsonl").open("a") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")


# ---------------------------------------------------------------------------------------------
# Production capture: route every Facebook response to the job running in that response's tab.
# ---------------------------------------------------------------------------------------------

class Event:
    """One captured response. kind = "graphql" (name = fb_api_req_friendly_name) or "document" (name = url)."""
    __slots__ = ("kind", "name", "variables", "chunks", "html", "status", "t")

    def __init__(self, kind, name, variables=None, chunks=None, html=None, status=200):
        self.kind, self.name, self.variables, self.chunks, self.html, self.status = kind, name, variables, chunks, html, status
        self.t = time.monotonic()


class CaptureHub:
    """Context-wide response listener that fans events out to per-page subscribers.

    Usage inside a job:  async with hub.subscribe(page) as events:  ev = await events.get()
    Nothing is written to disk unless `dump_dir` is set (debugging; files contain real posts).
    """

    def __init__(self, ctx, dump_dir: Path | None = None):
        import asyncio
        self._asyncio = asyncio
        self._subs: dict[int, list] = {}
        self.is_front = lambda page: None      # set by the owner (FacebookAPI) to report tab visibility
        self.stats = {"graphql": 0, "graphql_hidden": 0, "feed": 0, "feed_hidden": 0, "document": 0}
        self._dump = Capture(None, dump_dir.name) if dump_dir else None
        if self._dump:
            self._dump.dir = dump_dir
        ctx.on("response", self._on_response)
        ctx.on("request", self._on_request)

    def _on_request(self, req) -> None:
        """Emit kind="request" for outgoing graphql POSTs, so jobs know a response is on its way."""
        try:
            if "/api/graphql" not in req.url or req.method != "POST":
                return
            page = req.frame.page if req.frame else None
            if page is None or id(page) not in self._subs:
                return
            form = dict(parse_qsl(req.post_data or "", keep_blank_values=True))
            self._emit(page, Event("request", form.get("fb_api_req_friendly_name", "unknown")))
        except Exception:
            return

    def subscribe(self, page):
        hub = self

        class _Sub:
            async def __aenter__(self_inner):
                self_inner.q = hub._asyncio.Queue()
                hub._subs.setdefault(id(page), []).append(self_inner.q)
                return self_inner.q

            async def __aexit__(self_inner, *exc):
                lst = hub._subs.get(id(page), [])
                if self_inner.q in lst:
                    lst.remove(self_inner.q)
                if not lst:
                    hub._subs.pop(id(page), None)

        return _Sub()

    def _emit(self, page, ev: Event) -> None:
        for q in self._subs.get(id(page), []):
            q.put_nowait(ev)

    async def _on_response(self, resp) -> None:
        try:
            req = resp.request
            if not urlparse(resp.url).netloc.endswith("facebook.com"):
                return
            frame = req.frame
            page = frame.page if frame else None
            if page is None or id(page) not in self._subs:
                if self._dump:
                    await self._dump._on_response(resp)
                return
            if "/api/graphql" in resp.url and req.method == "POST":
                form = dict(parse_qsl(req.post_data or "", keep_blank_values=True))
                try:
                    variables = json.loads(form.get("variables") or "null")
                except json.JSONDecodeError:
                    variables = None
                ev = Event("graphql", form.get("fb_api_req_friendly_name", "unknown"), variables,
                           split_chunks(await resp.text()), status=resp.status)
            elif req.resource_type == "document" and frame.parent_frame is None:
                html = await resp.text() if resp.status < 300 else ""
                ev = Event("document", resp.url, html=html, chunks=html_json_blobs(html), status=resp.status)
            else:
                return
            if ev.kind == "graphql":
                hidden = self.is_front(page) is False
                feed = ev.name.startswith(("GroupsCometFeed", "CometNewsFeed", "GroupsCometCrossGroupFeed"))
                self.stats["graphql"] += 1
                self.stats["graphql_hidden"] += hidden
                self.stats["feed"] += feed
                self.stats["feed_hidden"] += feed and hidden
            else:
                self.stats["document"] += 1
            self._emit(page, ev)
            if self._dump:
                await self._dump._on_response(resp)
        except Exception:  # a closed page/target mid-read must never break browsing
            return
