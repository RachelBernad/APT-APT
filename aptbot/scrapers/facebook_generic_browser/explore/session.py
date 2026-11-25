"""Exploration sessions on the logged-in account. Slow, one tab unless stated, health-checked.

    ~/.fbgb/venv/bin/python -m aptbot.scrapers.facebook_generic_browser.explore.session joined
    ~/.fbgb/venv/bin/python -m aptbot.scrapers.facebook_generic_browser.explore.session group <group_url> [scrolls] [posts]
    ~/.fbgb/venv/bin/python -m aptbot.scrapers.facebook_generic_browser.explore.session tabs <group_url> [posts]

Everything Facebook fetches is captured passively (capture.py) into ~/.fbgb/captures/<session>/.
Milestone snapshots (HTML via page.content(), screenshots) go there too.
"""
from __future__ import annotations

import asyncio
import json
import random
import re
import sys
import time

from aptbot.scrapers.facebook_generic_browser import health, human
from aptbot.scrapers.facebook_generic_browser.capture import Capture
from aptbot.scrapers.facebook_generic_browser.browser import PROFILE_DIR, open_context
from aptbot.scrapers.facebook_generic_browser.tabs import TabPool

_rng = random.SystemRandom()


async def snap(page, cap: Capture, label: str) -> None:
    cap.seq += 1
    base = cap.dir / f"{cap.seq:04d}_snap_{label}"
    base.with_suffix(".html").write_text(await page.content())
    await page.screenshot(path=str(base.with_suffix(".png")))
    cap._index({"kind": "snap", "label": label, "url": page.url[:200], "file": base.name})


async def landmarks(page) -> dict:
    """Cheap structural census of the current page (role/aria based; classes are obfuscated)."""
    return await page.evaluate("""() => {
      const q = s => document.querySelectorAll(s).length;
      return {
        url: location.href, title: document.title, lang: document.documentElement.lang,
        dir: document.documentElement.dir,
        main: q('[role="main"]'), feed: q('[role="feed"]'), articles: q('[role="article"]'),
        navigation: q('[role="navigation"]'), banner: q('[role="banner"]'), dialog: q('[role="dialog"]'),
        complementary: q('[role="complementary"]'), search: q('[role="search"], input[type="search"]'),
        group_links: q('a[href*="/groups/"]'), posts_links: q('a[href*="/posts/"], a[href*="/permalink/"]'),
        see_more: [...document.querySelectorAll('[role="button"]')].filter(b => /^(See more|ראה עוד|עוד)$/.test(b.innerText.trim())).length,
      };
    }""")


async def start(ctx, cap: Capture):
    page = ctx.pages[0] if ctx.pages else await ctx.new_page()
    pool = TabPool(ctx, max_tabs=3)
    await pool.goto(page, "https://www.facebook.com/")
    await page.wait_for_load_state("load")
    await health.check(page)
    await human.think(2, 4)
    async with pool.foreground(page):
        await human.idle_wiggle(page)
        await human.wheel(page, _rng.uniform(600, 1400))  # glance at the home feed like anyone would
        await human.think(1.5, 3.5)
        await human.wheel(page, -_rng.uniform(300, 800))
    await health.check(page)
    cap._index({"kind": "landmarks", "where": "home", **await landmarks(page)})
    await snap(page, cap, "home")
    return page, pool


async def joined(cap: Capture) -> None:
    async with open_context(PROFILE_DIR) as ctx:
        cap.attach(ctx)
        page, pool = await start(ctx, cap)
        # Go to Groups the way a user does: the left-nav / shortcut link, falling back to the URL.
        link = page.locator('a[href$="/groups/"], a[href*="/groups/?"], a[href*="/groups/feed"]').first
        if await link.count():
            async with pool.foreground(page):
                await human.click(page, link)
            await page.wait_for_load_state("load")
        else:
            await pool.goto(page, "https://www.facebook.com/groups/feed/")
        await human.think(2, 4)
        await health.check(page)
        cap._index({"kind": "landmarks", "where": "groups_feed", **await landmarks(page)})
        await snap(page, cap, "groups_feed")
        await pool.goto(page, "https://www.facebook.com/groups/joins/")
        await page.wait_for_load_state("load")
        await health.check(page)
        await human.think(2, 4)
        async with pool.foreground(page):
            for _ in range(_rng.randint(3, 5)):
                await human.wheel(page, _rng.uniform(700, 1300))
                await human.think(1.2, 3.0)
        await health.check(page)
        cap._index({"kind": "landmarks", "where": "groups_joins", **await landmarks(page)})
        await snap(page, cap, "groups_joins")
        groups = await page.evaluate("""() => {
          const seen = new Map();
          for (const a of document.querySelectorAll('[role="main"] a[href*="/groups/"]')) {
            const m = a.href.match(/facebook\\.com\\/groups\\/([^/?#]+)\\/?(?:[?#]|$)/);
            if (!m || ['feed','joins','discover','create','notifications'].includes(m[1])) continue;
            const t = a.innerText.trim().split('\\n')[0];
            if (t && !seen.has(m[1])) seen.set(m[1], t);
          }
          return [...seen].map(([id, name]) => ({id, name}));
        }""")
        (cap.dir / "joined_groups.json").write_text(json.dumps(groups, ensure_ascii=False, indent=1))
        print(f"{len(groups)} joined groups")
        for g in groups:
            print(f"  {g['id']}\t{g['name']}")
        await human.think(1, 3)


ARTICLE_CENSUS_JS = """() => {
  const feed = document.querySelector('[role="feed"]');
  const arts = [...document.querySelectorAll('[role="article"]')];
  const top = arts.filter(a => !a.parentElement.closest('[role="article"]'));
  return {
    feed_children: feed ? feed.children.length : null,
    articles: arts.length, top_level_articles: top.length,
    with_posinset: document.querySelectorAll('[aria-posinset]').length,
    sample: top.slice(0, 4).map(a => ({
      attrs: [...a.attributes].map(x => x.name + '=' + x.value.slice(0, 40)),
      links: [...a.querySelectorAll('a[href]')].map(l => l.getAttribute('href'))
              .filter(h => /posts|permalink|photo|[/]user[/]|groups/.test(h)).slice(0, 8)
              .map(h => h.replace(/[?&](__cft__|__tn__)[^&]*/g, '').slice(0, 90)),
      buttons: [...a.querySelectorAll('[role="button"]')].map(b => (b.getAttribute('aria-label') || b.innerText).trim().slice(0, 30)).filter(Boolean).slice(0, 14),
      text_len: a.innerText.length,
    })),
  };
}"""


async def group(cap: Capture, url: str, scrolls: int) -> None:
    async with open_context(PROFILE_DIR) as ctx:
        cap.attach(ctx)
        page, pool = await start(ctx, cap)
        await pool.goto(page, url)
        await page.wait_for_load_state("load")
        await health.check(page)
        await human.think(2, 4)
        cap._index({"kind": "landmarks", "where": "group_top", **await landmarks(page)})
        await snap(page, cap, "group_top")
        for i in range(scrolls):
            async with pool.foreground(page):
                await human.wheel(page, _rng.uniform(900, 1800))
            await human.think(1.5, 4.0)  # skim what scrolled into view
            if i % 2:
                await human.idle_wiggle(page)
        await health.check(page)
        census = await page.evaluate(ARTICLE_CENSUS_JS)
        cap._index({"kind": "census", **census})
        (cap.dir / "census.json").write_text(json.dumps(census, ensure_ascii=False, indent=1))
        cap._index({"kind": "landmarks", "where": "group_scrolled", **await landmarks(page)})
        await snap(page, cap, "group_scrolled")
        print(json.dumps(census, ensure_ascii=False, indent=1)[:4000])


def _find(o, key, out):
    if isinstance(o, dict):
        for k, v in o.items():
            if k == key:
                out.append(v)
            _find(v, key, out)
    elif isinstance(o, list):
        for v in o:
            _find(v, key, out)


def stories(chunks) -> list[dict]:
    """Story nodes (group posts) in graphql chunks: {post_id, creation_time}."""
    nodes: list = []
    _find(chunks, "node", nodes)
    out = {}
    for n in nodes:
        if isinstance(n, dict) and n.get("__typename") == "Story" and n.get("post_id"):
            out[n["post_id"]] = {"post_id": n["post_id"], "creation_time": n.get("creation_time")}
    return list(out.values())


async def harvest(cap: Capture, url: str, want: int) -> None:
    """Fast path: one typed URL, then scroll; every wait is bounded by FB's own pagination event."""
    seen: dict[str, dict] = {}
    arrived = asyncio.Event()

    def on_gql(name, variables, chunks, page):
        if name.startswith("GroupsCometFeed") or name == "CometGroupDiscussionRootSuccessQuery":
            for s in stories(chunks):
                seen.setdefault(s["post_id"], s)
            arrived.set()

    cap.listeners.append(on_gql)
    async with open_context(PROFILE_DIR) as ctx:
        cap.attach(ctx)
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()
        pool = TabPool(ctx, max_tabs=1)
        t0 = time.monotonic()
        await pool.goto(page, url)
        await health.check(page)
        timeline = []
        stalls = 0
        while len(seen) < want and stalls < 4:
            arrived.clear()
            async with pool.foreground(page):
                await human.wheel(page, _rng.uniform(900, 1600))
            try:
                await asyncio.wait_for(arrived.wait(), timeout=_rng.uniform(2.5, 3.5))
                stalls = 0
            except asyncio.TimeoutError:
                stalls += 1  # nothing paginated: keep scrolling
            await human.micro()
            timeline.append((round(time.monotonic() - t0, 1), len(seen)))
        await health.check(page)
        dt = time.monotonic() - t0
        await snap(page, cap, "harvest_end")
        rep = {"url": url, "posts": len(seen), "seconds": round(dt, 1), "posts_per_min": round(len(seen) / dt * 60, 1),
               "timeline": timeline, "order_check": [s["creation_time"] for s in seen.values()]}
        (cap.dir / "harvest.json").write_text(json.dumps(rep, indent=1))
        print(json.dumps({k: v for k, v in rep.items() if k != "order_check"}))
        print("creation_time in arrival order:", [time.strftime("%d %H:%M", time.localtime(t or 0)) for t in rep["order_check"]])


async def deep(cap: Capture, group_url: str) -> None:
    """Private group feed → open one post's comments (dialog) → 2 permalinks in background tabs → search."""
    found: dict[str, dict] = {}
    arrived = asyncio.Event()

    def on_gql(name, variables, chunks, page):
        if name.startswith("GroupsCometFeed"):
            for st in stories(chunks):
                found.setdefault(st["post_id"], st)
            arrived.set()

    cap.listeners.append(on_gql)
    async with open_context(PROFILE_DIR) as ctx:
        cap.attach(ctx)
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()
        pool = TabPool(ctx, max_tabs=3)
        await pool.goto(page, group_url)
        await health.check(page)
        cap._index({"kind": "landmarks", "where": "private_group_top", **await landmarks(page)})
        await snap(page, cap, "private_group_top")
        for _ in range(4):
            arrived.clear()
            async with pool.foreground(page):
                await human.wheel(page, _rng.uniform(900, 1500))
            try:
                await asyncio.wait_for(arrived.wait(), timeout=2.5)
            except asyncio.TimeoutError:
                pass
        print("private group posts:", len(found))

        # 1) comments: click a "N comments" button inside a rendered feed post → post dialog
        btn = page.locator('[role="feed"] [role="button"]').filter(
            has_text=re.compile(r"^\s*\d+\s+(comments?|תגובות|תגובה)\s*$", re.I)).first
        if await btn.count():
            async with pool.foreground(page):
                await human.click(page, btn)
            await page.wait_for_selector('[role="dialog"]', timeout=10_000)
            await asyncio.sleep(_rng.uniform(1.5, 2.5))
            await health.check(page)
            cap._index({"kind": "landmarks", "where": "post_dialog", **await landmarks(page)})
            await snap(page, cap, "post_dialog")
            async with pool.foreground(page):
                await human.wheel(page, _rng.uniform(500, 900))  # scroll comments inside the dialog
                await asyncio.sleep(_rng.uniform(0.8, 1.5))
                await page.keyboard.press("Escape")
            await human.think()
        else:
            print("no comment button rendered")

        # 2) two permalinks in background tabs (multi-tab + permalink page capture)
        gid = re.search(r"/groups/([^/?]+)", group_url).group(1)
        tabs = []
        for pid in list(found)[:2]:
            tabs.append(await pool.open_url_in_background(page, f"https://www.facebook.com/groups/{gid}/posts/{pid}/"))
            await human.think()
        vis = [await t.evaluate("() => document.visibilityState") for t in tabs]
        cap._index({"kind": "multitab", "bg_visibility": vis})
        print("background tabs visibility:", vis)
        await asyncio.sleep(_rng.uniform(3, 4))  # they load while we stay on the feed
        for t in tabs:
            async with pool.foreground(t):
                await health.check(t)
                cap._index({"kind": "landmarks", "where": "permalink", **await landmarks(t)})
                await snap(t, cap, "permalink")
                await human.wheel(t, _rng.uniform(400, 800))
            await pool.close(t)
        await page.bring_to_front()

        # 3) search box: type a query, Enter, then the Groups filter
        box = page.locator('input[type="search"]').first
        async with pool.foreground(page):
            await human.click(page, box)
            await human.type_text(page, "דירה להשכרה")
            await human.micro()
            await page.keyboard.press("Enter")
        await page.wait_for_load_state("domcontentloaded")
        await asyncio.sleep(_rng.uniform(2, 3))
        await health.check(page)
        cap._index({"kind": "landmarks", "where": "search_top", **await landmarks(page)})
        await snap(page, cap, "search_top")
        grp = page.locator('a[href*="/search/groups"]').first
        if await grp.count():
            async with pool.foreground(page):
                await human.click(page, grp)
            await asyncio.sleep(_rng.uniform(2, 3))
            await health.check(page)
            cap._index({"kind": "landmarks", "where": "search_groups", **await landmarks(page)})
            await snap(page, cap, "search_groups")
        print("search url:", page.url)


async def main(argv: list[str]) -> None:
    cmd = argv[0] if argv else "joined"
    cap = Capture(None, f"{time.strftime('%Y%m%d-%H%M%S')}_{cmd}")
    t0 = time.monotonic()
    try:
        if cmd == "joined":
            await joined(cap)
        elif cmd == "harvest":
            await harvest(cap, argv[1], int(argv[2]) if len(argv) > 2 else 30)
        elif cmd == "deep":
            await deep(cap, argv[1])
        elif cmd == "group":
            await group(cap, argv[1], int(argv[2]) if len(argv) > 2 else 6)
        else:
            raise SystemExit(f"unknown command {cmd}")
    except health.AccountAtRisk as e:
        print(f"STOPPED, account at risk: {e}")
        cap._index({"kind": "stop", "reason": str(e)})
    finally:
        print(f"session {time.monotonic() - t0:.0f}s, captures: {cap.dir}")
        print("graphql seen:", json.dumps(cap.friendly_names, indent=1))


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:]))
