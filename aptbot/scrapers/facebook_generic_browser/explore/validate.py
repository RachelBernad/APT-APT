"""A/B the browser controllers against public bot-detection pages. No Facebook involved.

    ~/.fbgb/venv/bin/python -m aptbot.scrapers.facebook_generic_browser.explore.validate [engine ...]

engines: playwright (vanilla control), patchright, zendriver. Each runs headful on real Chrome with
a throwaway profile under ~/.fbgb/validation/<engine>/profile. Per site we save a screenshot, the
page text and the extracted verdicts to ~/.fbgb/validation/<engine>/, plus summary.json.
"""
from __future__ import annotations

import asyncio
import json
import shutil
import sys
from pathlib import Path

from aptbot.scrapers.facebook_generic_browser.browser import VALIDATION_DIR, open_context

SITES = {
    "rebrowser": "https://bot-detector.rebrowser.net/",
    "sannysoft": "https://bot.sannysoft.com/",
    "creepjs": "https://abrahamjuliot.github.io/creepjs/",
    "browserscan": "https://www.browserscan.net/bot-detection",
}
SETTLE_S = {"rebrowser": 6, "sannysoft": 6, "creepjs": 25, "browserscan": 15}
# Triggers rebrowser's mainWorldExecution + sourceUrlLeak tests (their documented probe).
REBROWSER_PROBE = "document.getElementById('detections-json')"
# Same probe for everyone, read from the page itself.
FINGERPRINT_JS = """() => ({
  webdriver: navigator.webdriver, ua: navigator.userAgent, platform: navigator.platform,
  languages: navigator.languages, hc: navigator.hardwareConcurrency, mem: navigator.deviceMemory,
  plugins: navigator.plugins.length, chrome: typeof window.chrome, visibility: document.visibilityState,
  screen: [screen.width, screen.height, window.innerWidth, window.innerHeight, devicePixelRatio],
  tz: Intl.DateTimeFormat().resolvedOptions().timeZone,
  webgl: (() => { try { const g = document.createElement('canvas').getContext('webgl');
    const e = g.getExtension('WEBGL_debug_renderer_info');
    return [g.getParameter(e.UNMASKED_VENDOR_WEBGL), g.getParameter(e.UNMASKED_RENDERER_WEBGL)]; }
    catch (err) { return String(err); } })(),
})"""


async def _run_pw(engine: str, out: Path) -> dict:
    results = {}
    async with open_context(out / "profile", engine=engine) as ctx:
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()
        for name, url in SITES.items():
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=60_000)
                await asyncio.sleep(2)
                if name == "rebrowser":
                    await page.evaluate(REBROWSER_PROBE)
                await asyncio.sleep(SETTLE_S[name])
                rec = {"text": await page.locator("body").inner_text()}
                if name == "rebrowser":
                    rec["detections"] = await page.locator("#detections-json").input_value()
                if name == "sannysoft":
                    rec["fingerprint"] = await page.evaluate(FINGERPRINT_JS)
                await page.screenshot(path=str(out / f"{name}.png"), full_page=True)
                results[name] = rec
            except Exception as e:  # keep going: one broken site must not hide the others
                results[name] = {"error": repr(e)}
        results["background_tab"] = await _background_probe(ctx, page)
        cmd = await ctx.new_page()
        await cmd.goto("chrome://version")
        results["command_line"] = await cmd.locator("#command_line").inner_text()
        await cmd.close()
    return results


# Does a hidden tab get throttled like in a normal Chrome? Count 100 ms interval ticks for 3 s while
# another tab is in front: ~30 = unthrottled (automation default), ~3 = normal Chrome behavior.
_TICK_START = """() => { window.__t = 0; setInterval(() => { window.__t++; }, 100); }"""


async def _background_probe(ctx, page) -> dict:
    await page.bring_to_front()
    await page.evaluate(_TICK_START)
    other = await ctx.new_page()
    await other.goto("https://example.com/")
    await other.bring_to_front()
    await asyncio.sleep(1)
    before = await page.evaluate("() => window.__t")
    vis = await page.evaluate("() => document.visibilityState")
    await asyncio.sleep(3)
    after = await page.evaluate("() => window.__t")
    await other.close()
    return {"hidden_visibility": vis, "ticks_in_3s": after - before}


async def _run_zendriver(out: Path) -> dict:
    import zendriver as zd

    results = {}
    browser = await zd.start(user_data_dir=str(out / "profile"), headless=False)
    try:
        page = await browser.get("about:blank")
        for name, url in SITES.items():
            try:
                page = await browser.get(url)
                await asyncio.sleep(2)
                if name == "rebrowser":
                    await page.evaluate(REBROWSER_PROBE)
                await asyncio.sleep(SETTLE_S[name])
                rec = {"text": await page.evaluate("document.body.innerText")}
                if name == "rebrowser":
                    rec["detections"] = await page.evaluate(
                        "document.querySelector('#detections-json')?.value")
                if name == "sannysoft":
                    rec["fingerprint"] = await page.evaluate(f"({FINGERPRINT_JS})()")
                await page.save_screenshot(str(out / f"{name}.png"), full_page=True)
                results[name] = rec
            except Exception as e:
                results[name] = {"error": repr(e)}
    finally:
        await browser.stop()
    return results


async def main(engines: list[str]) -> None:
    for engine in engines:
        out = VALIDATION_DIR / engine
        shutil.rmtree(out, ignore_errors=True)
        out.mkdir(parents=True)
        print(f"== {engine}", flush=True)
        res = await (_run_zendriver(out) if engine == "zendriver" else _run_pw(engine, out))
        (out / "summary.json").write_text(json.dumps(res, ensure_ascii=False, indent=1, default=str))
        for name, rec in res.items():
            print(f"  {name}: {'ERROR ' + rec['error'] if 'error' in rec else 'ok'}", flush=True)


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:] or ["playwright", "patchright", "zendriver"]))
