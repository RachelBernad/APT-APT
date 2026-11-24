"""Check the browser ENVIRONMENT (not a website): sandbox, GPU/WebGL, and the "headless-like" desktop signals.

    python -m aptbot.scrapers.facebook_generic_browser.explore.tools.envprobe [--json]

Use it whenever the browser runs somewhere new (Docker, Pi, VM, CI) before any real site (playbook §1b).
Compare its output with the same probe on a real desktop: every difference is a candidate tell.
Uses a throwaway profile; touches only example.com and chrome:// pages.
"""
import asyncio
import json
import sys
import tempfile
from pathlib import Path

from aptbot.scrapers.facebook_generic_browser.browser import open_context

SIGNALS_JS = """async () => {
  const r = {};
  const c = document.createElement('canvas'), g = c.getContext('webgl');
  if (g) { const e = g.getExtension('WEBGL_debug_renderer_info');
           r.webgl = [g.getParameter(e.UNMASKED_VENDOR_WEBGL), g.getParameter(e.UNMASKED_RENDERER_WEBGL)]; }
  else r.webgl = null;
  r.webgl2 = !!document.createElement('canvas').getContext('webgl2');      // fresh canvas: one context type per canvas
  r.ua = navigator.userAgent; r.platform = navigator.platform;
  r.brands = (navigator.userAgentData && navigator.userAgentData.brands || []).map(b => b.brand);
  r.webdriver = navigator.webdriver;
  r.tz = Intl.DateTimeFormat().resolvedOptions().timeZone; r.languages = navigator.languages;
  r.screen = [screen.width, screen.height, screen.availWidth, screen.availHeight, innerWidth, innerHeight, devicePixelRatio, screen.colorDepth];
  r.noTaskbar = screen.height === screen.availHeight && screen.width === screen.availWidth;
  try { r.mediaDevices = (await navigator.mediaDevices.enumerateDevices()).map(d => d.kind); } catch (e) { r.mediaDevices = String(e); }
  r.hardware = [navigator.hardwareConcurrency, navigator.deviceMemory]; r.plugins = navigator.plugins.length;
  r.share = 'share' in navigator; r.pdfViewer = navigator.pdfViewerEnabled;
  r.notification = Notification.permission;
  r.hebrewFont = document.fonts.check('16px "Noto Sans Hebrew"') || document.fonts.check('16px "David CLM"');
  return r;
}"""


async def probe() -> dict:
    with tempfile.TemporaryDirectory() as d:
        async with open_context(Path(d)) as ctx:
            p = ctx.pages[0] if ctx.pages else await ctx.new_page()
            out = {}
            await p.goto("chrome://sandbox")
            await asyncio.sleep(0.5)
            text = await p.locator("body").inner_text()
            out["sandbox_ok"] = "adequately sandboxed" in text
            out["sandbox_layer1"] = next((l.split("\t")[-1] for l in text.splitlines() if l.startswith("Layer 1")), None)
            await p.goto("chrome://version")
            out["command_line"] = await p.locator("#command_line").inner_text()
            await p.goto("https://example.com/")
            await asyncio.sleep(1)
            out.update(await p.evaluate(SIGNALS_JS))
            return out


def main(argv):
    r = asyncio.run(probe())
    if "--json" in argv:
        print(json.dumps(r, ensure_ascii=False))
        return
    for k, v in r.items():
        print(f"{k:16} {v if k != 'command_line' else v[:300] + '…'}")


if __name__ == "__main__":
    main(sys.argv[1:])
