"""Human-like input and timing. Every wait is random; nothing uses a constant interval.

Timing model (see api_docs/stealth/behavior-model.md):
- Speed is a requirement: waits are short and EVENT-driven (wait for FB's response, not for time).
- micro(): the gap between two small actions (wheel tick, hover, key). Log-normal, median ~110 ms,
  clipped to 40-350 ms, with ~3% "distraction" outliers of 0.4-1.2 s.
- Mouse paths: cubic Bezier from the last known position with a little overshoot, 60-125 Hz events.
- Scrolling: mouse.wheel in uneven ticks (real wheel/trackpad deltas), sometimes a short scroll-back.
- read_time(): dwell proportional to text length (220-320 wpm), clipped.
"""
from __future__ import annotations

import asyncio
import math
import random
import sys

_rng = random.SystemRandom()
_mouse: dict[int, tuple[float, float]] = {}  # last pointer position per page (Playwright doesn't expose it)

OPEN_IN_BG_MODIFIER = "Meta" if sys.platform == "darwin" else "Control"


def _lognormal_ms(median: float, sigma: float, lo: float, hi: float) -> float:
    return min(hi, max(lo, _rng.lognormvariate(math.log(median), sigma)))


async def micro() -> None:
    """Very short random gap between two small actions."""
    if _rng.random() < 0.03:
        await asyncio.sleep(_rng.uniform(0.4, 1.2))
    else:
        await asyncio.sleep(_lognormal_ms(110, 0.55, 40, 350) / 1000)


async def think(lo: float = 0.25, hi: float = 0.9) -> None:
    """A short decision pause (choosing what to click next). Kept short: speed is a requirement."""
    await asyncio.sleep(_rng.uniform(lo, hi))


def read_time(text: str, *, lo: float = 1.5, hi: float = 25.0) -> float:
    words = max(1, len(text.split()))
    return min(hi, max(lo, words / _rng.uniform(220, 320) * 60 + _rng.uniform(0.3, 1.5)))


async def read(text: str) -> None:
    await asyncio.sleep(read_time(text))


def _bezier(p0, p1, p2, p3, t):
    u = 1 - t
    return (u**3 * p0[0] + 3 * u * u * t * p1[0] + 3 * u * t * t * p2[0] + t**3 * p3[0],
            u**3 * p0[1] + 3 * u * u * t * p1[1] + 3 * u * t * t * p2[1] + t**3 * p3[1])


async def move_to(page, x: float, y: float) -> None:
    """Curved, eased pointer movement to (x, y), ending exactly on target."""
    sx, sy = _mouse.get(id(page), (_rng.uniform(200, 700), _rng.uniform(150, 450)))
    dist = math.hypot(x - sx, y - sy)
    if dist < 2:
        return
    spread = min(200.0, dist * 0.3)
    c1 = (sx + (x - sx) * _rng.uniform(0.2, 0.4) + _rng.uniform(-spread, spread),
          sy + (y - sy) * _rng.uniform(0.2, 0.4) + _rng.uniform(-spread, spread))
    c2 = (sx + (x - sx) * _rng.uniform(0.6, 0.8) + _rng.uniform(-spread / 2, spread / 2),
          sy + (y - sy) * _rng.uniform(0.6, 0.8) + _rng.uniform(-spread / 2, spread / 2))
    steps = int(min(32, max(8, dist / _rng.uniform(18, 30))))  # ~0.1-0.4 s per move
    for i in range(1, steps + 1):
        t = i / steps
        t = t * t * (3 - 2 * t)  # ease in/out
        px, py = _bezier((sx, sy), c1, c2, (x, y), t)
        await page.mouse.move(px, py)
        await asyncio.sleep(_rng.uniform(0.008, 0.017))
    _mouse[id(page)] = (x, y)


async def wheel(page, total_px: float) -> None:
    """Scroll by roughly total_px (negative = up) in uneven wheel ticks."""
    done = 0.0
    direction = 1 if total_px >= 0 else -1
    while abs(done) < abs(total_px):
        tick = direction * _rng.choice((100, 100, 100, 120, 80, 57, 140))
        await page.mouse.wheel(0, tick)
        done += tick
        await asyncio.sleep(_lognormal_ms(55, 0.5, 18, 200) / 1000)
        if _rng.random() < 0.06:
            await micro()
    if direction > 0 and _rng.random() < 0.08:  # overshoot, then come back a little
        await micro()
        await page.mouse.wheel(0, -_rng.choice((100, 120, 200)))


async def scroll_into_view(page, locator, *, margin: float = 120) -> dict | None:
    """Wheel-scroll (not scrollIntoView) until the element sits comfortably in the viewport."""
    for _ in range(40):
        box = await locator.bounding_box()
        if box is None:
            return None
        vh = await page.evaluate("() => innerHeight")
        if margin <= box["y"] <= vh - margin - min(box["height"], vh / 3):
            return box
        delta = box["y"] - vh * _rng.uniform(0.3, 0.45)
        await wheel(page, max(-900, min(900, delta)))
        await micro()
    return await locator.bounding_box()


async def click(page, locator, *, modifiers: list[str] | None = None) -> None:
    """Human click: wheel the element into view, curve to a jittered point inside it, press/release."""
    box = await scroll_into_view(page, locator)
    if box is None:
        raise RuntimeError("element not visible")
    x = box["x"] + box["width"] * min(0.85, max(0.15, _rng.gauss(0.5, 0.15)))
    y = box["y"] + box["height"] * min(0.8, max(0.2, _rng.gauss(0.5, 0.12)))
    await move_to(page, x, y)
    await micro()
    for m in modifiers or []:
        await page.keyboard.down(m)
    await page.mouse.down()
    await asyncio.sleep(_rng.uniform(0.05, 0.13))
    await page.mouse.up()
    for m in reversed(modifiers or []):
        await asyncio.sleep(_rng.uniform(0.02, 0.08))
        await page.keyboard.up(m)


async def type_text(page, text: str) -> None:
    """Type with per-key random gaps (search boxes; the assisted login form). Never log `text`."""
    for ch in text:
        await page.keyboard.type(ch)
        await asyncio.sleep(_lognormal_ms(120, 0.45, 45, 400) / 1000)


async def idle_wiggle(page) -> None:
    """Small aimless pointer drift, like a hand resting on a mouse while reading."""
    x, y = _mouse.get(id(page), (400.0, 300.0))
    await move_to(page, x + _rng.uniform(-60, 60), y + _rng.uniform(-40, 40))
