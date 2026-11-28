"""Synchronous wrapper around one `browser_oxide.Browser`, used only inside the worker process.

Input goes through oxide's trusted-input hook (kb/input-and-scrolling.md): real pointer paths, wheel ticks and key
events, never `element.click()` or `scrollTo`. Facebook's data comes from the engine-side network tap
(kb/engine-changes.md#network-tap), never from wrapping page JS.
"""
from __future__ import annotations

import json
import math
import os
import random
import time

from aptbot.scrapers.facebook_generic_browser.capture import Event
from aptbot.scrapers.facebook_generic_browser.oxide.events import TAP_PATTERNS, to_events
from aptbot.scrapers.facebook_generic_browser.oxide.health import SNAPSHOT_JS, check_snapshot

_rng = random.SystemRandom()
FB = "https://www.facebook.com"
HOOK = "globalThis[Symbol.for('__browser_oxide_script_hook__')].input"


def micro() -> None:
    """Gap between two small actions: log-normal, median ~110 ms, 40-350 ms, ~3% 0.4-1.2 s (same model as ..human)."""
    if _rng.random() < 0.03:
        time.sleep(_rng.uniform(0.4, 1.2))
    else:
        time.sleep(min(0.35, max(0.04, _rng.lognormvariate(math.log(0.11), 0.55))))


def think(lo: float = 0.25, hi: float = 0.9) -> None:
    time.sleep(_rng.uniform(lo, hi))


def rss_mb(pid: int | None = None) -> int:
    """Resident set size of `pid` (default: this process) in MB. /proc on Linux, ps elsewhere."""
    pid = pid or os.getpid()
    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) // 1024
    except OSError:
        pass
    import subprocess
    out = subprocess.run(["ps", "-o", "rss=", "-p", str(pid)], capture_output=True, text=True).stdout
    return int(out.strip() or 0) // 1024


class Driver:
    """One page at a time. Every method blocks; the worker calls them one by one."""

    def __init__(self, browser=None, *, boot_pump_ms: int = 6000):
        if browser is None:
            from browser_oxide import Browser, Profile
            browser = Browser(profile=Profile.chrome())
        self.b = browser
        self.boot_pump_ms = boot_pump_ms
        self.dropped = 0                     # tap records lost to the queue cap (should stay 0)
        self.b.tap(TAP_PATTERNS, max_kb=3072)

    # ------------------------------------------------------------------ network
    def events(self) -> list[Event]:
        """Everything the tap recorded since the last call, as capture.Events."""
        recs, dropped = self.b.take_responses()
        self.dropped += dropped
        return to_events(recs)

    def pump(self, ms: int) -> list[Event]:
        self.b.pump(int(ms))
        return self.events()

    def goto(self, url: str) -> list[Event]:
        """Typed-URL navigation (a new document; the heap resets), boot pump, health check. Returns its events."""
        self.events()                        # drop leftovers from the previous page
        self.b.navigate(url, max_iterations=10)
        evs = self.pump(self.boot_pump_ms)
        self.check_health()
        return evs

    # ------------------------------------------------------------------ input
    def _act(self, call: str, timeout: float = 40) -> str:
        """Run one input-hook action and pump until its queue drains."""
        first = self.b.evaluate(f"{HOOK}.{call}")
        t0 = time.monotonic()
        while time.monotonic() - t0 < timeout:
            self.b.pump(40)
            if self.b.evaluate(f"{HOOK}.busy()") == "false":
                break
        self.b.evaluate(f"{HOOK}.takeLog()")
        return first

    def wheel(self, px: float) -> None:
        self._act(f"wheel({int(px)})")

    def press(self, key: str) -> None:
        self._act(f"press({json.dumps(key)})")

    def click(self, element_js: str) -> bool:
        """Click the element `element_js` evaluates to (trusted pointer path). False if it doesn't exist."""
        found = self.b.evaluate(f"(() => {{ const el = {element_js}; return el ? 'yes' : 'no'; }})()") == "yes"
        if found:
            self._act(f"click({element_js})")
        return found

    def scroll_burst(self) -> None:
        """1-3 quick wheel flicks of 700-1400 px, like a person before glancing (same as the Chrome jobs)."""
        for _ in range(_rng.choice((1, 2, 2, 3))):
            self.wheel(_rng.uniform(700, 1400))
            micro()

    # ------------------------------------------------------------------ page state
    def js(self, expr: str) -> str:
        return self.b.evaluate(expr)

    def count(self, selector: str) -> int:
        return int(self.b.evaluate(f"String(document.querySelectorAll({json.dumps(selector)}).length)") or 0)

    def url(self) -> str:
        return self.b.evaluate("location.href")

    def check_health(self) -> None:
        """Raise AccountAtRisk on a checkpoint/login wall/block (never retried; the caller halts)."""
        check_snapshot(json.loads(self.b.evaluate(SNAPSHOT_JS)))

    def cookie_names(self, url: str = FB) -> set[str]:
        """Names only: the values are dropped right here and never leave this function."""
        header = self.b.cookies(url) or ""
        return {part.split("=", 1)[0].strip() for part in header.split(";") if "=" in part}

    def save_cookies(self, path: str) -> None:
        self.b.save_cookies(path)
        os.chmod(path, 0o600)
