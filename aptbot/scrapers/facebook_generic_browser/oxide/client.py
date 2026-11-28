"""OxideBackend: the bot-side, async handle on one oxide worker subprocess.

- Spawned lazily on the first call, and closed by FacebookAPI after each new_posts() cycle (a fresh process per
  cycle: no leak carries over, and peak memory is bounded by one group page).
- Calls are serialized (one page, like one tab). A worker over `recycle_mb` after a call is ended (the next call
  starts fresh).
- Watchdog: polls the worker's RSS. Above `rss_limit_mb` it kills the worker and the running call fails with
  RequestTimeout("rss limit"). It never retries inside the same call.
- stderr is drained and dropped (it can contain URLs). Only crash lines (panic/Fatal/Traceback heads) are logged,
  with anything cookie- or token-like redacted.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import re
import sys

from aptbot.scrapers.facebook_generic_browser.errors import (FBGBError, NotLoggedIn, NotMember, RequestTimeout,
                                                             SchemaDrift)
from aptbot.scrapers.facebook_generic_browser.health import AccountAtRisk

logger = logging.getLogger(__name__)

# Tuned for FB on the Pi (kb/memory.md, kb/env-vars.md). Caller env wins.
ENGINE_ENV = {
    "BROWSER_OXIDE_HEAP_INITIAL_MB": "16",
    "BROWSER_OXIDE_HEAP_MAX_MB": "384",
    "BROWSER_OXIDE_V8_FLAGS": ("--max-semi-space-size=2 --optimize-for-size --no-opt --no-maglev --max-lazy "
                               "--bytecode-old-age=2 --memory-reducer-for-small-heaps --heap-growing-percent=20"),
    "MALLOC_ARENA_MAX": "2",
}
_NEVER_FORWARD = ("BO_DEBUG_HEAP", "BO_NET_LOG", "BROWSER_OXIDE_DUMP_POST_DIR")
_CRASH = re.compile(r"panicked|Fatal|FATAL|Traceback|Error:", re.I)
_SECRET = re.compile(r"(cookie|c_user|xs|datr|fr|sb|fb_dtsg|lsd|jazoest|token)[\"']?\s*[:=]\s*[^\s;,&]+", re.I)


def native_lib() -> str | None:
    """Path of browser_oxide's native module, found without importing it."""
    import glob
    import importlib.util
    spec = importlib.util.find_spec("browser_oxide")
    for d in (spec.submodule_search_locations or []) if spec else []:
        hits = glob.glob(os.path.join(d, "_native*.so"))
        if hits:
            return hits[0]
    return None


def worker_env(jar: str | None) -> dict:
    env = {k: v for k, v in os.environ.items() if k not in _NEVER_FORWARD}
    for k, v in ENGINE_ENV.items():
        env.setdefault(k, v)
    # glibc: V8 inside a dlopen'd module needs more static TLS than glibc reserves ("cannot allocate memory in
    # static TLS block"), and the GLIBC_TUNABLES workaround then crashes V8 (IsOnCentralStack). Preloading the
    # module into the worker only (python exports the Py* symbols it needs) works. kb/caveats.md#linux-tls
    if sys.platform.startswith("linux") and "LD_PRELOAD" not in env:
        lib = native_lib()
        if lib:
            env["LD_PRELOAD"] = lib
    if jar:
        env["BROWSER_OXIDE_COOKIE_JAR"] = jar
    return env


def error_from(reply: dict) -> Exception:
    code, msg = reply.get("error"), reply.get("message", "")
    if code == "account_at_risk":
        return AccountAtRisk(msg)
    if code == "not_member":
        return NotMember(reply.get("group_id", ""), reply.get("join_state"))
    if code == "schema_drift":
        return SchemaDrift(msg)
    if code == "not_logged_in":
        return NotLoggedIn(msg)
    return FBGBError(f"oxide worker {code}: {msg}")


class OxideBackend:
    def __init__(self, *, jar: str | None = None, rss_limit_mb: int = 650, recycle_mb: int = 460,
                 cmd: list[str] | None = None):
        self.jar = jar
        self.rss_limit_mb = rss_limit_mb
        # After a call, a worker above this RSS is ended, and the next call starts fresh. In one worker the RSS climbs
        # ~30 MB per group page (521 → 556 → 584 MB over 3 groups, 2026-11-28). A fresh one boots FB at ≤ ~540.
        self.recycle_mb = recycle_mb
        self.cmd = cmd or [sys.executable, "-m", "aptbot.scrapers.facebook_generic_browser.oxide.worker"]
        self._proc: asyncio.subprocess.Process | None = None
        self._lock = asyncio.Lock()
        self._seq = 0
        self._tasks: list[asyncio.Task] = []
        self._killed_for: str | None = None
        self.peak_rss_mb = 0

    @property
    def running(self) -> bool:
        return self._proc is not None and self._proc.returncode is None

    async def _start(self) -> None:
        self._killed_for = None
        self._proc = await asyncio.create_subprocess_exec(
            *self.cmd, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE, env=worker_env(self.jar), limit=64 * 1024 * 1024)
        self._tasks = [asyncio.create_task(self._drain_stderr(self._proc)),
                       asyncio.create_task(self._watch(self._proc))]
        hello = await self._call_locked("hello", {}, 120)
        if not hello.get("logged_in"):
            await self._stop()
            raise NotLoggedIn("no Facebook session in the oxide cookie jar")

    async def _drain_stderr(self, proc) -> None:
        with contextlib.suppress(Exception):
            async for raw in proc.stderr:
                line = raw.decode("utf-8", "replace").rstrip()
                if _CRASH.search(line):
                    logger.warning("oxide worker: %s", _SECRET.sub(r"\1=<redacted>", line)[:300])

    async def _watch(self, proc) -> None:
        from aptbot.scrapers.facebook_generic_browser.oxide.driver import rss_mb
        while proc.returncode is None:
            await asyncio.sleep(1.0)
            try:
                r = await asyncio.to_thread(rss_mb, proc.pid)
            except Exception:
                continue
            self.peak_rss_mb = max(self.peak_rss_mb, r)
            if r > self.rss_limit_mb:
                self._killed_for = f"rss limit ({r} MB > {self.rss_limit_mb} MB)"
                logger.warning("oxide worker killed: %s", self._killed_for)
                with contextlib.suppress(ProcessLookupError):
                    proc.kill()
                return

    async def _call_locked(self, op: str, args: dict, timeout: float):
        proc = self._proc
        self._seq += 1
        rid = self._seq
        proc.stdin.write((json.dumps({"id": rid, "op": op, "args": args}) + "\n").encode())
        await proc.stdin.drain()
        try:
            while True:
                line = await asyncio.wait_for(proc.stdout.readline(), timeout)
                if not line:
                    reason = self._killed_for or f"worker exited ({await proc.wait()})"
                    await self._stop()
                    raise RequestTimeout(f"oxide {op}: {reason}")
                reply = json.loads(line)
                if reply.get("id") == rid:
                    break
        except asyncio.TimeoutError:
            await self._stop()
            raise RequestTimeout(f"oxide {op}: no reply in {timeout:.0f}s") from None
        if "error" in reply:
            raise error_from(reply)
        return reply["ok"]

    async def call(self, op: str, args: dict | None = None, *, timeout: float = 240):
        async with self._lock:
            if not self.running:
                await self._start()
            res = await self._call_locked(op, args or {}, timeout)
            if self.recycle_mb and self.running:
                from aptbot.scrapers.facebook_generic_browser.oxide.driver import rss_mb
                with contextlib.suppress(Exception):
                    if await asyncio.to_thread(rss_mb, self._proc.pid) > self.recycle_mb:
                        await self._stop()
            return res

    async def _stop(self) -> None:
        proc, self._proc = self._proc, None
        if proc and proc.returncode is None:
            with contextlib.suppress(Exception):
                proc.stdin.write(b'{"id": 0, "op": "shutdown"}\n')
                await proc.stdin.drain()
                await asyncio.wait_for(proc.wait(), 15)
            if proc.returncode is None:
                with contextlib.suppress(ProcessLookupError):
                    proc.kill()
                await proc.wait()
        for t in self._tasks:
            t.cancel()
        self._tasks = []

    async def close(self) -> None:
        """End the worker (saves the cookie jar). The next call starts a fresh one."""
        async with self._lock:
            await self._stop()
