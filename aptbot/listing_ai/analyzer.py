"""ListingAnalyzer: posts in, Listing Facts out, never raises.

Flow for :meth:`ListingAnalyzer.analyze`:
dedupe by key -> cache lookup (same PROMPT_VERSION only) -> chunks of ``batch_size``
-> at most ``concurrency`` batches in flight -> strict structured output -> map back
by ``ref`` -> store -> return ``{key: ListingFacts}``.

Failure policy (the AI is purely additive): a batch that fails is retried once, then
its posts are simply absent from the result, so the caller sends them raw. When the
subscription's usage window is exhausted, the analyzer pauses until the reset time the
backend reports and returns cache hits only (no calls) until then.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Protocol, Sequence

from aptbot.listing_ai.llm.client import UsageLimited, usage_limit_from
from aptbot.listing_ai.prompt import PROMPT_VERSION, SYSTEM_PROMPT, build_batch_payload, post_item
from aptbot.listing_ai.schema import BatchResult, ListingFacts

logger = logging.getLogger(__name__)

_HOME_ONLY = [f for f in ListingFacts.model_fields if f not in ("ref", "post_kind")]


@dataclass(frozen=True)
class PostInput:
    """One post to analyze. ``key`` is the caller's dedup id (the FB content hash)."""
    key: str
    text: str
    posted_at: Optional[datetime] = None
    group_name: Optional[str] = None
    photo_count: int = 0


class AnalysisCache(Protocol):
    async def get_fb_analysis(self, keys: Sequence[str], prompt_version: str) -> Dict[str, str]: ...
    async def put_fb_analysis(self, rows: Sequence[tuple]) -> None: ...   # (key, model, version, json)


@dataclass
class AnalyzerState:
    """What /status reports about the AI side."""
    paused_until: Optional[datetime] = None
    last_error: Optional[str] = None
    analyzed: int = 0              # posts analyzed by the model (cache hits excluded)
    failed: int = 0                # posts that came back without facts
    last_batch_seconds: Optional[float] = None


class ListingAnalyzer:
    def __init__(self, model: Any, *, model_name: str, cache: Optional[AnalysisCache] = None,
                 batch_size: int = 15, concurrency: int = 3, timeout: float = 120.0,
                 clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc)):
        """``model``: a LangChain chat model (anything with ``with_structured_output``)."""
        self._structured = model.with_structured_output(BatchResult, method="json_schema", strict=True)
        self.model_name = model_name
        self.cache = cache
        self.batch_size = max(1, batch_size)
        self.timeout = timeout
        self._sem = asyncio.Semaphore(max(1, concurrency))
        self._clock = clock
        self.state = AnalyzerState()

    @classmethod
    def from_config(cls, cache: Optional[AnalysisCache] = None) -> "ListingAnalyzer":
        from aptbot import config
        from aptbot.listing_ai.llm.client import make_chat_model
        model = make_chat_model(config.AI_MODEL, instructions=SYSTEM_PROMPT, effort=config.AI_EFFORT,
                                timeout=config.AI_TIMEOUT)
        return cls(model, model_name=config.AI_MODEL, cache=cache, batch_size=config.AI_BATCH,
                   concurrency=config.AI_CONCURRENCY, timeout=config.AI_TIMEOUT)

    @property
    def paused(self) -> bool:
        return self.state.paused_until is not None and self._clock() < self.state.paused_until

    async def analyze(self, posts: Sequence[PostInput]) -> Dict[str, ListingFacts]:
        unique: Dict[str, PostInput] = {}
        for p in posts:
            if p.key not in unique and (p.text or "").strip():
                unique[p.key] = p
        if not unique:
            return {}
        out: Dict[str, ListingFacts] = await self._from_cache(list(unique))
        todo = [p for k, p in unique.items() if k not in out]
        if not todo:
            return out
        if self.paused:
            logger.info("AI paused until %s: %d post(s) go without facts", self.state.paused_until, len(todo))
            self.state.failed += len(todo)
            return out
        chunks = [todo[i:i + self.batch_size] for i in range(0, len(todo), self.batch_size)]
        results = await asyncio.gather(*(self._run_batch(c) for c in chunks))
        fresh: Dict[str, ListingFacts] = {}
        for r in results:
            fresh.update(r)
        self.state.analyzed += len(fresh)
        self.state.failed += len(todo) - len(fresh)
        await self._to_cache(fresh)
        out.update(fresh)
        return out

    async def _run_batch(self, chunk: List[PostInput]) -> Dict[str, ListingFacts]:
        payload = build_batch_payload([
            post_item(i, p.text, posted_at=p.posted_at, group_name=p.group_name, photo_count=p.photo_count)
            for i, p in enumerate(chunk)])
        async with self._sem:
            for attempt in (1, 2):
                if self.paused:
                    return {}
                started = time.monotonic()
                try:
                    result = await asyncio.wait_for(self._structured.ainvoke(payload), self.timeout)
                except Exception as exc:          # noqa: BLE001 — every failure degrades to "no facts"
                    limited = exc if isinstance(exc, UsageLimited) else usage_limit_from(exc)
                    if limited is not None:
                        self.state.paused_until = limited.resets_at
                        self.state.last_error = str(limited)
                        logger.warning("AI usage limit reached; paused until %s", limited.resets_at)
                        return {}
                    self.state.last_error = f"{type(exc).__name__}: {str(exc)[:200]}"
                    logger.warning("AI batch of %d failed (attempt %d): %s", len(chunk), attempt,
                                   self.state.last_error)
                    continue
                self.state.last_batch_seconds = time.monotonic() - started
                return _map_results(chunk, result)
        return {}

    async def _from_cache(self, keys: List[str]) -> Dict[str, ListingFacts]:
        if self.cache is None:
            return {}
        try:
            rows = await self.cache.get_fb_analysis(keys, PROMPT_VERSION)
        except Exception as exc:                  # noqa: BLE001
            logger.warning("AI cache read failed: %s", exc)
            return {}
        out = {}
        for k, raw in rows.items():
            try:
                out[k] = ListingFacts.model_validate_json(raw)
            except Exception:                     # noqa: BLE001 — stale/corrupt row: re-analyze
                pass
        return out

    async def _to_cache(self, facts: Dict[str, ListingFacts]) -> None:
        if self.cache is None or not facts:
            return
        rows = [(k, self.model_name, PROMPT_VERSION, f.model_dump_json()) for k, f in facts.items()]
        try:
            await self.cache.put_fb_analysis(rows)
        except Exception as exc:                  # noqa: BLE001
            logger.warning("AI cache write failed: %s", exc)


def _map_results(chunk: List[PostInput], result: Any) -> Dict[str, ListingFacts]:
    """Results -> {key: facts} by ``ref``. Unknown/duplicate refs are dropped; a missing ref
    just leaves that post without facts. Non-home kinds keep only ``post_kind``."""
    if isinstance(result, dict):                  # include_raw / json-mode shapes
        result = result.get("parsed", result)
        if isinstance(result, dict):
            result = BatchResult.model_validate(result)
    out: Dict[str, ListingFacts] = {}
    for f in getattr(result, "results", None) or []:
        if not 0 <= f.ref < len(chunk):
            logger.warning("AI returned unknown ref %s", f.ref)
            continue
        key = chunk[f.ref].key
        if key in out:
            continue
        if not f.is_home:
            f = f.model_copy(update={name: None for name in _HOME_ONLY})
        elif f.city_area_or_address:
            f = f.model_copy(update={"city_area_or_address": _dedupe_parts(f.city_area_or_address)})
        out[key] = f
    if len(out) < len(chunk):
        logger.warning("AI returned facts for %d of %d posts", len(out), len(chunk))
    return out


def _dedupe_parts(address: str) -> str:
    """'פלורנטין, פלורנטין' -> 'פלורנטין' (the model repeats a street named like its area)."""
    parts = []
    for part in (x.strip() for x in address.split(",")):
        if part and part not in parts:
            parts.append(part)
    return ", ".join(parts) or address.strip()
