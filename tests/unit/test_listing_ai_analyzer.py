"""listing_ai: batching, ref mapping, cache, failure + usage-limit policy, payload.

Offline: a fake LangChain chat model; synthetic posts only (the repo is public).
"""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone

import pytest

from aptbot.listing_ai import PROMPT_VERSION, BatchResult, ListingAnalyzer, ListingFacts, PostInput, PostKind
from aptbot.listing_ai.llm.client import UsageLimited, usage_limit_from
from aptbot.listing_ai.prompt import SYSTEM_PROMPT, build_batch_payload, post_item

NULLS = {f: None for f in ListingFacts.model_fields if f not in ("ref", "post_kind")}


def facts(ref, kind="rent", **kw) -> ListingFacts:
    return ListingFacts(ref=ref, post_kind=kind, **{**NULLS, **kw})


def inputs(n, prefix="k"):
    return [PostInput(key=f"{prefix}{i}", text=f"דירה {i} חדרים להשכרה") for i in range(n)]


class FakeModel:
    """``with_structured_output`` -> runnable whose ``ainvoke`` answers from ``respond``."""

    def __init__(self, respond=None, delay=0.0):
        self.respond = respond or (lambda payload: BatchResult(results=[
            facts(p["ref"], rooms_count=float(p["ref"])) for p in json.loads(payload)["posts"]]))
        self.delay = delay
        self.payloads, self.in_flight, self.max_in_flight = [], 0, 0
        self.structured_kwargs = None

    def with_structured_output(self, schema, **kw):
        self.structured_kwargs = (schema, kw)
        return self

    async def ainvoke(self, payload):
        self.payloads.append(payload)
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        try:
            await asyncio.sleep(self.delay)
            r = self.respond(payload)
            if isinstance(r, BaseException):
                raise r
            return r
        finally:
            self.in_flight -= 1


class MemCache:
    def __init__(self):
        self.rows = {}

    async def get_fb_analysis(self, keys, prompt_version):
        return {k: v[2] for k, v in self.rows.items() if k in keys and v[1] == prompt_version}

    async def put_fb_analysis(self, rows):
        for key, model, version, js in rows:
            self.rows[key] = (model, version, js)


def analyzer(model, **kw):
    return ListingAnalyzer(model, model_name="fake", **kw)


# --- happy path / batching ---------------------------------------------------

async def test_strict_json_schema_structured_output_is_requested():
    m = FakeModel()
    analyzer(m)
    schema, kw = m.structured_kwargs
    assert schema is BatchResult and kw == {"method": "json_schema", "strict": True}


async def test_chunks_of_batch_size_and_refs_map_back_to_keys():
    m = FakeModel()
    out = await analyzer(m, batch_size=15).analyze(inputs(32))
    assert [len(json.loads(p)["posts"]) for p in m.payloads] == [15, 15, 2]
    assert len(out) == 32
    assert out["k16"].rooms_count == 1.0          # 2nd batch, ref 1


async def test_concurrency_is_capped():
    m = FakeModel(delay=0.02)
    await analyzer(m, batch_size=1, concurrency=3).analyze(inputs(9))
    assert m.max_in_flight == 3


async def test_duplicates_and_empty_texts_are_not_sent():
    m = FakeModel()
    posts = inputs(2) + [PostInput(key="k0", text="again"), PostInput(key="blank", text="  ")]
    out = await analyzer(m).analyze(posts)
    assert set(out) == {"k0", "k1"}
    assert len(json.loads(m.payloads[0])["posts"]) == 2


async def test_unknown_duplicate_and_missing_refs():
    m = FakeModel(lambda _: BatchResult(results=[facts(0), facts(0, rooms_count=9.0), facts(7)]))
    a = analyzer(m)
    out = await a.analyze(inputs(3))
    assert set(out) == {"k0"} and out["k0"].rooms_count is None    # first copy wins
    assert a.state.analyzed == 1 and a.state.failed == 2


async def test_non_home_kinds_keep_only_the_kind():
    m = FakeModel(lambda _: BatchResult(results=[facts(0, "not_listing", price_ils=900),
                                                 facts(1, "seeking", price_ils=3000)]))
    out = await analyzer(m).analyze(inputs(2))
    assert out["k0"].post_kind is PostKind.NOT_LISTING and out["k0"].price_ils is None
    assert out["k1"].price_ils is None


# --- cache -------------------------------------------------------------------

async def test_cache_hit_skips_the_model_and_version_mismatch_reanalyzes():
    cache = MemCache()
    m = FakeModel()
    await analyzer(m, cache=cache).analyze(inputs(2))
    assert len(m.payloads) == 1 and cache.rows["k0"][1] == PROMPT_VERSION
    await analyzer(m, cache=cache).analyze(inputs(2))
    assert len(m.payloads) == 1                       # all cached
    cache.rows["k0"] = ("fake", "old-version", cache.rows["k0"][2])
    await analyzer(m, cache=cache).analyze(inputs(2))
    assert len(json.loads(m.payloads[-1])["posts"]) == 1   # only k0 again


async def test_database_cache_round_trip(db):
    a = analyzer(FakeModel(), cache=db)
    await a.analyze(inputs(2))
    rows = await db.get_fb_analysis(["k0", "k1", "nope"], PROMPT_VERSION)
    assert set(rows) == {"k0", "k1"}
    assert await db.get_fb_analysis(["k0"], "other") == {}
    m2 = FakeModel()
    out = await analyzer(m2, cache=db).analyze(inputs(2))
    assert m2.payloads == [] and out["k1"].rooms_count == 1.0


# --- failures ----------------------------------------------------------------

async def test_failure_retries_once_then_posts_go_without_facts():
    calls = []
    m = FakeModel(lambda p: calls.append(1) or RuntimeError("boom"))
    a = analyzer(m)
    assert await a.analyze(inputs(3)) == {}
    assert len(calls) == 2 and a.state.failed == 3 and "boom" in a.state.last_error


async def test_transient_failure_then_success():
    seq = [RuntimeError("blip")]
    m = FakeModel(lambda p: seq.pop() if seq else BatchResult(results=[facts(0)]))
    assert set(await analyzer(m).analyze(inputs(1))) == {"k0"}


async def test_timeout_counts_as_failure():
    a = analyzer(FakeModel(delay=0.2), timeout=0.01)
    assert await a.analyze(inputs(1)) == {}
    assert "TimeoutError" in a.state.last_error


class _RateLimit(Exception):
    def __init__(self, body):
        super().__init__("429")
        self.body = body


async def test_usage_limit_pauses_until_reset_and_makes_no_calls():
    reset = datetime(2026, 1, 1, 13, 0, tzinfo=timezone.utc)
    now = [reset - timedelta(hours=2)]
    m = FakeModel(lambda p: _RateLimit({"type": "usage_limit_reached", "resets_at": reset.timestamp()}))
    a = analyzer(m, clock=lambda: now[0])
    assert await a.analyze(inputs(2)) == {}
    assert a.paused and a.state.paused_until == reset and len(m.payloads) == 1   # no retry
    await a.analyze(inputs(2, prefix="x"))
    assert len(m.payloads) == 1                                                 # paused: no call
    now[0] = reset + timedelta(seconds=1)
    m.respond = FakeModel().respond
    assert len(await a.analyze(inputs(2, prefix="y"))) == 2 and not a.paused


def test_usage_limit_mapping():
    assert usage_limit_from(_RateLimit({"type": "rate_limit"})) is None
    assert usage_limit_from(ValueError()) is None
    u = usage_limit_from(_RateLimit({"type": "usage_limit_reached", "resets_in_seconds": 60}))
    assert isinstance(u, UsageLimited) and u.resets_at > datetime.now(timezone.utc)


# --- prompt / payload --------------------------------------------------------

def test_payload_is_compact_hebrew_json_in_israel_time():
    item = post_item(0, "דירה בפלורנטין", posted_at=datetime(2026, 11, 24, 8, 30, tzinfo=timezone.utc),
                     group_name="קבוצה", photo_count=3)
    payload = build_batch_payload([item])
    assert "דירה בפלורנטין" in payload and ", " not in payload
    assert json.loads(payload) == {"posts": [{"ref": 0, "posted_at": "2026-11-24T10:30+02:00",
                                              "group_name": "קבוצה", "photo_count": 3,
                                              "text": "דירה בפלורנטין"}]}


def test_prompt_mentions_every_field_and_kind():
    for f in ListingFacts.model_fields:
        if f != "ref":
            assert f in SYSTEM_PROMPT, f
    for k in PostKind:
        assert k.value in SYSTEM_PROMPT, k


def test_strict_schema_requires_every_key():
    schema = BatchResult.model_json_schema()
    item = schema["$defs"]["ListingFacts"]
    assert set(item["required"]) == set(ListingFacts.model_fields)
    assert item["additionalProperties"] is False


def test_importing_listing_ai_does_not_import_langchain():
    import subprocess
    import sys
    code = "import sys, aptbot.listing_ai, aptbot.fb_engine; print('langchain_openai' in sys.modules)"
    from pathlib import Path
    root = Path(__file__).resolve().parents[2]
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True,
                         cwd=root, env={"PYTHONPATH": str(root), "OUTPUT_DIR": "/tmp"})
    assert out.stdout.strip() == "False"


@pytest.mark.parametrize("bad", ["{}", "not json"])
async def test_corrupt_cache_rows_are_reanalyzed(bad):
    cache = MemCache()
    cache.rows["k0"] = ("fake", PROMPT_VERSION, bad)
    m = FakeModel()
    out = await analyzer(m, cache=cache).analyze(inputs(1))
    assert "k0" in out and len(m.payloads) == 1


async def test_repeated_address_parts_are_collapsed():
    m = FakeModel(lambda _: BatchResult(results=[facts(0, city_area_or_address="פלורנטין, פלורנטין"),
                                                 facts(1, city_area_or_address="בן יהודה 1, הצפון הישן")]))
    out = await analyzer(m).analyze(inputs(2))
    assert out["k0"].city_area_or_address == "פלורנטין"
    assert out["k1"].city_area_or_address == "בן יהודה 1, הצפון הישן"
