"""LIVE Yad2 autocomplete smoke test. Run with `-m live`.

Confirms the real autocomplete resolves a well-known city and keeps its id as a
zero-padded / string value (INVARIANT #1).
"""
import pytest

import locations

pytestmark = pytest.mark.live


async def test_resolve_tel_aviv(http):
    import aiohttp  # noqa: F401  (http fixture is a real session)
    cands = await locations.resolve_candidates(http, "tel aviv", limit=8)
    if not cands:
        pytest.skip("autocomplete returned nothing (network / bot-challenge) — smoke skip")
    cities = [c for c in cands if c.level == "city"]
    assert cities, "expected at least one city candidate for 'tel aviv'"
    for c in cities:
        assert isinstance(c.city_id, str)   # never int()-ed
    assert any(c.city_id == "5000" for c in cities), "Tel Aviv-Yafo (5000) expected"


async def test_fetch_city_hoods_live(http):
    # Ashdod (region 2, city 0070) — the padded id must return real neighborhoods.
    hoods = await locations.fetch_city_hoods(http, region_id=2, city_id="0070")
    if not hoods:
        pytest.skip("no hoods returned for 0070 (network / challenge) — smoke skip")
    assert len(hoods) > 0 and all(isinstance(h, str) for h in hoods)
