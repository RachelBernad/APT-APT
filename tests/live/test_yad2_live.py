"""LIVE Yad2 gateway smoke tests. Run with `-m live`.

INVARIANT #2: the feed needs the 4-char zero-padded city id. city=0070 returns
markers; city=70 (leading zero dropped) returns none.
"""
import pytest

import yad2_gateway as gw
from models import GatewayFilters, LocationSignature

pytestmark = pytest.mark.live

# Ashdod: regionId 2, cityId "0070".
ASHDOD_REGION = 2
ASHDOD_CITY = "0070"


async def _markers(http, city_id):
    sig = LocationSignature(region_id=ASHDOD_REGION, area_id=None, city_id=city_id)
    params = gw._build_params(sig, GatewayFilters())
    return await gw._get_markers(http, params)


async def test_padded_city_id_returns_markers(http):
    markers = await _markers(http, ASHDOD_CITY)
    if not markers:
        pytest.skip("Yad2 returned nothing for 0070 (network / bot-challenge) — smoke skip")
    assert len(markers) > 0
    # markers carry the shape normalize_marker expects
    m = markers[0]
    assert "token" in m


async def test_unpadded_city_id_returns_nothing(http):
    # This is the whole point of INVARIANT #1/#2: "70" is a different (non-existent)
    # city to the feed. First confirm the padded id works, else skip (can't tell a
    # real 0-result from a network blip).
    padded = await _markers(http, ASHDOD_CITY)
    if not padded:
        pytest.skip("padded 0070 returned nothing (network / challenge) — can't compare")
    unpadded = await _markers(http, "70")
    assert len(unpadded) == 0, "city=70 must not resolve — leading zero is significant"


async def test_fetch_map_end_to_end(http):
    sig = LocationSignature(region_id=ASHDOD_REGION, area_id=None, city_id=ASHDOD_CITY)
    listings = await gw.fetch_map(http, sig, GatewayFilters(max_price=6000))
    if not listings:
        pytest.skip("fetch_map returned nothing (network / challenge) — smoke skip")
    normalized = gw.normalize_marker(listings[0], GatewayFilters())
    assert normalized["uid"].startswith("yad2:")
    assert normalized["city"]
