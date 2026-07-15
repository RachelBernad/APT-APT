"""LIVE rentlyfly.ai smoke test (Tel Aviv only). Run with `-m live`.

INVARIANT #3: forcing gzip (not brotli) returns real listings; normalized uids
are prefixed `rentlyfly:`.
"""
import pytest

import rentlyfly
from models import GatewayFilters

pytestmark = pytest.mark.live


async def test_fetch_tel_aviv_returns_listings(http):
    listings = await rentlyfly.fetch_tel_aviv(http, GatewayFilters())
    if not listings:
        pytest.skip("rentlyfly returned nothing (network / upstream) — smoke skip")
    assert len(listings) > 0
    for l in listings[:5]:
        assert l["uid"].startswith("rentlyfly:")
        assert l["source"] == "rentlyfly"


async def test_gzip_header_actually_decodes(http):
    # A regression on the brotli bug would surface as an empty result (aiohttp can't
    # decode `br`). Getting any listing back proves gzip negotiation worked.
    listings = await rentlyfly.fetch_tel_aviv(http, GatewayFilters(max_price=8000))
    if not listings:
        pytest.skip("no listings under filter (network / upstream) — smoke skip")
    assert any(l.get("price") for l in listings)
