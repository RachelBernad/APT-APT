"""LIVE Tel Aviv GIS smoke tests. Run with `-m live`.

Verifies against the real MapServer: no auth needed, server-side point+radius in WGS84,
typed mapping, and that the supported layers haven't drifted (catalog check).
"""
import math

import aiohttp
import pytest

from aptbot.scrapers.tel_aviv_gis import (LAYERS, PLANS_SITE_SCOPED_WHERE, Kind, Stage,
                                          TelAvivGisClient, TelAvivGisError)
from aptbot.scrapers.tel_aviv_gis.catalog import main as catalog_main

pytestmark = pytest.mark.live

# King George / Bograshov, central Tel Aviv: dense with permits every time we looked.
CENTER = (32.0754, 34.7748)
RADIUS_M = 200


def _meters(a, b):
    dy = (a[0] - b[0]) * 111_320
    dx = (a[1] - b[1]) * 111_320 * math.cos(math.radians(a[0]))
    return math.hypot(dx, dy)


async def _query_or_skip(gis, *args, **kw):
    try:
        return await gis.query(*args, **kw)
    except (aiohttp.ClientError, TelAvivGisError) as exc:
        pytest.skip(f"Tel Aviv GIS unreachable ({exc}): smoke skip")


async def test_permits_near_point_are_typed_and_nearby(http):
    async with TelAvivGisClient(http) as gis:
        feats = await _query_or_skip(gis, "permits", geometry=CENTER, distance_m=RADIUS_M)
    assert feats, "central Tel Aviv should always have permit records within 200 m"
    assert all(f.layer_id == 772 and f.centroid for f in feats)
    assert all(f.stage is not Stage.UNKNOWN for f in feats), "new building_stage value: update layers.py"
    # Polygons intersect the radius; their centroids can't be far outside it.
    assert max(_meters(CENTER, f.centroid) for f in feats) < RADIUS_M + 150


async def test_site_scoped_plans_near_point_are_typed(http):
    async with TelAvivGisClient(http) as gis:
        feats = await _query_or_skip(gis, "plans", geometry=CENTER, distance_m=300,
                                     where=PLANS_SITE_SCOPED_WHERE, return_geometry=False)
    assert feats, "central Tel Aviv should always have site-scoped town plans within 300 m"
    assert all(f.kind is Kind.PLAN for f in feats)
    assert all(f.stage is not Stage.UNKNOWN for f in feats), "new t_status_klali value: update layers.py"
    assert all(f.title and f.url for f in feats)


async def test_every_supported_layer_answers(http):
    async with TelAvivGisClient(http) as gis:
        for key in LAYERS:
            await _query_or_skip(gis, key, geometry=CENTER, distance_m=50, return_geometry=False)


async def test_catalog_reports_no_drift(capsys):
    try:
        code = await catalog_main([])
    except aiohttp.ClientError as exc:
        pytest.skip(f"Tel Aviv GIS unreachable ({exc}): smoke skip")
    assert code == 0, capsys.readouterr().out
