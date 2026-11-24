"""LIVE Tel Aviv GIS smoke tests. Run with `-m live`.

Verifies against the real MapServer: no auth needed, server-side point+radius in WGS84,
typed mapping, and that the supported layers haven't drifted (catalog check).
"""
import math

import aiohttp
import pytest

from aptbot.scrapers.tel_aviv_gis import (LAYERS, PLANS_SITE_SCOPED_WHERE, Kind, Stage,
                                          TelAvivGisClient, TelAvivGisError,
                                          TelAvivGisLayerUnavailable, TelAvivGisQueryError)
from aptbot.scrapers.tel_aviv_gis.catalog import EXIT_UNAVAILABLE
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
    """Skip on network trouble or a server-side layer outage; a rejected query still fails."""
    try:
        return await gis.query(*args, **kw)
    except TelAvivGisQueryError:
        raise  # our query is wrong (or a field was renamed): a real failure
    except TelAvivGisLayerUnavailable as exc:
        pytest.skip(f"layer outage on the city's side ({exc}): smoke skip")
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


@pytest.mark.parametrize("key", list(LAYERS))
async def test_every_supported_layer_answers(http, key):
    async with TelAvivGisClient(http) as gis:
        await _query_or_skip(gis, key, geometry=CENTER, distance_m=50, return_geometry=False)


async def test_bad_field_is_a_query_error_not_an_outage(http):
    # Same error body as an outage; the 1=1 probe must tell them apart.
    async with TelAvivGisClient(http) as gis:
        with pytest.raises(TelAvivGisQueryError):
            try:
                await gis.query("permits", where="no_such_field=1", return_geometry=False)
            except aiohttp.ClientError as exc:
                pytest.skip(f"Tel Aviv GIS unreachable ({exc}): smoke skip")


async def test_zero_radius_means_contains(http):
    # Raw distance=0 returns nothing on this server; distance_m=0 must behave like no radius.
    # (Centroids of L-shaped lots can fall outside their own polygon, so sample several.)
    async with TelAvivGisClient(http) as gis:
        near = await _query_or_skip(gis, "permits", geometry=CENTER, distance_m=RADIUS_M)
        hits = 0
        for f in near[:6]:
            zero = await _query_or_skip(gis, "permits", geometry=f.centroid, distance_m=0,
                                        return_geometry=False)
            bare = await _query_or_skip(gis, "permits", geometry=f.centroid,
                                        return_geometry=False)
            assert {x.object_id for x in zero} == {x.object_id for x in bare}
            hits += bool(zero)
    assert hits, "no sampled centroid fell inside any permit polygon"


async def test_catalog_reports_no_drift(capsys):
    try:
        code = await catalog_main([])
    except aiohttp.ClientError as exc:
        pytest.skip(f"Tel Aviv GIS unreachable ({exc}): smoke skip")
    out = capsys.readouterr().out
    if code == EXIT_UNAVAILABLE:
        pytest.skip(f"no drift, but a layer outage on the city's side:\n{out}")
    assert code == 0, out
