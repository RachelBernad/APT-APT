"""aptbot.scrapers.tel_aviv_gis.client: request params, paging, errors-as-200, retries (offline).

INVARIANTS (api_docs/operations/query.md, transport/http-and-auth.md):
- the point goes on the wire as ``lng,lat`` with inSR/outSR 4326;
- paging uses resultOffset + orderByFields=<oid> and follows exceededTransferLimit;
- an ``{"error": ...}`` body (sent with HTTP 200) raises, never returns empty.
"""
from datetime import datetime, timezone
from types import SimpleNamespace

import aiohttp
import pytest

from aptbot.scrapers.tel_aviv_gis.client import TelAvivGisClient, TelAvivGisError
from aptbot.scrapers.tel_aviv_gis.models import Kind, Severity, Stage

NOW = datetime(2026, 11, 23, 12, tzinfo=timezone.utc)

SQUARE = {"type": "Polygon", "coordinates": [[[34.0, 32.0], [34.002, 32.0], [34.002, 32.002],
                                              [34.0, 32.002], [34.0, 32.0]]]}


def _feature(oid, stage="בבניה"):
    return {"type": "Feature", "id": oid, "geometry": SQUARE,
            "properties": {"oid_permit": oid, "building_stage": stage,
                           "sug_bakasha": "בניה חדשה", "addresses": f"רחוב {oid}"}}


class _Resp:
    def __init__(self, status, body):
        self.status, self._body = status, body
        # aiohttp's ClientResponseError.__str__ reads request_info.real_url
        self.request_info, self.history = SimpleNamespace(real_url="https://fake/query"), ()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def raise_for_status(self):
        if self.status >= 400:
            raise aiohttp.ClientResponseError(self.request_info, (), status=self.status)

    async def json(self, content_type=None):
        return self._body


class FakeSession:
    """Replays queued (status, body) responses and records every request's params."""

    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, dict(params or {})))
        status, body = self.responses.pop(0)
        return _Resp(status, body)

    async def close(self):
        pass


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    async def instant(_):
        return None
    monkeypatch.setattr("aptbot.scrapers.tel_aviv_gis.client.asyncio.sleep", instant)


async def _query(session, *args, page_size=2000, **kw):
    async with TelAvivGisClient(session, page_size=page_size, retries=2) as gis:
        return await gis.query(*args, now=NOW, **kw)


async def test_point_radius_params_are_lng_lat_wgs84():
    s = FakeSession((200, {"type": "FeatureCollection", "features": [_feature(1)]}))
    feats = await _query(s, "permits", geometry=(32.0754, 34.7748), distance_m=200,
                         where="building_stage='בבניה'")
    url, p = s.calls[0]
    assert url.endswith("/MapServer/772/query")
    assert p["geometry"] == "34.7748,32.0754"
    assert p["inSR"] == 4326 and p["outSR"] == 4326 and p["f"] == "geojson"
    assert p["distance"] == 200 and p["units"] == "esriSRUnit_Meter"
    assert p["spatialRel"] == "esriSpatialRelIntersects"
    assert p["orderByFields"] == "oid_permit" and p["resultOffset"] == 0
    assert p["where"] == "building_stage='בבניה'"
    [f] = feats
    assert (f.layer, f.layer_id, f.object_id) == ("permits", 772, 1)
    assert f.kind is Kind.BUILDING and f.stage is Stage.ACTIVE and f.severity is Severity.HIGH
    assert f.centroid == pytest.approx((32.001, 34.001))
    assert f.raw_attributes["addresses"] == "רחוב 1"


async def test_no_geometry_means_no_spatial_params():
    s = FakeSession((200, {"features": []}))
    assert await _query(s, 499) == []
    p = s.calls[0][1]
    assert "geometry" not in p and "distance" not in p and p["orderByFields"] == "oid_site"


async def test_out_fields_always_include_mapper_and_oid_fields():
    s = FakeSession((200, {"features": []}))
    await _query(s, "permits", out_fields=["koteret"])
    fields = set(s.calls[0][1]["outFields"].split(","))
    assert {"koteret", "oid_permit", "building_stage", "sug_bakasha"} <= fields


async def test_distance_without_geometry_is_rejected():
    with pytest.raises(ValueError):
        await _query(FakeSession(), "permits", distance_m=100)


async def test_pages_until_transfer_limit_clears():
    s = FakeSession(
        (200, {"features": [_feature(1), _feature(2)], "exceededTransferLimit": True}),
        (200, {"features": [_feature(5), _feature(9)], "properties": {"exceededTransferLimit": True}}),
        (200, {"features": [_feature(11)]}),
    )
    feats = await _query(s, "permits", page_size=2)
    assert [f.object_id for f in feats] == [1, 2, 5, 9, 11]
    assert [c[1]["resultOffset"] for c in s.calls] == [0, 2, 4]
    assert all(c[1]["resultRecordCount"] == 2 for c in s.calls)


async def test_runaway_paging_raises():
    page = (200, {"features": [_feature(1)], "exceededTransferLimit": True})
    s = FakeSession(*[page] * 3)
    async with TelAvivGisClient(s, page_size=1, max_pages=3) as gis:
        with pytest.raises(TelAvivGisError, match="more than 3 pages"):
            await gis.query("permits", now=NOW)


async def test_error_body_with_http_200_raises():
    err = {"error": {"code": 400, "message": "Unable to complete operation.", "details": []}}
    s = FakeSession((200, err))
    with pytest.raises(TelAvivGisError, match="Unable to complete"):
        await _query(s, "permits")
    assert len(s.calls) == 1  # not retried


async def test_5xx_is_retried_then_succeeds():
    s = FakeSession((503, {}), (502, {}), (200, {"features": [_feature(3)]}))
    feats = await _query(s, "permits")
    assert [f.object_id for f in feats] == [3] and len(s.calls) == 3


async def test_5xx_exhausting_retries_raises():
    s = FakeSession((503, {}), (503, {}), (503, {}))
    with pytest.raises(TelAvivGisError, match="request failed"):
        await _query(s, "permits")


async def test_4xx_is_not_retried():
    s = FakeSession((404, {}))
    with pytest.raises(TelAvivGisError):
        await _query(s, "permits")
    assert len(s.calls) == 1


async def test_unsupported_layer_raises_before_any_request():
    s = FakeSession()
    with pytest.raises(KeyError):
        await _query(s, "roadworks")
    assert s.calls == []


async def test_v2_title_url_tags_flow_through():
    plan = {"type": "Feature", "id": 7, "geometry": SQUARE,
            "properties": {"oid_taba": 7, "taba": "4406   ", "shem_taba": "התחדשות ברמת אביב ב",
                           "t_status_klali": "בתכנון", "t_hekef": "מתחמית", "sug_nose": 'תב"ע',
                           "url_documents": "https://gisn.tel-aviv.gov.il/tabaot/docs.aspx?id_taba=1"}}
    s = FakeSession((200, {"features": [plan]}))
    [f] = await _query(s, "plans", geometry=(32.1, 34.8), distance_m=10)
    assert s.calls[0][0].endswith("/MapServer/528/query")
    assert s.calls[0][1]["orderByFields"] == "oid_taba"
    assert f.kind is Kind.PLAN and f.stage is Stage.PROPOSED and f.severity is Severity.HIGH
    assert f.title == "4406: התחדשות ברמת אביב ב" and f.url.startswith("https://gisn")
    assert f.tags == {"urban_renewal"} and f.address is None


async def test_v1_feature_gets_v2_defaults():
    s = FakeSession((200, {"features": [_feature(1)]}))
    [f] = await _query(s, "permits")
    assert f.tags == frozenset() and f.title == "בניה חדשה" and f.url is None
