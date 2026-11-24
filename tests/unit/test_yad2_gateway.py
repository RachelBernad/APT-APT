"""aptbot.scrapers.yad2.gateway: param building (string city id!), challenge detection,
marker normalization, and the 200-cap price-bisection recovery (offline).

INVARIANT #4: fetch_map bisects the price interval whenever a scope returns
>= the marker cap, so the ~200-marker hard cap can't silently drop listings.
"""
import pytest

from aptbot import config
from aptbot.scrapers.yad2 import gateway as gw
from aptbot.models import GatewayFilters, LocationSignature


# --- _build_params --------------------------------------------------------

def test_build_params_keeps_city_id_string():
    sig = LocationSignature(region_id=2, area_id=None, city_id="0070")
    params = gw._build_params(sig, GatewayFilters())
    # INVARIANT: the padded string id goes on the wire verbatim, never int()-ed.
    assert params["city"] == "0070"
    assert isinstance(params["city"], str)
    assert params["region"] == 2
    assert "area" not in params and "neighborhood" not in params


def test_build_params_filters_and_features():
    sig = LocationSignature(region_id=3, area_id=None, city_id="5000")
    f = GatewayFilters(min_price=4000, max_price=9000, min_rooms=2.5, max_rooms=4,
                       min_sqm=60, min_floor=1, property_ids=[1, 6],
                       features=["elevator", "mamad"])
    params = gw._build_params(sig, f)
    assert params["minPrice"] == 4000 and params["maxPrice"] == 9000
    assert params["minRooms"] == 2.5 and params["maxRooms"] == 4
    assert params["minSquaremeter"] == 60 and params["minFloor"] == 1
    assert params["property"] == "1,6"
    # feature keys map to their gateway param names, value 1
    assert params["elevator"] == 1 and params["shelter"] == 1


def test_build_params_price_override_beats_filter():
    sig = LocationSignature(region_id=3, area_id=None, city_id="5000")
    f = GatewayFilters(min_price=4000, max_price=9000)
    params = gw._build_params(sig, f, price_override=(5000, 6000))
    assert params["minPrice"] == 5000 and params["maxPrice"] == 6000


def test_build_params_area_scope_no_city():
    sig = LocationSignature(region_id=3, area_id=42, city_id=None)
    params = gw._build_params(sig, GatewayFilters())
    assert params["area"] == 42 and "city" not in params


# --- _is_challenge --------------------------------------------------------

def test_is_challenge_json_is_never_a_challenge():
    assert gw._is_challenge('{"data": {}}', "application/json") is False


@pytest.mark.parametrize("body", [
    "<!DOCTYPE html><html>...</html>",
    "please enable perfdrive to continue",
    "__uzdbm_ cookie challenge",
    "solve this captcha",
])
def test_is_challenge_detects_bot_walls(body):
    assert gw._is_challenge(body, "text/html") is True


# --- normalize_marker -----------------------------------------------------

def _marker():
    return {
        "token": "abc123",
        "orderId": 42,
        "price": 7500,
        "address": {
            "city": {"text": "תל אביב יפו"},
            "street": {"text": "דיזנגוף"},
            "neighborhood": {"text": "הצפון הישן"},
            "area": {"text": "תל אביב"},
            "coords": {"lat": 32.08, "lon": 34.77},
            "house": {"floor": 3},
        },
        "additionalDetails": {
            "roomsCount": 3, "squareMeter": 72,
            "property": {"text": "דירה"},
            "propertyCondition": {"id": 2},
        },
        "metaData": {"images": ["a.jpg"]},
    }


def test_normalize_marker_basic_shape():
    out = gw.normalize_marker(_marker(), GatewayFilters())
    assert out["uid"] == "yad2:abc123"
    assert out["source"] == "yad2" and out["type"] == "yad2"
    assert out["price"] == 7500 and isinstance(out["price"], int)
    assert out["rooms"] == "3" and out["size"] == "72" and out["floor"] == "3"
    assert out["city"] == "תל אביב יפו" and out["hood"] == "הצפון הישן"
    assert out["order_id"] == 42
    # property text -> id via the reverse map (loose spacing tolerated)
    assert out["property_type_id"] == 1
    assert out["property_condition"] == 2
    assert out["apartment_page_url"].endswith("/abc123")


def test_normalize_marker_features_come_from_scrape_filters():
    # Markers carry no feature flags; a Yad2 listing is known to have exactly the
    # features that were pushed server-side for its scrape group.
    f = GatewayFilters(features=["elevator", "mamad"])
    out = gw.normalize_marker(_marker(), f)
    assert set(out["features"]) == {"elevator", "mamad"}
    assert out["is_mamad"] is True          # mamad was in the group
    out2 = gw.normalize_marker(_marker(), GatewayFilters())
    assert out2["features"] == [] and out2["is_mamad"] is None


def test_normalize_marker_non_numeric_price_is_none():
    m = _marker()
    m["price"] = "לא צוין"
    assert gw.normalize_marker(m, GatewayFilters())["price"] is None


# --- fetch_map: 200-cap bisection (monkeypatched marker source) ------------

def _dataset(n=10, base=1000, step=100):
    return [{"token": f"t{i}", "price": base + i * step} for i in range(n)]


def _install_source(monkeypatch, dataset, cap):
    """Fake _get_markers: returns every dataset marker inside [minPrice,maxPrice],
    but truncated to `cap` (models the server's hard cap). Counts its own calls."""
    calls = {"n": 0}

    async def fake_get_markers(http, params):
        calls["n"] += 1
        lo, hi = params.get("minPrice"), params.get("maxPrice")
        ms = [m for m in dataset
              if (lo is None or m["price"] >= lo) and (hi is None or m["price"] <= hi)]
        ms.sort(key=lambda m: m["price"])
        return ms[:cap]

    monkeypatch.setattr(gw, "_get_markers", fake_get_markers)
    return calls


async def test_fetch_map_recovers_all_via_bisection(monkeypatch):
    dataset = _dataset(10)                      # prices 1000..1900
    calls = _install_source(monkeypatch, dataset, cap=3)
    monkeypatch.setattr(config, "MAP_MARKER_CAP", 3)
    monkeypatch.setattr(config, "MAP_MIN_PRICE_BAND", 10)
    monkeypatch.setattr(config, "MAP_MAX_REQUESTS", 500)

    sig = LocationSignature(region_id=3, area_id=None, city_id="5000")
    f = GatewayFilters(min_price=1000, max_price=1900)
    got = await gw.fetch_map(None, sig, f)

    assert {m["token"] for m in got} == {m["token"] for m in dataset}   # nothing dropped
    assert calls["n"] > 1                                               # it actually bisected


async def test_fetch_map_no_bisection_when_under_cap(monkeypatch):
    dataset = _dataset(2)
    calls = _install_source(monkeypatch, dataset, cap=3)
    monkeypatch.setattr(config, "MAP_MARKER_CAP", 3)
    monkeypatch.setattr(config, "MAP_MAX_REQUESTS", 500)

    sig = LocationSignature(region_id=3, area_id=None, city_id="5000")
    got = await gw.fetch_map(None, sig, GatewayFilters(min_price=1000, max_price=1900))
    assert len(got) == 2
    assert calls["n"] == 1        # a single request, no recursion


async def test_fetch_map_respects_request_cap(monkeypatch):
    dataset = _dataset(10)
    calls = _install_source(monkeypatch, dataset, cap=3)
    monkeypatch.setattr(config, "MAP_MARKER_CAP", 3)
    monkeypatch.setattr(config, "MAP_MIN_PRICE_BAND", 10)
    monkeypatch.setattr(config, "MAP_MAX_REQUESTS", 1)   # hard stop after 1 request

    sig = LocationSignature(region_id=3, area_id=None, city_id="5000")
    got = await gw.fetch_map(None, sig, GatewayFilters(min_price=1000, max_price=1900))
    assert calls["n"] == 1 and len(got) == 3   # truncated, no further requests


async def test_fetch_map_stops_when_band_too_narrow(monkeypatch):
    dataset = _dataset(10)
    calls = _install_source(monkeypatch, dataset, cap=3)
    monkeypatch.setattr(config, "MAP_MARKER_CAP", 3)
    monkeypatch.setattr(config, "MAP_MIN_PRICE_BAND", 10 ** 9)  # can never bisect
    monkeypatch.setattr(config, "MAP_MAX_REQUESTS", 500)

    sig = LocationSignature(region_id=3, area_id=None, city_id="5000")
    got = await gw.fetch_map(None, sig, GatewayFilters(min_price=1000, max_price=1900))
    assert calls["n"] == 1 and len(got) == 3   # gives up gracefully (logs a warning)


async def test_fetch_map_without_price_filter_reaches_beyond_the_sample(monkeypatch):
    """No min/max price (the Tel Aviv default): the capped sample only spans part of the
    price range, so the outer halves must stay open-ended or the rest is never asked for."""
    dataset = _dataset(10)                      # prices 1000..1900; the sample = 1000..1200
    _install_source(monkeypatch, dataset, cap=3)
    monkeypatch.setattr(config, "MAP_MARKER_CAP", 3)
    monkeypatch.setattr(config, "MAP_MIN_PRICE_BAND", 10)
    monkeypatch.setattr(config, "MAP_MAX_REQUESTS", 500)

    sig = LocationSignature(region_id=3, area_id=None, city_id="5000")
    got = await gw.fetch_map(None, sig, GatewayFilters())
    assert {m["token"] for m in got} == {m["token"] for m in dataset}


async def test_fetch_map_open_range_with_a_dense_price_point(monkeypatch):
    """Many listings at one price fill the cap: still explore above and below it."""
    dataset = ([{"token": f"low{i}", "price": 500 + i} for i in range(2)]
               + [{"token": f"d{i}", "price": 5000} for i in range(5)]
               + [{"token": f"high{i}", "price": 9000 + i} for i in range(2)])

    async def fake_get_markers(http, params):        # dense block first, like a capped server
        lo, hi = params.get("minPrice"), params.get("maxPrice")
        ms = [m for m in dataset
              if (lo is None or m["price"] >= lo) and (hi is None or m["price"] <= hi)]
        ms.sort(key=lambda m: (m["price"] != 5000, m["price"]))
        return ms[:3]

    monkeypatch.setattr(gw, "_get_markers", fake_get_markers)
    monkeypatch.setattr(config, "MAP_MARKER_CAP", 3)
    monkeypatch.setattr(config, "MAP_MIN_PRICE_BAND", 10)
    monkeypatch.setattr(config, "MAP_MAX_REQUESTS", 500)

    got = await gw.fetch_map(None, LocationSignature(region_id=3, area_id=None, city_id="5000"),
                             GatewayFilters())
    tokens = {m["token"] for m in got}
    assert {"low0", "low1", "high0", "high1"} <= tokens      # only the dense block is truncated
