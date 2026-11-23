"""rentlyfly.py: forced gzip (not brotli) header, normalization, paged fetch.

INVARIANT #3: rentlyfly MUST request `Accept-Encoding: gzip, deflate` — the
container's aiohttp can't decode Brotli, so `br` yields 0 listings. Normalized
uids are prefixed `rentlyfly:`.
"""
import rentlyfly
from models import GatewayFilters


# --- the gzip invariant ---------------------------------------------------

def test_headers_force_gzip_not_brotli():
    enc = rentlyfly._HEADERS["Accept-Encoding"]
    assert enc == "gzip, deflate"
    assert "br" not in {p.strip() for p in enc.split(",")}


# --- param building -------------------------------------------------------

def test_build_params_maps_filters():
    f = GatewayFilters(min_price=4000, max_price=9000, min_rooms=2, max_rooms=4,
                       min_floor=1, max_floor=10)
    params = rentlyfly._build_params(page=2, f=f)
    assert params["page"] == 2
    assert params["minPrice"] == 4000 and params["maxPrice"] == 9000
    assert params["minRooms"] == 2 and params["maxRooms"] == 4
    assert params["minFloor"] == 1 and params["maxFloor"] == 10


# --- normalize ------------------------------------------------------------

def _raw():
    return {
        "id": 55123,
        "price": "₪6,500",
        "roomsAvailable": 3,
        "location": {"city": "תל אביב יפו", "hood": "פלורנטין", "street": "פלורנטין 10",
                     "latitude": 32.05, "longitude": 34.77},
        "description": "דירה מהממת בקומה 4",
        "isElevator": True, "isBalcony": False, "isMamad": True,
        "isAirConditioner": True, "isParking": False,
        "photos": ["x.jpg"], "url": "https://facebook.com/post/1",
    }


def test_normalize_uid_prefix_and_price():
    out = rentlyfly.normalize(_raw())
    assert out["uid"] == "rentlyfly:55123"
    assert out["source"] == "rentlyfly"
    assert out["price"] == 6500                 # parsed out of "₪6,500"
    assert out["order_id"] is None              # already newest-first


def test_normalize_inline_features_and_mamad():
    out = rentlyfly.normalize(_raw())
    assert "elevator" in out["features"] and "ac" in out["features"] and "mamad" in out["features"]
    assert "balcony" not in out["features"] and "parking" not in out["features"]
    assert out["is_mamad"] is True


def test_normalize_extracts_floor_from_description():
    out = rentlyfly.normalize(_raw())
    assert out["floor"] == 4                     # 'קומה 4'
    ground = dict(_raw(), description="דירה בקומת קרקע")
    assert rentlyfly.normalize(ground)["floor"] == 0


def test_normalize_string_location_is_tolerated():
    raw = dict(_raw(), location="תל אביב יפו")
    out = rentlyfly.normalize(raw)
    assert out["city"] == "תל אביב יפו"


# --- fetch_tel_aviv (monkeypatched pages) ---------------------------------

async def test_fetch_tel_aviv_paginates_until_no_more(monkeypatch):
    pages = {
        1: {"data": [dict(_raw(), id=1), dict(_raw(), id=2)],
            "pagination": {"hasMore": True}},
        2: {"data": [dict(_raw(), id=3)], "pagination": {"hasMore": False}},
    }

    async def fake_fetch_page(http, page, f):
        return pages.get(page, {})

    monkeypatch.setattr(rentlyfly, "_fetch_page", fake_fetch_page)
    out = await rentlyfly.fetch_tel_aviv(None, GatewayFilters())
    assert [o["uid"] for o in out] == ["rentlyfly:1", "rentlyfly:2", "rentlyfly:3"]


async def test_fetch_tel_aviv_stops_on_empty_page(monkeypatch):
    async def fake_fetch_page(http, page, f):
        return {"data": []} if page == 1 else {"data": [_raw()]}

    monkeypatch.setattr(rentlyfly, "_fetch_page", fake_fetch_page)
    out = await rentlyfly.fetch_tel_aviv(None, GatewayFilters())
    assert out == []


async def test_fetch_tel_aviv_swallows_page_errors(monkeypatch):
    async def boom(http, page, f):
        raise RuntimeError("network down")

    monkeypatch.setattr(rentlyfly, "_fetch_page", boom)
    out = await rentlyfly.fetch_tel_aviv(None, GatewayFilters())
    assert out == []   # returns what it had (nothing) rather than raising
