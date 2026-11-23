"""aptbot.scrapers.tel_aviv_gis.geometry: centroid of WGS84 GeoJSON geometries, as (lat, lng)."""
import pytest

from aptbot.scrapers.tel_aviv_gis.geometry import centroid


def _square(x0, y0, size, ccw=True):
    ring = [[x0, y0], [x0 + size, y0], [x0 + size, y0 + size], [x0, y0 + size], [x0, y0]]
    return ring if ccw else ring[::-1]


def test_point_returns_lat_lng_order():
    assert centroid({"type": "Point", "coordinates": [34.77, 32.07]}) == (32.07, 34.77)


@pytest.mark.parametrize("ccw", [True, False])
def test_square_polygon_either_winding(ccw):
    lat, lng = centroid({"type": "Polygon", "coordinates": [_square(34.0, 32.0, 2.0, ccw)]})
    assert lat == pytest.approx(33.0) and lng == pytest.approx(35.0)


def test_polygon_hole_shifts_centroid_away():
    outer = _square(0.0, 0.0, 4.0, ccw=True)
    hole = _square(2.0, 0.0, 2.0, ccw=False)[:]  # hole in the right-bottom quarter, opposite winding
    lat, lng = centroid({"type": "Polygon", "coordinates": [outer, hole]})
    # area 16 - 4 = 12; centroid = (16*(2,2) - 4*(3,1)) / 12
    assert lng == pytest.approx((32 - 12) / 12)
    assert lat == pytest.approx((32 - 4) / 12)


def test_multipolygon_is_area_weighted():
    big = [_square(0.0, 0.0, 2.0)]    # area 4, centroid (1,1)
    small = [_square(10.0, 0.0, 1.0)]  # area 1, centroid (10.5,0.5)
    lat, lng = centroid({"type": "MultiPolygon", "coordinates": [big, small]})
    assert lng == pytest.approx((4 * 1 + 1 * 10.5) / 5)
    assert lat == pytest.approx((4 * 1 + 1 * 0.5) / 5)


def test_lines_use_vertex_mean():
    assert centroid({"type": "LineString", "coordinates": [[0, 0], [2, 4]]}) == (2.0, 1.0)
    assert centroid({"type": "MultiLineString", "coordinates": [[[0, 0]], [[2, 4]]]}) == (2.0, 1.0)


def test_degenerate_polygon_falls_back_to_vertex_mean():
    flat = [[0, 0], [2, 0], [4, 0], [0, 0]]
    lat, lng = centroid({"type": "Polygon", "coordinates": [flat]})
    assert lat == pytest.approx(0.0) and lng == pytest.approx(1.5)


@pytest.mark.parametrize("geom", [None, {}, {"type": "Polygon", "coordinates": []},
                                  {"type": "GeometryCollection", "geometries": []}])
def test_empty_or_unsupported_is_none(geom):
    assert centroid(geom) is None
