"""GeoJSON geometry helpers: centroid of a WGS84 geometry as (lat, lng).

At city scale a planar centroid on lng/lat is accurate to well under a metre, so no
projection library is needed (the server reprojects to 4326 for us, via ``outSR``).
"""
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

Coord = Sequence[float]


def _ring_area_centroid(ring: Sequence[Coord]) -> Tuple[float, float, float]:
    """Signed area and area-weighted centroid (x, y) of one closed ring (shoelace)."""
    a = cx = cy = 0.0
    n = len(ring)
    for i in range(n - 1 if n and ring[0] == ring[-1] else n):
        x0, y0 = ring[i][0], ring[i][1]
        x1, y1 = ring[(i + 1) % n][0], ring[(i + 1) % n][1]
        cross = x0 * y1 - x1 * y0
        a += cross
        cx += (x0 + x1) * cross
        cy += (y0 + y1) * cross
    a *= 0.5
    if a == 0:
        return 0.0, 0.0, 0.0
    return a, cx / (6 * a), cy / (6 * a)


def _mean(points: Iterable[Coord]) -> Optional[Tuple[float, float]]:
    pts = list(points)
    if not pts:
        return None
    return sum(p[0] for p in pts) / len(pts), sum(p[1] for p in pts) / len(pts)


def _polygons_centroid(polygons: List[List[Sequence[Coord]]]) -> Optional[Tuple[float, float]]:
    total = sx = sy = 0.0
    for rings in polygons:
        for ring in rings:  # holes carry the opposite sign under GeoJSON winding
            a, x, y = _ring_area_centroid(ring)
            total += a
            sx += x * a
            sy += y * a
    if total == 0:
        return _mean(p for rings in polygons for ring in rings for p in ring)
    return sx / total, sy / total


def centroid(geometry: Optional[Dict[str, Any]]) -> Optional[Tuple[float, float]]:
    """Centroid of a GeoJSON geometry, returned as (lat, lng); None if empty."""
    if not geometry:
        return None
    gtype = geometry.get("type")
    coords = geometry.get("coordinates")
    if not coords:
        return None
    xy: Optional[Tuple[float, float]]
    if gtype == "Point":
        xy = (coords[0], coords[1])
    elif gtype in ("MultiPoint", "LineString"):
        xy = _mean(coords)
    elif gtype == "MultiLineString":
        xy = _mean(p for line in coords for p in line)
    elif gtype == "Polygon":
        xy = _polygons_centroid([coords])
    elif gtype == "MultiPolygon":
        xy = _polygons_centroid(coords)
    else:
        return None
    if xy is None:
        return None
    return xy[1], xy[0]
