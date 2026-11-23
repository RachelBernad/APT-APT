"""Async client for the Tel Aviv-Yafo GIS MapServer (``WM/IView2WM``).

Plain HTTP, no auth (see api_docs/transport/http-and-auth.md). One public operation,
``query()``: a typed wrapper over the layer ``query`` endpoint that works in WGS84,
pushes the point+radius filter server-side, pages past the 2000-row cap and returns
``ConstructionFeature`` records.

    async with TelAvivGisClient() as gis:
        near = await gis.query("permits", geometry=(32.0754, 34.7748), distance_m=200,
                               where="building_stage IN ('בבניה','קיים היתר')")

or one-shot: ``await query("sites", geometry=(lat, lng), distance_m=150)``.
"""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import aiohttp

from aptbot import config
from aptbot.scrapers.tel_aviv_gis.geometry import centroid
from aptbot.scrapers.tel_aviv_gis.layers import LayerSpec, get_layer
from aptbot.scrapers.tel_aviv_gis.models import ConstructionFeature

logger = logging.getLogger(__name__)

LatLng = Tuple[float, float]


class TelAvivGisError(RuntimeError):
    """The server answered with an ArcGIS error body (often with HTTP 200), or paging ran away."""


class TelAvivGisClient:
    def __init__(self, session: Optional[aiohttp.ClientSession] = None, *,
                 base_url: str = config.TLV_GIS_MAPSERVER,
                 concurrency: int = config.TLV_GIS_CONCURRENCY,
                 page_size: int = config.TLV_GIS_PAGE_SIZE,
                 retries: int = config.TLV_GIS_RETRIES,
                 timeout_s: int = config.TLV_GIS_TIMEOUT,
                 max_pages: int = config.TLV_GIS_MAX_PAGES) -> None:
        self._session = session
        self._owns_session = session is None
        self._base = base_url.rstrip("/")
        self._sem = asyncio.Semaphore(max(1, concurrency))
        self._page_size = max(1, min(page_size, 2000))
        self._retries = max(0, retries)
        self._timeout = aiohttp.ClientTimeout(total=timeout_s)
        self._max_pages = max(1, max_pages)

    async def __aenter__(self) -> "TelAvivGisClient":
        if self._session is None:
            self._session = aiohttp.ClientSession(
                headers={"User-Agent": config.USER_AGENT, "Accept": "application/json"})
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    async def close(self) -> None:
        if self._owns_session and self._session is not None:
            await self._session.close()
            self._session = None

    # --- public ----------------------------------------------------------------

    async def query(self, layer: Union[str, int], *,
                    where: str = "1=1",
                    geometry: Optional[LatLng] = None,
                    distance_m: Optional[float] = None,
                    out_fields: Optional[Sequence[str]] = None,
                    return_geometry: bool = True,
                    now: Optional[datetime] = None) -> List[ConstructionFeature]:
        """Query one supported layer and return typed features (all pages).

        ``geometry`` is a WGS84 ``(lat, lng)`` point; with ``distance_m`` the server
        returns every feature intersecting that radius. ``where`` is passed through as
        ArcGIS SQL over the layer's raw field names (see api_docs layer pages).
        ``now`` fixes the reference time for date-derived stages (default: current time).
        """
        spec = get_layer(layer)
        if distance_m is not None and geometry is None:
            raise ValueError("distance_m requires geometry")
        params = self._params(spec, where, geometry, distance_m, out_fields, return_geometry)
        ref = now or datetime.now(timezone.utc)

        features: List[ConstructionFeature] = []
        for page in range(self._max_pages):
            body = await self._get_json(f"{self._base}/{spec.layer_id}/query",
                                        {**params, "resultOffset": page * self._page_size})
            raw = body.get("features") or []
            features.extend(self._to_feature(spec, f, ref) for f in raw)
            more = body.get("exceededTransferLimit") or (body.get("properties") or {}).get(
                "exceededTransferLimit")
            if not more or not raw:
                return features
        raise TelAvivGisError(
            f"layer {spec.layer_id}: more than {self._max_pages} pages of {self._page_size}; "
            f"narrow the query or raise TLV_GIS_MAX_PAGES")

    # --- internals --------------------------------------------------------------

    def _params(self, spec: LayerSpec, where: str, geometry: Optional[LatLng],
                distance_m: Optional[float], out_fields: Optional[Sequence[str]],
                return_geometry: bool) -> Dict[str, Any]:
        fields = "*"
        if out_fields:
            # The mapper and paging need these regardless of what the caller asked for.
            fields = ",".join(sorted(set(out_fields) | spec.expected_fields | {spec.oid_field}))
        p: Dict[str, Any] = {
            "where": where or "1=1",
            "outFields": fields,
            "returnGeometry": "true" if return_geometry else "false",
            "outSR": 4326,
            "orderByFields": spec.oid_field,  # stable paging (OIDs are not contiguous)
            "resultRecordCount": self._page_size,
            "f": "geojson",
        }
        if geometry is not None:
            lat, lng = geometry
            p.update({"geometry": f"{lng},{lat}", "geometryType": "esriGeometryPoint",
                      "inSR": 4326, "spatialRel": "esriSpatialRelIntersects"})
            if distance_m is not None:
                p.update({"distance": distance_m, "units": "esriSRUnit_Meter"})
        return p

    async def _get_json(self, url: str, params: Dict[str, Any]) -> Dict[str, Any]:
        assert self._session is not None, "use `async with TelAvivGisClient()`"
        last_exc: Optional[BaseException] = None
        for attempt in range(self._retries + 1):
            try:
                async with self._sem:
                    async with self._session.get(url, params=params, timeout=self._timeout) as resp:
                        if resp.status >= 500 or resp.status == 429:
                            raise aiohttp.ClientResponseError(
                                resp.request_info, resp.history, status=resp.status,
                                message=f"HTTP {resp.status}")
                        resp.raise_for_status()
                        # aiohttp honours Content-Encoding (the server gzips unasked);
                        # content_type=None because geojson isn't always labelled JSON.
                        body = await resp.json(content_type=None)
            except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError) as exc:
                retriable = not (isinstance(exc, aiohttp.ClientResponseError)
                                 and exc.status < 500 and exc.status != 429)
                last_exc = exc
                if not retriable or attempt == self._retries:
                    break
                delay = 0.5 * 2 ** attempt
                logger.warning("TLV GIS request failed (%s); retry %d/%d in %.1fs",
                               exc, attempt + 1, self._retries, delay)
                await asyncio.sleep(delay)
                continue
            if isinstance(body, dict) and "error" in body:
                # ArcGIS reports errors with HTTP 200 + {"error": {...}}.
                raise TelAvivGisError(f"{url}: {body['error']}")
            if not isinstance(body, dict):
                raise TelAvivGisError(f"{url}: unexpected response {type(body).__name__}")
            return body
        raise TelAvivGisError(f"{url}: request failed: {last_exc}") from last_exc

    @staticmethod
    def _to_feature(spec: LayerSpec, f: Dict[str, Any], now: datetime) -> ConstructionFeature:
        attrs = f.get("properties") or {}
        m = spec.mapper(attrs, now)
        geom = f.get("geometry")
        oid = attrs.get(spec.oid_field, f.get("id"))
        return ConstructionFeature(
            layer=spec.key, layer_id=spec.layer_id,
            object_id=int(oid) if oid is not None else -1,
            kind=spec.kind, stage=m.stage, severity=m.severity, address=m.address,
            dates=m.dates, geometry_wgs84=geom, centroid=centroid(geom), raw_attributes=attrs,
            title=m.title, url=m.url, tags=m.tags)


async def query(layer: Union[str, int], **kwargs: Any) -> List[ConstructionFeature]:
    """One-shot ``TelAvivGisClient().query(...)`` with its own short-lived session."""
    async with TelAvivGisClient() as gis:
        return await gis.query(layer, **kwargs)
