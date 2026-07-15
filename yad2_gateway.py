# yad2_gateway.py
"""Client for the Yad2 gateway (gw.yad2.co.il).

The ``/realestate-feed/rent/map`` endpoint applies price/rooms/sqm/floor/elevator
filters server-side and, with a full browser-like header set, is reachable from a
plain HTTP client (a minimal header set gets served the Radware JS challenge). Its
one limit is a hard cap of ~200 markers with no pagination, which we defeat by
recursively bisecting the price interval.

Note: markers carry no mamad/tags/description, and Yad2 item pages are Radware-
blocked to non-browser clients, so mamad is not determinable here. Elevator IS a
server-side filter, so it is pushed into the query instead of read from a marker.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from typing import Any, Dict, List, Optional

import aiohttp

import config
from models import (PROPERTY_TYPES, GatewayFilters, LocationSignature,
                   feature_param)

logger = logging.getLogger(__name__)

APARTMENT_PAGE_URL_TEMPLATE = "https://www.yad2.co.il/realestate/item/{token}"

# Reverse map of Yad2 property-type text -> id. Marker texts use loose spacing
# ("סטודיו/ לופט"), so match on a space-stripped key.
def _ptype_key(text: str) -> str:
    return "".join((text or "").split())


_PROPERTY_TEXT_TO_ID = {_ptype_key(name): pid for pid, name in PROPERTY_TYPES.items()}


def gateway_headers() -> Dict[str, str]:
    # A full browser-like header set is what gets past Radware/ShieldSquare bot
    # detection on the gateway; a minimal set gets served the JS challenge.
    return {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7",
        "User-Agent": config.USER_AGENT,
        "Referer": "https://www.yad2.co.il/",
        "Origin": "https://www.yad2.co.il",
        "Sec-Ch-Ua": '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
    }


def _is_challenge(text: str, content_type: str) -> bool:
    if "application/json" in content_type:
        return False
    lowered = text.lower()
    return (
        "perfdrive" in lowered
        or "__uzdbm" in lowered
        or "captcha" in lowered
        or "<!doctype html" in lowered
    )


def _md5(thing: Any) -> str:
    return hashlib.md5(str(thing).encode()).hexdigest()


def _num_price(value: Any) -> Optional[int]:
    """Coerce a marker price to int; None if not numeric (keeps comparisons safe)."""
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        digits = "".join(ch for ch in value if ch.isdigit())
        return int(digits) if digits else None
    return None


def _build_params(sig: LocationSignature, f: GatewayFilters,
                  price_override: Optional[tuple] = None) -> Dict[str, Any]:
    params: Dict[str, Any] = {"region": sig.region_id}
    if sig.area_id is not None:
        params["area"] = sig.area_id
    if sig.city_id is not None:
        params["city"] = sig.city_id
    if sig.neighborhood_id is not None:
        params["neighborhood"] = sig.neighborhood_id

    min_price, max_price = (
        price_override if price_override is not None else (f.min_price, f.max_price)
    )
    if min_price is not None:
        params["minPrice"] = int(min_price)
    if max_price is not None:
        params["maxPrice"] = int(max_price)
    if f.min_rooms is not None:
        params["minRooms"] = f.min_rooms
    if f.max_rooms is not None:
        params["maxRooms"] = f.max_rooms
    if f.min_sqm is not None:
        params["minSquaremeter"] = int(f.min_sqm)
    if f.max_sqm is not None:
        params["maxSquaremeter"] = int(f.max_sqm)
    if f.min_floor is not None:
        params["minFloor"] = int(f.min_floor)
    if f.max_floor is not None:
        params["maxFloor"] = int(f.max_floor)
    if f.property_ids:
        params["property"] = ",".join(str(p) for p in f.property_ids)
    for key in f.features:
        param = feature_param(key)
        if param:
            params[param] = 1
    return params


async def _get_markers(http: aiohttp.ClientSession, params: Dict[str, Any]) -> List[dict]:
    timeout = aiohttp.ClientTimeout(total=config.REQUEST_TIMEOUT)
    for attempt in range(4):
        try:
            async with http.get(
                config.YAD2_MAP_URL, params=params, headers=gateway_headers(), timeout=timeout
            ) as resp:
                if resp.status in {500, 502, 503, 504} and attempt < 3:
                    await asyncio.sleep(min(2 ** attempt, 3))
                    continue
                text = await resp.text()
                content_type = resp.headers.get("content-type", "")
                if _is_challenge(text, content_type) or resp.status == 403:
                    logger.warning("Yad2 map bot-challenged (attempt %d/4)", attempt + 1)
                    if attempt < 3:
                        await asyncio.sleep(1.0 * (attempt + 1))
                        continue
                    return []
                resp.raise_for_status()
                payload = json.loads(text)
                if payload.get("message") not in (None, "OK"):
                    logger.warning("Yad2 map message=%r params=%s", payload.get("message"), params)
                return (payload.get("data") or {}).get("markers", []) or []
        except (aiohttp.ClientError, json.JSONDecodeError) as exc:
            if attempt < 3:
                await asyncio.sleep(2 ** attempt)
                continue
            logger.error("Yad2 map request failed for %s: %s", params, exc)
            return []
    return []


def _min_marker_price(markers: List[dict]) -> Optional[int]:
    prices = [m.get("price") for m in markers if isinstance(m.get("price"), (int, float))]
    return int(min(prices)) if prices else None


def _max_marker_price(markers: List[dict]) -> Optional[int]:
    prices = [m.get("price") for m in markers if isinstance(m.get("price"), (int, float))]
    return int(max(prices)) if prices else None


async def fetch_map(
    http: aiohttp.ClientSession, sig: LocationSignature, f: GatewayFilters
) -> List[dict]:
    """Fetch markers for a location signature, defeating the 200-cap via bisection."""
    collected: Dict[str, dict] = {}
    requests_made = 0

    async def recurse(lo: Optional[int], hi: Optional[int]) -> None:
        nonlocal requests_made
        if requests_made >= config.MAP_MAX_REQUESTS:
            return
        requests_made += 1
        params = _build_params(sig, f, price_override=(lo, hi))
        markers = await _get_markers(http, params)
        for m in markers:
            token = m.get("token")
            if token:
                collected[token] = m

        if len(markers) < config.MAP_MARKER_CAP:
            return
        # Cap hit -> we may be truncated. Bisect the price interval if we can.
        lo_eff = lo if lo is not None else _min_marker_price(markers)
        hi_eff = hi if hi is not None else _max_marker_price(markers)
        if (lo_eff is None or hi_eff is None
                or hi_eff - lo_eff <= config.MAP_MIN_PRICE_BAND
                or requests_made >= config.MAP_MAX_REQUESTS):
            logger.warning(
                "Yad2 map still capped at %d for region=%s city=%s hood=%s band=[%s,%s] "
                "(requests=%d); some listings may be missed — tighten the search filters.",
                config.MAP_MARKER_CAP, sig.region_id, sig.city_id, sig.neighborhood_id,
                lo_eff, hi_eff, requests_made,
            )
            return
        mid = (lo_eff + hi_eff) // 2
        await recurse(lo_eff, mid)
        await recurse(mid + 1, hi_eff)

    await recurse(f.min_price, f.max_price)
    return list(collected.values())


def normalize_marker(m: Dict[str, Any], f: Optional[GatewayFilters] = None) -> Dict[str, Any]:
    """Convert a gateway marker into the normalized listing dict shape.

    Amenity flags (elevator/balcony/parking/renovated) are only knowable when the
    query pushed them server-side; when it did, every returned listing has them, so
    we mark the flag True (else None/unknown). Property type/condition and orderId
    come straight off the marker.
    """
    address = m.get("address", {}) or {}
    city = (address.get("city") or {}).get("text", "")
    street = (address.get("street") or {}).get("text", "")
    hood = (address.get("neighborhood") or {}).get("text", "")
    area = (address.get("area") or {}).get("text", "")
    region = (address.get("region") or {}).get("text", "")

    parts: List[str] = []
    for part in (street, hood, city):
        if part and part not in parts:
            parts.append(part)
    location = ", ".join(parts)

    coords = address.get("coords", {}) or {}
    house = address.get("house", {}) or {}
    additional = m.get("additionalDetails", {}) or {}
    metadata = m.get("metaData", {}) or {}
    token = m.get("token", "")

    property_text = (additional.get("property") or {}).get("text", "")
    property_type_id = _PROPERTY_TEXT_TO_ID.get(_ptype_key(property_text))
    condition = (additional.get("propertyCondition") or {}).get("id")

    # Markers carry no feature flags, so a Yad2 listing is known to have exactly
    # the features that were pushed server-side for this scrape.
    features = list(f.features) if f else []

    floor = house.get("floor")
    return {
        "uid": f"yad2:{token}",
        "id": token,
        "source_id": token,
        "source": "yad2",
        "type": "yad2",
        "price": _num_price(m.get("price")),
        "rooms": str(additional.get("roomsCount", "")),
        "size": str(additional.get("squareMeter", "")),
        "floor": "" if floor is None else str(floor),
        "city": city,
        "area": area or region,
        "hood": hood,
        "street": street,
        "location": location,
        "latitude": coords.get("lat"),
        "longitude": coords.get("lon"),
        "is_mamad": True if (f and "mamad" in f.features) else None,
        "features": features,
        "property_type": property_text,
        "property_type_id": property_type_id,
        "property_condition": condition,
        "order_id": m.get("orderId"),
        "description": None,
        "images": metadata.get("images", []) or [],
        "tags": [],
        "apartment_page_url": APARTMENT_PAGE_URL_TEMPLATE.format(token=token),
        "md5": _md5({"location": location, "price": m.get("price")}),
    }
