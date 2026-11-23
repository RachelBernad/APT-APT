# rentlyfly.py
"""rentlyfly.ai source — a Facebook-Groups rental aggregator.

Verified to cover Tel Aviv-Yafo only, so it is layered on top of a Yad2 search
whose city is Tel Aviv. Unlike Yad2, it filters server-side and returns amenity
flags inline (no per-item enrichment needed). Ported from the old
``facebook_groups_scraper.py``.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
from typing import Any, Dict, List, Optional

import aiohttp

import config
from models import GatewayFilters

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": config.USER_AGENT,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.5",
    # Force gzip/deflate (NOT br): rentlyfly is behind Cloudflare which otherwise
    # returns Brotli, and the container's aiohttp can't always decode it
    # ("Can not decode content-encoding: br") → 0 listings. Cloudflare honors this.
    "Accept-Encoding": "gzip, deflate",
    "Referer": "https://rentlyfly.ai/",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
}


def _md5(payload: Dict[str, Any]) -> str:
    return hashlib.md5(
        json.dumps(payload, sort_keys=True, default=str, ensure_ascii=False).encode()
    ).hexdigest()


def _extract_floor(description: Optional[str]) -> Optional[int]:
    if not description:
        return None
    text = description.replace("\n", " ")
    if "קומת קרקע" in text or "ground floor" in text.casefold():
        return 0
    match = re.search(r"(?:ב?קומה|floor)\s*(\d+)", text, re.IGNORECASE)
    return int(match.group(1)) if match else None


def _numeric_price(price: Any) -> Optional[int]:
    if isinstance(price, (int, float)):
        return int(price)
    if isinstance(price, str):
        digits = re.sub(r"[^\d]", "", price)
        return int(digits) if digits else None
    return None


def _build_params(page: int, f: GatewayFilters) -> Dict[str, Any]:
    params: Dict[str, Any] = {"page": page, "limit": config.RENTLYFLY_PAGE_LIMIT}
    if f.min_price is not None:
        params["minPrice"] = int(f.min_price)
    if f.max_price is not None:
        params["maxPrice"] = int(f.max_price)
    if f.min_rooms is not None:
        params["minRooms"] = f.min_rooms
    if f.max_rooms is not None:
        params["maxRooms"] = f.max_rooms
    if f.min_floor is not None:
        params["minFloor"] = int(f.min_floor)
    if f.max_floor is not None:
        params["maxFloor"] = int(f.max_floor)
    return params


def normalize(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize a rentlyfly listing into the shared listing dict shape."""
    item_id = str(raw.get("id", ""))
    location_details = raw.get("location", {})
    if isinstance(location_details, str):
        location_details = {"city": location_details}
    location_details = location_details or {}

    city = location_details.get("city", "")
    area = location_details.get("area", "")
    hood = location_details.get("hood", "")
    street = location_details.get("street", "")
    parts = [p for p in (street, hood, city) if p]
    full_address = ", ".join(parts)

    description = raw.get("description", "")
    price = raw.get("price")
    numeric_price = _numeric_price(price)

    # Map rentlyfly's inline amenity flags to our canonical feature keys.
    inline = {
        "elevator": raw.get("isElevator"), "balcony": raw.get("isBalcony"),
        "ac": raw.get("isAirConditioner"), "renovated": raw.get("isRenovated"),
        "furniture": raw.get("isFurnished"), "mamad": raw.get("isMamad"),
        "parking": raw.get("isParking"), "pets": raw.get("isPetsAllowed"),
    }
    features = [k for k, v in inline.items() if v]

    return {
        "uid": f"rentlyfly:{item_id}",
        "id": item_id,
        "source_id": item_id,
        "source": "rentlyfly",
        "type": "rentlyfly",
        "price": numeric_price,
        "formatted_price": price,
        "rooms": str(raw.get("roomsAvailable", "")),
        "size": "",
        "floor": _extract_floor(description),
        "city": city,
        "area": area,
        "hood": hood,
        "street": street,
        "location": full_address,
        "latitude": location_details.get("latitude"),
        "longitude": location_details.get("longitude"),
        "is_mamad": bool(raw.get("isMamad", False)),
        "features": features,
        "property_type": "",
        "property_type_id": None,
        "property_condition": None,
        "order_id": None,     # rentlyfly is returned newest-first already
        "description": description,
        "images": raw.get("photos", []) or [],
        "tags": [],
        "apartment_page_url": raw.get("url", ""),
        "group_name": raw.get("group_name"),
        "is_shared_apartment": raw.get("isSharedApartment"),
        "is_sublet": raw.get("isSublet"),
        "md5": _md5({"price": numeric_price, "id": item_id, "location": full_address}),
    }


async def _fetch_page(http: aiohttp.ClientSession, page: int, f: GatewayFilters) -> dict:
    params = _build_params(page, f)
    timeout = aiohttp.ClientTimeout(total=config.TOTAL_TIMEOUT)
    async with http.get(
        config.RENTLYFLY_API_URL, headers=_HEADERS, params=params, timeout=timeout
    ) as resp:
        if resp.status != 200:
            logger.warning("rentlyfly returned status %s for page %s", resp.status, page)
            return {}
        return await resp.json()


async def fetch_tel_aviv(http: aiohttp.ClientSession, f: GatewayFilters) -> List[Dict[str, Any]]:
    """Fetch the most-recent Tel Aviv listings (bounded to a few pages)."""
    results: List[Dict[str, Any]] = []
    for page in range(1, config.RENTLYFLY_MAX_PAGES + 1):
        try:
            payload = await _fetch_page(http, page, f)
        except Exception as exc:
            logger.warning("rentlyfly fetch failed on page %s: %s", page, exc)
            break
        data = payload.get("data", []) or []
        if not data:
            break
        results.extend(normalize(item) for item in data)
        if not (payload.get("pagination", {}) or {}).get("hasMore", False):
            break
    logger.debug("rentlyfly returned %d Tel Aviv listings", len(results))
    return results
