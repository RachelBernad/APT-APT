# locations.py
"""Resolve user input into Yad2 location targets, and browse a bundled catalog.

Two ways to pick an area:
- **Search** (nationwide): the Yad2 autocomplete does fuzzy *Hebrew* matching; we
  translate English/typos to Hebrew first (alias table + rapidfuzz), then rank.
- **Browse**: a bundled catalog harvested from Yad2 (``data/il_locations.json`` +
  ``data/hoods/<cityId>.json``) lets the user tap through cities → quarters → hoods
  without typing. Falls back to live autocomplete for anything not bundled.

All five "area templates" are supported: region / area / city / hood / street.
"""
from __future__ import annotations

import json
import logging
from functools import lru_cache
from typing import Any, Dict, List, Optional

import aiohttp
from rapidfuzz import fuzz, process

import config
from models import LocationTarget, ResolvedLocation, normalize_name

logger = logging.getLogger(__name__)

# Common English / transliterated names -> Hebrew, so English input still works.
ALIASES: Dict[str, str] = {
    "tel aviv": "תל אביב", "tel aviv yafo": "תל אביב יפו", "tlv": "תל אביב",
    "jaffa": "יפו", "yafo": "יפו", "florentin": "פלורנטין", "jerusalem": "ירושלים",
    "haifa": "חיפה", "beer sheva": "באר שבע", "beersheba": "באר שבע",
    "netanya": "נתניה", "herzliya": "הרצליה", "ramat gan": "רמת גן",
    "givatayim": "גבעתיים", "rishon lezion": "ראשון לציון", "petah tikva": "פתח תקווה",
    "holon": "חולון", "bat yam": "בת ים", "rehovot": "רחובות", "ashdod": "אשדוד",
    "ashkelon": "אשקלון", "eilat": "אילת", "kiryat": "קריית", "krayot": "קריות",
    "ramat aviv": "רמת אביב", "raanana": "רעננה", "kfar saba": "כפר סבא",
    "modiin": "מודיעין", "nahariya": "נהריה", "tiberias": "טבריה", "nazareth": "נצרת",
    "old north": "הצפון הישן", "new north": "הצפון החדש",
}

_ALIAS_KEYS = list(ALIASES.keys())

_LEVEL_RANK = {"city": 4, "hood": 3, "area": 2, "street": 1, "region": 0}


def _has_hebrew(text: str) -> bool:
    return any("֐" <= ch <= "׿" for ch in text)


def normalize_query(user_text: str) -> str:
    """Translate English / typo input to Hebrew when possible; else pass through."""
    text = (user_text or "").strip()
    if not text or _has_hebrew(text):
        return text
    lowered = text.casefold()
    if lowered in ALIASES:
        return ALIASES[lowered]
    match = process.extractOne(lowered, _ALIAS_KEYS, scorer=fuzz.WRatio, score_cutoff=72)
    if match:
        return ALIASES[match[0]]
    return text


def _as_int(value: Any) -> Optional[int]:
    try:
        return int(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


_AC_CACHE: Dict[str, Any] = {}   # text_he -> payload (locations are stable; safe to cache)
_AC_CACHE_MAX = 2000


async def autocomplete(http: aiohttp.ClientSession, text_he: str) -> Dict[str, Any]:
    """Call the Yad2 address-autocomplete endpoint (cached). Returns {} on failure."""
    import json as _json

    from yad2_gateway import _is_challenge, gateway_headers

    if text_he in _AC_CACHE:
        return _AC_CACHE[text_he]

    params = {"text": text_he}
    for attempt in range(3):
        try:
            async with http.get(
                config.YAD2_AUTOCOMPLETE_URL, params=params, headers=gateway_headers(),
                timeout=aiohttp.ClientTimeout(total=config.REQUEST_TIMEOUT),
            ) as resp:
                text = await resp.text()
                if _is_challenge(text, resp.headers.get("content-type", "")) or resp.status == 403:
                    if attempt < 2:
                        continue
                    logger.warning("Autocomplete bot-challenged for %r", text_he)
                    return {}
                if resp.status != 200:
                    logger.warning("Autocomplete status %s for %r", resp.status, text_he)
                    return {}
                payload = _json.loads(text)
                if len(_AC_CACHE) < _AC_CACHE_MAX:
                    _AC_CACHE[text_he] = payload
                return payload
        except Exception as exc:
            if attempt < 2:
                continue
            logger.warning("Autocomplete failed for %r: %s", text_he, exc)
            return {}
    return {}


def _first_segment(full_title: str) -> str:
    return (full_title or "").split(",")[0].strip()


def _flatten(payload: Dict[str, Any]) -> List[ResolvedLocation]:
    out: List[ResolvedLocation] = []

    for h in payload.get("hoods", []) or []:
        region_id = _as_int(h.get("regionId"))
        if region_id is None:
            continue
        out.append(ResolvedLocation(
            level="hood", display=h.get("fullTitleText", ""), region_id=region_id,
            area_id=_as_int(h.get("areaId")), city_id=_as_int(h.get("cityId")),
            hood_id=_as_int(h.get("hoodId")),
            match_name=_first_segment(h.get("fullTitleText", "")),
        ))

    for c in payload.get("cities", []) or []:
        region_id = _as_int(c.get("regionId"))
        if region_id is None:
            continue
        out.append(ResolvedLocation(
            level="city", display=c.get("fullTitleText", ""), region_id=region_id,
            area_id=_as_int(c.get("areaId")), city_id=_as_int(c.get("cityId")),
        ))

    for a in payload.get("areas", []) or []:
        region_id = _as_int(a.get("regionId"))
        if region_id is None:
            continue
        out.append(ResolvedLocation(
            level="area", display=a.get("fullTitleText", ""), region_id=region_id,
            area_id=_as_int(a.get("areaId")),
        ))

    for s in payload.get("streets", []) or []:
        region_id = _as_int(s.get("regionId"))
        if region_id is None:
            continue
        out.append(ResolvedLocation(
            level="street", display=s.get("fullTitleText", ""), region_id=region_id,
            area_id=_as_int(s.get("areaId")), city_id=_as_int(s.get("cityId")),
            street_id=_as_int(s.get("streetId")),
            match_name=_first_segment(s.get("fullTitleText", "")),
        ))

    for r in payload.get("regions", []) or []:
        region_id = _as_int(r.get("regionId"))
        if region_id is None:
            continue
        out.append(ResolvedLocation(
            level="region", display=r.get("fullTitleText", ""), region_id=region_id,
        ))

    return out


async def resolve_candidates(
    http: aiohttp.ClientSession, user_text: str, limit: int = 8
) -> List[ResolvedLocation]:
    """Return up to ``limit`` best-matching locations for the user's text."""
    query_he = normalize_query(user_text)
    if not query_he:
        return []
    candidates = _flatten(await autocomplete(http, query_he))
    if not candidates:
        return []

    def score(loc: ResolvedLocation) -> float:
        return fuzz.WRatio(query_he, loc.display) + _LEVEL_RANK.get(loc.level, 0)

    candidates.sort(key=score, reverse=True)
    return candidates[:limit]


async def batch_autocomplete(
    http: aiohttp.ClientSession, terms: List[str]
) -> List[ResolvedLocation]:
    """Union candidates across many queries (beats the ~5-per-category cap)."""
    seen: Dict[tuple, ResolvedLocation] = {}
    for term in terms:
        for loc in _flatten(await autocomplete(http, normalize_query(term))):
            key = (loc.level, loc.region_id, loc.area_id, loc.city_id,
                   loc.hood_id, loc.street_id, loc.display)
            seen.setdefault(key, loc)
    return list(seen.values())


def target_from_resolved(loc: ResolvedLocation) -> LocationTarget:
    """Build the persisted area target from a resolved autocomplete candidate."""
    match_name = ""
    if loc.level in ("hood", "street") and loc.match_name:
        match_name = normalize_name(loc.match_name)
    return LocationTarget(
        level=loc.level, region_id=loc.region_id, area_id=loc.area_id,
        city_id=loc.city_id, hood_id=loc.hood_id, street_id=loc.street_id,
        display_name=loc.display, match_name=match_name,
    )


# --- bundled catalog (browse) ---

@lru_cache(maxsize=1)
def load_catalog() -> Dict[str, Any]:
    try:
        with open(config.IL_LOCATIONS_FILE, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("Could not load location catalog: %s", exc)
        return {"regions": {}, "areas": {}, "cities": {}}


@lru_cache(maxsize=64)
def load_city_hoods(city_id: int) -> Optional[Dict[str, Any]]:
    path = config.HOODS_DIR / f"{city_id}.json"
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def browsable_city_ids() -> List[int]:
    """Cities that ship a bundled quarter→hood catalog for tap-to-browse."""
    ids = []
    try:
        for p in sorted(config.HOODS_DIR.glob("*.json")):
            if p.stem.isdigit():
                ids.append(int(p.stem))
    except OSError:
        pass
    return ids


def city_quarters(city_id: int) -> List[Dict[str, Any]]:
    data = load_city_hoods(city_id) or {}
    return data.get("quarters", [])


def _hood_display(hood_name: str, city_display: str) -> str:
    """Yad2's own 'hood, city' format so colliding hood names stay unambiguous."""
    return f"{hood_name}, {city_display}" if city_display else hood_name


def hood_target(city_catalog: Dict[str, Any], hood_name: str,
                city_display: str = "") -> LocationTarget:
    """Build a hood target from a bundled city catalog entry (hoods have no id)."""
    return LocationTarget(
        level="hood",
        region_id=int(city_catalog.get("regionId") or 0),
        area_id=_as_int(city_catalog.get("areaId")),
        city_id=_as_int(city_catalog.get("cityId")),
        hood_id=None,
        display_name=_hood_display(hood_name, city_display),
        match_name=normalize_name(hood_name),
    )


def make_hood_target(region_id: int, city_id: int, hood_name: str,
                     city_display: str = "") -> LocationTarget:
    """Build a hood target for a live-fetched (non-bundled) city hood."""
    return LocationTarget(
        level="hood", region_id=region_id, area_id=None, city_id=city_id,
        hood_id=None, display_name=_hood_display(hood_name, city_display),
        match_name=normalize_name(hood_name),
    )


# Cities offered as one-tap browse entries (resolved to real ids from the catalog).
_POPULAR_CITY_NAMES = [
    "תל אביב יפו", "ירושלים", "חיפה", "ראשון לציון", "פתח תקווה", "נתניה",
    "באר שבע", "בני ברק", "חולון", "רמת גן", "בת ים", "אשדוד", "הרצליה",
    "רעננה", "כפר סבא", "נס ציונה", "קרית ביאליק", "אשקלון", "רחובות", "מודיעין",
]


def city_name(city_id: int) -> str:
    """City display name from the bundled catalog (falls back to the id)."""
    c = load_catalog().get("cities", {}).get(str(city_id))
    return c["name"] if c and c.get("name") else str(city_id)


@lru_cache(maxsize=1)
def popular_cities() -> List[tuple]:
    """Return [(name, region_id, city_id)] for well-known cities present in the catalog."""
    cat = load_catalog()
    by_name = {}
    for cid, c in cat.get("cities", {}).items():
        by_name.setdefault(c.get("name"), (cid, c.get("regionId")))
    by_norm = {normalize_name(n): v for n, v in by_name.items()}
    out = []
    for name in _POPULAR_CITY_NAMES:
        hit = by_name.get(name) or by_norm.get(normalize_name(name))
        if hit and hit[1]:
            out.append((name, int(hit[1]), int(hit[0])))
    return out


async def fetch_city_hoods(http: aiohttp.ClientSession, region_id: int, city_id: int) -> List[str]:
    """Live-list a city's neighborhoods from the feed (one request, fast).

    Works for any city in Israel — markers carry the hood name. Capped at 200
    markers, which still surfaces essentially every active neighborhood.
    """
    import yad2_gateway
    from models import GatewayFilters, LocationSignature
    sig = LocationSignature(region_id, None, city_id)
    params = yad2_gateway._build_params(sig, GatewayFilters())
    markers = await yad2_gateway._get_markers(http, params)
    hoods: Dict[str, str] = {}
    for m in markers:
        text = ((m.get("address") or {}).get("neighborhood") or {}).get("text")
        if text:
            hoods.setdefault(normalize_name(text), text)
    return sorted(hoods.values())
