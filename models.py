# models.py
"""Typed data structures shared across the scraping/matching/routing pipeline."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


def normalize_name(text: str) -> str:
    """Canonical normalizer for hood/street names — the local match key.

    Used identically when building a target's ``match_name`` and when comparing
    against a listing's hood/street text, so they always line up.
    """
    text = (text or "").replace("קריית", "קרית")
    return "".join(text.split()).casefold()


@dataclass(frozen=True)
class LocationSignature:
    """Grouping key: searches that share this are scraped with a single request.

    Scrape granularity is the most specific *known* level of a target
    (city if known, else area, else region). Hood/street are never pushed
    server-side — they are matched locally — so ``neighborhood_id`` is kept only
    for backward-compatible callers and is normally None.
    """
    region_id: int
    area_id: Optional[int]
    city_id: Optional[str]        # Yad2 city ids are 4-char zero-padded strings ("0070")
    neighborhood_id: Optional[int] = None


@dataclass
class LocationTarget:
    """One area a saved search watches. A search can hold many of these.

    ``level`` is one of region|area|city|hood|street. The scrape scope is derived
    from the ids; ``match_name`` (normalized hood/street text) is the local match
    key for the finest levels, since markers carry hood/street names but no ids.
    """
    level: str
    region_id: int
    area_id: Optional[int] = None
    city_id: Optional[str] = None     # Yad2 city ids are 4-char zero-padded strings
    hood_id: Optional[int] = None
    street_id: Optional[int] = None
    display_name: str = ""
    match_name: str = ""


@dataclass
class ResolvedLocation:
    """A location candidate resolved from the Yad2 autocomplete API."""
    level: str                       # 'region' | 'area' | 'city' | 'hood' | 'street'
    display: str                     # fullTitleText
    region_id: int
    area_id: Optional[int] = None
    city_id: Optional[str] = None    # 4-char zero-padded string ("0070")
    hood_id: Optional[int] = None
    street_id: Optional[int] = None
    match_name: Optional[str] = None  # neighborhood/street text, for local matching


# Verified boolean feature filters on the Yad2 rent feed (param names read straight
# off the site's own filter URL). key -> (gateway param, Hebrew label, English
# label, emoji). All are server-side-only (markers don't carry them), so they are
# part of the scrape group key; rentlyfly reports the overlapping ones inline.
FEATURES: Dict[str, tuple] = {
    "elevator":         ("elevator",       "מעלית",       "Elevator",        "🛗"),
    "parking":          ("parking",        "חניה",        "Parking",         "🅿"),
    "balcony":          ("balcony",        "מרפסת",       "Balcony",         "🌇"),
    "ac":               ("airConditioner", "מיזוג",       "A/C",             "❄️"),
    "renovated":        ("renovated",      "משופצת",      "Renovated",       "✨"),
    "furniture":        ("furniture",      "מרוהטת",      "Furnished",       "🛋"),
    "mamad":            ("shelter",        "ממ״ד",        "Safe room",       "🛡"),
    "mamak":            ("floorShelter",   "ממ״ק",        "Floor safe room", "🛡"),
    "building_shelter": ("buildingShelter","מקלט בבניין", "Building shelter", "🚨"),
    "bars":             ("bars",           "סורגים",      "Window bars",     "🪟"),
    "warehouse":        ("warehouse",      "מחסן",        "Storage",         "📦"),
    "accessibility":    ("accessibility",  "גישה לנכים",  "Accessible",      "♿"),
    "exclusive":        ("assetExclusive", "בבלעדיות",    "Exclusive",       "⭐"),
    "pets":             ("pets",           "חיות מחמד",   "Pets allowed",    "🐾"),
    "image_only":       ("imageOnly",      "עם תמונה",    "With photo",      "🖼"),
    "price_only":       ("priceOnly",      "עם מחיר",     "With price",      "🏷"),
}


def feature_param(key: str) -> Optional[str]:
    spec = FEATURES.get(key)
    return spec[0] if spec else None


def feature_label(key: str) -> str:
    spec = FEATURES.get(key)
    return f"{spec[3]} {spec[2]}" if spec else key


@dataclass
class GatewayFilters:
    """The *loosest union* of a group's filters, sent to the gateway server-side.

    Boolean features (see ``FEATURES``) are not carried by markers, so they are
    part of the scrape group key and every member shares the same set. Everything
    else (price/rooms/sqm/floor/property type/condition) is on the marker and can
    also be matched locally.
    """
    min_price: Optional[int] = None
    max_price: Optional[int] = None
    min_rooms: Optional[float] = None
    max_rooms: Optional[float] = None
    min_sqm: Optional[int] = None
    max_sqm: Optional[int] = None
    min_floor: Optional[int] = None
    max_floor: Optional[int] = None
    property_ids: Optional[List[int]] = None  # Yad2 property-type ids; None = all
    property_condition: Optional[int] = None
    features: List[str] = field(default_factory=list)


@dataclass
class SavedSearch:
    """Mirrors a row of the ``saved_searches`` table plus its area targets.

    A search = one filter set + a list of ``locations`` (area targets). Feature
    requirements are search-level (shared across all its targets).
    """
    id: int
    chat_id: int
    label: str
    min_price: Optional[int] = None
    max_price: Optional[int] = None
    min_rooms: Optional[float] = None
    max_rooms: Optional[float] = None
    min_sqm: Optional[int] = None
    max_sqm: Optional[int] = None
    min_floor: Optional[int] = None
    max_floor: Optional[int] = None
    property_types: Optional[List[int]] = None
    property_condition: Optional[int] = None
    features: List[str] = field(default_factory=list)
    source_mode: str = "auto"        # 'auto' | 'yad2' | 'yad2+rentlyfly'
    is_active: bool = True
    is_primed: bool = False
    locations: List[LocationTarget] = field(default_factory=list)


# A "listing" stays a plain dict (the normalized shape consumed by
# formatting.format_apartment_message). The canonical keys are documented here.
Listing = Dict[str, Any]

# Keys every normalized listing dict carries:
#   uid, id, source_id, source, type, price, rooms, size, floor,
#   city, area, hood, street, location, latitude, longitude,
#   is_mamad, is_elevator, description, images, tags, apartment_page_url, md5,
#   property_type, property_condition, order_id
LISTING_KEYS = (
    "uid", "id", "source_id", "source", "type", "price", "rooms", "size",
    "floor", "city", "area", "hood", "street", "location", "latitude",
    "longitude", "is_mamad", "is_elevator", "description", "images", "tags",
    "apartment_page_url", "md5", "property_type", "property_condition", "order_id",
)


# Verified Yad2 property-type ids -> Hebrew label (see api_docs/yad2).
PROPERTY_TYPES: Dict[int, str] = {
    1: "דירה",
    3: "דירת גן",
    4: "סטודיו/לופט",
    5: "בית פרטי/קוטג'",
    6: "גג/פנטהאוז",
    7: "דופלקס",
    11: "יחידת דיור",
    39: "דו משפחתי",
    49: "מרתף/פרטר",
    51: "טריפלקס",
}

# Verified Yad2 property-condition ids -> Hebrew label (from the filters panel;
# ids observed on markers are 1,2,3,5,6).
PROPERTY_CONDITIONS: Dict[int, str] = {
    1: "חדש מקבלן",
    2: "חדש (עד 10 שנים)",
    3: "משופץ",
    5: "במצב שמור",
    6: "דרוש שיפוץ",
}
