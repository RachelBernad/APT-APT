"""Layer registry: which MapServer layers we support and how their attributes map onto
Kind / Stage / Severity / address / dates.

Every rule here is derived from verified values documented in
``api_docs/services/IView2WM/layers/<id>-*.md``. If the server's codes change, update
both the page and this file (see ../CLAUDE.md).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone, tzinfo
from typing import Any, Callable, Dict, FrozenSet, Iterable, Optional, Tuple, Union

from aptbot.scrapers.tel_aviv_gis.models import Kind, Severity, Stage

try:
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
    try:
        ISRAEL_TZ: tzinfo = ZoneInfo("Asia/Jerusalem")
    except ZoneInfoNotFoundError:  # slim images without tzdata
        ISRAEL_TZ = timezone(timedelta(hours=2), "IST")
except ImportError:  # pragma: no cover
    ISRAEL_TZ = timezone(timedelta(hours=2), "IST")

Attrs = Dict[str, Any]
Mapped = Tuple[Stage, Severity, Optional[str], Dict[str, datetime]]


# --- attribute parsing -------------------------------------------------------------

def epoch_ms(value: Any) -> Optional[datetime]:
    """ArcGIS Date fields: epoch milliseconds, UTC."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc)


def dmy(value: Any) -> Optional[datetime]:
    """String dates like ``23/11/2026`` (Israel local midnight)."""
    if not isinstance(value, str):
        return None
    try:
        d = datetime.strptime(value.strip()[:10], "%d/%m/%Y")
    except ValueError:
        return None
    return d.replace(tzinfo=ISRAEL_TZ)


def text(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    cleaned = " ".join(value.split()).strip(" ,")
    return cleaned or None


def _dates(**kv: Optional[datetime]) -> Dict[str, datetime]:
    return {k: v for k, v in kv.items() if v is not None}


# --- shared rules ------------------------------------------------------------------

# Substring tokens over the request-type text, normalised so בנייה == בניה.
# Highest matching token wins (docs/SPEC.md, decision 8).
_HIGH_TOKENS = ("בניה חדשה", "הריסה", "חפירה", "דיפון", "מרתפים", "רב קומות")
_MEDIUM_TOKENS = ("תוספות בניה", "תוספת בניה", "תוספות ושינויים", "ציבורי",
                  "חידוש היתר", "הארכת תוקף")
HIGH_UNITS_THRESHOLD = 4


def severity_from_type(*texts: Any) -> Severity:
    joined = " ".join(t for t in texts if isinstance(t, str) and t.strip())
    if not joined.strip():
        return Severity.MEDIUM  # unknown type: don't let callers silently drop it
    norm = joined.replace("בנייה", "בניה")
    if any(tok in norm for tok in _HIGH_TOKENS):
        return Severity.HIGH
    if any(tok in norm for tok in _MEDIUM_TOKENS):
        return Severity.MEDIUM
    return Severity.LOW


def stage_from_window(start: Optional[datetime], end: Optional[datetime],
                      now: datetime) -> Stage:
    """Date-range layers. ``end`` is a calendar day and is inclusive (whole day)."""
    if start is None and end is None:
        return Stage.UNKNOWN
    if start is not None and now < start:
        return Stage.PERMITTED
    if end is not None and now >= end + timedelta(days=1):
        return Stage.COMPLETED
    return Stage.ACTIVE


# --- per-layer mappings ------------------------------------------------------------

_PERMIT_STAGES = {
    "בתהליך היתר": Stage.REQUESTED,
    "קיים היתר": Stage.PERMITTED,
    "בבניה": Stage.ACTIVE,
    "קיימת לפחות תעודת גמר אחת": Stage.COMPLETED,
    "קיים אכלוס": Stage.COMPLETED,
}


def _map_permits(a: Attrs, now: datetime) -> Mapped:  # layer 772
    stage = _PERMIT_STAGES.get(text(a.get("building_stage")) or "", Stage.UNKNOWN)
    expires = epoch_ms(a.get("expiry_date"))
    if stage is Stage.PERMITTED and expires is not None and expires < now:
        stage = Stage.LAPSED
    severity = severity_from_type(a.get("sug_bakasha"))
    units = a.get("yechidot_diyur")
    if isinstance(units, (int, float)) and units >= HIGH_UNITS_THRESHOLD:
        severity = Severity.HIGH
    dates = _dates(requested=epoch_ms(a.get("open_request")),
                   issued=epoch_ms(a.get("permission_date")),
                   expires=expires,
                   start=epoch_ms(a.get("tr_hathalat_bniya")))
    return stage, severity, text(a.get("addresses")), dates


_SITE_STAGES = {
    "פתיחת תיק": Stage.PERMITTED,
    "פיצול תיק לבניה בשלבים": Stage.PERMITTED,
    "תחילת עבודות": Stage.ACTIVE,
    "עבודות עפר וביסוס": Stage.ACTIVE,
    "גמר יסודות": Stage.ACTIVE,
    "גמר שלד": Stage.ACTIVE,
    "גמר בניה": Stage.COMPLETED,
}


def _map_sites(a: Attrs, now: datetime) -> Mapped:  # layer 499
    raw_stage = text(a.get("matzav_bniya"))
    # Empty stage = finished building (all carry an occupancy/form-4/completion status).
    stage = Stage.COMPLETED if raw_stage is None else _SITE_STAGES.get(raw_stage, Stage.UNKNOWN)
    severity = severity_from_type(a.get("sug_bakasha"), a.get("sivug"))
    dates = _dates(works_approved=epoch_ms(a.get("tr_tchilat_avoda")),
                   status=epoch_ms(a.get("tr_status_pikuach")))
    return stage, severity, text(a.get("ktovet")), dates


def _map_street_licenses(a: Attrs, now: datetime) -> Mapped:  # layer 433
    start = epoch_ms(a.get("tr_tchilat_avoda_befoal")) or epoch_ms(a.get("tr_tokef_rishayon_me"))
    end = epoch_ms(a.get("tr_sium_avoda_metuchnan")) or epoch_ms(a.get("tr_tokef_rishayon_ad"))
    return (stage_from_window(start, end, now), Severity.MEDIUM, text(a.get("t_rechov")),
            _dates(start=start, end=end))


def _map_public_night_works(a: Attrs, now: datetime) -> Mapped:  # layer 858
    start = epoch_ms(a.get("tr_avodat_laila_me"))
    end = epoch_ms(a.get("tr_avodat_laila_ad"))
    street = text(a.get("t_rechov"))
    frm, to = text(a.get("t_rechov_me")), text(a.get("t_rechov_ad"))
    if street and frm and to:
        street = f"{street} ({frm} – {to})"
    return stage_from_window(start, end, now), Severity.HIGH, street, _dates(start=start, end=end)


def _map_site_night_works(a: Attrs, now: datetime) -> Mapped:  # layer 479
    start, end = dmy(a.get("tr_hatchala")), dmy(a.get("tr_siyum"))
    return stage_from_window(start, end, now), Severity.HIGH, text(a.get("ktovet")), _dates(start=start, end=end)


# --- registry ----------------------------------------------------------------------

@dataclass(frozen=True)
class LayerSpec:
    key: str
    layer_id: int
    name_he: str
    kind: Kind
    oid_field: str
    mapper: Callable[[Attrs, datetime], Mapped]
    # Fields the mapper reads; catalog.py flags them if the server drops any.
    expected_fields: FrozenSet[str]


LAYERS: Dict[str, LayerSpec] = {spec.key: spec for spec in (
    LayerSpec("permits", 772, "בקשות והיתרי בניה", Kind.BUILDING, "oid_permit", _map_permits,
              frozenset({"building_stage", "expiry_date", "sug_bakasha", "yechidot_diyur",
                         "open_request", "permission_date", "tr_hathalat_bniya", "addresses"})),
    LayerSpec("sites", 499, "אתרי בניה", Kind.BUILDING, "oid_site", _map_sites,
              frozenset({"matzav_bniya", "sug_bakasha", "sivug", "tr_tchilat_avoda",
                         "tr_status_pikuach", "ktovet"})),
    LayerSpec("street_licenses", 433, "רישיונות בביצוע", Kind.INFRASTRUCTURE, "oid",
              _map_street_licenses,
              frozenset({"tr_tchilat_avoda_befoal", "tr_tokef_rishayon_me",
                         "tr_sium_avoda_metuchnan", "tr_tokef_rishayon_ad", "t_rechov"})),
    LayerSpec("public_night_works", 858, "עבודות לילה – במרחב הציבורי", Kind.INFRASTRUCTURE,
              "OBJECTID", _map_public_night_works,
              frozenset({"tr_avodat_laila_me", "tr_avodat_laila_ad", "t_rechov",
                         "t_rechov_me", "t_rechov_ad"})),
    LayerSpec("site_night_works", 479, "עבודות לילה – אתרי בנייה", Kind.BUILDING, "oid_avoda",
              _map_site_night_works,
              frozenset({"tr_hatchala", "tr_siyum", "ktovet"})),
)}

_BY_ID: Dict[int, LayerSpec] = {s.layer_id: s for s in LAYERS.values()}


def get_layer(layer: Union[str, int]) -> LayerSpec:
    """Look up a supported layer by registry key (``"permits"``) or MapServer id (``772``)."""
    spec = _BY_ID.get(layer) if isinstance(layer, int) else LAYERS.get(layer)
    if spec is None:
        raise KeyError(f"unsupported Tel Aviv GIS layer {layer!r}; supported: "
                       f"{', '.join(f'{k} ({s.layer_id})' for k, s in LAYERS.items())}")
    return spec


def all_layers() -> Iterable[LayerSpec]:
    return LAYERS.values()
