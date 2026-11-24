"""Layer registry: which MapServer layers we support and how their attributes map onto
Kind / Stage / Severity / address / dates / title / url / tags.

Every rule here is derived from verified values documented in
``api_docs/services/IView2WM/layers/<id>-*.md``. If the server's codes change, update
both the page and this file (see ../CLAUDE.md).

This is a utility library: every record is labeled, nothing is filtered by default.
The ``*_WHERE`` constants are optional presets callers may pass as ``where=``.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone, tzinfo
from typing import Any, Callable, Dict, FrozenSet, Iterable, NamedTuple, Optional, Union

from aptbot.scrapers.tel_aviv_gis.models import (TAG_DEMOLITION_ORDER, TAG_IMMEDIATE_ORDER,
                                                 TAG_TAMA38_ADDITION, TAG_TAMA38_REBUILD,
                                                 TAG_URBAN_RENEWAL, Kind, Severity, Stage)

try:
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
    try:
        ISRAEL_TZ: tzinfo = ZoneInfo("Asia/Jerusalem")
    except ZoneInfoNotFoundError:  # slim images without tzdata
        ISRAEL_TZ = timezone(timedelta(hours=2), "IST")
except ImportError:  # pragma: no cover
    ISRAEL_TZ = timezone(timedelta(hours=2), "IST")

Attrs = Dict[str, Any]


class Mapped(NamedTuple):
    stage: Stage
    severity: Severity
    address: Optional[str]
    dates: Dict[str, datetime]
    title: Optional[str] = None
    url: Optional[str] = None
    tags: FrozenSet[str] = frozenset()


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


def url(value: Any) -> Optional[str]:
    v = text(value)
    return v if v and v.startswith(("http://", "https://")) else None


def _dates(**kv: Optional[datetime]) -> Dict[str, datetime]:
    return {k: v for k, v in kv.items() if v is not None}


def _yes(value: Any) -> bool:
    return text(value) == "כן"


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


# --- per-layer mappings: works (v1) -------------------------------------------------

_PERMIT_STAGES = {
    "בתהליך היתר": Stage.REQUESTED,
    "קיים היתר": Stage.PERMITTED,
    "בבניה": Stage.ACTIVE,
    "קיימת לפחות תעודת גמר אחת": Stage.COMPLETED,
    "קיים אכלוס": Stage.COMPLETED,
}


NO_EXPIRY_YEAR = 2900
_SAME_DAY_SLACK = timedelta(days=1)  # dates are calendar days in different time zones


def _on_or_after(when: Optional[datetime], ref: datetime) -> bool:
    return when is not None and when >= ref - _SAME_DAY_SLACK


def _completion(a: Attrs) -> Optional[datetime]:
    """772 ``finished`` / ``occupation`` are DD/MM/YYYY strings, often empty."""
    return dmy(a.get("finished")) or dmy(a.get("occupation"))


def _map_permits(a: Attrs, now: datetime) -> Mapped:  # layer 772
    stage = _PERMIT_STAGES.get(text(a.get("building_stage")) or "", Stage.UNKNOWN)
    expires = epoch_ms(a.get("expiry_date"))
    if expires is not None and expires.year >= NO_EXPIRY_YEAR:
        expires = None  # 2999-12-31 = "no expiry" sentinel (change-of-use permits)
    issued = epoch_ms(a.get("permission_date"))
    start = epoch_ms(a.get("tr_hathalat_bniya"))
    if stage is Stage.REQUESTED and issued is not None:
        # building_stage is the *building's* latest licensing activity, so an issued permit
        # reads "בתהליך היתר" once a newer request is filed on the same building (953 rows on
        # 2026-11-24). Use the record's own dates instead (api_docs 772 page, "Caveats").
        # Start/finish dates are per building: only count them if they come after this permit
        # was issued (282 of the 953 had a start *before* issue, i.e. earlier works).
        if _on_or_after(_completion(a), issued):
            stage = Stage.COMPLETED
        elif start is not None and _on_or_after(start, issued) and start <= now:
            stage = Stage.ACTIVE
        else:
            stage = Stage.PERMITTED
    if stage is Stage.PERMITTED and expires is not None and expires < now:
        stage = Stage.LAPSED
    severity = severity_from_type(a.get("sug_bakasha"))
    units = a.get("yechidot_diyur")
    if isinstance(units, (int, float)) and units >= HIGH_UNITS_THRESHOLD:
        severity = Severity.HIGH
    tags = set()
    if _yes(a.get("sw_tama_38_chadash")):
        tags.add(TAG_TAMA38_REBUILD)
    if _yes(a.get("sw_tama_38_tosefet")):
        tags.add(TAG_TAMA38_ADDITION)
    dates = _dates(requested=epoch_ms(a.get("open_request")),
                   issued=issued, expires=expires, start=start)
    return Mapped(stage, severity, text(a.get("addresses")), dates,
                  title=text(a.get("sug_bakasha")), url=url(a.get("url_hadmaya")),
                  tags=frozenset(tags))


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
    # url_archion_tik points at an intranet host (handasa-archive), so it is not exposed.
    return Mapped(stage, severity, text(a.get("ktovet")), dates, title=text(a.get("sug_bakasha")))


def _map_street_licenses(a: Attrs, now: datetime) -> Mapped:  # layer 433
    start = epoch_ms(a.get("tr_tchilat_avoda_befoal")) or epoch_ms(a.get("tr_tokef_rishayon_me"))
    end = epoch_ms(a.get("tr_sium_avoda_metuchnan")) or epoch_ms(a.get("tr_tokef_rishayon_ad"))
    return Mapped(stage_from_window(start, end, now), Severity.MEDIUM, text(a.get("t_rechov")),
                  _dates(start=start, end=end), title=text(a.get("ms_rishayon")))


def _map_public_night_works(a: Attrs, now: datetime) -> Mapped:  # layer 858
    start = epoch_ms(a.get("tr_avodat_laila_me"))
    end = epoch_ms(a.get("tr_avodat_laila_ad"))
    street = text(a.get("t_rechov"))
    frm, to = text(a.get("t_rechov_me")), text(a.get("t_rechov_ad"))
    if street and frm and to:
        street = f"{street} ({frm} – {to})"
    return Mapped(stage_from_window(start, end, now), Severity.HIGH, street,
                  _dates(start=start, end=end), title=text(a.get("display_label")))


def _map_site_night_works(a: Attrs, now: datetime) -> Mapped:  # layer 479
    start, end = dmy(a.get("tr_hatchala")), dmy(a.get("tr_siyum"))
    return Mapped(stage_from_window(start, end, now), Severity.HIGH, text(a.get("ktovet")),
                  _dates(start=start, end=end), title=text(a.get("mahut")))


# --- per-layer mappings: long horizon (v2) ------------------------------------------

_PLAN_STAGES = {
    "בתכנון": Stage.PROPOSED,
    "בתוקף": Stage.APPROVED,
    "מבוטלת": Stage.CANCELLED,  # includes shelved (גנוזה) and historic plans
}
SITE_SCOPES = ("נקודתית", "רב נקודתית", "מתחמית")
BROAD_SCOPES = ("אזורית", "כלל עירונית", "ארצית")
_RENEWAL_TOKENS = ("התחדשות", "פינוי")
_LOW_PLAN_SUBJECTS = ("מדיניות", "תרשצ")  # policy docs; re-parcellation (no construction)

# Optional presets for ``query("plans", where=...)``.
PLANS_SITE_SCOPED_WHERE = "t_hekef IN ('נקודתית','רב נקודתית','מתחמית')"
PLANS_LIVE_WHERE = ("(t_status_klali='בתכנון' OR "
                    "(t_status_klali='בתוקף' AND tr_matan_tokef > DATE '2020-01-01'))")


def _map_plans(a: Attrs, now: datetime) -> Mapped:  # layer 528
    stage = _PLAN_STAGES.get(text(a.get("t_status_klali")) or "", Stage.UNKNOWN)
    title = text(a.get("shem_taba"))
    scope = text(a.get("t_hekef"))
    subject = text(a.get("sug_nose"))
    detail = text(a.get("t_status")) or ""
    renewal = a.get("sw_hitchadshut_ironit") == 1 or any(t in (title or "") for t in _RENEWAL_TOKENS)
    if subject in _LOW_PLAN_SUBJECTS or scope in BROAD_SCOPES:
        severity = Severity.LOW
    elif scope in SITE_SCOPES and (renewal or detail.startswith("אישור תוכנית עיצוב בינוי")):
        severity = Severity.HIGH
    else:
        severity = Severity.MEDIUM  # site scope, or unknown scope (not silently dropped)
    # taarichPticha was dropped from the layer on 2026-11-24 (api_docs CHANGELOG); still read
    # if it comes back, but no longer an expected field.
    dates = _dates(opened=epoch_ms(a.get("taarichPticha")),
                   deposited=epoch_ms(a.get("tr_hafkada")),
                   approved=epoch_ms(a.get("tr_matan_tokef")),
                   status_changed=epoch_ms(a.get("tr_shinuy_status")))
    number = text(a.get("taba"))
    full_title = f"{number}: {title}" if number and title else (title or number)
    return Mapped(stage, severity, None, dates, title=full_title,
                  url=url(a.get("url_documents")) or url(a.get("url_iplan")),
                  tags=frozenset({TAG_URBAN_RENEWAL}) if renewal else frozenset())


def _map_dangerous_buildings(a: Attrs, now: datetime) -> Mapped:  # layer 591
    order = text(a.get("t_tzav")) or ""
    tags = set()
    if "סעיף 4" in order or "הריסה" in order:
        tags.add(TAG_DEMOLITION_ORDER)
    if order.startswith("צו 7"):
        tags.add(TAG_IMMEDIATE_ORDER)
    severity = Severity.HIGH if tags else Severity.MEDIUM
    return Mapped(Stage.ORDERED, severity, text(a.get("t_ktovet")), {}, title=order or None,
                  tags=frozenset(tags))


def _map_tama38_zones(a: Attrs, now: datetime) -> Mapped:  # layer 801
    name, desc = text(a.get("zone_name")), text(a.get("zone_desc"))
    title = f"{desc} ({name})" if desc and name else (desc or name)
    return Mapped(Stage.APPROVED, Severity.LOW, None, {}, title=title, url=url(a.get("document")))


_AFFORDABLE_STAGES = {
    "בתכנון לפני הפקדה": Stage.PROPOSED,
    "בתכנון אחרי הפקדה": Stage.PROPOSED,
    "תכנית בתוקף": Stage.APPROVED,
    "תכנית מתאר בתוקף": Stage.APPROVED,
    "ברישוי": Stage.REQUESTED,
    "בבניה": Stage.ACTIVE,
    "מאוכלס": Stage.COMPLETED,
}


def _map_affordable_housing(a: Attrs, now: datetime) -> Mapped:  # layer 746
    stage = _AFFORDABLE_STAGES.get(text(a.get("Status")) or "", Stage.UNKNOWN)
    return Mapped(stage, Severity.MEDIUM, text(a.get("Address")), {},
                  title=text(a.get("Project_Name")))


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
                         "open_request", "permission_date", "tr_hathalat_bniya", "addresses",
                         "finished", "occupation",
                         "sw_tama_38_chadash", "sw_tama_38_tosefet", "url_hadmaya"})),
    LayerSpec("sites", 499, "אתרי בניה", Kind.BUILDING, "oid_site", _map_sites,
              frozenset({"matzav_bniya", "sug_bakasha", "sivug", "tr_tchilat_avoda",
                         "tr_status_pikuach", "ktovet"})),
    LayerSpec("street_licenses", 433, "רישיונות בביצוע", Kind.INFRASTRUCTURE, "oid",
              _map_street_licenses,
              frozenset({"tr_tchilat_avoda_befoal", "tr_tokef_rishayon_me",
                         "tr_sium_avoda_metuchnan", "tr_tokef_rishayon_ad", "t_rechov",
                         "ms_rishayon"})),
    LayerSpec("public_night_works", 858, "עבודות לילה – במרחב הציבורי", Kind.INFRASTRUCTURE,
              "OBJECTID", _map_public_night_works,
              frozenset({"tr_avodat_laila_me", "tr_avodat_laila_ad", "t_rechov",
                         "t_rechov_me", "t_rechov_ad", "display_label"})),
    LayerSpec("site_night_works", 479, "עבודות לילה – אתרי בנייה", Kind.BUILDING, "oid_avoda",
              _map_site_night_works,
              frozenset({"tr_hatchala", "tr_siyum", "ktovet", "mahut"})),
    LayerSpec("plans", 528, "תוכניות בניין עיר", Kind.PLAN, "oid_taba", _map_plans,
              frozenset({"t_status_klali", "t_status", "shem_taba", "taba", "t_hekef",
                         "sug_nose", "sw_hitchadshut_ironit", "tr_hafkada",
                         "tr_matan_tokef", "tr_shinuy_status", "url_documents", "url_iplan"})),
    LayerSpec("dangerous_buildings", 591, "מבנים מסוכנים", Kind.HAZARD, "oid_mivne",
              _map_dangerous_buildings, frozenset({"t_tzav", "t_ktovet"})),
    LayerSpec("tama38_zones", 801, 'מדיניות תמ"א 38 – מפתח אזורים', Kind.POLICY, "oid_zone",
              _map_tama38_zones, frozenset({"zone_name", "zone_desc", "document"})),
    LayerSpec("affordable_housing", 746, "פרויקטים דיור בהישג יד", Kind.BUILDING, "oid",
              _map_affordable_housing, frozenset({"Status", "Address", "Project_Name"})),
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
