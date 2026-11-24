"""aptbot.scrapers.tel_aviv_gis.layers: Kind/Stage/Severity mapping per layer (offline).

Attribute samples are trimmed copies of real 2026-11-23 responses
(see aptbot/scrapers/tel_aviv_gis/api_docs/services/IView2WM/layers/).
"""
from datetime import datetime, timezone

import pytest

from aptbot.scrapers.tel_aviv_gis.layers import (LAYERS, get_layer, severity_from_type,
                                                 stage_from_window)
from aptbot.scrapers.tel_aviv_gis.models import Kind, Severity, Stage

NOW = datetime(2026, 11, 23, 12, 0, tzinfo=timezone.utc)
MS = lambda y, m, d: int(datetime(y, m, d, tzinfo=timezone.utc).timestamp() * 1000)  # noqa: E731


def _map(key, attrs, now=NOW):
    return LAYERS[key].mapper(attrs, now)


# --- registry ----------------------------------------------------------------------

def test_registry_lookup_by_key_and_id():
    assert get_layer("permits").layer_id == 772
    assert get_layer(499).key == "sites"
    assert {s.layer_id for s in LAYERS.values()} == {772, 499, 433, 858, 479, 528, 591, 801, 746}


def test_unsupported_layer_raises_with_supported_list():
    with pytest.raises(KeyError, match="permits"):
        get_layer(852)  # roadworks: deliberately excluded


def test_kinds():
    assert LAYERS["street_licenses"].kind is Kind.INFRASTRUCTURE
    assert LAYERS["public_night_works"].kind is Kind.INFRASTRUCTURE
    assert LAYERS["site_night_works"].kind is Kind.BUILDING


# --- severity tokens ---------------------------------------------------------------

@pytest.mark.parametrize("sug, expected", [
    ('בניה חדשה בנייה חדשה תמ"א 38', Severity.HIGH),
    ("בניה חדשה/חפירה ו/או דיפון", Severity.HIGH),
    ("תוספות בניה הריסה", Severity.HIGH),
    ('תוספות בניה תוספת בניה לפי תמ"א 38', Severity.MEDIUM),
    ("שינויים חידוש היתר", Severity.MEDIUM),              # renewal = medium (user choice)
    ("שינויים שינוי ללא תוספת שטח/חזית", Severity.LOW),
    ("שימוש חורג שמוש חורג לעסקים", Severity.LOW),
    ("", Severity.MEDIUM),                                # unknown type is not dropped
    (None, Severity.MEDIUM),
])
def test_severity_from_type(sug, expected):
    assert severity_from_type(sug) is expected


def test_severity_highest_token_wins():
    assert severity_from_type("שינויים חידוש היתר , בניה חדשה מרתפים") is Severity.HIGH


def test_severity_normalises_double_yod_spelling():
    assert severity_from_type("בנייה חדשה") is Severity.HIGH


# --- date windows --------------------------------------------------------------------

def test_window_stages_and_inclusive_end_day():
    start, end = datetime(2026, 11, 1, tzinfo=timezone.utc), datetime(2026, 11, 23, tzinfo=timezone.utc)
    assert stage_from_window(start, end, NOW) is Stage.ACTIVE  # end day itself still active
    assert stage_from_window(start, end, datetime(2026, 11, 24, 1, tzinfo=timezone.utc)) is Stage.COMPLETED
    assert stage_from_window(start, end, datetime(2026, 10, 1, tzinfo=timezone.utc)) is Stage.PERMITTED
    assert stage_from_window(None, None, NOW) is Stage.UNKNOWN


# --- 772 permits -----------------------------------------------------------------------

PERMIT = {
    "oid_permit": 2, "building_stage": "בתהליך היתר", "open_request": 1574940120000,
    "sug_bakasha": 'תוספות בניה תוספת בניה לפי תמ"א 38', "yechidot_diyur": 0,
    "addresses": "המלך ג'ורג' 66", "expiry_date": None, "permission_date": None,
}


@pytest.mark.parametrize("raw, stage", [
    ("בתהליך היתר", Stage.REQUESTED), ("קיים היתר", Stage.PERMITTED), ("בבניה", Stage.ACTIVE),
    ("קיימת לפחות תעודת גמר אחת", Stage.COMPLETED), ("קיים אכלוס", Stage.COMPLETED),
    ("ערך חדש", Stage.UNKNOWN),
])
def test_permit_stages(raw, stage):
    assert _map("permits", {**PERMIT, "building_stage": raw})[0] is stage


def test_permit_expired_before_start_is_lapsed():
    attrs = {**PERMIT, "building_stage": "קיים היתר", "expiry_date": MS(2025, 1, 1)}
    assert _map("permits", attrs)[0] is Stage.LAPSED


# building_stage is building-level: an issued permit reads "בתהליך היתר" once a newer
# request is filed on the same building. The record's own dates decide (api_docs 772 page).
ISSUED_UNDER_NEWER_REQUEST = {**PERMIT, "building_stage": "בתהליך היתר",
                              "permission_date": MS(2024, 3, 4), "expiry_date": MS(2030, 3, 4)}


def test_issued_permit_under_a_newer_request_is_permitted_not_requested():
    assert _map("permits", ISSUED_UNDER_NEWER_REQUEST).stage is Stage.PERMITTED


def test_issued_and_started_permit_under_a_newer_request_is_active():
    attrs = {**ISSUED_UNDER_NEWER_REQUEST, "tr_hathalat_bniya": MS(2024, 10, 15)}
    assert _map("permits", attrs).stage is Stage.ACTIVE


def test_start_date_in_the_future_is_not_active_yet():
    attrs = {**ISSUED_UNDER_NEWER_REQUEST, "tr_hathalat_bniya": MS(2027, 1, 1)}
    assert _map("permits", attrs).stage is Stage.PERMITTED


def test_issued_unstarted_expired_permit_under_a_newer_request_is_lapsed():
    attrs = {**ISSUED_UNDER_NEWER_REQUEST, "expiry_date": MS(2025, 1, 1)}
    assert _map("permits", attrs).stage is Stage.LAPSED


def test_issued_permit_with_completion_under_a_newer_request_is_completed():
    attrs = {**ISSUED_UNDER_NEWER_REQUEST, "tr_hathalat_bniya": MS(2020, 1, 1),
             "finished": "01/02/2023"}
    assert _map("permits", attrs).stage is Stage.COMPLETED


def test_unissued_request_stays_requested_even_with_a_building_start_date():
    attrs = {**PERMIT, "tr_hathalat_bniya": MS(2024, 10, 15)}
    assert _map("permits", attrs).stage is Stage.REQUESTED


def test_active_site_with_past_expiry_stays_active():
    attrs = {**PERMIT, "building_stage": "בבניה", "expiry_date": MS(2025, 1, 1)}
    assert _map("permits", attrs)[0] is Stage.ACTIVE


def test_permit_unit_count_bumps_severity():
    assert _map("permits", PERMIT)[1] is Severity.MEDIUM
    assert _map("permits", {**PERMIT, "yechidot_diyur": 7})[1] is Severity.HIGH


def test_permit_address_and_dates():
    m = _map("permits", PERMIT)
    address, dates = m.address, m.dates
    assert address == "המלך ג'ורג' 66"
    assert dates == {"requested": datetime(2019, 11, 28, 11, 22, tzinfo=timezone.utc)}


# --- 499 sites -------------------------------------------------------------------------

SITE = {
    "oid_site": 2, "matzav_bniya": "תחילת עבודות", "sivug": "הארכת תוקף החלטה ",
    "sug_bakasha": "בניה חדשה/בניין מגורים לא גבוה (עד 13 מ') ,שינויים-מהיר/הארכת תוקף החלטה",
    "ktovet": "רחוב בוגרשוב   מס' 57 ", "tr_tchilat_avoda": 1785715200000,
}


@pytest.mark.parametrize("raw, stage", [
    ("פתיחת תיק", Stage.PERMITTED), ("פיצול תיק לבניה בשלבים", Stage.PERMITTED),
    ("תחילת עבודות", Stage.ACTIVE), ("עבודות עפר וביסוס", Stage.ACTIVE),
    ("גמר שלד", Stage.ACTIVE), ("גמר בניה", Stage.COMPLETED),
    ("", Stage.COMPLETED), (None, Stage.COMPLETED),  # empty = occupancy/completion issued
])
def test_site_stages(raw, stage):
    assert _map("sites", {**SITE, "matzav_bniya": raw})[0] is stage


def test_site_severity_and_whitespace_cleanup():
    m = _map("sites", SITE)
    severity, address, dates = m.severity, m.address, m.dates
    assert severity is Severity.HIGH
    assert address == "רחוב בוגרשוב מס' 57"
    assert set(dates) == {"works_approved"}


# --- 433 / 858 / 479 date-driven layers -----------------------------------------------

def test_street_license_prefers_actual_start_and_planned_end():
    attrs = {"oid": 1, "t_rechov": 'רש"י (רחוב)', "tr_tokef_rishayon_me": MS(2024, 9, 3),
             "tr_tokef_rishayon_ad": MS(2027, 1, 4), "tr_sium_avoda_metuchnan": MS(2026, 11, 1)}
    m = _map("street_licenses", attrs)
    stage, severity, address = m.stage, m.severity, m.address
    assert stage is Stage.COMPLETED  # planned end 2026-11-01 beats licence validity
    assert severity is Severity.MEDIUM and address == 'רש"י (רחוב)'
    assert _map("street_licenses", {**attrs, "tr_sium_avoda_metuchnan": None})[0] is Stage.ACTIVE


def test_public_night_works_range_address():
    attrs = {"OBJECTID": 1, "t_rechov": "לבון פנחס", "t_rechov_me": "בן צבי",
             "t_rechov_ad": "קבוץ גלויות", "tr_avodat_laila_me": 1792281600000,
             "tr_avodat_laila_ad": 1795651200000, "natzig": "someone, 050"}
    m = _map("public_night_works", attrs)
    stage, severity, address = m.stage, m.severity, m.address
    assert stage is Stage.ACTIVE and severity is Severity.HIGH
    assert address == "לבון פנחס (בן צבי – קבוץ גלויות)"


def test_site_night_works_parses_string_dates():
    attrs = {"oid_avoda": 2, "tr_hatchala": "23/11/2026", "tr_siyum": "23/11/2026",
             "ktovet": "רחוב אלון יגאל מס' 114"}
    m = _map("site_night_works", attrs)
    stage, severity, dates = m.stage, m.severity, m.dates
    assert stage is Stage.ACTIVE and severity is Severity.HIGH
    assert dates["start"].date().isoformat() == "2026-11-23"
    assert _map("site_night_works", {**attrs, "tr_hatchala": "garbage", "tr_siyum": None})[0] is Stage.UNKNOWN


# --- 772 TAMA 38 tags (v2) -------------------------------------------------------------

def test_permit_tama38_tags_and_links():
    m = _map("permits", {**PERMIT, "sw_tama_38_chadash": "כן", "sw_tama_38_tosefet": "לא",
                         "url_hadmaya": "https://archive-binyan.tel-aviv.gov.il/pages/results.aspx?x=1"})
    assert m.tags == {"tama38_rebuild"}
    assert m.title == 'תוספות בניה תוספת בניה לפי תמ"א 38'
    assert m.url.startswith("https://archive-binyan")
    assert _map("permits", {**PERMIT, "sw_tama_38_tosefet": "כן"}).tags == {"tama38_addition"}
    assert _map("permits", PERMIT).tags == frozenset()


# --- 528 plans -------------------------------------------------------------------------

PLAN = {
    "oid_taba": 2546, "taba": "4406                ", "shem_taba": "התחדשות ברמת אביב ב",
    "t_status_klali": "בתכנון", "t_status": "לפני ועדה מקומית", "t_hekef": "מתחמית",
    "sug_nose": 'תב"ע', "maslul": "ועדה מקומית", "sw_hitchadshut_ironit": 0,
    "taarichPticha": MS(2018, 5, 1), "tr_hafkada": None, "tr_matan_tokef": None,
    "url_documents": "https://gisn.tel-aviv.gov.il/tabaot/docs.aspx?id_taba=6824&st_taba=4406&mode=internet",
    "url_iplan": None,
}


@pytest.mark.parametrize("general, stage", [
    ("בתכנון", Stage.PROPOSED), ("בתוקף", Stage.APPROVED), ("מבוטלת", Stage.CANCELLED),
    ("", Stage.UNKNOWN), (None, Stage.UNKNOWN),
])
def test_plan_stages(general, stage):
    assert _map("plans", {**PLAN, "t_status_klali": general}).stage is stage


def test_plan_renewal_by_title_is_high_and_tagged():
    m = _map("plans", PLAN)
    assert m.severity is Severity.HIGH and m.tags == {"urban_renewal"}
    assert m.title == "4406: התחדשות ברמת אביב ב"  # plan number's trailing spaces stripped
    assert m.url.endswith("mode=internet") and m.address is None
    assert set(m.dates) == {"opened"}


def test_plan_renewal_flag_without_title_keyword():
    m = _map("plans", {**PLAN, "shem_taba": "יהודה הלוי 3", "sw_hitchadshut_ironit": 1})
    assert m.tags == {"urban_renewal"} and m.severity is Severity.HIGH


def test_plan_design_approval_at_site_scope_is_high():
    attrs = {**PLAN, "shem_taba": "תכנית עיצוב למתחם אילת", "t_status_klali": "בתוקף",
             "t_status": "אישור תוכנית עיצוב בינוי מקומית", "t_hekef": "נקודתית"}
    m = _map("plans", attrs)
    assert m.stage is Stage.APPROVED and m.severity is Severity.HIGH and m.tags == frozenset()


@pytest.mark.parametrize("override, severity", [
    ({"shem_taba": "יהודה הלוי 3", "t_hekef": "נקודתית"}, Severity.MEDIUM),  # site, no renewal
    ({"shem_taba": "יהודה הלוי 3", "t_hekef": ""}, Severity.MEDIUM),         # unknown scope kept
    ({"t_hekef": "אזורית"}, Severity.LOW),                                  # broad scope = noise
    ({"t_hekef": "כלל עירונית"}, Severity.LOW),
    ({"sug_nose": "מדיניות"}, Severity.LOW),                                # policy document
    ({"sug_nose": "תרשצ"}, Severity.LOW),                                   # re-parcellation
])
def test_plan_severity_scope_and_subject(override, severity):
    assert _map("plans", {**PLAN, **override}).severity is severity


def test_plan_url_falls_back_to_iplan():
    m = _map("plans", {**PLAN, "url_documents": None,
                       "url_iplan": "https://mavat.iplan.gov.il/SV4/1/5000281091/310"})
    assert m.url.startswith("https://mavat.iplan.gov.il")


def test_plan_presets_are_valid_sql_fragments():
    from aptbot.scrapers.tel_aviv_gis import PLANS_LIVE_WHERE, PLANS_SITE_SCOPED_WHERE
    assert "t_hekef IN" in PLANS_SITE_SCOPED_WHERE
    assert PLANS_LIVE_WHERE.count("(") == PLANS_LIVE_WHERE.count(")")


# --- 591 dangerous buildings ------------------------------------------------------------

@pytest.mark.parametrize("order, severity, tags", [
    ("צו 3 - בדיקת מהנדס פרטי", Severity.MEDIUM, set()),
    ("הודעה לפי סעיף 4 - הריסה", Severity.HIGH, {"demolition_order"}),
    ("הודעה לפי סעיף 4 - אטימה", Severity.HIGH, {"demolition_order"}),
    ("צו 7 - ביצוע עבודות מיידי", Severity.HIGH, {"immediate_order"}),
    ("צו 7 - הריסה מיידית", Severity.HIGH, {"immediate_order", "demolition_order"}),
])
def test_dangerous_building_orders(order, severity, tags):
    m = _map("dangerous_buildings", {"oid_mivne": 1, "t_tzav": order,
                                     "t_ktovet": "רחוב נחלת בנימין מס' 105"})
    assert m.stage is Stage.ORDERED and m.severity is severity and m.tags == tags
    assert m.title == order and m.address == "רחוב נחלת בנימין מס' 105"


# --- 801 TAMA 38 zones / 746 affordable housing -----------------------------------------

def test_tama38_zone_is_low_policy_with_document():
    m = _map("tama38_zones", {"oid_zone": 1, "zone_desc": "שיכון ל'", "zone_name": "30",
                              "document": "https://gisn.tel-aviv.gov.il/taba_raster/9086_MD.pdf"})
    assert LAYERS["tama38_zones"].kind is Kind.POLICY
    assert (m.stage, m.severity, m.title) == (Stage.APPROVED, Severity.LOW, "שיכון ל' (30)")
    assert m.url.endswith(".pdf")


@pytest.mark.parametrize("status, stage", [
    ("בתכנון לפני הפקדה", Stage.PROPOSED), ("בתכנון אחרי הפקדה", Stage.PROPOSED),
    ("תכנית בתוקף", Stage.APPROVED), ("ברישוי", Stage.REQUESTED), ("בבניה", Stage.ACTIVE),
    ("מאוכלס", Stage.COMPLETED), ("משהו חדש", Stage.UNKNOWN),
])
def test_affordable_housing_stages(status, stage):
    m = _map("affordable_housing", {"oid": 1, "Project_Name": "המשתלה", "Status": status,
                                    "Address": "משה וילנסקי 13"})
    assert m.stage is stage and m.title == "המשתלה" and m.address == "משה וילנסקי 13"


def test_non_http_links_are_dropped():
    m = _map("tama38_zones", {"oid_zone": 1, "zone_desc": "x", "document": "file://intranet/x.pdf"})
    assert m.url is None
