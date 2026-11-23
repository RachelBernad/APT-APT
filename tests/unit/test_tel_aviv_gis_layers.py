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
    assert {s.layer_id for s in LAYERS.values()} == {772, 499, 433, 858, 479}


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


def test_active_site_with_past_expiry_stays_active():
    attrs = {**PERMIT, "building_stage": "בבניה", "expiry_date": MS(2025, 1, 1)}
    assert _map("permits", attrs)[0] is Stage.ACTIVE


def test_permit_unit_count_bumps_severity():
    assert _map("permits", PERMIT)[1] is Severity.MEDIUM
    assert _map("permits", {**PERMIT, "yechidot_diyur": 7})[1] is Severity.HIGH


def test_permit_address_and_dates():
    _, _, address, dates = _map("permits", PERMIT)
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
    stage, severity, address, dates = _map("sites", SITE)
    assert severity is Severity.HIGH
    assert address == "רחוב בוגרשוב מס' 57"
    assert set(dates) == {"works_approved"}


# --- 433 / 858 / 479 date-driven layers -----------------------------------------------

def test_street_license_prefers_actual_start_and_planned_end():
    attrs = {"oid": 1, "t_rechov": 'רש"י (רחוב)', "tr_tokef_rishayon_me": MS(2024, 9, 3),
             "tr_tokef_rishayon_ad": MS(2027, 1, 4), "tr_sium_avoda_metuchnan": MS(2026, 11, 1)}
    stage, severity, address, dates = _map("street_licenses", attrs)
    assert stage is Stage.COMPLETED  # planned end 2026-11-01 beats licence validity
    assert severity is Severity.MEDIUM and address == 'רש"י (רחוב)'
    assert _map("street_licenses", {**attrs, "tr_sium_avoda_metuchnan": None})[0] is Stage.ACTIVE


def test_public_night_works_range_address():
    attrs = {"OBJECTID": 1, "t_rechov": "לבון פנחס", "t_rechov_me": "בן צבי",
             "t_rechov_ad": "קבוץ גלויות", "tr_avodat_laila_me": 1792281600000,
             "tr_avodat_laila_ad": 1795651200000, "natzig": "someone, 050"}
    stage, severity, address, _ = _map("public_night_works", attrs)
    assert stage is Stage.ACTIVE and severity is Severity.HIGH
    assert address == "לבון פנחס (בן צבי – קבוץ גלויות)"


def test_site_night_works_parses_string_dates():
    attrs = {"oid_avoda": 2, "tr_hatchala": "23/11/2026", "tr_siyum": "23/11/2026",
             "ktovet": "רחוב אלון יגאל מס' 114"}
    stage, severity, _, dates = _map("site_night_works", attrs)
    assert stage is Stage.ACTIVE and severity is Severity.HIGH
    assert dates["start"].date().isoformat() == "2026-11-23"
    assert _map("site_night_works", {**attrs, "tr_hatchala": "garbage", "tr_siyum": None})[0] is Stage.UNKNOWN
