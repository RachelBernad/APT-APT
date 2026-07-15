"""locations.py: string city-id invariant, bundled catalog, autocomplete parsing.

INVARIANT #1: Yad2 city ids are 4-char zero-padded (and sometimes alphanumeric)
STRINGS. They must never be int()-ed.
"""
import pytest

import locations
from models import ResolvedLocation


# --- _str_id: the string-preserving id coercion ---------------------------

def test_str_id_preserves_leading_zeros_and_alnum():
    assert locations._str_id("0070") == "0070"      # Ashdod
    assert locations._str_id("5000") == "5000"      # Tel Aviv
    assert locations._str_id("103P") == "103P"      # alphanumeric id
    assert locations._str_id(70) == "70"            # never zero-pads, but never int-drops either
    assert locations._str_id(" 5000 ") == "5000"    # trims


def test_str_id_blank_is_none():
    assert locations._str_id(None) is None
    assert locations._str_id("") is None


# --- bundled catalog ------------------------------------------------------

def test_popular_cities_return_string_ids():
    cities = locations.popular_cities()
    assert cities, "expected some popular cities in the bundled catalog"
    for name, region_id, city_id in cities:
        assert isinstance(region_id, int)
        assert isinstance(city_id, str), f"{name}: city_id must stay a string"
    ids = {c[2] for c in cities}
    assert "5000" in ids  # Tel Aviv-Yafo present, as a string


def test_city_name_lookup_by_padded_id():
    assert locations.city_name("0070") == "אשדוד"
    assert locations.city_name("5000") == "תל אביב יפו"
    # unknown id falls back to the id itself
    assert locations.city_name("zzzz") == "zzzz"


def test_load_catalog_shape():
    cat = locations.load_catalog()
    assert "cities" in cat and cat["cities"]
    # leading-zero and alphanumeric ids both exist as keys
    assert "0070" in cat["cities"]
    assert any(not k.isdigit() for k in cat["cities"]), "expected an alphanumeric city id"


# --- query normalization (English/typo -> Hebrew) -------------------------

def test_normalize_query_alias_and_fuzzy():
    assert locations.normalize_query("tel aviv") == "תל אביב"
    assert locations.normalize_query("ashdod") == "אשדוד"
    # fuzzy: a small typo still resolves via rapidfuzz
    assert locations.normalize_query("herzliyaa") == "הרצליה"
    # already-Hebrew passes through untouched
    assert locations.normalize_query("רמת גן") == "רמת גן"


# --- autocomplete parsing (_flatten) --------------------------------------

def _payload():
    return {
        "cities": [
            {"regionId": 3, "areaId": 1, "cityId": "5000", "fullTitleText": "תל אביב יפו"},
            {"regionId": 2, "areaId": 21, "cityId": "0070", "fullTitleText": "אשדוד"},
        ],
        "hoods": [
            {"regionId": 3, "areaId": 1, "cityId": "5000", "hoodId": 1483,
             "fullTitleText": "פלורנטין, תל אביב יפו"},
        ],
        "areas": [{"regionId": 2, "areaId": 21, "fullTitleText": "אשדוד והסביבה"}],
        "streets": [
            {"regionId": 3, "areaId": 1, "cityId": "5000", "streetId": 900,
             "fullTitleText": "דיזנגוף, תל אביב יפו"},
        ],
        "regions": [{"regionId": 3, "fullTitleText": "תל אביב"}],
    }


def test_flatten_keeps_city_id_as_string():
    resolved = locations._flatten(_payload())
    cities = {r.display: r for r in resolved if r.level == "city"}
    assert cities["תל אביב יפו"].city_id == "5000"
    assert cities["אשדוד"].city_id == "0070"        # leading zero preserved
    assert isinstance(cities["אשדוד"].city_id, str)


def test_flatten_hood_and_street_match_name_first_segment():
    resolved = locations._flatten(_payload())
    hood = next(r for r in resolved if r.level == "hood")
    assert hood.match_name == "פלורנטין"            # first comma segment
    assert hood.city_id == "5000"
    street = next(r for r in resolved if r.level == "street")
    assert street.match_name == "דיזנגוף"
    assert street.street_id == 900


async def test_resolve_candidates_ranks_and_uses_string_ids(monkeypatch):
    async def fake_autocomplete(http, text_he):
        assert text_he == "תל אביב"     # english was translated first
        return _payload()

    monkeypatch.setattr(locations, "autocomplete", fake_autocomplete)
    out = await locations.resolve_candidates(None, "tel aviv", limit=8)
    assert out, "expected candidates"
    # every resolved city id stays a string; the Tel Aviv city ("5000") is present
    city = next(r for r in out if r.level == "city" and r.display == "תל אביב יפו")
    assert city.city_id == "5000" and isinstance(city.city_id, str)


async def test_batch_autocomplete_unions_and_dedupes(monkeypatch):
    async def fake_autocomplete(http, text_he):
        return _payload()   # same payload for every term

    monkeypatch.setattr(locations, "autocomplete", fake_autocomplete)
    out = await locations.batch_autocomplete(None, ["tel aviv", "ashdod"])
    # two identical payloads unioned -> deduped to the distinct candidates of one
    keys = {(r.level, r.city_id, r.display) for r in out}
    assert len(keys) == len(out)   # no dupes
    assert any(r.city_id == "0070" for r in out)


# --- target builders ------------------------------------------------------

def test_target_from_resolved_hood_sets_normalized_match_name():
    loc = ResolvedLocation(level="hood", display="פלורנטין, תל אביב יפו", region_id=3,
                           area_id=1, city_id="5000", hood_id=1483, match_name="פלורנטין")
    from models import normalize_name
    t = locations.target_from_resolved(loc)
    assert t.city_id == "5000"
    assert t.match_name == normalize_name("פלורנטין")


def test_make_hood_target_and_hood_target_keep_string_city_id():
    t = locations.make_hood_target(region_id=3, city_id="5000", hood_name="פלורנטין",
                                   city_display="תל אביב יפו")
    assert t.level == "hood" and t.city_id == "5000"
    assert t.display_name == "פלורנטין, תל אביב יפו"

    catalog = {"regionId": "2", "areaId": "21", "cityId": "0070"}
    ht = locations.hood_target(catalog, "רובע ט", city_display="אשדוד")
    assert ht.city_id == "0070" and ht.region_id == 2
