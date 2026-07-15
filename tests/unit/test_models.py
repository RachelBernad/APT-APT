"""models.py: feature params, name normalization, filter/target dataclasses."""
import models
from models import (FEATURES, PROPERTY_CONDITIONS, PROPERTY_TYPES,
                    GatewayFilters, LocationTarget, feature_param, normalize_name)


def test_feature_param_known_and_unknown():
    assert feature_param("elevator") == "elevator"
    assert feature_param("mamad") == "shelter"          # key != gateway param
    assert feature_param("ac") == "airConditioner"
    assert feature_param("does_not_exist") is None


def test_every_feature_has_a_gateway_param():
    for key in FEATURES:
        assert feature_param(key), f"{key} missing gateway param"


def test_normalize_name_strips_space_and_casefolds():
    assert normalize_name("  Florentin  ") == "florentin"
    # collapses all internal whitespace
    assert normalize_name("הצפון הישן") == normalize_name("הצפוןהישן")


def test_normalize_name_kiryat_variants_unify():
    # 'קריית' is rewritten to 'קרית' so the two spellings match.
    assert normalize_name("קריית ביאליק") == normalize_name("קרית ביאליק")


def test_property_maps_are_ints_to_hebrew():
    assert PROPERTY_TYPES[1] == "דירה"
    assert all(isinstance(k, int) for k in PROPERTY_TYPES)
    assert all(isinstance(k, int) for k in PROPERTY_CONDITIONS)


def test_location_target_city_id_default_is_none_and_stays_string():
    t = LocationTarget(level="city", region_id=2, city_id="0070")
    assert t.city_id == "0070" and isinstance(t.city_id, str)
    assert LocationTarget(level="region", region_id=1).city_id is None


def test_gateway_filters_defaults():
    f = GatewayFilters()
    assert f.features == [] and f.property_ids is None and f.min_price is None
