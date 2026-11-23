"""Tel Aviv-Yafo municipal GIS (gisn.tel-aviv.gov.il): construction signals near a point.

Utility library, not a dataset: a typed async ``query()`` over the city's ArcGIS REST
layers, from long-horizon signals (town plans, TAMA 38 zones, dangerous-building orders,
affordable-housing projects) to permits, requests, active sites, street and night works.
Everything is labeled; callers pick the layers/stages/severities/tags they care about.

    from aptbot.scrapers.tel_aviv_gis import TelAvivGisClient
    async with TelAvivGisClient() as gis:
        feats = await gis.query("permits", geometry=(32.0754, 34.7748), distance_m=200)

Library contract: ./docs/SPEC.md
Remote API reference: ./api_docs/README.md
"""
from aptbot.scrapers.tel_aviv_gis.client import TelAvivGisClient, TelAvivGisError, query
from aptbot.scrapers.tel_aviv_gis.layers import (LAYERS, PLANS_LIVE_WHERE,
                                                 PLANS_SITE_SCOPED_WHERE, LayerSpec, get_layer)
from aptbot.scrapers.tel_aviv_gis.models import (TAG_DEMOLITION_ORDER, TAG_IMMEDIATE_ORDER,
                                                 TAG_TAMA38_ADDITION, TAG_TAMA38_REBUILD,
                                                 TAG_URBAN_RENEWAL, ConstructionFeature, Kind,
                                                 Severity, Stage)

__all__ = [
    "TelAvivGisClient", "TelAvivGisError", "query",
    "LAYERS", "LayerSpec", "get_layer", "PLANS_SITE_SCOPED_WHERE", "PLANS_LIVE_WHERE",
    "ConstructionFeature", "Kind", "Severity", "Stage",
    "TAG_TAMA38_REBUILD", "TAG_TAMA38_ADDITION", "TAG_URBAN_RENEWAL",
    "TAG_DEMOLITION_ORDER", "TAG_IMMEDIATE_ORDER",
]
