"""Tel Aviv-Yafo municipal GIS (gisn.tel-aviv.gov.il): construction features near a point.

Utility library, not a dataset: a typed async ``query()`` over the city's ArcGIS REST
layers (building permits, permit requests, sites, street works, night works).

    from aptbot.scrapers.tel_aviv_gis import TelAvivGisClient
    async with TelAvivGisClient() as gis:
        feats = await gis.query("permits", geometry=(32.0754, 34.7748), distance_m=200)

Library contract: ./docs/SPEC.md
Remote API reference: ./api_docs/README.md
"""
from aptbot.scrapers.tel_aviv_gis.client import TelAvivGisClient, TelAvivGisError, query
from aptbot.scrapers.tel_aviv_gis.layers import LAYERS, LayerSpec, get_layer
from aptbot.scrapers.tel_aviv_gis.models import ConstructionFeature, Kind, Severity, Stage

__all__ = [
    "TelAvivGisClient", "TelAvivGisError", "query",
    "LAYERS", "LayerSpec", "get_layer",
    "ConstructionFeature", "Kind", "Severity", "Stage",
]
