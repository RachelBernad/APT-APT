"""aptbot.scrapers.tel_aviv_gis.catalog: drift vs outage reporting (offline).

INVARIANTS (api_docs/transport/http-and-auth.md#outages):
- a layer whose metadata loads but whose `1=1` count fails is UNAVAILABLE, not a crash;
- exit 0 = clean, 1 = DRIFT (wins over outages), 3 = only UNAVAILABLE layers.
"""
import pytest

from aptbot.scrapers.tel_aviv_gis import catalog
from aptbot.scrapers.tel_aviv_gis.layers import LAYERS


def _info(spec, drop=()):
    fields = (spec.expected_fields | {spec.oid_field}) - set(drop)
    return {"id": spec.layer_id, "name": spec.name_he, "geometryType": "esriGeometryPolygon",
            "fields": [{"name": f} for f in fields]}


@pytest.fixture
def fake_server(monkeypatch):
    state = {"down": set(), "drop": {}}

    async def _get(session, url, **params):
        if url.endswith("/layers"):
            return {"layers": [_info(s, state["drop"].get(s.layer_id, ())) for s in LAYERS.values()]}
        layer_id = int(url.rstrip("/").split("/")[-2])
        if layer_id in state["down"]:
            raise RuntimeError(f"{url}: {{'code': 400, 'message': 'Failed to execute query.'}}")
        if params.get("returnCountOnly"):
            return {"count": 3}
        return {"features": [{"attributes": {"date_import": "24/11/2026 01:00:00"}}]}

    monkeypatch.setattr(catalog, "_get", _get)
    return state


async def test_clean_service_exits_0(fake_server, capsys):
    assert await catalog.main([]) == 0
    assert "no drift" in capsys.readouterr().out


async def test_failing_layer_is_unavailable_not_a_crash(fake_server, capsys):
    fake_server["down"].add(528)
    assert await catalog.main([]) == catalog.EXIT_UNAVAILABLE
    out = capsys.readouterr().out
    assert "UNAVAILABLE 528 (plans)" in out and "n=None" in out
    assert " 772 permits" in out  # the other layers are still reported


async def test_drift_wins_over_unavailable(fake_server, capsys):
    fake_server["down"].add(528)
    fake_server["drop"][772] = ("building_stage",)
    assert await catalog.main([]) == 1
    assert "DRIFT missing fields: building_stage" in capsys.readouterr().out
