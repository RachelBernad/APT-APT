"""Live catalog / drift check for the Tel Aviv GIS MapServer.

    python -m aptbot.scrapers.tel_aviv_gis.catalog          # supported layers: drift report
    python -m aptbot.scrapers.tel_aviv_gis.catalog --all    # every layer on the service

For each supported layer it reports name, geometry, feature count and latest
``date_import``, plus any field the mapper reads that the server no longer has
(DRIFT) and fields that are new. Any DRIFT line means api_docs/ and layers.py need
updating (see ../CLAUDE.md). An UNAVAILABLE line means the layer's metadata loads but its
data source fails even a ``1=1`` count (a server-side outage).

Exit codes: 0 = all good, 1 = DRIFT, 3 = no drift but some layer UNAVAILABLE
(2 is argparse's usage error).
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from typing import Any, Dict, List

import aiohttp

from aptbot import config
from aptbot.scrapers.tel_aviv_gis.layers import all_layers

EXIT_UNAVAILABLE = 3  # not 2: argparse uses 2 for usage errors


async def _get(session: aiohttp.ClientSession, url: str, **params: Any) -> Dict[str, Any]:
    params.setdefault("f", "json")
    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=120)) as r:
        r.raise_for_status()
        body = await r.json(content_type=None)
    if "error" in body:
        raise RuntimeError(f"{url}: {body['error']}")
    return body


async def _layer_report(session: aiohttp.ClientSession, base: str, spec, info: Dict[str, Any]) -> List[str]:
    url = f"{base}/{spec.layer_id}"
    lines = []
    if not info:
        return [f"DRIFT {spec.layer_id} ({spec.key}): layer missing from service"]
    fields = {f["name"] for f in info.get("fields") or []}
    try:
        count = (await _get(session, f"{url}/query", where="1=1", returnCountOnly="true")).get("count")
    except RuntimeError as exc:
        # Metadata loads but the data source is down (api_docs/transport/http-and-auth.md#outages).
        lines.append(f"     UNAVAILABLE {spec.layer_id} ({spec.key}): `1=1` count fails: {exc}")
        count = None
    latest = None
    if "date_import" in fields and count is not None:
        top = await _get(session, f"{url}/query", where="1=1", outFields="date_import",
                         returnGeometry="false", resultRecordCount=1,
                         orderByFields=f"{spec.oid_field} DESC")
        feats = top.get("features") or []
        latest = feats[0]["attributes"].get("date_import") if feats else None
    lines.insert(0, f"{spec.layer_id:>4} {spec.key:<20} {info.get('name')!s:<32} "
                    f"{info.get('geometryType')!s:<22} n={count!s:<6} date_import={latest}")
    if info.get("name") != spec.name_he:
        lines.append(f"     DRIFT name: expected {spec.name_he!r}")
    missing = sorted(spec.expected_fields - fields)
    if missing:
        lines.append(f"     DRIFT missing fields: {', '.join(missing)}")
    if spec.oid_field not in fields:
        lines.append(f"     DRIFT oid field {spec.oid_field!r} missing")
    return lines


async def main(argv: List[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--all", action="store_true", help="list every layer on the service")
    ap.add_argument("--json", action="store_true", help="with --all: dump raw layer JSON")
    args = ap.parse_args(argv)
    base = config.TLV_GIS_MAPSERVER.rstrip("/")
    async with aiohttp.ClientSession(headers={"User-Agent": config.USER_AGENT}) as session:
        layers = (await _get(session, f"{base}/layers")).get("layers") or []
        by_id = {l["id"]: l for l in layers}
        if args.all:
            if args.json:
                json.dump(layers, sys.stdout, ensure_ascii=False, indent=1)
                return 0
            for l in layers:
                print(f"{l['id']:>4} {l.get('type', ''):<14} {l.get('geometryType') or '':<22} {l['name']}")
            return 0
        drift = unavailable = False
        for spec in all_layers():
            for line in await _layer_report(session, base, spec, by_id.get(spec.layer_id, {})):
                drift |= "DRIFT" in line
                unavailable |= "UNAVAILABLE" in line
                print(line)
        if drift:
            print("\nDRIFT detected: update api_docs/ and layers.py")
            return 1
        if unavailable:
            print("\nno drift, but some layers are failing server-side (outage, not our code)")
            return EXIT_UNAVAILABLE
        print("\nno drift")
        return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main(sys.argv[1:])))
