#!/usr/bin/env python3
"""Reproducibly harvest Yad2's location catalog into ``data/``.

Two methods (see api_docs/yad2/harvesting-all-areas.md):
  1. Autocomplete **prefix sweep** (2-letter + 3-letter for truncated parents) →
     nationwide regions / areas / cities with their id chains → data/il_locations.json.
  2. Feed **bBox tiling** for a city → its neighborhood names → data/hoods/<cityId>.json.

Runs on the bot's own cookie'd HTTP path (browser-like headers get past Radware).

Usage:
    python scripts/harvest_locations.py                 # sweep + Tel Aviv hoods
    python scripts/harvest_locations.py --skip-sweep    # only re-harvest hoods
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

import aiohttp

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import config  # noqa: E402
from yad2_gateway import _is_challenge, gateway_headers  # noqa: E402

HEBREW = list("אבגדהוזחטיכלמנסעפצקרשת")

REGION_NAMES = {  # regionId -> name (autocomplete's `regions` rarely returns them all)
    "1": "מרכז והשרון", "2": "דרום", "3": "תל אביב והסביבה",
    "4": "יהודה, שומרון ובקעת הירדן", "5": "מישור החוף הצפוני",
    "6": "ירושלים והסביבה", "7": "צפון והעמקים", "8": "ירושלים",
}

# Tel Aviv-Yafo bBox tiles (lat1,lng1,lat2,lng2) covering 32.01–32.16 N × 34.74–34.85 E.
TLV_TILES = [
    "32.01,34.74,32.08,34.80", "32.01,34.78,32.06,34.83",
    "32.05,34.74,32.10,34.80", "32.05,34.78,32.10,34.83",
    "32.08,34.76,32.13,34.82", "32.10,34.78,32.15,34.84",
    "32.06,34.75,32.10,34.79", "32.02,34.75,32.06,34.79",
    "32.09,34.77,32.13,34.81", "32.11,34.79,32.16,34.85",
]


async def _get_json(http, url, params):
    async with http.get(url, params=params, headers=gateway_headers(),
                        timeout=aiohttp.ClientTimeout(total=config.REQUEST_TIMEOUT)) as resp:
        text = await resp.text()
        if resp.status != 200 or _is_challenge(text, resp.headers.get("content-type", "")):
            return None
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return None


async def sweep(http) -> dict:
    regions, areas, cities = {}, {}, {}
    prefixes = [a + b for a in HEBREW for b in HEBREW]
    truncated = []
    for i, p in enumerate(prefixes):
        d = await _get_json(http, config.YAD2_AUTOCOMPLETE_URL, {"text": p})
        if d:
            _absorb(d, regions, areas, cities)
            if len(d.get("cities") or []) >= 5 or len(d.get("hoods") or []) >= 5:
                truncated.append(p)
        if i % 50 == 0:
            print(f"  sweep {i}/{len(prefixes)} … cities={len(cities)}")
        await asyncio.sleep(0.03)
    # 3-letter expansion for truncated parents
    for p in truncated:
        for c in HEBREW:
            d = await _get_json(http, config.YAD2_AUTOCOMPLETE_URL, {"text": p + c})
            if d:
                _absorb(d, regions, areas, cities)
            await asyncio.sleep(0.02)
    for rid, name in REGION_NAMES.items():
        regions.setdefault(rid, name)
    print(f"  sweep done: {len(regions)} regions, {len(areas)} areas, {len(cities)} cities")
    return {"regions": regions, "areas": areas, "cities": cities}


def _absorb(d, regions, areas, cities):
    for r in d.get("regions") or []:
        if r.get("regionId"):
            regions[r["regionId"]] = r.get("regionHeb") or r.get("fullTitleText")
    for a in d.get("areas") or []:
        if a.get("areaId"):
            areas[a["areaId"]] = {"name": a.get("fullTitleText"), "regionId": a.get("regionId"),
                                  "topAreaId": a.get("topAreaId")}
        if a.get("regionId") and a.get("regionHeb"):
            regions.setdefault(a["regionId"], a["regionHeb"])
    for c in d.get("cities") or []:
        if c.get("cityId"):
            cities[c["cityId"]] = {"name": c.get("fullTitleText"), "regionId": c.get("regionId"),
                                   "areaId": c.get("areaId"), "topAreaId": c.get("topAreaId")}
        if c.get("regionId") and c.get("regionHeb"):
            regions.setdefault(c["regionId"], c["regionHeb"])


async def harvest_city_hoods(http, region_id: int, city_id: int, tiles) -> list:
    names = set()
    city_text = None
    for bbox in tiles:
        d = await _get_json(http, config.YAD2_MAP_URL,
                            {"region": region_id, "city": city_id, "bBox": bbox, "zoom": 14})
        for m in ((d or {}).get("data") or {}).get("markers") or []:
            addr = m.get("address") or {}
            ctext = (addr.get("city") or {}).get("text")
            if city_text is None and ctext:
                city_text = ctext
            hood = (addr.get("neighborhood") or {}).get("text")
            if hood and ctext == city_text:  # exclude bBox spillover into other cities
                names.add(hood)
        await asyncio.sleep(0.05)
    return sorted(names)


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-sweep", action="store_true")
    args = ap.parse_args()
    config.DATA_DIR.mkdir(parents=True, exist_ok=True)
    config.HOODS_DIR.mkdir(parents=True, exist_ok=True)

    async with aiohttp.ClientSession() as http:
        if not args.skip_sweep:
            print("Sweeping nationwide locations …")
            cat = await sweep(http)
            cat["generated"] = "harvested"
            cat["source"] = "yad2 address-autocomplete 2-letter+3-letter prefix sweep"
            with open(config.IL_LOCATIONS_FILE, "w", encoding="utf-8") as f:
                json.dump(cat, f, ensure_ascii=False, indent=1, sort_keys=True)
            print(f"  wrote {config.IL_LOCATIONS_FILE}")

        print("Harvesting Tel Aviv-Yafo hoods …")
        hoods = await harvest_city_hoods(http, 3, config.TEL_AVIV_CITY_ID, TLV_TILES)
        print(f"  {len(hoods)} hoods (quarters must be curated by hand — see existing 5000.json)")
        # Note: we do NOT overwrite the curated quarter grouping automatically; print for review.
        for h in hoods:
            print("   ", h)


if __name__ == "__main__":
    asyncio.run(main())
