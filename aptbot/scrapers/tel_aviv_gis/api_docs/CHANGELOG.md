# api_docs changelog

Newest first. Each entry: date, what changed, how it was observed, code impact.

## 2026-11-24 (outage + caveats)
- **Outage on 528/683 (town plans).** Every query fails with `"Failed to execute query."`, even a `1=1`
  count, on all services. `identify`/`find` return empty, `export` is blank, and the viewer's `tabaot*.json`
  hold the error body. Observed via a live point query, then probes on every service and the viewer bundle.
  New pages/sections: `transport/http-and-auth.md#outages`, and banners on 528/683.
- **Schema change on 528/683:** `taarichPticha` removed (48 fields), seen in `catalog.py` DRIFT.
- Query caveats (`operations/query.md`): `distance=0` matches nothing; a bad field and an outage
  return the same error body (probe with `1=1`); grouped statistics are capped at 2000 groups and
  ignore `resultOffset` (page with `where` ranges); centroid distance ≠ feature distance; date
  sentinels (2999-12-31), `""`/`null` empties, and 255-character truncation.
- Cache note corrected: identical query URLs were always `x-cache-status: MISS`.
- 772: `building_stage` is building-level (953 issued rows read "requested"); `tochen_bakasha`
  structure; several requests per building. 499: verified join `heterim` ↔ 772 `permission_num`
  (58/60) and `tik_tipul` ↔ `tik_tipul_1` (55/60), identical polygons. 433: 697 licenses over 2563
  polygons. 591: `t_mimzaim` truncated in 553/577. 801: city-scale zones, so use a bare point.
- Code impact: `client.py` raises `TelAvivGisQueryError` / `TelAvivGisLayerUnavailable` (probe) and
  sends a bare point for `distance_m=0`; `layers.py` derives the stage of issued 772 rows from their own
  dates and drops `taarichPticha` from the expected fields; `catalog.py` prints `UNAVAILABLE` (exit 2)
  instead of crashing.

## 2026-11-23 (v2: long horizon)
- New mapped-layer pages: 528 town plans, 591 dangerous buildings, 801 TAMA 38 zones,
  746 affordable housing, with codes and counts taken from live group-by statistics.
- New reference-only pages: 683 city-wide plans, 852/853 roadworks, light rail/metro,
  and the `TA5500WM`/`TA5500WM2026` master-plan services (their layer ids differ between editions).
- Verified public links: `tabaot/docs.aspx` (all 5077 plans), `taba_raster/*.pdf`,
  `archive-binyan.tel-aviv.gov.il` (permit `url_hadmaya`), `mavat.iplan.gov.il` (468 plans).
  499 `url_archion_tik` is intranet-only (`handasa-archive`).
- Code impact: `layers.py` gains 4 layers, plus title/url/tags on all layers; TAMA 38 tags on 772.

## 2026-11-23
- Added layer pages for the 5 supported layers (772, 499, 433, 858, 479) with verified codes
  and counts. Verified that server SQL accepts `CURRENT_TIMESTAMP` / `DATE 'YYYY-MM-DD'`, that
  `returnCentroid` is not supported, and that 433's type/status fields are all null.
  Code impact: `layers.py` mappings and `client.py` (v1) are built on these.
- First analysis (DevTools capture of the iView load, then curl/urllib replay). Documented:
  no auth; F5 edge; gzip-without-Accept-Encoding and BOM quirks; errors returned as HTTP 200;
  `maxRecordCount` 2000 with offset paging; server-side point+radius with `inSR/outSR=4326`;
  candidate construction layers 772/499/479/858/433/852/853 (nightly) and the light-rail layers (stale).
  Code impact: none yet (pre-implementation).
- Created the skeleton.
