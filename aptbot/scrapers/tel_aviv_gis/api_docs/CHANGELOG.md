# api_docs changelog

Newest first. Each entry: date, what changed, how it was observed, code impact.

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
