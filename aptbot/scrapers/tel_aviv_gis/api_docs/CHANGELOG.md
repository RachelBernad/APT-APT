# api_docs changelog

Newest first. Each entry: date, what changed, how it was observed, code impact.

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
