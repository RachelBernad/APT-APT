# Service: WM/IView2WM (MapServer)

Last verified: 2026-11-23

- URL: `https://gisn.tel-aviv.gov.il/arcgis/rest/services/WM/IView2WM/MapServer`
- ArcGIS Server **11.1**; capabilities `Map,Query,Data`; formats `JSON, geoJSON, PBF`;
  `supportsDynamicLayers: true`; `maxRecordCount: 2000`.
- Native SR: **102100 / 3857** (Web Mercator). Use `inSR=4326&outSR=4326` to work in WGS84.
- Twin service `IView2/MapServer` (root folder) has the **same 256 layers** in **ITM (EPSG:2039)**.
- 256 layers (group layers plus feature layers) and 2 tables (`401 tlv_streets`, `402 ktv_rechov_ironi_hist`).
- The full layer JSON is mirrored by the viewer at `/metadata/iviewservice.json`; see `../../viewer/`.

## Each layer's OID field has its own name
Statistics/orderBy need it: 772 `oid_permit`, 499 `oid_site`, 479 `oid_avoda`,
858/852/853 `OBJECTID`, 433 and the light-rail layers `oid`.

## Construction-related layers (candidates)
| id | Hebrew name | English | geometry | count (2026-11-23) | refresh |
|---|---|---|---|---|---|
| 772 | בקשות והיתרי בניה | Building permits and requests | Polygon | 10642 | nightly |
| 499 | אתרי בניה | Construction sites (supervision files) | Polygon | 1611 | nightly |
| 479 | עבודות לילה – אתרי בנייה | Night works at building sites | Polygon | 12 | nightly |
| 858 | עבודות לילה – במרחב הציבורי | Night works in public space | Polygon | 234 | nightly |
| 433 | רישיונות בביצוע | Street-works licenses in execution | Polygon | 2563 | nightly |
| 852 | עבודות כבישים | Roadworks (lines) | Polyline | 149 | nightly |
| 853 | עבודות כבישים - נקודתי | Roadworks (points), same records as 852 | Point | 149 | nightly |
| 762/761/760 | הקו הסגול/הירוק/האדום - תוואי מסילה | Light-rail paths | Polygon | 15/49/1 | stale (2025) |
| 763 | רכבת קלה - מתקנים הנדסיים | LR engineering sites | Polygon | 18 | stale (2022) |
| 633 | רכבת קלה - שטחי התארגנות | LR staging areas | Polygon | 36 | stale (2025) |
| 954 | קווי מטרו | Metro lines (planned) | Polyline | 4 | stale (2025) |

Context-only (not construction itself): 801 TAMA 38 zones, 528/683 town plans,
591 dangerous buildings, 746 affordable-housing projects, 557 infrastructure-improvement zones.

Per-layer pages: `layers/<id>-<slug>.md`, added for the layers selected at the checkpoint.
