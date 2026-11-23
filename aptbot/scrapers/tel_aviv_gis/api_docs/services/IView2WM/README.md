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
858/852/853 `OBJECTID`, 433 and the light-rail layers `oid`, 528/683 `oid_taba`,
591 `oid_mivne`, 801 `oid_zone`, 746 `oid`.

## Construction-related layers
"Mapped" = registered in `layers.py` (registry key in parentheses). Every row has a page under `layers/`.

| id | Hebrew name | English | geometry | count (2026-11-23) | refresh | mapped? |
|---|---|---|---|---|---|---|
| 772 | בקשות והיתרי בניה | Building permits and requests | Polygon | 10642 | nightly | yes (`permits`) |
| 499 | אתרי בניה | Construction sites (supervision files) | Polygon | 1611 | nightly | yes (`sites`) |
| 479 | עבודות לילה – אתרי בנייה | Night works at building sites | Polygon | 12 | nightly | yes (`site_night_works`) |
| 858 | עבודות לילה – במרחב הציבורי | Night works in public space | Polygon | 234 | nightly | yes (`public_night_works`) |
| 433 | רישיונות בביצוע | Street-works licenses in execution | Polygon | 2563 | nightly | yes (`street_licenses`) |
| 528 | תוכניות בניין עיר | Town plans (תב"ע) | Polygon | 5077 | nightly | yes (`plans`) |
| 591 | מבנים מסוכנים | Dangerous buildings | Point | 577 | nightly | yes (`dangerous_buildings`) |
| 801 | מדיניות תמ"א 38 – מפתח אזורים | TAMA 38 policy zones | Polygon | 48 | stale (2021) | yes (`tama38_zones`) |
| 746 | פרויקטים דיור בהישג יד | Affordable-housing projects | Point | 112 | nightly | yes (`affordable_housing`) |
| 683 | תוכניות כלל עירוניות | City-wide town plans | Polygon | 104 | nightly | no: covers everything |
| 852 | עבודות כבישים | Roadworks (lines) | Polyline | 149 | nightly | no: events mixed in |
| 853 | עבודות כבישים - נקודתי | Roadworks (points), same records as 852 | Point | 149 | nightly | no |
| 762/761/760 | הקו הסגול/הירוק/האדום - תוואי מסילה | Light-rail paths | Polygon | 15/49/1 | stale (2025) | no |
| 763 | רכבת קלה - מתקנים הנדסיים | LR engineering sites | Polygon | 18 | stale (2022) | no |
| 633 | רכבת קלה - שטחי התארגנות | LR staging areas | Polygon | 36 | stale (2025) | no |
| 954 | קווי מטרו | Metro lines (planned) | Polyline | 4 | stale (2025) | no |

Not investigated yet: 557 infrastructure-improvement zones (אזורי שיקום ושיפור תשתיות),
682 historic/preservation buildings (מבנים ואתרים לשימור, a constraint on rebuilding).
The master plan is a separate service: `../TA5500WM/README.md`.
