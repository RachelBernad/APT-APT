# Layer 801: מדיניות תמ"א 38 – מפתח אזורים (TAMA 38 policy zones)

Last verified: 2026-11-24 · Status: **mapped** (`tama38_zones`)

- URL: `…/WM/IView2WM/MapServer/801` · DB name `kl_tg_tama38_zones`
- Geometry: **Polygon**, from compound-sized (zone 30 is ~298k m²) up to whole quarters (a
  "quarters 3,4" zone has `Shape_Area` 5.57M and 239 vertices). `Shape_Area`/`Shape_Length` are in
Web Mercator units, which overstate ground area by about 1.39× at Tel Aviv's latitude (so ~4 km² here). OID field: `oid_zone`. Count: 48.
- **Stale:** `date_import` is `07/10/2021`. It's a policy map, not a live feed.
- **Ask "which zone contains the point", not "which zones are near".** Query with a bare point (no
  `distance`, or `distance_m=0` in the client). A radius returns neighboring zones too, and
  centroid distances are meaningless at this scale: the containing zone's centroid can be farther
  than a neighbor's.

## Fields
| name | alias | type | notes |
|---|---|---|---|
| `zone_name` | שם מתחם | String | zone number as text, `"30"` (48 distinct) |
| `zone_desc` | תאור מתחם | String | `"שיכון ל'"` |
| `document` | מסמך מדיניות | String | policy PDF, e.g. `https://gisn.tel-aviv.gov.il/taba_raster/9086_MD.pdf`. Public, verified 200 (2 MB PDF) |

The rules per zone (what TAMA 38 allows there) are only in the PDF, not in attributes.

## Mapping
Kind = policy. Stage = approved. Severity = low. Title = `"<zone_desc> (<zone_name>)"`. URL = `document`.
