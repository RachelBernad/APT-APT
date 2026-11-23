# Layer 801: מדיניות תמ"א 38 – מפתח אזורים (TAMA 38 policy zones)

Last verified: 2026-11-23 · Status: **mapped** (`tama38_zones`)

- URL: `…/WM/IView2WM/MapServer/801` · DB name `kl_tg_tama38_zones`
- Geometry: **Polygon** (neighborhood-sized compounds; zone 30 is ~298k m²). OID field: `oid_zone`. Count: 48.
- **Stale:** `date_import` is `07/10/2021`. It's a policy map, not a live feed.

## Fields
| name | alias | type | notes |
|---|---|---|---|
| `zone_name` | שם מתחם | String | zone number as text, `"30"` (48 distinct) |
| `zone_desc` | תאור מתחם | String | `"שיכון ל'"` |
| `document` | מסמך מדיניות | String | policy PDF, e.g. `https://gisn.tel-aviv.gov.il/taba_raster/9086_MD.pdf`. Public, verified 200 (2 MB PDF) |

The rules per zone (what TAMA 38 allows there) are only in the PDF, not in attributes.

## Mapping
Kind = policy. Stage = approved. Severity = low. Title = `"<zone_desc> (<zone_name>)"`. URL = `document`.
