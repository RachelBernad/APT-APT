# Layer 591: מבנים מסוכנים (Dangerous buildings)

Last verified: 2026-11-23 · Status: **mapped** (`dangerous_buildings`)

- URL: `…/WM/IView2WM/MapServer/591` · DB name `kl_tg_mivne_mesukan`
- Geometry: **Point** (address point). OID field: `oid_mivne`. Count: 577. Refresh: nightly.
- `metadata.json` says `lastUpdateDate 15/09/2011`, but `date_import` is nightly.
- Point geometry: use a radius of ~10–15 m to catch the building at a listing's coordinates.

## Fields
| name | alias | type | notes |
|---|---|---|---|
| `t_tzav` | תיאור צו | String | order/notice type, see codes |
| `t_mimzaim` | תאור ממצאים | String | engineer's findings, free text (cracks, balconies...) |
| `t_ktovet` | תאור כתובת | String | `"רחוב נחלת בנימין מס' 105"` |
| `shem_rechov`, `ms_bayit`, `knisa`, `k_rechov` | street, number, entrance, street code | | |
| `UniqueId`, `date_import` | | String | |

No date of the order is published.

## Codes: `t_tzav` (2026-11-23 counts)
| value | count | tags |
|---|---|---|
| צו 3 - בדיקת מהנדס פרטי | 435 | (private engineer inspection) |
| הודעה לפי סעיף 3 | 68 | |
| הודעה לפי 3 קירות תומכים | 16 | |
| הודעה לפי סעיף 4 - הריסה | 15 | demolition_order |
| צו 7 - ביצוע עבודות מיידי | 12 | immediate_order |
| הודעה לפי 3 - נכס עירוני | 12 | |
| הודעה לפי סעיף 4 - אטימה | 10 | demolition_order (sealing) |
| הודעה לפי סעיף 9 | 4 | |
| צו 7 - פינוי מיידי | 2 | immediate_order |
| צו 7 - הריסה מיידית | 2 | immediate_order, demolition_order |
| צו 7 - סגירה מיידית | 1 | immediate_order |

## Mapping
Kind = hazard. Stage = ordered. Severity high when a tag applies, else medium.
Title = `t_tzav`. Address = `t_ktovet`.
