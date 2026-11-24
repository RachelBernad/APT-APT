# Layer 591: מבנים מסוכנים (Dangerous buildings)

Last verified: 2026-11-24 · Status: **mapped** (`dangerous_buildings`)

- URL: `…/WM/IView2WM/MapServer/591` · DB name `kl_tg_mivne_mesukan`
- Geometry: **Point** (address point). OID field: `oid_mivne`. Count: 577. Refresh: nightly.
- `metadata.json` says `lastUpdateDate 15/09/2011`, but `date_import` is nightly.
- Point geometry: use a radius of ~10–15 m to catch the building at a listing's coordinates.

## Fields
| name | alias | type | notes |
|---|---|---|---|
| `t_tzav` | תיאור צו | String | order/notice type, see codes |
| `t_mimzaim` | תאור ממצאים | String(255) | engineer's findings, free text (cracks, balconies...) with `\r\n` line breaks. **Truncated at 255 characters** mid-word, with no ellipsis: 553 of 577 rows are ≥ 250 characters, i.e. cut. The full text is not published here |
| `t_ktovet` | תאור כתובת | String | `"רחוב נחלת בנימין מס' 105"` |
| `shem_rechov`, `ms_bayit`, `knisa`, `k_rechov` | street, number, entrance, street code | | |
| `UniqueId`, `date_import` | | String | |

No date of the order is published. Every string field is declared `length: 255`.
`ms_bayit` (house number) plus `shem_rechov` gives an exact address match, which is more reliable than a
radius around a point for "is *this* building listed".

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
