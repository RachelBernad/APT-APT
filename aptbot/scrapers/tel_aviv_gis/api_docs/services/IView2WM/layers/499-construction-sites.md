# Layer 499: אתרי בניה (Construction sites)

Last verified: 2026-11-23

- URL: `…/WM/IView2WM/MapServer/499`
- Geometry: **Polygon**. OID field: `oid_site`. Count: 1611. Refresh: nightly.
- One record per construction-supervision file (תיק טיפול), i.e. the site after the permit was handed over.
- `metadata.json` says `lastUpdateDate 03/02/2022`, but `date_import` is nightly. Trust `date_import`.

## Fields we use
| name | alias | type | meaning / sample |
|---|---|---|---|
| `matzav_bniya` | שלב בניה | String | physical stage, see codes |
| `status_pikuach` | סטטוס תיק | String | supervision status (values have **trailing spaces**) |
| `tr_status_pikuach` | מתאריך | Date | date of that status |
| `tr_tchilat_avoda` | ת. אישור עבודות | Date | works-approval date |
| `sivug` | מסלול רישוי | String | `בניה חדשה` 802, `תוספות ושינויים` 612, `ציבורי/תעסוקה` 93, `הארכת תוקף החלטה ` 68, `מסלול מקוצר` 36 |
| `sug_bakasha` | מהות | String | `"<category>/<subtype>"` items joined with ` ,` |
| `tochen_bakasha` | תוכן בבקשה | String | free text |
| `ktovet` | כתובת | String | `"שדרות המעפילים מס' 18,  רחוב מורדי הגטאות מס' 35 "` |
| `heterim` | היתרים בתיק | String | permit numbers joined with ` ,` (e.g. `20220951 ,20260532`). **Unverified:** same 8-digit format as 772 `request_num`; the join was not tested |
| `ishurei_laila` | אישור לעבודות לילה | String | `DD/MM/YYYY-DD/MM/YYYY` ranges joined with ` ,` (repeats), mostly null |
| `tik_tipul`, `ms_tik_binyan`, `gush_chelka`, `baalei_heter`, `shimush` | ids, parcel, owner, use | | |
| `url_archion_tik` | | String | **intranet** host `handasa-archive`, not public |

## Codes: `matzav_bniya`
| value | count | Stage |
|---|---|---|
| פתיחת תיק | 220 | permitted |
| פיצול תיק לבניה בשלבים | 11 | permitted |
| תחילת עבודות | 708 | active |
| עבודות עפר וביסוס | 322 | active |
| גמר יסודות | 118 | active |
| גמר שלד | 50 | active |
| גמר בניה | 83 | completed |
| *(empty)* | 99 | completed (all have an occupancy/form-4/completion status) |

## Mapping (in `layers.py`)
Kind = building. Severity from `sug_bakasha` + `sivug` tokens. Address = `ktovet`.
Dates: works_approved=`tr_tchilat_avoda`, status=`tr_status_pikuach`.
