# Layer 499: אתרי בניה (Construction sites)

Last verified: 2026-11-24 · Status: **mapped** (`sites`)

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
| `heterim` | היתרים בתיק | String | permit numbers joined with ` ,` (e.g. `20220951 ,20260532`). These are 772 **`permission_num`** values (not `request_num`); see Joining |
| `ishurei_laila` | אישור לעבודות לילה | String | night-work approvals: `DD/MM/YYYY-DD/MM/YYYY` ranges joined with ` ,` (repeats), mostly null. Past approvals stay listed; this is a history, not a schedule |
| `baalei_heter` | בעלי היתר | String | permit holder, often the developer company |
| `tik_tipul`, `ms_tik_binyan`, `gush_chelka`, `shimush` | supervision file, building file, parcel (`"6215 / 200 "`), use | | |
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

## Joining with 772 permits (verified 2026-11-24)
A site and the permit(s) it executes describe the same building twice. Link them by:
- `heterim` → split on `,`, strip, match 772 `permission_num`. 58 of 60 random sites matched at
  least one permit; 29 of the 60 list **more than one** permit (amendments, renewals).
- `tik_tipul` = 772 `tik_tipul_1` (55 of 60), and `ms_tik_binyan` = 772 `ms_tik_binyan`.
- The polygons are identical (same vertices), so a radius query returns both. Callers that
  count "construction near X" should de-duplicate on this join.
The 499 row is the better source of the physical stage (`matzav_bniya`); 772 has the request
details (`sug_bakasha`, `tochen_bakasha`, units).

## Mapping (in `layers.py`)
Kind = building. Severity from `sug_bakasha` + `sivug` tokens. Address = `ktovet`.
Dates: works_approved=`tr_tchilat_avoda`, status=`tr_status_pikuach`.
Title = `sug_bakasha`. No URL (`url_archion_tik` is intranet-only).
