# Layer 772: בקשות והיתרי בניה (Building permits and requests)

Last verified: 2026-11-23

- URL: `…/WM/IView2WM/MapServer/772`
- Geometry: **Polygon** (building footprint/parcel). `outSR=4326` works.
- OID field: `oid_permit`. Count: 10642. Refresh: nightly (`date_import`).
- Capabilities: statistics, orderBy, distinct, pagination; no `returnCentroid`.
- One record per building permit file; the latest licensing activity sets `building_stage`.

## Fields we use
| name | alias | type | meaning / sample |
|---|---|---|---|
| `oid_permit` | מזהה ממג | OID | |
| `building_stage` | פעילות רישוי אחרונה בבניין | String | see codes below |
| `request_stage` | שלב בקשה | String | `1,2,3,5,6`. Meaning **Unverified** |
| `sug_bakasha` | סוג בקשה | String | comma-joined `"<category> <subtype>"` items, 442 combos |
| `tochen_bakasha` | תוכן בקשה | String | free-text description |
| `maslul_rishuy` | מסלול רישוי | String | `רישוי מלא…`, `הקלה/שמוש חורג/תמ"א38`, `שינויים בסמכות מה"ע`, `רישוי מקוצר` |
| `open_request` | תאריך פתיחת בקשה | Date (epoch ms) | |
| `permission_date` | תאריך היתר | Date | null until issued |
| `expiry_date` | תאריך תוקף היתר | Date | permit validity (deadline to start work) |
| `tr_hathalat_bniya` | תאריך התחלת בניה | Date | |
| `finished`, `occupation` | תאריך גמר / טופס אכלוס | **String** | often empty |
| `addresses` | כתובות | String | `"המלך ג'ורג' 66"` |
| `yechidot_diyur` | יחידות דיור | Integer | housing units added |
| `sw_tama_38`, `sw_tama_38_chadash`, `sw_tama_38_tosefet` | תמא 38 (חדש/תוספת) | String | `כן`/`לא` |
| `request_num`, `permission_num`, `ms_tik_binyan`, `building_num` | ids | Integer | |
| `url_hadmaya` | הדמייה | String | link to archive-binyan.tel-aviv.gov.il |

Also present: `koteret`, `progress` (a sort key, unique per row), `hakala_*` (relief details),
`tik_tipul_1..5`, `sivug_makor`, `protected` (always 0), `date_import`.

## Codes: `building_stage` (2026-11-23 counts)
| value | count | Stage |
|---|---|---|
| בתהליך היתר | 4138 | requested |
| קיים היתר | 1756 | permitted, or **lapsed** if `expiry_date < now` (193) |
| בבניה | 2434 | active (expiry is irrelevant once work has started: 728 have a past expiry) |
| קיימת לפחות תעודת גמר אחת | 1881 | completed |
| קיים אכלוס | 433 | completed |

## Common `sug_bakasha` items (in-scope stages)
`שינויים חידוש היתר` 631 · `בניה חדשה בנייה חדשה תמ"א 38` 585 · `תוספות בניה תוספות בניה שונות` 517 ·
`שינויים שינוי ללא תוספת שטח/חזית` 440 · `תוספות בניה תוספת בניה לפי תמ"א 38` 401 ·
`בניה חדשה חפירה ו/או דיפון` 119 · `תוספות בניה הריסה` 77 · `ציבורי פיתוח עירוני/שצ"פ/מדרכות/כבישים/גשרים` 54.

## Server SQL notes
`CURRENT_TIMESTAMP` and `DATE 'YYYY-MM-DD'` literals work in `where`
(e.g. `expiry_date < CURRENT_TIMESTAMP`).

## Mapping (in `layers.py`)
Kind = building. Stage per the table above. Severity from `sug_bakasha` tokens (see `docs/SPEC.md`),
bumped to high when `yechidot_diyur ≥ 4`. Address = `addresses`. Dates: requested=`open_request`,
issued=`permission_date`, expires=`expiry_date`, start=`tr_hathalat_bniya`.
