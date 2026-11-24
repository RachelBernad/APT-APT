# Layer 772: בקשות והיתרי בניה (Building permits and requests)

Last verified: 2026-11-24 · Status: **mapped** (`permits`)

- URL: `…/WM/IView2WM/MapServer/772`
- Geometry: **Polygon** (building footprint/parcel). `outSR=4326` works.
- OID field: `oid_permit`. Count: 10642. Refresh: nightly (`date_import`).
- Capabilities: statistics, orderBy, distinct, pagination; no `returnCentroid`.
- One record per **request** (`request_num`). A building (`ms_tik_binyan`) usually has several:
  10642 rows over 5823 buildings. `building_stage` is a **building-level** field copied onto every
  request of that building (see Caveats).

## Fields we use
| name | alias | type | meaning / sample |
|---|---|---|---|
| `oid_permit` | מזהה ממג | OID | |
| `building_stage` | פעילות רישוי אחרונה בבניין | String | see codes below |
| `request_stage` | שלב בקשה | String | `1,2,3,5,6`. Meaning **Unverified** |
| `sug_bakasha` | סוג בקשה | String | comma-joined `"<category> <subtype>"` items, 442 combos |
| `tochen_bakasha` | תוכן בקשה | String | semi-structured description, see Caveats |
| `maslul_rishuy` | מסלול רישוי | String | `רישוי מלא…`, `הקלה/שמוש חורג/תמ"א38`, `שינויים בסמכות מה"ע`, `רישוי מקוצר` |
| `open_request` | תאריך פתיחת בקשה | Date (epoch ms) | |
| `permission_date` | תאריך היתר | Date | null until issued |
| `expiry_date` | תאריך תוקף היתר | Date | permit validity (deadline to start work). `2999-12-31` = no expiry (94 rows, 89 of them change-of-use `שימוש חורג`) |
| `tr_hathalat_bniya` | תאריך התחלת בניה | Date | start of works. Appears to be per building: sibling requests share it (**Unverified** in general) |
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

## Caveats (verified 2026-11-24)

### `building_stage` describes the building, not the request
Its alias is "פעילות רישוי אחרונה בבניין" (the building's latest licensing activity). Every request of a
building carries the same value, so when a **new** request is filed on a building whose earlier
permit was already issued (often while the work is under way), the older, issued request also reads
`בתהליך היתר`. City-wide: 953 of the 4138 `בתהליך היתר` rows have their own `permission_date`,
and 747 of them also have a start date. For 613 rows that were issued, started and not finished, the matching
499 site (`heterim` contains the `permission_num`) was mostly under way: earthworks 227,
works started 190, foundations 39, frame 44. Some were not yet started (file opened, 120) or finished (22).
The start and finish dates are **per building** too. 282 of the 953 have a start date *before* their
own `permission_date`, which means earlier works on the building. In a sample of 40 of those, 28 had no 499
site at all. All 105 `finished` dates are on or after the permit date. `finished`/`occupation` are
`DD/MM/YYYY` strings (1632 + 980 non-empty values checked, all in that format).
**Rule:** if `building_stage` is `בתהליך היתר` but the row has a `permission_date`, decide the stage from
the row's own dates, counting a start or finish only if it is on or after the permit date (±1 day for
time zones): finished/occupied → completed; started and start ≤ now → active (even past
`expiry_date`, as with `בבניה`); else permitted, or lapsed if `expiry_date` has passed.

### `tochen_bakasha` is semi-structured
Newline-separated `<heading>: <details>` lines, e.g.
`הריסה: מספר קומות להריסה: N, שטח הריסה (מ"ר): X`,
`פירוט המבוקש במרתפים: …`, `פירוט המבוקש בקומות: כמות קומות מגורים: N, כמות יח"ד מבוקשות: M`,
`פירוט המבוקש על הגג: …`, `פירוט נוסף: <free text>`. It's the only place with demolished floors and
area, floor counts and basements. Not parsed by `layers.py` yet; it's available in `raw_attributes`.
**Unverified:** that every request type uses these headings.

### One building, several rows (and the 499 site)
Group by `ms_tik_binyan` to see a building's history. The 499 site for an issued permit
shares its polygon and links by `permission_num` (see `499-construction-sites.md`).

## Common `sug_bakasha` items (in-scope stages)
`שינויים חידוש היתר` 631 · `בניה חדשה בנייה חדשה תמ"א 38` 585 · `תוספות בניה תוספות בניה שונות` 517 ·
`שינויים שינוי ללא תוספת שטח/חזית` 440 · `תוספות בניה תוספת בניה לפי תמ"א 38` 401 ·
`בניה חדשה חפירה ו/או דיפון` 119 · `תוספות בניה הריסה` 77 · `ציבורי פיתוח עירוני/שצ"פ/מדרכות/כבישים/גשרים` 54.

## Server SQL notes
`CURRENT_TIMESTAMP` and `DATE 'YYYY-MM-DD'` literals work in `where`
(e.g. `expiry_date < CURRENT_TIMESTAMP`).

## Mapping (in `layers.py`)
Kind = building. Stage per the table above, except issued rows under a newer request (Caveats rule).
Severity from `sug_bakasha` tokens (see `docs/SPEC.md`),
bumped to high when `yechidot_diyur ≥ 4`. Address = `addresses`. Dates: requested=`open_request`,
issued=`permission_date`, expires=`expiry_date`, start=`tr_hathalat_bniya`.
Title = `sug_bakasha`. URL = `url_hadmaya` (public permit archive, verified 200).
Tags: `tama38_rebuild` when `sw_tama_38_chadash='כן'` (1565 total), `tama38_addition` when
`sw_tama_38_tosefet='כן'` (607 total).
