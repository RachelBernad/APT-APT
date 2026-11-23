# Layer 433: רישיונות בביצוע (Street-works licenses in execution)

Last verified: 2026-11-23

- URL: `…/WM/IView2WM/MapServer/433`
- Geometry: **Polygon** (street segment being dug up). OID field: `oid`. Count: 2563. Refresh: nightly.
- Several polygons can share one license number (`ms_rishayon`).

## Fields
| name | alias | type | notes |
|---|---|---|---|
| `ms_rishayon` | מספר רישיון | String | `2023-50017-02-01` |
| `t_rechov` | רחוב | String | street name, e.g. `רש"י (רחוב)` |
| `tr_tokef_rishayon_me` | תוקף רישיון מ | Date | license valid from |
| `tr_tokef_rishayon_ad` | תוקף רישיון עד | Date | license valid until |
| `tr_sium_avoda_metuchnan` | תאריך סיום עבודות מתוכנן | Date | planned end |
| `tr_tchilat_avoda_befoal` | תאריך תחילת עבודה בפועל | Date | actual start, often null |
| `t_sug_rishayon`, `tr_status`, `t_teur_avoda`, `t_menahel_project`, `t_kishrey_kehila` | type, status, description, PM, community liaison | String | **all null on 2026-11-23** |

## Mapping
Kind = infrastructure. Severity = medium. Stage by date: start = `tr_tchilat_avoda_befoal` or
`tr_tokef_rishayon_me`; end = `tr_sium_avoda_metuchnan` or `tr_tokef_rishayon_ad`.
now < start → permitted; start ≤ now ≤ end → active; end < now → completed.
Address = `t_rechov`.
