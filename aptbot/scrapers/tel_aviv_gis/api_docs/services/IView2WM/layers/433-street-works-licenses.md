# Layer 433: רישיונות בביצוע (Street-works licenses in execution)

Last verified: 2026-11-24 · Status: **mapped** (`street_licenses`)

- URL: `…/WM/IView2WM/MapServer/433`
- Geometry: **Polygon** (street segment being dug up). OID field: `oid`. Count: 2563. Refresh: nightly.
- **Many polygons per license:** 2563 rows are only 697 distinct `ms_rishayon`. 577 licenses
  have more than one polygon, up to 42. Each segment of the work is its own row with the same
  dates, so a radius query returns the "same" license several times. De-duplicate on
  `ms_rishayon` when counting or listing works.
- Segments can be long (a whole street), so their centroid says little about where the work is
  relative to a point (see `operations/query.md`, distance caveat).

## Fields
| name | alias | type | notes |
|---|---|---|---|
| `ms_rishayon` | מספר רישיון | String | `2023-50017-02-01` |
| `t_rechov` | רחוב | String | street name, e.g. `רש"י (רחוב)` |
| `tr_tokef_rishayon_me` | תוקף רישיון מ | Date | license valid from |
| `tr_tokef_rishayon_ad` | תוקף רישיון עד | Date | license valid until |
| `tr_sium_avoda_metuchnan` | תאריך סיום עבודות מתוכנן | Date | planned end |
| `tr_tchilat_avoda_befoal` | תאריך תחילת עבודה בפועל | Date | actual start, often null |
| `t_sug_rishayon`, `tr_status`, `t_teur_avoda`, `t_menahel_project`, `t_kishrey_kehila` | type, status, description, PM, community liaison | String | **all null** (re-checked 2026-11-24: `t_teur_avoda` null in 2563/2563). The layer never says *what* the work is; the license number is the only handle |

## Mapping
Kind = infrastructure. Severity = medium. Stage by date: start = `tr_tchilat_avoda_befoal` or
`tr_tokef_rishayon_me`; end = `tr_sium_avoda_metuchnan` or `tr_tokef_rishayon_ad`.
now < start → permitted; start ≤ now ≤ end → active; end < now → completed.
Address = `t_rechov`.
Title = `ms_rishayon` (license number).
