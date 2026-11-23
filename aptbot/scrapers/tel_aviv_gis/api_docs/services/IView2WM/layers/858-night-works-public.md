# Layer 858: עבודות לילה – במרחב הציבורי (Night works in public space)

Last verified: 2026-11-23 · Status: **mapped** (`public_night_works`)

- URL: `…/WM/IView2WM/MapServer/858`
- Geometry: **Polygon** (street stretch). OID field: `OBJECTID`. Count: 234. Refresh: nightly.
- Rows repeat per polygon part with identical attributes.

## Fields
| name | alias | type | notes |
|---|---|---|---|
| `tr_avodat_laila_me` | תחילת עבודות | Date | night-works period start |
| `tr_avodat_laila_ad` | סיום עבודות | Date | period end |
| `taarichim_shaot` | תאריכים ושעות פעילות העבודות | String | `" DD/MM/YYYY - DD/MM/YYYY, HH:MM-HH:MM \| …"` |
| `t_rechov`, `t_rechov_me`, `t_rechov_ad`, `ms_bayit_me`, `ms_bayit_ad` | street, from-street, to-street, house range | String | |
| `display_label` | תווית רישיון | String | license number, same format as 433 `ms_rishayon` |
| `k_rishayon` | קוד רישיון | Integer | |
| `gorem_yozem_rishayon` | גורם יוזם | String | initiator, e.g. `חברת TMT` |
| `natzig` | נציג | String | **contact name and phone (personal data)**. Not copied into normalized fields |

## Mapping
Kind = infrastructure. Severity = high (night noise). Stage by `tr_avodat_laila_me`/`_ad` dates.
Address = `t_rechov` (+ ` (t_rechov_me – t_rechov_ad)` when present).
Title = `display_label` (license number). `natzig` (personal contact data) is only in `raw_attributes`.
