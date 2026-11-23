# Layer 479: עבודות לילה – אתרי בנייה (Night works at building sites)

Last verified: 2026-11-23

- URL: `…/WM/IView2WM/MapServer/479`
- Geometry: **Polygon**. OID field: `oid_avoda`. Count: 12. Refresh: nightly.

## Fields
| name | alias | type | notes |
|---|---|---|---|
| `mahut` | מהות | String | e.g. `עבודות יציקה` (concrete pouring) |
| `tr_hatchala` | מתאריך | **String** `DD/MM/YYYY` | not a Date field |
| `tr_siyum` | עד תאריך | **String** `DD/MM/YYYY` | |
| `shaot_between` | שעות | String | `19:00-05:00` |
| `ktovet` | כתובת | String | `"רחוב החרש מס' 18"` |
| `tik_tipul` | תיק טיפול | String | `63-1-2022-0516`. **Unverified:** same format as 499 `tik_tipul`; the join was not tested |
| `id_binyan`, `UniqueId` | ids | | |

## Mapping
Kind = building. Severity = high. Stage by parsed `tr_hatchala`/`tr_siyum` (end date inclusive).
Address = `ktovet`.
