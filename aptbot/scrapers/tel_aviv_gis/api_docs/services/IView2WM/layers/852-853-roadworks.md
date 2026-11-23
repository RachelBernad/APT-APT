# Layers 852 / 853: עבודות כבישים (Roadworks, line / point)

Last verified: 2026-11-23 · Status: **documented, not mapped** (user decision: mixed with events)

- URLs: `…/MapServer/852` (Polyline) and `…/MapServer/853` (Point, the same 149 records).
- OID field: `OBJECTID`. Count: 149 each. Refresh: nightly.

## Fields (852; 853 adds `date_start_time_start`/`date_end_time_end` as Date, plus `x`, `y`)
| name | alias | notes |
|---|---|---|
| `mahut_avoda` | מהות העבודה | 83 distinct, 41 null. Real works (`חיבור מים+ביוב`, `עבודות מנוף`, `הריסת מבנה`, `הנחת מסילות`...) **mixed with events** (`פסטיבל הג'אז`, `יריד חנוכה`, football fixtures like `בני יהודה – הפועל עכו`) |
| `kablan_type` | סוג קבלן | `מי אביבים` 32 · `דרכים` 12 · `קבלן פרטי` 11 · `רק"ל סגול` 10 · `חברה פרטי` 9 · `קשרי קהילה` 6 · `חברת חשמל` 4 · null 55 |
| `kablan_name`, `t_ktovet` | contractor, address text | |
| `avodot_ptuhot` | עבודות פתוחות | `עבודות פתוחות` 76 · `עבודות עתידיות` 73 |
| `date_start`, `date_end`, `time_start`, `time_end` | strings | |
| `sivug_yom_layla`, `tzimtzum_netivim`, `reclass_dates_strt_work` | day/night, lane reduction, start-date class | |
| `kishur_kovets`, `commnets` (sic), `GlobalID` | file link, notes | |

To map it later, filter out events, e.g. `kablan_type <> 'קשרי קהילה'` plus a keyword
exclusion on `mahut_avoda`. Verify against the live distribution first.
