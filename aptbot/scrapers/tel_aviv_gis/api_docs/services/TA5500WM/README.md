# Service: WM/TA5500WM and WM/TA5500WM2026 (master plan תא/5500)

Last verified: 2026-11-23 · Status: **documented, not mapped**

The city's outline master plan: zoning designations, urban design (building heights),
planning areas, environment/infrastructure and transport. It's policy, years out, and
describes what *may* be built, not specific projects.

- `https://gisn.tel-aviv.gov.il/arcgis/rest/services/WM/TA5500WM/MapServer`: 47 layers
- `https://gisn.tel-aviv.gov.il/arcgis/rest/services/WM/TA5500WM2026/MapServer`: 50 layers (newer edition)
- Both: ArcGIS 11.1, `Map,Query,Data`, maxRecordCount 2000, SR 3857. Same query rules as
  `../../operations/query.md`.
- The viewer loads the layer JSON as `/metadata/iviewservice5500.json`. It has 50 layers with the
  2026 ids (heights = 137), so the viewer tracks **TA5500WM2026**.
- **Layer ids differ between the two editions.** E.g. the building-height layer
  (`גובה בינוי`) is **138** in `TA5500WM` but **137** in `TA5500WM2026`. Always resolve by name.

## Groups (both editions)
`1200` zoning designations (אזורי ייעוד) · `1300` urban design (עיצוב עירוני) ·
`1400` planning areas (אזורי ומרחבי תכנון) · `1500` infrastructure / environment and infrastructure ·
`1600` transport and mobility (תחבורה וניידות). Each group repeats helper layers (plan boundary,
street background, white background).

## Building heights: `TA5500WM2026/137` `גובה בינוי`
| field | alias | sample |
|---|---|---|
| `EzorYeud` | סוג אזור יעוד | `אזור מעורב` |
| `k_gova` | מדרגת גובה | `30` (height step) |
| `MaxGovaInc` | מדרגת גובה - כולל הוראות מיוחדות | `40` |
| `MaxRahakIncAdd` | רח"ק מירבי כולל תוספת | `7` (max floor-area ratio) |
| `Instructions` | הפניה הוראות מיוחדות | `101 ב' - תחום תכניות: 3700/2, …` |
| `InTA` | בתחום העיר | `1` |

In `TA5500WM/138` the same layer has only `k_gova`, `MaxGovaInc`.

## Other notable layers (TA5500WM2026)
`128` zoning designations (`אזורי יעוד`), `137` building heights, `145` planning areas
(`מרחבי תכנון`), `164` transport networks (`רשתות תחבורה`: `TypeName1/2`, `Elevation`).
Generic `פוליגונים`/`קווים`/`מוקדים` layers carry `TypeCode1/TypeName1/TypeCode2/TypeName2`
(sampled in `TA5500WM/137` `פוליגונים`, 337 rows: `שטח פתוח` / `שטח פתוח עירוני`).

Possible future use: "how much taller could this street become" (height step / FAR at a point).
