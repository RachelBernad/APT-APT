# Layer 746: פרויקטים דיור בהישג יד (Affordable-housing projects)

Last verified: 2026-11-23 · Status: **mapped** (`affordable_housing`)

- URL: `…/WM/IView2WM/MapServer/746` · DB name `kl_tg_affordable_housing_projects`
- Geometry: **Point**. OID field: `oid`. Count: 112. Refresh: nightly-ish (`date_import` 20/11/2026).

## Fields
| name | alias | type | notes |
|---|---|---|---|
| `Project_Name` | שם פרויקט | String | `"המשתלה"` |
| `Status` | סטאטוס מפורט | String | see codes |
| `Address` | כתובת | String | `"משה וילנסקי 13"` |
| `Number_of_Affordable_Units` | מספר יחידות דיור בר השגה | Integer | |
| `Taba_Link` | שם תב"ע | String | plan number, joins to 528 `taba` (after stripping spaces). **Unverified** join |
| `Debi_Period` | תקופת דב"י בשנים | String | years as text, `"20"` |
| `publish` | לפרסום | String | always `כן` |

## Codes: `Status`
| value | count | Stage |
|---|---|---|
| תכנית בתוקף | 46 | approved |
| מאוכלס | 25 | completed |
| ברישוי | 19 | requested |
| בתכנון אחרי הפקדה | 11 | proposed |
| בתכנון לפני הפקדה | 8 | proposed |
| בבניה | 2 | active |
| תכנית מתאר בתוקף | 1 | approved |

## Mapping
Kind = building. Severity = medium. Title = `Project_Name`. Address = `Address`.
