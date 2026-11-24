# Layer 528: תוכניות בניין עיר (Town plans / תב"ע)

Last verified: 2026-11-24 · Status: **mapped** (`plans`) · **OUTAGE since 2026-11-24 (see below)**

- URL: `…/WM/IView2WM/MapServer/528` · DB name `yk_tg_taba_union`
- Geometry: **Polygon** (plan boundary). OID field: `oid_taba`. Count: 5077 (2026-11-23). Refresh: nightly.

> **Outage (observed 2026-11-24, ~09:45–10:45 IST and ongoing):** every query fails with
> `{"error":{"code":400,"message":"Failed to execute query."}}`, even `where=1=1&returnCountOnly=true`,
> on every service, and the viewer's `tabaot*.json` caches hold the same error. The metadata still loads,
> but `taarichPticha` has disappeared from the field list (48 fields). Details and detection:
> [transport/http-and-auth.md#outages](../../../transport/http-and-auth.md#outages). The client raises
> `TelAvivGisLayerUnavailable`. When the layer returns, re-run `catalog.py` and re-check the codes and
> counts below before trusting them.
- Layer 683 (city-wide plans) has the **same schema** (see `683-city-wide-plans.md`).

## Fields
| name | alias | type | notes |
|---|---|---|---|
| `taba` | שם תבע | String | plan number, **padded with trailing spaces** (`"4406                "`); also `תעא/…`, `תמ"א/…` |
| `shem_taba` | נושא | String | plan title, e.g. `התחדשות ברמת אביב ב` |
| `t_status_klali` | סטטוס כללי | String | general status, see codes |
| `t_status` | סטטוס מפורט | String | detailed status, see codes |
| `t_hekef` | היקף | String | scope: `נקודתית` 2233 · `אזורית` 1174 · `מתחמית` 537 · `רב נקודתית` 350 · `כלל עירונית` 74 · `ארצית` 24 · empty 685 |
| `t_sivug` | סיווג | String | `מפורטת` 2679 · `מתאר` 260 · `מתאר מפורטת` 77 · empty 2051 |
| `sug_nose` | סוג נושא | String | `תב"ע` 4089 · `עיצוב ארכיטקטוני` 738 · `תרשצ` (re-parcellation) 119 · `מדיניות` (policy) 106 · `תדמת` 3 |
| `maslul` | מסלול | String | `ועדה מקומית` 929 · `ועדה מחוזית` 551 · empty 3597 |
| `sw_hitchadshut_ironit` | כולל התחדשות עירונית | SmallInteger | 1 on only **39** plans. The title is a better signal |
| `sw_mimush_male` | מומשה במלואה | Integer | 1 on only 7. Unreliable |
| ~~`taarichPticha`~~ | תאריך פתיחה | Date | plan opened. **Removed from the layer on 2026-11-24**; `layers.py` still reads it if it comes back, but no longer expects it |
| `tr_hafkada` | הפקדה | Date | deposited |
| `tr_matan_tokef` | מתן תוקף | Date | approved; old plans go back decades, so filter on it |
| `tr_shinuy_status` | תאריך שינוי סטטוס | Date | |
| `url_documents` | קישור למסמכים | String | **always set** (5077/5077): `https://gisn.tel-aviv.gov.il/tabaot/docs.aspx?id_taba=…&st_taba=…&mode=internet`. Public, verified 200 |
| `url_iplan` | קישור למנהל התכנון | String | 468/5077: `https://mavat.iplan.gov.il/SV4/1/<id>/310` |
| `mispar_tochnit_mavat`, `mp_id`, `id_taba`, `ms_yalkut_acharon` | national plan id, ids, gazette no. | | |
| `diyur_bar_hasaga_*`, `total_y_diyur_bar_hasaga` | affordable-housing units | Integer | mostly 0 |
| `melonaut_rooms_mechushav`, `sw_niyud_zchuyot`, `sw_repartzelatzya`, `ms_shetach_graphi` | hotel rooms, rights transfer, re-parcellation, area (m²) | | |

## Codes: general → detailed status (2026-11-23 counts)
| `t_status_klali` | `t_status` values (count) | Stage |
|---|---|---|
| בתכנון (190) | הפקדה 80 · פרסום תנאים מגבילים 78 41 · לפני הפקדה 31 · החלטה על הכנת תוכנית תמ"א 10 · לפני ועדה מקומית 9 · הערות והשגות 7 · סטטוס 77 "הכנת תכנית" 3 · objections/approval steps 1–3 each | proposed |
| בתוקף (4078) | בתוקף 3126 · אישור תוכנית עיצוב בינוי מקומית 681 · אישור שיכון ציבורי 121 · מתן תוקף (ללא פרסום) 75 · אישור תוכנית מתאר ארצית 41 · אישור תוכנית עיצוב בינוי מחוזית 34 | approved |
| מבוטלת (781) | תכנית גנוזה 575 · תכנית מבוטלת 174 · תכנית הסטורית/ללא זכויות נוכחיות 32 | cancelled |
| *(empty)* (28) | *(empty)* | unknown |

## Noise notes (from a 200-listing live test)
A point query matches every plan covering the point, including street-wide plans
(Ibn Gabirol, Arlozorov), policy documents, coastline plans and national ones (`תמ"א/65`
substations). Filter with `t_hekef IN ('נקודתית','רב נקודתית','מתחמית')` (exported as
`PLANS_SITE_SCOPED_WHERE`) and recent/in-planning status (`PLANS_LIVE_WHERE`). With both,
19 of 200 listings were in a matching plan.

## Mapping (in `layers.py`)
Kind = plan. Stage by `t_status_klali`. Severity: **low** if `sug_nose` is מדיניות/תרשצ or the
scope is broad (אזורית/כלל עירונית/ארצית); **high** if site-scoped and (urban renewal, by flag or by
title containing התחדשות/פינוי, or a design-plan approval `אישור תוכנית עיצוב בינוי…`);
otherwise **medium** (including empty scope). Tag `urban_renewal`. Title = `"<taba>: <shem_taba>"`.
URL = `url_documents`, falling back to `url_iplan`. Dates: opened, deposited, approved, status_changed.
