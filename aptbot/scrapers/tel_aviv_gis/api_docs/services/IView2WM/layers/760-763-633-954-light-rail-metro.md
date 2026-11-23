# Layers 760–763, 633, 950, 954: light rail and metro

Last verified: 2026-11-23 · Status: **documented, not mapped** (stale imports; mostly routes)

| id | name | geometry | count | last `date_import` | useful fields |
|---|---|---|---|---|---|
| 762 | הקו הסגול - תוואי מסילה (Purple Line path) | Polygon | 15 | 20/03/2025 | `mikta_teor` (segment), `startWork`, `endWork`, `startInf2`, `endinf2` (strings `DD/MM/YYYY`, may contain `\r\n`), `plan_name`, `line`, `level` |
| 761 | הקו הירוק - תוואי מסילה (Green Line path) | Polygon | 49 | 20/03/2025 | `mikta_teor`, `startwork`, `endWork` (null in the 3 sampled rows), `plan_name`, `line`, `level` |
| 760 | הקו האדום - תוואי מסילה (Red Line path) | Polygon | 1 | | `line` |
| 763 | רכבת קלה - מתקנים הנדסיים (LR engineering sites) | Polygon | 18 | 09/09/2022 | `st_taba` (e.g. `תתל/70א`), `t_yeud_kar` |
| 633 | רכבת קלה - שטחי התארגנות (LR staging areas) | Polygon | 36 | 31/07/2025 | geometry only |
| 950 | הקו הירוק - מבני דרך (Green Line structures) | Polyline | 10 | | |
| 954 | קווי מטרו (Metro lines, planned) | Polyline | 4 | 12/07/2025 | `line` (`M1-north`, `M2`, `M3`) |
| 766 / 764 / 423 | Purple / Green / Red line stations | Point | 19 / 33 / 12 | | |

OID field is `oid` on 760–763 and 633. Active light-rail street work also appears in the
nightly layers 433 (street-works licenses) and 852 (roadworks, `kablan_type='רק"ל סגול'`).
To map the metro as a decade-scale signal later, 954 lines buffered by a few hundred metres
would work. Verify station layers first.
