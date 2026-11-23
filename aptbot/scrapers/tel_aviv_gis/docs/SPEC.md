# tel_aviv_gis: SPEC

Status: **v2 implemented** (2026-11-23: v1 works, v2 long-horizon signals). Remote-API facts live in `../api_docs/`; this file
holds our contract and the decisions behind it. Vocabulary: `GLOSSARY.md`.

## Purpose
Let the APT-APT bot tell whether a Yad2 listing (which carries latitude/longitude) is
near construction that causes noise or dust now or within roughly 1–3 years (v1), or in
a building/area slated for redevelopment further out: plans, urban renewal, TAMA 38,
dangerous-building orders (v2).

It's a **utility library**: it labels everything and filters nothing. The bot uses only the layers,
stages, severities and tags it needs.

Not a dataset: no storage, no refresh job, no browser at runtime.

## Scope
| Dimension | In scope | Out of scope |
|---|---|---|
| Stage | everything is labeled, from proposed plans to completed works | |
| Kind | building works; street/infrastructure works; town plans; dangerous-building orders; TAMA 38 policy zones | roadworks 852/853, light rail/metro, city-wide plans 683, master plan TA5500 (documented in api_docs, not mapped) |
| Source | `WM/IView2WM` MapServer behind gisn.tel-aviv.gov.il | permit-file system (מידע הנדסי), open-data portal |
| Area | Tel Aviv-Yafo municipality | other cities |

## Decisions
1. **Library, not dataset.** Callers query live; nothing is persisted here.
2. **Geometry:** the polygon (GeoJSON, WGS84) plus a locally computed area-weighted centroid
   `(lat, lng)`. The server reprojects via `outSR=4326`, so no projection library is needed.
   It has no `returnCentroid`.
3. **Public API:** a single low-level typed async `query()`. Callers compose proximity logic.
4. **Results are typed and normalized**, with the raw attributes always kept.
5. **Severity** is assigned per record so callers can ignore minor permits.
6. **Supported layers** (user choice): 772 permits/requests, 499 construction sites,
   433 street-works licenses, 858 public night works, 479 site night works.
   Roadworks 852/853 were excluded (mixed with events such as matches and festivals).
   Light-rail/metro layers were excluded (stale imports, planned routes).
7. **Stage** labels every record: `requested | permitted | lapsed | active | completed | unknown`.
   `lapsed` = a 772 `קיים היתר` whose `expiry_date` < now (user choice). `unknown` = a code
   we haven't seen. It is surfaced rather than guessed, and the live test fails on it.
8. **Severity** (user choice): token rules over the request type; the highest token wins; a permit
   renewal with nothing else is `medium`; 772 with `yechidot_diyur ≥ 4` is bumped to `high`.
9. Wire format: `inSR=4326&outSR=4326&f=geojson`, `orderByFields=<oid>` + `resultOffset` paging.
10. **Date windows** (433/858/479): the end date is a calendar day and is inclusive.
11. **v2 layers** (user choice): 528 town plans, 591 dangerous buildings, 801 TAMA 38 zones,
    746 affordable housing. Not mapped but documented for the future: 683, 852/853, light rail/metro, TA5500.
12. **One type** (user choice): `ConstructionFeature` is extended, not split. New `Kind` values are
    `plan | hazard | policy`, new `Stage` values `proposed | approved | cancelled | ordered`, and new
    fields `title`, `url`, `tags` (defaults keep v1 callers working).
13. **No default filters.** Plans are noisy at a point (street-wide, policy, national plans), so the
    noise is labeled (low severity) rather than dropped. Optional presets `PLANS_SITE_SCOPED_WHERE` and
    `PLANS_LIVE_WHERE` are exported for callers.
14. Severity describes the nature of the thing, not whether it's still alive. A cancelled renewal
    plan stays `high`; callers filter on `stage`.

## Public API
```python
from aptbot.scrapers.tel_aviv_gis import TelAvivGisClient, query

async with TelAvivGisClient(session=None) as gis:     # owns a session if none given
    feats = await gis.query(
        "permits",                     # registry key or MapServer id: permits/772, sites/499,
                                       # street_licenses/433, public_night_works/858, site_night_works/479,
                                       # plans/528, dangerous_buildings/591, tama38_zones/801,
                                       # affordable_housing/746
        where="1=1",                   # ArcGIS SQL over raw field names; CURRENT_TIMESTAMP works
        geometry=(32.0754, 34.7748),   # WGS84 (lat, lng), optional
        distance_m=200,                # server-side radius; requires geometry
        out_fields=None,               # None = all; mapper/oid fields are always added
        return_geometry=True,
        now=None,                      # reference time for date-derived stages
    )                                  # -> list[ConstructionFeature], all pages

feats = await query("sites", geometry=(lat, lng), distance_m=150)   # one-shot helper
```
Bounded concurrency per client (`TLV_GIS_CONCURRENCY`), retries on network errors / 5xx / 429
with exponential backoff, and `TelAvivGisError` on an ArcGIS error body (sent as HTTP 200),
on exhausted retries, or past `TLV_GIS_MAX_PAGES`. Unknown layers raise `KeyError`.

```python
@dataclass(frozen=True)
class ConstructionFeature:
    layer: str                    # registry key
    layer_id: int                 # MapServer id
    object_id: int
    kind: Kind                    # building | infrastructure | plan | hazard | policy
    stage: Stage                  # proposed | approved | cancelled | requested | permitted | lapsed |
                                  # active | completed | ordered | unknown
    severity: Severity            # high | medium | low  (Severity.rank for comparisons)
    address: str | None
    dates: dict[str, datetime]    # tz-aware; keys per layer, see below
    geometry_wgs84: dict | None   # GeoJSON geometry, [lng, lat]
    centroid: tuple[float, float] | None   # (lat, lng)
    raw_attributes: dict          # untouched layer attributes (Hebrew-transliterated names)
    title: str | None             # plan "<no>: <name>", request type, project name, order type...
    url: str | None               # public document link (http/https only)
    tags: frozenset[str]          # tama38_rebuild | tama38_addition | urban_renewal |
                                  # demolition_order | immediate_order
```

Settings (`aptbot/config.py`): `TLV_GIS_MAPSERVER`, `TLV_GIS_PAGE_SIZE` (≤2000),
`TLV_GIS_CONCURRENCY` (4), `TLV_GIS_TIMEOUT` (60 s), `TLV_GIS_RETRIES` (3), `TLV_GIS_MAX_PAGES` (50).

Drift check: `python -m aptbot.scrapers.tel_aviv_gis.catalog` (exit 1 on drift; `--all` lists every layer).

## Layer → Kind/Stage/Severity mapping
Field-level detail: `../api_docs/services/IView2WM/layers/`.

| Layer | Kind | Stage rule | Severity rule | Address | `dates` keys |
|---|---|---|---|---|---|
| 772 permits | building | `building_stage`: בתהליך היתר→requested; קיים היתר→permitted (lapsed if expired); בבניה→active; תעודת גמר / אכלוס→completed | tokens on `sug_bakasha`; units ≥ 4 → high | `addresses` | requested, issued, expires, start |
| 499 sites | building | `matzav_bniya`: פתיחת תיק/פיצול→permitted; תחילת עבודות/עפר/יסודות/שלד→active; גמר בניה or empty→completed | tokens on `sug_bakasha` + `sivug` | `ktovet` | works_approved, status |
| 433 street_licenses | infrastructure | window (actual start or validity start, then planned end or validity end) | medium | `t_rechov` | start, end |
| 858 public_night_works | infrastructure | window `tr_avodat_laila_me`..`_ad` | high | street (from – to) | start, end |
| 479 site_night_works | building | window `tr_hatchala`..`tr_siyum` (DD/MM/YYYY strings) | high | `ktovet` | start, end |
| 528 plans | plan | `t_status_klali`: בתכנון→proposed, בתוקף→approved, מבוטלת→cancelled (incl. shelved/historic) | low: policy/re-parcellation subject or broad scope; high: site scope + (urban renewal, or design-plan approval); else medium | none (title + url) | opened, deposited, approved, status_changed |
| 591 dangerous_buildings | hazard | ordered | high with a demolition/immediate tag, else medium | `t_ktovet` | none |
| 801 tama38_zones | policy | approved | low | none (title + PDF url) | none |
| 746 affordable_housing | building | `Status`: before/after deposit→proposed, plan in force→approved, ברישוי→requested, בבניה→active, מאוכלס→completed | medium | `Address` | none |

Tags: 772 `sw_tama_38_chadash='כן'` → `tama38_rebuild`, `sw_tama_38_tosefet='כן'` → `tama38_addition`;
528 renewal flag or title התחדשות/פינוי → `urban_renewal`; 591 section 4 or הריסה → `demolition_order`,
order 7 → `immediate_order`.

### Severity tokens (substring match, בנייה normalized to בניה)
- **high:** `בניה חדשה`, `הריסה`, `חפירה`, `דיפון`, `מרתפים`, `רב קומות`
- **medium:** `תוספות בניה`, `תוספת בניה`, `תוספות ושינויים`, `ציבורי`, `חידוש היתר`, `הארכת תוקף`
- **low:** everything else (changes without added area, change of use, fast-track, name/condition changes)
- Empty type → `medium`, so callers don't silently drop it.

## Verification (2026-11-23)
- Unit: `tests/unit/test_tel_aviv_gis_{geometry,layers,client}.py` (59 tests, offline).
- Live: `tests/live/test_tel_aviv_gis_live.py` (point+radius typed query, every layer answers, no drift).
- Precision: 772 centroids within 200 m of King George/Bograshov were compared with the city's
  own address points (layer 527) for the same addresses. Typical gap 3–20 m (building centroid
  vs entrance point); one 67 m outlier on a large lot.
- Performance: 5 layers × 250 m radius in parallel took ~1.3 s; all 2434 active permits city-wide
  (2 pages, no geometry) took ~7 s.
- v2 end to end on 200 live Yad2 listings (₪7–8k): 48 had a long-horizon signal, from same-building
  permits (27, mostly TAMA 38 rebuilds), site-scoped live plans (19) and dangerous-building orders (7).
  3 plan document links (4890, 5146, 4474) were spot-checked and opened the right plan page.
- Yad2 marker coordinates vs city address points (layer 527): median 5 m, p90 35 m. Only 98 of 200
  street names matched exactly, so callers needing the exact building should fuzzy-match the street.

## Open questions
- `request_stage` codes (1,2,3,5,6) on 772: meaning unknown, unused.
- Should 772 and 499 records for the same building be de-duplicated? (Possible join:
  499 `heterim` ↔ 772 `request_num`, untested.) Left to callers.
- Bot integration (which layers/stages/severities/tags/radius to flag on a listing) is a separate change.
- A street-address → exact-point helper (layer 527, fuzzy street names) is not built yet.
- Not investigated: 557 infrastructure-improvement zones, 682 preservation buildings.
