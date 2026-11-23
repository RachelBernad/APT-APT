# tel_aviv_gis: SPEC

Status: **v1 implemented** (2026-11-23). Remote-API facts live in `../api_docs/`; this file
holds our contract and the decisions behind it. Vocabulary: `GLOSSARY.md`.

## Purpose
Let the APT-APT bot tell whether a Yad2 listing (which carries latitude/longitude) is
near construction that causes noise or dust now or within roughly 1–3 years.

This is a **utility library**, not a dataset: no storage, no refresh job, no browser at runtime.

## Scope
| Dimension | In scope | Out of scope |
|---|---|---|
| Stage | everything is labeled; callers usually keep requested / permitted / active | zoning plans (תב"ע) without a permit |
| Kind | building works (new build, demolish/rebuild, additions, TAMA 38, renovation); street/infrastructure works | roadworks feed 852/853, light rail/metro layers (see decision 6) |
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

## Public API
```python
from aptbot.scrapers.tel_aviv_gis import TelAvivGisClient, query

async with TelAvivGisClient(session=None) as gis:     # owns a session if none given
    feats = await gis.query(
        "permits",                     # registry key or MapServer id: permits/772, sites/499,
                                       # street_licenses/433, public_night_works/858, site_night_works/479
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
    kind: Kind                    # building | infrastructure
    stage: Stage                  # requested | permitted | lapsed | active | completed | unknown
    severity: Severity            # high | medium | low  (Severity.rank for comparisons)
    address: str | None
    dates: dict[str, datetime]    # tz-aware; keys per layer, see below
    geometry_wgs84: dict | None   # GeoJSON geometry, [lng, lat]
    centroid: tuple[float, float] | None   # (lat, lng)
    raw_attributes: dict          # untouched layer attributes (Hebrew-transliterated names)
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

## Open questions
- `request_stage` codes (1,2,3,5,6) on 772: meaning unknown, unused.
- Should 772 and 499 records for the same building be de-duplicated? (Possible join:
  499 `heterim` ↔ 772 `request_num`, untested.) Left to callers.
- Bot integration (which stages/severities/radius to flag on a listing) is a separate change.
