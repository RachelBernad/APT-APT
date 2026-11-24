# Operation: layer `query`

Last verified: 2026-11-24

`GET {MapServer}/{layerId}/query?...`. Standard ArcGIS Server 11.1 MapServer layer query.

## Parameters we rely on (all verified)
| Param | Value we use | Notes |
|---|---|---|
| `where` | SQL, e.g. `building_stage IN ('בבניה','קיים היתר')` | Hebrew literals work URL-encoded |
| `geometry` | `lng,lat` (e.g. `34.7748,32.0754`) | **x = longitude first** |
| `geometryType` | `esriGeometryPoint` | |
| `inSR` | `4326` | Input point in WGS84 |
| `spatialRel` | `esriSpatialRelIntersects` | |
| `distance` + `units` | `200` + `esriSRUnit_Meter` | Server-side buffer around the point. **`distance=0` matches nothing** (see Caveats) |
| `outFields` | `*` or a comma list | |
| `outSR` | `4326` | Server reprojects output to WGS84 (verified on polygons) |
| `returnGeometry` | `true`/`false` | |
| `returnCountOnly` | `true` → `{"count":N}` | |
| `orderByFields` | the layer's OID field | **Required for stable paging** |
| `resultOffset` / `resultRecordCount` | `0..`, `≤2000` | |
| `f` | `json` or `geojson` (also `pbf`) | |
| `groupByFieldsForStatistics` + `outStatistics` | count by field | `onStatisticField` must be the layer's real OID name. **Capped at 2000 groups; `resultOffset` is ignored** (see Caveats) |

## Paging
- `maxRecordCount` = **2000** (service-wide).
- If more rows exist, the response has `exceededTransferLimit: true` (also in geoJSON, at top level).
- OIDs are **not contiguous** (offset 2000 starts at oid 2039 on layer 772), so page with
  `resultOffset` + `orderByFields=<oid>`, never with OID ranges.
- Verified on layer 772: offsets 0, 2000, …, 10000 return 2000, 2000, …, 642 rows (total 10642).

## Output shapes
- `f=geojson` + `outSR=4326`: `FeatureCollection`, `geometry.coordinates` in `[lng, lat]`,
  attributes in `properties`. Polygons come back as `Polygon` (rings).
- `f=json`: Esri JSON (`features[].attributes`, `features[].geometry.rings|paths|x,y`).
- Date fields (`esriFieldTypeDate`) are **epoch milliseconds (UTC)**. Many "dates" are
  plain strings instead: `DD/MM/YYYY` (479 dates, 772 `finished`), `DD/MM/YYYY HH:MM:SS`
  (`date_import` everywhere), or `DD/MM/YYYY-DD/MM/YYYY` lists (499 `ishurei_laila`); see each layer page.
- Far-future dates are used as "no expiry" sentinels (772 `expiry_date` = 2999-12-31). Treat
  any year ≥ 2900 as "none", not as a real deadline.
- Empty values come as both `""` and `null`, sometimes in the same field, and string values often
  carry trailing spaces. Normalize both before comparing.
- String fields are typically `length: 255` in the layer metadata, and long free text is
  **cut at 255 characters**, mid-word, with no marker (591 `t_mimzaim`: 553 of 577 rows).

## Caveats (verified 2026-11-24)

### `distance=0` returns nothing
A point with `distance=0` matches **no** features, even when the point lies inside a polygon.
Leave `distance`/`units` out entirely to ask "which features contain this point"; a bare
point with `spatialRel=esriSpatialRelIntersects` does that.
Verified on layer 772: for points inside permit polygons, no `distance` → 1 hit,
`distance=0` → 0, `distance=1` → 1. The client sends a bare point for `distance_m=0`.

### The same error body means "bad query" and "layer down"
`{"error":{"code":400,"message":"Failed to execute query.","details":[]}}` (HTTP 200) is
returned both for an unknown field in `where` (e.g. `no_such_field=1` on a healthy layer) and
for every query on a layer whose data source is down (see
[transport/http-and-auth.md#outages](../transport/http-and-auth.md#outages)). The body never
says which. To tell them apart, re-ask the same layer with `where=1=1&returnCountOnly=true`:
if that also fails, the layer is down. (Malformed SQL such as `1==1` gives a different body:
`"Unable to complete operation."`, `extendedCode -2147220985`.) The client raises
`TelAvivGisQueryError` vs `TelAvivGisLayerUnavailable` based on this probe.

### Grouped statistics stop at 2000 groups, and offsets don't page them
A `groupByFieldsForStatistics` query returns at most `maxRecordCount` (2000) groups, with
`exceededTransferLimit: true`. `resultOffset` is **ignored** here: offsets 0, 2000 and 6000 all
return the same first 2000 groups. So a value distribution over a high-cardinality field (a
building id, a license number) is silently truncated. To get every group, order by the group field
and page with a `where` range: `<field> > <last value of the previous page>`, and add `IS NULL` once.
Verified on 772 grouped by `ms_tik_binyan`: 3 range pages plus nulls gave 5823 groups (= the
`returnDistinctValues` count) totalling 10642 rows (= the layer count).

### Distance to a centroid is not distance to the feature
A radius query returns everything whose geometry **intersects** the buffer. Ranking those
results by centroid distance is misleading for long or large shapes:
- A street-works polygon (433) that runs along a whole street can have its centroid hundreds of
  meters away while its edge is in front of the point.
- City-scale policy polygons (801 TAMA 38 zones are whole quarters) contain the point while their
  centroid is kilometers away.
- Centroids of L-shaped or multi-part lots can fall **outside** their own polygon. Querying
  with such a centroid as a bare point may not return the lot itself.
For "how far is it", measure to the nearest edge (0 when the point is inside), not to the centroid.

## Example (verified)
```
/WM/IView2WM/MapServer/772/query?geometry=34.7748,32.0754&geometryType=esriGeometryPoint
  &inSR=4326&spatialRel=esriSpatialRelIntersects&distance=200&units=esriSRUnit_Meter
  &where=1%3D1&returnCountOnly=true&f=json
→ {"count":66}
```
