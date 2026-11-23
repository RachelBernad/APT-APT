# Operation: layer `query`

Last verified: 2026-11-23

`GET {MapServer}/{layerId}/query?...`. Standard ArcGIS Server 11.1 MapServer layer query.

## Parameters we rely on (all verified)
| Param | Value we use | Notes |
|---|---|---|
| `where` | SQL, e.g. `building_stage IN ('בבניה','קיים היתר')` | Hebrew literals work URL-encoded |
| `geometry` | `lng,lat` (e.g. `34.7748,32.0754`) | **x = longitude first** |
| `geometryType` | `esriGeometryPoint` | |
| `inSR` | `4326` | Input point in WGS84 |
| `spatialRel` | `esriSpatialRelIntersects` | |
| `distance` + `units` | `200` + `esriSRUnit_Meter` | Server-side buffer around the point |
| `outFields` | `*` or a comma list | |
| `outSR` | `4326` | Server reprojects output to WGS84 (verified on polygons) |
| `returnGeometry` | `true`/`false` | |
| `returnCountOnly` | `true` → `{"count":N}` | |
| `orderByFields` | the layer's OID field | **Required for stable paging** |
| `resultOffset` / `resultRecordCount` | `0..`, `≤2000` | |
| `f` | `json` or `geojson` (also `pbf`) | |
| `groupByFieldsForStatistics` + `outStatistics` | count by field | `onStatisticField` must be the layer's real OID name |

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
  plain strings instead (`DD/MM/YYYY`); see each layer page.

## Example (verified)
```
/WM/IView2WM/MapServer/772/query?geometry=34.7748,32.0754&geometryType=esriGeometryPoint
  &inSR=4326&spatialRel=esriSpatialRelIntersects&distance=200&units=esriSRUnit_Meter
  &where=1%3D1&returnCountOnly=true&f=json
→ {"count":66}
```
