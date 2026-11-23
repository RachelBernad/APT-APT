# Viewer: what iView requests on load

Last verified: 2026-11-23 (Chrome DevTools capture of `https://gisn.tel-aviv.gov.il/iview2js4/index.aspx`)

The viewer is an ASP.NET page plus the ArcGIS JS API 4.30 (`js.arcgis.com/4.30`) and a
bundled app script (`/iView2js4/bundles/MyJavaScripts?v=…`, ~2 MB).

## Load sequence (XHR/fetch, in order)
1. `/metadata/manifest.json`: list of metadata files with size and sha256 (use it to detect changes).
2. `/metadata/layers.sizes.json`: `[{code, count}]`, feature count per layer id.
3. `/metadata/dataimage_url.json`, `dictionary.json`, `dictionary.ims.json`: UI strings and images.
4. `/metadata/dtm.10.txt`, `gushim.json`, `Index_Benny_Clean_all.csv`: terrain, parcels, address index.
5. `/metadata/iviewservice.json` (~2.4 MB): **a full dump of the MapServer's layer JSON**
   (`{layers:[256], tables:[2]}`), each with `fields`, `geometryType`, `advancedQueryCapabilities`,
   `subLayers`. Equivalent to `MapServer/layers?f=json`.
6. `/metadata/iviewservice5500.json`: the same for the TA5500 service.
7. `/metadata/mekomot.json`, `metadata.json` (620 entries), `streets*.json`, `sug_mismach.csv`, `tabaot*.json`.
   - `metadata.json` per layer: `layerCode` (= layer id), `layerName`, `layerNameEng`,
     `nameInDatabase`, `lastUpdateDate`, and flags `isInternet`/`isIdentify`/`isFreeSearch`/….

All are plain GETs (cache-busting `?_=<ms>` is optional) and work without browser state.

## Services referenced by the bundle
- `https://gisn.tel-aviv.gov.il/arcgis/rest/services/WM/IView2WM/MapServer` (main, Web Mercator)
- `.../WM/IView2Ortho{YYYY}WM/MapServer` (aerial imagery by year)
- `.../WM/ExportWebMapIViewWM/GPServer/Export` (print)
- `.../Utilities/Geometry/GeometryServer`
- Vector basemaps on `tiles.arcgis.com/tiles/PcGFyTym9yKZBRgz/...`
- Internal hosts (`dgt-ags01`, `dgt-ags02`, `gisportal02.tlv.gov.il`) appear in the code
  **Unverified:** they look intranet-only and were not tested. Ignore them.
