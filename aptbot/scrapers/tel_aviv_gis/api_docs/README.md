# Tel Aviv-Yafo GIS: remote API reference

Observed behavior of the municipal ArcGIS REST API behind gisn.tel-aviv.gov.il.
Only verified facts go here; inferred items are marked **Unverified**.
Maintenance rules: `../CLAUDE.md`. History of changes: [CHANGELOG.md](CHANGELOG.md).

Last verified: 2026-11-23

## Layout
```
api_docs/
  README.md        this index: base URLs, spatial refs, page index
  CHANGELOG.md     dated log of observed API changes
  transport/       auth-and-tokens, headers-and-anti-bot, limits-and-errors
  viewer/          what the iView web viewer requests and in what order
  operations/      ArcGIS operations as this server implements them (query, identify, find)
  services/<Service>/README.md               service metadata and layer list
  services/<Service>/layers/<id>-<slug>.md   one page per layer
```

## Layer page template
Each `services/<Service>/layers/<id>-<slug>.md` has these sections:
- **Header:** layer name (Hebrew + English), REST URL, `Last verified:` date
- **Geometry:** type, native spatial ref, whether `outSR=4326` works
- **Capabilities:** query/identify, maxRecordCount, paging, supported spatialRel/distance
- **Fields:** table of name | alias (Hebrew) | type | meaning | sample values
- **Domains/codes:** coded values (e.g. permit status) and their meaning
- **Sample:** one working query URL and a trimmed response
- **Mapping:** how fields map to Kind/Stage/Severity/dates/address in `layers.py`

## Base URLs
| What | URL |
|---|---|
| REST root | `https://gisn.tel-aviv.gov.il/arcgis/rest/services` (folders: OpenData, Test, Utilities, WM) |
| Main MapServer (Web Mercator) | `…/WM/IView2WM/MapServer` |
| Same layers in ITM (EPSG:2039) | `…/IView2/MapServer` |
| Master plan תא/5500 | `…/WM/TA5500WM2026/MapServer` (and the older `…/WM/TA5500WM`) |
| Viewer metadata | `https://gisn.tel-aviv.gov.il/metadata/*.json` |
| Public documents | plan docs `gisn.tel-aviv.gov.il/tabaot/docs.aspx`, policy PDFs `gisn.tel-aviv.gov.il/taba_raster/`, permit archive `archive-binyan.tel-aviv.gov.il`, national `mavat.iplan.gov.il` |

We query in WGS84 (`inSR=4326&outSR=4326`), so the native SR doesn't matter to callers.

## Page index
- [transport/http-and-auth.md](transport/http-and-auth.md): no auth, F5 edge, gzip/BOM quirks, errors-as-200, CDN cache
- [operations/query.md](operations/query.md): query params, server-side radius, paging, output shapes
- [viewer/iview-network-flow.md](viewer/iview-network-flow.md): what the web viewer loads (layer catalog dumps)
- [services/IView2WM/README.md](services/IView2WM/README.md): service facts plus the construction-layer candidate table
  - [layers/772-building-permits.md](services/IView2WM/layers/772-building-permits.md)
  - [layers/499-construction-sites.md](services/IView2WM/layers/499-construction-sites.md)
  - [layers/433-street-works-licenses.md](services/IView2WM/layers/433-street-works-licenses.md)
  - [layers/858-night-works-public.md](services/IView2WM/layers/858-night-works-public.md)
  - [layers/479-night-works-sites.md](services/IView2WM/layers/479-night-works-sites.md)
  - [layers/528-town-plans.md](services/IView2WM/layers/528-town-plans.md)
  - [layers/591-dangerous-buildings.md](services/IView2WM/layers/591-dangerous-buildings.md)
  - [layers/801-tama38-zones.md](services/IView2WM/layers/801-tama38-zones.md)
  - [layers/746-affordable-housing.md](services/IView2WM/layers/746-affordable-housing.md)
  - reference only (not mapped): [683-city-wide-plans.md](services/IView2WM/layers/683-city-wide-plans.md) ·
    [852-853-roadworks.md](services/IView2WM/layers/852-853-roadworks.md) ·
    [760-763-633-954-light-rail-metro.md](services/IView2WM/layers/760-763-633-954-light-rail-metro.md)
- [services/TA5500WM/README.md](services/TA5500WM/README.md): master plan תא/5500 (zoning, building heights), reference only

Page header convention: `Last verified: <date> · Status: mapped (<key>) | documented, not mapped`.
