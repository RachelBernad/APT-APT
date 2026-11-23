# tel_aviv_gis: instructions for Claude

This folder is a self-contained async library over the Tel Aviv-Yafo municipal GIS
(ArcGIS REST behind gisn.tel-aviv.gov.il). Two doc trees, two different jobs:

- `docs/SPEC.md`: **our** contract (scope, decisions, public API, open questions).
- `docs/GLOSSARY.md`: domain terms only. No implementation details.
- `api_docs/`: **their** API, as observed (endpoints, params, fields, codes, quirks).

## Keep `api_docs/` in sync with reality (mandatory)

Whenever you observe the remote API behaving differently from what `api_docs/` says
(a new or removed layer or field, a renamed service, changed codes or domains, a new
required header or token, a different error, paging limits, spatial reference, response
shape), in the same session you must:

1. Update the affected page(s) under `api_docs/` and set their `Last verified:` line
   to today's date.
2. Add a dated entry to `api_docs/CHANGELOG.md`: what changed, how you observed it
   (browser capture, live replay, `catalog.py` diff, failing live test), and the impact on code.
3. If code depends on the change (`layers.py` mappings, `client.py` params), fix the
   code and tests in the same change, or record it under "Open questions" in `docs/SPEC.md`.

Rules:
- Document only verified behavior. Mark anything inferred as **Unverified** and say why.
- Keep sample responses trimmed (a few features, long rings shortened) and free of secrets
  or tokens.
- One page per layer at `api_docs/services/<Service>/layers/<id>-<slug>.md`. Update the
  index in `api_docs/README.md` when you add or remove pages.
- `python -m aptbot.scrapers.tel_aviv_gis.catalog` dumps the live service/layer directory.
  Diff it against `api_docs/` to detect drift.

## Code conventions
- Absolute imports (`from aptbot.scrapers.tel_aviv_gis import client`). Settings go in
  `aptbot/config.py` as `TLV_GIS_*` env-overridable constants.
- Runtime is plain HTTP (aiohttp). Never use a browser at runtime; the browser is only
  for analysis.
- No new deps unless they ship arm64 wheels (the bot runs on a Raspberry Pi).
- Tests: `tests/unit/test_tel_aviv_gis_*.py` (offline) and
  `tests/live/test_tel_aviv_gis_live.py` (`@pytest.mark.live`).
