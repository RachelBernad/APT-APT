# Layer 683: תוכניות כלל עירוניות (City-wide town plans)

Last verified: 2026-11-23 · Status: **documented, not mapped**

- URL: `…/WM/IView2WM/MapServer/683` · same `nameInDatabase` as 528 (`yk_tg_taba_union`).
  **Unverified:** that it's a definition-query subset of 528 (likely, given the name and schema).
- Geometry: Polygon. OID field: `oid_taba`. Count: 104.
- **Schema identical to layer 528.** See `528-town-plans.md` for fields and codes.

Not mapped because a city-wide plan covers every listing, so it says nothing about a specific
building. 528 already contains city-wide plans (`t_hekef='כלל עירונית'`, 74), labeled low.
To map it later: register a `LayerSpec` reusing `_map_plans`.
