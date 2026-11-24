# Selector strategy

Last verified: 2026-11-24

1. Never use class names (atomic CSS, regenerated every deploy).
2. Use, in order: `role` + `aria-*` (`[role="feed"]`, `[role="main"]`, `[role="dialog"]`, `[role="banner"]`,
   `input[type="search"]`), href patterns (`a[href*="/groups/"]`, `a[href*="/search/groups"]`,
   `a[href*="set=pcb."]`), then visible text (bilingual regex, see `../i18n.md`).
3. `aria-labelledby` ids (`_r_1e_ …`) are per-render. Use the attribute's *presence*, not its value.
4. `role="article"` ≠ post (skeletons). See `../feed/infinite-scroll.md`.
5. Every read goes through Patchright (isolated world). Never inject scripts into the main world.
