# Facebook frontend: observed reference

Their side, as seen from a real logged-in Chrome (Patchright). Our design is in `../SPEC.md`.
Rules for keeping this current: `../CLAUDE.md`. Raw captures (not committed, they contain real posts): `~/.fbgb/captures/`.

Reading order: stealth → graphql → feed → groups → posts.

- **stealth/**: [browser-setup](stealth/browser-setup.md) (validated launch config + scores) ·
  [automation-signals](stealth/automation-signals.md) · [behavior-model](stealth/behavior-model.md) (fast + human) ·
  [rate-limits-and-checkpoints](stealth/rate-limits-and-checkpoints.md) · [pi-headless](stealth/pi-headless.md)
- **session/**: [login-and-cookies](session/login-and-cookies.md) · [account-health](session/account-health.md)
- **navigation/**: [url-map](navigation/url-map.md) · [routing-spa](navigation/routing-spa.md)
- **graphql/**: [README](graphql/README.md) (request/response format, query catalogue)
  - queries/: [GroupsCometFeedRegularStoriesPaginationQuery](graphql/queries/GroupsCometFeedRegularStoriesPaginationQuery.md) ·
    [GroupsCometCrossGroupFeedContainerQuery](graphql/queries/GroupsCometCrossGroupFeedContainerQuery.md) ·
    [SearchCometResults](graphql/queries/SearchCometResults.md) · [background-noise](graphql/queries/background-noise.md)
- **groups/**: [groups-hub](groups/groups-hub.md) (start here: `last_post_time` per group) · [group-page](groups/group-page.md) · [joined-groups-list](groups/joined-groups-list.md) ·
  [group-search](groups/group-search.md) · [private-vs-public](groups/private-vs-public.md)
- **feed/**: [infinite-scroll](feed/infinite-scroll.md) (virtualization!) · [sorting](feed/sorting.md) ·
  [seen-dedup](feed/seen-dedup.md) · [multi-tab](feed/multi-tab.md)
- **posts/**: [post-dom](posts/post-dom.md) · [post-permalink](posts/post-permalink.md) · [comments](posts/comments.md) ·
  [media](posts/media.md) · [sale-style-posts](posts/sale-style-posts.md)
- **dom/**: [selectors-strategy](dom/selectors-strategy.md) · [dialogs-and-overlays](dom/dialogs-and-overlays.md)
- [i18n](i18n.md): English UI over Hebrew content, auto-translation
