# Exploration playbook: mapping any logged-in web app without getting flagged

Last verified: 2026-11-24. Written generically: Facebook is only the worked example (`→ FB:` lines). Use it
for the next site too (X/Twitter, Instagram, LinkedIn, Marketplace, …). Tools live in `../explore/tools/`.

**Goal of an exploration:** for each thing you want (a list, a feed, a search, a detail page) know (1) which URL or
click gets you there, (2) which response carries the data and where inside it, (3) what triggers more of it,
(4) what the page looks like when something is wrong, all observed live, documented, and reproducible.

---

## 0. Ground rules (before touching the site)
- **The account is the scarce resource.** Short sessions (≤ 1–2 min), few navigations, ≥ 20–30 min between bursts
  of sessions, stop on the first warning. Explore in a handful of focused sessions, not one long crawl.
- **Read-only.** Never click like/follow/join/post/message while exploring. Never type in a composer.
- **Credentials never pass through code or chat.** A human logs in by hand into a persistent profile.
- **Write down what you observed, when, and from which capture.** Inferred = mark it **Unverified**.

## 1. Make the browser boring first (no site involved)
1. Use a patched controller on the **real installed browser**, headful, persistent profile, nothing spoofed
   (`browser.py`). Fake fingerprints create inconsistencies that are worse than the truth.
2. Run the detectors and A/B against a vanilla control: `explore/validate.py` (rebrowser bot-detector, sannysoft,
   CreepJS, BrowserScan). Read every red/amber row, find its cause in the driver's default switches
   (`chrome://version` shows the real command line), fix, re-run.
3. Check multi-tab behavior with a probe: only the front tab should be `visible`+focused; hidden tabs must have
   throttled timers (count `setInterval` ticks in a background tab).
   → FB: Playwright's defaults leaked storage-partitioning, unthrottled hidden tabs and "all tabs focused";
   fixed with `ignore_default_args`, `focus_control=True`, `chromium_sandbox=True`.

## 2. Log in once, by hand
`explore/open_profile.py`: open the profile, the human logs in (2FA, "trust this device"), closes the window.
Detect success by **cookie names only** (FB: `c_user`, `xs`), never values. Always close the context cleanly.

## 3. Capture everything the page fetches, passively
Attach a context-wide response listener **before** navigating (`capture.Capture` / `capture.CaptureHub`):
- Index every response in `index.jsonl` (kind, operation name, bytes, page url, time).
- Save bodies of the API calls (keep only non-secret request params: operation name, variables. Store only the
  *names* of tokens) and of main-frame documents (HTML).
- Also listen to **requests**: "a request went out" is the signal you'll later wait on.
- Name things by the app's own operation name. → FB: graphql form field `fb_api_req_friendly_name`
  (`doc_id` changes every deploy). Other apps: GraphQL `operationName`, REST path, `x-*` operation headers.
- Expect streamed bodies: several JSON objects per response (`@stream`/`@defer`), anti-hijack prefixes like
  `for (;;);` or `)]}'`. Split by lines and strip the prefix (`capture.split_chunks`).

## 4. Look inside the HTML too: first-screen data is often not an API call
Many SPAs ship the first screen's data inside the document (`<script type="application/json">`, `__NEXT_DATA__`,
`window.__APOLLO_STATE__`, FB `data-sjs` islands). Full loads and in-app navigations deliver the **same data by
different routes**. Parse both.
- `tools/islands.py PAGE.html --find "<a value you see on screen>"`: locate any visible value in the raw HTML.
- `tools/islands.py PAGE.html --keys "<regex>"`: list keys across all islands (e.g. `join_state|privacy|count`).
→ FB: group privacy, member count and membership state, and the first feed post(s) come only in the HTML.

## 5. Find where each field lives: shape first, then search by key
- `tools/shape.py RESPONSE.json all 40 --grep "<field or value>"`: every key path with its type + example.
- Grep the shape for what you see on screen (a name, a price, a timestamp) to find its path.
- **Parse by key, not by path:** find nodes by type (`__typename == "Story"`) and pick fields by key, with
  fallbacks. Deep paths shift between deploys; keys and types rarely do (`parse.py`).
- Check for **duplicates/merges:** the same entity often appears in several lists on one page with different
  fields (→ FB: the groups rail has `last_post_time`, the full list doesn't. Merging by first occurrence lost the
  times on a live run). Merge field by field.
- Look for "free" extras the page already loads (→ FB: inline comment previews in the feed, full post text even
  when the UI truncates with "See more", original + translated text).

## 6. Learn what triggers more data: the timeline
`tools/timeline.py CAPTURE_DIR [--only REGEX]` lists documents and calls with timestamps. Read it next to what you did:
- Which action (scroll, click, hover, typing) caused which request? → FB: hovering a name fires a hovercard query
  (good: our pointer looks real). Typing fires a typeahead query per burst. Scrolling near the bottom sentinel
  fires the next feed page (3 posts).
- Which requests are **noise** on every load (→ FB: ~10 shell queries per full load)? They're a health signal and
  a reason to prefer in-app navigation over typed URLs.
- Anything **you didn't intend**? → FB: scrolling a permalink for comments loaded an unrelated feed below the post
  (7 MB). Found only by reading the timeline.

## 7. Understand the DOM only enough to click
- Save snapshots (`page.content()` + screenshot) at each milestone. Never rely on class names (atomic CSS).
- `tools/domcensus.py SNAPSHOT.html [--container role=feed]`: landmark roles of the page, and per child of a
  list: text length, images, aria/data attributes, links.
- Detect **virtualization**: children with zero text but kept height / `data-virtualized` / only an
  `aria-labelledby`. The DOM then can't be read after scrolling. Use the network data. → FB: ~3 live posts out of 19 slots.
  `role="article"` marked only loading skeletons.
- Select by role/aria/href/visible text (bilingual), in that order. Ids like `_r_1e_` are per-render.

## 8. Map navigation: URLs, sort orders, states
- Build a URL map (`api_docs/navigation/url-map.md`) marking ✅ visited / 👁 seen in payloads / Unverified.
- Try **URL parameters for ordering and filtering** and confirm they reach the API variables
  (→ FB: `?sorting_setting=CHRONOLOGICAL` → `sortingSetting: "CHRONOLOGICAL"`, strictly newest-first).
- Map **membership / permission states** and how each looks (→ FB: `viewer_join_state` MEMBER vs REQUESTED. A
  pending private group shows no feed at all, and "joined groups" pages list pending requests too).
- Note which navigations are full loads vs. in-app (document request present or not).

## 9. Make waits event-driven (speed without looking robotic)
- After an action, wait for **the request it should cause**, briefly (0.35–0.7 s). If one went out, wait for
  **that response** (timeout ~8 s). If none did, act again immediately. → FB: feed speed went from 36 to ~76
  posts/min with this change alone.
- Never wait for a page load that doesn't happen: in-app navigations have no document (→ FB: search results).
- Keep human-input timing (short random gaps, curved pointer, wheel ticks), but **do fewer actions**: every action
  you can skip because the data is already in a payload is both faster and less bot-like.

## 10. Multi-tab behavior
Probe with two or three tabs: who is `visible`, does a hidden tab keep receiving responses (count responses that
arrive while their tab isn't in front), does pagination only fire in the front tab? → FB: hidden tabs do receive
responses (15–30 of 44–64), feed pagination happens only while scrolling (front). So: input serialized behind one
"attention" lock, network waits overlapped → ~2× with 3 tabs.

## 11. Know the "something is wrong" screens
Collect URL paths, login forms and dialog texts of checkpoints, blocks, "confirm it's you" and rate-limit pages (both
languages) into a health check that runs after every navigation, and **stop** on a hit (`health.py`). Positive
signals matter too: the normal background calls firing means a healthy logged-in shell.

## 12. Turn observations into tests and docs, safely
- `explore/anonymize.py` → fixtures: fake people (every User-typed object, including the viewer/account owner),
  synthetic post/comment bodies, no phone numbers, no signed CDN URLs, tracking blobs dropped. `--html` keeps only the
  JSON-island objects your parsers need.
- **Leak-scan fixtures** before committing: grep for real names you saw, phone patterns, `fbcdn`/signed-URL markers,
  tokens. (→ FB: the first pass leaked the account owner's own name via the viewer object.)
- Fixtures **from several pages/orders**: a test that passes on one capture can fail on the next page load
  (element order changed). Add an order-independence test.
- Document in `api_docs/` (their side) with `Last verified:` + CHANGELOG. Keep `SPEC.md` (our side) in sync.

## Checklist for a new site
1. [ ] validate.py green on the real browser; multi-tab probe OK
2. [ ] manual login; cookie-name check
3. [ ] session 1: home + the main list page, with capture on → timeline, shape, islands, domcensus
4. [ ] operation catalogue (name → page → what it carries → trigger)
5. [ ] URL map, sort/filter params verified against API variables, permission states
6. [ ] event-driven scroll/pagination loop measured (items/min) with 1 tab, then 2–3 tabs
7. [ ] health screens catalogued; stop-on-warning in place
8. [ ] anonymized fixtures + parser tests (incl. order independence); leak scan clean
9. [ ] docs: api_docs pages + CHANGELOG + SPEC; every inferred item marked Unverified
