# facebook_generic_browser: SPEC

Status 2026-11-24: **library built and verified live on the Mac** (8 sessions, 0 warnings). Every decision below
cites its evidence (a validation result, an `api_docs/` page or a test). The Raspberry Pi deployment and the
in-container service (§12) are designed but not yet built or run.

## 1. Goal
An independent async Python library that reads Facebook **groups** through a real, logged-in Chrome: list the
account's groups, pick one by id, read newest posts, stream and poll, search, open posts with comments, and filter
the cache. It looks like one person and **is fast**, and it serves **many concurrent callers over several tabs of the
same account**.

Non-goals: any write action (post, comment, react, join, message), stored credentials or automatic re-login, Marketplace, parsing price/rooms
authoritatively (only `hints`), coupling to `aptbot` (a future adapter will map `Post` → `LISTING_KEYS`).

Risk: against Meta's Terms. The account can be checkpointed or banned. Mitigated by §3, §7 and §8. Use a secondary account.

## 2. Requirements → how they're met
| # | Requirement | Met by | Evidence |
|---|---|---|---|
| R1 | No automation fingerprint | §3 launch config | `api_docs/stealth/browser-setup.md` (all detectors green, vanilla = "Robot") |
| R2 | Human-looking input | `human.py`, attention lock | hovercard queries fire from our pointer path (real-pointer behavior) |
| R3 | **Fast** | payload-first, request-aware waits, short random gaps | ~76 posts/min in one tab (`api_docs/feed/infinite-scroll.md`) |
| R4 | Concurrent async callers, multiple tabs, one account | §5 dispatcher + tab workers | 2.05× with 3 tabs live. `tests/unit/test_fbgb_dispatcher.py` |
| R5 | Raspberry Pi without GUI | headful Chromium under Xvfb + VNC login | **validated in Docker linux/arm64** (`api_docs/stealth/pi-headless.md`), physical Pi not yet |
| R6 | No credentials stored anywhere | login by hand, or assisted `login --stdin` (memory only) into the profile | `explore/open_profile.py` checks cookie names only. Assisted login verified in Docker |
| R7 | Account safety first | §7 budgets, §8 breaker | 8 sessions, 0 interstitials |
| R8 | Easy for future implementers | `docs/` + examples + CLI + docs-sync test | `tests/unit/test_fbgb_docs.py` |
| R9 | Reusable exploration method | `docs/exploration-playbook.md` + `explore/tools/` | used for every finding here |

## 3. Browser layer (`browser.py`)
Patchright 1.64.4 (patched Playwright; Puppeteer is Node-only and pyppeteer is dead) on the **installed Google Chrome**:
`launch_persistent_context(profile, channel="chrome", headless=False, no_viewport=True, chromium_sandbox=True,
focus_control=True, ignore_default_args=IGNORED_DEFAULT_ARGS)`. Nothing spoofed. Forbidden: `route`, `expose_*`,
`add_init_script`, headless, `scrollTo`, custom UA/headers, proxies. Re-validate (`fbgb validate`) after upgrades.

## 4. Data layer (`capture.py`, `parse.py`)
- Passive only: `CaptureHub` listens to the context's requests and responses and routes each to the job running in that
  tab. The library never sends its own requests.
- graphql operations are keyed by `fb_api_req_friendly_name` (never `doc_id`). Bodies are newline-separated chunks.
- Full loads carry first-screen data in HTML `data-sjs` islands. Parse both sources.
- Parsers find nodes by type/key (Story with `post_id`, Comment by `body+created_time+author`, Group by `id`) and
  merge duplicates field by field. Pure functions, tested on anonymized real payloads.

## 5. Concurrency (`dispatcher.py`, `tabs.py`, `jobs.py`): see `docs/concurrency.md`
```
callers → Dispatcher (single-flight by key, per-group exclusion, priority INTERACTIVE>POLL>BACKGROUND, timeout/cancel,
          halt) → max_tabs workers (one tab each, opened lazily) → jobs
jobs: input only inside TabPool.foreground (FIFO attention lock, short bursts, switch = bring_to_front + think);
      network waits outside the lock (overlap across tabs); shared Limiter; health.check after every navigation
```
Verified: bounded parallelism, coalescing, group exclusion, priorities, timeout frees the tab, halt fails all,
crashed tab recreated, attention exclusive (offline tests). Live: 2.05× with 3 tabs. Hidden tabs keep receiving
responses. Feed pagination needs the front tab.

## 6. Speed design
1. **Do less:** the feed payload has full text, photos, counts and comment previews, so no "See more", no opening posts.
2. **Request-aware waits:** after a scroll burst, look 0.35–0.7 s for the pagination *request*, and wait for its
   response only if one went out. No fixed sleeps. 36 → 76 posts/min.
3. **Short human timing:** `micro` 40–350 ms (log-normal), `think` 0.25–0.9 s at decision points, bursts of 1–3 flicks.
4. **Cache-first:** `max_age` reads, `filter_posts` from SQLite, `new_posts` skips groups whose `last_post_time` didn't move.
5. **Parallel tabs** for independent work.

## 7. Budgets (account-wide; start conservative)
`Limiter`: 8 navigations/min, burst 3, 120/hour → `BudgetExhausted(retry_after)`. Polling every 20–35 min, jittered,
not 01:00–07:00. Tabs: 3 (Mac), 2 (Pi). Chrome closes after 10 idle minutes. Loosen only after weeks clean.

## 8. Failure handling (`errors.py`, `health.py`; `docs/errors-and-safety.md`)
| Event | Behavior |
|---|---|
| checkpoint / block / login wall | `AccountAtRisk` → `<home>/HALTED` written, all calls fail with `Halted`, persists across restarts until `resume()` by a human |
| no session cookies | `NotLoggedIn` (a `Halted`) |
| private group not readable | `NotMember(group_id, join_state)` |
| hourly budget hit | `BudgetExhausted(retry_after)` |
| slow call | `RequestTimeout`, job cancelled, tab freed |
| frontend changed | `SchemaDrift` → follow `docs/extending.md` drift workflow |
| tab crash | the worker recreates its tab on the next job |

## 9. Public API (`api.py`; full reference `docs/api-reference.md`)
`status, resume, list_groups, get_group, latest_posts, stream_posts, new_posts, search_groups, search_group,
get_post, get_comments, cross_group_feed, filter_posts, close`. All coroutines, safe concurrently. CLI mirrors them
(`python -m aptbot.scrapers.facebook_generic_browser --help`).

## 10. Data model (`models.py`; `docs/data-model.md`)
`Post{group_id, group_name, post_id, url, created_at, text, text_translated, author_*, photos[Photo], photo_count,
comment_count, reaction_count, comments[Comment]|None, hints{prices, rooms, phones}}`, `Group`, `GroupHit`,
`Comment`, `Status`. Cache: `~/.fbgb/fbgb.db` (groups, posts with edit detection, fetch times, watermarks).

## 11. Deployment
- **Mac (now):** venv `~/.fbgb/venv` (own `requirements.txt`), profile/cache in `~/.fbgb/` (`docs/operations.md`).
- **No GUI (Pi), validated in Docker linux/arm64 2026-11-24:** image `deploy/fbgb-headless/`. Debian Chromium headful
  under Xvfb + openbox (32 px work-area margin) + PulseAudio null sink, custom seccomp (Docker default + namespaces)
  for the sandbox, Mesa WebGL (`FBGB_CHROME_ARGS`), maximized. Detectors: all green except the "Chromium" brand
  (unavoidable on Linux ARM). Login: assisted `login --stdin` (verified) or by hand over VNC on 127.0.0.1. Details: `api_docs/stealth/pi-headless.md`.
  Still to do on the physical Pi: same checks, `max_tabs=2`, optional V3D GPU renderer.

## 12. Integration with the bot (future phase, designed, NOT built)
The library runs as **its own process inside the bot's Docker container**, behind an authenticated local service:
```
container (supervisord as PID 1)
 ├─ Xvfb :99 + openbox (-nolisten tcp)      ├─ x11vnc -localhost (on demand, login only)
 ├─ fbgb service (user fbgb): FacebookAPI behind aiohttp on a UNIX socket /run/fbgb/api.sock
 └─ aptbot (user app): FBGBClient (aiohttp + UnixConnector, no browser deps)
```
- **No TCP port at all** (UNIX socket). Socket dir `0750 fbgb:fbgbclients`, socket `0660`.
- **Bearer token** generated at container start into `/run/fbgb/token` (`0640`), constant-time compare. Never in env/logs.
- **Peer check:** `SO_PEERCRED`, so only the bot's uid is accepted.
- **Input validation:** ids only (no arbitrary URLs), size limits, so the service can't become a browser proxy.
- Routes mirror §9 (`GET /v1/groups`, `/v1/groups/{id}/posts`, `/v1/new-posts`, `POST /v1/filter`, …). Errors map to
  `{"error": code}` (`halted` → 503). The client returns the same dataclasses.
- Image: + chromium xvfb openbox x11vnc fonts-noto fonts-culmus supervisor. The fbgb venv is at `/opt/fbgb`, volume
  `fbgb-home` (profile + cache), `shm_size: 1g`, no published ports.

## 13. Open questions
1. "View more comments" (click) to get comments beyond the embedded ~10. Comment replies (`reply_to`) not yet seen.
2. `last_post_time` exists only for the 10 most active member groups. For more groups, page the rail ("See all")
   or open the group. `new_posts` currently treats unknown as "maybe new" (opens it).
3. Clicking into groups from the hub rail (SPA) instead of typed URLs, plus switching the sort via its control.
4. The Pi: run `fbgb validate` under Xvfb, WebGL, login (assisted or VNC), then a timed run with `max_tabs=2`.
5. A private group where the account is a MEMBER (all 3 private ones are pending). Expected to work like public ones.
6. Long-term account health under daily polling: watch for "confirm it's you" after day 1.
