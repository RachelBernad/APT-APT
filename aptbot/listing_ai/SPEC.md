# listing_ai: AI summaries of apartment posts over a ChatGPT subscription

Status: built and validated on 2026-11-25 (langchain-openai 1.8.5, model `gpt-6-luna`, ChatGPT Plus).
Terms such as **Post**, **Listing Facts**, **Post kind** and **Analysis batch** are defined in the root [`GLOSSARY.md`](../../GLOSSARY.md).
The choice of auth is recorded in [`docs/adr/0001-chatgpt-subscription-oauth.md`](../../docs/adr/0001-chatgpt-subscription-oauth.md).

## 1. What it does
Each Facebook post the bot is about to send is turned into **Listing Facts**: a fixed set of fields such as kind, address,
price, rooms, ממ״ד and floor. The bot shows them as a compact Hebrew summary above the raw post. The AI only adds
information: when it is off, failing or rate-limited, posts are sent exactly as before.

```
fb_engine.route_fb_posts ──(posts about to be sent)──▶ ListingAnalyzer.analyze
        │                                              │ dedupe → cache (fb_analysis) → batches of 15
        │                                              │ → ≤3 in flight → strict JSON schema → map by ref
        ◀──────────── {content_hash: ListingFacts} ────┘
fb_engine.wanted(facts): kind ∈ FB_POST_KINDS (default "rent"), else marked seen and not sent
send_post(chat, post, facts, search=monitor) → formatting.format_fb_summary(facts, monitor) + format_fb_post(summary=…)
```

**Filtering by kind:** `FB_POST_KINDS` (comma-separated, default `rent`, empty = every kind) decides which
analyzed posts reach the chat. The others are recorded as seen, so they are never analyzed or offered again.
Posts without facts (AI off, failed or paused) are always sent. The taster analyzes up to 3× its cap so it can
still fill the cap with wanted kinds.

**Message layout:** header (group · time), summary, the "לפוסט בפייסבוק ↗" link, a `────────` separator, then the
raw text. The link sits in the top block so it shows without scrolling. When the text overflows a photo caption,
the album caption carries header + summary + link and the text follows as its own message.

## 2. Layout
| Module | Role |
|---|---|
| `llm/auth.py` | Token store path (`AI_AUTH_PATH`, default `~/.aptbot/chatgpt-auth.json`), device and browser login, `status`. |
| `llm/client.py` | `make_chat_model()`: the **only** file that knows the backend. Also `UsageLimited` and its error mapping. |
| `schema.py` | `ListingFacts`, `PostKind`, `BatchResult` (pydantic, strict-schema friendly). |
| `prompt.py` | `SYSTEM_PROMPT`, `PROMPT_VERSION`, `post_item()`, `build_batch_payload()`. |
| `analyzer.py` | `ListingAnalyzer`, `PostInput`, `AnalyzerState`, `AnalysisCache` protocol. |
| `__main__.py` | `python -m aptbot.listing_ai login [--browser] \| status \| try POSTS.json`. |

`llm/` has no apartment knowledge and can be reused. Importing `aptbot.listing_ai` does **not** import langchain
(a test checks this). Only `make_chat_model()` does, so the bot runs without langchain when `AI_ENABLED=0`.
`listing_ai` never imports the Facebook scraper: `fb_engine.analyzer_inputs()` converts fbgb `Post`s into `PostInput`s.

## 3. The OAuth wrapper
- **Backend:** `langchain_openai.chat_models.codex._ChatOpenAICodex`. It targets `https://chatgpt.com/backend-api/codex`
  with the Responses API, `store=False` and streaming (all forced by the class). It sends the prompt as a cacheable
  `developer` message. The base URL is pinned and an API key is refused, so the token cannot be sent anywhere else.
  It is a **private, experimental** class, so `langchain-openai` / `langchain-core` are pinned in `requirements.txt`.
  To bump them: rerun the lab benchmark (§8), then `python -m aptbot.listing_ai try`.
- **Async and concurrency:** `ainvoke` throughout. The token provider (`_FileChatGPTOAuthTokenProvider`) refreshes 5 min
  before expiry under a thread lock plus a cross-process `flock`, writes atomically with mode 0600, and its
  `aget_token` runs off the event loop. One analyzer serves all monitors and users. `AI_CONCURRENCY` (default 3)
  caps batches in flight; 3 parallel batches were measured to finish in the time of one.
- **Token store:** our own file and never `~/.codex/auth.json`, because two clients rotating one refresh token log each
  other out. It lives outside the repo: `~/.aptbot/` locally, the `aptbot-ai` Docker volume on the Pi.
- **Login:** `login` uses a device code by default (headless: it prints a URL and a code). It speaks Codex CLI's device
  protocol: JSON `{client_id}` → `device_auth_id` + `user_code`, then poll, then the server returns the PKCE verifier,
  then a token exchange with `redirect_uri=https://auth.openai.com/deviceauth/callback`. langchain-openai 1.8.5's
  `login_chatgpt_device` sends a form body with the wrong fields and fails with HTTP 400, so it is not used.
  `login --browser` uses langchain's loopback flow on port 1455.
- **Model:** `AI_MODEL` (default `gpt-6-luna`) with reasoning effort `AI_EFFORT=low`. Temperature is not sent;
  determinism comes from the strict schema, explicit rules and the cache (§6).

## 4. The Analysis batch contract
Input (user message), compact JSON, Hebrew kept as is:
```json
{"posts":[{"ref":0,"posted_at":"2026-11-24T10:30+02:00","group_name":"…","photo_count":6,"text":"…"}]}
```
- `ref` is the index within the batch; results map back by `ref`. Unknown or duplicate refs are dropped. A missing ref
  means that post gets no facts.
- `posted_at` is in Israel time, because relative dates in the text ("כניסה ב-1.12") refer to it.
- `group_name` gives the city context (all watched groups are in Tel Aviv). `photo_count` is a weak spam signal.
  Photos and comments are not sent.

Output: `{"results":[ListingFacts, …]}` via `with_structured_output(BatchResult, method="json_schema", strict=True)`.
Every key is always present. Booleans have three states: `true` = stated, `false` = explicitly denied, `null` = not mentioned.

| Field | Type | Notes |
|---|---|---|
| `post_kind` | enum | `rent`, `roommate`, `sublet`, `sale` (homes only), `seeking`, `not_listing` (items, services, spam) |
| `city_area_or_address` | str | `"<street> <number>, <neighborhood>"`, missing parts omitted, never translated |
| `has_accurate_address` | bool | true only for street + house number |
| `rooms_count`, `square_meters`, `floor` | float | Israeli room count; sq ft ×0.0929; קרקע=0, מרתף=-1 |
| `elevator`, `has_mamad`, `has_mamak`, `has_shelter`, `splitted_apt`, `renovated`, `balcony`, `underground_apartment` | bool | |
| `balcony_type` | str | as written ("מרפסת שמש") |
| `price_ils`, `arnona_ils`, `vaad_bayit_ils` | int | monthly; bimonthly arnona ÷2; sale = total price |
| `bills_included` | bool | "כולל הכל (חוץ מחשמל)" counts as true |
| `entry_date` | str | `YYYY-MM-DD`, `YYYY-MM`, or `immediate` |
| `lease_term_months` | int | |
| `is_broker`, `broker_fee` | bool, str | |

Post-processing in `_map_results`: for `seeking` and `not_listing` every field except the kind is cleared, and repeated
address parts are collapsed ("פלורנטין, פלורנטין").

## 5. The system prompt
`prompt.SYSTEM_PROMPT` is about 4.1k characters of English instructions, using the Hebrew cue words the posts actually
contain. It holds the general null/false rules, the kind definitions (including "למכירה about an item is not_listing"),
and one rule per field. It is a stable prefix so the backend can cache it. **Bump `PROMPT_VERSION` whenever the prompt
or schema changes**: cached facts from another version are ignored and the post is analyzed again.

## 6. Caching, determinism, cost
- Table `fb_analysis(content_hash PK, model, prompt_version, facts_json, analyzed_at)`. It is added with
  `CREATE TABLE IF NOT EXISTS`, so there is no `SCHEMA_VERSION` bump and monitors survive. One analysis per content
  hash is shared by every monitor and user, which also makes the result for a post deterministic once stored.
- Only posts that are about to be sent are analyzed, not the whole taster pool.
- Measured: about 520 tokens per post (15 posts: 5.5k input + 2.3k output) and about 45 s per 15-post batch.
  This is charged to the ChatGPT plan's shared 5-hour Codex usage window.

## 7. Failure policy
| Event | Behavior |
|---|---|
| Exception or timeout (`AI_TIMEOUT`, default 120 s) | one retry, then the batch's posts are sent raw; logged; `state.failed` += n |
| HTTP 429 `usage_limit_reached` | no retry; **pause until `resets_at`** (the window is shared with the user's own Codex use); posts are sent raw; `/status` shows "⏸ paused until HH:MM" |
| No token store or langchain missing at startup | analyzer off, with one log line; the bot runs as before |
| Cache read/write error | logged, then treated as a cache miss |

`ListingAnalyzer.analyze` never raises, and `fb_engine.make_analyze` adds a second guard.

## 8. Security (the repo is public)
- Token store: outside the repo, 0600, our own file. `.gitignore` and `.dockerignore` also exclude `.aptbot/` and
  `chatgpt-auth.json*`. Nothing logs or prints tokens; `status` prints only the plan and expiry.
- Experiments with real posts happen outside the repo (`~/aptbot-ai-lab/`). Repo tests use synthetic posts only.
- Before committing: `git diff --cached | grep -nE 'eyJ[A-Za-z0-9_-]{10,}|refresh_token"\s*:|sk-[A-Za-z0-9]{20}'` must be empty.

## 9. Validation (2026-11-25, `~/aptbot-ai-lab`, outside the repo)
- 15 real hand-labeled posts: **338/338 fields correct** after prompt tuning (99.7% before).
- 5 synthetic posts: a sofa, a "selling everything" list, an English desk ad, an apartment for sale, and an English rental
  in sq ft that also sells its sofa. Item ads were classified `not_listing`, the sale `sale`, and the sq ft converted (538 → 50 m²).
- 3 batches in parallel: 29.7 s in total, the same as one batch. A repeat run left 5 of 338 fields different, all on
  ambiguous text; this is moot after caching.
- End to end through `python -m aptbot.listing_ai try` on 22 posts: 22 analyzed, 0 failed.

## 10. Later
Per-monitor filters on facts (hide `not_listing`, require ממ״ד, a price cap), neighborhood or GIS lookup from
`city_area_or_address`, and possibly analyzing Yad2 descriptions.
