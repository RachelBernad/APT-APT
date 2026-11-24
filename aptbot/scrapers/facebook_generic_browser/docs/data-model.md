# Data model

> **Time zone: every datetime is timezone-aware UTC** (`created_at`, `last_post_time`, comment times, `started_at`).
> Facebook sends unix seconds, and we never convert. Israel is UTC+2 (winter) / UTC+3 (summer). Convert for display only:
> `post.created_at.astimezone(ZoneInfo("Asia/Jerusalem"))`. In JSON (`to_dict()`) they're ISO-8601 with `+00:00`.
> The CLI prints them with a `UTC` suffix.

Last verified: 2026-11-24. Code: `models.py`. Every type has `to_dict()` → JSON-safe dict (datetimes → ISO-8601 UTC).
Facebook paths are relative to the payload node and found by key, not by full path (`parse.py`).
Field maps of the underlying queries: `../api_docs/graphql/queries/`.

## `Post`
| Field | Type | Source | Notes |
|---|---|---|---|
| `post_id` | str | Story `post_id` | numeric string. Unique per group |
| `group_id`, `group_name` | str \| None | `…title.story.to{id,name}` (Group) | |
| `url` | str \| None | `permalink_url` | canonical `/groups/<gid>/posts/<pid>/`, tracking params stripped |
| `created_at` | datetime \| None | `creation_time` | UTC |
| `text` | str | `comet_sections.content.story.message.text` | **original language**, full (no truncation) |
| `text_translated` | str \| None | `…translation.message.text` | only when the UI language differs from the post's |
| `author_id`, `author_name`, `author_url` | str \| None | `actors[0]` | url may be group-scoped `/groups/<gid>/user/<uid>/` |
| `photos` | list[Photo] | `all_subattachments.nodes[].media` | the feed delivers ~4–8 per post |
| `photo_count` | int | `all_subattachments.count` | can exceed `len(photos)` |
| `comment_count` | int \| None | `comments.total_count` | |
| `reaction_count` | int \| None | `reaction_count.count` | |
| `comments` | list[Comment] \| None | `interesting_top_level_comments` (feed) / permalink | `None` = not fetched; feed gives a 1–2 comment preview |
| `hints` | Hints | `extract.py` on `text` | best-effort, not authoritative |

## `Photo`
`id` (Facebook photo id), `url` (full-size CDN url, **signed and expiring**: the `oe=` param is a hex unix expiry),
`width`, `height`, `page_url` (facebook.com photo page). Download promptly if you need the image.

## `Comment`
`id`, `text`, `created_at`, `author_id`, `author_name` (anonymous group posts show "Anonymous participant NNN"),
`reply_to` (parent comment id for replies, else `None`).

## `Hints`
`prices: list[int]` (₪ amounts 500–100,000 near a currency word or "מחיר/price"), `rooms: float | None` ("3 חדרים",
"4.5 rooms", "דירת 3"), `phones: list[str]` (Israeli numbers, digits only). Heuristics: a sale price, a deposit or a
"1+1" can fool them. Treat them as filter helpers.

## `Group`
`id`, `name`, `url`, `last_post_time` (datetime \| None; only for the 10 most active member groups),
`privacy` ("public" \| "private" \| None), `join_state` ("MEMBER" \| "REQUESTED" \| …), `member_count` (localized
string, e.g. "139.8K members"), `description`.

## `GroupHit` (search result)
`id`, `name`, `url`, `summary` ("Public · 43K members · 30+ posts a day"), `privacy`.

## `Status`
`state` ("stopped" \| "ok" \| "halted"), `logged_in`, `halt_reason`, `tabs_busy`, `max_tabs`, `queued`, `budget_left`,
`started_at`.
