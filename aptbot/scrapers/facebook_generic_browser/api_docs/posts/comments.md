# Comments

Last verified: 2026-11-24

| Where | What you get | Cost |
|---|---|---|
| group feed (`GroupsCometFeedRegularStoriesPaginationQuery`) | `interesting_top_level_comments[]`: a preview of 1–2 comments per post (not every post) + `comments.total_count` | free (already loaded) |
| permalink page HTML | the first ~10 comments, embedded with the Story | 1 navigation |
| "View more comments" | the rest | **Unverified** (needs a click; not implemented) |

Comment node (found by key: `body.text` + `created_time` + `author`): `id` (base64 `comment:<post>_<comment>`),
`legacy_fbid` (sometimes), `body.text`, `created_time` (unix), `author{__typename (User | GroupAnonAuthorProfile), id, name}`,
`depth` (0 = top level), `comment_parent.id` for replies (**Unverified**: no reply seen yet).
Anonymous group participants appear as `GroupAnonAuthorProfile` named "Anonymous participant NNN".
