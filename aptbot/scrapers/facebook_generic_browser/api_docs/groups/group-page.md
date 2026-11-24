# Group page

Last verified: 2026-11-24 (public group, 139.8K members, captures `-094608_group`, `-094808_harvest`)

## Metadata (HTML islands on a full load)
| Field | Where | Example |
|---|---|---|
| name | `<title>` = `(N) <group name> \| Facebook` (N = unread notifications) | |
| privacy | `"visibility":"OPEN"` (public). Also `privacy_info.title.text` = "Public group" / "Private group" | |
| member count | `group_member_profiles.formatted_count_text` | `"139.8K members"` (localized string) |
| viewer membership | `"viewer_join_state"` | `MEMBER` / `REQUESTED` / (Unverified: `CAN_JOIN`, …) |

## Layout landmarks (role-based; classes are obfuscated)
- `[role="banner"]`: top bar (search input `input[type="search"]`, notifications, messenger, account).
- `[role="main"]`: the group: header, tabs, composer, sort control, then **`[role="feed"]`** (exactly one).
- `[role="navigation"]`: left rail (on `/groups/feed/`: your groups list).
- A floating "new post" (pencil) button sits bottom-right.

See `../feed/infinite-scroll.md` for the feed DOM and `../posts/post-dom.md` for a post.
