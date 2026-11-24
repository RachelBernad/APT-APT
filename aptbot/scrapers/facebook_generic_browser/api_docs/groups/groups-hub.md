# The Groups hub (top-bar "Groups" tab → `/groups/feed/`)

Last verified: 2026-11-24 (capture `20261124-094403_joined`, snapshot `0018_snap_groups_feed`). Reached by clicking the
top-bar Groups tab: an SPA navigation (no document request).

## Layout
Left rail (`[role="navigation"]`):
- **Search groups** box: searches groups only. (We used the global search instead. **Unverified** flow/queries.)
- **Your feed** (`/groups/feed/`): posts across joined groups ("Recent activity"), via `GroupsCometCrossGroupFeedContainerQuery`.
- **Discover** (`/groups/discover/`): suggestions. **Unverified.**
- **Your groups** (`/groups/joins/`, see `joined-groups-list.md`).
- **Groups you've joined** list + "See all": each row = name, avatar, "Last active N minutes ago". Clicking a row is an
  SPA navigation into the group (cheaper and more natural than a typed URL).

Main column: "Recent activity" = the cross-group feed (algorithmic ranking, English auto-translation like everywhere).

## `GroupsCometLeftRailContainerQuery` (the "Groups you've joined" list)
Variables: `{"adminGroupsCount": 3, "memberGroupsCount": 10, "scale": 2}` (page sizes).
| Field | Path |
|---|---|
| groups you admin | `data.adminGroups.groups_tab.tab_groups_list.edges[]` (empty here) |
| member groups | `data.nonAdminGroups.groups_tab.tab_groups_list.edges[].node` |
| id / name / url | `node.{id, name, url}` (`id` numeric even when the url is a vanity slug) |
| **last post time** | `node.last_post_time` (unix s). This drives "Last active …" |
| pinned | `node.has_viewer_pinned` |
| avatar | `node.profile_picture_48.uri` |
| paging | `…tab_groups_list.page_info.{end_cursor, has_next_page}` (10 per page; "See all" loads more, **Unverified** query) |

Observed: 10 groups in page 1, none of the 3 pending-request groups among them. So the list is **probably
members-only** (**Unverified** until the remaining pages are seen).

## Why this matters: the scheduler's cheapest signal
One visit to the hub tells, for every member group, when its latest post was made. A run can:
1. Click Groups (SPA, no full load) → read `last_post_time` per group.
2. Skip groups whose `last_post_time` ≤ our stored watermark (nothing new, so don't open them).
3. Visit the rest by **clicking their row** in the rail, newest-active first, then sort the feed CHRONOLOGICAL.
