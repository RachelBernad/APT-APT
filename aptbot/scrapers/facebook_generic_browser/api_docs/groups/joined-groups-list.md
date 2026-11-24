# Joined groups list

Last verified: 2026-11-24 (capture `-094403_joined`)

**Best source (verified 2026-11-24): one full load of `/groups/joins/`.** Its HTML islands contain both the left
rail (10 most active member groups **with `last_post_time`**) and the complete "Your groups" list (all members
+ pending requests with `viewer_join_state`, names, urls), so `list_groups` costs 1 navigation (~2 s).
⚠️ The same group appears in both lists, and their order in the payload varies between loads. Merge field by field
(a first-occurrence merge lost every `last_post_time` on one live run).

Two sources:
1. **`/groups/joins/`** (full load): groups as `a[href*="/groups/<id>/"]` inside `[role="main"]`. The name is the link's
   first text line. Each group object in the HTML islands has `viewer_join_state` (`MEMBER` 22×, `REQUESTED` 6×
   in the capture, with duplicates). No graphql on scroll for 14 groups (all inline).
2. **`GroupsCometLeftRailContainerQuery`** (graphql, on the Groups hub): member groups with `id`, `name`, `url`,
   **`last_post_time`**, 10 per page. Preferred: it comes free with clicking the Groups tab. Field map in [groups-hub.md](groups-hub.md).

Filter out non-group paths: `feed`, `joins`, `discover`, `create`, `notifications`. Group ids are numeric or a
vanity slug. Store both; the numeric id is in `to.id` of any story.
