# GroupsCometFeedRegularStoriesPaginationQuery

Last verified: 2026-11-24 (doc_id seen: varies per deploy; 10 calls in `20261124-094808_harvest`)

**The workhorse.** Fired by the group feed's infinite-scroll sentinel. Each response carries **3 posts** (`count: 3`)
with their **full text** (no "See more" truncation), photos, author, timestamps, reaction/comment counts and permalinks.

## Request variables (relevant)
```json
{"count": 3, "cursor": "<opaque base64>", "id": "<group_id>", "youthIntegrityHostID": "<group_id>",
 "feedLocation": "GROUP", "feedType": "DISCUSSION", "sortingSetting": "CHRONOLOGICAL", "renderLocation": "group",
 "stream_initial_count": 1, "scale": 2, "filterTopicId": null, "focusCommentID": null, "…relayprovider": true}
```
`sortingSetting` mirrors the `?sorting_setting=` URL param (see `../../feed/sorting.md`). Only `CHRONOLOGICAL` was
exercised. Other values are **Unverified**.

## Response: Story node paths
Story = any `node` with `__typename == "Story"` and `post_id`. It arrives as streamed chunks: one edge per chunk,
plus a deferred `page_info`. Paths relative to the Story node (same schema as `CometNewsFeedPaginationQuery`
and the cross-group feed):

| Field | Path | Example / notes |
|---|---|---|
| post id | `post_id` | `"3357580591104433"` (numeric string, unique per group post) |
| story id | `id` | base64 `UzpfST…` (Relay global id) |
| created | `creation_time` | unix seconds |
| text (original) | `comet_sections.content.story.message.text` | full text, original language (Hebrew) |
| text (translated) | `comet_sections.content.story.comet_sections.message_container.story.translation.message.text` | only if the viewer's UI language differs |
| permalink | `comet_sections.content.story.wwwURL` / `…message.story.permalink_url` | `/groups/<gid>/permalink/<post_id>/` and `/groups/<gid>/posts/<post_id>/` |
| author | `comet_sections.content.story.actors[0].{id,name,url}` | in groups, `url` may be `/groups/<gid>/user/<uid>/` |
| group | `comet_sections.context_layout.story.comet_sections.title.story.to.{id,name,url}` | |
| photos | `attachments[].styles.attachment.all_subattachments.{count,nodes[]}` | `nodes[].media.{id, image.uri, viewer_image.{uri,width,height}}`, `nodes[].url` (photo page) |
| single photo | `attachments[].styles.attachment.media.{photo_image,image}.uri` | **Unverified** path variant (single-photo posts) |
| album link | `attachments[].styles.attachment.url` | `/media/set/?set=pcb.<post_id>` |
| comment count | `comet_sections.feedback.story.story_ufi_container.story.feedback_context.feedback_target_with_context.comment_rendering_instance.comments.total_count` | int |
| reactions | `…comet_ufi_summary_and_actions_renderer.feedback.top_reactions.edges[].{reaction_count, node.id}` | |
| dedup key | `deduplication_key` (on the edge's `data`) | |

Parsing rule: **search by key, not by full path.** Deep paths shift between deploys. Find the Story node, then pick
`message.text` from `comet_sections.content.story` (fall back to any `message.text` in the node), and so on.

## Pagination facts (measured)
- First 3–6 posts come with the page (HTML islands on a full load). Every later batch = 1 request = 3 posts.
- A new request fires when the feed's bottom sentinel enters the viewport. With 900–1600 px wheel scrolls, about every
  second scroll triggers one. 30 posts arrived in 49.6 s (36/min) with bounded waits (see `../../feed/infinite-scroll.md`).
- With `CHRONOLOGICAL`, posts arrive strictly newest → oldest (verified over 30 posts spanning 10 h).
