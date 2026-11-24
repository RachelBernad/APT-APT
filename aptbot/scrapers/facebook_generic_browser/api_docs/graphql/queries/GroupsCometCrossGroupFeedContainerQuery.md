# GroupsCometCrossGroupFeedContainerQuery

Last verified: 2026-11-24 (capture `20261124-094403_joined`, file `0017_…`)

Fired by `/groups/feed/` ("Your feed" across all joined groups). Variables: `feedLocation: "GROUP"`,
`renderLocation: "groups_tab"`, `feedbackSource: 69`. Response: 3 chunks. Chunk 0 is the container, chunk 1 a
`$stream$` edge at path `viewer.groups_tab.cross_group_feed.edges[n]`, chunk 2 the `$defer$…page_info`.

Story nodes have **the same schema** as [GroupsCometFeedRegularStoriesPaginationQuery](GroupsCometFeedRegularStoriesPaginationQuery.md).
The group is at `…context_layout.story.comet_sections.title.story.to`. Useful as a cheap "what's new across all my
groups" sweep, but its ranking is algorithmic (not chronological) and it mixes in non-apartment groups.
