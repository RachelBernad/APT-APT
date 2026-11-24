# URL map

Last verified: 2026-11-24 (✅ = visited in a capture session; 👁 = seen in captured links/payloads, not visited; others **Unverified**)

| URL | What | Notes |
|---|---|---|
| ✅ `/` | home feed | `CometNewsFeedPaginationQuery` on scroll |
| ✅ `/groups/feed/` | **Groups hub**: "Your feed" + left rail of joined groups with last activity | reached by clicking the top-bar Groups tab (SPA); see `../groups/groups-hub.md` |
| `/groups/discover/` | group suggestions (Unverified) | |
| ✅ `/groups/joins/` | groups you joined **and pending requests** | list rendered from HTML islands; carries `viewer_join_state` |
| ✅ `/groups/<id>/` | group main feed (Discussion) | `<id>` is numeric or a vanity slug (`secrettelaviv`) |
| ✅ `/groups/<id>/?sorting_setting=CHRONOLOGICAL` | same, **newest first** | see `../feed/sorting.md` |
| 👁 `/groups/<id>/posts/<post_id>/` | post permalink (canonical) | from `permalink_url`; not opened yet (see `../feed/multi-tab.md`) |
| 👁 `/groups/<id>/permalink/<post_id>/` | older permalink form | from `wwwURL`; same post |
| 👁 `/groups/<id>/user/<uid>/` | member's profile **within the group** | author links in group posts |
| 👁 `/photo/?fbid=<photo_id>&set=pcb.<post_id>` | photo viewer for a post's album | `pcb.<post_id>` ties the photo to the post |
| 👁 `/media/set/?set=pcb.<post_id>` | whole album of a post | |
| ✅ `/search/top?q=<q>` | search, all verticals | after typing in the search box + Enter |
| ✅ `/search/groups/?q=<q>` | search, Groups vertical | via the "Groups" filter link |
| `/search/posts/?q=<q>` | search posts (Unverified) | |
| `/groups/<id>/search/?q=<q>` | search inside one group (Unverified) | |
| `/checkpoint/…`, `/login/…`, `/two_step_verification/…` | **stop** screens | `explore/health.py` |

Tracking params (`__cft__[0]`, `__tn__`) are appended to most hrefs. Strip them before storing or comparing URLs.
