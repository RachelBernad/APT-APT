# Private vs. public groups

Last verified: 2026-11-24

| | Public (`visibility: OPEN`) | Private, member | Private, request **pending** |
|---|---|---|---|
| feed visible | ✅ (verified) | ✅ expected (**Unverified**: no accepted private group tested yet) | ❌ verified: no `[role="feed"]`, zero feed queries |
| HTML marker | `"text":"Public group"` | `"text":"Private group"` | `"text":"Private group"` + `"viewer_join_state":"REQUESTED"` |

**Gotcha:** `/groups/joins/` lists pending requests next to real memberships. The account had 3 pending requests
(including both "…מפייס לאוזן … המאגר המלא" groups). Always check `viewer_join_state == "MEMBER"` before scheduling a
group, and treat "no feed" + `REQUESTED` as *not a failure*.

Membership questions / join requests are a write action and **out of scope** for the scraper. A human joins by hand.
