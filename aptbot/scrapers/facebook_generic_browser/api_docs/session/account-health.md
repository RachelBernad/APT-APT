# Account health

Last verified: 2026-11-24. No interstitial seen yet. The markers below are from public reports (**Unverified**) and are
encoded in `explore/health.py`.

Stop immediately (no retry, notify a human) when:
- URL path starts with `/checkpoint`, `/login`, `/recover`, `/two_step_verification`, `/authentication`, `/device-based`,
  `/confirmemail`, `/disabled`.
- A login form (`form[action*="/login"] input[name="pass"]`) appears.
- A dialog (or a page without a feed) says: "You're Temporarily Blocked", "We suspect automated behavior",
  "Confirm it's you", "Your account has been locked/suspended", "Security check", or the Hebrew equivalents.

Positive health signals: the background-noise queries fire on full loads (`../graphql/queries/background-noise.md`),
`useGroupsCometVisitMutation` succeeds, and feed queries return HTTP 200 with stories.

Session log 2026-11-24 (same account, Mac, home IP): 5 sessions within ~6 minutes, 42–66 s each, about 50 feed posts read and
1 search. No warnings.
