# Errors & account safety

Last verified: 2026-11-24

The account is the scarce resource. A checkpoint can lock it for days, so the library fails **closed**.

## Exceptions (`from aptbot.scrapers.facebook_generic_browser import …`)
| Exception | Raised when | Caller should |
|---|---|---|
| `Halted(reason)` | a safety stop happened (now or earlier: it persists in `<home>/HALTED`) | **stop**. Alert a human. Nothing touches Facebook until `resume()` |
| `NotLoggedIn` (a `Halted`) | the profile has no session cookies | a human runs `fbgb login` |
| `AccountAtRisk` | internal: a checkpoint/block/login screen was seen. Converted to `Halted` for callers | n/a |
| `NotMember(group_id, join_state)` | the group's feed isn't readable (private + not a member / pending) | skip the group |
| `BudgetExhausted(retry_after)` | the hourly navigation cap was hit | retry after `retry_after` s (or use the cache) |
| `RequestTimeout` | the call exceeded `timeout`; its job was cancelled and the tab freed | retry later or with a bigger timeout |
| `SchemaDrift` | Facebook changed its frontend: expected data didn't arrive or didn't parse | stop using that function. Fix per [extending.md](extending.md) |
| `FBGBError` | base class of all the above except `AccountAtRisk` | n/a |

## Circuit breaker
1. Every navigation is followed by `health.check(page)`: URL path (`/checkpoint`, `/login`, `/two_step_verification`,
   …), a login form, or blocking text in a dialog ("You're Temporarily Blocked", "Confirm it's you", Hebrew
   equivalents). The list is in `health.py` and `../api_docs/session/account-health.md`.
2. On a hit, the job raises. The API writes `<home>/HALTED` (timestamp + reason), fails every queued and running
   call with `Halted`, and rejects new browser calls. **Cache reads (`filter_posts`, fresh `max_age` reads, `status`)
   keep working.**
3. A human opens the account in a normal browser (on the Pi: over VNC, see [operations.md](operations.md)), clears
   whatever Facebook asks, waits (a day after a "temporarily blocked"), then calls `resume()` / `fbgb resume`.
   **Never automate step 3.**

## Built-in safety behavior
- No scripted login, ever. No credentials anywhere in code, config or logs.
- Real Chrome, headful, nothing spoofed ([../api_docs/stealth/browser-setup.md](../api_docs/stealth/browser-setup.md)).
- Human input only: wheel scrolling, curved pointer moves, jittered clicks and typing. Input only to the front tab.
- Read-only: no likes, comments, posts, joins or messages.
- Passive data: only Facebook's own responses are read. The library never sends its own requests.
- Account-wide budget across all tabs. Chrome closes when idle (people close browsers).

## Operating limits (start here; SPEC §7)
| Budget | Default |
|---|---|
| navigations | 8/min, burst 3, 120/hour (`Limiter`) |
| polling (`new_posts`) | every 20–35 min, jittered, not 01:00–07:00 |
| tabs | 3 (Mac), 2 (Pi) |
Day-1 history: 8 sessions on 2026-11-24 with no warnings.
