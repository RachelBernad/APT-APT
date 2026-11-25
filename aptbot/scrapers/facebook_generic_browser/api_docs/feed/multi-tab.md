# Multiple tabs

Last verified: 2026-11-24 (validation probe, see `../stealth/browser-setup.md`. Facebook-side behavior partly **Unverified**)

Verified browser behavior with `focus_control=True`:
- Only the front tab is `visible` + focused. Others are `hidden`, timers throttled to about 1 Hz.
- `context.new_page()` opens and **focuses** a new tab. Our `TabPool.open_url_in_background` brings the opener back to
  the front right after, so the new tab loads while hidden (like ctrl+click).
- Cmd/Ctrl+click on a real link opens a genuine background tab (`hidden`, 0×0 until shown).

Facebook-side, verified 2026-11-24 (`examples/concurrent.py`, hidden-tab counters in `CaptureHub.stats`):
- Hidden tabs **do** receive responses: 30/64 and 15/44 graphql responses arrived while their tab wasn't in front.
- Feed pagination requests fired only while the feed was being scrolled, i.e. in front (0 of 8 feed responses hidden).
- 2 group feeds + 1 group search over 3 tabs: 76.6 s wall vs 157.2 s summed → **2.05×**, 11 tab switches, no warnings.

Earlier open questions (now answered above):
- Does a hidden tab's group feed still paginate? (Expected **no**: pagination needs scrolling, and scrolling a hidden tab
  is a tell.) ⇒ background tabs are for **loading single pages** (permalinks), not for feeds.
- Does a permalink page in a background tab deliver its post + comments graphql while hidden? The deep session's group
  had no posts (pending membership), so this wasn't exercised.

Design rule: one tab drives the feed (foreground). Up to 2 extra tabs load permalinks for posts that need comments.
Input only ever goes to the foreground tab (`TabPool.foreground`), and every attention grab waits a 10–30 ms random gap first (`tabs.pace`).
