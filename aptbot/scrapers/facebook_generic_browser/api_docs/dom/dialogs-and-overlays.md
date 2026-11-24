# Dialogs & overlays

Last verified: 2026-11-24

Seen: none during 5 sessions (no cookie banner on a logged-in profile, no notification-permission prompt).
`[role="dialog"]` count was 0 on all pages except where we opened one ourselves.
Expected/handled (**Unverified**): the post view dialog (Escape closes it), "Join group" prompts on non-member groups,
checkpoint/"temporarily blocked" interstitials (`../session/account-health.md`), and the Chrome "Restore pages?"
bubble after an unclean shutdown (avoid by always closing the context cleanly).
