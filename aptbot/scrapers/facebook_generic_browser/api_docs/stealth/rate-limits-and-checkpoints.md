# Rate limits & checkpoints

Last verified: 2026-11-24. **No limit hit yet. Thresholds are unknown and must be discovered conservatively.**

Facebook doesn't publish limits. Public reports (Unverified) agree that the triggers are bursts of page loads right
after a login, IP changes mid-session, datacenter IPs, and sustained high-rate activity.

Our starting budget (tighten if any warning appears, loosen slowly only after weeks of clean runs):
| Budget | Start value |
|---|---|
| navigations + opened posts (Limiter) | 6–8/min, burst 2–3 |
| per session | ≤ 15 navigations, ≤ 10 min |
| sessions | ≤ 1 per 20–30 min, with jittered start times, none at night (01:00–07:00 local) |
| posts read via scroll | unbounded per se. Bounded by session length |

On any warning: stop, log, notify, and don't run again until a human has looked (`account-health.md`).
