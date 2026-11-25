# Rate limits & checkpoints

Last verified: 2026-11-24. **No limit hit yet. Thresholds are unknown and must be discovered conservatively.**

Facebook doesn't publish limits. Public reports (Unverified) agree that the triggers are bursts of page loads right
after a login, IP changes mid-session, datacenter IPs, and sustained high-rate activity.

We set no navigation budget (decision 2026-11-25). Our rules: one tab acts at a time, a 10–30 ms random gap before
each operation, human input timing (`human.py`), and:
| Rule | Value |
|---|---|
| sessions | ≤ 1 per 20–30 min, with jittered start times, none at night (01:00–07:00 local) |
| posts read via scroll | unbounded per se. Bounded by session length |

On any warning: stop, log, notify, and don't run again until a human has looked (`account-health.md`).
