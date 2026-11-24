# Behavior model: human-looking AND fast

Last verified: 2026-11-24 (code: `explore/human.py`, `explore/tabs.py`)

**Speed is a requirement.** The scraper must look human without being slow. Principles:
1. **Do less, not slower.** The feed payload already has full text, photos and counts, so no "See more" clicks, no opening
   posts, no reading dwell. Each avoided action is both faster and less bot-like.
2. **Event-driven waits.** Wait for Facebook's own response (`GroupsCometFeed…` arrived) or a landmark, each with a
   short jittered timeout. Never sleep a fixed time "to be safe".
3. **Tiny random gaps between actions.** `micro()`: log-normal, median about 110 ms, 40–350 ms, with 3% 0.4–1.2 s outliers.
   `think()`: 0.25–0.9 s, only at decision points (before a click, after switching tabs).
4. **Real input, quickly.** Wheel ticks 57–140 px with 18–200 ms gaps (a fast flick), sometimes a short scroll-back. Bezier
   mouse paths of 8–32 steps (~0.1–0.4 s). Jittered click point and a 50–130 ms press. Per-key typing gaps of 45–400 ms.
5. **Budgets, not slowness, protect the account.** An account-wide token bucket (`Limiter`) caps *navigations/opened
   posts* per minute and per session. Scrolling is free (it's what people do most).

Measured: 30 posts in 49.6 s ≈ 36 posts/min, no interstitials. Target after tuning: about 60 posts/min.
