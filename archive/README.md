# archive/

Retired scrapers kept for reference / possible revival. **Nothing here is imported
by the bot, collected by pytest (`testpaths = tests`), or copied into the Docker
image (`.dockerignore` excludes `archive/`).** Code here is frozen as-is and may
not run against the current codebase without porting.

| Folder | What | Why retired |
|--------|------|-------------|
| [`facebook/`](./facebook/) | Facebook Marketplace scraper (BeautifulSoup) + the old generic scraper/config it plugged into | Unreliable; dropped in `f6b2068` |
| [`rentlyfly/`](./rentlyfly/) | rentlyfly.ai client (a Facebook-Groups aggregator, Tel Aviv only) + its tests | Site offline since at least 2026-11-23 (HTTP 402 `DEPLOYMENT_DISABLED`) |
