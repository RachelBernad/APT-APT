# Facebook Marketplace scraper (archived)

Restored verbatim from git at `f6b2068^` (the commit before it was deleted, when the
bot was a single-user scraper). Unmaintained.

- `facebook.py` — Marketplace search + HTML parsing (needs `beautifulsoup4`, `httpx`
  era deps; the old `requirements.txt` had `beautifulsoup4==4.14.2`).
- `generic_scraper.py` — the old fan-out that called `facebook` + the old `yad2`.
- `shared_scrapers_config.py` — the old hard-coded config it imports (superseded by
  `aptbot/config.py`).

To revive: port it into `aptbot/scrapers/facebook/` returning the shared listing
dict shape (see `aptbot/models.py` `LISTING_KEYS`) and call it from
`aptbot.engine.scrape_group`, like the Yad2 scraper.
