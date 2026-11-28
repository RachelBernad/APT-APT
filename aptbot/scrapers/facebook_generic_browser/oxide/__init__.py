"""fbgb on BrowserOxide (`FB_BROWSER=oxide`): the same FacebookAPI, store and parsers, with the browser work done by a
`browser_oxide` engine in a worker subprocess instead of Chrome.

    client.py   OxideBackend (async, in the bot's process): spawns/feeds/kills the worker, RSS watchdog
    worker.py   the subprocess: line-JSON requests on stdin → jobs → line-JSON replies (python -m ...oxide.worker)
    jobs.py     group_feed / list_groups / get_group / get_post / feed_ids, synchronous, on a Driver
    driver.py   Browser wrapper: navigate, pump, human wheel/click/press, network-tap events, health check
    events.py   tap records → capture.Event (pure)
    health.py   account-health check on a page snapshot (pure; reuses ..health's patterns)

Design and caveats: aptbot/browsers/oxide/kb/aptbot-integration.md.
"""
