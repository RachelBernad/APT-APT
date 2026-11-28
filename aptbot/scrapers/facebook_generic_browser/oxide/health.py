"""Account-health check for oxide pages. Same rules and patterns as `..health.check` (Chrome), computed from one JS
snapshot of the page instead of Playwright locators."""
from __future__ import annotations

from urllib.parse import urlparse

from aptbot.scrapers.facebook_generic_browser.health import _BAD_PATHS, _BAD_TEXT, AccountAtRisk

# One evaluate: the path, a login form, dialog texts, and the body text only when the page has no user content
# (a post could quote the blocking words).
SNAPSHOT_JS = """JSON.stringify((() => {
  const t = e => ((e && (e.innerText || e.textContent)) || '').slice(0, 4000);
  const feed = !!document.querySelector('[role="feed"], [role="article"]');
  return {
    url: location.href,
    login_form: !!document.querySelector('form[action*="/login"] input[name="pass"]'),
    dialogs: [...document.querySelectorAll('[role="dialog"]')].slice(0, 5).map(t),
    body: feed ? null : t(document.body),
  };
})())"""


def check_snapshot(snap: dict) -> None:
    """Raise AccountAtRisk if the snapshot shows a checkpoint, login wall or blocking interstitial."""
    path = urlparse(snap.get("url") or "").path
    if _BAD_PATHS.search(path):
        raise AccountAtRisk(f"redirected to {path}")
    if snap.get("login_form"):
        raise AccountAtRisk("login form shown (session lost)")
    for scope, texts in (('[role="dialog"]', snap.get("dialogs") or []), ("body", [snap.get("body") or ""])):
        for text in texts:
            m = _BAD_TEXT.search(text or "")
            if m:
                raise AccountAtRisk(f"blocking text: {m.group(0)!r} in {scope}")
