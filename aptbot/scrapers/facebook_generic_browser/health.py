"""Account-health guard: detect the screens after which we must STOP (never retry automatically).

Call `await check(page)` after every navigation. Raises AccountAtRisk with a reason.
URL and text markers are documented in api_docs/session/account-health.md.
"""
from __future__ import annotations

import re

_BAD_PATHS = re.compile(r"^/(checkpoint|login|recover|two_step_verification|authentication|"
                        r"device-based|confirmemail|disabled|help/contact)", re.I)
# Hebrew + English strings of the blocking interstitials. Verified ones are marked in account-health.md.
_BAD_TEXT = re.compile(
    "|".join([
        r"You(?:'|’)re Temporarily Blocked", r"temporarily blocked", r"We suspect automated behavior",
        r"Confirm (?:it(?:'|’)s )?you", r"Your account has been (?:locked|suspended|disabled)",
        r"We(?:'|’)ve detected unusual activity", r"Security check", r"Enter the characters you see",
        r"נחסמת באופן זמני", r"חסומ(?:ה|ים)? זמנית", r"פעילות חשודה", r"אשר(?:/י)? שזה את(?:ה|ה/את)",
        r"החשבון שלך (?:ננעל|הושעה)", r"בדיקת אבטחה",
    ]), re.I)


class AccountAtRisk(RuntimeError):
    pass


async def check(page) -> None:
    from urllib.parse import urlparse

    path = urlparse(page.url).path
    if _BAD_PATHS.search(path):
        raise AccountAtRisk(f"redirected to {path}")
    if await page.locator('form[action*="/login"] input[name="pass"]').count():
        raise AccountAtRisk("login form shown (session lost)")
    # Dialogs always; the whole body only on pages without user content (a post could quote these words).
    scopes = ['[role="dialog"]']
    if not await page.locator('[role="feed"], [role="article"]').count():
        scopes.append("body")
    for sel in scopes:
        for loc in (await page.locator(sel).all())[:5]:
            try:
                text = (await loc.inner_text(timeout=2000))[:4000]
            except Exception:
                continue
            m = _BAD_TEXT.search(text)
            if m:
                raise AccountAtRisk(f"blocking text: {m.group(0)!r} in {sel}")
