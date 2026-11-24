"""Exceptions raised by FacebookAPI. See docs/errors-and-safety.md for what a caller should do with each."""
from __future__ import annotations

from aptbot.scrapers.facebook_generic_browser.health import AccountAtRisk


class FBGBError(Exception):
    """Base class for every error this library raises (except AccountAtRisk, which is re-exported below)."""


class Halted(FBGBError):
    """The circuit breaker is open: an account-safety stop happened earlier. Nothing touches Facebook until a human
    checks the account and calls `FacebookAPI.resume()`. Cache-only calls still work."""

    def __init__(self, reason: str):
        super().__init__(f"halted: {reason}")
        self.reason = reason


class NotLoggedIn(Halted):
    """The profile has no Facebook session. A human must run the login command (docs/operations.md)."""


class NotMember(FBGBError):
    """The group's feed isn't readable for this account (private group, not a member or request pending)."""

    def __init__(self, group_id: str, join_state: str | None):
        super().__init__(f"not a member of group {group_id} (join_state={join_state})")
        self.group_id, self.join_state = group_id, join_state


class BudgetExhausted(FBGBError):
    """The account-wide navigation budget is used up. Retry after `retry_after` seconds."""

    def __init__(self, retry_after: float):
        super().__init__(f"navigation budget exhausted; retry in {retry_after:.0f}s")
        self.retry_after = retry_after


class SchemaDrift(FBGBError):
    """Facebook's frontend changed: expected data didn't arrive or didn't parse. Update api_docs/ + parse.py."""


class RequestTimeout(FBGBError):
    """The request didn't finish within its timeout. The job was cancelled and its tab released."""


__all__ = ["AccountAtRisk", "BudgetExhausted", "FBGBError", "Halted", "NotLoggedIn", "NotMember",
           "RequestTimeout", "SchemaDrift"]
