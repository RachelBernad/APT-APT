"""The single place that knows which backend serves the model.

Today: langchain-openai's experimental ``_ChatOpenAICodex`` (ChatGPT Codex backend,
Responses API, always streamed, ``store=False``). It is a private class, so the
langchain-openai version is pinned in requirements.txt; if it moves, only this file
changes. langchain is imported lazily so the bot loads without it when AI is off.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

ORIGINATOR = "aptbot"


class UsageLimited(Exception):
    """The subscription's usage window is exhausted (HTTP 429 ``usage_limit_reached``).
    Shared with the user's own Codex use: don't retry before ``resets_at``."""

    def __init__(self, resets_at: datetime, message: str = "usage limit reached"):
        super().__init__(f"{message}; resets at {resets_at:%Y-%m-%d %H:%M} UTC")
        self.resets_at = resets_at


def make_chat_model(model: str, *, instructions: str, effort: str = "low",
                    store: Optional[Path] = None, timeout: float = 120.0) -> Any:
    """A LangChain chat model (async-capable) authed by the ChatGPT OAuth token store."""
    import warnings

    from langchain_openai.chat_models.codex import _ChatOpenAICodex

    from aptbot.listing_ai.llm.auth import provider

    with warnings.catch_warnings():          # the class warns "experimental" on every construction
        warnings.simplefilter("ignore")
        return _ChatOpenAICodex(model=model, token_provider=provider(store), instructions=instructions,
                                reasoning={"effort": effort}, originator=ORIGINATOR,
                                timeout=timeout, max_retries=1)


def usage_limit_from(exc: BaseException) -> Optional[UsageLimited]:
    """Map a backend error to :class:`UsageLimited` when it is the subscription limit."""
    body = getattr(exc, "body", None)
    if not isinstance(body, dict) or body.get("type") != "usage_limit_reached":
        return None
    ts = body.get("resets_at")
    if isinstance(ts, (int, float)):
        resets = datetime.fromtimestamp(ts, timezone.utc)
    else:
        secs = body.get("resets_in_seconds") or 15 * 60
        resets = datetime.now(timezone.utc) + timedelta(seconds=float(secs))
    return UsageLimited(resets, str(body.get("message") or "usage limit reached"))
