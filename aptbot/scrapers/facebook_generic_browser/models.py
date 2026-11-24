"""Public data types. Plain dataclasses; `to_dict()` gives JSON-safe dicts (datetimes → ISO-8601 UTC).

Field sources (Facebook payload paths) are documented in docs/data-model.md.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


def _jsonable(v: Any) -> Any:
    if isinstance(v, datetime):
        return v.astimezone(timezone.utc).isoformat()
    if isinstance(v, Enum):
        return v.value
    if isinstance(v, dict):
        return {k: _jsonable(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_jsonable(x) for x in v]
    return v


class _Base:
    def to_dict(self) -> dict:
        return _jsonable(asdict(self))  # type: ignore[arg-type]


def ts(unix: int | float | None) -> datetime | None:
    return datetime.fromtimestamp(unix, tz=timezone.utc) if unix else None


@dataclass
class Group(_Base):
    """A group the account belongs to (or one we looked at)."""
    id: str                                  # numeric id as a string
    name: str
    url: str
    last_post_time: datetime | None = None   # newest post in the group (from the Groups hub rail)
    privacy: str | None = None               # "public" | "private" | None (unknown)
    join_state: str | None = None            # "MEMBER" | "REQUESTED" | "CAN_JOIN" | … | None (unknown)
    member_count: str | None = None          # localized text as shown, e.g. "139.8K members"
    description: str | None = None


@dataclass
class GroupHit(_Base):
    """A group found by search_groups()."""
    id: str
    name: str
    url: str
    summary: str | None = None               # e.g. "Public · 43K members · 30+ posts a day"
    privacy: str | None = None               # parsed from summary when possible


@dataclass
class Photo(_Base):
    id: str | None
    url: str | None                          # full-size CDN url (signed, expires: see api_docs/posts/media.md)
    width: int | None = None
    height: int | None = None
    page_url: str | None = None              # facebook.com/photo/?fbid=… page


@dataclass
class Hints(_Base):
    """Best-effort values extracted from the text (extract.py). Not authoritative."""
    prices: list[int] = field(default_factory=list)
    rooms: float | None = None
    phones: list[str] = field(default_factory=list)


@dataclass
class Comment(_Base):
    id: str
    text: str
    created_at: datetime | None = None
    author_id: str | None = None
    author_name: str | None = None
    reply_to: str | None = None              # parent comment id for replies


@dataclass
class Post(_Base):
    group_id: str | None
    post_id: str
    url: str | None
    created_at: datetime | None
    text: str
    group_name: str | None = None
    text_translated: str | None = None
    author_id: str | None = None
    author_name: str | None = None
    author_url: str | None = None
    photos: list[Photo] = field(default_factory=list)
    photo_count: int = 0
    comment_count: int | None = None
    reaction_count: int | None = None
    hints: Hints = field(default_factory=Hints)
    comments: list[Comment] | None = None    # None = not fetched


class State(str, Enum):
    STOPPED = "stopped"
    OK = "ok"
    HALTED = "halted"                        # AccountAtRisk seen; needs a human (resume())


@dataclass
class Status(_Base):
    state: State
    logged_in: bool | None
    halt_reason: str | None
    tabs_busy: int
    max_tabs: int
    queued: int
    budget_left: int
    started_at: datetime | None
