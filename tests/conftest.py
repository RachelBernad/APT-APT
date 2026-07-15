"""Shared fixtures + helpers for the APT-APT test suite.

Unit tests (tests/unit) are fully offline: any HTTP is monkeypatched and SQLite
uses temp files. Live tests (tests/live) hit the real Yad2 / rentlyfly APIs and
are opt-in via `-m live`.
"""
from __future__ import annotations

from typing import Dict, List, Optional

import pytest
import pytest_asyncio


# --- listing / model builders ---------------------------------------------

def make_listing(uid: str, price: Optional[int] = 5000, order_id: Optional[int] = 1,
                 source: str = "yad2", **overrides) -> Dict:
    """A minimal-but-valid normalized listing dict (the shape the router/formatter
    consume). Only the keys the code actually reads need to be present."""
    listing = {
        "uid": uid,
        "id": uid.split(":", 1)[-1],
        "source_id": uid.split(":", 1)[-1],
        "source": source,
        "type": source,
        "price": price,
        "rooms": overrides.pop("rooms", "3"),
        "size": overrides.pop("size", ""),
        "floor": overrides.pop("floor", ""),
        "city": overrides.pop("city", "תל אביב יפו"),
        "area": "",
        "hood": overrides.pop("hood", ""),
        "street": overrides.pop("street", ""),
        "location": overrides.pop("location", "רחוב הבדיקה, תל אביב"),
        "latitude": None,
        "longitude": None,
        "is_mamad": None,
        "features": overrides.pop("features", []),
        "property_type": overrides.pop("property_type", ""),
        "property_type_id": overrides.pop("property_type_id", None),
        "property_condition": overrides.pop("property_condition", None),
        "order_id": order_id,
        "description": overrides.pop("description", None),
        "images": [],
        "tags": [],
        "apartment_page_url": f"https://example.test/{uid}",
        "md5": uid,
    }
    listing.update(overrides)
    return listing


class FakeSender:
    """Stand-in for the Telegram send callable used by the router.

    Records every (chat_id, text) pair. By default every send succeeds; pass
    ``fail=True`` to simulate a transient delivery failure (the router must then
    NOT record the listing as seen).
    """

    def __init__(self, fail: bool = False):
        self.fail = fail
        self.sent: List[tuple] = []

    async def __call__(self, chat_id: int, text: str) -> bool:
        self.sent.append((chat_id, text))
        return not self.fail

    # convenience views over what was sent
    def texts(self) -> List[str]:
        return [t for _, t in self.sent]

    def count(self, needle: str) -> int:
        return sum(1 for t in self.texts() if needle in t)

    @property
    def backfill_intros(self) -> int:
        return self.count("Monitor started")

    @property
    def apartments(self) -> int:
        return self.count("Apartment Found")

    @property
    def price_changes(self) -> int:
        return self.count("Price Changed")


# --- database fixture ------------------------------------------------------

@pytest_asyncio.fixture
async def db(tmp_path):
    """A freshly-connected Database on an isolated temp-file SQLite DB."""
    from db import Database
    database = await Database(str(tmp_path / "test_bot.db")).connect()
    try:
        yield database
    finally:
        await database.close()


@pytest.fixture
def sender():
    return FakeSender()
