"""Listing Facts: the fixed, per-post result of the AI analysis.

Every key is always present (strict JSON-schema output). Booleans are three-state:
``True`` = stated, ``False`` = explicitly denied, ``None`` = not mentioned.
"""
from __future__ import annotations

from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, ConfigDict


class PostKind(str, Enum):
    RENT = "rent"                # a whole apartment for rent
    ROOMMATE = "roommate"        # a room in a shared apartment
    SUBLET = "sublet"            # short term / fixed dates
    SALE = "sale"                # an apartment for sale (never items)
    SEEKING = "seeking"          # the poster is looking for a home
    NOT_LISTING = "not_listing"  # items, services, spam: not a home at all


class ListingFacts(BaseModel):
    model_config = ConfigDict(extra="forbid", use_enum_values=False)

    ref: int
    post_kind: PostKind
    city_area_or_address: Optional[str]
    has_accurate_address: Optional[bool]
    rooms_count: Optional[float]
    square_meters: Optional[float]
    floor: Optional[float]
    elevator: Optional[bool]
    has_mamad: Optional[bool]
    has_mamak: Optional[bool]
    has_shelter: Optional[bool]
    splitted_apt: Optional[bool]
    renovated: Optional[bool]
    balcony: Optional[bool]
    balcony_type: Optional[str]
    underground_apartment: Optional[bool]
    price_ils: Optional[int]
    arnona_ils: Optional[int]
    vaad_bayit_ils: Optional[int]
    bills_included: Optional[bool]
    entry_date: Optional[str]          # "YYYY-MM-DD" | "YYYY-MM" | "immediate"
    lease_term_months: Optional[int]
    is_broker: Optional[bool]
    broker_fee: Optional[str]

    @property
    def is_home(self) -> bool:
        """A home offered (rent/roommate/sublet/sale), as opposed to seeking/not_listing."""
        return self.post_kind not in (PostKind.SEEKING, PostKind.NOT_LISTING)


class BatchResult(BaseModel):
    """What the model returns for one Analysis Batch."""
    model_config = ConfigDict(extra="forbid")
    results: List[ListingFacts]
