"""Typed results for the Tel Aviv GIS construction library (terms: docs/GLOSSARY.md)."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional, Tuple


class Kind(str, Enum):
    BUILDING = "building"
    INFRASTRUCTURE = "infrastructure"


class Stage(str, Enum):
    REQUESTED = "requested"
    PERMITTED = "permitted"
    LAPSED = "lapsed"
    ACTIVE = "active"
    COMPLETED = "completed"
    UNKNOWN = "unknown"


class Severity(str, Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"

    @property
    def rank(self) -> int:
        return {"low": 0, "medium": 1, "high": 2}[self.value]


@dataclass(frozen=True)
class ConstructionFeature:
    """One piece of construction-related work at a place, as one GIS layer records it."""
    layer: str                                  # registry key, e.g. "permits"
    layer_id: int                               # MapServer layer id, e.g. 772
    object_id: int
    kind: Kind
    stage: Stage
    severity: Severity
    address: Optional[str]
    dates: Dict[str, datetime]                  # tz-aware; keys per layer (see layers.py)
    geometry_wgs84: Optional[Dict[str, Any]]    # GeoJSON geometry, [lng, lat] order
    centroid: Optional[Tuple[float, float]]     # (lat, lng)
    raw_attributes: Dict[str, Any] = field(repr=False, default_factory=dict)
