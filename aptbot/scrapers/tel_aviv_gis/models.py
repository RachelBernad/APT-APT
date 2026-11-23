"""Typed results for the Tel Aviv GIS construction library (terms: docs/GLOSSARY.md)."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, FrozenSet, Optional, Tuple


class Kind(str, Enum):
    BUILDING = "building"              # work on a building (permit, site, project)
    INFRASTRUCTURE = "infrastructure"  # street / utility works
    PLAN = "plan"                      # town plan (תב"ע) that enables future construction
    HAZARD = "hazard"                  # dangerous-building order
    POLICY = "policy"                  # policy zone (e.g. TAMA 38 rules for an area)


class Stage(str, Enum):
    # Plans and policy
    PROPOSED = "proposed"      # plan before approval (pre-deposit, deposited, objections, 77/78)
    APPROVED = "approved"      # plan / policy in force
    CANCELLED = "cancelled"    # plan cancelled, shelved or historic
    # Works
    REQUESTED = "requested"
    PERMITTED = "permitted"
    LAPSED = "lapsed"
    ACTIVE = "active"
    COMPLETED = "completed"
    # Hazards
    ORDERED = "ordered"        # dangerous-building order or notice issued
    UNKNOWN = "unknown"


class Severity(str, Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"

    @property
    def rank(self) -> int:
        return {"low": 0, "medium": 1, "high": 2}[self.value]


# Tags: cross-layer facts worth filtering on (see layers.py for which layer sets which).
TAG_TAMA38_REBUILD = "tama38_rebuild"      # TAMA 38 demolish-and-rebuild
TAG_TAMA38_ADDITION = "tama38_addition"    # TAMA 38 strengthening + additions
TAG_URBAN_RENEWAL = "urban_renewal"        # plan includes urban renewal / pinui-binui
TAG_DEMOLITION_ORDER = "demolition_order"  # dangerous building: demolition or sealing
TAG_IMMEDIATE_ORDER = "immediate_order"    # dangerous building: immediate-action order (צו 7)


@dataclass(frozen=True)
class ConstructionFeature:
    """Anything the GIS records that signals construction at a place, from a plan to active
    work, as one layer records it."""
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
    title: Optional[str] = None                 # plan name, project name, order type...
    url: Optional[str] = None                   # public document link when the layer has one
    tags: FrozenSet[str] = frozenset()
