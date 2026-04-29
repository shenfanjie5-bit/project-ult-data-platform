"""Shared fake records for ``IcebergCanonicalGraphWriter`` tests.

Both the unit tests in ``tests/cycle/test_graph_phase1_adapters.py`` and
the live-Iceberg integration tests in
``tests/integration/test_iceberg_canonical_graph_writer_live.py`` need
plain-data stand-ins for graph-engine's
``GraphNodeRecord`` / ``GraphEdgeRecord`` / ``GraphAssertionRecord`` /
``PromotionPlan`` Pydantic models. Defining them once here avoids
the silent drift the M2.6 follow-up #1 review caught
(integration test passed tz-aware datetimes; unit test passed
tz-naive ones).

These dataclasses are deliberately minimal — they expose only the
attributes ``IcebergCanonicalGraphWriter._*_records_to_arrow`` reads,
plus a ``cycle_id`` on the plan. Each test file still owns its own
factory helpers (``_node_record`` etc.) so test data shape stays
local to the test that needs it.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass
class FakeNodeRecord:
    node_id: str
    canonical_entity_id: str
    label: str
    properties: dict[str, Any]
    created_at: datetime
    updated_at: datetime


@dataclass
class FakeEdgeRecord:
    edge_id: str
    source_node_id: str
    target_node_id: str
    relationship_type: str
    properties: dict[str, Any]
    weight: float
    created_at: datetime
    updated_at: datetime


@dataclass
class FakeAssertionRecord:
    assertion_id: str
    source_node_id: str
    target_node_id: str | None
    assertion_type: str
    evidence: dict[str, Any]
    confidence: float
    created_at: datetime


@dataclass
class FakePromotionPlan:
    cycle_id: str
    selection_ref: str
    delta_ids: list[str]
    node_records: list[FakeNodeRecord]
    edge_records: list[FakeEdgeRecord]
    assertion_records: list[FakeAssertionRecord]
    created_at: datetime
