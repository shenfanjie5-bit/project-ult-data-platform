"""Live Iceberg integration test for ``IcebergCanonicalGraphWriter``.

Provisions an isolated PostgreSQL-backed Iceberg SQL catalog + a
filesystem warehouse path under ``tmp_path`` (no compose-PG dependency
since the SQL catalog can use SQLite for ephemeral testing). Registers
the three ``canonical.graph_*`` Iceberg tables, exercises the full
``IcebergCanonicalGraphWriter.write_canonical_records()`` chain against a
real ``PromotionPlan``, then reads the rows back via DuckDB to verify the
round-trip.

This is the closest unit-test approximation of the M2.6 deployment-env
write path. It does NOT require compose-PG to be up because it uses
SQLite for the catalog backend (still a real ``SqlCatalog``, just with a
file-based driver).
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest


def _now() -> datetime:
    return datetime(2026, 4, 30, 12, 0, 0, tzinfo=timezone.utc)


@dataclass
class _FakeNodeRecord:
    node_id: str
    canonical_entity_id: str
    label: str
    properties: dict[str, Any]
    created_at: datetime
    updated_at: datetime


@dataclass
class _FakeEdgeRecord:
    edge_id: str
    source_node_id: str
    target_node_id: str
    relationship_type: str
    properties: dict[str, Any]
    weight: float
    created_at: datetime
    updated_at: datetime


@dataclass
class _FakeAssertionRecord:
    assertion_id: str
    source_node_id: str
    target_node_id: str | None
    assertion_type: str
    evidence: dict[str, Any]
    confidence: float
    created_at: datetime


@dataclass
class _FakePromotionPlan:
    cycle_id: str
    selection_ref: str
    delta_ids: list[str]
    node_records: list[_FakeNodeRecord]
    edge_records: list[_FakeEdgeRecord]
    assertion_records: list[_FakeAssertionRecord]
    created_at: datetime


@contextmanager
def _live_iceberg(tmp_path: Path) -> Iterator[Any]:
    """Provision a fresh SQLite-backed Iceberg catalog + warehouse for one
    test, register the canonical.graph_* tables, and tear down after."""

    pyiceberg_catalog_sql = pytest.importorskip("pyiceberg.catalog.sql")
    SqlCatalog = pyiceberg_catalog_sql.SqlCatalog

    from data_platform.ddl.iceberg_tables import (
        CANONICAL_GRAPH_PROMOTION_TABLE_SPECS,
        ensure_tables,
    )

    warehouse_path = tmp_path / "warehouse"
    warehouse_path.mkdir(parents=True, exist_ok=True)
    catalog_db = tmp_path / "catalog.db"
    catalog = SqlCatalog(
        "graph_writer_test",
        **{
            "uri": f"sqlite:///{catalog_db}",
            "warehouse": f"file://{warehouse_path}",
        },
    )
    catalog.create_namespace_if_not_exists("canonical")
    ensure_tables(catalog, list(CANONICAL_GRAPH_PROMOTION_TABLE_SPECS))
    try:
        yield catalog
    finally:
        # SQLite catalog file is cleaned up with tmp_path automatically.
        pass


def _node(node_id: str = "live-node-1") -> _FakeNodeRecord:
    return _FakeNodeRecord(
        node_id=node_id,
        canonical_entity_id="ent-live",
        label="Entity",
        properties={"sector": "tech", "weight": 1.0},
        created_at=_now(),
        updated_at=_now(),
    )


def _edge(edge_id: str = "live-edge-1") -> _FakeEdgeRecord:
    return _FakeEdgeRecord(
        edge_id=edge_id,
        source_node_id="live-node-1",
        target_node_id="live-node-2",
        relationship_type="SUPPLY_CHAIN",
        properties={"strength": 0.8, "evidence_refs": ["fact-1"]},
        weight=1.0,
        created_at=_now(),
        updated_at=_now(),
    )


def _assertion(assertion_id: str = "live-assert-1") -> _FakeAssertionRecord:
    return _FakeAssertionRecord(
        assertion_id=assertion_id,
        source_node_id="live-node-1",
        target_node_id="live-node-2",
        assertion_type="OPERATES_IN",
        evidence={"source": "test", "score": 0.95},
        confidence=0.95,
        created_at=_now(),
    )


def test_iceberg_canonical_graph_writer_round_trip_against_live_catalog(
    tmp_path: Path,
) -> None:
    """End-to-end: write 1 node + 1 edge + 1 assertion via the real writer,
    then read the rows back through ``catalog.load_table()`` + Arrow scan
    to verify the snapshot was committed and the row shape matches."""

    pytest.importorskip("pyiceberg.catalog.sql")

    from data_platform.cycle.graph_phase1_adapters import (
        IcebergCanonicalGraphWriter,
    )

    plan = _FakePromotionPlan(
        cycle_id="CYCLE_20260430",
        selection_ref="cycle_candidate_selection:CYCLE_20260430",
        delta_ids=["d-1"],
        node_records=[_node()],
        edge_records=[_edge()],
        assertion_records=[_assertion()],
        created_at=_now(),
    )

    with _live_iceberg(tmp_path) as catalog:
        writer = IcebergCanonicalGraphWriter(catalog=catalog)
        writer.write_canonical_records(plan)

        # Read each table back via Arrow scan and pin the row count + values.
        node_table = catalog.load_table("canonical.graph_node")
        node_rows = node_table.scan().to_arrow()
        assert node_rows.num_rows == 1
        assert node_rows.column("node_id").to_pylist() == ["live-node-1"]
        assert node_rows.column("cycle_id").to_pylist() == ["CYCLE_20260430"]
        assert (
            node_rows.column("properties_json").to_pylist()[0]
            == '{"sector": "tech", "weight": 1.0}'
        )

        edge_table = catalog.load_table("canonical.graph_edge")
        edge_rows = edge_table.scan().to_arrow()
        assert edge_rows.num_rows == 1
        assert edge_rows.column("edge_id").to_pylist() == ["live-edge-1"]
        assert edge_rows.column("relationship_type").to_pylist() == [
            "SUPPLY_CHAIN"
        ]

        assertion_table = catalog.load_table("canonical.graph_assertion")
        assertion_rows = assertion_table.scan().to_arrow()
        assert assertion_rows.num_rows == 1
        assert assertion_rows.column("assertion_id").to_pylist() == [
            "live-assert-1"
        ]
        assert assertion_rows.column("confidence").to_pylist() == [0.95]


def test_iceberg_writer_appends_across_two_cycles(tmp_path: Path) -> None:
    """Cycle A writes 1 node; cycle B writes 1 different node. After both
    appends, the canonical.graph_node table contains 2 rows (the writer
    is append-only, not overwrite-only)."""

    pytest.importorskip("pyiceberg.catalog.sql")

    from data_platform.cycle.graph_phase1_adapters import (
        IcebergCanonicalGraphWriter,
    )

    cycle_a_plan = _FakePromotionPlan(
        cycle_id="CYCLE_20260430",
        selection_ref="cycle_candidate_selection:CYCLE_20260430",
        delta_ids=["d-1"],
        node_records=[_node(node_id="live-node-A")],
        edge_records=[],
        assertion_records=[],
        created_at=_now(),
    )
    cycle_b_plan = _FakePromotionPlan(
        cycle_id="CYCLE_20260501",
        selection_ref="cycle_candidate_selection:CYCLE_20260501",
        delta_ids=["d-2"],
        node_records=[_node(node_id="live-node-B")],
        edge_records=[],
        assertion_records=[],
        created_at=_now(),
    )

    with _live_iceberg(tmp_path) as catalog:
        writer = IcebergCanonicalGraphWriter(catalog=catalog)
        writer.write_canonical_records(cycle_a_plan)
        writer.write_canonical_records(cycle_b_plan)

        node_rows = (
            catalog.load_table("canonical.graph_node").scan().to_arrow()
        )
        # Two rows total — one per cycle.
        assert node_rows.num_rows == 2
        node_ids = sorted(node_rows.column("node_id").to_pylist())
        assert node_ids == ["live-node-A", "live-node-B"]
        cycle_ids = sorted(set(node_rows.column("cycle_id").to_pylist()))
        assert cycle_ids == ["CYCLE_20260430", "CYCLE_20260501"]
