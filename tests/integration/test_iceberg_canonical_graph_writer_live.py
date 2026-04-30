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

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

# Shared fake dataclasses live in tests/_graph_promotion_fakes.py so unit
# + integration tests use the same definitions without adding all test helpers
# as top-level modules on pytest's pythonpath.
from tests._graph_promotion_fakes import (
    FakeAssertionRecord as _FakeAssertionRecord,
    FakeEdgeRecord as _FakeEdgeRecord,
    FakeNodeRecord as _FakeNodeRecord,
    FakePromotionPlan as _FakePromotionPlan,
)


def _now() -> datetime:
    return datetime(2026, 4, 30, 12, 0, 0, tzinfo=timezone.utc)


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


def test_iceberg_writer_writes_distinct_cycles_without_overwriting_each_other(
    tmp_path: Path,
) -> None:
    """Cycle A writes 1 node + 1 edge + 1 assertion; cycle B writes one
    of each with different IDs. After both writes, all three tables
    contain 2 rows — one per cycle (codex review #6: seed all three
    record types so the cross-cycle preservation contract is verified
    on every table, not just graph_node)."""

    pytest.importorskip("pyiceberg.catalog.sql")

    from data_platform.cycle.graph_phase1_adapters import (
        IcebergCanonicalGraphWriter,
    )

    cycle_a_plan = _FakePromotionPlan(
        cycle_id="CYCLE_20260430",
        selection_ref="cycle_candidate_selection:CYCLE_20260430",
        delta_ids=["d-1"],
        node_records=[_node(node_id="live-node-A")],
        edge_records=[_edge(edge_id="live-edge-A")],
        assertion_records=[_assertion(assertion_id="live-assert-A")],
        created_at=_now(),
    )
    cycle_b_plan = _FakePromotionPlan(
        cycle_id="CYCLE_20260501",
        selection_ref="cycle_candidate_selection:CYCLE_20260501",
        delta_ids=["d-2"],
        node_records=[_node(node_id="live-node-B")],
        edge_records=[_edge(edge_id="live-edge-B")],
        assertion_records=[_assertion(assertion_id="live-assert-B")],
        created_at=_now(),
    )

    with _live_iceberg(tmp_path) as catalog:
        writer = IcebergCanonicalGraphWriter(catalog=catalog)
        writer.write_canonical_records(cycle_a_plan)
        writer.write_canonical_records(cycle_b_plan)

        for identifier, id_column, expected_ids in (
            (
                "canonical.graph_node",
                "node_id",
                ["live-node-A", "live-node-B"],
            ),
            (
                "canonical.graph_edge",
                "edge_id",
                ["live-edge-A", "live-edge-B"],
            ),
            (
                "canonical.graph_assertion",
                "assertion_id",
                ["live-assert-A", "live-assert-B"],
            ),
        ):
            rows = catalog.load_table(identifier).scan().to_arrow()
            # Two rows total per table — one per cycle.
            assert rows.num_rows == 2, identifier
            assert (
                sorted(rows.column(id_column).to_pylist()) == expected_ids
            ), identifier
            cycle_ids = sorted(set(rows.column("cycle_id").to_pylist()))
            assert cycle_ids == [
                "CYCLE_20260430",
                "CYCLE_20260501",
            ], identifier


def test_iceberg_writer_clears_prior_cycle_rows_when_retry_slice_is_empty(
    tmp_path: Path,
) -> None:
    """codex review #1 live-Iceberg regression: a retry whose slice is
    empty for a record type MUST clear that cycle's prior rows for that
    record type, not leave them in place. Run 1 writes node + edge +
    assertion for ``CYCLE_GHOST``. Run 2 writes another cycle so each
    table has rows that must survive. Run 3 retries ``CYCLE_GHOST``
    with empty node/edge/assertion slices. After Run 3: only the other
    cycle remains in all three tables, proving the zero-row overwrite is
    scoped to ``cycle_id``."""

    pytest.importorskip("pyiceberg.catalog.sql")

    from data_platform.cycle.graph_phase1_adapters import (
        IcebergCanonicalGraphWriter,
    )

    full_plan = _FakePromotionPlan(
        cycle_id="CYCLE_GHOST",
        selection_ref="cycle_candidate_selection:CYCLE_GHOST",
        delta_ids=["d-1"],
        node_records=[_node(node_id="live-node-1")],
        edge_records=[_edge(edge_id="live-edge-1")],
        assertion_records=[_assertion(assertion_id="live-assert-1")],
        created_at=_now(),
    )
    empty_slice_plan = _FakePromotionPlan(
        cycle_id="CYCLE_GHOST",
        selection_ref="cycle_candidate_selection:CYCLE_GHOST",
        delta_ids=["d-1"],
        node_records=[],  # cycle now produces zero nodes
        edge_records=[],  # cycle now produces zero edges
        assertion_records=[],  # cycle now produces zero assertions
        created_at=_now(),
    )
    other_cycle_plan = _FakePromotionPlan(
        cycle_id="CYCLE_KEEP",
        selection_ref="cycle_candidate_selection:CYCLE_KEEP",
        delta_ids=["d-2"],
        node_records=[_node(node_id="live-node-keep")],
        edge_records=[_edge(edge_id="live-edge-keep")],
        assertion_records=[_assertion(assertion_id="live-assert-keep")],
        created_at=_now(),
    )

    with _live_iceberg(tmp_path) as catalog:
        writer = IcebergCanonicalGraphWriter(catalog=catalog)
        writer.write_canonical_records(full_plan)
        writer.write_canonical_records(other_cycle_plan)
        writer.write_canonical_records(empty_slice_plan)

        for identifier, id_column, preserved_id in (
            ("canonical.graph_node", "node_id", "live-node-keep"),
            ("canonical.graph_edge", "edge_id", "live-edge-keep"),
            (
                "canonical.graph_assertion",
                "assertion_id",
                "live-assert-keep",
            ),
        ):
            rows = catalog.load_table(identifier).scan().to_arrow()
            assert rows.num_rows == 1, identifier
            assert rows.column(id_column).to_pylist() == [preserved_id], identifier
            assert rows.column("cycle_id").to_pylist() == ["CYCLE_KEEP"], identifier


def test_iceberg_writer_is_idempotent_across_two_runs_of_same_cycle(
    tmp_path: Path,
) -> None:
    """Phase 1 retry recovery contract: re-running the same cycle's plan
    twice MUST leave the graph_node / graph_edge / graph_assertion
    tables in the same state as a single run. This is the new behaviour
    introduced by M2.6 follow-up #1 review-fold P1-A — replacing the
    blind append with a cycle-scoped Iceberg row-filter overwrite."""

    pytest.importorskip("pyiceberg.catalog.sql")

    from data_platform.cycle.graph_phase1_adapters import (
        IcebergCanonicalGraphWriter,
    )

    plan = _FakePromotionPlan(
        cycle_id="CYCLE_20260430",
        selection_ref="cycle_candidate_selection:CYCLE_20260430",
        delta_ids=["d-1"],
        node_records=[_node(node_id="live-node-1")],
        edge_records=[_edge(edge_id="live-edge-1")],
        assertion_records=[_assertion(assertion_id="live-assert-1")],
        created_at=_now(),
    )

    with _live_iceberg(tmp_path) as catalog:
        writer = IcebergCanonicalGraphWriter(catalog=catalog)
        writer.write_canonical_records(plan)
        writer.write_canonical_records(plan)  # retry with the same plan

        for identifier, expected_id_col, expected_id in (
            ("canonical.graph_node", "node_id", "live-node-1"),
            ("canonical.graph_edge", "edge_id", "live-edge-1"),
            ("canonical.graph_assertion", "assertion_id", "live-assert-1"),
        ):
            rows = catalog.load_table(identifier).scan().to_arrow()
            # Each table holds exactly one row regardless of retry count;
            # the cycle-scoped overwrite cleared the prior write before
            # appending the new one.
            assert rows.num_rows == 1, identifier
            assert rows.column(expected_id_col).to_pylist() == [
                expected_id
            ], identifier
            assert rows.column("cycle_id").to_pylist() == [
                "CYCLE_20260430"
            ], identifier
