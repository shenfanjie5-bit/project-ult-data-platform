"""Unit tests for data-platform's graph-engine Phase 1 adapters."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any

import pyarrow as pa  # type: ignore[import-untyped]
import pytest

from contracts.schemas import CandidateGraphDelta

from data_platform.cycle.graph_phase1_adapters import (
    IcebergCanonicalGraphWriter,
    IcebergEntityAnchorReader,
    PostgresCandidateDeltaReader,
    StubCanonicalGraphWriter,
    _FailClosedCanonicalGraphWriter,
)

# Shared fake dataclasses live under tests/ so the unit + integration tests
# import the same definitions without adding all test helpers as top-level
# modules on pytest's pythonpath (M2.6f1 graph-writer review fix).
from tests._graph_promotion_fakes import (
    FakeAssertionRecord as _FakeAssertionRecord,
    FakeEdgeRecord as _FakeEdgeRecord,
    FakeNodeRecord as _FakeNodeRecord,
    FakePromotionPlan as _FakePromotionPlan,
)


# ---------------------------------------------------------------------------
# PostgresCandidateDeltaReader — mock SQLAlchemy engine
# ---------------------------------------------------------------------------


class _FakeMappingResult:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def mappings(self) -> _FakeMappingResult:
        return self

    def all(self) -> list[dict[str, Any]]:
        return self._rows


class _FakeConnection:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows
        self.executed: list[tuple[Any, dict[str, Any]]] = []

    def execute(self, statement: Any, params: dict[str, Any]) -> _FakeMappingResult:
        self.executed.append((statement, params))
        return _FakeMappingResult(self._rows)

    def __enter__(self) -> _FakeConnection:
        return self

    def __exit__(self, *exc: Any) -> None:
        return None


class _FakeEngine:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows
        self.connection = _FakeConnection(rows)

    @contextmanager
    def connect(self):
        yield self.connection


def _ex3_payload(*, delta_id: str = "delta-1") -> dict[str, Any]:
    return {
        "subsystem_id": "test-subsystem",
        "delta_id": delta_id,
        "delta_type": "create_edge",
        "source_node": "node-source",
        "target_node": "node-target",
        "relation_type": "SUPPLY_CHAIN",
        "properties": {"weight": 1.0},
        "evidence": ["fact-1"],
    }


def test_candidate_delta_reader_returns_typed_deltas() -> None:
    rows = [
        {"payload": _ex3_payload(delta_id="delta-1")},
        {"payload": _ex3_payload(delta_id="delta-2")},
    ]
    engine = _FakeEngine(rows)
    reader = PostgresCandidateDeltaReader(engine=engine)

    deltas = reader.read_candidate_graph_deltas(
        cycle_id="CYCLE_20260429",
        selection_ref="cycle_candidate_selection:CYCLE_20260429",
    )

    assert len(deltas) == 2
    assert all(isinstance(delta, CandidateGraphDelta) for delta in deltas)
    assert deltas[0].delta_id == "delta-1"
    assert deltas[1].delta_id == "delta-2"
    # Confirm the bound parameters reach the SQL layer correctly.
    statement, params = engine.connection.executed[0]
    assert params == {"cycle_id": "CYCLE_20260429", "payload_type": "Ex-3"}


def test_candidate_delta_reader_sql_includes_validation_status_and_order_by() -> None:
    """Pin the WHERE / ORDER BY clauses so a regression that drops the
    validation_status filter or the ingest_seq ordering is caught here
    rather than at production runtime."""

    engine = _FakeEngine([])
    reader = PostgresCandidateDeltaReader(engine=engine)
    reader.read_candidate_graph_deltas(cycle_id="C", selection_ref="r")

    statement, _ = engine.connection.executed[0]
    sql = str(statement)

    # Filter must reject pending/rejected candidates.
    assert "validation_status = 'accepted'" in sql
    # Determinism: order by the monotonic ingest sequence.
    assert "ORDER BY cq.ingest_seq ASC" in sql
    # Joins the right tables on candidate id.
    assert "candidate_queue" in sql
    assert "cycle_candidate_selection" in sql
    assert "sel.candidate_id = cq.id" in sql
    # Bound parameters (not interpolated) for the user-supplied values.
    assert ":cycle_id" in sql
    assert ":payload_type" in sql


def test_candidate_delta_reader_propagates_pydantic_validation_error() -> None:
    """Malformed payload from PG fails Pydantic validation. Today the error
    propagates (fail-fast). Pin this contract so any future change to
    swallow / log validation errors is a deliberate decision."""

    rows = [{"payload": {"subsystem_id": "x"}}]  # missing required Ex-3 fields
    reader = PostgresCandidateDeltaReader(engine=_FakeEngine(rows))

    with pytest.raises(Exception) as exc_info:
        reader.read_candidate_graph_deltas(cycle_id="C", selection_ref="r")
    # The Pydantic validation error is the underlying cause; the exact class
    # name is from pydantic, so we only assert on message content.
    assert "delta_id" in str(exc_info.value) or "Field required" in str(
        exc_info.value
    )


def test_candidate_delta_reader_handles_string_payload_for_resilience() -> None:
    """psycopg may return JSONB as already-parsed dict OR raw string depending
    on driver version; the reader must accept both."""
    import json

    rows = [{"payload": json.dumps(_ex3_payload(delta_id="delta-stringified"))}]
    reader = PostgresCandidateDeltaReader(engine=_FakeEngine(rows))

    deltas = reader.read_candidate_graph_deltas(
        cycle_id="CYCLE_X", selection_ref="ref"
    )
    assert deltas[0].delta_id == "delta-stringified"


def test_candidate_delta_reader_returns_empty_for_no_rows() -> None:
    reader = PostgresCandidateDeltaReader(engine=_FakeEngine([]))
    deltas = reader.read_candidate_graph_deltas(
        cycle_id="CYCLE_EMPTY", selection_ref="ref"
    )
    assert deltas == []


def test_candidate_delta_reader_from_env_returns_unbound_instance() -> None:
    reader = PostgresCandidateDeltaReader.from_env()
    assert isinstance(reader, PostgresCandidateDeltaReader)
    # _engine is not constructed until first read; this is intentional so
    # the factory does not require live PG at import time.
    assert reader._engine is None


# ---------------------------------------------------------------------------
# IcebergEntityAnchorReader — fake read_canonical
# ---------------------------------------------------------------------------


class _FakeAnchorReader(IcebergEntityAnchorReader):
    def __init__(self, table_by_name: dict[str, pa.Table]) -> None:
        self._tables = table_by_name
        self.calls: list[tuple[str, list[str] | None, list[Any] | None]] = []

    def read_canonical_table(
        self, table: str, columns=None, filters=None
    ) -> pa.Table:
        self.calls.append((table, columns, filters))
        return self._tables[table]


def _alias_table(rows: list[tuple[str, str]]) -> pa.Table:
    return pa.table(
        {
            "alias": [row[0] for row in rows],
            "canonical_entity_id": [row[1] for row in rows],
        }
    )


def _entity_table(canonical_ids: list[str]) -> pa.Table:
    return pa.table({"canonical_entity_id": canonical_ids})


def test_entity_anchor_reader_maps_node_ids_to_canonical_ids() -> None:
    """The fake mimics the real read_canonical contract: it pre-filters
    the table to the rows the filters argument would have returned, since
    test does not exercise DuckDB's filter implementation."""

    reader = _FakeAnchorReader(
        {
            # Only the matched rows; the real DuckDB IN-filter applies the
            # ``alias IN (sorted node_ids)`` clause for us upstream.
            "entity_alias": _alias_table(
                [
                    ("node-1", "ent-1"),
                    ("node-2", "ent-2"),
                ]
            ),
        }
    )

    result = reader.canonical_entity_ids_for_node_ids({"node-1", "node-2"})
    assert result == {"node-1": "ent-1", "node-2": "ent-2"}


def test_entity_anchor_reader_returns_empty_dict_for_empty_node_set() -> None:
    reader = IcebergEntityAnchorReader()
    # Empty node_ids must short-circuit; no read_canonical call is made.
    assert reader.canonical_entity_ids_for_node_ids(set()) == {}


def test_entity_anchor_reader_constructs_in_filter_correctly() -> None:
    """Pin the filter argument so a regression to the wrong operator
    (e.g. ``IN`` instead of ``in``, or a typo) is caught at unit-test time
    rather than producing UnsupportedFilter at production runtime via the
    DuckDB-backed reader.py.
    """

    reader = _FakeAnchorReader({"entity_alias": _alias_table([])})

    reader.canonical_entity_ids_for_node_ids({"node-z", "node-a", "node-m"})

    assert len(reader.calls) == 1
    table, columns, filters = reader.calls[0]
    assert table == "entity_alias"
    assert columns == ["alias", "canonical_entity_id"]
    # Sorted node_ids into a deterministic IN clause; the operator string
    # MUST be lowercase 'in' to match data_platform.serving.reader._compile_filters.
    assert filters == [("alias", "in", ["node-a", "node-m", "node-z"])]


def test_entity_anchor_reader_existing_entity_ids_constructs_in_filter_correctly() -> None:
    reader = _FakeAnchorReader({"canonical_entity": _entity_table([])})

    reader.existing_entity_ids({"ent-z", "ent-a"})

    assert len(reader.calls) == 1
    table, columns, filters = reader.calls[0]
    assert table == "canonical_entity"
    assert columns == ["canonical_entity_id"]
    assert filters == [("canonical_entity_id", "in", ["ent-a", "ent-z"])]


def test_entity_anchor_reader_returns_empty_when_table_missing() -> None:
    """Phase 0 should not block on absent canonical tables — the Iceberg
    warehouse may not have materialised entity_alias yet on first cycle."""

    from data_platform.serving.reader import CanonicalTableNotFound

    class _MissingTableReader(IcebergEntityAnchorReader):
        def read_canonical_table(self, table, columns=None, filters=None):
            raise CanonicalTableNotFound(table)

    reader = _MissingTableReader()
    assert reader.canonical_entity_ids_for_node_ids({"node-x"}) == {}
    assert reader.existing_entity_ids({"ent-x"}) == set()


def test_entity_anchor_reader_filters_existing_entity_ids() -> None:
    reader = _FakeAnchorReader(
        {"canonical_entity": _entity_table(["ent-1", "ent-2"])}
    )

    present = reader.existing_entity_ids({"ent-1", "ent-2", "ent-3"})
    assert present == {"ent-1", "ent-2"}


def test_entity_anchor_reader_returns_empty_set_for_empty_input() -> None:
    reader = IcebergEntityAnchorReader()
    assert reader.existing_entity_ids(set()) == set()


# ---------------------------------------------------------------------------
# IcebergCanonicalGraphWriter (M2.6 follow-up #1: real Phase 1 write-back)
# ---------------------------------------------------------------------------


def test_stub_canonical_graph_writer_alias_now_points_at_iceberg_impl() -> None:
    """Backwards-compatibility alias: importing ``StubCanonicalGraphWriter``
    must continue to work and now resolve to the real
    ``IcebergCanonicalGraphWriter`` class."""

    assert StubCanonicalGraphWriter is IcebergCanonicalGraphWriter
    assert isinstance(StubCanonicalGraphWriter(), IcebergCanonicalGraphWriter)


def test_iceberg_writer_from_env_returns_lazy_instance() -> None:
    """``from_env`` must NOT eagerly load the catalog so the writer is
    constructable in environments without live Iceberg access (Definitions-
    load-time use case for the orchestrator)."""

    writer = IcebergCanonicalGraphWriter.from_env()
    assert isinstance(writer, IcebergCanonicalGraphWriter)
    assert writer._catalog is None


class _FakeIcebergTable:
    """Minimal Iceberg table fake that records ``overwrite`` (and legacy
    ``append``) calls for tests. ``IcebergCanonicalGraphWriter`` issues
    cycle-scoped ``overwrite`` since M2.6 follow-up #1 review-fold P1-A;
    legacy ``append`` is retained on the fake so any test path that still
    constructs an append-only fake still works.

    The ``overwrite`` signature mirrors the real pyiceberg signature
    (``overwrite_filter`` / ``snapshot_properties`` / ``case_sensitive`` /
    ``branch``) explicitly rather than swallowing future kwargs via
    ``**``. A typo or new pyiceberg kwarg surfaces as a TypeError rather
    than a silent no-op (codex review #9)."""

    def __init__(self, identifier: str) -> None:
        self.identifier = identifier
        self.appended: list[Any] = []
        # Each entry: (arrow_table, overwrite_filter, snapshot_properties).
        # snapshot_properties is recorded so a future writer evolution that
        # adds traceability properties is visible to test assertions.
        self.overwritten: list[tuple[Any, Any, dict[str, str] | None]] = []

    def append(self, table_arrow: Any) -> None:
        self.appended.append(table_arrow)

    def overwrite(
        self,
        table_arrow: Any,
        *,
        overwrite_filter: Any,
        snapshot_properties: dict[str, str] | None = None,
        case_sensitive: bool = True,
        branch: str | None = "main",
    ) -> None:
        # Record the call. Any unsupported kwarg the writer might pass
        # surfaces as TypeError (no ``**`` swallow).
        del case_sensitive, branch
        self.overwritten.append(
            (table_arrow, overwrite_filter, snapshot_properties)
        )


class _FakeCatalog:
    """Records ``load_table`` lookups + returns _FakeIcebergTable instances."""

    def __init__(self) -> None:
        self.tables: dict[str, _FakeIcebergTable] = {}
        self.lookups: list[str] = []

    def load_table(self, identifier: str) -> _FakeIcebergTable:
        self.lookups.append(identifier)
        if identifier not in self.tables:
            self.tables[identifier] = _FakeIcebergTable(identifier)
        return self.tables[identifier]


def _now() -> datetime:
    # tz-aware to match the canonical.graph_* schema's
    # GRAPH_TIMESTAMP_TYPE = pa.timestamp("us", tz="UTC")
    # (M2.6 follow-up #1 review-fold P1-B).
    return datetime(2026, 4, 30, 12, 0, 0, tzinfo=timezone.utc)


def _node_record(*, node_id: str = "n-1") -> _FakeNodeRecord:
    return _FakeNodeRecord(
        node_id=node_id,
        canonical_entity_id="ent-1",
        label="Entity",
        properties={"sector": "tech", "weight": 1.0},
        created_at=_now(),
        updated_at=_now(),
    )


def _edge_record(*, edge_id: str = "e-1") -> _FakeEdgeRecord:
    return _FakeEdgeRecord(
        edge_id=edge_id,
        source_node_id="n-1",
        target_node_id="n-2",
        relationship_type="SUPPLY_CHAIN",
        properties={"strength": 0.8, "evidence_refs": ["fact-1"]},
        weight=1.0,
        created_at=_now(),
        updated_at=_now(),
    )


def _assertion_record(*, assertion_id: str = "a-1") -> _FakeAssertionRecord:
    return _FakeAssertionRecord(
        assertion_id=assertion_id,
        source_node_id="n-1",
        target_node_id="n-2",
        assertion_type="OPERATES_IN",
        evidence={"source": "test", "score": 0.95},
        confidence=0.95,
        created_at=_now(),
    )


def _plan(
    *,
    cycle_id: str = "CYCLE_20260430",
    nodes: list[_FakeNodeRecord] | None = None,
    edges: list[_FakeEdgeRecord] | None = None,
    assertions: list[_FakeAssertionRecord] | None = None,
) -> _FakePromotionPlan:
    # Use ``is None`` (not ``or [...]``) so callers can pass ``[]`` to
    # explicitly seed an empty list; ``[] or default`` would resolve to
    # the default and break the empty-slice test.
    return _FakePromotionPlan(
        cycle_id=cycle_id,
        selection_ref=f"cycle_candidate_selection:{cycle_id}",
        delta_ids=["d-1"],
        node_records=[_node_record()] if nodes is None else nodes,
        edge_records=[_edge_record()] if edges is None else edges,
        assertion_records=[_assertion_record()] if assertions is None else assertions,
        created_at=_now(),
    )


def _overwrite_arrow(
    table: _FakeIcebergTable, idx: int = 0
) -> Any:
    """Helper: pull the arrow payload out of a recorded ``overwrite`` call
    on the fake table (the fake records
    ``(arrow, overwrite_filter, snapshot_properties)`` triples since
    the writer issues cycle-scoped overwrites for re-run idempotency
    and the fake is strict about pyiceberg's full kwarg surface)."""

    arrow, _filter, _props = table.overwritten[idx]
    return arrow


def test_iceberg_writer_overwrites_three_canonical_graph_tables() -> None:
    catalog = _FakeCatalog()
    writer = IcebergCanonicalGraphWriter(catalog=catalog)

    writer.write_canonical_records(_plan())

    # All three tables loaded + overwritten (cycle-scoped) exactly once.
    assert catalog.lookups == [
        "canonical.graph_node",
        "canonical.graph_edge",
        "canonical.graph_assertion",
    ]
    assert len(catalog.tables["canonical.graph_node"].overwritten) == 1
    assert len(catalog.tables["canonical.graph_edge"].overwritten) == 1
    assert len(catalog.tables["canonical.graph_assertion"].overwritten) == 1
    # Append must NOT be used since M2.6 follow-up #1 review-fold P1-A;
    # cycle-scoped overwrite is the recovery contract.
    assert catalog.tables["canonical.graph_node"].appended == []


def test_iceberg_writer_overwrite_filter_pins_cycle_id() -> None:
    """Re-run idempotency: the overwrite_filter MUST be EqualTo("cycle_id",
    plan.cycle_id) on every call so retries replace only that cycle's
    rows and never duplicate or touch other cycles
    (M2.6 follow-up #1 review-fold P1-A)."""

    from pyiceberg.expressions import EqualTo

    catalog = _FakeCatalog()
    writer = IcebergCanonicalGraphWriter(catalog=catalog)

    writer.write_canonical_records(_plan(cycle_id="CYCLE_20260430"))

    for identifier in (
        "canonical.graph_node",
        "canonical.graph_edge",
        "canonical.graph_assertion",
    ):
        (_arrow, overwrite_filter, _props) = (
            catalog.tables[identifier].overwritten[0]
        )
        # Pinned via class + value so a regression to ALWAYS_TRUE or a
        # different column is caught here.
        assert isinstance(overwrite_filter, EqualTo)
        assert overwrite_filter.term.name == "cycle_id"
        assert overwrite_filter.literal.value == "CYCLE_20260430"


def test_iceberg_writer_node_arrow_schema_and_values() -> None:
    catalog = _FakeCatalog()
    writer = IcebergCanonicalGraphWriter(catalog=catalog)

    writer.write_canonical_records(_plan(cycle_id="CYCLE_20260501"))

    written = _overwrite_arrow(catalog.tables["canonical.graph_node"])
    assert written.column_names == [
        "node_id",
        "canonical_entity_id",
        "label",
        "properties_json",
        "cycle_id",
        "created_at",
        "updated_at",
    ]
    assert written.column("node_id").to_pylist() == ["n-1"]
    assert written.column("canonical_entity_id").to_pylist() == ["ent-1"]
    # properties dict serialised as sorted JSON string.
    assert written.column("properties_json").to_pylist() == [
        '{"sector": "tech", "weight": 1.0}'
    ]
    assert written.column("cycle_id").to_pylist() == ["CYCLE_20260501"]


def test_iceberg_writer_edge_arrow_schema_and_values() -> None:
    catalog = _FakeCatalog()
    writer = IcebergCanonicalGraphWriter(catalog=catalog)

    writer.write_canonical_records(_plan())

    written = _overwrite_arrow(catalog.tables["canonical.graph_edge"])
    assert written.column_names == [
        "edge_id",
        "source_node_id",
        "target_node_id",
        "relationship_type",
        "properties_json",
        "weight",
        "cycle_id",
        "created_at",
        "updated_at",
    ]
    assert written.column("edge_id").to_pylist() == ["e-1"]
    assert written.column("relationship_type").to_pylist() == ["SUPPLY_CHAIN"]
    # weight column is float64 and the value 1.0 round-trips.
    assert written.column("weight").to_pylist() == [1.0]


def test_iceberg_writer_assertion_arrow_schema_and_values() -> None:
    catalog = _FakeCatalog()
    writer = IcebergCanonicalGraphWriter(catalog=catalog)

    writer.write_canonical_records(_plan())

    written = _overwrite_arrow(catalog.tables["canonical.graph_assertion"])
    assert written.column_names == [
        "assertion_id",
        "source_node_id",
        "target_node_id",
        "assertion_type",
        "evidence_json",
        "confidence",
        "cycle_id",
        "created_at",
    ]
    assert written.column("confidence").to_pylist() == [0.95]


def test_iceberg_writer_overwrites_all_three_tables_even_for_empty_slices() -> None:
    """Empty record slices MUST still issue a cycle-scoped overwrite —
    with a zero-row Arrow batch — so the prior cycle's rows for that
    same ``cycle_id`` are cleared (codex review #1). Skipping the
    overwrite on empty would leave ghost rows on retry when a cycle
    legitimately produces zero nodes, edges, or assertions."""

    catalog = _FakeCatalog()
    writer = IcebergCanonicalGraphWriter(catalog=catalog)

    plan_with_all_slices_empty = _plan(nodes=[], edges=[], assertions=[])
    writer.write_canonical_records(plan_with_all_slices_empty)

    # All three tables loaded + overwritten exactly once, regardless
    # of whether their slice was empty.
    assert catalog.lookups == [
        "canonical.graph_node",
        "canonical.graph_edge",
        "canonical.graph_assertion",
    ]
    assert len(catalog.tables["canonical.graph_node"].overwritten) == 1
    assert len(catalog.tables["canonical.graph_edge"].overwritten) == 1
    assert len(catalog.tables["canonical.graph_assertion"].overwritten) == 1

    # The empty-slice overwrites MUST carry zero-row Arrow batches with
    # the canonical schema, not None / not 1-row dummies.
    node_arrow = _overwrite_arrow(catalog.tables["canonical.graph_node"])
    assert node_arrow.num_rows == 0
    assert node_arrow.column_names == [
        "node_id",
        "canonical_entity_id",
        "label",
        "properties_json",
        "cycle_id",
        "created_at",
        "updated_at",
    ]
    edge_arrow = _overwrite_arrow(catalog.tables["canonical.graph_edge"])
    assert edge_arrow.num_rows == 0
    assert edge_arrow.column_names == [
        "edge_id",
        "source_node_id",
        "target_node_id",
        "relationship_type",
        "properties_json",
        "weight",
        "cycle_id",
        "created_at",
        "updated_at",
    ]
    assertion_arrow = _overwrite_arrow(
        catalog.tables["canonical.graph_assertion"]
    )
    assert assertion_arrow.num_rows == 0
    assert assertion_arrow.column_names == [
        "assertion_id",
        "source_node_id",
        "target_node_id",
        "assertion_type",
        "evidence_json",
        "confidence",
        "cycle_id",
        "created_at",
    ]


def test_iceberg_writer_handles_assertion_with_null_target_node_id() -> None:
    catalog = _FakeCatalog()
    writer = IcebergCanonicalGraphWriter(catalog=catalog)

    null_target_assertion = _assertion_record()
    null_target_assertion.target_node_id = None
    writer.write_canonical_records(_plan(assertions=[null_target_assertion]))

    written = _overwrite_arrow(catalog.tables["canonical.graph_assertion"])
    assert written.column("target_node_id").to_pylist() == [None]


def test_iceberg_writer_serialises_properties_with_sorted_keys() -> None:
    """Determinism: the properties_json column must be byte-stable for a
    given dict (sorted keys + default=str for non-JSON-native types)."""

    catalog = _FakeCatalog()
    writer = IcebergCanonicalGraphWriter(catalog=catalog)

    record = _node_record()
    record.properties = {"z_key": 1, "a_key": 2, "m_key": 3}
    writer.write_canonical_records(_plan(nodes=[record]))

    written = _overwrite_arrow(catalog.tables["canonical.graph_node"])
    serialised = written.column("properties_json").to_pylist()[0]
    # Sorted-key invariant: a_key < m_key < z_key.
    assert serialised == '{"a_key": 2, "m_key": 3, "z_key": 1}'


def test_fail_closed_canonical_graph_writer_raises_runtime_error() -> None:
    writer = _FailClosedCanonicalGraphWriter()
    with pytest.raises(RuntimeError, match="fail-closed"):
        writer.write_canonical_records(_plan())


def test_iceberg_writer_rejects_tz_naive_created_at_on_node() -> None:
    """codex review #7: writer must fail-fast on tz-naive datetimes
    rather than letting PyArrow silently coerce against the
    ``GRAPH_TIMESTAMP_TYPE`` (UTC-tagged) column."""

    catalog = _FakeCatalog()
    writer = IcebergCanonicalGraphWriter(catalog=catalog)

    naive_node = _node_record()
    naive_node.created_at = datetime(2026, 4, 30, 12, 0, 0)  # tz-naive

    with pytest.raises(ValueError, match="tz-aware UTC"):
        writer.write_canonical_records(_plan(nodes=[naive_node]))

    # Validation runs before the catalog is touched, so no overwrite
    # should have occurred.
    assert catalog.tables == {}


def test_iceberg_writer_rejects_non_utc_offset_on_edge() -> None:
    """codex review #7: writer must reject datetimes whose offset is
    not zero (e.g. ``+08:00``)."""

    from datetime import timedelta, timezone as _tz

    catalog = _FakeCatalog()
    writer = IcebergCanonicalGraphWriter(catalog=catalog)

    plus_eight = _tz(timedelta(hours=8))
    skewed_edge = _edge_record()
    skewed_edge.updated_at = datetime(2026, 4, 30, 20, 0, 0, tzinfo=plus_eight)

    with pytest.raises(ValueError, match="must be in UTC"):
        writer.write_canonical_records(_plan(edges=[skewed_edge]))

    assert catalog.tables == {}


def test_iceberg_writer_empty_slice_overwrite_carries_zero_row_arrow_with_cycle_filter(
) -> None:
    """codex review #1: an empty slice MUST issue a cycle-scoped
    overwrite with a zero-row Arrow batch — proving prior rows for
    the same ``cycle_id`` are cleared on retry."""

    from pyiceberg.expressions import EqualTo

    catalog = _FakeCatalog()
    writer = IcebergCanonicalGraphWriter(catalog=catalog)

    # Plan with empty node + edge + assertion slices. Every table still
    # needs a cycle-scoped overwrite so stale rows from a prior retry are
    # cleared for that cycle only.
    writer.write_canonical_records(
        _plan(
            cycle_id="CYCLE_GHOST_TEST",
            nodes=[],
            edges=[],
            assertions=[],
        )
    )

    for identifier in (
        "canonical.graph_node",
        "canonical.graph_edge",
        "canonical.graph_assertion",
    ):
        arrow, overwrite_filter, _props = catalog.tables[identifier].overwritten[0]
        # Zero rows: the overwrite is a pure cycle_id-scoped delete on the
        # table (no INSERT half).
        assert arrow.num_rows == 0, identifier
        # Filter MUST still scope to this cycle_id so other cycles' rows
        # are not affected.
        assert isinstance(overwrite_filter, EqualTo), identifier
        assert overwrite_filter.term.name == "cycle_id", identifier
        assert overwrite_filter.literal.value == "CYCLE_GHOST_TEST", identifier


def test_iceberg_writer_partial_write_exception_propagates_after_first_table(
) -> None:
    """Atomicity contract (M2.6 follow-up #1 review-fold P2-3): if the
    second table's overwrite raises, the exception MUST propagate to the
    caller (so Phase 1 marks the cycle as failed and re-runs trigger
    recovery), and the first table's overwrite MUST already be committed
    on the catalog (so the cycle-scoped overwrite on retry replaces it
    cleanly)."""

    class _FailOnEdgeIcebergTable(_FakeIcebergTable):
        def overwrite(
            self,
            table_arrow: Any,
            *,
            overwrite_filter: Any,
            snapshot_properties: dict[str, str] | None = None,
            case_sensitive: bool = True,
            branch: str | None = "main",
        ) -> None:
            raise RuntimeError("simulated catalog failure on graph_edge")

    class _PartialFailureCatalog:
        """Records overwrites on graph_node + graph_assertion normally;
        raises on the second table (graph_edge) to simulate a mid-write
        catalog failure."""

        def __init__(self) -> None:
            self.tables: dict[str, _FakeIcebergTable] = {}
            self.lookups: list[str] = []

        def load_table(self, identifier: str) -> _FakeIcebergTable:
            self.lookups.append(identifier)
            if identifier not in self.tables:
                if identifier == "canonical.graph_edge":
                    self.tables[identifier] = _FailOnEdgeIcebergTable(
                        identifier
                    )
                else:
                    self.tables[identifier] = _FakeIcebergTable(identifier)
            return self.tables[identifier]

    catalog = _PartialFailureCatalog()
    writer = IcebergCanonicalGraphWriter(catalog=catalog)

    with pytest.raises(
        RuntimeError, match="simulated catalog failure on graph_edge"
    ):
        writer.write_canonical_records(_plan())

    # graph_node overwrite must have committed BEFORE the failure on
    # graph_edge; this is what the Phase 1 retry contract relies on.
    assert "canonical.graph_node" in catalog.tables
    assert len(catalog.tables["canonical.graph_node"].overwritten) == 1

    # graph_assertion overwrite must NOT have been attempted (control
    # flow halted on the graph_edge exception). The catalog never
    # ``load_table``-ed the assertion table because the writer issues
    # the three overwrites strictly in sequence.
    assert "canonical.graph_assertion" not in catalog.tables
