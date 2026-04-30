"""data-platform adapters that satisfy graph-engine's Phase 1 Protocols.

graph-engine Phase 1 promotion needs three data-platform-owned read/write
boundaries (per ``GraphPhase1Service.__init__`` constructor):

* ``CandidateDeltaReader`` — reads frozen ``Ex3CandidateGraphDelta`` payloads
  from PostgreSQL ``candidate_queue`` joined to ``cycle_candidate_selection``.
* ``EntityAnchorReader`` — looks up ``canonical_entity`` / ``entity_alias``
  Iceberg tables via DuckDB to resolve graph node ids to canonical entity ids
  and to existence-check declared anchors.
* ``CanonicalWriter`` — persists the ``PromotionPlan`` produced by Phase 1
  back into Layer A canonical storage. **This adapter is a stub for M2.3a-2;**
  the actual persistence path for graph promotion records is not yet defined
  in data-platform's canonical Iceberg schema (see M2.6 follow-up TODO).

Module ownership rationale (per CLAUDE.md):

* ``EntityAnchorReader`` impl lives here, not in entity-registry, because
  ``data-platform`` OWNs the storage of ``canonical_entity`` / ``entity_alias``
  Iceberg tables (data-platform/CLAUDE.md OWN list). entity-registry OWNs the
  rules for canonical id *generation*; the reader path is data access.
* ``CandidateDeltaReader`` impl is data-platform-owned because the cycle
  control tables (`cycle_candidate_selection`, `candidate_queue`) are
  data-platform's responsibility (CLAUDE.md OWN list).
* ``CanonicalWriter`` write-back is also data-platform's surface.

These adapters are imported by ``graph_engine.providers.phase1`` via the
``build_graph_phase1_runtime_from_env()`` factory.
"""

from __future__ import annotations

import re
from threading import Lock
from typing import TYPE_CHECKING, Any

from data_platform.cycle.models import CYCLE_CANDIDATE_SELECTION_TABLE
from data_platform.cycle.repository import _create_engine, _text
from data_platform.queue.models import CANDIDATE_QUEUE_TABLE

if TYPE_CHECKING:
    # Type-only import keeps contracts an optional runtime dep — installed in
    # production environments via ``project-ult-contracts`` editable / git
    # but not required for data-platform unit tests that don't touch this
    # module.
    from contracts.schemas import CandidateGraphDelta


_EX3_PAYLOAD_TYPE = "Ex-3"

# Identifiers we interpolate into SQL via f-strings must be validated as
# safe identifiers (matching ``data_platform.serving.reader._IDENTIFIER_PATTERN``)
# even though they originate as module-level constants today. Defence in depth:
# if a future change exposes either name to env / config, the validation
# raises before the SQL runs.
_QUALIFIED_IDENTIFIER_PATTERN = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*$"
)


def _validated_identifier(identifier: str, *, kind: str) -> str:
    if not _QUALIFIED_IDENTIFIER_PATTERN.fullmatch(identifier):
        raise ValueError(f"unsafe {kind} identifier: {identifier!r}")
    return identifier


# Validate at import time so a malformed module-level constant fails fast.
_QUALIFIED_CANDIDATE_QUEUE_TABLE = _validated_identifier(
    CANDIDATE_QUEUE_TABLE, kind="candidate_queue table"
)
_QUALIFIED_CYCLE_CANDIDATE_SELECTION_TABLE = _validated_identifier(
    CYCLE_CANDIDATE_SELECTION_TABLE, kind="cycle_candidate_selection table"
)


class PostgresCandidateDeltaReader:
    """Read ``Ex3CandidateGraphDelta`` payloads from the lite-mode PG queue.

    Implementation queries ``cycle_candidate_selection`` joined to
    ``candidate_queue``, filters to ``payload_type='Ex-3'`` accepted rows
    selected for the requested cycle, and deserialises each ``payload``
    JSONB column into a ``CandidateGraphDelta`` Pydantic model.

    The ``selection_ref`` parameter (per the graph-engine Protocol) is
    accepted for traceability but not consulted for filtering: the cycle
    candidate set is fully identified by ``cycle_id``.
    """

    def __init__(self, *, engine: Any | None = None) -> None:
        self._engine = engine
        self._engine_lock = Lock()

    def _resolve_engine(self) -> Any:
        # Thread-safe lazy initialisation: Dagster's asset model is
        # single-threaded today, but a future change to multi-threaded
        # asset materialisation would race on the unguarded ``self._engine
        # is None`` check. ``Lock`` is cheap and removes the data race.
        if self._engine is None:
            with self._engine_lock:
                if self._engine is None:
                    self._engine = _create_engine()
        return self._engine

    def read_candidate_graph_deltas(
        self,
        cycle_id: str,
        selection_ref: str,
    ) -> list["CandidateGraphDelta"]:
        # Lazy import so this module imports cleanly in environments that do
        # not yet have project-ult-contracts installed (e.g. data-platform
        # unit-test only venvs); tests touching this method are still
        # expected to pip-install or PYTHONPATH-add contracts.
        from contracts.schemas import CandidateGraphDelta

        engine = self._resolve_engine()
        sql = _text(
            f"""
            SELECT cq.payload AS payload
            FROM {_QUALIFIED_CANDIDATE_QUEUE_TABLE} AS cq
            JOIN {_QUALIFIED_CYCLE_CANDIDATE_SELECTION_TABLE} AS sel
              ON sel.candidate_id = cq.id
            WHERE sel.cycle_id = :cycle_id
              AND cq.payload_type = :payload_type
              AND cq.validation_status = 'accepted'
            ORDER BY cq.ingest_seq ASC
            """
        )

        with engine.connect() as connection:
            rows = connection.execute(
                sql,
                {
                    "cycle_id": cycle_id,
                    "payload_type": _EX3_PAYLOAD_TYPE,
                },
            ).mappings().all()

        deltas: list[CandidateGraphDelta] = []
        for row in rows:
            payload = row["payload"]
            # psycopg returns JSONB as already-parsed dict in modern versions;
            # accept either dict or stringified JSON for resilience.
            if isinstance(payload, str):
                import json
                payload = json.loads(payload)
            deltas.append(CandidateGraphDelta.model_validate(payload))
        return deltas

    @classmethod
    def from_env(cls) -> PostgresCandidateDeltaReader:
        """Construct from ``DP_PG_DSN`` (validated lazily on first read)."""

        return cls()


class IcebergEntityAnchorReader:
    """Resolve graph node ids and existing canonical entity ids from Iceberg.

    Reads the ``canonical_entity`` and ``entity_alias`` tables under the
    ``canonical`` namespace via the data-platform DuckDB-backed reader. The
    Iceberg specs declare:

    * ``canonical_entity(canonical_entity_id, created_at)``
    * ``entity_alias(alias, canonical_entity_id, source, created_at)``

    For graph-engine the mapping is:

    * ``alias`` ≡ ``node_id`` (subsystem-asserted graph node identifier)
    * ``canonical_entity_id`` ≡ canonical anchor id

    Existence checks fall back to an empty result on
    ``CanonicalTableNotFound`` so the reader fails open as ``no anchors yet``
    when the Iceberg warehouse has not yet materialised these tables (rather
    than blocking Phase 1 with a CanonicalTableNotFound exception).
    """

    def read_canonical_table(
        self,
        table: str,
        columns: list[str] | None = None,
        filters: list[tuple[str, str, Any]] | None = None,
    ) -> Any:
        """Indirection point — overridable in tests to inject a fake reader.

        Default implementation calls ``data_platform.serving.reader.read_canonical``.
        """

        from data_platform.serving.reader import read_canonical

        return read_canonical(table, columns=columns, filters=filters)

    def canonical_entity_ids_for_node_ids(
        self,
        node_ids: set[str],
    ) -> dict[str, str]:
        if not node_ids:
            return {}

        from data_platform.serving.reader import CanonicalTableNotFound

        try:
            table = self.read_canonical_table(
                "entity_alias",
                columns=["alias", "canonical_entity_id"],
                filters=[("alias", "in", sorted(node_ids))],
            )
        except CanonicalTableNotFound:
            return {}

        result: dict[str, str] = {}
        aliases = table.column("alias").to_pylist()
        canonical_ids = table.column("canonical_entity_id").to_pylist()
        for alias, canonical_id in zip(aliases, canonical_ids, strict=True):
            if alias is None or canonical_id is None:
                continue
            result[str(alias)] = str(canonical_id)
        return result

    def existing_entity_ids(self, entity_ids: set[str]) -> set[str]:
        if not entity_ids:
            return set()

        from data_platform.serving.reader import CanonicalTableNotFound

        try:
            table = self.read_canonical_table(
                "canonical_entity",
                columns=["canonical_entity_id"],
                filters=[("canonical_entity_id", "in", sorted(entity_ids))],
            )
        except CanonicalTableNotFound:
            return set()

        present: set[str] = set()
        for value in table.column("canonical_entity_id").to_pylist():
            if value is None:
                continue
            present.add(str(value))
        return present

    @classmethod
    def from_env(cls) -> IcebergEntityAnchorReader:
        """Construct from data-platform settings (DP_ICEBERG_WAREHOUSE_PATH /
        DP_DUCKDB_PATH consumed lazily by ``read_canonical``)."""

        return cls()


class IcebergCanonicalGraphWriter:
    """Real Phase 1 graph promotion canonical write-back (M2.6 follow-up #1).

    Replaces the M2.3a-2 ``StubCanonicalGraphWriter`` (which raised
    ``NotImplementedError``). For each ``PromotionPlan`` produced by
    ``GraphPhase1Service``, this writer appends the contained
    ``GraphNodeRecord`` / ``GraphEdgeRecord`` / ``GraphAssertionRecord``
    Pydantic models to the three canonical Iceberg tables defined at
    ``data_platform.ddl.iceberg_tables.CANONICAL_GRAPH_PROMOTION_TABLE_SPECS``:

    * ``canonical.graph_node`` — per-promotion node identity rows
    * ``canonical.graph_edge`` — per-promotion edge rows
    * ``canonical.graph_assertion`` — per-promotion assertion rows

    Properties / evidence dicts are JSON-serialised into ``properties_json``
    / ``evidence_json`` columns (Iceberg/PyArrow does not have a native
    arbitrary-key struct type). Each record carries the ``cycle_id`` of
    the promotion that produced it for traceability.

    **Re-run idempotency (M2.6 follow-up #1 review-fold P1-A + codex
    review #1):** each write issues an Iceberg row-filter overwrite scoped
    to the plan's ``cycle_id`` on **all three** ``canonical.graph_*``
    tables — even when a record slice is empty. An empty slice is
    materialised as a zero-row Arrow batch carrying the canonical
    ``TableSpec.schema``; pyiceberg's ``overwrite(filter=...)`` then
    deletes any prior rows for that ``cycle_id`` and commits a no-op
    snapshot. This closes the codex#1 ghost-row gap: a retry whose
    plan legitimately produces empty edges/assertions (e.g. a cycle of
    isolated-node promotions) now clears the prior cycle's stale
    edges/assertions instead of leaving them in place. Rows for *other*
    ``cycle_id`` values are untouched.

    **Atomicity:** the three table overwrites happen sequentially. A
    failure on the second table leaves the first table's overwrite
    committed. Re-running the same cycle is the recovery contract — the
    cycle-scoped overwrite means a second run produces the correct final
    state regardless of how far the first run got. Iceberg snapshots at
    the table level let operators inspect partial writes via the catalog.

    **Timestamp contract (codex review #7):** ``created_at`` /
    ``updated_at`` on each record MUST be a tz-aware datetime with a
    UTC offset. The writer validates each value before constructing
    the Arrow batch and raises ``ValueError`` on a tz-naive datetime
    or a non-UTC offset, rather than letting PyArrow silently coerce
    against the tz-tagged ``GRAPH_TIMESTAMP_TYPE`` column.
    """

    def __init__(self, *, catalog: Any | None = None) -> None:
        self._catalog = catalog
        self._catalog_lock = Lock()

    def _resolve_catalog(self) -> Any:
        # Thread-safe lazy initialisation mirroring
        # PostgresCandidateDeltaReader._resolve_engine.
        if self._catalog is None:
            with self._catalog_lock:
                if self._catalog is None:
                    from data_platform.serving.catalog import load_catalog

                    self._catalog = load_catalog()
        return self._catalog

    def write_canonical_records(self, plan: Any) -> None:
        """Cycle-scoped overwrite of the three canonical.graph_* tables.

        ``plan`` is a ``graph_engine.models.PromotionPlan``. For each
        of the three tables (``graph_node``, ``graph_edge``,
        ``graph_assertion``) the writer issues
        ``target.overwrite(arrow, overwrite_filter=EqualTo("cycle_id",
        plan.cycle_id))`` — atomically replacing the rows belonging to
        this ``cycle_id`` with the plan's records. This makes Phase 1
        retries idempotent at the table level.

        **All three tables are overwritten on every call**, even when a
        slice is empty (codex review #1). An empty slice is materialised
        as a zero-row Arrow batch carrying the canonical
        ``CANONICAL_GRAPH_*_SPEC.schema``. The cycle-scoped delete still
        runs and removes any prior rows for ``cycle_id`` from that
        table; this closes the ghost-row gap that the prior "skip on
        empty slice" semantics opened (a retry with a smaller plan
        would have left the previous run's rows for that same cycle).

        Order (node → edge → assertion) preserves referential intent in
        the Iceberg snapshot history if a downstream reader inspects
        partial writes after a failure.
        """

        catalog = self._resolve_catalog()
        cycle_id = plan.cycle_id

        # Always materialise three Arrow batches — empty slices become
        # zero-row tables so the cycle-scoped overwrite still clears
        # prior rows for ``cycle_id`` (codex review #1).
        node_arrow = self._node_records_to_arrow(plan.node_records, cycle_id)
        edge_arrow = self._edge_records_to_arrow(plan.edge_records, cycle_id)
        assertion_arrow = self._assertion_records_to_arrow(
            plan.assertion_records, cycle_id
        )

        self._overwrite_cycle_slice(
            catalog, "canonical.graph_node", node_arrow, cycle_id
        )
        self._overwrite_cycle_slice(
            catalog, "canonical.graph_edge", edge_arrow, cycle_id
        )
        self._overwrite_cycle_slice(
            catalog, "canonical.graph_assertion", assertion_arrow, cycle_id
        )

    @staticmethod
    def _overwrite_cycle_slice(
        catalog: Any, identifier: str, table_arrow: Any, cycle_id: str
    ) -> None:
        # Lazy import: ``pyiceberg.expressions`` is only needed when a
        # real Iceberg target is wired up; tests pass a fake catalog
        # whose ``_FakeIcebergTable.overwrite`` ignores the filter.
        from pyiceberg.expressions import EqualTo

        target_table = catalog.load_table(identifier)
        target_table.overwrite(
            table_arrow, overwrite_filter=EqualTo("cycle_id", cycle_id)
        )

    @staticmethod
    def _require_utc_datetime(value: Any, *, field: str, record_idx: int) -> Any:
        """Validate a datetime value is tz-aware UTC (codex review #7).

        Raises ``ValueError`` for tz-naive datetimes or non-UTC offsets so
        the writer fails closed rather than letting PyArrow silently
        coerce against the tz-tagged ``GRAPH_TIMESTAMP_TYPE`` column.
        """

        from datetime import datetime, timezone

        if not isinstance(value, datetime):
            msg = (
                f"canonical graph record[{record_idx}].{field} must be a "
                f"datetime; got {type(value).__name__}"
            )
            raise ValueError(msg)
        if value.tzinfo is None:
            msg = (
                f"canonical graph record[{record_idx}].{field} must be "
                f"tz-aware UTC (got tz-naive datetime); the canonical "
                f"graph schema requires explicit UTC timestamps"
            )
            raise ValueError(msg)
        offset = value.utcoffset()
        if offset is None or offset.total_seconds() != 0:
            msg = (
                f"canonical graph record[{record_idx}].{field} must be in "
                f"UTC (got offset {offset!r}); convert to UTC before "
                f"writing"
            )
            raise ValueError(msg)
        return value

    @staticmethod
    def _node_records_to_arrow(records: list[Any], cycle_id: str) -> Any:
        import json

        import pyarrow as pa

        # Single source of truth: derive schema from the canonical TableSpec
        # (M2.6 follow-up #1 review-fold P1-C). If the spec evolves a column,
        # the writer no longer needs a parallel update.
        from data_platform.ddl.iceberg_tables import CANONICAL_GRAPH_NODE_SPEC

        # Validate tz-aware UTC for every datetime up-front so a partial
        # write does not commit malformed rows (codex review #7).
        for idx, record in enumerate(records):
            IcebergCanonicalGraphWriter._require_utc_datetime(
                record.created_at, field="created_at", record_idx=idx
            )
            IcebergCanonicalGraphWriter._require_utc_datetime(
                record.updated_at, field="updated_at", record_idx=idx
            )

        # NOTE: an empty ``records`` list still produces a zero-row Arrow
        # batch with the canonical schema so ``write_canonical_records``
        # can issue a cycle-scoped overwrite that clears prior rows
        # (codex review #1).
        return pa.table(
            {
                "node_id": [r.node_id for r in records],
                "canonical_entity_id": [r.canonical_entity_id for r in records],
                "label": [r.label for r in records],
                "properties_json": [
                    json.dumps(r.properties, sort_keys=True, default=str)
                    for r in records
                ],
                "cycle_id": [cycle_id for _ in records],
                "created_at": [r.created_at for r in records],
                "updated_at": [r.updated_at for r in records],
            },
            schema=CANONICAL_GRAPH_NODE_SPEC.schema,
        )

    @staticmethod
    def _edge_records_to_arrow(records: list[Any], cycle_id: str) -> Any:
        import json

        import pyarrow as pa

        from data_platform.ddl.iceberg_tables import CANONICAL_GRAPH_EDGE_SPEC

        for idx, record in enumerate(records):
            IcebergCanonicalGraphWriter._require_utc_datetime(
                record.created_at, field="created_at", record_idx=idx
            )
            IcebergCanonicalGraphWriter._require_utc_datetime(
                record.updated_at, field="updated_at", record_idx=idx
            )

        return pa.table(
            {
                "edge_id": [r.edge_id for r in records],
                "source_node_id": [r.source_node_id for r in records],
                "target_node_id": [r.target_node_id for r in records],
                "relationship_type": [r.relationship_type for r in records],
                "properties_json": [
                    json.dumps(r.properties, sort_keys=True, default=str)
                    for r in records
                ],
                "weight": [float(r.weight) for r in records],
                "cycle_id": [cycle_id for _ in records],
                "created_at": [r.created_at for r in records],
                "updated_at": [r.updated_at for r in records],
            },
            schema=CANONICAL_GRAPH_EDGE_SPEC.schema,
        )

    @staticmethod
    def _assertion_records_to_arrow(records: list[Any], cycle_id: str) -> Any:
        import json

        import pyarrow as pa

        # ``target_node_id`` is nullable per ``GraphAssertionRecord``
        # (graph-engine models.py:108); the spec field defaults to
        # nullable=True under PyArrow which round-trips ``None`` correctly.
        from data_platform.ddl.iceberg_tables import (
            CANONICAL_GRAPH_ASSERTION_SPEC,
        )

        for idx, record in enumerate(records):
            IcebergCanonicalGraphWriter._require_utc_datetime(
                record.created_at, field="created_at", record_idx=idx
            )

        return pa.table(
            {
                "assertion_id": [r.assertion_id for r in records],
                "source_node_id": [r.source_node_id for r in records],
                "target_node_id": [r.target_node_id for r in records],
                "assertion_type": [r.assertion_type for r in records],
                "evidence_json": [
                    json.dumps(r.evidence, sort_keys=True, default=str)
                    for r in records
                ],
                "confidence": [float(r.confidence) for r in records],
                "cycle_id": [cycle_id for _ in records],
                "created_at": [r.created_at for r in records],
            },
            schema=CANONICAL_GRAPH_ASSERTION_SPEC.schema,
        )

    @classmethod
    def from_env(cls) -> IcebergCanonicalGraphWriter:
        """Construct a lazy-initialising writer.

        ``DP_PG_DSN`` (Iceberg SQL catalog backing) +
        ``DP_ICEBERG_WAREHOUSE_PATH`` are consumed by ``load_catalog()``
        on first ``write_canonical_records`` call. This delays catalog
        connection so the writer is constructable in Definitions-load
        environments without live Iceberg access.
        """

        return cls()


# Backwards-compatibility alias: ``StubCanonicalGraphWriter`` is retained
# so any existing imports do not break. The alias now points at the real
# IcebergCanonicalGraphWriter; callers that genuinely want fail-closed
# behaviour can construct ``_FailClosedCanonicalGraphWriter`` directly.
StubCanonicalGraphWriter = IcebergCanonicalGraphWriter


class _FailClosedCanonicalGraphWriter:
    """Explicit fail-closed writer for environments where Iceberg is
    unavailable. Mirrors the orchestrator's ``_FailClosedGraphStatusProvider``
    pattern: callers wire this when they want
    ``write_canonical_records`` to raise rather than attempt a real
    Iceberg write."""

    def write_canonical_records(self, plan: Any) -> None:
        raise RuntimeError(
            "Canonical graph writer is fail-closed; configure DP_PG_DSN + "
            "DP_ICEBERG_WAREHOUSE_PATH and use IcebergCanonicalGraphWriter "
            "to enable writes",
        )


# NOTE: ``StubCanonicalGraphWriter`` is intentionally NOT in ``__all__`` (M2.6
# follow-up #1 review-fold P2-1). The alias is retained at module level for
# import compatibility with M2.3a-2 callers, but a name containing "Stub" that
# resolves to a production writer would be misleading via ``import *``. New
# code should reference ``IcebergCanonicalGraphWriter`` (or
# ``_FailClosedCanonicalGraphWriter`` for explicit fail-closed wiring).
__all__ = [
    "IcebergCanonicalGraphWriter",
    "IcebergEntityAnchorReader",
    "PostgresCandidateDeltaReader",
]
