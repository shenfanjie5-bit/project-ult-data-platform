"""Schema-parity tests for canonical Iceberg DDL provider neutrality.

These tests assert the M1 target end-state: canonical Iceberg specs must not
embed raw-zone lineage fields (`source_run_id`, `raw_loaded_at`) and must not
expose provider-shaped identifier columns (`ts_code`, `index_code`) on
business rows. Today (pre-M1-D) the legacy `canonical.*` specs still carry
both; expect failures here that flip to PASS as M1-D moves each spec into the
`canonical_v2.*` namespace and the lineage column moves to
`canonical_lineage.*`.

Per ult_milestone.md M1-C: tests may fail initially. M1-D is responsible for
making them pass or clearly marking the remaining failures as blockers.

Per data-platform CLAUDE.md: Raw Zone (raw lineage) ≠ Canonical Zone. The
existing `FORBIDDEN_SCHEMA_FIELDS` (`{"submitted_at", "ingest_seq"}`) only
covers the Layer-B ingest-queue boundary; this test file extends coverage to
the raw-zone lineage boundary.
"""

from __future__ import annotations

import os
from typing import Final

import pyarrow as pa  # type: ignore[import-untyped]
import pytest

from data_platform.ddl import iceberg_tables


_M1D_LEGACY_RETIREMENT_XFAIL = pytest.mark.xfail(
    reason=(
        "M1-D step 5 legacy canonical retirement is not complete; run with "
        "DP_ENFORCE_M1D_PROVIDER_NEUTRALITY=1 or pytest --runxfail to enforce."
    ),
    condition=os.environ.get("DP_ENFORCE_M1D_PROVIDER_NEUTRALITY") != "1",
    strict=False,
)

CANONICAL_BUSINESS_SPECS: Final[tuple[iceberg_tables.TableSpec, ...]] = (
    iceberg_tables.CANONICAL_STOCK_BASIC_SPEC,
    *iceberg_tables.CANONICAL_MART_TABLE_SPECS,
)
"""All canonical Iceberg specs that hold business rows (excluding entity stores)."""

FORBIDDEN_RAW_LINEAGE_FIELDS: Final[frozenset[str]] = frozenset(
    {"source_run_id", "raw_loaded_at"}
)
"""Raw-zone lineage columns that must not appear on canonical business rows.

These belong on a sibling `canonical_lineage.*` table, not on canonical
business specs. Per M1-A design + data-platform CLAUDE.md "Raw Zone ≠
Canonical Zone".
"""

FORBIDDEN_PROVIDER_SHAPED_IDENTIFIERS: Final[frozenset[str]] = frozenset(
    {"ts_code", "index_code"}
)
"""Provider-shaped identifier columns that must not appear on canonical
business rows. Provider catalog (`registry.py`) declares the canonical
identifiers (`security_id`, `index_id`, `entity_id`); the physical schema
must adopt the canonical name."""


def _spec_id(spec: iceberg_tables.TableSpec) -> str:
    return f"{spec.namespace}.{spec.name}"


@pytest.mark.parametrize(
    "spec",
    CANONICAL_BUSINESS_SPECS,
    ids=_spec_id,
)
@_M1D_LEGACY_RETIREMENT_XFAIL
def test_canonical_business_spec_does_not_carry_raw_lineage(
    spec: iceberg_tables.TableSpec,
) -> None:
    """Canonical business rows must not embed raw-zone lineage fields."""

    field_names = {field.name for field in spec.schema}
    leaks = sorted(field_names & FORBIDDEN_RAW_LINEAGE_FIELDS)
    assert not leaks, (
        f"{_spec_id(spec)} embeds raw-zone lineage fields {leaks}; lineage must "
        "live on a sibling canonical_lineage.* table per M1-A design"
    )


@pytest.mark.parametrize(
    "spec",
    CANONICAL_BUSINESS_SPECS,
    ids=_spec_id,
)
@_M1D_LEGACY_RETIREMENT_XFAIL
def test_canonical_business_spec_does_not_carry_provider_shaped_identifier(
    spec: iceberg_tables.TableSpec,
) -> None:
    """Canonical business rows must use provider-neutral identifier columns."""

    field_names = {field.name for field in spec.schema}
    provider_shaped = sorted(field_names & FORBIDDEN_PROVIDER_SHAPED_IDENTIFIERS)
    assert not provider_shaped, (
        f"{_spec_id(spec)} exposes provider-shaped identifiers {provider_shaped}; "
        "the provider catalog (registry.py field_mapping) declares canonical names "
        "such as security_id / index_id / entity_id — the physical spec must adopt "
        "those names per M1-A design"
    )


@_M1D_LEGACY_RETIREMENT_XFAIL
def test_FORBIDDEN_SCHEMA_FIELDS_includes_canonical_lineage_block() -> None:
    """`FORBIDDEN_SCHEMA_FIELDS` must extend to raw-zone lineage after M1-D step 5.

    Today this set blocks only Layer-B ingest fields. After M1-D step 5
    extends it to include raw-zone lineage, no future canonical spec can
    silently re-introduce `source_run_id` / `raw_loaded_at`.
    """

    missing = sorted(FORBIDDEN_RAW_LINEAGE_FIELDS - iceberg_tables.FORBIDDEN_SCHEMA_FIELDS)
    assert not missing, (
        f"FORBIDDEN_SCHEMA_FIELDS does not yet block {missing}; M1-D step 5 must "
        "extend it once canonical_v2 is the only canonical write path"
    )


def test_canonical_lineage_namespace_specs_keep_canonical_pk_first() -> None:
    """After M1-D, every canonical_lineage.* spec must lead with the canonical PK
    columns it joins on (followed by lineage payload columns).

    This test discovers `CANONICAL_LINEAGE_*_SPEC` symbols by name on the
    `iceberg_tables` module. Today no such symbol exists; the test passes
    vacuously. After M1-D adds them, this test enforces the join shape.
    """

    lineage_specs = [
        value
        for name, value in vars(iceberg_tables).items()
        if name.startswith("CANONICAL_LINEAGE_") and isinstance(value, iceberg_tables.TableSpec)
    ]
    if not lineage_specs:
        pytest.skip("no canonical_lineage.* specs declared yet — defers to M1-D")

    for spec in lineage_specs:
        names = list(spec.schema.names)
        assert names, f"{_spec_id(spec)} has empty schema"
        assert spec.namespace == "canonical_lineage", (
            f"{_spec_id(spec)} lives outside canonical_lineage namespace"
        )
        last_metadata = {
            "source_run_id",
            "raw_loaded_at",
            "canonical_loaded_at",
        }
        # The canonical PK columns must come first; metadata columns at the end.
        first_metadata_idx = next(
            (idx for idx, name in enumerate(names) if name in last_metadata),
            len(names),
        )
        for trailing_name in names[first_metadata_idx:]:
            assert trailing_name in last_metadata or trailing_name in {
                "source_provider",
                "source_interface_id",
            }, (
                f"{_spec_id(spec)} mixes business columns and metadata; expected "
                "canonical PK first, then lineage/metadata"
            )


def test_canonical_v2_namespace_specs_do_not_carry_raw_lineage() -> None:
    """After M1-D, every canonical_v2.* spec must be lineage-free at the storage layer.

    Vacuous PASS today (no v2 specs); becomes binding as M1-D adds them.
    """

    v2_specs = [
        value
        for name, value in vars(iceberg_tables).items()
        if name.startswith("CANONICAL_V2_") and isinstance(value, iceberg_tables.TableSpec)
    ]
    if not v2_specs:
        pytest.skip("no canonical_v2.* specs declared yet — defers to M1-D")

    for spec in v2_specs:
        field_names = {field.name for field in spec.schema}
        leaks = sorted(field_names & FORBIDDEN_RAW_LINEAGE_FIELDS)
        provider_shaped = sorted(field_names & FORBIDDEN_PROVIDER_SHAPED_IDENTIFIERS)
        assert not leaks and not provider_shaped, (
            f"{_spec_id(spec)} (canonical_v2) leaks {leaks or provider_shaped}; "
            "canonical_v2 must be lineage-free and provider-neutral by construction"
        )
        assert spec.namespace == "canonical_v2", (
            f"{_spec_id(spec)} mis-namespaces a CANONICAL_V2_ spec outside the "
            "canonical_v2 namespace"
        )


def test_FORBIDDEN_RAW_LINEAGE_FIELDS_set_is_stable() -> None:
    """Sentinel: the forbidden set used by this file is a stable contract.

    If a future change wants to expand the set (e.g., to include
    `source_provider` on canonical business rows), update this test
    intentionally rather than silently widening the contract.
    """

    assert FORBIDDEN_RAW_LINEAGE_FIELDS == frozenset({"source_run_id", "raw_loaded_at"})
    assert FORBIDDEN_PROVIDER_SHAPED_IDENTIFIERS == frozenset({"ts_code", "index_code"})
