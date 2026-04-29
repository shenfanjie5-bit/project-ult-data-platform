"""Schema-parity tests for canonical writer provider-neutrality.

These tests assert the M1 target end-state at the canonical writer's
`required_columns` boundary. Today the writer mart specs require
`source_run_id` / `raw_loaded_at` and `ts_code` / `index_code` columns. After
M1-D the v2 writer specs (`CANONICAL_V2_MART_LOAD_SPECS`) must not require
those columns; lineage moves to a parallel `CANONICAL_LINEAGE_LOAD_SPECS` set.

Per ult_milestone.md M1-C: tests may fail initially.
"""

from __future__ import annotations

import os
from typing import Final

import pytest

from data_platform.serving import canonical_writer


_M1D_LEGACY_RETIREMENT_XFAIL = pytest.mark.xfail(
    reason=(
        "M1-D step 5 legacy writer retirement is not complete; run with "
        "DP_ENFORCE_M1D_PROVIDER_NEUTRALITY=1 or pytest --runxfail to enforce."
    ),
    condition=os.environ.get("DP_ENFORCE_M1D_PROVIDER_NEUTRALITY") != "1",
    strict=False,
)

FORBIDDEN_LINEAGE_REQUIRED_COLUMNS: Final[frozenset[str]] = frozenset(
    {"source_run_id", "raw_loaded_at"}
)
FORBIDDEN_PROVIDER_SHAPED_REQUIRED_COLUMNS: Final[frozenset[str]] = frozenset(
    {"ts_code", "index_code"}
)


def _spec_id(spec: canonical_writer.CanonicalLoadSpec) -> str:
    return spec.identifier


@pytest.mark.parametrize(
    "spec",
    canonical_writer.CANONICAL_MART_LOAD_SPECS,
    ids=_spec_id,
)
@_M1D_LEGACY_RETIREMENT_XFAIL
def test_canonical_mart_load_spec_does_not_require_raw_lineage(
    spec: canonical_writer.CanonicalLoadSpec,
) -> None:
    """Canonical mart writer specs must not require raw-zone lineage columns.

    Today RED for all 8 mart specs; flips to GREEN as M1-D moves the writer to
    the `canonical_v2` namespace and lineage becomes a sibling load spec.
    """

    required = set(spec.required_columns)
    leaks = sorted(required & FORBIDDEN_LINEAGE_REQUIRED_COLUMNS)
    assert not leaks, (
        f"{_spec_id(spec)} writer spec still requires raw-zone lineage {leaks}; "
        "lineage must be loaded via a sibling CANONICAL_LINEAGE_LOAD_SPECS entry"
    )


@pytest.mark.parametrize(
    "spec",
    canonical_writer.CANONICAL_MART_LOAD_SPECS,
    ids=_spec_id,
)
@_M1D_LEGACY_RETIREMENT_XFAIL
def test_canonical_mart_load_spec_does_not_require_provider_shaped_identifier(
    spec: canonical_writer.CanonicalLoadSpec,
) -> None:
    """Canonical mart writer specs must use provider-neutral identifier columns.

    Today RED for the specs that key on `ts_code` / `index_code`; flips to
    GREEN as M1-D moves the writer to canonical_v2 with `security_id` / `index_id`.
    """

    required = set(spec.required_columns)
    provider_shaped = sorted(required & FORBIDDEN_PROVIDER_SHAPED_REQUIRED_COLUMNS)
    assert not provider_shaped, (
        f"{_spec_id(spec)} writer spec still requires provider-shaped identifier "
        f"columns {provider_shaped}; the canonical contract uses security_id / "
        "index_id / entity_id"
    )


@_M1D_LEGACY_RETIREMENT_XFAIL
def test_FORBIDDEN_PAYLOAD_FIELDS_extends_to_canonical_lineage() -> None:
    """`FORBIDDEN_PAYLOAD_FIELDS` must extend to raw-zone lineage after M1-D step 5.

    Today only blocks `submitted_at` / `ingest_seq`; the lineage block flips on
    once canonical_v2 is the only write path.
    """

    missing = sorted(FORBIDDEN_LINEAGE_REQUIRED_COLUMNS - canonical_writer.FORBIDDEN_PAYLOAD_FIELDS)
    assert not missing, (
        f"FORBIDDEN_PAYLOAD_FIELDS does not yet block {missing}; M1-D step 5 must "
        "extend it once canonical_v2 is the only canonical write path"
    )


def test_canonical_v2_load_specs_drop_raw_lineage() -> None:
    """After M1-D, every CANONICAL_V2_*_LOAD_SPEC must be lineage-free.

    Vacuous PASS today; becomes binding as M1-D adds the v2 specs.
    """

    v2_load_specs_collections = [
        value
        for name, value in vars(canonical_writer).items()
        if name.startswith("CANONICAL_V2_")
        and isinstance(value, tuple)
        and value
        and isinstance(value[0], canonical_writer.CanonicalLoadSpec)
    ]
    if not v2_load_specs_collections:
        pytest.skip("no CANONICAL_V2_*_LOAD_SPECS declared yet — defers to M1-D")

    for collection in v2_load_specs_collections:
        for spec in collection:
            required = set(spec.required_columns)
            leaks = sorted(required & FORBIDDEN_LINEAGE_REQUIRED_COLUMNS)
            assert not leaks, (
                f"{spec.identifier} (canonical_v2) requires raw-zone lineage {leaks}; "
                "v2 must be lineage-free by construction"
            )
            provider_shaped = sorted(required & FORBIDDEN_PROVIDER_SHAPED_REQUIRED_COLUMNS)
            assert not provider_shaped, (
                f"{spec.identifier} (canonical_v2) requires provider-shaped "
                f"identifier {provider_shaped}; rename via field_mapping must apply"
            )


def test_canonical_lineage_load_specs_carry_lineage_explicitly() -> None:
    """After M1-D, every CANONICAL_LINEAGE_*_LOAD_SPEC must require lineage columns.

    Vacuous PASS today; becomes binding as M1-D adds the lineage specs.
    """

    lineage_load_specs_collections = [
        value
        for name, value in vars(canonical_writer).items()
        if name.startswith("CANONICAL_LINEAGE_")
        and isinstance(value, tuple)
        and value
        and isinstance(value[0], canonical_writer.CanonicalLoadSpec)
    ]
    if not lineage_load_specs_collections:
        pytest.skip("no CANONICAL_LINEAGE_*_LOAD_SPECS declared yet — defers to M1-D")

    for collection in lineage_load_specs_collections:
        for spec in collection:
            required = set(spec.required_columns)
            assert "source_run_id" in required, (
                f"{spec.identifier} (canonical_lineage) must require source_run_id"
            )
            assert "raw_loaded_at" in required, (
                f"{spec.identifier} (canonical_lineage) must require raw_loaded_at"
            )
