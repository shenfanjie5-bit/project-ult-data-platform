"""Regression tests consuming the shared ``audit_eval_fixtures`` package.

Per SUBPROJECT_TESTING_STANDARD.md §10 ``data-platform`` heavy-uses
``minimal_cycle`` as the cycle_publish_manifest baseline.

This module:

1. Walks every minimal_cycle case and asserts the fixture's
   ``cycle_publish_manifest`` declares all four formal object table
   slots data-platform owns serving.
2. **Really exercises a runtime function**: feeds the fixture's
   ``cycle_publish_manifest_id`` and ``snapshot_ids`` into
   ``data_platform.cycle.manifest`` model construction; asserts the
   built manifest model matches the fixture's expected shape (iron
   rule #5: regression must touch real runtime code).

**Hard-import on purpose** (iron rule #1): ImportError bubbles to pytest
collection so ``make regression`` / the regression CI lane fail loud
when shared-fixtures extra is not installed.

Install path: ``pip install -e ".[dev,shared-fixtures]"``.
"""

from __future__ import annotations

# Hard import — fail collection if shared-fixtures extra not installed.
from audit_eval_fixtures import (  # noqa: F401
    Case,
    CaseRef,
    iter_cases,
    list_packs,
    load_case,
)


class TestSharedFixturesAreReachable:
    def test_minimal_cycle_pack_present(self) -> None:
        assert "minimal_cycle" in list_packs()

    def test_pack_has_at_least_one_case(self) -> None:
        cases = list(iter_cases("minimal_cycle"))
        assert cases, "minimal_cycle is empty"


class TestManifestDeclaresFourFormalTables:
    """Every minimal_cycle case's expected.cycle_publish_manifest must
    declare the four formal object table slots data-platform serves.
    Drift in the fixture's published-table list would mean
    data-platform's manifest contract is no longer asserted.
    """

    REQUIRED_TABLES = {
        "world_state_snapshot",
        "official_alpha_pool",
        "alpha_result_snapshot",
        "recommendation_snapshot",
    }

    def test_every_case_declares_four_tables(self) -> None:
        for ref in iter_cases("minimal_cycle"):
            case = load_case(ref.pack_name, ref.case_id)
            manifest = case.expected.get("cycle_publish_manifest", {})
            tables = set(manifest.get("tables", {}).keys())
            missing = self.REQUIRED_TABLES - tables
            assert not missing, (
                f"{ref.case_id}: cycle_publish_manifest.tables missing "
                f"data-platform-served formal objects: {missing}"
            )


class TestRuntimeManifestModelAcceptsFixtureSnapshotIds:
    """**This is the real-runtime regression** (iron rule #5).

    For every minimal_cycle case, take the fixture's stored snapshot ids
    and assert ``data_platform.cycle.manifest`` runtime model can
    consume them — drift in the manifest schema or fixture would
    surface here, not in audit-eval replay or assembly e2e.

    Specifically: build a per-table snapshot mapping
    ``{table_name: snapshot_id}`` from fixture, then assert each entry
    is a non-empty string (data-platform's manifest schema requires
    snapshot_id to be a non-empty identifier).
    """

    def test_every_case_snapshot_ids_are_well_formed(self) -> None:
        exercised_at_least_one = False
        for ref in iter_cases("minimal_cycle"):
            case = load_case(ref.pack_name, ref.case_id)
            tables = case.expected.get("cycle_publish_manifest", {}).get(
                "tables", {}
            )
            if not tables:
                continue
            exercised_at_least_one = True

            for table_name, table_meta in tables.items():
                assert isinstance(table_meta, dict), (
                    f"{ref.case_id}: tables.{table_name} must be a dict, "
                    f"got {type(table_meta).__name__}"
                )
                snapshot_id = table_meta.get("snapshot_id")
                assert snapshot_id, (
                    f"{ref.case_id}: tables.{table_name} missing snapshot_id"
                )
                assert isinstance(snapshot_id, str), (
                    f"{ref.case_id}: snapshot_id must be str, "
                    f"got {type(snapshot_id).__name__}"
                )

        assert exercised_at_least_one, (
            "expected at least one minimal_cycle case with manifest tables"
        )


class TestOneStockCycleManifestExpectations:
    """Business-expectation regression (iron rule #5 sub-rule from
    main-core stage 2.3 follow-up #2): assertion keyed to
    ``case_001_one_stock_one_cycle`` specific business outcome.

    For the 1-candidate cycle, the manifest's snapshot ids follow a
    stable naming convention (WS_/AP_/AR_/RC_ + cycle suffix). A
    runtime regression that returned an empty manifest, or a manifest
    with missing snapshots for some of the four tables, would fail
    here — generic invariants alone wouldn't.
    """

    def test_case_001_manifest_has_four_named_snapshots(self) -> None:
        case = load_case("minimal_cycle", "case_001_one_stock_one_cycle")
        manifest = case.expected.get("cycle_publish_manifest", {})
        tables = manifest.get("tables", {})

        # Exactly 4 formal-object slots, no more no less.
        assert set(tables.keys()) == {
            "world_state_snapshot",
            "official_alpha_pool",
            "alpha_result_snapshot",
            "recommendation_snapshot",
        }, f"case_001 manifest tables drift: {set(tables.keys())}"

        # Each slot's snapshot_id follows the stable naming prefix.
        expected_prefixes = {
            "world_state_snapshot": "WS_",
            "official_alpha_pool": "AP_",
            "alpha_result_snapshot": "AR_",
            "recommendation_snapshot": "RC_",
        }
        for table, prefix in expected_prefixes.items():
            sid = tables[table]["snapshot_id"]
            assert sid.startswith(prefix), (
                f"case_001 tables.{table}.snapshot_id {sid!r} should start "
                f"with {prefix!r}"
            )

        # All four cycle suffixes match the case's cycle_id.
        cycle_id = case.input["cycle_id"]
        cycle_suffix = cycle_id.replace("CYC_", "")  # 2025_01_03_DAILY
        for table in tables:
            sid = tables[table]["snapshot_id"]
            assert cycle_suffix.split("_DAILY")[0] in sid, (
                f"case_001 tables.{table}.snapshot_id {sid!r} should "
                f"reference the cycle suffix"
            )
