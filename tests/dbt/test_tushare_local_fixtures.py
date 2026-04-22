"""Tushare Phase B — real-consumption tests for on-disk local raw fixtures.

Plan: ~/.claude/plans/wise-cooking-wolf.md §6.4.4.

Drives the weekly + monthly dbt staging models against the on-disk
parquet fixtures shipped under
``tests/dbt/fixtures/raw/tushare/{weekly,monthly}/dt=20260415/``. The
existing ``test_tushare_staging_models.py`` suite stays untouched —
it uses an in-memory RawWriter synthesis pattern that covers all 24
adapter datasets without serializing fixtures. This file adds a
parallel lane that REALLY reads the on-disk parquet so the two
serialized fixtures are provably consumed, not just sitting in git.

For each of ``weekly`` and ``monthly``, the test asserts:

1. Fixture directory exists.
2. ``_manifest.json`` exists and matches the RawWriter-written shape
   (``artifacts[0].{dataset, partition_date, path, row_count, run_id,
   source_id, written_at}``).
3. ``_source.json`` exists and satisfies the Phase B §7 traceability
   8-key contract + ``completeness_status == "未见明显遗漏"``.
4. The parquet file named by the manifest exists on disk.
5. The parquet column set matches the canonical ``TUSHARE_BAR_SCHEMA``
   (from ``data_platform.adapters.tushare.assets``).
6. ``stg_<dataset>`` runs to success against the on-disk fixture
   through the Jinja-rendered dbt SQL + DuckDB.
7. Staged row count equals the manifest's ``row_count`` (proves the
   staging path really read the parquet, not an empty/wrong dataset).
"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]
import pytest

from data_platform.adapters.tushare.assets import TUSHARE_BAR_SCHEMA


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DBT_PROJECT_DIR = PROJECT_ROOT / "src" / "data_platform" / "dbt"
STAGING_DIR = DBT_PROJECT_DIR / "models" / "staging"
#: Raw Zone root — ``dp_raw_path`` macro prepends the ``tushare/`` source
#: segment, so the raw-zone root must stop at ``raw/`` (not descend into
#: ``tushare/``). This mirrors how ``DP_RAW_ZONE_PATH`` is laid out in
#: production (raw zone contains one subdir per source_id).
RAW_ZONE_ROOT = PROJECT_ROOT / "tests" / "dbt" / "fixtures" / "raw"
FIXTURE_ROOT = RAW_ZONE_ROOT / "tushare"

PHASE_B_DATASETS: tuple[str, ...] = ("weekly", "monthly")
PARTITION_DIR = "dt=20260415"

_REQUIRED_TRACEABILITY_KEYS = frozenset(
    {
        "corpus_root",
        "dataset_path",
        "datasets",
        "selected_ts_codes",
        "date_window",
        "audit_timestamp",
        "completeness_status",
        "coverage_note",
    }
)


def _render_staging_model(model_name: str, raw_zone_path: Path) -> str:
    """Render the dbt model Jinja template into executable SQL.

    Mirrors the helper in ``test_tushare_staging_models.py`` so both
    test files exercise the real dbt macros + raw-zone path logic.
    """

    jinja2 = pytest.importorskip("jinja2")

    environment = jinja2.Environment()
    environment.globals["config"] = lambda **_kwargs: ""
    environment.globals["env_var"] = lambda name, default=None: (
        str(raw_zone_path) if name == "DP_RAW_ZONE_PATH" else default
    )

    path_module = environment.from_string(
        (DBT_PROJECT_DIR / "macros" / "dp_raw_path.sql").read_text()
    ).make_module({})
    environment.globals["dp_raw_path"] = path_module.dp_raw_path
    environment.globals["dp_raw_manifest_path"] = path_module.dp_raw_manifest_path

    latest_module = environment.from_string(
        (DBT_PROJECT_DIR / "macros" / "stg_latest_raw.sql").read_text()
    ).make_module({})
    environment.globals["stg_latest_raw"] = latest_module.stg_latest_raw

    return environment.from_string(
        (STAGING_DIR / f"{model_name}.sql").read_text()
    ).render().strip()


@pytest.mark.parametrize("dataset", PHASE_B_DATASETS)
def test_fixture_directory_and_manifest_are_present(dataset: str) -> None:
    partition_dir = FIXTURE_ROOT / dataset / PARTITION_DIR
    assert partition_dir.is_dir(), f"missing fixture dir: {partition_dir}"

    manifest_path = partition_dir / "_manifest.json"
    assert manifest_path.is_file(), f"missing manifest: {manifest_path}"

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["dataset"] == dataset
    assert manifest["source_id"] == "tushare"
    assert manifest["partition_date"] == "2026-04-15"
    assert len(manifest["artifacts"]) >= 1
    artifact = manifest["artifacts"][0]
    # Match the RawWriter-written manifest artifact shape exactly.
    for key in ("dataset", "partition_date", "path", "row_count", "run_id", "source_id", "written_at"):
        assert key in artifact, f"manifest artifact missing key: {key}"


@pytest.mark.parametrize("dataset", PHASE_B_DATASETS)
def test_source_json_has_all_eight_traceability_keys(dataset: str) -> None:
    source_path = FIXTURE_ROOT / dataset / PARTITION_DIR / "_source.json"
    assert source_path.is_file(), f"missing traceability sidecar: {source_path}"

    source = json.loads(source_path.read_text(encoding="utf-8"))
    missing = _REQUIRED_TRACEABILITY_KEYS - set(source.keys())
    assert not missing, f"{dataset} _source.json missing keys: {missing}"
    assert source["completeness_status"] == "未见明显遗漏"
    assert source["datasets"] == [dataset]


@pytest.mark.parametrize("dataset", PHASE_B_DATASETS)
def test_parquet_exists_and_columns_match_tushare_bar_schema(dataset: str) -> None:
    partition_dir = FIXTURE_ROOT / dataset / PARTITION_DIR
    manifest = json.loads((partition_dir / "_manifest.json").read_text(encoding="utf-8"))
    artifact = manifest["artifacts"][0]

    # Plan requires the manifest's ``path`` to point at a real on-disk parquet.
    parquet_path = PROJECT_ROOT / "tests" / "dbt" / "fixtures" / "raw" / artifact["path"]
    assert parquet_path.is_file(), f"manifest-referenced parquet missing: {parquet_path}"

    table = pq.read_table(parquet_path)
    expected_columns = set(TUSHARE_BAR_SCHEMA.names)
    # ``dt`` is auto-added by hive partitioning at read time; strip it
    # so the comparison is against the canonical TUSHARE_BAR_SCHEMA.
    actual_columns = set(table.column_names) - {"dt"}
    assert actual_columns == expected_columns, (
        f"{dataset} parquet column set drifted from TUSHARE_BAR_SCHEMA: "
        f"expected {sorted(expected_columns)}, got {sorted(actual_columns)}"
    )


@pytest.mark.parametrize("dataset", PHASE_B_DATASETS)
def test_staging_model_runs_against_on_disk_fixture(dataset: str) -> None:
    """End-to-end: stg_<dataset> must execute against the on-disk
    fixture and return the expected row count. This is the critical
    real-consumption gate — if the staging model can't read the
    fixture, the fixture is effectively dead weight.
    """

    duckdb = pytest.importorskip("duckdb")

    # ``dp_raw_path`` expects DP_RAW_ZONE_PATH to be the raw-zone ROOT
    # (containing the ``tushare/`` subdir), not the tushare subdir itself.
    model_sql = _render_staging_model(f"stg_{dataset}", RAW_ZONE_ROOT)

    connection = duckdb.connect(":memory:")
    try:
        connection.execute(f'create view "stg_{dataset}" as {model_sql}')
        staged_rows = connection.execute(
            f'select count(*) from "stg_{dataset}"'
        ).fetchone()[0]
    finally:
        connection.close()

    manifest = json.loads(
        (FIXTURE_ROOT / dataset / PARTITION_DIR / "_manifest.json").read_text(
            encoding="utf-8"
        )
    )
    expected_row_count = manifest["artifacts"][0]["row_count"]
    assert staged_rows == expected_row_count, (
        f"stg_{dataset} staged {staged_rows} rows but fixture manifest "
        f"declares row_count={expected_row_count}; the staging model "
        f"is not reading the on-disk fixture correctly"
    )


@pytest.mark.parametrize("dataset", PHASE_B_DATASETS)
def test_fixture_all_rows_share_declared_ts_code(dataset: str) -> None:
    """Sanity: the fixture's parquet contents must match the
    ``selected_ts_codes`` declared in ``_source.json`` — otherwise the
    traceability claim is fraudulent."""

    partition_dir = FIXTURE_ROOT / dataset / PARTITION_DIR
    source = json.loads((partition_dir / "_source.json").read_text(encoding="utf-8"))
    declared_ts_codes = set(source["selected_ts_codes"])

    manifest = json.loads((partition_dir / "_manifest.json").read_text(encoding="utf-8"))
    parquet_path = PROJECT_ROOT / "tests" / "dbt" / "fixtures" / "raw" / manifest["artifacts"][0]["path"]
    table = pq.read_table(parquet_path)
    actual_ts_codes = set(table.column("ts_code").to_pylist())

    assert actual_ts_codes.issubset(declared_ts_codes), (
        f"{dataset} parquet contains ts_codes {actual_ts_codes} not "
        f"declared in _source.json selected_ts_codes {declared_ts_codes}"
    )
