from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from data_platform import assets as assets_module
from data_platform.adapters.base import AssetSpec, DataSourceAdapter
from data_platform.assets import (
    DataPlatformAssetSpec,
    build_assets,
    build_resources,
    main,
)
from data_platform.config import Settings
from data_platform.raw import RawReader, RawWriter


pa = pytest.importorskip("pyarrow")


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DATA_PLATFORM = PROJECT_ROOT / "src" / "data_platform"


class FakeAdapter(DataSourceAdapter):
    def source_id(self) -> str:
        return "fake"

    def get_assets(self) -> list[AssetSpec]:
        return [
            AssetSpec(
                name="fake_stock_basic",
                dataset="stock_basic",
                partition="static",
                schema=pa.schema([("ts_code", pa.string())]),
            )
        ]

    def get_resources(self) -> dict[str, Any]:
        return {"source_id": self.source_id(), "token_env": "FAKE_TOKEN"}

    def get_staging_dbt_models(self) -> list[str]:
        return ["stg_stock_basic"]

    def get_quota_config(self) -> dict[str, Any]:
        return {"requests_per_minute": 1, "daily_credit_quota": None}


def test_build_assets_links_raw_staging_marts_and_canonical_specs() -> None:
    specs = build_assets([FakeAdapter()])
    by_key = {spec.key: spec for spec in specs}

    raw_key = ("raw", "fake", "stock_basic")
    staging_key = ("dbt", "stg_stock_basic")
    stock_basic_key = ("canonical", "stock_basic")
    mart_dbt_key = ("dbt", "mart_dim_security")
    mart_canonical_key = ("canonical", "dim_security")

    assert by_key[raw_key].kind == "raw"
    assert by_key[staging_key].deps == (raw_key,)
    assert by_key[stock_basic_key].deps == (staging_key,)
    assert by_key[mart_canonical_key].deps == (mart_dbt_key,)
    assert by_key[mart_canonical_key].metadata["identifier"] == "canonical.dim_security"
    assert by_key[mart_canonical_key].callable_import_path.endswith(":load_canonical_marts")

    _assert_dependency_order(specs)
    _assert_acyclic(specs)


def test_build_resources_returns_runtime_objects_from_settings(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    settings = Settings(
        pg_dsn="postgresql://user:pass@localhost/data_platform",
        raw_zone_path=tmp_path / "raw",
        iceberg_warehouse_path=tmp_path / "warehouse",
        duckdb_path=tmp_path / "duckdb" / "data_platform.duckdb",
    )
    fake_catalog = object()
    monkeypatch.setattr(assets_module, "load_catalog", lambda: fake_catalog)

    resources = build_resources(settings)

    assert isinstance(resources["raw_writer"], RawWriter)
    assert resources["raw_writer"].raw_zone_path == settings.raw_zone_path
    assert isinstance(resources["raw_reader"], RawReader)
    assert resources["raw_reader"].raw_zone_path == settings.raw_zone_path
    assert resources["iceberg_catalog"] is fake_catalog
    assert resources["duckdb_path"] == settings.duckdb_path
    assert resources["dbt_project_dir"] == assets_module.DBT_PROJECT_DIR


def test_assets_cli_outputs_stable_json_with_canonical_marts(
    capsys: pytest.CaptureFixture[str],
) -> None:
    exit_code = main(["--json"])

    captured = capsys.readouterr()
    payload = assets_module.json.loads(captured.out)
    canonical_identifiers = {
        item["metadata"]["identifier"]
        for item in payload
        if item["kind"] == "canonical"
    }

    assert exit_code == 0
    assert captured.err == ""
    assert "canonical.dim_security" in canonical_identifiers
    assert "canonical.dim_index" in canonical_identifiers
    assert "canonical.fact_price_bar" in canonical_identifiers
    assert "canonical.fact_financial_indicator" in canonical_identifiers
    assert "canonical.fact_event" in canonical_identifiers


def test_assets_cli_filters_by_kind(capsys: pytest.CaptureFixture[str]) -> None:
    exit_code = main(["--json", "--kind", "canonical"])

    payload = assets_module.json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload
    assert {item["kind"] for item in payload} == {"canonical"}


def test_data_platform_asset_spec_normalizes_tuple_fields() -> None:
    spec = DataPlatformAssetSpec(
        key=("dbt", "stg_stock_basic"),
        kind="dbt",
        deps=[("raw", "fake", "stock_basic")],  # type: ignore[arg-type]
        metadata={"model": "stg_stock_basic"},
        callable_import_path="dbt.cli.main:dbtRunner",
    )

    assert spec.key == ("dbt", "stg_stock_basic")
    assert spec.deps == (("raw", "fake", "stock_basic"),)
    assert spec.metadata["callable_import_path"] == "dbt.cli.main:dbtRunner"


def test_data_platform_package_has_no_orchestrator_runtime_definitions() -> None:
    for path in SRC_DATA_PLATFORM.rglob("*.py"):
        source = path.read_text(encoding="utf-8")
        lowered = source.lower()

        assert "import dagster" not in lowered, path
        assert "from dagster" not in lowered, path
        assert "definitions(" not in source, path
        assert "@job" not in source, path
        assert "@schedule" not in source, path
        assert "@sensor" not in source, path


def _assert_dependency_order(specs: list[DataPlatformAssetSpec]) -> None:
    position_by_key = {spec.key: index for index, spec in enumerate(specs)}
    for spec in specs:
        for dep in spec.deps:
            assert position_by_key[dep] < position_by_key[spec.key]


def _assert_acyclic(specs: list[DataPlatformAssetSpec]) -> None:
    deps_by_key = {spec.key: spec.deps for spec in specs}
    visiting: set[tuple[str, ...]] = set()
    visited: set[tuple[str, ...]] = set()

    def visit(key: tuple[str, ...]) -> None:
        if key in visited:
            return
        assert key not in visiting
        visiting.add(key)
        for dep in deps_by_key[key]:
            visit(dep)
        visiting.remove(key)
        visited.add(key)

    for key in deps_by_key:
        visit(key)
