from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import date
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest

from data_platform import daily_refresh
from data_platform.config import reset_settings_cache
from data_platform.serving.canonical_writer import CANONICAL_MART_LOAD_SPECS, WriteResult


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PARTITION_DATE = date(2026, 4, 15)


def test_mock_daily_refresh_is_repeatable_and_writes_report(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _set_daily_refresh_env(monkeypatch, tmp_path)
    assert "DP_TUSHARE_TOKEN" not in os.environ
    _install_fast_success_stubs(monkeypatch)

    first_report = tmp_path / "daily-refresh-first.json"
    second_report = tmp_path / "daily-refresh-second.json"

    first = daily_refresh.run_daily_refresh(
        PARTITION_DATE,
        mock=True,
        json_report=first_report,
    )
    second = daily_refresh.run_daily_refresh(
        PARTITION_DATE,
        mock=True,
        json_report=second_report,
    )

    assert first.ok is True
    assert second.ok is True
    assert [step.name for step in second.steps] == [
        "adapter",
        "dbt_run",
        "dbt_test",
        "canonical",
        "raw_health",
    ]
    assert {step.status for step in second.steps} == {"ok"}

    report = json.loads(second_report.read_text(encoding="utf-8"))
    assert report["ok"] is True
    assert report["partition_date"] == "2026-04-15"
    assert {step["name"] for step in report["steps"]} >= {
        "adapter",
        "dbt_run",
        "dbt_test",
        "canonical",
        "raw_health",
    }
    assert all(isinstance(step["duration_ms"], int) for step in report["steps"])

    adapter_step = _step(report, "adapter")
    assert adapter_step["metadata"]["mock"] is True
    assert adapter_step["metadata"]["artifact_count"] == len(daily_refresh.TUSHARE_ASSETS)
    assert all(
        UUID(artifact["run_id"]).version == 4
        for artifact in adapter_step["metadata"]["artifacts"]
    )

    stock_basic_manifest = json.loads(
        (
            tmp_path
            / "raw"
            / "tushare"
            / "stock_basic"
            / "dt=20260415"
            / "_manifest.json"
        ).read_text(encoding="utf-8")
    )
    assert len(stock_basic_manifest["artifacts"]) == 2
    assert all(
        UUID(artifact["run_id"]).version == 4
        for artifact in stock_basic_manifest["artifacts"]
    )

    canonical_step = _step(report, "canonical")
    write_results = canonical_step["metadata"]["write_results"]
    assert [item["row_count"] for item in write_results] == [1, 1, 1, 1, 1, 1]
    assert canonical_step["metadata"]["skipped_writes"] == []

    raw_health_step = _step(report, "raw_health")
    assert raw_health_step["metadata"]["checked_artifacts"] == 2 * len(
        daily_refresh.TUSHARE_ASSETS
    )
    assert raw_health_step["metadata"]["issues"] == []


def test_daily_refresh_reports_missing_dp_pg_dsn(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    _set_daily_refresh_env(monkeypatch, tmp_path, include_pg_dsn=False)
    report_path = tmp_path / "missing-dsn-report.json"

    result = daily_refresh.run_daily_refresh(
        PARTITION_DATE,
        mock=True,
        json_report=report_path,
    )

    assert result.ok is False
    assert result.steps[0].name == "config"
    assert result.steps[0].status == "failed"
    assert "DP_PG_DSN is required" in result.steps[0].metadata["error"]

    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["ok"] is False
    assert "DP_PG_DSN is required" in report["steps"][0]["metadata"]["error"]


def test_daily_refresh_script_reports_missing_dp_pg_dsn(tmp_path: Path) -> None:
    env = os.environ.copy()
    env.update(
        {
            "DP_RAW_ZONE_PATH": str(tmp_path / "raw"),
            "DP_ICEBERG_WAREHOUSE_PATH": str(tmp_path / "warehouse"),
            "DP_DUCKDB_PATH": str(tmp_path / "duckdb" / "data_platform.duckdb"),
            "DP_ICEBERG_CATALOG_NAME": "data_platform_daily_refresh_test",
            "DP_ENV": "test",
            "PYTHON": sys.executable,
            "PYTHONPATH": str(PROJECT_ROOT / "src"),
        }
    )
    env.pop("DP_PG_DSN", None)
    env.pop("DP_TUSHARE_TOKEN", None)
    report_path = tmp_path / "script-missing-dsn.json"

    result = subprocess.run(
        [
            "bash",
            str(PROJECT_ROOT / "scripts" / "daily_refresh.sh"),
            "--date",
            "20260415",
            "--mock",
            "--json-report",
            str(report_path),
        ],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )

    assert result.returncode == 2
    assert "DP_PG_DSN is required" in result.stderr
    assert "Daily refresh OK" not in result.stdout

    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["ok"] is False
    assert report["steps"][0]["name"] == "config"


def _set_daily_refresh_env(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    include_pg_dsn: bool = True,
) -> dict[str, str]:
    values = {
        "DP_RAW_ZONE_PATH": str(tmp_path / "raw"),
        "DP_ICEBERG_WAREHOUSE_PATH": str(tmp_path / "warehouse"),
        "DP_DUCKDB_PATH": str(tmp_path / "duckdb" / "data_platform.duckdb"),
        "DP_ICEBERG_CATALOG_NAME": "data_platform_daily_refresh_test",
        "DP_ENV": "test",
    }
    if include_pg_dsn:
        values["DP_PG_DSN"] = "postgresql://dp:dp@localhost/data_platform_daily_refresh_test"
    else:
        monkeypatch.delenv("DP_PG_DSN", raising=False)

    for key, value in values.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("DP_TUSHARE_TOKEN", raising=False)
    reset_settings_cache()
    return values


def _install_fast_success_stubs(monkeypatch: pytest.MonkeyPatch) -> None:
    dbt_calls: list[list[str]] = []
    snapshots = iter(range(1001, 2000))

    class FakeMigrationRunner:
        def apply_pending(self, dsn: str) -> list[str]:
            assert dsn.startswith("postgresql://")
            return []

    def fake_run_dbt_command(args: list[str], settings: Any) -> dict[str, Any]:
        dbt_calls.append(args)
        return {
            "command": ["dbt", *args],
            "dbt_executable": "dbt",
            "returncode": 0,
            "stdout_tail": "",
            "stderr_tail": "",
            "duckdb_path": str(settings.duckdb_path),
        }

    def fake_build_resources(settings: Any) -> dict[str, Any]:
        return {
            "iceberg_catalog": object(),
            "duckdb_path": settings.duckdb_path,
        }

    def fake_load_stock_basic(_catalog: object, _duckdb_path: Path) -> WriteResult:
        return WriteResult(
            table="canonical.stock_basic",
            snapshot_id=next(snapshots),
            row_count=1,
            duration_ms=0,
        )

    def fake_load_marts(_catalog: object, _duckdb_path: Path) -> list[WriteResult]:
        return [
            WriteResult(
                table=spec.identifier,
                snapshot_id=next(snapshots),
                row_count=1,
                duration_ms=0,
            )
            for spec in CANONICAL_MART_LOAD_SPECS
        ]

    monkeypatch.setattr(daily_refresh, "MigrationRunner", FakeMigrationRunner)
    monkeypatch.setattr(daily_refresh, "_run_dbt_command", fake_run_dbt_command)
    monkeypatch.setattr(daily_refresh, "build_resources", fake_build_resources)
    monkeypatch.setattr(daily_refresh, "ensure_namespaces", lambda *_args: None)
    monkeypatch.setattr(daily_refresh, "ensure_tables", lambda *_args: [])
    monkeypatch.setattr(daily_refresh, "load_canonical_stock_basic", fake_load_stock_basic)
    monkeypatch.setattr(daily_refresh, "load_canonical_marts", fake_load_marts)


def _step(report: dict[str, Any], name: str) -> dict[str, Any]:
    for step in report["steps"]:
        if step["name"] == name:
            return step
    raise AssertionError(f"missing daily refresh step: {name}")
