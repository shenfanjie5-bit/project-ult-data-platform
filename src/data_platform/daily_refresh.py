"""Mock-scheduler entry point for the daily structured-data refresh."""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import os
import shutil
import subprocess
import sys
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, is_dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from time import perf_counter
from typing import Any, Literal, NoReturn
from uuid import UUID, uuid4

import pyarrow as pa  # type: ignore[import-untyped]
from pydantic import ValidationError

from data_platform.adapters.base import AssetSpec, FetchParams, FetchableAdapter
from data_platform.adapters.tushare.adapter import TushareAdapter, run_tushare_asset
from data_platform.adapters.tushare.assets import TUSHARE_ASSETS
from data_platform.assets import build_assets, build_resources
from data_platform.config import Settings, get_settings
from data_platform.ddl.iceberg_tables import DEFAULT_TABLE_SPECS, ensure_tables
from data_platform.ddl.runner import MigrationRunner
from data_platform.raw import RawArtifact, RawWriter, check_raw_zone
from data_platform.serving.canonical_writer import (
    WriteResult,
    load_canonical_marts,
    load_canonical_stock_basic,
)
from data_platform.serving.catalog import DEFAULT_NAMESPACES, ensure_namespaces


StepStatus = Literal["ok", "skipped", "failed"]
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DBT_SCRIPT = PROJECT_ROOT / "scripts" / "dbt.sh"
DATE_FORMAT = "%Y%m%d"
DEFAULT_DBT_SELECTORS = ("staging", "intermediate", "marts")
TRUTHY_VALUES = frozenset({"1", "true", "yes", "on"})
DATE_FIELD_NAMES = frozenset(
    {
        "actual_date",
        "ann_date",
        "base_date",
        "cal_date",
        "delist_date",
        "end_date",
        "ex_date",
        "exp_date",
        "f_ann_date",
        "float_date",
        "in_date",
        "list_date",
        "modify_date",
        "out_date",
        "pay_date",
        "pre_date",
        "pretrade_date",
        "record_date",
        "setup_date",
        "start_date",
        "trade_date",
    }
)
STRING_NUMERIC_FIELD_NAMES = frozenset(
    {
        "adj_factor",
        "amount",
        "base_point",
        "cash_div",
        "change",
        "circ_mv",
        "close",
        "dv_ratio",
        "dv_ttm",
        "employees",
        "float_share",
        "free_share",
        "high",
        "holder_num",
        "low",
        "open",
        "pb",
        "pct_chg",
        "pe",
        "pe_ttm",
        "pre_close",
        "ps",
        "ps_ttm",
        "reg_capital",
        "stk_div",
        "total_mv",
        "total_share",
        "turnover_rate",
        "turnover_rate_f",
        "vol",
        "volume_ratio",
        "weight",
    }
)


@dataclass(frozen=True)
class DailyRefreshStepResult:
    name: str
    status: StepStatus
    duration_ms: int
    metadata: dict[str, Any]


@dataclass(frozen=True)
class DailyRefreshResult:
    partition_date: date
    steps: list[DailyRefreshStepResult]
    ok: bool


class DailyRefreshConfigError(RuntimeError):
    """Raised when the daily refresh environment is incomplete."""


class DailyRefreshStepError(RuntimeError):
    """Raised by a refresh step with structured failure metadata."""

    def __init__(self, message: str, metadata: Mapping[str, Any]) -> None:
        super().__init__(message)
        self.metadata = dict(metadata)


class DailyRefreshLockError(DailyRefreshStepError):
    """Raised when another refresh owns the same catalog/date critical section."""


@dataclass(frozen=True)
class _DailyRefreshLock:
    path: Path
    fd: int

    def release(self) -> None:
        with contextlib.suppress(OSError):
            os.close(self.fd)
        with contextlib.suppress(FileNotFoundError):
            self.path.unlink()


class _JsonArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        raise ValueError(message)


class _MockTushareAdapter(FetchableAdapter):
    """Local fixture adapter used by CI and mock scheduler smoke tests."""

    def source_id(self) -> str:
        return "tushare"

    def get_assets(self) -> list[AssetSpec]:
        return list(TUSHARE_ASSETS)

    def get_resources(self) -> dict[str, Any]:
        return {"source_id": self.source_id(), "mode": "mock"}

    def get_staging_dbt_models(self) -> list[str]:
        return [f"stg_{asset.dataset}" for asset in TUSHARE_ASSETS]

    def get_quota_config(self) -> dict[str, Any]:
        return {"requests_per_minute": 1_000_000, "daily_credit_quota": None}

    def fetch(self, asset_id: str, params: FetchParams) -> pa.Table:
        asset = _asset_by_name(self.get_assets(), asset_id)
        partition_date = params.get("partition_date")
        if not isinstance(partition_date, date):
            msg = "mock adapter requires partition_date"
            raise ValueError(msg)
        return _mock_table(asset, partition_date)


def run_daily_refresh(
    partition_date: date,
    *,
    mock: bool = False,
    select: Sequence[str] | None = None,
    json_report: Path | None = None,
) -> DailyRefreshResult:
    """Run one daily refresh and optionally write a structured JSON report."""

    steps: list[DailyRefreshStepResult] = []

    try:
        settings = _load_settings()
        _prepare_runtime_paths(settings)
        adapter = _build_adapter(mock=mock)
        all_assets = adapter.get_assets()
        selected_assets = _select_assets(all_assets, select)
        asset_specs = build_assets([adapter])
    except Exception as exc:
        _append_failed_step(steps, "config", exc, started_at=perf_counter())
        result = _result(partition_date, steps)
        _write_report_if_requested(json_report, result)
        return result

    lock_started_at = perf_counter()
    try:
        refresh_lock = _acquire_refresh_lock(settings, partition_date)
    except DailyRefreshLockError as exc:
        _append_failed_step(steps, "refresh_lock", exc, started_at=lock_started_at)
        result = _result(partition_date, steps)
        _write_report_if_requested(json_report, result)
        return result

    try:
        if not _append_step(
            steps,
            "adapter",
            lambda: _run_adapter_step(
                adapter,
                settings,
                partition_date,
                selected_assets,
                asset_specs_count=len(asset_specs),
                mock=mock,
            ),
        ):
            result = _result(partition_date, steps)
            _write_report_if_requested(json_report, result)
            return result

        dbt_selectors = _dbt_selectors(selected_assets, all_assets)
        if not _append_step(
            steps,
            "dbt_run",
            lambda: _run_dbt_step(
                "run",
                settings,
                partition_date,
                selectors=dbt_selectors,
            ),
        ):
            result = _result(partition_date, steps)
            _write_report_if_requested(json_report, result)
            return result

        if not _append_step(
            steps,
            "dbt_test",
            lambda: _run_dbt_step(
                "test",
                settings,
                partition_date,
                selectors=dbt_selectors,
            ),
        ):
            result = _result(partition_date, steps)
            _write_report_if_requested(json_report, result)
            return result

        if not _append_step(
            steps,
            "canonical",
            lambda: _run_canonical_step(settings, selected_assets, all_assets),
        ):
            result = _result(partition_date, steps)
            _write_report_if_requested(json_report, result)
            return result

        _append_step(
            steps,
            "raw_health",
            lambda: _run_raw_health_step(settings, partition_date, selected_assets),
        )
        result = _result(partition_date, steps)
        _write_report_if_requested(json_report, result)
        return result
    finally:
        refresh_lock.release()


def main(argv: Sequence[str] | None = None) -> int:
    parser = _JsonArgumentParser(description="Run the data-platform daily refresh.")
    parser.add_argument("--date", required=True, help="partition date in YYYYMMDD format")
    parser.add_argument("--mock", action="store_true", help="use local fixture adapter data")
    parser.add_argument("--select", help="comma-separated asset names or datasets")
    parser.add_argument("--json-report", type=Path, help="write a daily refresh JSON report")

    try:
        args = parser.parse_args(argv)
        partition_date = _parse_partition_date(args.date)
    except Exception as exc:
        print(f"Daily refresh failed: {exc}", file=sys.stderr)
        return 2

    result = run_daily_refresh(
        partition_date,
        mock=args.mock or _env_flag("DP_DAILY_REFRESH_MOCK"),
        select=_split_select(args.select),
        json_report=args.json_report,
    )
    if result.ok:
        return 0

    failed_step = next((step for step in result.steps if step.status == "failed"), None)
    detail = "unknown failure" if failed_step is None else failed_step.metadata.get("error", "")
    print(f"Daily refresh failed: {detail}", file=sys.stderr)
    return 2 if failed_step is not None and failed_step.name == "config" else 1


def _load_settings() -> Settings:
    try:
        return get_settings()
    except ValidationError as exc:
        missing = {
            str(error["loc"][0]).upper()
            for error in exc.errors()
            if error.get("type") == "missing" and error.get("loc")
        }
        if "PG_DSN" in missing:
            msg = "DP_PG_DSN is required for the PostgreSQL-backed Iceberg catalog"
            raise DailyRefreshConfigError(msg) from exc
        msg = "daily refresh settings are incomplete: " + ", ".join(sorted(missing))
        raise DailyRefreshConfigError(msg) from exc


def _prepare_runtime_paths(settings: Settings) -> None:
    settings.raw_zone_path.expanduser().mkdir(parents=True, exist_ok=True)
    settings.iceberg_warehouse_path.expanduser().mkdir(parents=True, exist_ok=True)
    settings.duckdb_path.expanduser().parent.mkdir(parents=True, exist_ok=True)


def _build_adapter(*, mock: bool) -> FetchableAdapter:
    if mock:
        return _MockTushareAdapter()
    return TushareAdapter()


def _run_adapter_step(
    adapter: FetchableAdapter,
    settings: Settings,
    partition_date: date,
    selected_assets: Sequence[AssetSpec],
    *,
    asset_specs_count: int,
    mock: bool,
) -> dict[str, Any]:
    artifacts: list[RawArtifact] = []
    writer = RawWriter(
        settings.raw_zone_path,
        iceberg_warehouse_path=settings.iceberg_warehouse_path,
    )
    for asset in selected_assets:
        if mock:
            table = adapter.fetch(asset.name, {"partition_date": partition_date})
            artifacts.append(
                writer.write_arrow(
                    adapter.source_id(),
                    asset.dataset,
                    partition_date,
                    str(uuid4()),
                    table,
                )
            )
            continue
        artifacts.append(run_tushare_asset(asset.name, partition_date))

    return {
        "mock": mock,
        "asset_specs_count": asset_specs_count,
        "artifact_count": len(artifacts),
        "artifacts": [_raw_artifact_metadata(artifact) for artifact in artifacts],
    }


def _run_dbt_step(
    command: Literal["run", "test"],
    settings: Settings,
    partition_date: date,
    *,
    selectors: Sequence[str],
) -> dict[str, Any]:
    profiles_dir = settings.duckdb_path.expanduser().parent / "daily_refresh_dbt_profiles"
    target_path = (
        settings.duckdb_path.expanduser().parent
        / "daily_refresh_dbt_target"
        / f"{command}_{partition_date:%Y%m%d}"
    )
    profiles_dir.mkdir(parents=True, exist_ok=True)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    _write_duckdb_profile(profiles_dir / "profiles.yml")

    args = [
        command,
        "--profiles-dir",
        str(profiles_dir),
        "--target-path",
        str(target_path),
        "--select",
        *selectors,
    ]
    return _run_dbt_command(args, settings)


def _run_dbt_command(args: Sequence[str], settings: Settings) -> dict[str, Any]:
    dbt_executable = shutil.which("dbt")
    local_dbt = PROJECT_ROOT / ".venv" / "bin" / "dbt"
    if dbt_executable is None and local_dbt.exists():
        dbt_executable = str(local_dbt)
    if dbt_executable is None:
        msg = "dbt executable is not installed; expected dbt on PATH or .venv/bin/dbt"
        raise DailyRefreshStepError(msg, {"error": msg, "error_type": "missing_dbt"})

    env = os.environ.copy()
    env.update(
        {
            "DP_RAW_ZONE_PATH": str(settings.raw_zone_path),
            "DP_DUCKDB_PATH": str(settings.duckdb_path),
            "DP_ICEBERG_WAREHOUSE_PATH": str(settings.iceberg_warehouse_path),
            "DP_PG_DSN": str(settings.pg_dsn),
            "PYTHONPATH": f"{PROJECT_ROOT / 'src'}{os.pathsep}{env.get('PYTHONPATH', '')}",
        }
    )
    process_args = [str(DBT_SCRIPT), *args]
    completed = subprocess.run(
        process_args,
        cwd=PROJECT_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=300,
    )
    metadata = {
        "command": process_args,
        "dbt_executable": dbt_executable,
        "returncode": completed.returncode,
        "stdout_tail": _tail(completed.stdout),
        "stderr_tail": _tail(completed.stderr),
    }
    if completed.returncode != 0:
        msg = f"dbt command failed with exit code {completed.returncode}"
        metadata["error"] = msg
        metadata["error_type"] = "dbt_command_failed"
        raise DailyRefreshStepError(msg, metadata)
    return metadata


def _run_canonical_step(
    settings: Settings,
    selected_assets: Sequence[AssetSpec],
    all_assets: Sequence[AssetSpec],
) -> dict[str, Any]:
    migrations = MigrationRunner().apply_pending(str(settings.pg_dsn))
    resources = build_resources(settings)
    catalog = resources["iceberg_catalog"]
    ensure_namespaces(catalog, DEFAULT_NAMESPACES)
    ensure_tables(catalog, DEFAULT_TABLE_SPECS)

    selected_datasets = {asset.dataset for asset in selected_assets}
    all_datasets = {asset.dataset for asset in all_assets}
    write_results: list[WriteResult] = []
    skipped_writes: list[str] = []

    if "stock_basic" in selected_datasets:
        write_results.append(
            load_canonical_stock_basic(
                catalog,
                Path(resources["duckdb_path"]),
            )
        )
    else:
        skipped_writes.append("canonical.stock_basic")

    if selected_datasets == all_datasets:
        write_results.extend(
            load_canonical_marts(
                catalog,
                Path(resources["duckdb_path"]),
            )
        )
    else:
        skipped_writes.append("canonical.canonical_marts")

    return {
        "applied_migrations": migrations,
        "ensured_tables": [f"{spec.namespace}.{spec.name}" for spec in DEFAULT_TABLE_SPECS],
        "write_results": [_write_result_metadata(result) for result in write_results],
        "skipped_writes": skipped_writes,
    }


def _acquire_refresh_lock(
    settings: Settings,
    partition_date: date,
) -> _DailyRefreshLock:
    lock_path = _refresh_lock_path(settings, partition_date)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "catalog": settings.iceberg_catalog_name,
        "partition_date": f"{partition_date:%Y%m%d}",
        "pid": os.getpid(),
        "acquired_at": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
    try:
        fd = os.open(lock_path, flags, 0o600)
    except FileExistsError as exc:
        metadata = {
            "error": "daily refresh already running for catalog/date",
            "error_type": "refresh_lock_held",
            "catalog": settings.iceberg_catalog_name,
            "partition_date": f"{partition_date:%Y%m%d}",
            "lock_path": str(lock_path),
        }
        raise DailyRefreshLockError(metadata["error"], metadata) from exc

    try:
        os.write(fd, (json.dumps(payload, sort_keys=True) + "\n").encode("utf-8"))
    except Exception:
        with contextlib.suppress(OSError):
            os.close(fd)
        with contextlib.suppress(FileNotFoundError):
            lock_path.unlink()
        raise
    return _DailyRefreshLock(path=lock_path, fd=fd)


def _refresh_lock_path(settings: Settings, partition_date: date) -> Path:
    key = f"{settings.iceberg_catalog_name}:{partition_date:%Y%m%d}"
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()[:24]
    return (
        settings.iceberg_warehouse_path.expanduser()
        / "_daily_refresh_locks"
        / f"{digest}.lock"
    )


def _run_raw_health_step(
    settings: Settings,
    partition_date: date,
    selected_assets: Sequence[AssetSpec],
) -> dict[str, Any]:
    checked_artifacts = 0
    issue_payloads: list[dict[str, Any]] = []
    for asset in selected_assets:
        report = check_raw_zone(
            settings.raw_zone_path,
            source_id="tushare",
            dataset=asset.dataset,
            partition_date=partition_date,
            deep=True,
        )
        checked_artifacts += report.checked_artifacts
        issue_payloads.extend(
            {
                "severity": issue.severity,
                "path": str(issue.path),
                "code": issue.code,
                "message": issue.message,
            }
            for issue in report.issues
        )

    if any(issue["severity"] == "error" for issue in issue_payloads):
        msg = "Raw Zone health check failed"
        raise DailyRefreshStepError(
            msg,
            {
                "error": msg,
                "error_type": "raw_health_failed",
                "checked_artifacts": checked_artifacts,
                "issues": issue_payloads,
            },
        )

    return {
        "checked_artifacts": checked_artifacts,
        "issues": issue_payloads,
    }


def _append_step(
    steps: list[DailyRefreshStepResult],
    name: str,
    callback: Any,
) -> bool:
    started_at = perf_counter()
    try:
        metadata = callback()
    except Exception as exc:
        _append_failed_step(steps, name, exc, started_at=started_at)
        return False

    steps.append(
        DailyRefreshStepResult(
            name=name,
            status="ok",
            duration_ms=_duration_ms(started_at),
            metadata=_json_safe(metadata),
        )
    )
    return True


def _append_failed_step(
    steps: list[DailyRefreshStepResult],
    name: str,
    exc: BaseException,
    *,
    started_at: float,
) -> None:
    metadata: dict[str, Any] = {
        "error": str(exc),
        "error_type": type(exc).__name__,
    }
    if isinstance(exc, DailyRefreshStepError):
        metadata.update(exc.metadata)
    steps.append(
        DailyRefreshStepResult(
            name=name,
            status="failed",
            duration_ms=_duration_ms(started_at),
            metadata=_json_safe(metadata),
        )
    )


def _result(partition_date: date, steps: list[DailyRefreshStepResult]) -> DailyRefreshResult:
    return DailyRefreshResult(
        partition_date=partition_date,
        steps=steps,
        ok=not any(step.status == "failed" for step in steps),
    )


def _select_assets(
    assets: Sequence[AssetSpec],
    select: Sequence[str] | None,
) -> list[AssetSpec]:
    tokens = _normalize_select(select)
    if not tokens:
        return list(assets)

    by_name_or_dataset: dict[str, AssetSpec] = {}
    for asset in assets:
        by_name_or_dataset[asset.name] = asset
        by_name_or_dataset[asset.dataset] = asset

    selected: list[AssetSpec] = []
    unknown: list[str] = []
    for token in tokens:
        asset = by_name_or_dataset.get(token)
        if asset is None:
            unknown.append(token)
            continue
        if asset not in selected:
            selected.append(asset)

    if unknown:
        msg = "unknown daily refresh asset selector(s): " + ", ".join(unknown)
        raise ValueError(msg)
    if not selected:
        msg = "daily refresh select resolved to no assets"
        raise ValueError(msg)
    return selected


def _dbt_selectors(
    selected_assets: Sequence[AssetSpec],
    all_assets: Sequence[AssetSpec],
) -> tuple[str, ...]:
    selected_datasets = {asset.dataset for asset in selected_assets}
    all_datasets = {asset.dataset for asset in all_assets}
    if selected_datasets == all_datasets:
        return DEFAULT_DBT_SELECTORS
    return tuple(f"stg_{asset.dataset}" for asset in selected_assets)


def _normalize_select(select: Sequence[str] | None) -> list[str]:
    if select is None:
        return []
    tokens: list[str] = []
    for item in select:
        tokens.extend(part.strip() for part in str(item).split(","))
    return [token for token in tokens if token]


def _split_select(value: str | None) -> list[str] | None:
    if value is None:
        return None
    return _normalize_select([value])


def _asset_by_name(assets: Sequence[AssetSpec], asset_id: str) -> AssetSpec:
    for asset in assets:
        if asset.name == asset_id:
            return asset
    msg = f"unknown mock asset: {asset_id!r}"
    raise ValueError(msg)


def _mock_table(asset: AssetSpec, partition_date: date) -> pa.Table:
    return pa.table(
        {
            field.name: [_mock_value(asset.dataset, field, partition_date)]
            for field in asset.schema
        },
        schema=asset.schema,
    )


def _mock_value(dataset: str, field: pa.Field, partition_date: date) -> Any:
    if field.name in DATE_FIELD_NAMES:
        return _mock_date_value(dataset, field.name, partition_date)
    if field.name == "ts_code":
        if dataset in {"index_basic", "index_daily"}:
            return "000300.SH"
        return "000001.SZ"
    if field.name == "index_code":
        return "000300.SH"
    if field.name == "con_code":
        return "000001.SZ"
    if field.name == "symbol":
        return "000001"
    if field.name == "list_status":
        return "L"
    if field.name in {"report_type", "comp_type", "is_open"}:
        return "1"
    if field.name == "update_flag":
        return "0"
    if field.name == "url":
        return "https://example.test/tushare-fixture"
    if field.name == "rec_time":
        return "10:30:00"
    if field.name in STRING_NUMERIC_FIELD_NAMES:
        return "1.123456789012345678"
    if pa.types.is_decimal(field.type):
        return Decimal("1.123456789012345678")
    return f"{field.name}-fixture"


def _mock_date_value(dataset: str, field_name: str, partition_date: date) -> str | None:
    if dataset == "stock_basic" and field_name == "delist_date":
        return None
    if dataset in {"income", "balancesheet", "cashflow", "fina_indicator"}:
        if field_name == "end_date":
            return f"{partition_date.replace(day=1) - timedelta(days=1):%Y%m%d}"
        if field_name == "f_ann_date":
            return f"{partition_date + timedelta(days=1):%Y%m%d}"
    return f"{partition_date:%Y%m%d}"


def _write_duckdb_profile(path: Path) -> None:
    path.write_text(
        """
data_platform:
  target: daily_refresh
  outputs:
    daily_refresh:
      type: duckdb
      path: "{{ env_var('DP_DUCKDB_PATH') }}"
      threads: 1
""".lstrip(),
        encoding="utf-8",
    )


def _parse_partition_date(value: str) -> date:
    try:
        parsed = datetime.strptime(value, DATE_FORMAT).date()
    except ValueError as exc:
        msg = f"date must use YYYYMMDD format: {value!r}"
        raise ValueError(msg) from exc
    if f"{parsed:%Y%m%d}" != value:
        msg = f"date must use YYYYMMDD format: {value!r}"
        raise ValueError(msg)
    return parsed


def _write_report_if_requested(json_report: Path | None, result: DailyRefreshResult) -> None:
    if json_report is None:
        return
    json_report.expanduser().parent.mkdir(parents=True, exist_ok=True)
    json_report.expanduser().write_text(
        json.dumps(_result_to_dict(result), ensure_ascii=False, indent=2, sort_keys=True)
        + "\n",
        encoding="utf-8",
    )


def _result_to_dict(result: DailyRefreshResult) -> dict[str, Any]:
    return {
        "partition_date": result.partition_date.isoformat(),
        "ok": result.ok,
        "steps": [
            {
                "name": step.name,
                "status": step.status,
                "duration_ms": step.duration_ms,
                "metadata": _json_safe(step.metadata),
            }
            for step in result.steps
        ],
    }


def _raw_artifact_metadata(artifact: RawArtifact) -> dict[str, Any]:
    parsed_run_id = UUID(artifact.run_id)
    return {
        "source_id": artifact.source_id,
        "dataset": artifact.dataset,
        "partition_date": artifact.partition_date.isoformat(),
        "run_id": artifact.run_id,
        "run_id_version": parsed_run_id.version,
        "path": str(artifact.path),
        "row_count": artifact.row_count,
        "written_at": artifact.written_at.isoformat(),
    }


def _write_result_metadata(result: WriteResult) -> dict[str, Any]:
    return asdict(result)


def _json_safe(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        return _json_safe(asdict(value))
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, str | int | float | bool) or value is None:
        return value
    return str(value)


def _tail(value: str, *, max_lines: int = 40) -> str:
    lines = value.splitlines()
    return "\n".join(lines[-max_lines:])


def _duration_ms(started_at: float) -> int:
    return int((perf_counter() - started_at) * 1000)


def _env_flag(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in TRUTHY_VALUES


__all__ = [
    "DailyRefreshResult",
    "DailyRefreshStepResult",
    "main",
    "run_daily_refresh",
]


if __name__ == "__main__":
    raise SystemExit(main())
