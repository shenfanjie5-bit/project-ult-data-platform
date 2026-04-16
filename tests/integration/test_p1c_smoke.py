from __future__ import annotations

from collections.abc import Iterator
import json
import os
from pathlib import Path
import subprocess
import sys
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import SQLAlchemyError

from data_platform.ddl.runner import _sqlalchemy_postgres_uri


PROJECT_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture()
def postgres_dsn() -> Iterator[str]:
    admin_dsn = os.environ.get("DATABASE_URL")
    if not admin_dsn:
        pytest.skip("P1c smoke integration requires DATABASE_URL")

    admin_engine = create_engine(_sqlalchemy_postgres_uri(admin_dsn), isolation_level="AUTOCOMMIT")
    database_name = f"dp_p1c_smoke_{uuid4().hex}"
    try:
        with admin_engine.connect() as connection:
            connection.execute(text(f'CREATE DATABASE "{database_name}"'))
    except SQLAlchemyError as exc:
        admin_engine.dispose()
        pytest.skip(f"P1c smoke integration requires a usable PostgreSQL DATABASE_URL: {exc}")

    test_dsn = str(make_url(admin_dsn).set(database=database_name))
    try:
        yield test_dsn
    finally:
        with admin_engine.connect() as connection:
            connection.execute(
                text(
                    """
                    SELECT pg_terminate_backend(pid)
                    FROM pg_stat_activity
                    WHERE datname = :database_name
                      AND pid <> pg_backend_pid()
                    """
                ),
                {"database_name": database_name},
            )
            connection.execute(text(f'DROP DATABASE IF EXISTS "{database_name}"'))
        admin_engine.dispose()


def test_p1c_smoke_pipeline_is_repeatable(tmp_path: Path, postgres_dsn: str) -> None:
    env = _smoke_env(tmp_path, postgres_dsn)

    first = _run_smoke(env)
    second = _run_smoke(env)

    first_payload = _json_summary(first.stdout)
    second_payload = _json_summary(second.stdout)

    assert first_payload["accepted"] == 2
    assert first_payload["candidate_count"] == 2
    assert first_payload["formal_row_count"] == 2
    assert second_payload["accepted"] == 2
    assert second_payload["candidate_count"] == 2
    assert second_payload["formal_row_count"] == 2


def test_p1c_smoke_requires_dp_pg_dsn(tmp_path: Path) -> None:
    env = _base_script_env(tmp_path)

    result = _run_smoke_raw(env)

    assert result.returncode == 2
    assert "DP_PG_DSN is required" in result.stderr


def test_p1c_smoke_requires_destructive_confirmation(tmp_path: Path) -> None:
    env = _base_script_env(tmp_path)
    env.update(
        {
            "DP_PG_DSN": "postgresql://dp:dp@localhost/dp_p1c_smoke_local",
            "DP_ENV": "test",
        }
    )

    result = _run_smoke_raw(env)

    assert result.returncode == 2
    assert "DP_SMOKE_P1C_CONFIRM_DESTRUCTIVE=1" in result.stderr


def _smoke_env(tmp_path: Path, postgres_dsn: str) -> dict[str, str]:
    smoke_dir = tmp_path / "p1c-smoke"
    env = _base_script_env(tmp_path)
    env.update(
        {
            "DP_PG_DSN": postgres_dsn,
            "DP_SMOKE_WORK_DIR": str(smoke_dir),
            "DP_RAW_ZONE_PATH": str(smoke_dir / "raw"),
            "DP_ICEBERG_WAREHOUSE_PATH": str(smoke_dir / "warehouse"),
            "DP_DUCKDB_PATH": str(smoke_dir / "data_platform.duckdb"),
            "DP_ICEBERG_CATALOG_NAME": "data_platform_p1c_smoke",
            "DP_ENV": "test",
            "DP_SMOKE_P1C_CONFIRM_DESTRUCTIVE": "1",
            "PYTHON": sys.executable,
            "PYTHONPATH": str(PROJECT_ROOT / "src"),
        }
    )
    env["PATH"] = f"{Path(sys.executable).parent}{os.pathsep}{env.get('PATH', '')}"
    return env


def _base_script_env(tmp_path: Path) -> dict[str, str]:
    smoke_dir = tmp_path / "p1c-smoke"
    env = os.environ.copy()
    env.update(
        {
            "DP_SMOKE_WORK_DIR": str(smoke_dir),
            "DP_RAW_ZONE_PATH": str(smoke_dir / "raw"),
            "DP_ICEBERG_WAREHOUSE_PATH": str(smoke_dir / "warehouse"),
            "DP_DUCKDB_PATH": str(smoke_dir / "data_platform.duckdb"),
            "DP_ICEBERG_CATALOG_NAME": "data_platform_p1c_smoke",
            "DP_ENV": "test",
            "PYTHON": sys.executable,
            "PYTHONPATH": str(PROJECT_ROOT / "src"),
        }
    )
    env.pop("DP_PG_DSN", None)
    env.pop("DP_SMOKE_P1C_CONFIRM_DESTRUCTIVE", None)
    env["PATH"] = f"{Path(sys.executable).parent}{os.pathsep}{env.get('PATH', '')}"
    return env


def _run_smoke_raw(env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", str(PROJECT_ROOT / "scripts" / "smoke_p1c.sh")],
        cwd=PROJECT_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=300,
    )


def _run_smoke(env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    result = _run_smoke_raw(env)

    assert result.returncode == 0, result.stdout + result.stderr
    assert "P1c smoke OK" in result.stdout
    return result


def _json_summary(stdout: str) -> dict[str, object]:
    for line in stdout.splitlines():
        if line.startswith("{") and line.endswith("}"):
            return json.loads(line)
    raise AssertionError(f"missing JSON summary in output: {stdout}")
