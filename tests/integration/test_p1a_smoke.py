from __future__ import annotations

from collections.abc import Iterator
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import SQLAlchemyError


PROJECT_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_ROW_COUNT = 3
EXPECTED_COLUMNS = [
    "ts_code",
    "symbol",
    "name",
    "area",
    "industry",
    "market",
    "list_date",
    "is_active",
    "source_run_id",
    "canonical_loaded_at",
]


@pytest.fixture()
def postgres_dsn() -> Iterator[str]:
    admin_dsn = os.environ.get("DATABASE_URL")
    if not admin_dsn:
        pytest.skip("P1a smoke integration requires DATABASE_URL")

    admin_engine = create_engine(_sqlalchemy_postgres_uri(admin_dsn), isolation_level="AUTOCOMMIT")
    database_name = f"dp_p1a_smoke_{uuid4().hex}"
    try:
        with admin_engine.connect() as connection:
            connection.execute(text(f'CREATE DATABASE "{database_name}"'))
    except SQLAlchemyError as exc:
        admin_engine.dispose()
        pytest.skip(f"P1a smoke integration requires a usable PostgreSQL DATABASE_URL: {exc}")

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


def test_p1a_smoke_pipeline_is_repeatable(
    tmp_path: Path,
    postgres_dsn: str,
) -> None:
    env = _smoke_env(tmp_path, postgres_dsn)

    first = _run_smoke(env)
    second = _run_smoke(env)
    payload = _query_canonical_payload(env)

    assert "P1a smoke OK" in first.stdout
    assert "P1a smoke OK" in second.stdout
    assert _duration_seconds(second.stdout) < 300
    assert payload["catalog_row_count"] == FIXTURE_ROW_COUNT
    assert payload["duckdb_row_count"] == FIXTURE_ROW_COUNT
    assert payload["duckdb_columns"] == EXPECTED_COLUMNS


def _smoke_env(tmp_path: Path, postgres_dsn: str) -> dict[str, str]:
    smoke_dir = tmp_path / "smoke"
    env = os.environ.copy()
    env.update(
        {
            "DP_PG_DSN": postgres_dsn,
            "DP_SMOKE_WORK_DIR": str(smoke_dir),
            "DP_RAW_ZONE_PATH": str(smoke_dir / "raw"),
            "DP_ICEBERG_WAREHOUSE_PATH": str(smoke_dir / "warehouse"),
            "DP_DUCKDB_PATH": str(smoke_dir / "data_platform.duckdb"),
            "DP_ICEBERG_CATALOG_NAME": "data_platform_p1a_test",
            "DP_ENV": "test",
            "PYTHON": sys.executable,
            "PYTHONPATH": str(PROJECT_ROOT / "src"),
        }
    )
    env["PATH"] = f"{Path(sys.executable).parent}{os.pathsep}{env.get('PATH', '')}"
    return env


def _run_smoke(env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        ["bash", str(PROJECT_ROOT / "scripts" / "smoke_p1a.sh")],
        cwd=PROJECT_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=300,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert "skipped" not in result.stdout.lower()
    return result


def _query_canonical_payload(env: dict[str, str]) -> dict[str, object]:
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            """
from __future__ import annotations

import json

from data_platform.serving.catalog import load_catalog
from data_platform.serving.reader import read_canonical

catalog_table = load_catalog().load_table("canonical.stock_basic").scan().to_arrow()
duckdb_table = read_canonical("stock_basic")
print(
    json.dumps(
        {
            "catalog_row_count": catalog_table.num_rows,
            "duckdb_columns": duckdb_table.schema.names,
            "duckdb_row_count": duckdb_table.num_rows,
        },
        sort_keys=True,
    )
)
""",
        ],
        cwd=PROJECT_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    return json.loads(result.stdout.strip().splitlines()[-1])


def _duration_seconds(output: str) -> int:
    match = re.search(r"duration_s=(\d+)", output)
    assert match is not None, output
    return int(match.group(1))


def _sqlalchemy_postgres_uri(dsn: str) -> str:
    if dsn.startswith("postgresql://"):
        return "postgresql+psycopg://" + dsn.removeprefix("postgresql://")
    if dsn.startswith("postgres://"):
        return "postgresql+psycopg://" + dsn.removeprefix("postgres://")
    return dsn
