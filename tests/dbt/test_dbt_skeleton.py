from __future__ import annotations

import os
import shutil
import subprocess
from datetime import date
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DBT_PROJECT_DIR = PROJECT_ROOT / "src" / "data_platform" / "dbt"
DBT_FIXTURE_RAW = PROJECT_ROOT / "tests" / "dbt" / "fixtures" / "raw"


def test_dbt_skeleton_files_are_present() -> None:
    required_paths = [
        DBT_PROJECT_DIR / "dbt_project.yml",
        DBT_PROJECT_DIR / "profiles.yml.example",
        DBT_PROJECT_DIR / "macros" / "dp_raw_path.sql",
        DBT_PROJECT_DIR / "models" / "staging" / "_sources.yml",
        DBT_PROJECT_DIR / "models" / "staging" / "stg_stock_basic.sql",
        DBT_PROJECT_DIR / "models" / "intermediate" / ".gitkeep",
        DBT_PROJECT_DIR / "models" / "marts" / ".gitkeep",
        DBT_PROJECT_DIR / "seeds" / ".gitkeep",
        DBT_PROJECT_DIR / "macros" / ".gitkeep",
        PROJECT_ROOT / "scripts" / "dbt.sh",
        DBT_FIXTURE_RAW / "tushare" / "stock_basic" / "dt=20260415" / "sample.parquet",
    ]

    missing_paths = [path for path in required_paths if not path.exists()]

    assert missing_paths == []
    assert os.access(PROJECT_ROOT / "scripts" / "dbt.sh", os.X_OK)
    sql_models = sorted(
        path.relative_to(DBT_PROJECT_DIR).as_posix()
        for path in (DBT_PROJECT_DIR / "models").glob("**/*.sql")
    )
    assert sql_models == ["models/staging/stg_stock_basic.sql"]

    dbt_project = (DBT_PROJECT_DIR / "dbt_project.yml").read_text()
    assert "name: data_platform" in dbt_project
    assert "profile: data_platform" in dbt_project
    assert 'require-dbt-version: ">=1.7"' in dbt_project
    assert "+materialized: view" in dbt_project
    assert "+materialized: table" in dbt_project

    profile = (DBT_PROJECT_DIR / "profiles.yml.example").read_text()
    assert "DP_DUCKDB_PATH" in profile
    assert "DP_PG_DSN" in profile
    assert 'extensions: ["iceberg", "httpfs"]' in profile
    assert "type: postgres" in profile


def test_stg_stock_basic_files_match_issue_contract() -> None:
    sources_yml = (DBT_PROJECT_DIR / "models" / "staging" / "_sources.yml").read_text()
    model_sql = (
        DBT_PROJECT_DIR / "models" / "staging" / "stg_stock_basic.sql"
    ).read_text()
    macro_sql = (DBT_PROJECT_DIR / "macros" / "dp_raw_path.sql").read_text()
    lowered_model_sql = model_sql.lower()

    assert "name: raw" in sources_yml
    assert "name: tushare_stock_basic" in sources_yml
    assert "Raw Zone path pattern: ${DP_RAW_ZONE_PATH}/tushare/stock_basic/**/*.parquet" in (
        sources_yml
    )
    assert "name: stg_stock_basic" in sources_yml
    assert "- unique" in sources_yml
    assert "- not_null" in sources_yml

    assert "DP_RAW_ZONE_PATH" in macro_sql
    assert '"/" ~ source_id ~ "/" ~ dataset ~ "/**/*.parquet"' in macro_sql

    assert '{{ config(materialized="view") }}' in model_sql
    assert "read_parquet" in model_sql
    assert '{{ dp_raw_path("tushare", "stock_basic") }}' in model_sql
    assert "hive_partitioning=1" in model_sql
    assert "filename=1" in model_sql
    assert "trim(cast(ts_code as varchar))" in model_sql
    assert "strptime(nullif(trim(cast(list_date as varchar)), ''), '%Y%m%d')::date" in (
        model_sql
    )
    assert "source_run_id" in model_sql
    assert "raw_loaded_at" in model_sql
    assert "canonical." not in lowered_model_sql
    assert "iceberg_scan" not in lowered_model_sql


def test_stg_stock_basic_sql_transforms_fixture_with_duckdb(tmp_path: Path) -> None:
    duckdb = pytest.importorskip("duckdb")

    raw_zone_path = tmp_path / "raw"
    shutil.copytree(DBT_FIXTURE_RAW, raw_zone_path)
    model_sql = _render_stg_stock_basic_sql_for_raw_path(raw_zone_path)

    connection = duckdb.connect(":memory:")
    try:
        connection.execute(f"create view stg_stock_basic as {model_sql}")
        transformed_rows = connection.execute(
            """
            select ts_code, list_date, source_run_id, raw_loaded_at, is_active
            from stg_stock_basic
            order by ts_code
            """
        ).fetchall()
        type_row = connection.execute(
            "select typeof(ts_code), typeof(list_date) from stg_stock_basic limit 1"
        ).fetchone()
    finally:
        connection.close()

    assert len(transformed_rows) == 3
    assert transformed_rows[0][0] == "000001.SZ"
    assert transformed_rows[0][1] == date(1991, 4, 3)
    assert transformed_rows[0][2] == "sample"
    assert transformed_rows[0][3] is not None
    assert transformed_rows[2][4] is False
    assert type_row == ("VARCHAR", "DATE")


def resolve_dbt_executable() -> str | None:
    dbt = shutil.which("dbt")
    if dbt is not None:
        return dbt

    local_dbt = PROJECT_ROOT / ".venv" / "bin" / "dbt"
    if local_dbt.exists():
        return str(local_dbt)

    return None


def require_working_dbt() -> str:
    dbt = resolve_dbt_executable()
    if dbt is None:
        pytest.skip("dbt executable is not installed in this environment")

    version_result = subprocess.run(
        [dbt, "--version"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    version_output = version_result.stdout + version_result.stderr
    if version_result.returncode != 0:
        if os.environ.get("DP_DBT_RUNTIME_OPTIONAL") == "1":
            pytest.skip(
                "dbt runtime is explicitly optional in this environment; "
                f"startup failed:\n{version_output}"
            )
        pytest.fail(f"dbt --version failed:\n{version_output}")

    return dbt


def test_dbt_parse_succeeds_with_skeleton_profile(tmp_path: Path) -> None:
    dbt = require_working_dbt()

    profiles_dir = tmp_path / "profiles"
    profiles_dir.mkdir()
    shutil.copyfile(DBT_PROJECT_DIR / "profiles.yml.example", profiles_dir / "profiles.yml")

    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = str(profiles_dir)
    env["DP_DUCKDB_PATH"] = str(tmp_path / "data_platform.duckdb")
    env["DP_PG_DSN"] = "postgresql://dp:dp@localhost:5432/data_platform"

    result = subprocess.run(
        [
            dbt,
            "parse",
            "--project-dir",
            str(DBT_PROJECT_DIR),
            "--profiles-dir",
            str(profiles_dir),
            "--target-path",
            str(tmp_path / "target"),
        ],
        cwd=PROJECT_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr


def test_stg_stock_basic_run_and_test_with_fixture(tmp_path: Path) -> None:
    duckdb = pytest.importorskip("duckdb")
    dbt = require_working_dbt()

    raw_zone_path = tmp_path / "raw"
    shutil.copytree(DBT_FIXTURE_RAW, raw_zone_path)
    duckdb_path = tmp_path / "data_platform.duckdb"
    profiles_dir = tmp_path / "profiles"
    profiles_dir.mkdir()
    _write_duckdb_only_profile(profiles_dir / "profiles.yml")

    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = str(profiles_dir)
    env["DP_RAW_ZONE_PATH"] = str(raw_zone_path)
    env["DP_DUCKDB_PATH"] = str(duckdb_path)
    env["PATH"] = f"{Path(dbt).parent}{os.pathsep}{env.get('PATH', '')}"

    debug_result = _run_dbt_wrapper(
        [
            "debug",
            "--profiles-dir",
            str(profiles_dir),
        ],
        env=env,
    )
    assert debug_result.returncode == 0, debug_result.stdout + debug_result.stderr

    run_result = _run_dbt_wrapper(
        [
            "run",
            "--profiles-dir",
            str(profiles_dir),
            "--target-path",
            str(tmp_path / "target-run"),
            "--select",
            "stg_stock_basic",
        ],
        env=env,
    )
    assert run_result.returncode == 0, run_result.stdout + run_result.stderr

    test_result = _run_dbt_wrapper(
        [
            "test",
            "--profiles-dir",
            str(profiles_dir),
            "--target-path",
            str(tmp_path / "target-test"),
            "--select",
            "stg_stock_basic",
        ],
        env=env,
    )
    assert test_result.returncode == 0, test_result.stdout + test_result.stderr

    connection = duckdb.connect(str(duckdb_path))
    try:
        transformed_rows = connection.execute(
            """
            select ts_code, list_date, source_run_id, raw_loaded_at, is_active
            from stg_stock_basic
            order by ts_code
            """
        ).fetchall()
        type_row = connection.execute(
            "select typeof(ts_code), typeof(list_date) from stg_stock_basic limit 1"
        ).fetchone()
    finally:
        connection.close()

    assert len(transformed_rows) == 3
    assert transformed_rows[0][0] == "000001.SZ"
    assert transformed_rows[0][1] == date(1991, 4, 3)
    assert transformed_rows[0][2] == "sample"
    assert transformed_rows[0][3] is not None
    assert transformed_rows[2][4] is False
    assert type_row == ("VARCHAR", "DATE")


def _write_duckdb_only_profile(path: Path) -> None:
    path.write_text(
        """
data_platform:
  target: test
  outputs:
    test:
      type: duckdb
      path: "{{ env_var('DP_DUCKDB_PATH') }}"
      threads: 1
""".lstrip()
    )


def _render_stg_stock_basic_sql_for_raw_path(raw_zone_path: Path) -> str:
    model_sql = (
        DBT_PROJECT_DIR / "models" / "staging" / "stg_stock_basic.sql"
    ).read_text()
    raw_glob = raw_zone_path / "tushare" / "stock_basic" / "**" / "*.parquet"
    return (
        model_sql.replace('{{ config(materialized="view") }}', "")
        .replace("'{{ dp_raw_path(\"tushare\", \"stock_basic\") }}'", f"'{raw_glob}'")
        .strip()
    )


def _run_dbt_wrapper(args: list[str], *, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(PROJECT_ROOT / "scripts" / "dbt.sh"), *args],
        cwd=PROJECT_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
