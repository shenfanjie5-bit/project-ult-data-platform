from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DBT_PROJECT_DIR = PROJECT_ROOT / "src" / "data_platform" / "dbt"
DBT_FIXTURE_RAW = PROJECT_ROOT / "tests" / "dbt" / "fixtures" / "raw"
DBT_FIXTURE_RUN_ID = "123e4567-e89b-42d3-a456-426614174000"
DBT_LATER_RUN_ID = "223e4567-e89b-42d3-a456-426614174000"
DBT_LEGACY_RUN_ID = "323e4567-e89b-42d3-a456-426614174000"


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
        DBT_FIXTURE_RAW / "tushare" / "stock_basic" / "dt=20260415" / (
            f"{DBT_FIXTURE_RUN_ID}.parquet"
        ),
        DBT_FIXTURE_RAW / "tushare" / "stock_basic" / "dt=20260415" / "_manifest.json",
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
    assert '.rstrip("/")' in macro_sql
    assert '"/" ~ source_id ~ "/" ~ dataset ~ "/**/*.parquet"' in macro_sql
    assert '"/" ~ source_id ~ "/" ~ dataset ~ "/**/_manifest.json"' in macro_sql

    assert '{{ config(materialized="view") }}' in model_sql
    assert "read_parquet" in model_sql
    assert "read_json_auto" in model_sql
    assert '{{ dp_raw_path("tushare", "stock_basic") }}' in model_sql
    assert '{{ dp_raw_manifest_path("tushare", "stock_basic") }}' in model_sql
    assert "hive_partitioning=1" in model_sql
    assert "filename=1" in model_sql
    assert "union_by_name=1" in model_sql
    assert "row_number() over" in model_sql
    assert "order by raw_loaded_at desc, partition_date desc, source_run_id desc" in (
        model_sql
    )
    assert "select *" not in lowered_model_sql
    assert "trim(cast(ts_code as varchar))" in model_sql
    assert "strptime(nullif(trim(cast(list_date as varchar)), ''), '%Y%m%d')::date" in (
        model_sql
    )
    assert "source_run_id" in model_sql
    assert "raw_loaded_at" in model_sql
    assert "current_timestamp" not in lowered_model_sql
    assert "canonical." not in lowered_model_sql
    assert "iceberg_scan" not in lowered_model_sql


def test_dp_raw_path_macros_strip_trailing_slash() -> None:
    jinja2 = pytest.importorskip("jinja2")

    environment = jinja2.Environment()
    environment.globals["env_var"] = lambda _name, _default=None: "/tmp/dp_raw/"
    template = environment.from_string(
        (DBT_PROJECT_DIR / "macros" / "dp_raw_path.sql").read_text()
    )
    module = template.make_module({})

    assert module.dp_raw_path("tushare", "stock_basic") == (
        "/tmp/dp_raw/tushare/stock_basic/**/*.parquet"
    )
    assert module.dp_raw_manifest_path("tushare", "stock_basic") == (
        "/tmp/dp_raw/tushare/stock_basic/**/_manifest.json"
    )


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
    assert transformed_rows[0][2] == DBT_FIXTURE_RUN_ID
    assert transformed_rows[0][3] == datetime(2026, 4, 15, 1, 0)
    assert transformed_rows[2][4] is False
    assert type_row == ("VARCHAR", "DATE")


def test_stg_stock_basic_sql_transforms_rawwriter_fixture_with_duckdb(
    tmp_path: Path,
) -> None:
    duckdb = pytest.importorskip("duckdb")

    raw_zone_path = tmp_path / "raw"
    _write_rawwriter_stock_basic_fixture(raw_zone_path, tmp_path)
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
    finally:
        connection.close()

    assert len(transformed_rows) == 3
    assert transformed_rows[0][0] == "000001.SZ"
    assert transformed_rows[0][1] == date(1991, 4, 3)
    assert transformed_rows[0][2] == DBT_FIXTURE_RUN_ID
    assert transformed_rows[2][4] is False


def test_stg_stock_basic_selects_latest_manifest_artifact_with_duplicate_keys(
    tmp_path: Path,
) -> None:
    duckdb = pytest.importorskip("duckdb")

    raw_zone_path = tmp_path / "raw"
    shutil.copytree(DBT_FIXTURE_RAW, raw_zone_path)
    partition_path = raw_zone_path / "tushare" / "stock_basic" / "dt=20260415"
    shutil.copyfile(
        partition_path / f"{DBT_FIXTURE_RUN_ID}.parquet",
        partition_path / f"{DBT_LATER_RUN_ID}.parquet",
    )
    _write_stock_basic_manifest(
        partition_path / "_manifest.json",
        [
            (DBT_FIXTURE_RUN_ID, "2026-04-15T01:00:00+00:00"),
            (DBT_LATER_RUN_ID, "2026-04-15T02:00:00+00:00"),
        ],
    )

    model_sql = _render_stg_stock_basic_sql_for_raw_path(raw_zone_path)
    connection = duckdb.connect(":memory:")
    try:
        connection.execute(f"create view stg_stock_basic as {model_sql}")
        transformed_rows = connection.execute(
            """
            select ts_code, source_run_id, raw_loaded_at
            from stg_stock_basic
            order by ts_code
            """
        ).fetchall()
        duplicate_count = connection.execute(
            """
            select count(*)
            from (
                select ts_code
                from stg_stock_basic
                group by ts_code
                having count(*) > 1
            )
            """
        ).fetchone()[0]
    finally:
        connection.close()

    assert len(transformed_rows) == 3
    assert {row[0] for row in transformed_rows} == {"000001.SZ", "000002.SZ", "000003.BJ"}
    assert {row[1] for row in transformed_rows} == {DBT_LATER_RUN_ID}
    assert {row[2] for row in transformed_rows} == {datetime(2026, 4, 15, 2, 0)}
    assert duplicate_count == 0


def test_stg_stock_basic_tolerates_schema_drifted_historical_artifact(
    tmp_path: Path,
) -> None:
    duckdb = pytest.importorskip("duckdb")

    raw_zone_path = tmp_path / "raw"
    shutil.copytree(DBT_FIXTURE_RAW, raw_zone_path)
    partition_path = raw_zone_path / "tushare" / "stock_basic" / "dt=20260415"
    legacy_path = partition_path / f"{DBT_LEGACY_RUN_ID}.parquet"

    connection = duckdb.connect(":memory:")
    try:
        connection.execute(
            f"""
            copy (
                select
                    '000001.SZ'::varchar as ts_code,
                    19910403::integer as list_date
            )
            to '{legacy_path}' (format parquet)
            """
        )
    finally:
        connection.close()

    _write_stock_basic_manifest(
        partition_path / "_manifest.json",
        [
            (DBT_LEGACY_RUN_ID, "2026-04-15T00:00:00+00:00"),
            (DBT_FIXTURE_RUN_ID, "2026-04-15T01:00:00+00:00"),
        ],
    )

    model_sql = _render_stg_stock_basic_sql_for_raw_path(raw_zone_path)
    connection = duckdb.connect(":memory:")
    try:
        connection.execute(f"create view stg_stock_basic as {model_sql}")
        transformed_rows = connection.execute(
            """
            select ts_code, source_run_id, raw_loaded_at
            from stg_stock_basic
            order by ts_code
            """
        ).fetchall()
        duplicate_count = connection.execute(
            """
            select count(*)
            from (
                select ts_code
                from stg_stock_basic
                group by ts_code
                having count(*) > 1
            )
            """
        ).fetchone()[0]
    finally:
        connection.close()

    assert len(transformed_rows) == 3
    assert {row[0] for row in transformed_rows} == {"000001.SZ", "000002.SZ", "000003.BJ"}
    assert {row[1] for row in transformed_rows} == {DBT_FIXTURE_RUN_ID}
    assert {row[2] for row in transformed_rows} == {datetime(2026, 4, 15, 1, 0)}
    assert duplicate_count == 0


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
        if _is_python_314_mashumaro_startup_failure(version_output):
            pytest.skip(
                "dbt runtime is installed, but this sandbox is running Python 3.14 "
                "and the installed dbt dependency stack crashes in mashumaro during "
                f"startup:\n{version_output}"
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
    assert transformed_rows[0][2] == DBT_FIXTURE_RUN_ID
    assert transformed_rows[0][3] == datetime(2026, 4, 15, 1, 0)
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


def _write_rawwriter_stock_basic_fixture(raw_zone_path: Path, tmp_path: Path) -> None:
    pa = pytest.importorskip("pyarrow")

    from data_platform.raw import RawWriter

    RawWriter(
        raw_zone_path=raw_zone_path,
        iceberg_warehouse_path=tmp_path / "iceberg" / "warehouse",
    ).write_arrow(
        "tushare",
        "stock_basic",
        date(2026, 4, 15),
        DBT_FIXTURE_RUN_ID,
        pa.table(
            {
                "ts_code": ["000001.SZ", "000002.SZ", "000003.BJ"],
                "symbol": ["000001", "000002", "000003"],
                "name": ["Ping An Bank", "Vanke A", "Test BJ"],
                "area": ["Shenzhen", "Shenzhen", ""],
                "industry": ["Bank", "Real Estate", ""],
                "market": ["Main", "Main", "BJ"],
                "list_status": ["L", "L", "D"],
                "list_date": [19910403, 19910129, 20200101],
            }
        ),
    )


def _render_stg_stock_basic_sql_for_raw_path(raw_zone_path: Path) -> str:
    model_sql = (
        DBT_PROJECT_DIR / "models" / "staging" / "stg_stock_basic.sql"
    ).read_text()
    raw_glob = raw_zone_path / "tushare" / "stock_basic" / "**" / "*.parquet"
    manifest_glob = raw_zone_path / "tushare" / "stock_basic" / "**" / "_manifest.json"
    return (
        model_sql.replace('{{ config(materialized="view") }}', "")
        .replace("'{{ dp_raw_path(\"tushare\", \"stock_basic\") }}'", f"'{raw_glob}'")
        .replace(
            "'{{ dp_raw_manifest_path(\"tushare\", \"stock_basic\") }}'",
            f"'{manifest_glob}'",
        )
        .strip()
    )


def _write_stock_basic_manifest(
    path: Path,
    artifacts: list[tuple[str, str]],
) -> None:
    path.write_text(
        json.dumps(
            {
                "source_id": "tushare",
                "dataset": "stock_basic",
                "partition_date": "2026-04-15",
                "artifacts": [
                    {
                        "source_id": "tushare",
                        "dataset": "stock_basic",
                        "partition_date": "2026-04-15",
                        "run_id": run_id,
                        "path": f"tushare/stock_basic/dt=20260415/{run_id}.parquet",
                        "row_count": 3,
                        "written_at": written_at,
                    }
                    for run_id, written_at in artifacts
                ],
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def _is_python_314_mashumaro_startup_failure(version_output: str) -> bool:
    return (
        sys.version_info >= (3, 14)
        and "mashumaro.exceptions.UnserializableField" in version_output
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
