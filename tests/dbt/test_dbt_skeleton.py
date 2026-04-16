from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

from data_platform.adapters.tushare import TUSHARE_ASSETS


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DBT_PROJECT_DIR = PROJECT_ROOT / "src" / "data_platform" / "dbt"
STAGING_DIR = DBT_PROJECT_DIR / "models" / "staging"
INTERMEDIATE_DIR = DBT_PROJECT_DIR / "models" / "intermediate"
INTERMEDIATE_MODEL_NAMES = [
    "int_event_timeline",
    "int_financial_reports_latest",
    "int_index_membership",
    "int_price_bars_adjusted",
    "int_security_master",
]
MART_MODEL_NAMES = [
    "mart_dim_index",
    "mart_dim_security",
    "mart_fact_event",
    "mart_fact_financial_indicator",
    "mart_fact_price_bar",
]


def test_dbt_skeleton_files_are_present() -> None:
    required_paths = [
        DBT_PROJECT_DIR / "dbt_project.yml",
        DBT_PROJECT_DIR / "profiles.yml.example",
        DBT_PROJECT_DIR / "macros" / "dp_raw_path.sql",
        DBT_PROJECT_DIR / "macros" / "stg_latest_raw.sql",
        DBT_PROJECT_DIR / "macros" / "staging_tests.sql",
        DBT_PROJECT_DIR / "macros" / "intermediate_tests.sql",
        STAGING_DIR / "_sources.yml",
        STAGING_DIR / "_schema.yml",
        INTERMEDIATE_DIR / "_schema.yml",
        DBT_PROJECT_DIR / "models" / "intermediate" / ".gitkeep",
        DBT_PROJECT_DIR / "models" / "marts" / ".gitkeep",
        DBT_PROJECT_DIR / "models" / "marts" / "_schema.yml",
        DBT_PROJECT_DIR / "seeds" / ".gitkeep",
        PROJECT_ROOT / "scripts" / "dbt.sh",
        *[STAGING_DIR / f"stg_{asset.dataset}.sql" for asset in TUSHARE_ASSETS],
        *[INTERMEDIATE_DIR / f"{model_name}.sql" for model_name in INTERMEDIATE_MODEL_NAMES],
        *[
            DBT_PROJECT_DIR / "models" / "marts" / f"{model_name}.sql"
            for model_name in MART_MODEL_NAMES
        ],
    ]

    missing_paths = [path for path in required_paths if not path.exists()]
    sql_models = sorted(
        path.relative_to(DBT_PROJECT_DIR).as_posix()
        for path in (DBT_PROJECT_DIR / "models").glob("**/*.sql")
    )

    assert missing_paths == []
    assert os.access(PROJECT_ROOT / "scripts" / "dbt.sh", os.X_OK)
    assert sql_models == sorted(
        [
            *(f"models/staging/stg_{asset.dataset}.sql" for asset in TUSHARE_ASSETS),
            *(f"models/intermediate/{model_name}.sql" for model_name in INTERMEDIATE_MODEL_NAMES),
            *(f"models/marts/{model_name}.sql" for model_name in MART_MODEL_NAMES),
        ]
    )

    dbt_project = (DBT_PROJECT_DIR / "dbt_project.yml").read_text()
    assert "name: data_platform" in dbt_project
    assert "profile: data_platform" in dbt_project
    # Accept both legacy and dbt 1.11+ semver array format
    assert "require-dbt-version:" in dbt_project
    assert 'test-paths: ["tests"]' in dbt_project
    assert "+materialized: view" in dbt_project
    assert "+materialized: table" in dbt_project

    profile = (DBT_PROJECT_DIR / "profiles.yml.example").read_text()
    assert "DP_DUCKDB_PATH" in profile
    assert "DP_PG_DSN" in profile
    assert 'extensions: ["iceberg", "httpfs"]' in profile
    assert "type: postgres" in profile


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


def test_stg_stock_basic_preserves_p1a_contract() -> None:
    model_sql = (STAGING_DIR / "stg_stock_basic.sql").read_text()
    macro_sql = (DBT_PROJECT_DIR / "macros" / "stg_latest_raw.sql").read_text()
    lowered_model_sql = model_sql.lower()

    assert '{{ config(materialized="view") }}' in model_sql
    assert "{{ stg_latest_raw(" in model_sql
    assert '"stock_basic"' in model_sql
    assert "strptime(nullif(trim(cast(\\\"list_date\\\" as varchar)), ''), '%Y%m%d')::date" in (
        model_sql
    )
    assert "as \\\"is_active\\\"" in model_sql
    assert "source_run_id" in macro_sql
    assert "raw_loaded_at" in macro_sql
    assert "select *" not in lowered_model_sql
    assert "canonical." not in lowered_model_sql
    assert "formal." not in lowered_model_sql
    assert "iceberg_scan" not in lowered_model_sql


def test_dbt_wrapper_does_not_mask_relationship_test_selection(tmp_path: Path) -> None:
    assert _run_dbt_wrapper_with_fake_dbt(
        tmp_path, ["test", "--select", "intermediate"]
    ) == [
        "test",
        "--select",
        "intermediate",
    ]


def test_dbt_wrapper_preserves_explicit_test_selection_args(tmp_path: Path) -> None:
    assert _run_dbt_wrapper_with_fake_dbt(
        tmp_path,
        [
            "test",
            "--indirect-selection",
            "eager",
            "--select",
            "intermediate",
        ],
    ) == [
        "test",
        "--indirect-selection",
        "eager",
        "--select",
        "intermediate",
    ]


def _run_dbt_wrapper_with_fake_dbt(tmp_path: Path, args: list[str]) -> list[str]:
    fake_bin_dir = tmp_path / "bin"
    fake_bin_dir.mkdir()
    args_capture = tmp_path / "dbt_args.txt"
    fake_dbt = fake_bin_dir / "dbt"
    fake_dbt.write_text(
        """
#!/usr/bin/env bash
printf '%s\n' "$@" > "${DBT_ARG_CAPTURE}"
""".lstrip(),
        encoding="utf-8",
    )
    fake_dbt.chmod(0o755)

    env = os.environ.copy()
    env["DBT_ARG_CAPTURE"] = str(args_capture)
    env["PATH"] = f"{fake_bin_dir}{os.pathsep}{env['PATH']}"

    result = subprocess.run(
        [str(PROJECT_ROOT / "scripts" / "dbt.sh"), *args],
        cwd=PROJECT_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    return args_capture.read_text(encoding="utf-8").splitlines()
