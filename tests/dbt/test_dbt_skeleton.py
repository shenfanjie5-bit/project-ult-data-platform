from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DBT_PROJECT_DIR = PROJECT_ROOT / "src" / "data_platform" / "dbt"


def test_dbt_skeleton_files_are_present() -> None:
    required_paths = [
        DBT_PROJECT_DIR / "dbt_project.yml",
        DBT_PROJECT_DIR / "profiles.yml.example",
        DBT_PROJECT_DIR / "models" / "staging" / "_sources.yml",
        DBT_PROJECT_DIR / "models" / "intermediate" / ".gitkeep",
        DBT_PROJECT_DIR / "models" / "marts" / ".gitkeep",
        DBT_PROJECT_DIR / "seeds" / ".gitkeep",
        DBT_PROJECT_DIR / "macros" / ".gitkeep",
        PROJECT_ROOT / "scripts" / "dbt.sh",
    ]

    missing_paths = [path for path in required_paths if not path.exists()]

    assert missing_paths == []
    assert os.access(PROJECT_ROOT / "scripts" / "dbt.sh", os.X_OK)
    assert list((DBT_PROJECT_DIR / "models").glob("**/*.sql")) == []

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


def resolve_dbt_executable() -> str | None:
    dbt = shutil.which("dbt")
    if dbt is not None:
        return dbt

    local_dbt = PROJECT_ROOT / ".venv" / "bin" / "dbt"
    if local_dbt.exists():
        return str(local_dbt)

    return None


def test_dbt_parse_succeeds_with_skeleton_profile(tmp_path: Path) -> None:
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
        if "mashumaro.exceptions.UnserializableField" in version_output:
            pytest.skip("dbt cannot start in this Python environment; use the project Python 3.12 venv")
        pytest.fail(f"dbt --version failed:\n{version_output}")

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
