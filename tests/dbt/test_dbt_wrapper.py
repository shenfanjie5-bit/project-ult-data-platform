from __future__ import annotations

import os
from pathlib import Path
import subprocess


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_dbt_wrapper_defaults_test_indirect_selection_to_cautious(tmp_path: Path) -> None:
    argv_file = tmp_path / "argv.txt"
    env = _fake_dbt_env(tmp_path, argv_file)

    result = subprocess.run(
        [str(PROJECT_ROOT / "scripts" / "dbt.sh"), "test", "--select", "staging"],
        cwd=PROJECT_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert argv_file.read_text(encoding="utf-8").splitlines() == [
        "test",
        "--indirect-selection",
        "cautious",
        "--select",
        "staging",
    ]


def test_dbt_wrapper_keeps_explicit_indirect_selection_override(tmp_path: Path) -> None:
    argv_file = tmp_path / "argv.txt"
    env = _fake_dbt_env(tmp_path, argv_file)

    result = subprocess.run(
        [
            str(PROJECT_ROOT / "scripts" / "dbt.sh"),
            "test",
            "--indirect-selection=eager",
            "--select",
            "staging",
        ],
        cwd=PROJECT_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert argv_file.read_text(encoding="utf-8").splitlines() == [
        "test",
        "--indirect-selection=eager",
        "--select",
        "staging",
    ]


def _fake_dbt_env(tmp_path: Path, argv_file: Path) -> dict[str, str]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    fake_dbt = bin_dir / "dbt"
    fake_dbt.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$@" > "${DBT_ARGV_FILE}"
""",
        encoding="utf-8",
    )
    fake_dbt.chmod(0o755)

    env = os.environ.copy()
    env["DBT_ARGV_FILE"] = str(argv_file)
    env["PATH"] = f"{bin_dir}{os.pathsep}{env['PATH']}"
    return env
