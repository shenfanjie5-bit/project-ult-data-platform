from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
GUARD_SCRIPT = PROJECT_ROOT / "scripts" / "check_repository_artifacts.py"


def _load_guard_module() -> object:
    spec = importlib.util.spec_from_file_location(
        "check_repository_artifacts", GUARD_SCRIPT
    )
    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_guard_accepts_tracked_source_and_tests_without_generated_artifacts() -> None:
    guard = _load_guard_module()

    check = guard.check_tracked_paths(
        [
            "src/data_platform/__init__.py",
            "src/data_platform/dbt/models/staging/stg_stock_basic.sql",
            "tests/test_smoke.py",
        ]
    )

    assert check.ok
    assert check.generated_artifacts == ()
    assert check.error_messages() == []


def test_guard_rejects_generated_artifacts_and_missing_sources() -> None:
    guard = _load_guard_module()

    check = guard.check_tracked_paths(
        [
            "src/data_platform/adapters/__pycache__/adapter.cpython-314.pyc",
            "src/project_ult_data_platform.egg-info/PKG-INFO",
            "README.md",
        ]
    )

    assert not check.ok
    assert check.generated_artifacts == (
        "src/data_platform/adapters/__pycache__/adapter.cpython-314.pyc",
        "src/project_ult_data_platform.egg-info/PKG-INFO",
    )
    assert check.error_messages() == [
        "generated Python artifacts are tracked:",
        "  - src/data_platform/adapters/__pycache__/adapter.cpython-314.pyc",
        "  - src/project_ult_data_platform.egg-info/PKG-INFO",
        "no tracked Python source files found under src/data_platform/",
        "no tracked Python test files found under tests/",
    ]


def test_git_index_has_source_tests_and_no_generated_artifacts() -> None:
    guard = _load_guard_module()

    check = guard.check_tracked_paths(guard.git_ls_files(PROJECT_ROOT))

    assert check.ok, "\n".join(check.error_messages())
