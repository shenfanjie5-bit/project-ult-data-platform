#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class RepositoryArtifactCheck:
    tracked_paths: tuple[str, ...]
    generated_artifacts: tuple[str, ...]
    has_platform_source: bool
    has_tests: bool

    @property
    def ok(self) -> bool:
        return not self.generated_artifacts and self.has_platform_source and self.has_tests

    def error_messages(self) -> list[str]:
        messages: list[str] = []
        if self.generated_artifacts:
            messages.append("generated Python artifacts are tracked:")
            messages.extend(f"  - {path}" for path in self.generated_artifacts)
        if not self.has_platform_source:
            messages.append("no tracked Python source files found under src/data_platform/")
        if not self.has_tests:
            messages.append("no tracked Python test files found under tests/")
        return messages


def git_ls_files(repo_root: Path) -> tuple[str, ...]:
    result = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=repo_root,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if result.returncode != 0:
        stderr = result.stderr.decode(errors="replace").strip()
        raise RuntimeError(f"git ls-files failed: {stderr}")

    return tuple(
        path
        for path in result.stdout.decode(errors="replace").split("\0")
        if path
    )


def check_tracked_paths(paths: Sequence[str]) -> RepositoryArtifactCheck:
    normalized_paths = tuple(_normalize_path(path) for path in paths)
    generated_artifacts = tuple(
        path for path in normalized_paths if _is_generated_python_artifact(path)
    )

    return RepositoryArtifactCheck(
        tracked_paths=normalized_paths,
        generated_artifacts=generated_artifacts,
        has_platform_source=any(_is_platform_source(path) for path in normalized_paths),
        has_tests=any(_is_test_source(path) for path in normalized_paths),
    )


def _normalize_path(path: str) -> str:
    return path.replace("\\", "/").strip("/")


def _is_generated_python_artifact(path: str) -> bool:
    parts = path.split("/")
    return (
        path.endswith((".pyc", ".pyo"))
        or "__pycache__" in parts
        or any(part.endswith(".egg-info") for part in parts)
    )


def _is_platform_source(path: str) -> bool:
    return path.startswith("src/data_platform/") and path.endswith(".py")


def _is_test_source(path: str) -> bool:
    return path.startswith("tests/") and path.endswith(".py")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Fail if generated Python artifacts are tracked or if required "
            "source/test files are missing from the git index."
        )
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help="repository root to inspect; defaults to this script's parent checkout",
    )
    args = parser.parse_args(argv)

    try:
        tracked_paths = git_ls_files(args.repo_root)
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    check = check_tracked_paths(tracked_paths)
    if check.ok:
        print(
            "Repository artifact guard passed: tracked source and tests are present, "
            "and no generated Python artifacts are tracked."
        )
        return 0

    print("Repository artifact guard failed:", file=sys.stderr)
    for message in check.error_messages():
        print(message, file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
