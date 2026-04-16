"""Optional helpers for loading the sibling contracts package in a workspace checkout."""

from __future__ import annotations

from functools import lru_cache
from importlib import import_module
from pathlib import Path
import sys
from types import ModuleType


@lru_cache(maxsize=1)
def _contracts_search_paths() -> tuple[Path, ...]:
    workspace_root = Path(__file__).resolve().parents[3]
    candidates = (
        workspace_root / "contracts" / "build" / "lib",
        workspace_root / "contracts" / "src",
    )
    return tuple(path for path in candidates if path.exists())


def _module_name_matches_contracts(exc: ModuleNotFoundError, module_name: str) -> bool:
    requested_root = module_name.split(".", maxsplit=1)[0]
    missing_name = exc.name or ""
    return missing_name == requested_root or missing_name.startswith(f"{requested_root}.")


@lru_cache(maxsize=None)
def load_contracts_module(module_name: str) -> ModuleType | None:
    """Import one contracts module, falling back to the sibling workspace when present."""

    try:
        return import_module(module_name)
    except ModuleNotFoundError as exc:
        if not _module_name_matches_contracts(exc, module_name):
            raise

    for search_path in _contracts_search_paths():
        path_value = str(search_path)
        if path_value not in sys.path:
            sys.path.append(path_value)
        try:
            return import_module(module_name)
        except ModuleNotFoundError as exc:
            if not _module_name_matches_contracts(exc, module_name):
                raise

    return None


__all__ = ["load_contracts_module"]
