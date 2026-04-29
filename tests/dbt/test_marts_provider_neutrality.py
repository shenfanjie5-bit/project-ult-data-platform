"""dbt mart SQL parity tests for provider-neutrality.

These tests scan the dbt marts `.sql` files and assert that the M1 target
end-state holds: no canonical mart SELECT exposes `source_run_id` /
`raw_loaded_at` (those move to `mart_lineage_*.sql` files), and no canonical
mart SELECT directly forwards `ts_code` / `index_code` (those rename via
`alias AS canonical_id`).

Today RED for all 8 marts under `dbt/models/marts/`. Flips to GREEN as M1-D
adds parallel `dbt/models/marts_v2/*.sql` files and migrates the canonical
write path to consume them. Until M1-D step 5 retires the legacy marts, this
file documents the gap.
"""

from __future__ import annotations

import os
from pathlib import Path
import re
from typing import Final, Iterable

import pytest


_M1D_LEGACY_RETIREMENT_XFAIL = pytest.mark.xfail(
    reason=(
        "M1-D step 5 legacy mart retirement is not complete; run with "
        "DP_ENFORCE_M1D_PROVIDER_NEUTRALITY=1 or pytest --runxfail to enforce."
    ),
    condition=os.environ.get("DP_ENFORCE_M1D_PROVIDER_NEUTRALITY") != "1",
    strict=False,
)

PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parents[2]
LEGACY_MARTS_DIR: Final[Path] = (
    PROJECT_ROOT / "src" / "data_platform" / "dbt" / "models" / "marts"
)
V2_MARTS_DIR: Final[Path] = (
    PROJECT_ROOT / "src" / "data_platform" / "dbt" / "models" / "marts_v2"
)
LINEAGE_MARTS_DIR: Final[Path] = (
    PROJECT_ROOT / "src" / "data_platform" / "dbt" / "models" / "marts_lineage"
)

FORBIDDEN_LINEAGE_TOKENS: Final[tuple[re.Pattern[str], ...]] = (
    re.compile(r"(?<![A-Za-z0-9_])source_run_id(?![A-Za-z0-9_])"),
    re.compile(r"(?<![A-Za-z0-9_])raw_loaded_at(?![A-Za-z0-9_])"),
)
FORBIDDEN_PROVIDER_SHAPED_TOKENS: Final[tuple[re.Pattern[str], ...]] = (
    re.compile(r"(?<![A-Za-z0-9_])ts_code(?![A-Za-z0-9_])"),
    re.compile(r"(?<![A-Za-z0-9_])index_code(?![A-Za-z0-9_])"),
)


def _legacy_mart_sql_files() -> Iterable[Path]:
    if not LEGACY_MARTS_DIR.is_dir():
        return ()
    return sorted(p for p in LEGACY_MARTS_DIR.glob("*.sql") if p.is_file())


def _v2_mart_sql_files() -> Iterable[Path]:
    if not V2_MARTS_DIR.is_dir():
        return ()
    return sorted(p for p in V2_MARTS_DIR.glob("*.sql") if p.is_file())


def _lineage_mart_sql_files() -> Iterable[Path]:
    if not LINEAGE_MARTS_DIR.is_dir():
        return ()
    return sorted(p for p in LINEAGE_MARTS_DIR.glob("*.sql") if p.is_file())


@pytest.mark.parametrize(
    "mart_sql",
    list(_legacy_mart_sql_files()),
    ids=lambda p: p.name,
)
@_M1D_LEGACY_RETIREMENT_XFAIL
def test_canonical_mart_sql_does_not_select_lineage_columns(
    mart_sql: Path,
) -> None:
    """Canonical mart SQL must not surface raw-zone lineage columns.

    Today RED for all 8 legacy marts. Flips to GREEN once M1-D moves the
    canonical write path off these files (step 5 retires the legacy marts).
    """

    body = mart_sql.read_text(encoding="utf-8")
    leaks = sorted(
        match.group(0)
        for pattern in FORBIDDEN_LINEAGE_TOKENS
        for match in pattern.finditer(body)
    )
    assert not leaks, (
        f"{mart_sql.relative_to(PROJECT_ROOT)} surfaces raw-zone lineage "
        f"{sorted(set(leaks))}; lineage must move to dbt/models/marts_lineage/*.sql"
    )


@pytest.mark.parametrize(
    "mart_sql",
    list(_v2_mart_sql_files()),
    ids=lambda p: p.name,
)
def test_canonical_v2_mart_sql_does_not_select_lineage_columns(
    mart_sql: Path,
) -> None:
    """Canonical_v2 mart SQL must be lineage-free at the SELECT.

    Vacuous PASS today (no v2 marts); becomes binding as M1-D adds them.
    """

    body = mart_sql.read_text(encoding="utf-8")
    leaks = sorted(
        match.group(0)
        for pattern in FORBIDDEN_LINEAGE_TOKENS
        for match in pattern.finditer(body)
    )
    assert not leaks, (
        f"{mart_sql.relative_to(PROJECT_ROOT)} (canonical_v2) surfaces lineage "
        f"{sorted(set(leaks))}"
    )


@pytest.mark.parametrize(
    "mart_sql",
    list(_v2_mart_sql_files()),
    ids=lambda p: p.name,
)
def test_canonical_v2_mart_sql_does_not_select_provider_shaped_identifier(
    mart_sql: Path,
) -> None:
    """Canonical_v2 mart SELECT must not forward provider-shaped identifiers.

    Acceptable inside the SQL is `... ts_code AS security_id ...` (alias). The
    test scans for bare `ts_code` references not followed by ` AS ` within a
    short window, to avoid false-positives on aliases.
    """

    body = mart_sql.read_text(encoding="utf-8")
    if not body.strip():
        pytest.skip(f"{mart_sql.name} is empty")

    bare_provider_shaped: list[str] = []
    for pattern in FORBIDDEN_PROVIDER_SHAPED_TOKENS:
        for match in pattern.finditer(body):
            start = match.end()
            window = body[start : start + 8].lower().lstrip()
            if window.startswith("as "):
                continue
            bare_provider_shaped.append(match.group(0))
    assert not bare_provider_shaped, (
        f"{mart_sql.relative_to(PROJECT_ROOT)} (canonical_v2) forwards bare "
        f"provider-shaped identifier {sorted(set(bare_provider_shaped))}; rename via "
        "`<provider> AS <canonical_id>` is required"
    )


@pytest.mark.parametrize(
    "mart_sql",
    list(_lineage_mart_sql_files()),
    ids=lambda p: p.name,
)
def test_lineage_mart_sql_carries_lineage_columns(
    mart_sql: Path,
) -> None:
    """canonical_lineage marts must carry source_run_id + raw_loaded_at SELECT.

    Vacuous PASS today (no lineage marts); becomes binding as M1-D adds them.
    """

    body = mart_sql.read_text(encoding="utf-8").lower()
    if not body.strip():
        pytest.skip(f"{mart_sql.name} is empty")

    assert "source_run_id" in body, (
        f"{mart_sql.relative_to(PROJECT_ROOT)} (lineage mart) must SELECT source_run_id"
    )
    assert "raw_loaded_at" in body, (
        f"{mart_sql.relative_to(PROJECT_ROOT)} (lineage mart) must SELECT raw_loaded_at"
    )


def test_dim_security_lineage_mart_discloses_composite_source_interface() -> None:
    """dim_security lineage must not claim all contributed fields came from stock_basic."""

    mart_sql = LINEAGE_MARTS_DIR / "mart_lineage_dim_security.sql"
    body = mart_sql.read_text(encoding="utf-8").lower()

    assert "concat_ws(" in body
    assert "stock_basic" in body
    assert "stock_company" in body
    assert "namechange" in body
    assert "stock_basic_source_run_id" in body
    assert "stock_company_source_run_id" in body
    assert "namechange_source_run_id" in body
    assert "cast('stock_basic' as varchar) as source_interface_id" not in body


def test_legacy_marts_directory_exists_and_is_inventoried() -> None:
    """Sentinel — confirms this test file looks at the right location.

    If `marts/` ever moves, this fails first and the parametrization above
    reports zero collected items, masking the real signal.
    """

    assert LEGACY_MARTS_DIR.is_dir(), (
        f"legacy marts directory missing: {LEGACY_MARTS_DIR.relative_to(PROJECT_ROOT)}"
    )
    legacy_marts = list(_legacy_mart_sql_files())
    assert len(legacy_marts) >= 1, (
        f"legacy marts directory contains no .sql files: {LEGACY_MARTS_DIR.relative_to(PROJECT_ROOT)}"
    )
