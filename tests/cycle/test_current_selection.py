from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import pyarrow as pa
import pytest

from data_platform.cycle.current_selection import (
    CURRENT_CYCLE_SYMBOLS_ENV,
    DEFAULT_CURRENT_CYCLE_SYMBOLS,
    CurrentCycleReadinessProvider,
    CurrentCycleSelectionError,
    freeze_current_cycle_candidates,
    select_current_cycle,
)
from data_platform.cycle.models import CycleMetadata
from data_platform.raw import RawWriter


def test_selector_selects_latest_open_trade_day_and_records_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(CURRENT_CYCLE_SYMBOLS_ENV, raising=False)
    raw_zone_path = tmp_path / "raw"
    writer = _writer(raw_zone_path, tmp_path)
    _write_trade_cal(writer, date(2026, 4, 15), is_open="1")
    _write_trade_cal(writer, date(2026, 4, 16), is_open="1")
    _write_trade_cal(writer, date(2026, 4, 17), is_open="0")
    _write_daily(writer, date(2026, 4, 16), DEFAULT_CURRENT_CYCLE_SYMBOLS)
    _write_stock_basic(writer, date(2026, 4, 1), DEFAULT_CURRENT_CYCLE_SYMBOLS)

    selection = select_current_cycle(raw_zone_path=raw_zone_path)

    assert selection.trade_date == date(2026, 4, 16)
    assert selection.cycle_id == "CYCLE_20260416"
    assert selection.symbols == DEFAULT_CURRENT_CYCLE_SYMBOLS
    assert selection.evidence["symbols"] == list(DEFAULT_CURRENT_CYCLE_SYMBOLS)
    assert selection.evidence["input_tables"] == [
        "main.stg_trade_cal",
        "main.stg_daily",
        "main.stg_stock_basic",
    ]
    artifact_refs = selection.evidence["input_artifact_refs"]
    assert isinstance(artifact_refs, dict)
    assert set(artifact_refs) == {"trade_cal", "daily", "stock_basic"}
    assert all(artifact_refs[dataset] for dataset in artifact_refs)


def test_selector_uses_env_symbol_override_and_records_actual_symbols(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_zone_path = tmp_path / "raw"
    override_symbols = ("000001.SZ",)
    monkeypatch.setenv(CURRENT_CYCLE_SYMBOLS_ENV, "000001.SZ")
    writer = _writer(raw_zone_path, tmp_path)
    _write_trade_cal(writer, date(2026, 4, 16), is_open="1")
    _write_daily(writer, date(2026, 4, 16), override_symbols)
    _write_stock_basic(writer, date(2026, 4, 1), override_symbols)

    selection = select_current_cycle(raw_zone_path=raw_zone_path)

    assert selection.symbols == override_symbols
    assert selection.evidence["symbols"] == ["000001.SZ"]


def test_selector_fails_closed_when_trade_cal_has_no_open_date(tmp_path: Path) -> None:
    raw_zone_path = tmp_path / "raw"
    writer = _writer(raw_zone_path, tmp_path)
    _write_trade_cal(writer, date(2026, 4, 16), is_open="0")
    _write_daily(writer, date(2026, 4, 16), DEFAULT_CURRENT_CYCLE_SYMBOLS)
    _write_stock_basic(writer, date(2026, 4, 1), DEFAULT_CURRENT_CYCLE_SYMBOLS)

    with pytest.raises(CurrentCycleSelectionError) as exc_info:
        select_current_cycle(raw_zone_path=raw_zone_path)

    assert exc_info.value.code == "no_open_trade_date"
    assert exc_info.value.cycle_id is None
    assert exc_info.value.evidence["symbols"] == list(DEFAULT_CURRENT_CYCLE_SYMBOLS)


def test_selector_fails_closed_when_symbol_data_is_missing(tmp_path: Path) -> None:
    raw_zone_path = tmp_path / "raw"
    writer = _writer(raw_zone_path, tmp_path)
    _write_trade_cal(writer, date(2026, 4, 16), is_open="1")
    _write_daily(writer, date(2026, 4, 16), ("600519.SH",))
    _write_stock_basic(writer, date(2026, 4, 1), DEFAULT_CURRENT_CYCLE_SYMBOLS)

    with pytest.raises(CurrentCycleSelectionError) as exc_info:
        select_current_cycle(raw_zone_path=raw_zone_path)

    assert exc_info.value.code == "missing_symbol_data"
    assert exc_info.value.cycle_id == "CYCLE_20260416"
    assert exc_info.value.evidence["missing_symbols"] == ["000001.SZ"]
    assert exc_info.value.evidence["symbols"] == list(DEFAULT_CURRENT_CYCLE_SYMBOLS)


def test_selector_fails_closed_when_input_artifact_ref_is_missing(tmp_path: Path) -> None:
    raw_zone_path = tmp_path / "raw"
    writer = _writer(raw_zone_path, tmp_path)
    _write_trade_cal(writer, date(2026, 4, 16), is_open="1")
    _write_daily(writer, date(2026, 4, 16), DEFAULT_CURRENT_CYCLE_SYMBOLS)

    with pytest.raises(CurrentCycleSelectionError) as exc_info:
        select_current_cycle(raw_zone_path=raw_zone_path)

    assert exc_info.value.code == "missing_input_artifact_refs"
    assert exc_info.value.cycle_id == "CYCLE_20260416"
    assert exc_info.value.evidence["missing_datasets"] == ["stock_basic"]
    assert exc_info.value.evidence["symbols"] == list(DEFAULT_CURRENT_CYCLE_SYMBOLS)


def test_freeze_wrapper_returns_selection_candidate_and_cutoff_evidence(
    tmp_path: Path,
) -> None:
    raw_zone_path = tmp_path / "raw"
    writer = _writer(raw_zone_path, tmp_path)
    _write_trade_cal(writer, date(2026, 4, 16), is_open="1")
    _write_daily(writer, date(2026, 4, 16), DEFAULT_CURRENT_CYCLE_SYMBOLS)
    _write_stock_basic(writer, date(2026, 4, 1), DEFAULT_CURRENT_CYCLE_SYMBOLS)
    called_cycle_ids: list[str] = []

    def fake_get_cycle(cycle_id: str) -> CycleMetadata:
        return _cycle_metadata(cycle_id, status="pending")

    def fake_freeze(cycle_id: str) -> CycleMetadata:
        called_cycle_ids.append(cycle_id)
        return _cycle_metadata(
            cycle_id,
            status="phase0",
            candidate_count=2,
            cutoff_ingest_seq=42,
            cutoff_submitted_at=datetime(2026, 4, 27, 9, 30, tzinfo=UTC),
            selection_frozen_at=datetime(2026, 4, 27, 9, 31, tzinfo=UTC),
        )

    result = freeze_current_cycle_candidates(
        raw_zone_path=raw_zone_path,
        get_cycle_fn=fake_get_cycle,
        freeze_fn=fake_freeze,
        candidate_id_loader=lambda cycle_id: (101, 102),
    )

    evidence = result.evidence
    assert called_cycle_ids == ["CYCLE_20260416"]
    assert evidence["selected_trade_date"] == "2026-04-16"
    assert evidence["cycle_id"] == "CYCLE_20260416"
    assert evidence["symbols"] == list(DEFAULT_CURRENT_CYCLE_SYMBOLS)
    assert evidence["frozen_candidate_ids"] == [101, 102]
    assert evidence["cutoff_metadata"] == {
        "status": "phase0",
        "cutoff_submitted_at": "2026-04-27T09:30:00+00:00",
        "cutoff_ingest_seq": 42,
        "candidate_count": 2,
        "selection_frozen_at": "2026-04-27T09:31:00+00:00",
        "cycle_created_at": "2026-04-27T09:00:00+00:00",
        "cycle_updated_at": "2026-04-27T09:00:00+00:00",
    }
    assert set(evidence["input_artifact_refs"]) == {"trade_cal", "daily", "stock_basic"}


def test_readiness_provider_fails_closed_without_pg_freeze_condition(
    tmp_path: Path,
) -> None:
    raw_zone_path = tmp_path / "raw"
    writer = _writer(raw_zone_path, tmp_path)
    _write_trade_cal(writer, date(2026, 4, 16), is_open="1")
    _write_daily(writer, date(2026, 4, 16), DEFAULT_CURRENT_CYCLE_SYMBOLS)
    _write_stock_basic(writer, date(2026, 4, 1), DEFAULT_CURRENT_CYCLE_SYMBOLS)

    def missing_cycle(cycle_id: str) -> CycleMetadata:
        raise RuntimeError(f"missing {cycle_id}")

    provider = CurrentCycleReadinessProvider(
        raw_zone_path=raw_zone_path,
        get_cycle_fn=missing_cycle,
    )

    readiness = provider.get_current_cycle_readiness()
    signal = readiness.as_data_readiness_signal()

    assert readiness.ready is False
    assert readiness.cycle_id == "CYCLE_20260416"
    assert "pg_freeze_conditions_unavailable" in str(readiness.reason)
    assert signal == {
        "ready": False,
        "cycle_id": "CYCLE_20260416",
        "reason": readiness.reason,
        "failed_node": "data_platform.current_selection",
    }
    assert provider.last_evidence["symbols"] == list(DEFAULT_CURRENT_CYCLE_SYMBOLS)


def test_selector_has_no_fixed_cycle_fallback(tmp_path: Path) -> None:
    with pytest.raises(CurrentCycleSelectionError) as exc_info:
        select_current_cycle(raw_zone_path=tmp_path / "empty")

    assert exc_info.value.code == "missing_input_artifact_refs"
    assert exc_info.value.cycle_id is None
    module_source = Path(select_current_cycle.__code__.co_filename).read_text(encoding="utf-8")
    assert "CYCLE_20260415" not in module_source


def _writer(raw_zone_path: Path, tmp_path: Path) -> RawWriter:
    return RawWriter(
        raw_zone_path=raw_zone_path,
        iceberg_warehouse_path=tmp_path / "iceberg" / "warehouse",
    )


def _write_trade_cal(
    writer: RawWriter,
    partition_date: date,
    *,
    is_open: str,
) -> None:
    writer.write_arrow(
        "tushare",
        "trade_cal",
        partition_date,
        str(uuid4()),
        pa.table(
            {
                "exchange": ["SSE"],
                "cal_date": [f"{partition_date:%Y%m%d}"],
                "is_open": [is_open],
                "pretrade_date": ["20260415"],
            }
        ),
    )


def _write_daily(
    writer: RawWriter,
    partition_date: date,
    symbols: tuple[str, ...],
) -> None:
    writer.write_arrow(
        "tushare",
        "daily",
        partition_date,
        str(uuid4()),
        pa.table(
            {
                "ts_code": list(symbols),
                "trade_date": [f"{partition_date:%Y%m%d}" for _ in symbols],
                "close": ["10.0" for _ in symbols],
                "pre_close": ["9.9" for _ in symbols],
                "pct_chg": ["1.0" for _ in symbols],
                "vol": ["100" for _ in symbols],
                "amount": ["1000" for _ in symbols],
            }
        ),
    )


def _write_stock_basic(
    writer: RawWriter,
    partition_date: date,
    symbols: tuple[str, ...],
) -> None:
    writer.write_arrow(
        "tushare",
        "stock_basic",
        partition_date,
        str(uuid4()),
        pa.table(
            {
                "ts_code": list(symbols),
                "symbol": [symbol.split(".")[0] for symbol in symbols],
                "name": [f"name-{index}" for index, _ in enumerate(symbols)],
                "list_status": ["L" for _ in symbols],
            }
        ),
    )


def _cycle_metadata(
    cycle_id: str,
    *,
    status: Any,
    candidate_count: int = 0,
    cutoff_submitted_at: datetime | None = None,
    cutoff_ingest_seq: int | None = None,
    selection_frozen_at: datetime | None = None,
) -> CycleMetadata:
    created_at = datetime(2026, 4, 27, 9, 0, tzinfo=UTC)
    return CycleMetadata(
        cycle_id=cycle_id,
        cycle_date=_cycle_date(cycle_id),
        status=status,
        cutoff_submitted_at=cutoff_submitted_at,
        cutoff_ingest_seq=cutoff_ingest_seq,
        candidate_count=candidate_count,
        selection_frozen_at=selection_frozen_at,
        created_at=created_at,
        updated_at=created_at,
    )


def _cycle_date(cycle_id: str) -> date:
    return date.fromisoformat(
        f"{cycle_id[6:10]}-{cycle_id[10:12]}-{cycle_id[12:14]}"
    )
