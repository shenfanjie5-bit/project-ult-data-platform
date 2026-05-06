from __future__ import annotations

import importlib.util
import json
from datetime import date
from decimal import Decimal
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

pa = pytest.importorskip("pyarrow")

from data_platform.adapters.tushare import (  # noqa: E402
    TUSHARE_ASSETS,
    TUSHARE_FUND_PORTFOLIO_ASSET,
    TUSHARE_HSGT_HOLD_TOP10_ASSET,
    TUSHARE_HSGT_TOP10_ASSET,
    TUSHARE_TOP10_FLOATHOLDERS_ASSET,
    TUSHARE_TOP10_HOLDERS_ASSET,
)
from data_platform.holdings_backfill import (  # noqa: E402
    SUPPORTED_HOLDINGS_BACKFILL_DATASETS,
    HoldingsBackfillPlan,
    HoldingsBackfillPlanError,
    HoldingsBackfillPlanItem,
    build_holdings_backfill_plan,
    execute_holdings_backfill_plan,
    public_plan_summary,
)
from data_platform.raw import RawWriter  # noqa: E402


class FakeHoldingsAdapter:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def source_id(self) -> str:
        return "tushare"

    def get_assets(self) -> list[Any]:
        return list(TUSHARE_ASSETS)

    def get_resources(self) -> dict[str, Any]:
        return {}

    def get_staging_dbt_models(self) -> list[str]:
        return []

    def get_quota_config(self) -> dict[str, Any]:
        return {}

    def fetch(self, asset_id: str, params: dict[str, Any]) -> Any:
        self.calls.append((asset_id, dict(params)))
        asset = next(asset for asset in TUSHARE_ASSETS if asset.name == asset_id)
        return _table_for_asset(asset, params)


def test_five_holdings_interfaces_generate_bounded_plan() -> None:
    plan = _full_plan()

    assert plan.ok is True
    assert [item.dataset for item in plan.planned_items] == [
        "top10_holders",
        "top10_floatholders",
        "fund_portfolio",
        "hsgt_top10",
        "hsgt_hold_top10",
    ]
    assert {item.status for item in plan.items} == {"planned"}
    assert {item.dataset for item in plan.items} == set(SUPPORTED_HOLDINGS_BACKFILL_DATASETS)


@pytest.mark.parametrize(
    ("datasets", "kwargs", "reason_type"),
    [
        (
            ["top10_holders"],
            {"periods": ["20240331"]},
            "missing_bounded_inputs",
        ),
        (
            ["fund_portfolio"],
            {"fund_codes": ["TESTFUND.OF"]},
            "missing_bounded_inputs",
        ),
        (
            ["hsgt_top10"],
            {"market_types": ["1"]},
            "missing_bounded_inputs",
        ),
        (
            ["hsgt_hold_top10"],
            {"trade_dates": ["20240402"], "exchanges": ["SH"]},
            "missing_bounded_inputs",
        ),
    ],
)
def test_missing_code_or_date_boundaries_are_rejected(
    datasets: list[str],
    kwargs: dict[str, list[str]],
    reason_type: str,
) -> None:
    plan = build_holdings_backfill_plan(datasets=datasets, **kwargs)

    assert plan.ok is False
    assert len(plan.rejected_items) == 1
    assert plan.rejected_items[0].reason_type == reason_type
    assert plan.planned_items == ()


@pytest.mark.parametrize(
    ("dataset", "kwargs"),
    [
        ("hsgt_top10", {"trade_dates": ["20240402"], "market_types": ["1"]}),
        ("hsgt_hold_top10", {"trade_dates": ["20240402"], "exchanges": ["SH"]}),
    ],
)
def test_hsgt_backfill_rejects_unbounded_market_or_exchange_scope(
    dataset: str,
    kwargs: dict[str, list[str]],
) -> None:
    plan = build_holdings_backfill_plan(datasets=[dataset], **kwargs)

    assert plan.ok is False
    assert plan.planned_items == ()
    assert plan.rejected_items[0].reason_type == "missing_bounded_inputs"
    assert "stock_codes" in plan.rejected_items[0].bounds["missing"]


def test_hsgt_hold_top10_skips_after_cutoff_without_calling_live_path() -> None:
    plan = build_holdings_backfill_plan(
        datasets=["hsgt_hold_top10"],
        stock_codes=["TESTSTK.SZ"],
        trade_dates=["20240820", "20240821"],
        exchanges=["SH"],
    )

    assert [item.status for item in plan.items] == ["planned", "skipped"]
    assert plan.items[0].fetch_params == {
        "trade_date": "20240820",
        "exchange": "SH",
        "ts_code": "TESTSTK.SZ",
    }
    assert plan.items[1].reason_type == "post_publication_cutoff"
    assert "2024-08-20" in str(plan.items[1].reason)


def test_execute_uses_adapter_params_from_plan_and_writes_raw(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    adapter = FakeHoldingsAdapter()
    writer = RawWriter(
        raw_zone_path=tmp_path / "raw",
        iceberg_warehouse_path=tmp_path / "warehouse",
    )

    monkeypatch.setenv("DP_TUSHARE_LIVE_HOLDINGS_BACKFILL", "1")

    result = execute_holdings_backfill_plan(
        _full_plan(),
        adapter=adapter,  # type: ignore[arg-type]
        raw_writer=writer,
        execute_live=True,
    )

    assert result.ok is True
    assert len(result.artifacts) == 5
    assert adapter.calls == [
        (TUSHARE_TOP10_HOLDERS_ASSET.name, {"ts_code": "TESTSTK.SZ", "period": "20240331"}),
        (
            TUSHARE_TOP10_FLOATHOLDERS_ASSET.name,
            {"ts_code": "TESTSTK.SZ", "period": "20240331"},
        ),
        (TUSHARE_FUND_PORTFOLIO_ASSET.name, {"ts_code": "TESTFUND.OF", "period": "20240331"}),
        (
            TUSHARE_HSGT_TOP10_ASSET.name,
            {"trade_date": "20240402", "market_type": "1", "ts_code": "TESTSTK.SZ"},
        ),
        (
            TUSHARE_HSGT_HOLD_TOP10_ASSET.name,
            {"trade_date": "20240402", "exchange": "SH", "ts_code": "TESTSTK.SZ"},
        ),
    ]
    assert {
        artifact.path.parent.name
        for artifact in result.artifacts
    } == {"dt=20240331", "dt=20240402"}


@pytest.mark.parametrize(
    ("execute_live", "env_value", "match"),
    [
        (False, "1", "execute_live=True"),
        (True, None, "DP_TUSHARE_LIVE_HOLDINGS_BACKFILL=1"),
        (True, "0", "DP_TUSHARE_LIVE_HOLDINGS_BACKFILL=1"),
    ],
)
def test_execute_requires_double_live_gate_before_adapter_or_writer_call(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    execute_live: bool,
    env_value: str | None,
    match: str,
) -> None:
    adapter = FakeHoldingsAdapter()
    writer = RecordingRawWriter(
        raw_zone_path=tmp_path / "raw",
        iceberg_warehouse_path=tmp_path / "warehouse",
    )
    if env_value is None:
        monkeypatch.delenv("DP_TUSHARE_LIVE_HOLDINGS_BACKFILL", raising=False)
    else:
        monkeypatch.setenv("DP_TUSHARE_LIVE_HOLDINGS_BACKFILL", env_value)

    with pytest.raises(HoldingsBackfillPlanError, match=match):
        execute_holdings_backfill_plan(
            _full_plan(),
            adapter=adapter,  # type: ignore[arg-type]
            raw_writer=writer,
            execute_live=execute_live,
        )

    assert adapter.calls == []
    assert writer.write_arrow_calls == 0


def test_execute_refuses_rejected_plan(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    adapter = FakeHoldingsAdapter()
    monkeypatch.setenv("DP_TUSHARE_LIVE_HOLDINGS_BACKFILL", "1")
    plan = build_holdings_backfill_plan(
        datasets=["top10_holders"],
        periods=["20240331"],
    )

    with pytest.raises(HoldingsBackfillPlanError, match="rejected"):
        execute_holdings_backfill_plan(
            plan,
            adapter=adapter,  # type: ignore[arg-type]
            raw_writer=RawWriter(
                raw_zone_path=tmp_path / "raw",
                iceberg_warehouse_path=tmp_path / "warehouse",
            ),
            execute_live=True,
        )

    assert adapter.calls == []


@pytest.mark.parametrize(
    ("item", "match"),
    [
        (
            HoldingsBackfillPlanItem(
                dataset="top10_holders",
                status="planned",
                partition_date=date(2024, 3, 31),
                fetch_params={"ts_code": "TESTSTK.SZ", "period": "20240430"},
            ),
            "period matching partition_date",
        ),
        (
            HoldingsBackfillPlanItem(
                dataset="top10_floatholders",
                status="planned",
                partition_date=date(2024, 3, 31),
                fetch_params={"ts_code": "TESTSTK.SZ"},
            ),
            "non-empty scalar period",
        ),
        (
            HoldingsBackfillPlanItem(
                dataset="fund_portfolio",
                status="planned",
                partition_date=date(2024, 3, 31),
                fetch_params={},
            ),
            "non-empty scalar ts_code",
        ),
        (
            HoldingsBackfillPlanItem(
                dataset="hsgt_top10",
                status="planned",
                partition_date=date(2024, 4, 2),
                fetch_params={"trade_date": "20240402", "market_type": "1"},
            ),
            "non-empty scalar ts_code",
        ),
        (
            HoldingsBackfillPlanItem(
                dataset="hsgt_top10",
                status="planned",
                partition_date=date(2024, 4, 2),
                fetch_params={
                    "trade_date": "20240402",
                    "market_type": "1",
                    "ts_code": ["TESTSTK.SZ"],
                },
            ),
            "non-empty scalar ts_code",
        ),
        (
            HoldingsBackfillPlanItem(
                dataset="hsgt_top10",
                status="planned",
                partition_date=date(2024, 4, 2),
                fetch_params={"trade_date": "20240403", "market_type": "1", "ts_code": "TESTSTK.SZ"},
            ),
            "trade_date matching partition_date",
        ),
        (
            HoldingsBackfillPlanItem(
                dataset="hsgt_top10",
                status="planned",
                partition_date=date(2024, 4, 2),
                fetch_params={"trade_date": "20240402", "ts_code": "TESTSTK.SZ"},
            ),
            "non-empty scalar market_type",
        ),
        (
            HoldingsBackfillPlanItem(
                dataset="hsgt_hold_top10",
                status="planned",
                partition_date=date(2024, 4, 2),
                fetch_params={"trade_date": "20240402", "ts_code": "TESTSTK.SZ"},
            ),
            "non-empty scalar exchange",
        ),
        (
            HoldingsBackfillPlanItem(
                dataset="hsgt_hold_top10",
                status="planned",
                partition_date=date(2024, 8, 21),
                fetch_params={
                    "trade_date": "20240821",
                    "exchange": "SH",
                    "ts_code": "TESTSTK.SZ",
                },
            ),
            "2024-08-20 cutoff",
        ),
    ],
)
def test_execute_revalidates_malformed_planned_hsgt_items_without_adapter_call(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    item: HoldingsBackfillPlanItem,
    match: str,
) -> None:
    adapter = FakeHoldingsAdapter()
    monkeypatch.setenv("DP_TUSHARE_LIVE_HOLDINGS_BACKFILL", "1")
    plan = HoldingsBackfillPlan((item,))

    with pytest.raises(HoldingsBackfillPlanError, match=match):
        execute_holdings_backfill_plan(
            plan,
            adapter=adapter,  # type: ignore[arg-type]
            raw_writer=RawWriter(
                raw_zone_path=tmp_path / "raw",
                iceberg_warehouse_path=tmp_path / "warehouse",
            ),
            execute_live=True,
        )

    assert adapter.calls == []


def test_execute_preflights_full_plan_before_first_adapter_call(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    adapter = FakeHoldingsAdapter()
    monkeypatch.setenv("DP_TUSHARE_LIVE_HOLDINGS_BACKFILL", "1")
    plan = HoldingsBackfillPlan(
        (
            HoldingsBackfillPlanItem(
                dataset="top10_holders",
                status="planned",
                partition_date=date(2024, 3, 31),
                fetch_params={"ts_code": "TESTSTK.SZ", "period": "20240331"},
            ),
            HoldingsBackfillPlanItem(
                dataset="hsgt_top10",
                status="planned",
                partition_date=date(2024, 4, 2),
                fetch_params={"trade_date": "20240402", "ts_code": "TESTSTK.SZ"},
            ),
        )
    )

    with pytest.raises(HoldingsBackfillPlanError, match="non-empty scalar market_type"):
        execute_holdings_backfill_plan(
            plan,
            adapter=adapter,  # type: ignore[arg-type]
            raw_writer=RawWriter(
                raw_zone_path=tmp_path / "raw",
                iceberg_warehouse_path=tmp_path / "warehouse",
            ),
            execute_live=True,
        )

    assert adapter.calls == []


def test_public_plan_summary_does_not_leak_raw_provider_or_private_fields() -> None:
    summary = public_plan_summary(_full_plan())
    encoded = json.dumps(summary, sort_keys=True)

    assert "TESTSTK.SZ" not in encoded
    assert "TESTFUND.OF" not in encoded
    assert "stock_code_count" in encoded
    assert "fund_code_count" in encoded
    forbidden_terms = {
        "asset",
        "doc_api",
        "fetch_params",
        "fields",
        "hk_hold",
        "provider",
        "raw",
        "request_params",
        "source_id",
        "source_interface_id",
        "Tushare",
        "_",
    }
    assert not forbidden_terms.intersection(encoded.split('"'))
    assert "ts_code" not in encoded


def test_public_plan_summary_drops_unknown_identifier_bounds() -> None:
    plan = HoldingsBackfillPlan(
        (
            HoldingsBackfillPlanItem(
                dataset="top10_holders",
                status="rejected",
                bounds={
                    "period": "20240331",
                    "requested_identifiers": ["TESTSTK.SZ"],
                    "_raw_params": {"ts_code": "TESTSTK.SZ"},
                },
                reason_type="example",
                reason="example rejection",
            ),
        )
    )

    summary = public_plan_summary(plan)
    encoded = json.dumps(summary, sort_keys=True)

    assert summary["items"][0]["bounds"] == {"period": "20240331"}
    assert "TESTSTK.SZ" not in encoded
    assert "requested_identifiers" not in encoded
    assert "ts_code" not in encoded


def test_cli_json_report_sanitizes_identifier_bounds(tmp_path: Path) -> None:
    report_path = tmp_path / "reports" / "holdings.json"
    cli = _load_holdings_backfill_cli()

    exit_code = cli.main(
        [
            "--dataset",
            "top10_holders",
            "--stock-code",
            "TESTSTK.SZ",
            "--period",
            "20240331",
            "--json-report",
            str(report_path),
        ]
    )

    assert exit_code == 0
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    encoded = json.dumps(payload, sort_keys=True)
    assert payload["execute_live"] is False
    assert "execution" not in payload
    assert payload["plan"]["items"][0]["bounds"] == {
        "period": "20240331",
        "stock_code_class": "stock_code",
        "stock_code_count": 1,
    }
    assert "TESTSTK.SZ" not in encoded
    assert "ts_code" not in encoded


def test_cli_execute_live_requires_env_gate_before_live_objects(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    cli = _load_holdings_backfill_cli()
    monkeypatch.delenv("DP_TUSHARE_LIVE_HOLDINGS_BACKFILL", raising=False)
    monkeypatch.setattr(
        cli,
        "RawWriter",
        lambda *_args, **_kwargs: pytest.fail("RawWriter must not be constructed"),
    )
    monkeypatch.setattr(
        cli,
        "execute_holdings_backfill_plan",
        lambda *_args, **_kwargs: pytest.fail("execution must not be called"),
    )

    exit_code = cli.main(
        [
            "--dataset",
            "top10_holders",
            "--stock-code",
            "TESTSTK.SZ",
            "--period",
            "20240331",
            "--execute-live",
            "--raw-zone-path",
            str(tmp_path / "raw"),
            "--iceberg-warehouse-path",
            str(tmp_path / "warehouse"),
        ]
    )

    assert exit_code == 2


class RecordingRawWriter(RawWriter):
    def __init__(self, *, raw_zone_path: Path, iceberg_warehouse_path: Path) -> None:
        super().__init__(
            raw_zone_path=raw_zone_path,
            iceberg_warehouse_path=iceberg_warehouse_path,
        )
        self.write_arrow_calls = 0

    def write_arrow(self, *args: Any, **kwargs: Any) -> Any:
        self.write_arrow_calls += 1
        return super().write_arrow(*args, **kwargs)


def _full_plan() -> Any:
    return build_holdings_backfill_plan(
        datasets=SUPPORTED_HOLDINGS_BACKFILL_DATASETS,
        stock_codes=["TESTSTK.SZ"],
        fund_codes=["TESTFUND.OF"],
        periods=["20240331"],
        trade_dates=["20240402"],
        market_types=["1"],
        exchanges=["SH"],
    )


def _load_holdings_backfill_cli() -> ModuleType:
    script_path = Path(__file__).parents[2] / "scripts" / "holdings_backfill.py"
    spec = importlib.util.spec_from_file_location("holdings_backfill_cli", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _table_for_asset(asset: Any, params: dict[str, Any]) -> Any:
    row = _row_for_asset(asset, params)
    return pa.table(
        {field.name: [row[field.name]] for field in asset.schema},
        schema=asset.schema,
    )


def _row_for_asset(asset: Any, params: dict[str, Any]) -> dict[str, Any]:
    row: dict[str, Any] = {}
    date_value = params.get("period") or params.get("trade_date") or "20240331"
    for field in asset.schema:
        if field.name == "ts_code":
            row[field.name] = params.get("ts_code", "TESTSTK.SZ")
        elif field.name == "symbol":
            row[field.name] = "TESTSTK.SZ"
        elif field.name in {"ann_date", "end_date", "trade_date"}:
            row[field.name] = date_value
        elif field.name == "market_type":
            row[field.name] = params.get("market_type", "1")
        elif field.name == "exchange":
            row[field.name] = params.get("exchange", "SH")
        elif field.name == "rank":
            row[field.name] = "1"
        elif pa.types.is_decimal(field.type):
            row[field.name] = Decimal("1.0")
        elif pa.types.is_integer(field.type):
            row[field.name] = 1
        elif pa.types.is_floating(field.type):
            row[field.name] = 1.0
        elif pa.types.is_boolean(field.type):
            row[field.name] = True
        else:
            row[field.name] = f"{field.name}-value"
    return row
