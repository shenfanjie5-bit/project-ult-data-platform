from __future__ import annotations

import json
import sys
import uuid
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

pd = pytest.importorskip("pandas")
pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

from data_platform.adapters.base import DataSourceAdapter  # noqa: E402
from data_platform.adapters.tushare import (  # noqa: E402
    FINANCIAL_VERSION_FIELDS,
    TUSHARE_ASSETS,
    TUSHARE_BALANCESHEET_ASSET,
    TUSHARE_CASHFLOW_ASSET,
    TUSHARE_FINA_INDICATOR_ASSET,
    TUSHARE_INCOME_ASSET,
    TushareAdapter,
)
from data_platform.adapters.tushare import adapter as tushare_adapter_module  # noqa: E402


FINANCIAL_ASSETS = [
    TUSHARE_INCOME_ASSET,
    TUSHARE_BALANCESHEET_ASSET,
    TUSHARE_CASHFLOW_ASSET,
    TUSHARE_FINA_INDICATOR_ASSET,
]


class FakeTushareFinancialClient:
    def __init__(self, frames_by_method: dict[str, Any]) -> None:
        self.frames_by_method = frames_by_method
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def income(self, **kwargs: Any) -> Any:
        return self._record_call("income", kwargs)

    def balancesheet(self, **kwargs: Any) -> Any:
        return self._record_call("balancesheet", kwargs)

    def cashflow(self, **kwargs: Any) -> Any:
        return self._record_call("cashflow", kwargs)

    def fina_indicator(self, **kwargs: Any) -> Any:
        return self._record_call("fina_indicator", kwargs)

    def _record_call(self, method_name: str, kwargs: dict[str, Any]) -> Any:
        self.calls.append((method_name, kwargs))
        return self.frames_by_method[method_name]


@pytest.fixture
def raw_zone_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    raw_path = tmp_path / "raw"
    monkeypatch.setenv("DP_RAW_ZONE_PATH", str(raw_path))
    monkeypatch.setenv("DP_ICEBERG_WAREHOUSE_PATH", str(tmp_path / "iceberg" / "warehouse"))
    return raw_path


def _fields_csv(asset: Any) -> str:
    return ",".join(asset.schema.names)


def _financial_row(
    asset: Any,
    index: int = 0,
    *,
    ann_date: str = "20260415",
    f_ann_date: str = "20260416",
    end_date: str = "20260331",
    update_flag: str | None = None,
) -> dict[str, Any]:
    row: dict[str, Any] = {}
    for field in asset.schema:
        if field.name == "ts_code":
            row[field.name] = "000001.SZ"
        elif field.name == "ann_date":
            row[field.name] = f" {ann_date} "
        elif field.name == "f_ann_date":
            row[field.name] = f" {f_ann_date} "
        elif field.name == "end_date":
            row[field.name] = f" {end_date} "
        elif field.name == "report_type":
            row[field.name] = "1"
        elif field.name == "comp_type":
            row[field.name] = "1"
        elif field.name == "update_flag":
            row[field.name] = str(index) if update_flag is None else update_flag
        else:
            row[field.name] = str(1000 + index)
    return row


def _financial_frame(asset: Any, row_count: int, end_date: str = "20260331") -> Any:
    frame = pd.DataFrame(
        [_financial_row(asset, index=index, end_date=end_date) for index in range(row_count)]
    )
    frame["extra_column"] = ["ignored"] * row_count
    return frame


def _empty_financial_frame(asset: Any) -> Any:
    return pd.DataFrame(columns=[*asset.schema.names, "extra_column"])


def _client_for_asset(asset: Any, frame: Any) -> FakeTushareFinancialClient:
    return FakeTushareFinancialClient({asset.dataset: frame})


def _install_tushare_client(
    monkeypatch: pytest.MonkeyPatch,
    client: FakeTushareFinancialClient,
) -> list[str]:
    tokens: list[str] = []

    def pro_api(token: str) -> FakeTushareFinancialClient:
        tokens.append(token)
        return client

    monkeypatch.setenv("DP_TUSHARE_TOKEN", "test-token")
    monkeypatch.setitem(sys.modules, "tushare", SimpleNamespace(pro_api=pro_api))
    return tokens


def test_financial_assets_and_staging_models_are_registered() -> None:
    client = _client_for_asset(TUSHARE_INCOME_ASSET, _financial_frame(TUSHARE_INCOME_ASSET, 1))
    adapter = TushareAdapter(token="test-token", client=client)
    assets_by_name = {asset.name: asset for asset in adapter.get_assets()}

    assert isinstance(adapter, DataSourceAdapter)
    for asset in FINANCIAL_ASSETS:
        assert assets_by_name[asset.name] == asset
        assert asset.partition == "daily"
        assert asset.name == f"tushare_{asset.dataset}"
    assert adapter.get_staging_dbt_models() == [f"stg_{asset.dataset}" for asset in TUSHARE_ASSETS]
    assert {"stg_income", "stg_balancesheet", "stg_cashflow", "stg_fina_indicator"} <= set(
        adapter.get_staging_dbt_models()
    )
    assert type(adapter.get_quota_config()) is dict
    assert FINANCIAL_VERSION_FIELDS == (
        "ts_code",
        "ann_date",
        "f_ann_date",
        "end_date",
        "report_type",
        "comp_type",
        "update_flag",
    )


@pytest.mark.parametrize("asset", FINANCIAL_ASSETS, ids=lambda asset: asset.name)
def test_fetch_financial_asset_returns_declared_schema(asset: Any) -> None:
    frame = _financial_frame(asset, 2)
    first_numeric_field = asset.schema.names[len(FINANCIAL_VERSION_FIELDS)]
    frame.loc[0, first_numeric_field] = None
    client = _client_for_asset(asset, frame)
    adapter = TushareAdapter(token="test-token", client=client)
    params = {
        "ts_code": "000001.SZ",
        "ann_date": "20260415",
        "start_date": "20260401",
        "end_date": "20260430",
        "period": "20260331",
        "fields": "bad",
    }

    table = adapter.fetch(asset.name, params)

    assert isinstance(table, pa.Table)
    assert table.schema == asset.schema
    assert table.num_rows == 2
    assert table["ann_date"].to_pylist() == ["20260415", "20260415"]
    assert table["f_ann_date"].to_pylist() == ["20260416", "20260416"]
    assert table["end_date"].to_pylist() == ["20260331", "20260331"]
    assert table[first_numeric_field].to_pylist()[0] is None
    assert table.schema.field(first_numeric_field).type == pa.float64()
    assert client.calls == [(asset.dataset, {**params, "fields": _fields_csv(asset)})]


def test_fetch_financial_asset_preserves_revised_report_versions() -> None:
    rows = [
        _financial_row(TUSHARE_INCOME_ASSET, index=0, update_flag="0"),
        _financial_row(TUSHARE_INCOME_ASSET, index=1, update_flag="1"),
    ]
    client = _client_for_asset(TUSHARE_INCOME_ASSET, pd.DataFrame(rows))
    adapter = TushareAdapter(token="test-token", client=client)

    table = adapter.fetch(
        TUSHARE_INCOME_ASSET.name,
        {"ts_code": "000001.SZ", "period": "20260331"},
    )

    assert table.num_rows == 2
    assert table["ts_code"].to_pylist() == ["000001.SZ", "000001.SZ"]
    assert table["end_date"].to_pylist() == ["20260331", "20260331"]
    assert table["update_flag"].to_pylist() == ["0", "1"]


@pytest.mark.parametrize("asset", FINANCIAL_ASSETS, ids=lambda asset: asset.name)
def test_fetch_financial_empty_return_keeps_schema(asset: Any) -> None:
    client = _client_for_asset(asset, _empty_financial_frame(asset))
    adapter = TushareAdapter(token="test-token", client=client)

    table = adapter.fetch(asset.name, {"period": "20260331"})

    assert table.schema == asset.schema
    assert table.num_rows == 0


def test_cli_writes_financial_raw_artifact_and_manifest(
    raw_zone_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    client = _client_for_asset(TUSHARE_INCOME_ASSET, _financial_frame(TUSHARE_INCOME_ASSET, 2))
    tokens = _install_tushare_client(monkeypatch, client)

    exit_code = tushare_adapter_module.main(
        ["--asset", TUSHARE_INCOME_ASSET.name, "--date", "20260331"]
    )

    captured = capsys.readouterr()
    artifact_path = Path(captured.out.strip())
    manifest = json.loads((artifact_path.parent / "_manifest.json").read_text(encoding="utf-8"))
    table = pq.read_table(artifact_path).select(TUSHARE_INCOME_ASSET.schema.names)

    assert exit_code == 0
    assert captured.err == ""
    assert tokens == ["test-token"]
    assert uuid.UUID(artifact_path.stem).version == 4
    assert artifact_path.parent == raw_zone_path / "tushare" / "income" / "dt=20260331"
    assert table.schema == TUSHARE_INCOME_ASSET.schema
    assert table.num_rows == 2
    assert manifest["artifacts"][0]["dataset"] == "income"
    assert manifest["artifacts"][0]["row_count"] == 2
    assert client.calls == [("income", {"period": "20260331", "fields": _fields_csv(TUSHARE_INCOME_ASSET)})]


@pytest.mark.parametrize("missing_field", ["ts_code", "ann_date", "end_date"])
def test_cli_rejects_financial_missing_key_field_without_artifact(
    raw_zone_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    missing_field: str,
) -> None:
    frame = _financial_frame(TUSHARE_INCOME_ASSET, 1).drop(columns=[missing_field])
    client = _client_for_asset(TUSHARE_INCOME_ASSET, frame)
    _install_tushare_client(monkeypatch, client)

    exit_code = tushare_adapter_module.main(
        ["--asset", TUSHARE_INCOME_ASSET.name, "--date", "20260331"]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.err)

    assert exit_code == 1
    assert captured.out == ""
    assert payload["asset"] == TUSHARE_INCOME_ASSET.name
    assert "income" in payload["error"]
    assert missing_field in payload["error"]
    assert list(raw_zone_path.rglob("*.parquet")) == []


def test_run_tushare_asset_rejects_financial_period_mismatch_without_artifact(
    raw_zone_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _client_for_asset(TUSHARE_INCOME_ASSET, _financial_frame(TUSHARE_INCOME_ASSET, 1))
    _install_tushare_client(monkeypatch, client)

    with pytest.raises(ValueError, match="does not match Raw partition date"):
        tushare_adapter_module.run_tushare_asset(
            TUSHARE_INCOME_ASSET.name,
            date(2026, 3, 31),
            {"period": "20260430"},
        )

    assert client.calls == []
    assert list(raw_zone_path.rglob("*.parquet")) == []
