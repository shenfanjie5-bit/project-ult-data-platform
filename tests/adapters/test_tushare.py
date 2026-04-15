from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

pd = pytest.importorskip("pandas")
pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

from data_platform.adapters.base import AdapterRegistry, QuotaConfig  # noqa: E402
from data_platform.adapters.tushare import (  # noqa: E402
    TUSHARE_STOCK_BASIC_ASSET,
    TushareAdapter,
)
from data_platform.adapters.tushare import adapter as tushare_adapter_module  # noqa: E402
from data_platform.adapters.tushare.assets import (  # noqa: E402
    TUSHARE_STOCK_BASIC_FIELDS_CSV,
)


class FakeTushareClient:
    def __init__(self, frame: Any) -> None:
        self.frame = frame
        self.calls: list[dict[str, Any]] = []

    def stock_basic(self, **kwargs: Any) -> Any:
        self.calls.append(kwargs)
        return self.frame


def _stock_basic_frame(row_count: int) -> Any:
    return pd.DataFrame(
        {
            "ts_code": [f"{index:06d}.SZ" for index in range(row_count)],
            "symbol": [f"{index:06d}" for index in range(row_count)],
            "name": [f"stock-{index}" for index in range(row_count)],
            "list_date": ["20260415"] * row_count,
            "extra_column": ["ignored"] * row_count,
        }
    )


@pytest.fixture
def raw_zone_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    raw_path = tmp_path / "raw"
    monkeypatch.setenv("DP_RAW_ZONE_PATH", str(raw_path))
    monkeypatch.setenv("DP_ICEBERG_WAREHOUSE_PATH", str(tmp_path / "iceberg" / "warehouse"))
    return raw_path


def test_tushare_adapter_declares_stock_basic_asset_and_quota() -> None:
    adapter = TushareAdapter(token="test-token", client=FakeTushareClient(_stock_basic_frame(1)))

    assert adapter.source_id() == "tushare"
    assert adapter.get_assets() == [TUSHARE_STOCK_BASIC_ASSET]
    assert adapter.get_assets()[0].dataset == "stock_basic"
    assert adapter.get_assets()[0].partition == "static"
    assert adapter.get_staging_dbt_models() == ["stg_stock_basic"]
    assert adapter.get_quota_config() == QuotaConfig(
        requests_per_minute=200,
        daily_credit_quota=None,
    )


def test_fetch_stock_basic_returns_declared_schema() -> None:
    client = FakeTushareClient(_stock_basic_frame(2))
    adapter = TushareAdapter(token="test-token", client=client)

    table = adapter.fetch("tushare_stock_basic", {"list_status": "L", "fields": "bad"})

    assert isinstance(table, pa.Table)
    assert table.schema == TUSHARE_STOCK_BASIC_ASSET.schema
    assert table.num_rows == 2
    assert client.calls == [
        {
            "list_status": "L",
            "fields": TUSHARE_STOCK_BASIC_FIELDS_CSV,
        }
    ]


def test_tushare_adapter_can_be_registered_and_fetched() -> None:
    registry = AdapterRegistry()
    adapter = TushareAdapter(token="test-token", client=FakeTushareClient(_stock_basic_frame(1)))

    registry.register(adapter)

    assert registry.get("tushare") is adapter


def test_cli_writes_stock_basic_parquet_artifact(
    raw_zone_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    client = FakeTushareClient(_stock_basic_frame(5_000))
    tokens: list[str] = []

    def pro_api(token: str) -> FakeTushareClient:
        tokens.append(token)
        return client

    monkeypatch.setenv("DP_TUSHARE_TOKEN", "test-token")
    monkeypatch.setitem(sys.modules, "tushare", SimpleNamespace(pro_api=pro_api))

    exit_code = tushare_adapter_module.main(
        ["--asset", "tushare_stock_basic", "--date", "20260415"]
    )

    captured = capsys.readouterr()
    artifact_path = Path(captured.out.strip())

    assert exit_code == 0
    assert captured.err == ""
    assert tokens == ["test-token"]
    assert artifact_path.parent == raw_zone_path / "tushare" / "stock_basic" / "dt=20260415"
    assert artifact_path.suffix == ".parquet"
    assert len(list(artifact_path.parent.glob("*.parquet"))) == 1
    assert pq.read_table(artifact_path).num_rows == 5_000


def test_cli_missing_token_outputs_json_error(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.delenv("DP_TUSHARE_TOKEN", raising=False)

    exit_code = tushare_adapter_module.main(
        ["--asset", "tushare_stock_basic", "--date", "20260415"]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.err)

    assert exit_code == 1
    assert captured.out == ""
    assert payload["asset"] == "tushare_stock_basic"
    assert "DP_TUSHARE_TOKEN" in payload["error"]
