"""Tushare adapter package."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from data_platform.adapters.tushare.assets import (
    TUSHARE_ASSETS,
    TUSHARE_STOCK_BASIC_ASSET,
    TUSHARE_STOCK_BASIC_ASSET_NAME,
    TUSHARE_STOCK_BASIC_FIELDS,
    TUSHARE_STOCK_BASIC_SCHEMA,
)

if TYPE_CHECKING:
    from data_platform.adapters.tushare.adapter import AdapterConfigError, TushareAdapter


def __getattr__(name: str) -> Any:
    if name == "AdapterConfigError":
        from data_platform.adapters.tushare.adapter import AdapterConfigError

        return AdapterConfigError
    if name == "TushareAdapter":
        from data_platform.adapters.tushare.adapter import TushareAdapter

        return TushareAdapter
    raise AttributeError(name)


__all__ = [
    "AdapterConfigError",
    "TUSHARE_ASSETS",
    "TUSHARE_STOCK_BASIC_ASSET",
    "TUSHARE_STOCK_BASIC_ASSET_NAME",
    "TUSHARE_STOCK_BASIC_FIELDS",
    "TUSHARE_STOCK_BASIC_SCHEMA",
    "TushareAdapter",
]
