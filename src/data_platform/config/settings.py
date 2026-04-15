"""Runtime settings shared by data-platform components."""

from functools import lru_cache
from pathlib import Path
from typing import Any, Literal

from pydantic import PostgresDsn
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DP_", env_file=".env", extra="ignore")

    pg_dsn: PostgresDsn
    raw_zone_path: Path
    iceberg_warehouse_path: Path
    duckdb_path: Path
    iceberg_catalog_name: str = "data_platform"
    env: Literal["dev", "test", "prod"] = "dev"

    def __init__(self, **values: Any) -> None:
        super().__init__(**values)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


def reset_settings_cache() -> None:
    get_settings.cache_clear()
