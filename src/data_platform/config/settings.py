"""Runtime settings shared by data-platform components."""

from functools import lru_cache
import os
from pathlib import Path
from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel, ConfigDict, PostgresDsn

if TYPE_CHECKING:
    def SettingsConfigDict(
        *,
        env_prefix: str,
        env_file: str,
        extra: Literal["allow", "ignore", "forbid"] | None,
    ) -> ConfigDict:
        return ConfigDict(extra=extra)

    class BaseSettings(BaseModel):
        def __init__(self, **values: object) -> None: ...

else:
    try:
        from pydantic_settings import BaseSettings, SettingsConfigDict
    except ModuleNotFoundError as error:
        if error.name != "pydantic_settings":
            raise

        SettingsConfigDict = ConfigDict

        class BaseSettings(BaseModel):
            """Small fallback for test sandboxes without pydantic-settings installed."""

            def __init__(self, **values: object) -> None:
                config = self.model_config
                env_prefix = str(config.get("env_prefix", ""))
                env_file = config.get("env_file")
                field_names = set(self.__class__.model_fields)
                settings_values: dict[str, object] = {}

                if isinstance(env_file, str | os.PathLike):
                    settings_values.update(_read_env_file(Path(env_file), env_prefix, field_names))

                settings_values.update(_read_prefixed_environment(env_prefix, field_names))
                settings_values.update(values)
                super().__init__(**settings_values)


def _read_env_file(env_file: Path, env_prefix: str, field_names: set[str]) -> dict[str, str]:
    if not env_file.exists():
        return {}

    values: dict[str, str] = {}
    for raw_line in env_file.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        field_name = _field_name_from_env_key(key.strip(), env_prefix)
        if field_name in field_names:
            values[field_name] = _clean_env_value(value)
    return values


def _read_prefixed_environment(env_prefix: str, field_names: set[str]) -> dict[str, str]:
    values: dict[str, str] = {}
    for field_name in field_names:
        env_key = f"{env_prefix}{field_name}".upper()
        if env_key in os.environ:
            values[field_name] = os.environ[env_key]
    return values


def _field_name_from_env_key(env_key: str, env_prefix: str) -> str:
    normalized_key = env_key
    if env_prefix and normalized_key.upper().startswith(env_prefix.upper()):
        normalized_key = normalized_key[len(env_prefix) :]
    return normalized_key.lower()


def _clean_env_value(value: str) -> str:
    cleaned = value.strip()
    if len(cleaned) >= 2 and cleaned[0] == cleaned[-1] and cleaned[0] in {"'", '"'}:
        return cleaned[1:-1]
    return cleaned


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DP_", env_file=".env", extra="ignore")

    pg_dsn: PostgresDsn
    raw_zone_path: Path
    iceberg_warehouse_path: Path
    duckdb_path: Path
    iceberg_catalog_name: str = "data_platform"
    env: Literal["dev", "test", "prod"] = "dev"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]


def reset_settings_cache() -> None:
    get_settings.cache_clear()
