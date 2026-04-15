"""PyIceberg SQL catalog wiring for Canonical/Formal/Analytical zones."""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Final

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.typedef import Identifier
from sqlalchemy.exc import SQLAlchemyError

from data_platform.config import get_settings


DEFAULT_NAMESPACES: Final[tuple[str, str, str]] = ("canonical", "formal", "analytical")
DEFAULT_NAMESPACE_IDENTIFIERS: Final[tuple[Identifier, ...]] = tuple(
    (namespace,) for namespace in DEFAULT_NAMESPACES
)
CATALOG_TABLE_PREFIX: Final[str] = "iceberg_"
RAW_NAMESPACE: Final[str] = "raw"


class CatalogConnectError(RuntimeError):
    """Raised when the configured Iceberg SQL catalog cannot be reached."""

    def __init__(self, detail: str) -> None:
        self.detail = detail
        super().__init__(f"failed to connect to Iceberg SQL catalog: {detail}")


class DataPlatformSqlCatalog(SqlCatalog):
    """Project catalog with stable namespace ordering for runbook checks."""

    def list_namespaces(self, namespace: str | Identifier = ()) -> list[Identifier]:
        namespaces = super().list_namespaces(namespace)
        return sorted(namespaces, key=_namespace_sort_key)


SQL_CATALOG_CLASS: type[SqlCatalog] = DataPlatformSqlCatalog


def load_catalog(name: str | None = None) -> SqlCatalog:
    """Load the project PG-backed PyIceberg SQL catalog."""

    settings = get_settings()
    catalog_name = name or settings.iceberg_catalog_name
    properties = {
        "uri": _sqlalchemy_postgres_uri(str(settings.pg_dsn)),
        "warehouse": _warehouse_location(settings.iceberg_warehouse_path),
        "pool_pre_ping": "true",
        "init_catalog_tables": "true",
    }

    try:
        return SQL_CATALOG_CLASS(catalog_name, **properties)
    except SQLAlchemyError as exc:
        raise CatalogConnectError(str(exc)) from exc


def ensure_namespaces(catalog: SqlCatalog, names: Iterable[str]) -> None:
    """Create Iceberg namespaces idempotently, excluding Raw Zone by contract."""

    for namespace in names:
        if namespace == RAW_NAMESPACE:
            msg = "raw namespace must not be created in the Iceberg catalog"
            raise ValueError(msg)
        catalog.create_namespace_if_not_exists(namespace)


def _sqlalchemy_postgres_uri(dsn: str) -> str:
    if dsn.startswith("jdbc:postgresql://"):
        return "postgresql+psycopg://" + dsn.removeprefix("jdbc:postgresql://")
    if dsn.startswith("postgresql://"):
        return "postgresql+psycopg://" + dsn.removeprefix("postgresql://")
    if dsn.startswith("postgres://"):
        return "postgresql+psycopg://" + dsn.removeprefix("postgres://")
    return dsn


def _warehouse_location(path: Path) -> str:
    return str(path.expanduser())


def _namespace_sort_key(namespace: Identifier) -> tuple[int, str]:
    try:
        default_index = DEFAULT_NAMESPACE_IDENTIFIERS.index(namespace)
    except ValueError:
        default_index = len(DEFAULT_NAMESPACE_IDENTIFIERS)
    return (default_index, ".".join(namespace))
