from __future__ import annotations

import importlib.util
from collections.abc import Generator
from pathlib import Path

import pytest
from sqlalchemy.exc import SQLAlchemyError

from data_platform.config import reset_settings_cache
from data_platform.serving import catalog as catalog_module
from data_platform.serving.catalog import (
    DEFAULT_NAMESPACES,
    CatalogConnectError,
    ensure_namespaces,
    load_catalog,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DP_ENV_KEYS = [
    "DP_PG_DSN",
    "DP_RAW_ZONE_PATH",
    "DP_ICEBERG_WAREHOUSE_PATH",
    "DP_DUCKDB_PATH",
    "DP_ICEBERG_CATALOG_NAME",
    "DP_ENV",
]


class FakeCatalog:
    def __init__(self) -> None:
        self.namespaces: list[str] = []
        self.create_calls: list[str] = []

    def create_namespace_if_not_exists(self, namespace: str) -> None:
        self.create_calls.append(namespace)
        if namespace not in self.namespaces:
            self.namespaces.append(namespace)

    def list_namespaces(self) -> list[tuple[str, ...]]:
        return [(namespace,) for namespace in self.namespaces]


@pytest.fixture(autouse=True)
def isolated_settings_cache(monkeypatch: pytest.MonkeyPatch) -> Generator[None]:
    reset_settings_cache()
    for key in DP_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    yield
    reset_settings_cache()


def set_required_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("DP_PG_DSN", "postgresql://user:pass@localhost/data_platform")
    monkeypatch.setenv("DP_RAW_ZONE_PATH", str(tmp_path / "raw"))
    monkeypatch.setenv("DP_ICEBERG_WAREHOUSE_PATH", str(tmp_path / "warehouse"))
    monkeypatch.setenv("DP_DUCKDB_PATH", str(tmp_path / "duckdb" / "data_platform.duckdb"))


def test_ensure_namespaces_is_idempotent_and_does_not_create_raw() -> None:
    catalog = FakeCatalog()

    ensure_namespaces(catalog, DEFAULT_NAMESPACES)  # type: ignore[arg-type]
    ensure_namespaces(catalog, DEFAULT_NAMESPACES)  # type: ignore[arg-type]

    assert catalog.list_namespaces() == [("canonical",), ("formal",), ("analytical",)]
    assert "raw" not in [namespace[0] for namespace in catalog.list_namespaces()]
    assert catalog.create_calls == [
        "canonical",
        "formal",
        "analytical",
        "canonical",
        "formal",
        "analytical",
    ]


def test_ensure_namespaces_rejects_raw_namespace() -> None:
    catalog = FakeCatalog()

    with pytest.raises(ValueError, match="raw namespace"):
        ensure_namespaces(catalog, ["raw"])  # type: ignore[arg-type]

    assert catalog.list_namespaces() == []


def test_load_catalog_uses_configured_name_uri_and_warehouse(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    set_required_env(monkeypatch, tmp_path)
    monkeypatch.setenv("DP_ICEBERG_CATALOG_NAME", "configured_catalog")
    calls: list[tuple[str, dict[str, str]]] = []

    class CapturingCatalog:
        def __init__(self, name: str, **properties: str) -> None:
            self.name = name
            self.properties = properties
            calls.append((name, properties))

    monkeypatch.setattr(catalog_module, "SQL_CATALOG_CLASS", CapturingCatalog)

    catalog = load_catalog()

    assert isinstance(catalog, CapturingCatalog)
    assert calls == [
        (
            "configured_catalog",
            {
                "uri": "postgresql+psycopg://user:pass@localhost/data_platform",
                "warehouse": str(tmp_path / "warehouse"),
                "pool_pre_ping": "true",
                "init_catalog_tables": "true",
            },
        )
    ]


def test_load_catalog_name_argument_overrides_settings(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    set_required_env(monkeypatch, tmp_path)
    monkeypatch.setenv("DP_ICEBERG_CATALOG_NAME", "configured_catalog")
    calls: list[str] = []

    class CapturingCatalog:
        def __init__(self, name: str, **properties: str) -> None:
            calls.append(name)

    monkeypatch.setattr(catalog_module, "SQL_CATALOG_CLASS", CapturingCatalog)

    load_catalog("override_catalog")

    assert calls == ["override_catalog"]


def test_load_catalog_wraps_sqlalchemy_connection_errors(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    set_required_env(monkeypatch, tmp_path)

    class BrokenCatalog:
        def __init__(self, name: str, **properties: str) -> None:
            raise SQLAlchemyError("database unavailable")

    monkeypatch.setattr(catalog_module, "SQL_CATALOG_CLASS", BrokenCatalog)

    with pytest.raises(CatalogConnectError) as exc_info:
        load_catalog()

    assert "database unavailable" in exc_info.value.detail


def test_sqlalchemy_postgres_uri_supports_plain_and_jdbc_dsn() -> None:
    assert (
        catalog_module._sqlalchemy_postgres_uri("postgresql://user:pass@host:5432/db")
        == "postgresql+psycopg://user:pass@host:5432/db"
    )
    assert (
        catalog_module._sqlalchemy_postgres_uri("postgres://user:pass@host:5432/db")
        == "postgresql+psycopg://user:pass@host:5432/db"
    )
    assert (
        catalog_module._sqlalchemy_postgres_uri("jdbc:postgresql://user:pass@host:5432/db")
        == "postgresql+psycopg://user:pass@host:5432/db"
    )
    assert (
        catalog_module._sqlalchemy_postgres_uri("postgresql+psycopg://user:pass@host:5432/db")
        == "postgresql+psycopg://user:pass@host:5432/db"
    )


def test_namespace_sort_key_keeps_default_namespaces_in_acceptance_order() -> None:
    unordered = [("canonical",), ("analytical",), ("raw",), ("formal",)]

    assert sorted(unordered, key=catalog_module._namespace_sort_key) == [
        ("canonical",),
        ("formal",),
        ("analytical",),
        ("raw",),
    ]


def test_init_iceberg_catalog_cli_returns_zero_on_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_init_script()
    fake_catalog = object()
    calls: list[tuple[object, tuple[str, ...]]] = []

    monkeypatch.setattr(module, "load_catalog", lambda: fake_catalog)
    monkeypatch.setattr(
        module,
        "ensure_namespaces",
        lambda catalog, names: calls.append((catalog, tuple(names))),
    )

    assert module.main([]) == 0
    assert calls == [(fake_catalog, DEFAULT_NAMESPACES)]


def test_init_iceberg_catalog_cli_returns_one_on_failure(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = load_init_script()

    def fail_to_load() -> object:
        raise CatalogConnectError("database unavailable")

    monkeypatch.setattr(module, "load_catalog", fail_to_load)

    assert module.main([]) == 1
    assert "failed to initialize Iceberg catalog" in capsys.readouterr().err


def load_init_script() -> object:
    script_path = PROJECT_ROOT / "scripts" / "init_iceberg_catalog.py"
    spec = importlib.util.spec_from_file_location("init_iceberg_catalog", script_path)
    if spec is None or spec.loader is None:
        raise AssertionError("failed to load init_iceberg_catalog.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
