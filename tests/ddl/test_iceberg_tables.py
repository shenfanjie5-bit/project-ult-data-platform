from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa  # type: ignore[import-untyped]
import pytest
from pyiceberg.catalog.memory import InMemoryCatalog

from data_platform.ddl import iceberg_tables
from data_platform.ddl.iceberg_tables import (
    CANONICAL_DIM_INDEX_SPEC,
    CANONICAL_DIM_SECURITY_SPEC,
    CANONICAL_ENTITY_SPEC,
    CANONICAL_FACT_EVENT_SPEC,
    CANONICAL_FACT_FINANCIAL_INDICATOR_SPEC,
    CANONICAL_FACT_PRICE_BAR_SPEC,
    CANONICAL_MART_TABLE_SPECS,
    CANONICAL_STOCK_BASIC_SPEC,
    DECIMAL_TYPE,
    DEFAULT_TABLE_SPECS,
    ENTITY_ALIAS_SPEC,
    TableSpec,
    ensure_tables,
    register_table,
)


@dataclass(frozen=True, slots=True)
class FakeTable:
    identifier: str
    schema: pa.Schema
    properties: dict[str, str]


class FakeCatalog:
    def __init__(self) -> None:
        self.namespaces: list[tuple[str, ...]] = []
        self.tables: dict[str, FakeTable] = {}
        self.create_calls: list[tuple[str, dict[str, Any]]] = []
        self.drop_calls: list[str] = []

    def create_namespace_if_not_exists(self, namespace: tuple[str, ...]) -> None:
        if namespace not in self.namespaces:
            self.namespaces.append(namespace)

    def create_table_if_not_exists(self, identifier: str, **kwargs: Any) -> FakeTable:
        if identifier not in self.tables:
            self.create_calls.append((identifier, kwargs))
            self.tables[identifier] = FakeTable(
                identifier=identifier,
                schema=kwargs["schema"],
                properties=kwargs["properties"],
            )
        return self.tables[identifier]

    def create_table(self, identifier: str, **kwargs: Any) -> FakeTable:
        self.create_calls.append((identifier, kwargs))
        table = FakeTable(
            identifier=identifier,
            schema=kwargs["schema"],
            properties=kwargs["properties"],
        )
        self.tables[identifier] = table
        return table

    def drop_table(self, identifier: str) -> None:
        self.drop_calls.append(identifier)
        self.tables.pop(identifier, None)


def test_canonical_stock_basic_schema_matches_contract() -> None:
    assert CANONICAL_STOCK_BASIC_SPEC.namespace == "canonical"
    assert CANONICAL_STOCK_BASIC_SPEC.name == "stock_basic"
    assert [(field.name, field.type) for field in CANONICAL_STOCK_BASIC_SPEC.schema] == [
        ("ts_code", pa.string()),
        ("symbol", pa.string()),
        ("name", pa.string()),
        ("area", pa.string()),
        ("industry", pa.string()),
        ("market", pa.string()),
        ("list_date", pa.date32()),
        ("is_active", pa.bool_()),
        ("source_run_id", pa.string()),
        ("canonical_loaded_at", pa.timestamp("us")),
    ]


def test_entity_storage_point_schemas_are_minimal() -> None:
    assert CANONICAL_ENTITY_SPEC.namespace == "canonical"
    assert CANONICAL_ENTITY_SPEC.name == "canonical_entity"
    assert CANONICAL_ENTITY_SPEC.schema.names == ["canonical_entity_id", "created_at"]

    assert ENTITY_ALIAS_SPEC.namespace == "canonical"
    assert ENTITY_ALIAS_SPEC.name == "entity_alias"
    assert ENTITY_ALIAS_SPEC.schema.names == [
        "alias",
        "canonical_entity_id",
        "source",
        "created_at",
    ]


def test_canonical_mart_storage_point_schemas_match_contract() -> None:
    assert CANONICAL_DIM_SECURITY_SPEC.namespace == "canonical"
    assert CANONICAL_DIM_SECURITY_SPEC.name == "dim_security"
    assert CANONICAL_DIM_SECURITY_SPEC.schema.names == [
        "ts_code",
        "symbol",
        "name",
        "market",
        "industry",
        "list_date",
        "is_active",
        "area",
        "fullname",
        "exchange",
        "curr_type",
        "list_status",
        "delist_date",
        "setup_date",
        "province",
        "city",
        "reg_capital",
        "employees",
        "main_business",
        "latest_namechange_name",
        "latest_namechange_start_date",
        "latest_namechange_end_date",
        "latest_namechange_ann_date",
        "latest_namechange_reason",
        "source_run_id",
        "raw_loaded_at",
        "canonical_loaded_at",
    ]
    assert CANONICAL_DIM_SECURITY_SPEC.schema.field("reg_capital").type == DECIMAL_TYPE
    assert CANONICAL_DIM_SECURITY_SPEC.schema.field("raw_loaded_at").type == pa.timestamp("us")

    assert CANONICAL_DIM_INDEX_SPEC.namespace == "canonical"
    assert CANONICAL_DIM_INDEX_SPEC.name == "dim_index"
    assert CANONICAL_DIM_INDEX_SPEC.schema.names == [
        "index_code",
        "index_name",
        "index_market",
        "index_category",
        "first_effective_date",
        "latest_effective_date",
        "source_run_id",
        "raw_loaded_at",
        "canonical_loaded_at",
    ]

    assert CANONICAL_FACT_PRICE_BAR_SPEC.name == "fact_price_bar"
    assert CANONICAL_FACT_PRICE_BAR_SPEC.schema.field("trade_date").type == pa.date32()
    assert CANONICAL_FACT_PRICE_BAR_SPEC.schema.field("open").type == DECIMAL_TYPE
    assert CANONICAL_FACT_PRICE_BAR_SPEC.schema.field("adj_factor").type == DECIMAL_TYPE

    assert CANONICAL_FACT_FINANCIAL_INDICATOR_SPEC.name == "fact_financial_indicator"
    assert CANONICAL_FACT_FINANCIAL_INDICATOR_SPEC.schema.field("end_date").type == pa.date32()
    assert CANONICAL_FACT_FINANCIAL_INDICATOR_SPEC.schema.field("roe").type == DECIMAL_TYPE

    assert CANONICAL_FACT_EVENT_SPEC.name == "fact_event"
    assert CANONICAL_FACT_EVENT_SPEC.schema.names == [
        "event_type",
        "ts_code",
        "event_date",
        "title",
        "summary",
        "event_subtype",
        "related_date",
        "reference_url",
        "rec_time",
        "source_run_id",
        "raw_loaded_at",
        "canonical_loaded_at",
    ]


def test_table_specs_reject_raw_namespace_and_queue_fields() -> None:
    with pytest.raises(ValueError, match="raw namespace"):
        TableSpec(namespace="raw", name="bad", schema=pa.schema([("id", pa.string())]))

    with pytest.raises(ValueError, match="producer queue fields"):
        TableSpec(
            namespace="canonical",
            name="bad",
            schema=pa.schema([("submitted_at", pa.timestamp("us"))]),
        )


def test_default_table_specs_do_not_include_queue_fields() -> None:
    forbidden_fields = {"submitted_at", "ingest_seq"}

    for spec in DEFAULT_TABLE_SPECS:
        assert not forbidden_fields.intersection(
            field_name.lower() for field_name in spec.schema.names
        )


def test_register_table_creates_namespace_and_table() -> None:
    catalog = FakeCatalog()
    spec = TableSpec(
        namespace="canonical",
        name="sample",
        schema=pa.schema([("id", pa.string())]),
        properties={"owner": "data-platform"},
    )

    table = register_table(catalog, spec)  # type: ignore[arg-type]

    assert getattr(table, "identifier") == "canonical.sample"
    assert catalog.namespaces == [("canonical",)]
    assert catalog.create_calls == [
        (
            "canonical.sample",
            {
                "schema": spec.schema,
                "properties": {"owner": "data-platform"},
            },
        )
    ]


def test_ensure_tables_is_idempotent() -> None:
    catalog = FakeCatalog()

    first_tables = ensure_tables(catalog, DEFAULT_TABLE_SPECS)  # type: ignore[arg-type]
    second_tables = ensure_tables(catalog, DEFAULT_TABLE_SPECS)  # type: ignore[arg-type]

    assert first_tables == second_tables
    assert sorted(catalog.tables) == [
        "canonical.canonical_entity",
        "canonical.dim_index",
        "canonical.dim_security",
        "canonical.entity_alias",
        "canonical.fact_event",
        "canonical.fact_financial_indicator",
        "canonical.fact_price_bar",
        "canonical.stock_basic",
    ]
    assert [identifier for identifier, _ in catalog.create_calls] == [
        "canonical.stock_basic",
        "canonical.canonical_entity",
        "canonical.entity_alias",
        *[
            f"{spec.namespace}.{spec.name}"
            for spec in CANONICAL_MART_TABLE_SPECS
        ],
    ]


def test_register_table_replace_is_disabled_before_catalog_mutation() -> None:
    catalog = FakeCatalog()
    spec = TableSpec(
        namespace="canonical",
        name="sample",
        schema=pa.schema([("id", pa.string())]),
    )
    register_table(catalog, spec)  # type: ignore[arg-type]

    with pytest.raises(NotImplementedError, match="replace=True is disabled"):
        register_table(catalog, spec, replace=True)  # type: ignore[arg-type]

    assert catalog.drop_calls == []
    assert [identifier for identifier, _ in catalog.create_calls] == [
        "canonical.sample",
    ]
    assert catalog.tables["canonical.sample"].schema == spec.schema


def test_ensure_tables_rejects_existing_table_schema_drift() -> None:
    catalog = FakeCatalog()
    spec = TableSpec(
        namespace="canonical",
        name="sample",
        schema=pa.schema([("id", pa.string())]),
    )
    catalog.tables["canonical.sample"] = FakeTable(
        identifier="canonical.sample",
        schema=pa.schema([("id", pa.int64())]),
        properties={},
    )

    with pytest.raises(ValueError, match="canonical\\.sample schema drift"):
        ensure_tables(catalog, [spec])  # type: ignore[arg-type]

    assert catalog.create_calls == []


def test_ensure_tables_accepts_real_pyiceberg_string_schema(tmp_path: Path) -> None:
    catalog = InMemoryCatalog("test", warehouse=str(tmp_path / "warehouse"))

    tables = ensure_tables(catalog, [CANONICAL_STOCK_BASIC_SPEC])

    assert len(tables) == 1
    assert catalog.load_table("canonical.stock_basic").schema().as_arrow().names == (
        CANONICAL_STOCK_BASIC_SPEC.schema.names
    )


def test_cli_ensure_uses_project_catalog_and_default_specs(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    fake_catalog = object()
    namespace_calls: list[tuple[object, tuple[str, ...]]] = []
    table_calls: list[tuple[object, tuple[TableSpec, ...]]] = []

    monkeypatch.setattr(iceberg_tables, "load_catalog", lambda: fake_catalog)
    monkeypatch.setattr(
        iceberg_tables,
        "ensure_namespaces",
        lambda catalog, names: namespace_calls.append((catalog, tuple(names))),
    )
    monkeypatch.setattr(
        iceberg_tables,
        "ensure_tables",
        lambda catalog, specs: table_calls.append((catalog, tuple(specs))),
    )

    assert iceberg_tables.main(["--ensure"]) == 0

    assert namespace_calls == [
        (fake_catalog, ("canonical", "formal", "analytical")),
    ]
    assert table_calls == [(fake_catalog, DEFAULT_TABLE_SPECS)]
    assert "canonical.stock_basic" in capsys.readouterr().out
