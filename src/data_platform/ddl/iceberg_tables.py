"""Iceberg table registration for Canonical/Formal/Analytical storage points."""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from typing import Final, Sequence

import pyarrow as pa  # type: ignore[import-untyped]
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.table import Table

from data_platform.serving.catalog import (
    DEFAULT_NAMESPACES,
    RAW_NAMESPACE,
    ensure_namespaces,
    load_catalog,
)


FORBIDDEN_SCHEMA_FIELDS: Final[frozenset[str]] = frozenset({"submitted_at", "ingest_seq"})
CANONICAL_NAMESPACE: Final[str] = "canonical"
TIMESTAMP_TYPE: Final[pa.TimestampType] = pa.timestamp("us")


@dataclass(frozen=True, slots=True)
class TableSpec:
    """Declarative Iceberg table storage point."""

    namespace: str
    name: str
    schema: pa.Schema
    partition_by: list[str] | None = None
    properties: dict[str, str] | None = None

    def __post_init__(self) -> None:
        namespace = self.namespace.strip()
        name = self.name.strip()
        if not namespace:
            msg = "Iceberg table namespace must not be empty"
            raise ValueError(msg)
        if not name:
            msg = "Iceberg table name must not be empty"
            raise ValueError(msg)
        if namespace.split(".", maxsplit=1)[0].lower() == RAW_NAMESPACE:
            msg = "raw namespace must not be created in the Iceberg catalog"
            raise ValueError(msg)

        forbidden_fields = sorted(
            FORBIDDEN_SCHEMA_FIELDS.intersection(
                field_name.lower() for field_name in self.schema.names
            )
        )
        if forbidden_fields:
            msg = "Iceberg table schema must not include producer queue fields: "
            raise ValueError(msg + ", ".join(forbidden_fields))

        object.__setattr__(self, "namespace", namespace)
        object.__setattr__(self, "name", name)
        if self.partition_by is not None:
            object.__setattr__(self, "partition_by", list(self.partition_by))
        if self.properties is not None:
            object.__setattr__(self, "properties", dict(self.properties))


CANONICAL_STOCK_BASIC_SPEC: Final[TableSpec] = TableSpec(
    namespace=CANONICAL_NAMESPACE,
    name="stock_basic",
    schema=pa.schema(
        [
            pa.field("ts_code", pa.string()),
            pa.field("symbol", pa.string()),
            pa.field("name", pa.string()),
            pa.field("area", pa.string()),
            pa.field("industry", pa.string()),
            pa.field("market", pa.string()),
            pa.field("list_date", pa.date32()),
            pa.field("is_active", pa.bool_()),
            pa.field("source_run_id", pa.string()),
            pa.field("canonical_loaded_at", TIMESTAMP_TYPE),
        ]
    ),
)

CANONICAL_ENTITY_SPEC: Final[TableSpec] = TableSpec(
    namespace=CANONICAL_NAMESPACE,
    name="canonical_entity",
    schema=pa.schema(
        [
            pa.field("canonical_entity_id", pa.string()),
            pa.field("created_at", TIMESTAMP_TYPE),
        ]
    ),
)

ENTITY_ALIAS_SPEC: Final[TableSpec] = TableSpec(
    namespace=CANONICAL_NAMESPACE,
    name="entity_alias",
    schema=pa.schema(
        [
            pa.field("alias", pa.string()),
            pa.field("canonical_entity_id", pa.string()),
            pa.field("source", pa.string()),
            pa.field("created_at", TIMESTAMP_TYPE),
        ]
    ),
)

DEFAULT_TABLE_SPECS: Final[tuple[TableSpec, ...]] = (
    CANONICAL_STOCK_BASIC_SPEC,
    CANONICAL_ENTITY_SPEC,
    ENTITY_ALIAS_SPEC,
)


def register_table(catalog: SqlCatalog, spec: TableSpec, replace: bool = False) -> Table:
    """Register one Iceberg table, creating its namespace first."""

    ensure_namespaces(catalog, [spec.namespace])
    identifier = _table_identifier(spec)

    if replace:
        try:
            catalog.drop_table(identifier)
        except NoSuchTableError:
            pass
        return _create_table(catalog, identifier, spec)

    return _create_table_if_not_exists(catalog, identifier, spec)


def ensure_tables(catalog: SqlCatalog, specs: Sequence[TableSpec]) -> list[Table]:
    """Idempotently ensure all requested Iceberg tables exist."""

    return [register_table(catalog, spec) for spec in specs]


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Ensure data-platform Iceberg namespaces and table storage points."
    )
    parser.add_argument(
        "--ensure",
        action="store_true",
        help="create missing namespaces and tables",
    )
    args = parser.parse_args(argv)

    if not args.ensure:
        parser.error("only --ensure is supported")

    try:
        catalog = load_catalog()
        ensure_namespaces(catalog, DEFAULT_NAMESPACES)
        ensure_tables(catalog, DEFAULT_TABLE_SPECS)
    except Exception as exc:
        print(f"failed to ensure Iceberg tables: {exc}", file=sys.stderr)
        return 1

    ensured_tables = ", ".join(_table_identifier(spec) for spec in DEFAULT_TABLE_SPECS)
    print(f"ensured Iceberg tables: {ensured_tables}")
    return 0


def _table_identifier(spec: TableSpec) -> str:
    return f"{spec.namespace}.{spec.name}"


def _create_table(catalog: SqlCatalog, identifier: str, spec: TableSpec) -> Table:
    properties = spec.properties or {}
    if spec.partition_by:
        return catalog.create_table(
            identifier,
            schema=spec.schema,
            partition_spec=_identity_partition_spec(spec.schema, spec.partition_by),
            properties=properties,
        )
    return catalog.create_table(identifier, schema=spec.schema, properties=properties)


def _create_table_if_not_exists(catalog: SqlCatalog, identifier: str, spec: TableSpec) -> Table:
    properties = spec.properties or {}
    if spec.partition_by:
        return catalog.create_table_if_not_exists(
            identifier,
            schema=spec.schema,
            partition_spec=_identity_partition_spec(spec.schema, spec.partition_by),
            properties=properties,
        )
    return catalog.create_table_if_not_exists(identifier, schema=spec.schema, properties=properties)


def _identity_partition_spec(schema: pa.Schema, partition_by: Sequence[str]) -> PartitionSpec:
    from pyiceberg.io.pyarrow import pyarrow_to_schema
    from pyiceberg.partitioning import PartitionField
    from pyiceberg.transforms import IdentityTransform

    iceberg_schema = pyarrow_to_schema(schema)
    field_ids_by_name = {field.name: field.field_id for field in iceberg_schema.fields}
    unknown_fields = [
        field_name for field_name in partition_by if field_name not in field_ids_by_name
    ]
    if unknown_fields:
        msg = "partition fields are not present in the table schema: "
        raise ValueError(msg + ", ".join(unknown_fields))

    return PartitionSpec(
        *[
            PartitionField(
                source_id=field_ids_by_name[field_name],
                field_id=1000 + index,
                transform=IdentityTransform(),
                name=field_name,
            )
            for index, field_name in enumerate(partition_by)
        ]
    )


__all__ = [
    "CANONICAL_ENTITY_SPEC",
    "CANONICAL_STOCK_BASIC_SPEC",
    "DEFAULT_TABLE_SPECS",
    "ENTITY_ALIAS_SPEC",
    "TableSpec",
    "ensure_tables",
    "register_table",
]


if __name__ == "__main__":
    raise SystemExit(main())
