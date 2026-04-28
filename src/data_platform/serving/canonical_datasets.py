"""Provider-neutral canonical dataset to Iceberg table mapping."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

from data_platform.provider_catalog.registry import CANONICAL_DATASETS


class UnsupportedCanonicalDataset(LookupError):
    """Raised when a canonical dataset has no implemented Iceberg table."""

    def __init__(self, dataset_id: str) -> None:
        self.dataset_id = dataset_id
        super().__init__(f"canonical dataset is not mapped to an Iceberg table: {dataset_id}")


def _split_table_identifier(identifier: str) -> tuple[str, str]:
    if not isinstance(identifier, str):
        msg = f"table identifier must be a string: {identifier!r}"
        raise TypeError(msg)
    parts = tuple(part.strip() for part in identifier.split("."))
    if len(parts) != 2 or any(not part for part in parts):
        msg = f"table identifier must be namespace.table: {identifier!r}"
        raise ValueError(msg)
    return parts


def _table_name_from_identifier(identifier: str) -> str:
    if not isinstance(identifier, str):
        msg = f"table identifier must be a string: {identifier!r}"
        raise TypeError(msg)
    parts = tuple(part.strip() for part in identifier.split("."))
    if len(parts) == 1 and parts[0]:
        return parts[0]
    return _split_table_identifier(identifier)[1]


@dataclass(frozen=True, slots=True)
class CanonicalDatasetTable:
    """Explicit storage mapping for one provider-neutral canonical dataset."""

    dataset_id: str
    table_identifier: str

    def __post_init__(self) -> None:
        if self.dataset_id not in CANONICAL_DATASETS:
            msg = f"unknown canonical dataset: {self.dataset_id!r}"
            raise ValueError(msg)
        namespace, table_name = _split_table_identifier(self.table_identifier)
        object.__setattr__(self, "table_identifier", f"{namespace}.{table_name}")

    @property
    def namespace(self) -> str:
        return self.table_identifier.split(".", maxsplit=1)[0]

    @property
    def table_name(self) -> str:
        return self.table_identifier.split(".", maxsplit=1)[1]


CANONICAL_DATASET_TABLE_MAPPINGS: Final[tuple[CanonicalDatasetTable, ...]] = (
    CanonicalDatasetTable("security_master", "canonical.dim_security"),
    CanonicalDatasetTable("security_profile", "canonical.dim_security"),
    CanonicalDatasetTable("price_bar", "canonical.fact_price_bar"),
    CanonicalDatasetTable("adjustment_factor", "canonical.fact_price_bar"),
    CanonicalDatasetTable("market_daily_feature", "canonical.fact_market_daily_feature"),
    CanonicalDatasetTable("index_master", "canonical.dim_index"),
    CanonicalDatasetTable("index_price_bar", "canonical.fact_index_price_bar"),
    CanonicalDatasetTable("event_timeline", "canonical.fact_event"),
    CanonicalDatasetTable("financial_indicator", "canonical.fact_financial_indicator"),
    CanonicalDatasetTable("financial_forecast_event", "canonical.fact_forecast_event"),
)

_DATASET_TO_TABLE: Final[dict[str, CanonicalDatasetTable]] = {
    mapping.dataset_id: mapping for mapping in CANONICAL_DATASET_TABLE_MAPPINGS
}
_TABLE_TO_DATASETS: Final[dict[str, tuple[str, ...]]] = {}
for _mapping in CANONICAL_DATASET_TABLE_MAPPINGS:
    _TABLE_TO_DATASETS[_mapping.table_name] = (
        *_TABLE_TO_DATASETS.get(_mapping.table_name, ()),
        _mapping.dataset_id,
    )


def canonical_table_for_dataset(dataset_id: str) -> str:
    """Return the physical canonical Iceberg table name for a dataset id."""

    return canonical_table_mapping_for_dataset(dataset_id).table_name


def canonical_table_identifier_for_dataset(dataset_id: str) -> str:
    """Return the qualified canonical Iceberg table identifier for a dataset id."""

    return canonical_table_mapping_for_dataset(dataset_id).table_identifier


def canonical_table_mapping_for_dataset(dataset_id: str) -> CanonicalDatasetTable:
    """Return the explicit table mapping for a provider-neutral dataset id."""

    try:
        return _DATASET_TO_TABLE[dataset_id]
    except KeyError as exc:
        raise UnsupportedCanonicalDataset(dataset_id) from exc


def canonical_datasets_for_table(table: str) -> tuple[str, ...]:
    """Return provider-neutral dataset ids served from a physical table."""

    table_name = _table_name_from_identifier(table)
    return _TABLE_TO_DATASETS.get(table_name, ())


def canonical_dataset_for_table(table: str) -> str:
    """Return the primary provider-neutral dataset id for a physical table."""

    dataset_ids = canonical_datasets_for_table(table)
    if not dataset_ids:
        msg = f"canonical table is not mapped to a dataset id: {table}"
        raise LookupError(msg)
    return dataset_ids[0]


def canonical_mart_table_names() -> frozenset[str]:
    """Return implemented canonical mart table names, excluding stock/basic entity stores."""

    return frozenset(
        mapping.table_name for mapping in CANONICAL_DATASET_TABLE_MAPPINGS
    )


__all__ = [
    "CANONICAL_DATASET_TABLE_MAPPINGS",
    "CanonicalDatasetTable",
    "UnsupportedCanonicalDataset",
    "canonical_dataset_for_table",
    "canonical_datasets_for_table",
    "canonical_mart_table_names",
    "canonical_table_for_dataset",
    "canonical_table_identifier_for_dataset",
    "canonical_table_mapping_for_dataset",
]
