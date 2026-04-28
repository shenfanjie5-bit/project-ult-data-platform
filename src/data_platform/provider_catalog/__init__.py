"""Provider-neutral source catalog and canonical mapping registry."""

from data_platform.provider_catalog.registry import (
    CANONICAL_DATASETS,
    FUTURE_PROVIDER_TARGETS,
    PROVIDER_MAPPINGS,
    PROMOTION_CANDIDATE_MAPPINGS,
    RECONCILIATION_REQUIRED_METRICS,
    CanonicalDataset,
    CanonicalField,
    ProviderDatasetMapping,
    SourceInterface,
    catalog_summary,
    load_tushare_provider_catalog,
    mapping_for_provider_interface,
)

__all__ = [
    "CANONICAL_DATASETS",
    "FUTURE_PROVIDER_TARGETS",
    "PROVIDER_MAPPINGS",
    "PROMOTION_CANDIDATE_MAPPINGS",
    "RECONCILIATION_REQUIRED_METRICS",
    "CanonicalDataset",
    "CanonicalField",
    "ProviderDatasetMapping",
    "SourceInterface",
    "catalog_summary",
    "load_tushare_provider_catalog",
    "mapping_for_provider_interface",
]
