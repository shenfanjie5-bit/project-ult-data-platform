from __future__ import annotations

import csv
from collections import Counter
from pathlib import Path

import pytest

from data_platform.adapters.tushare.assets import TUSHARE_ASSETS
from data_platform.provider_catalog import (
    AmbiguousProviderInterface,
    CANONICAL_DATASETS,
    PROVIDER_MAPPINGS,
    PROMOTION_CANDIDATE_MAPPINGS,
    RECONCILIATION_REQUIRED_METRICS,
    TUSHARE_INTERFACE_REGISTRY,
    catalog_summary,
    load_tushare_provider_catalog,
    mapping_for_provider_interface,
    mapping_for_source_interface_id,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CATALOG_PATH = (
    PROJECT_ROOT
    / "src"
    / "data_platform"
    / "provider_catalog"
    / "tushare_available_interfaces.csv"
)
FORBIDDEN_CATALOG_FIELDS = {
    "folder_path",
    "dataset_path",
    "latest_file",
    "token",
    "dsn",
    "DP_TUSHARE_TOKEN",
    "DP_PG_DSN",
}


def test_tushare_provider_catalog_is_normalized_and_available() -> None:
    catalog = load_tushare_provider_catalog()
    doc_apis = [item.doc_api for item in catalog]
    interface_ids = [item.source_interface_id for item in catalog]

    assert len(catalog) == 138
    assert len(interface_ids) == len(set(interface_ids))
    duplicate_doc_apis = sorted(
        doc_api for doc_api, count in Counter(doc_apis).items() if count > 1
    )
    assert duplicate_doc_apis == ["trade_cal"]
    assert {"trade_cal_stock", "trade_cal_futures"}.issubset(set(interface_ids))
    assert {item.provider for item in catalog} == {"tushare"}
    assert {item.access_status for item in catalog} == {"available"}
    assert {"daily", "stock_basic", "trade_cal", "margin", "index_dailybasic"}.issubset(
        set(doc_apis)
    )
    assert "top10_floatholders" not in set(doc_apis)


def test_committed_catalog_does_not_persist_local_paths_or_secrets() -> None:
    with CATALOG_PATH.open("r", encoding="utf-8", newline="") as file_obj:
        reader = csv.DictReader(file_obj)
        fieldnames = set(reader.fieldnames or ())
        rows = list(reader)

    assert fieldnames.isdisjoint(FORBIDDEN_CATALOG_FIELDS)
    serialized = "\n".join(",".join(row.values()) for row in rows)
    assert "/Volumes/" not in serialized
    assert "/Users/" not in serialized
    assert "DP_TUSHARE_TOKEN" not in serialized
    assert "DP_PG_DSN" not in serialized


def test_existing_typed_tushare_assets_have_mapping_or_explicit_legacy_status() -> None:
    mapped_doc_apis = {(mapping.provider, mapping.doc_api): mapping for mapping in PROVIDER_MAPPINGS}
    catalog_interface_ids = {
        (item.provider, item.source_interface_id) for item in load_tushare_provider_catalog()
    }

    missing = []
    for asset in TUSHARE_ASSETS:
        mapping = mapped_doc_apis.get(("tushare", asset.dataset))
        if mapping is None:
            missing.append(asset.dataset)
            continue
        assert mapping.status in {"promoted", "legacy_typed_not_in_catalog"}
        assert mapping.canonical_dataset in CANONICAL_DATASETS
        if mapping.status != "legacy_typed_not_in_catalog":
            assert (mapping.provider, mapping.source_interface_id) in catalog_interface_ids

    assert missing == []


def test_promoted_and_candidate_mappings_are_canonical_contract_complete() -> None:
    for mapping in (*PROVIDER_MAPPINGS, *PROMOTION_CANDIDATE_MAPPINGS):
        canonical = CANONICAL_DATASETS[mapping.canonical_dataset]
        assert mapping.source_primary_key
        assert mapping.field_mapping
        assert mapping.unit_policy
        assert mapping.date_policy
        assert mapping.adjustment_policy
        assert mapping.update_policy
        assert mapping.coverage
        assert mapping.null_policy
        assert canonical.primary_key
        assert canonical.fields
        assert canonical.date_policy
        assert canonical.adjustment_policy
        assert canonical.update_policy
        assert canonical.coverage
        assert canonical.entity_scope
        assert canonical.extension_policy


def test_generic_unpromoted_interfaces_do_not_gain_business_mapping() -> None:
    assert mapping_for_provider_interface("tushare", "etf_basic") is None
    assert mapping_for_provider_interface("tushare", "fund_nav") is None
    assert mapping_for_provider_interface("tushare", "repo_daily") is None
    assert mapping_for_provider_interface("tushare", "daily").canonical_dataset == "price_bar"  # type: ignore[union-attr]

    with pytest.raises(AmbiguousProviderInterface):
        mapping_for_provider_interface("tushare", "trade_cal")

    assert (
        mapping_for_source_interface_id("tushare", "trade_cal_stock").source_interface_id  # type: ignore[union-attr]
        == "trade_cal_stock"
    )
    assert mapping_for_source_interface_id("tushare", "trade_cal_futures") is None
    assert (
        mapping_for_provider_interface(
            "tushare",
            "trade_cal",
            source_interface_id="trade_cal_stock",
        ).canonical_dataset  # type: ignore[union-attr]
        == "trading_calendar"
    )


def test_tushare_interface_registry_keeps_inventory_out_of_production_fetch() -> None:
    catalog = load_tushare_provider_catalog()
    catalog_interface_ids = {item.source_interface_id for item in catalog}
    typed_raw_datasets = {asset.dataset for asset in TUSHARE_ASSETS}
    production_entries = [
        entry
        for entry in TUSHARE_INTERFACE_REGISTRY.values()
        if entry.production_selectable
    ]
    inventory_only_catalog_entries = [
        entry
        for entry in TUSHARE_INTERFACE_REGISTRY.values()
        if entry.source_interface_id in catalog_interface_ids and not entry.production_selectable
    ]

    assert len(TUSHARE_INTERFACE_REGISTRY) == 148
    assert {entry.source_interface_id for entry in TUSHARE_INTERFACE_REGISTRY.values()} == set(
        TUSHARE_INTERFACE_REGISTRY
    )
    assert len(production_entries) == len(TUSHARE_ASSETS) == 28
    assert {entry.raw_dataset for entry in production_entries} == typed_raw_datasets
    assert all(entry.enabled for entry in production_entries)
    assert all(entry.fetch_support == "typed" for entry in production_entries)
    assert all(entry.dbt_support for entry in production_entries)
    assert all(not entry.enabled for entry in inventory_only_catalog_entries)
    assert all(entry.fetch_support == "inventory_only" for entry in inventory_only_catalog_entries)
    assert all(not entry.dbt_support for entry in inventory_only_catalog_entries)
    assert all(not entry.production_selectable for entry in inventory_only_catalog_entries)


def test_tushare_interface_registry_distinguishes_stock_and_futures_trade_cal() -> None:
    stock_entry = TUSHARE_INTERFACE_REGISTRY["trade_cal_stock"]
    futures_entry = TUSHARE_INTERFACE_REGISTRY["trade_cal_futures"]

    assert stock_entry.doc_api == "trade_cal"
    assert stock_entry.raw_dataset == "trade_cal"
    assert stock_entry.promotion_status == "promoted"
    assert stock_entry.production_selectable is True
    assert stock_entry.partition_key == ("cal_date",)

    assert futures_entry.doc_api == "trade_cal"
    assert futures_entry.raw_dataset is None
    assert futures_entry.promotion_status == "inventory_only"
    assert futures_entry.production_selectable is False
    assert futures_entry.fetch_support == "inventory_only"


def test_catalog_summary_supports_dual_provider_readiness_evidence() -> None:
    summary = catalog_summary()

    assert summary["provider"] == "tushare"
    assert summary["provider_interface_count"] == 138
    assert summary["promoted_mapping_count"] == len(PROVIDER_MAPPINGS)
    assert summary["promotion_candidate_count"] == len(PROMOTION_CANDIDATE_MAPPINGS)
    assert summary["canonical_dataset_count"] == len(CANONICAL_DATASETS)
    assert summary["future_provider_targets"] == ["choice", "internal", "wind"]
    assert RECONCILIATION_REQUIRED_METRICS == (
        "row_count_diff",
        "key_coverage_diff",
        "field_value_diff",
        "unit_normalization_diff",
        "date_policy_diff",
    )
