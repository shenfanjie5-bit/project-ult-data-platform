"""Provider catalog and canonical dataset contracts.

The source provider inventory is intentionally separated from canonical business
contracts. Tushare may define provider-specific Raw/staging inputs, but business
consumers must bind to canonical dataset ids declared here.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from importlib import resources
import re
from typing import Final, Literal

ProviderName = Literal["tushare", "wind", "choice", "internal"]
MappingStatus = Literal["promoted", "candidate", "legacy_typed_not_in_catalog"]

_DOC_API_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[a-z][a-z0-9_]*$")
_CANONICAL_DATASET_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[a-z][a-z0-9_]*$")
_PROVIDER_CATALOG_RESOURCE: Final[str] = "tushare_available_interfaces.csv"


def _validate_provider(provider: str) -> None:
    if provider not in {"tushare", "wind", "choice", "internal"}:
        msg = f"unsupported provider: {provider!r}"
        raise ValueError(msg)


def _validate_doc_api(doc_api: str) -> None:
    if not _DOC_API_PATTERN.fullmatch(doc_api):
        msg = f"invalid provider doc_api: {doc_api!r}"
        raise ValueError(msg)


def _validate_canonical_dataset(dataset_id: str) -> None:
    if not _CANONICAL_DATASET_PATTERN.fullmatch(dataset_id):
        msg = f"invalid canonical dataset id: {dataset_id!r}"
        raise ValueError(msg)


def _validate_identifier(identifier: str, label: str) -> None:
    if not _CANONICAL_DATASET_PATTERN.fullmatch(identifier):
        msg = f"invalid {label}: {identifier!r}"
        raise ValueError(msg)


@dataclass(frozen=True, slots=True)
class SourceInterface:
    """One provider-specific source interface from an availability inventory."""

    provider: ProviderName
    source_interface_id: str
    doc_api: str
    label: str
    level1: str
    level2: str
    level3: str
    level4: str
    doc_url: str
    storage_mode: str
    split_by_symbol: bool
    access_status: str
    access_reason: str
    completeness_status: str
    check_confidence: str

    def __post_init__(self) -> None:
        _validate_provider(self.provider)
        _validate_identifier(self.source_interface_id, "source interface id")
        _validate_doc_api(self.doc_api)
        if self.access_status != "available":
            msg = f"provider interface must be available: {self.doc_api}"
            raise ValueError(msg)
        if self.storage_mode not in {"all_csv", "by_symbol"}:
            msg = f"unsupported storage_mode for {self.doc_api}: {self.storage_mode!r}"
            raise ValueError(msg)


@dataclass(frozen=True, slots=True)
class CanonicalField:
    """One provider-neutral field in a canonical dataset contract."""

    name: str
    unit: str
    description: str

    def __post_init__(self) -> None:
        _validate_identifier(self.name, "canonical field")
        if not self.unit:
            msg = f"canonical field {self.name!r} requires a unit policy"
            raise ValueError(msg)
        if not self.description:
            msg = f"canonical field {self.name!r} requires a description"
            raise ValueError(msg)


@dataclass(frozen=True, slots=True)
class CanonicalDataset:
    """Provider-neutral dataset consumed by curated marts and business layers."""

    dataset_id: str
    description: str
    primary_key: tuple[str, ...]
    fields: tuple[CanonicalField, ...]
    date_policy: str
    adjustment_policy: str
    update_policy: str
    coverage: str
    entity_scope: str
    null_policy: str
    extension_policy: str

    def __post_init__(self) -> None:
        _validate_canonical_dataset(self.dataset_id)
        if not self.description:
            msg = f"canonical dataset {self.dataset_id!r} requires a description"
            raise ValueError(msg)
        if not self.primary_key:
            msg = f"canonical dataset {self.dataset_id!r} requires a primary key"
            raise ValueError(msg)
        field_names = {field.name for field in self.fields}
        missing_pk = sorted(set(self.primary_key).difference(field_names))
        if missing_pk:
            msg = f"canonical dataset {self.dataset_id!r} primary key fields missing: "
            raise ValueError(msg + ", ".join(missing_pk))
        required_text = {
            "date_policy": self.date_policy,
            "adjustment_policy": self.adjustment_policy,
            "update_policy": self.update_policy,
            "coverage": self.coverage,
            "entity_scope": self.entity_scope,
            "null_policy": self.null_policy,
            "extension_policy": self.extension_policy,
        }
        for field_name, value in required_text.items():
            if not value:
                msg = f"canonical dataset {self.dataset_id!r} requires {field_name}"
                raise ValueError(msg)


@dataclass(frozen=True, slots=True)
class ProviderDatasetMapping:
    """Provider-specific interface mapping into a canonical dataset."""

    provider: ProviderName
    source_interface_id: str
    doc_api: str
    canonical_dataset: str
    status: MappingStatus
    field_mapping: tuple[tuple[str, str], ...]
    source_primary_key: tuple[str, ...]
    unit_policy: str
    date_policy: str
    adjustment_policy: str
    update_policy: str
    coverage: str
    null_policy: str

    def __post_init__(self) -> None:
        _validate_provider(self.provider)
        _validate_identifier(self.source_interface_id, "source interface id")
        _validate_doc_api(self.doc_api)
        _validate_canonical_dataset(self.canonical_dataset)
        if self.canonical_dataset not in CANONICAL_DATASETS:
            msg = f"unknown canonical dataset for mapping: {self.canonical_dataset!r}"
            raise ValueError(msg)
        if not self.field_mapping:
            msg = f"provider mapping {self.provider}:{self.doc_api} requires field_mapping"
            raise ValueError(msg)
        if not self.source_primary_key:
            msg = f"provider mapping {self.provider}:{self.doc_api} requires source_primary_key"
            raise ValueError(msg)
        for source_field, canonical_field in self.field_mapping:
            _validate_identifier(source_field, "source field")
            _validate_identifier(canonical_field, "canonical field")
        for field_name, value in {
            "unit_policy": self.unit_policy,
            "date_policy": self.date_policy,
            "adjustment_policy": self.adjustment_policy,
            "update_policy": self.update_policy,
            "coverage": self.coverage,
            "null_policy": self.null_policy,
        }.items():
            if not value:
                msg = f"provider mapping {self.provider}:{self.doc_api} requires {field_name}"
                raise ValueError(msg)


def load_tushare_provider_catalog() -> tuple[SourceInterface, ...]:
    """Load the committed provider=tushare availability catalog."""

    package_files = resources.files("data_platform.provider_catalog")
    with package_files.joinpath(_PROVIDER_CATALOG_RESOURCE).open(
        "r",
        encoding="utf-8",
        newline="",
    ) as file_obj:
        rows = tuple(_source_interface_from_row(row) for row in csv.DictReader(file_obj))
    _assert_unique_source_interface_ids(rows)
    return rows


def catalog_summary() -> dict[str, object]:
    """Return a small evidence-friendly catalog summary."""

    catalog = load_tushare_provider_catalog()
    mapped_interfaces = {
        (mapping.provider, mapping.source_interface_id) for mapping in PROVIDER_MAPPINGS
    }
    candidate_interfaces = {
        (mapping.provider, mapping.source_interface_id)
        for mapping in PROMOTION_CANDIDATE_MAPPINGS
    }
    return {
        "provider": "tushare",
        "provider_interface_count": len(catalog),
        "promoted_mapping_count": len(PROVIDER_MAPPINGS),
        "promotion_candidate_count": len(PROMOTION_CANDIDATE_MAPPINGS),
        "generic_unpromoted_count": len(
            [
                item
                for item in catalog
                if (item.provider, item.source_interface_id) not in mapped_interfaces
                and (item.provider, item.source_interface_id) not in candidate_interfaces
            ]
        ),
        "canonical_dataset_count": len(CANONICAL_DATASETS),
        "future_provider_targets": sorted(FUTURE_PROVIDER_TARGETS),
    }


def mapping_for_provider_interface(
    provider: str,
    doc_api: str,
) -> ProviderDatasetMapping | None:
    """Return the canonical mapping for one source interface, if promoted."""

    for mapping in (*PROVIDER_MAPPINGS, *PROMOTION_CANDIDATE_MAPPINGS):
        if mapping.provider == provider and mapping.doc_api == doc_api:
            return mapping
    return None


def _source_interface_from_row(row: dict[str, str]) -> SourceInterface:
    return SourceInterface(
        provider="tushare",
        source_interface_id=row["source_interface_id"].strip(),
        doc_api=row["doc_api"].strip(),
        label=row["label"].strip(),
        level1=row["level1"].strip(),
        level2=row["level2"].strip(),
        level3=row["level3"].strip(),
        level4=row["level4"].strip(),
        doc_url=row["doc_url"].strip(),
        storage_mode=row["storage_mode"].strip(),
        split_by_symbol=row["split_by_symbol"].strip().lower() == "true",
        access_status=row["access_status"].strip(),
        access_reason=row["access_reason"].strip(),
        completeness_status=row["completeness_status"].strip(),
        check_confidence=row["check_confidence"].strip(),
    )


def _assert_unique_source_interface_ids(rows: tuple[SourceInterface, ...]) -> None:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for row in rows:
        key = f"{row.provider}:{row.source_interface_id}"
        if key in seen:
            duplicates.add(key)
        seen.add(key)
    if duplicates:
        msg = "duplicate provider source_interface_id values: "
        raise ValueError(msg + ", ".join(sorted(duplicates)))


def _field(name: str, unit: str, description: str) -> CanonicalField:
    return CanonicalField(name=name, unit=unit, description=description)


def _dataset(
    dataset_id: str,
    *,
    description: str,
    primary_key: tuple[str, ...],
    fields: tuple[CanonicalField, ...],
    date_policy: str,
    adjustment_policy: str,
    update_policy: str,
    coverage: str,
    entity_scope: str,
    null_policy: str = "Missing provider fields stay null with lineage; no provider default may imply business truth.",
    extension_policy: str = "Only additive nullable fields are allowed without a canonical contract version bump.",
) -> CanonicalDataset:
    return CanonicalDataset(
        dataset_id=dataset_id,
        description=description,
        primary_key=primary_key,
        fields=fields,
        date_policy=date_policy,
        adjustment_policy=adjustment_policy,
        update_policy=update_policy,
        coverage=coverage,
        entity_scope=entity_scope,
        null_policy=null_policy,
        extension_policy=extension_policy,
    )


def _mapping(
    doc_api: str,
    canonical_dataset: str,
    *,
    source_interface_id: str | None = None,
    status: MappingStatus = "promoted",
    field_mapping: tuple[tuple[str, str], ...],
    source_primary_key: tuple[str, ...],
    unit_policy: str,
    date_policy: str,
    adjustment_policy: str,
    update_policy: str,
    coverage: str,
    null_policy: str = "Provider nulls pass through as canonical nulls with source lineage.",
) -> ProviderDatasetMapping:
    return ProviderDatasetMapping(
        provider="tushare",
        source_interface_id=source_interface_id or doc_api,
        doc_api=doc_api,
        canonical_dataset=canonical_dataset,
        status=status,
        field_mapping=field_mapping,
        source_primary_key=source_primary_key,
        unit_policy=unit_policy,
        date_policy=date_policy,
        adjustment_policy=adjustment_policy,
        update_policy=update_policy,
        coverage=coverage,
        null_policy=null_policy,
    )


CANONICAL_DATASETS: Final[dict[str, CanonicalDataset]] = {
    dataset.dataset_id: dataset
    for dataset in (
        _dataset(
            "security_master",
            description="Listed security identity and lifecycle master data.",
            primary_key=("security_id",),
            fields=(
                _field("security_id", "canonical identifier", "Provider-neutral security id."),
                _field("symbol", "provider code", "Exchange-local symbol."),
                _field("display_name", "text", "Current security display name."),
                _field("market", "enum", "Listing market."),
                _field("industry", "text", "Provider-normalized industry label."),
                _field("list_date", "date", "Listing date."),
            ),
            date_policy="static with late corrections keyed by security_id",
            adjustment_policy="not applicable",
            update_policy="static reference data with daily correction scan",
            coverage="CN_A listed securities for Lite MVP",
            entity_scope="security",
        ),
        _dataset(
            "trading_calendar",
            description="Exchange trading-day calendar.",
            primary_key=("market", "trade_date"),
            fields=(
                _field("market", "enum", "Exchange or market calendar id."),
                _field("trade_date", "date", "Calendar date."),
                _field("is_open", "boolean", "Whether the market is open."),
                _field("previous_trade_date", "date", "Previous open trade date."),
            ),
            date_policy="calendar date, not event date",
            adjustment_policy="not applicable",
            update_policy="static calendar with exchange corrections",
            coverage="CN_A exchange calendar",
            entity_scope="market",
        ),
        _dataset(
            "price_bar",
            description="Provider-neutral OHLCV bars.",
            primary_key=("security_id", "trade_date", "frequency"),
            fields=(
                _field("security_id", "canonical identifier", "Security id."),
                _field("trade_date", "date", "Trading day."),
                _field("frequency", "enum", "daily, weekly, or monthly."),
                _field("open", "price", "Open price."),
                _field("high", "price", "High price."),
                _field("low", "price", "Low price."),
                _field("close", "price", "Close price."),
                _field("volume", "shares/lots", "Trading volume."),
                _field("amount", "currency", "Trading amount."),
            ),
            date_policy="trade_date",
            adjustment_policy="raw price plus separate adjustment factor mapping",
            update_policy="daily/weekly/monthly partition refresh",
            coverage="CN_A securities",
            entity_scope="security",
        ),
        _dataset(
            "adjustment_factor",
            description="Split/dividend adjustment factors for securities.",
            primary_key=("security_id", "trade_date"),
            fields=(
                _field("security_id", "canonical identifier", "Security id."),
                _field("trade_date", "date", "Trading day."),
                _field("adjustment_factor", "factor", "Cumulative adjustment factor."),
            ),
            date_policy="trade_date",
            adjustment_policy="factor-only",
            update_policy="daily refresh with late correction support",
            coverage="CN_A securities",
            entity_scope="security",
        ),
        _dataset(
            "market_daily_feature",
            description="Daily valuation, liquidity, limit, and fund-flow features.",
            primary_key=("security_id", "trade_date"),
            fields=(
                _field("security_id", "canonical identifier", "Security id."),
                _field("trade_date", "date", "Trading day."),
                _field("close", "price", "Close price context."),
                _field("turnover_rate", "percent", "Turnover rate."),
                _field("pe_ttm", "ratio", "Trailing PE."),
                _field("pb", "ratio", "PB ratio."),
                _field("total_market_value", "currency", "Total market capitalization."),
                _field("net_moneyflow_amount", "currency", "Net fund flow amount."),
            ),
            date_policy="trade_date",
            adjustment_policy="market features use raw same-day provider values",
            update_policy="daily refresh with late correction support",
            coverage="CN_A securities",
            entity_scope="security",
        ),
        _dataset(
            "index_master",
            description="Index identity and classification master data.",
            primary_key=("index_id",),
            fields=(
                _field("index_id", "canonical identifier", "Index id."),
                _field("display_name", "text", "Index name."),
                _field("market", "enum", "Index market."),
                _field("category", "enum", "Index category."),
            ),
            date_policy="static with late corrections keyed by index_id",
            adjustment_policy="not applicable",
            update_policy="static reference data with daily correction scan",
            coverage="CN_A indices",
            entity_scope="index",
        ),
        _dataset(
            "index_price_bar",
            description="Provider-neutral index OHLCV bars.",
            primary_key=("index_id", "trade_date", "frequency"),
            fields=(
                _field("index_id", "canonical identifier", "Index id."),
                _field("trade_date", "date", "Trading day."),
                _field("frequency", "enum", "daily, weekly, or monthly."),
                _field("close", "index points", "Close level."),
                _field("amount", "currency", "Trading amount."),
            ),
            date_policy="trade_date",
            adjustment_policy="raw index level",
            update_policy="daily/weekly/monthly partition refresh",
            coverage="CN_A and reference indices",
            entity_scope="index",
        ),
        _dataset(
            "index_membership",
            description="Index constituent membership and weights.",
            primary_key=("index_id", "security_id", "effective_date"),
            fields=(
                _field("index_id", "canonical identifier", "Index id."),
                _field("security_id", "canonical identifier", "Security id."),
                _field("effective_date", "date", "Membership effective date."),
                _field("weight", "percent", "Constituent weight."),
            ),
            date_policy="effective date or provider trade date",
            adjustment_policy="not applicable",
            update_policy="monthly/late-arriving membership refresh",
            coverage="CN_A index constituents",
            entity_scope="index/security",
        ),
        _dataset(
            "industry_classification",
            description="Provider-neutral industry hierarchy.",
            primary_key=("industry_id",),
            fields=(
                _field("industry_id", "canonical identifier", "Industry id."),
                _field("display_name", "text", "Industry display name."),
                _field("parent_id", "canonical identifier", "Parent industry id."),
                _field("level", "integer", "Hierarchy level."),
            ),
            date_policy="classification effective period",
            adjustment_policy="not applicable",
            update_policy="static reference data with late corrections",
            coverage="CN_A industry classifications",
            entity_scope="industry",
        ),
        _dataset(
            "security_profile",
            description="Issuer profile and business description.",
            primary_key=("security_id",),
            fields=(
                _field("security_id", "canonical identifier", "Security id."),
                _field("registered_capital", "currency", "Registered capital."),
                _field("employees", "count", "Employee count."),
                _field("business_scope", "text", "Business scope."),
            ),
            date_policy="latest known issuer profile",
            adjustment_policy="not applicable",
            update_policy="static profile with corrections",
            coverage="CN_A issuers",
            entity_scope="security/company",
        ),
        _dataset(
            "event_timeline",
            description="Provider-neutral market, company, and disclosure events.",
            primary_key=("event_type", "entity_id", "event_date", "event_key"),
            fields=(
                _field("event_type", "enum", "Canonical event type."),
                _field("entity_id", "canonical identifier", "Affected entity id."),
                _field("event_date", "date", "Event or announcement date."),
                _field("event_key", "text", "Stable event key within the event date."),
                _field("summary", "text", "Event summary."),
            ),
            date_policy="event date; announcement date retained when distinct",
            adjustment_policy="not applicable",
            update_policy="event-time with late-arriving corrections",
            coverage="CN_A company and market events",
            entity_scope="security/company/market",
        ),
        _dataset(
            "financial_statement",
            description="Provider-neutral financial statements.",
            primary_key=("security_id", "report_period", "statement_type", "version_key"),
            fields=(
                _field("security_id", "canonical identifier", "Security id."),
                _field("report_period", "date", "Financial report period end."),
                _field("statement_type", "enum", "Income, balance sheet, or cash flow."),
                _field("version_key", "text", "Announcement/version key."),
                _field("amount", "currency", "Canonical amount payload value."),
            ),
            date_policy="report period with announcement and final-announcement dates",
            adjustment_policy="reported currency units; no market adjustment",
            update_policy="quarterly with restatement/version retention",
            coverage="CN_A financial reports",
            entity_scope="security/company",
        ),
        _dataset(
            "financial_indicator",
            description="Provider-neutral financial indicator metrics.",
            primary_key=("security_id", "report_period", "version_key"),
            fields=(
                _field("security_id", "canonical identifier", "Security id."),
                _field("report_period", "date", "Financial report period end."),
                _field("version_key", "text", "Announcement/version key."),
                _field("roe", "ratio", "Return on equity."),
                _field("debt_to_assets", "ratio", "Debt to assets."),
            ),
            date_policy="report period with versioned announcement date",
            adjustment_policy="reported ratio units",
            update_policy="quarterly with restatement/version retention",
            coverage="CN_A financial reports",
            entity_scope="security/company",
        ),
        _dataset(
            "financial_forecast_event",
            description="Earnings forecast and express financial events.",
            primary_key=("security_id", "announcement_date", "report_period", "forecast_type"),
            fields=(
                _field("security_id", "canonical identifier", "Security id."),
                _field("announcement_date", "date", "Announcement date."),
                _field("report_period", "date", "Financial report period end."),
                _field("forecast_type", "enum", "Forecast or express event type."),
                _field("summary", "text", "Forecast summary."),
            ),
            date_policy="announcement date plus report period",
            adjustment_policy="reported values; no market adjustment",
            update_policy="event-time with version retention",
            coverage="CN_A financial disclosures",
            entity_scope="security/company",
        ),
        _dataset(
            "market_leverage_daily",
            description="Market-level financing and securities lending summary.",
            primary_key=("market", "trade_date"),
            fields=(
                _field("market", "enum", "Market id."),
                _field("trade_date", "date", "Trading day."),
                _field("financing_balance", "currency", "Financing balance."),
                _field("securities_lending_balance", "currency", "Securities lending balance."),
            ),
            date_policy="trade_date",
            adjustment_policy="reported values; no price adjustment",
            update_policy="daily refresh",
            coverage="CN_A margin market",
            entity_scope="market",
        ),
        _dataset(
            "security_leverage_detail",
            description="Security-level financing and lending detail.",
            primary_key=("security_id", "trade_date"),
            fields=(
                _field("security_id", "canonical identifier", "Security id."),
                _field("trade_date", "date", "Trading day."),
                _field("financing_balance", "currency", "Financing balance."),
                _field("lending_balance", "currency", "Securities lending balance."),
            ),
            date_policy="trade_date",
            adjustment_policy="reported values; no price adjustment",
            update_policy="daily refresh",
            coverage="CN_A margin securities",
            entity_scope="security",
        ),
        _dataset(
            "business_segment_exposure",
            description="Business segment revenue/cost/profit exposure.",
            primary_key=("security_id", "report_period", "segment_name"),
            fields=(
                _field("security_id", "canonical identifier", "Security id."),
                _field("report_period", "date", "Financial report period end."),
                _field("segment_name", "text", "Business segment name."),
                _field("revenue", "currency", "Segment revenue."),
                _field("gross_profit", "currency", "Segment gross profit."),
            ),
            date_policy="report period",
            adjustment_policy="reported currency units",
            update_policy="quarterly or annual late-arriving updates",
            coverage="CN_A issuer business segments",
            entity_scope="security/company/industry",
        ),
    )
}


PROVIDER_MAPPINGS: Final[tuple[ProviderDatasetMapping, ...]] = (
    _mapping(
        "stock_basic",
        "security_master",
        field_mapping=(("ts_code", "security_id"), ("symbol", "symbol"), ("name", "display_name")),
        source_primary_key=("ts_code",),
        unit_policy="identity/text fields only",
        date_policy="static list_date/delist_date lifecycle",
        adjustment_policy="not applicable",
        update_policy="static with corrections",
        coverage="CN_A",
    ),
    _mapping(
        "trade_cal",
        "trading_calendar",
        source_interface_id="trade_cal_stock",
        field_mapping=(("exchange", "market"), ("cal_date", "trade_date"), ("is_open", "is_open")),
        source_primary_key=("cal_date", "exchange"),
        unit_policy="calendar flags",
        date_policy="calendar date",
        adjustment_policy="not applicable",
        update_policy="static calendar refresh",
        coverage="CN_A",
    ),
    *(
        _mapping(
            doc_api,
            "price_bar",
            field_mapping=(("ts_code", "security_id"), ("trade_date", "trade_date"), ("close", "close")),
            source_primary_key=("ts_code", "trade_date"),
            unit_policy="prices in provider currency; volume/amount per provider contract",
            date_policy="trade_date",
            adjustment_policy="raw price; adj_factor is separate",
            update_policy=f"{frequency} refresh",
            coverage="CN_A",
        )
        for doc_api, frequency in (("daily", "daily"), ("weekly", "weekly"), ("monthly", "monthly"))
    ),
    _mapping(
        "adj_factor",
        "adjustment_factor",
        status="legacy_typed_not_in_catalog",
        field_mapping=(("ts_code", "security_id"), ("trade_date", "trade_date"), ("adj_factor", "adjustment_factor")),
        source_primary_key=("ts_code", "trade_date"),
        unit_policy="dimensionless factor",
        date_policy="trade_date",
        adjustment_policy="factor-only",
        update_policy="daily refresh",
        coverage="CN_A",
    ),
    *(
        _mapping(
            doc_api,
            "market_daily_feature",
            field_mapping=(("ts_code", "security_id"), ("trade_date", "trade_date")),
            source_primary_key=("ts_code", "trade_date"),
            unit_policy="provider numeric values normalized in marts",
            date_policy="trade_date",
            adjustment_policy="raw same-day feature values",
            update_policy="daily refresh",
            coverage="CN_A",
        )
        for doc_api in ("daily_basic", "stk_limit", "moneyflow")
    ),
    _mapping(
        "index_basic",
        "index_master",
        field_mapping=(("ts_code", "index_id"), ("name", "display_name"), ("market", "market")),
        source_primary_key=("ts_code",),
        unit_policy="identity/text fields only",
        date_policy="static index metadata",
        adjustment_policy="not applicable",
        update_policy="static with corrections",
        coverage="CN_A indices",
    ),
    _mapping(
        "index_daily",
        "index_price_bar",
        status="legacy_typed_not_in_catalog",
        field_mapping=(("ts_code", "index_id"), ("trade_date", "trade_date"), ("close", "close")),
        source_primary_key=("ts_code", "trade_date"),
        unit_policy="index points and amount",
        date_policy="trade_date",
        adjustment_policy="raw index level",
        update_policy="daily refresh",
        coverage="CN_A indices",
    ),
    *(
        _mapping(
            doc_api,
            "index_membership",
            status=status,
            field_mapping=(("index_code", "index_id"), ("con_code", "security_id")),
            source_primary_key=source_primary_key,
            unit_policy="percent weight where provided",
            date_policy="effective date or provider trade date",
            adjustment_policy="not applicable",
            update_policy="monthly/late-arriving updates",
            coverage="CN_A indices",
        )
        for doc_api, status, source_primary_key in (
            ("index_weight", "legacy_typed_not_in_catalog", ("index_code", "con_code", "trade_date")),
            ("index_member", "legacy_typed_not_in_catalog", ("index_code", "con_code", "in_date")),
        )
    ),
    _mapping(
        "index_classify",
        "industry_classification",
        field_mapping=(("industry_code", "industry_id"), ("industry_name", "display_name"), ("parent_code", "parent_id")),
        source_primary_key=("industry_code",),
        unit_policy="identity/text fields only",
        date_policy="classification effective period",
        adjustment_policy="not applicable",
        update_policy="static with corrections",
        coverage="CN_A industries",
    ),
    *(
        _mapping(
            doc_api,
            "security_profile",
            field_mapping=(("ts_code", "security_id"),),
            source_primary_key=("ts_code",),
            unit_policy="profile values normalized by field",
            date_policy="latest known profile",
            adjustment_policy="not applicable",
            update_policy="static/event correction",
            coverage="CN_A",
        )
        for doc_api in ("stock_company",)
    ),
    *(
        _mapping(
            doc_api,
            "event_timeline",
            status=status,
            field_mapping=(("ts_code", "entity_id"),),
            source_primary_key=source_primary_key,
            unit_policy="event text/date fields",
            date_policy="event date and announcement date retained when distinct",
            adjustment_policy="not applicable",
            update_policy="event-time with late corrections",
            coverage="CN_A",
        )
        for doc_api, status, source_primary_key in (
            ("namechange", "promoted", ("ts_code", "start_date")),
            ("anns", "legacy_typed_not_in_catalog", ("ts_code", "ann_date", "title")),
            ("suspend_d", "legacy_typed_not_in_catalog", ("ts_code", "trade_date")),
            ("dividend", "promoted", ("ts_code", "ann_date", "end_date")),
            ("share_float", "promoted", ("ts_code", "ann_date", "float_date")),
            ("stk_holdernumber", "promoted", ("ts_code", "ann_date", "end_date")),
            ("disclosure_date", "promoted", ("ts_code", "ann_date", "end_date")),
            ("block_trade", "promoted", ("ts_code", "trade_date")),
        )
    ),
    *(
        _mapping(
            doc_api,
            "financial_statement",
            status="legacy_typed_not_in_catalog",
            field_mapping=(("ts_code", "security_id"), ("end_date", "report_period")),
            source_primary_key=("ts_code", "ann_date", "end_date", "report_type", "comp_type", "update_flag"),
            unit_policy="reported currency units",
            date_policy="report period plus announcement dates",
            adjustment_policy="reported values; no market adjustment",
            update_policy="quarterly with version retention",
            coverage="CN_A",
        )
        for doc_api in ("income", "balancesheet", "cashflow")
    ),
    _mapping(
        "fina_indicator",
        "financial_indicator",
        status="legacy_typed_not_in_catalog",
        field_mapping=(("ts_code", "security_id"), ("end_date", "report_period")),
        source_primary_key=("ts_code", "ann_date", "end_date", "report_type", "comp_type", "update_flag"),
        unit_policy="reported ratios and amounts",
        date_policy="report period plus announcement dates",
        adjustment_policy="reported values; no market adjustment",
        update_policy="quarterly with version retention",
        coverage="CN_A",
    ),
    _mapping(
        "forecast",
        "financial_forecast_event",
        field_mapping=(("ts_code", "security_id"), ("ann_date", "announcement_date"), ("end_date", "report_period"), ("type", "forecast_type")),
        source_primary_key=("ts_code", "ann_date", "end_date", "update_flag", "type"),
        unit_policy="reported forecast text and amounts",
        date_policy="announcement date plus report period",
        adjustment_policy="reported values; no market adjustment",
        update_policy="event-time with version retention",
        coverage="CN_A",
    ),
)


PROMOTION_CANDIDATE_MAPPINGS: Final[tuple[ProviderDatasetMapping, ...]] = (
    _mapping(
        "index_dailybasic",
        "index_price_bar",
        status="candidate",
        field_mapping=(("ts_code", "index_id"), ("trade_date", "trade_date")),
        source_primary_key=("ts_code", "trade_date"),
        unit_policy="index valuation and turnover metrics",
        date_policy="trade_date",
        adjustment_policy="raw index metrics",
        update_policy="daily refresh",
        coverage="CN_A indices",
    ),
    _mapping(
        "margin",
        "market_leverage_daily",
        status="candidate",
        field_mapping=(("trade_date", "trade_date"),),
        source_primary_key=("trade_date",),
        unit_policy="reported financing and lending currency amounts",
        date_policy="trade_date",
        adjustment_policy="reported values; no market adjustment",
        update_policy="daily refresh",
        coverage="CN_A margin market",
    ),
    _mapping(
        "margin_detail",
        "security_leverage_detail",
        status="candidate",
        field_mapping=(("ts_code", "security_id"), ("trade_date", "trade_date")),
        source_primary_key=("ts_code", "trade_date"),
        unit_policy="reported financing and lending currency amounts",
        date_policy="trade_date",
        adjustment_policy="reported values; no market adjustment",
        update_policy="daily refresh",
        coverage="CN_A margin securities",
    ),
    *(
        _mapping(
            doc_api,
            "event_timeline",
            status="candidate",
            field_mapping=(("ts_code", "entity_id"),),
            source_primary_key=source_primary_key,
            unit_policy="event text/date/amount fields",
            date_policy="event or announcement date",
            adjustment_policy="not applicable",
            update_policy="event-time with late corrections",
            coverage="CN_A",
        )
        for doc_api, source_primary_key in (
            ("pledge_stat", ("ts_code", "end_date")),
            ("pledge_detail", ("ts_code", "ann_date")),
            ("repurchase", ("ts_code", "ann_date")),
            ("stk_holdertrade", ("ts_code", "ann_date")),
            ("limit_list_ths", ("trade_date", "ts_code")),
            ("limit_list_d", ("trade_date", "ts_code")),
            ("hm_detail", ("trade_date", "ts_code")),
            ("stk_surv", ("ts_code", "ann_date")),
        )
    ),
    _mapping(
        "express",
        "financial_forecast_event",
        status="candidate",
        field_mapping=(("ts_code", "security_id"), ("ann_date", "announcement_date"), ("end_date", "report_period")),
        source_primary_key=("ts_code", "ann_date", "end_date"),
        unit_policy="reported express financial amounts",
        date_policy="announcement date plus report period",
        adjustment_policy="reported values; no market adjustment",
        update_policy="event-time with version retention",
        coverage="CN_A",
    ),
    _mapping(
        "fina_mainbz",
        "business_segment_exposure",
        status="candidate",
        field_mapping=(("ts_code", "security_id"), ("end_date", "report_period"), ("bz_item", "segment_name")),
        source_primary_key=("ts_code", "end_date", "bz_item"),
        unit_policy="reported currency and percent values",
        date_policy="report period",
        adjustment_policy="reported values; no market adjustment",
        update_policy="quarterly/annual late-arriving updates",
        coverage="CN_A",
    ),
)


FUTURE_PROVIDER_TARGETS: Final[frozenset[str]] = frozenset({"wind", "choice", "internal"})
RECONCILIATION_REQUIRED_METRICS: Final[tuple[str, ...]] = (
    "row_count_diff",
    "key_coverage_diff",
    "field_value_diff",
    "unit_normalization_diff",
    "date_policy_diff",
)
