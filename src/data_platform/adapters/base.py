"""Base abstractions for data source adapters."""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from threading import Lock
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Literal,
    Mapping,
    Protocol,
    Sequence,
    TypeAlias,
    runtime_checkable,
)


@runtime_checkable
class _ArrowTable(Protocol):
    num_rows: int


if TYPE_CHECKING:
    Schema: TypeAlias = Any
    Table: TypeAlias = _ArrowTable
else:
    try:
        from pyarrow import Schema, Table
        import pyarrow as pa
    except ModuleNotFoundError:

        class Schema:
            """Minimal fallback used only when pyarrow is unavailable locally."""

            def __init__(self, fields: Sequence[tuple[str, Any]] | None = None) -> None:
                self.fields = tuple(fields or ())

        class Table:
            """Minimal fallback used only when pyarrow is unavailable locally."""

            def __init__(self, rows: Sequence[Mapping[str, Any]] | None = None) -> None:
                self.num_rows = len(rows or ())

        class _PyArrowFallback:
            Schema = Schema
            Table = Table

            @staticmethod
            def schema(fields: Sequence[tuple[str, Any]]) -> Schema:
                return Schema(fields)

            @staticmethod
            def table(rows: Sequence[Mapping[str, Any]]) -> Table:
                return Table(rows)

        pa = _PyArrowFallback()

FetchParams: TypeAlias = Mapping[str, Any]
FetchResult: TypeAlias = Table | list[dict[str, Any]]
PartitionType: TypeAlias = Literal["daily", "static"]


@dataclass(frozen=True, slots=True)
class AssetSpec:
    """Data structure describing an adapter-provided asset."""

    name: str
    dataset: str
    partition: PartitionType
    schema: Schema


@dataclass(frozen=True, slots=True)
class QuotaConfig:
    """Rate and credit quota configuration for an adapter."""

    requests_per_minute: int
    daily_credit_quota: int | None
    backoff_seconds: float = 1.0

    def __post_init__(self) -> None:
        if self.requests_per_minute <= 0:
            msg = "requests_per_minute must be greater than zero"
            raise ValueError(msg)
        if self.daily_credit_quota is not None and self.daily_credit_quota < 0:
            msg = "daily_credit_quota must be non-negative or None"
            raise ValueError(msg)
        if self.backoff_seconds < 0:
            msg = "backoff_seconds must be non-negative"
            raise ValueError(msg)


class AdapterQuotaExceeded(RuntimeError):
    """Raised when an adapter-level quota is exhausted."""


class AdapterAlreadyRegistered(RuntimeError):
    """Raised when registering another adapter for the same source."""


class AdapterFetchError(RuntimeError):
    """Raised when adapter fetch I/O fails."""

    def __init__(self, source_id: str, asset_id: str, cause: BaseException) -> None:
        self.source_id = source_id
        self.asset_id = asset_id
        self.cause = cause
        super().__init__(
            f"adapter fetch failed for source_id={source_id!r}, asset_id={asset_id!r}: {cause}"
        )


class DataSourceAdapter(ABC):
    """Contract implemented by data source adapters."""

    @abstractmethod
    def source_id(self) -> str:
        """Return the unique source identifier for this adapter."""

    @abstractmethod
    def get_assets(self) -> list[AssetSpec]:
        """Return data assets exposed by this adapter."""

    @abstractmethod
    def get_resources(self) -> dict[str, Any]:
        """Return runtime resources required by this adapter."""

    @abstractmethod
    def get_staging_dbt_models(self) -> list[str]:
        """Return staging dbt model names required for this adapter."""

    @abstractmethod
    def get_quota_config(self) -> QuotaConfig:
        """Return quota configuration for this adapter."""

    @abstractmethod
    def fetch(self, asset_id: str, params: FetchParams) -> FetchResult:
        """Fetch one asset from the source."""


class BaseAdapter(DataSourceAdapter):
    """Base adapter with shared throttling, retry, and fetch error handling."""

    def __init__(
        self,
        *,
        max_retries: int = 3,
        clock: Callable[[], float] | None = None,
        sleep: Callable[[float], None] | None = None,
    ) -> None:
        if max_retries < 0:
            msg = "max_retries must be non-negative"
            raise ValueError(msg)
        self._max_retries = max_retries
        self._clock = clock or time.monotonic
        self._sleep = sleep or time.sleep
        self._quota_lock = Lock()
        self._daily_credit_lock = Lock()
        self._tokens = 0.0
        self._last_token_refill = self._clock()
        self._daily_credits_used = 0

    def fetch(self, asset_id: str, params: FetchParams) -> FetchResult:
        """Fetch with quota throttling, retry-on-429, and error wrapping."""

        self._reserve_daily_credit()
        start = self._clock()
        attempt = 0
        while True:
            self._throttle()
            try:
                result = self._fetch(asset_id, params)
            except Exception as exc:
                if self._is_rate_limit_error(exc) and attempt < self._max_retries:
                    attempt += 1
                    self._sleep(self.get_quota_config().backoff_seconds)
                    continue
                raise AdapterFetchError(self.source_id(), asset_id, exc) from exc

            duration_s = self._clock() - start
            self.on_fetch_complete(asset_id, self._row_count(result), duration_s)
            return result

    def _throttle(self) -> None:
        """Block until this adapter has a request token available."""

        quota = self.get_quota_config()
        seconds_per_token = 60.0 / quota.requests_per_minute
        with self._quota_lock:
            while self._tokens < 1.0:
                now = self._clock()
                elapsed = max(0.0, now - self._last_token_refill)
                self._tokens = min(1.0, self._tokens + (elapsed / seconds_per_token))
                self._last_token_refill = now
                if self._tokens >= 1.0:
                    break

                wait_seconds = (1.0 - self._tokens) * seconds_per_token
                self._sleep(wait_seconds)

            self._tokens -= 1.0

    def on_fetch_complete(self, asset_id: str, row_count: int, duration_s: float) -> None:
        """Hook called after successful fetch completion."""

    @abstractmethod
    def _fetch(self, asset_id: str, params: FetchParams) -> FetchResult:
        """Fetch one asset without shared BaseAdapter error handling."""

    def _reserve_daily_credit(self) -> None:
        quota = self.get_quota_config()
        if quota.daily_credit_quota is None:
            return

        with self._daily_credit_lock:
            if self._daily_credits_used >= quota.daily_credit_quota:
                msg = f"daily credit quota exceeded for source_id={self.source_id()!r}"
                raise AdapterQuotaExceeded(msg)
            self._daily_credits_used += 1

    @staticmethod
    def _row_count(result: FetchResult) -> int:
        if isinstance(result, Table):
            return int(result.num_rows)
        return len(result)

    @staticmethod
    def _is_rate_limit_error(exc: BaseException) -> bool:
        status_code = getattr(exc, "status_code", None)
        if status_code == 429:
            return True

        response = getattr(exc, "response", None)
        response_status_code = getattr(response, "status_code", None)
        if response_status_code == 429:
            return True

        code = getattr(exc, "code", None)
        return code == 429


class AdapterRegistry:
    """Thread-safe registry for source adapters."""

    def __init__(self) -> None:
        self._adapters: dict[str, DataSourceAdapter] = {}
        self._lock = Lock()

    def register(self, adapter: DataSourceAdapter) -> None:
        source_id = adapter.source_id()
        with self._lock:
            if source_id in self._adapters:
                msg = f"adapter already registered for source_id={source_id!r}"
                raise AdapterAlreadyRegistered(msg)
            self._adapters[source_id] = adapter

    def get(self, source_id: str) -> DataSourceAdapter:
        with self._lock:
            return self._adapters[source_id]

    def list_sources(self) -> list[str]:
        with self._lock:
            return list(self._adapters)


_REGISTRY = AdapterRegistry()

__all__ = [
    "AdapterAlreadyRegistered",
    "AdapterFetchError",
    "AdapterQuotaExceeded",
    "AdapterRegistry",
    "AssetSpec",
    "BaseAdapter",
    "DataSourceAdapter",
    "FetchParams",
    "FetchResult",
    "PartitionType",
    "QuotaConfig",
    "_REGISTRY",
]
