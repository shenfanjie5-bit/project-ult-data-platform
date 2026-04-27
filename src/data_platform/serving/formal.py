"""Formal serving APIs backed by publish-manifest pinned Iceberg snapshots."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

import pyarrow as pa  # type: ignore[import-untyped]

from data_platform.cycle.manifest import (
    CyclePublishManifest,
    FormalTableSnapshot,
    InvalidFormalSnapshotManifest,
    PublishManifestNotFound,
    get_latest_publish_manifest,
    get_publish_manifest_for_snapshot,
    get_publish_manifest,
    validate_snapshot_id,
)
import data_platform.serving.reader as serving_reader
from data_platform.formal_registry import validate_formal_object_name


FORMAL_NAMESPACE: Final[str] = "formal"


class FormalObjectTypeInvalid(ValueError):
    """Raised when object_type cannot be used as a formal table identifier."""

    def __init__(self, object_type: object) -> None:
        self.object_type = object_type
        super().__init__(f"invalid formal object_type: {object_type!r}")


class FormalManifestNotFound(LookupError):
    """Raised when the required cycle publish manifest does not exist."""

    def __init__(self, cycle_id: str | None = None) -> None:
        self.cycle_id = cycle_id
        if cycle_id is None:
            super().__init__("formal publish manifest not found")
        else:
            super().__init__(f"formal publish manifest not found: {cycle_id}")


class FormalSnapshotNotPublished(LookupError):
    """Raised when a requested snapshot is not present in any publish manifest."""

    def __init__(self, snapshot_id: int, table_identifier: str) -> None:
        self.snapshot_id = snapshot_id
        self.table_identifier = table_identifier
        super().__init__(
            f"formal snapshot is not published: {table_identifier} snapshot_id={snapshot_id}"
        )


class FormalTableSnapshotNotFound(LookupError):
    """Raised when a publish manifest does not pin the requested formal table."""

    def __init__(self, cycle_id: str, table_identifier: str) -> None:
        self.cycle_id = cycle_id
        self.table_identifier = table_identifier
        super().__init__(
            "formal table snapshot not found in publish manifest: "
            f"{cycle_id} {table_identifier}"
        )


@dataclass(frozen=True, slots=True)
class FormalObject:
    """A formal object materialized from one manifest-pinned Iceberg snapshot."""

    cycle_id: str
    object_type: str
    snapshot_id: int
    payload: pa.Table


def get_formal_latest(object_type: str) -> FormalObject:
    """Read the newest published formal object for object_type."""

    table_identifier = formal_table_identifier(object_type)
    try:
        manifest = get_latest_publish_manifest()
    except PublishManifestNotFound as exc:
        raise FormalManifestNotFound() from exc
    return _formal_object_from_manifest(manifest, object_type, table_identifier)


def get_formal_by_id(cycle_id: str, object_type: str) -> FormalObject:
    """Read the formal object pinned by one published cycle_id."""

    table_identifier = formal_table_identifier(object_type)
    try:
        manifest = get_publish_manifest(cycle_id)
    except PublishManifestNotFound as exc:
        raise FormalManifestNotFound(cycle_id) from exc
    return _formal_object_from_manifest(manifest, object_type, table_identifier)


def get_formal_by_snapshot(snapshot_id: int, object_type: str) -> FormalObject:
    """Read a formal object only if snapshot_id has been published in a manifest."""

    table_identifier = formal_table_identifier(object_type)
    validated_snapshot_id = _validate_formal_snapshot_id(snapshot_id)
    try:
        manifest = get_publish_manifest_for_snapshot(
            validated_snapshot_id,
            table_identifier,
        )
    except PublishManifestNotFound as exc:
        raise FormalSnapshotNotPublished(
            validated_snapshot_id,
            table_identifier,
        ) from exc
    snapshot = _snapshot_from_manifest(manifest, table_identifier)
    if snapshot.snapshot_id != validated_snapshot_id:
        raise FormalSnapshotNotPublished(validated_snapshot_id, table_identifier)
    return FormalObject(
        cycle_id=manifest.published_cycle_id,
        object_type=object_type,
        snapshot_id=snapshot.snapshot_id,
        payload=_read_formal_snapshot(table_identifier, snapshot.snapshot_id),
    )


def formal_table_identifier(object_type: str) -> str:
    """Return the manifest key and Iceberg identifier for one formal object type."""

    try:
        validate_formal_object_name(object_type)
        serving_reader._validate_identifier(object_type)
    except (TypeError, ValueError) as exc:
        raise FormalObjectTypeInvalid(object_type) from exc
    return f"{FORMAL_NAMESPACE}.{object_type}"


def _formal_object_from_manifest(
    manifest: CyclePublishManifest,
    object_type: str,
    table_identifier: str,
) -> FormalObject:
    snapshot = _snapshot_from_manifest(manifest, table_identifier)
    return FormalObject(
        cycle_id=manifest.published_cycle_id,
        object_type=object_type,
        snapshot_id=snapshot.snapshot_id,
        payload=_read_formal_snapshot(table_identifier, snapshot.snapshot_id),
    )


def _snapshot_from_manifest(
    manifest: CyclePublishManifest,
    table_identifier: str,
) -> FormalTableSnapshot:
    snapshot = manifest.formal_table_snapshots.get(table_identifier)
    if snapshot is None:
        raise FormalTableSnapshotNotFound(manifest.published_cycle_id, table_identifier)
    return snapshot


def _read_formal_snapshot(table_identifier: str, snapshot_id: int) -> pa.Table:
    payload = serving_reader.read_iceberg_snapshot(table_identifier, snapshot_id)
    if isinstance(payload, pa.Table):
        return payload
    msg = "formal DuckDB Iceberg scan did not return a PyArrow table"
    raise TypeError(msg)


def _validate_formal_snapshot_id(snapshot_id: int) -> int:
    try:
        return validate_snapshot_id(snapshot_id)
    except InvalidFormalSnapshotManifest as exc:
        raise ValueError(str(exc)) from exc


__all__ = [
    "FormalManifestNotFound",
    "FormalObject",
    "FormalObjectTypeInvalid",
    "FormalSnapshotNotPublished",
    "FormalTableSnapshotNotFound",
    "formal_table_identifier",
    "get_formal_by_id",
    "get_formal_by_snapshot",
    "get_formal_latest",
]
