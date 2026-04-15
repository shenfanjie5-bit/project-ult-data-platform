"""Raw Zone artifact writer and reader helpers."""

from __future__ import annotations

import gzip
import json
import os
import re
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, TypeAlias

import fcntl
import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]


JsonPayload: TypeAlias = dict[str, Any] | list[dict[str, Any]]

_ULID_RE = re.compile(r"^[0-7][0-9A-HJKMNP-TV-Z]{25}$")


@dataclass(frozen=True, slots=True)
class RawArtifact:
    """Metadata for one Raw Zone artifact."""

    source_id: str
    dataset: str
    partition_date: date
    run_id: str
    path: Path
    row_count: int
    written_at: datetime


class RawArtifactExists(FileExistsError):
    """Raised when the same Raw Zone artifact already exists."""


class RawZonePathError(ValueError):
    """Raised when Raw Zone paths cross an Iceberg warehouse boundary."""


class RawWriter:
    """Write Raw Zone Parquet and JSON artifacts."""

    def __init__(
        self,
        raw_zone_path: str | Path | None = None,
        *,
        iceberg_warehouse_path: str | Path | None = None,
    ) -> None:
        self.raw_zone_path = _default_raw_zone_path(raw_zone_path)
        self.iceberg_warehouse_path = _default_iceberg_warehouse_path(iceberg_warehouse_path)
        _ensure_not_in_iceberg_warehouse(self.raw_zone_path, self.iceberg_warehouse_path)

    def write_arrow(
        self,
        source_id: str,
        dataset: str,
        partition_date: date,
        run_id: str,
        table: pa.Table,
    ) -> RawArtifact:
        """Write a pyarrow table to Raw Zone parquet."""

        artifact_path = self._artifact_path(source_id, dataset, partition_date, run_id, "parquet")
        row_count = int(table.num_rows)

        def write_tmp(tmp_path: Path) -> None:
            pq.write_table(table, tmp_path)

        return self._write_artifact(
            source_id=source_id,
            dataset=dataset,
            partition_date=partition_date,
            run_id=run_id,
            artifact_path=artifact_path,
            row_count=row_count,
            write_tmp=write_tmp,
        )

    def write_json(
        self,
        source_id: str,
        dataset: str,
        partition_date: date,
        run_id: str,
        payload: JsonPayload,
    ) -> RawArtifact:
        """Write a JSON payload to Raw Zone gzip-compressed JSON."""

        artifact_path = self._artifact_path(source_id, dataset, partition_date, run_id, "json.gz")
        row_count = 1 if isinstance(payload, dict) else len(payload)

        def write_tmp(tmp_path: Path) -> None:
            with gzip.open(tmp_path, "wt", encoding="utf-8") as file:
                json.dump(payload, file, ensure_ascii=False)

        return self._write_artifact(
            source_id=source_id,
            dataset=dataset,
            partition_date=partition_date,
            run_id=run_id,
            artifact_path=artifact_path,
            row_count=row_count,
            write_tmp=write_tmp,
        )

    def _write_artifact(
        self,
        *,
        source_id: str,
        dataset: str,
        partition_date: date,
        run_id: str,
        artifact_path: Path,
        row_count: int,
        write_tmp: Callable[[Path], None],
    ) -> RawArtifact:
        artifact_path.parent.mkdir(parents=True, exist_ok=True)

        tmp_path = artifact_path.with_name(f".{artifact_path.name}.{uuid.uuid4().hex}.tmp")
        try:
            write_tmp(tmp_path)
            with _partition_lock(self._lock_path(artifact_path.parent)):
                self._raise_if_run_id_exists(artifact_path.parent, run_id, artifact_path)
                _materialize_artifact(tmp_path, artifact_path)

                artifact = RawArtifact(
                    source_id=source_id,
                    dataset=dataset,
                    partition_date=partition_date,
                    run_id=run_id,
                    path=artifact_path,
                    row_count=row_count,
                    written_at=datetime.now(tz=UTC),
                )
                try:
                    self._write_manifest(artifact)
                except Exception as exc:
                    try:
                        artifact_path.unlink(missing_ok=True)
                    except OSError as cleanup_exc:
                        exc.add_note(
                            f"failed to remove uncommitted raw artifact {artifact_path}: "
                            f"{cleanup_exc}"
                        )
                    raise
                return artifact
        except Exception:
            tmp_path.unlink(missing_ok=True)
            raise

    def _artifact_path(
        self,
        source_id: str,
        dataset: str,
        partition_date: date,
        run_id: str,
        extension: str,
    ) -> Path:
        _validate_path_segment(source_id, "source_id")
        _validate_path_segment(dataset, "dataset")
        _validate_run_id(run_id)

        partition = f"dt={partition_date:%Y%m%d}"
        path = self.raw_zone_path / source_id / dataset / partition / f"{run_id}.{extension}"
        _ensure_path_stays_under(path, self.raw_zone_path)
        _ensure_not_in_iceberg_warehouse(path, self.iceberg_warehouse_path)
        return path

    def _manifest_path(self, partition_path: Path) -> Path:
        return partition_path / "_manifest.json"

    def _lock_path(self, partition_path: Path) -> Path:
        return partition_path / "._manifest.lock"

    def _raise_if_run_id_exists(
        self,
        partition_path: Path,
        run_id: str,
        artifact_path: Path,
    ) -> None:
        self._raise_if_manifest_contains_run_id(partition_path, run_id)
        self._remove_unmanifested_run_artifacts(partition_path, run_id, artifact_path)

    def _remove_unmanifested_run_artifacts(
        self,
        partition_path: Path,
        run_id: str,
        artifact_path: Path,
    ) -> None:
        existing_paths = {artifact_path}
        for existing_path in partition_path.glob(f"{run_id}.*"):
            if existing_path.name.endswith(".tmp"):
                continue
            existing_paths.add(existing_path)

        for existing_path in sorted(existing_paths, key=str):
            if not existing_path.exists():
                continue
            try:
                existing_path.unlink()
            except OSError as exc:
                raise RawArtifactExists(
                    f"uncommitted raw artifact could not be recovered for run_id={run_id!r}: "
                    f"{existing_path}"
                ) from exc

    def _raise_if_manifest_contains_run_id(self, partition_path: Path, run_id: str) -> None:
        manifest_path = self._manifest_path(partition_path)
        if not manifest_path.exists():
            return

        manifest = _read_manifest(manifest_path)
        for artifact in _manifest_artifacts(manifest):
            if artifact.get("run_id") == run_id:
                raise RawArtifactExists(f"raw artifact already exists for run_id={run_id!r}")

    def _write_manifest(self, artifact: RawArtifact) -> None:
        manifest_path = self._manifest_path(artifact.path.parent)
        existing_artifacts: list[dict[str, Any]] = []
        if manifest_path.exists():
            existing_artifacts = _manifest_artifacts(_read_manifest(manifest_path))

        artifact_dict = _artifact_to_dict(artifact)
        artifacts = [*existing_artifacts, artifact_dict]
        artifacts.sort(key=lambda item: str(item["written_at"]))

        manifest = {
            "source_id": artifact.source_id,
            "dataset": artifact.dataset,
            "partition_date": artifact.partition_date.isoformat(),
            "artifacts": artifacts,
        }
        tmp_path = manifest_path.with_name(f".{manifest_path.name}.{uuid.uuid4().hex}.tmp")
        try:
            with tmp_path.open("w", encoding="utf-8") as file:
                json.dump(manifest, file, ensure_ascii=False, indent=2, sort_keys=True)
                file.write("\n")
            os.replace(tmp_path, manifest_path)
        except Exception:
            tmp_path.unlink(missing_ok=True)
            raise


class RawReader:
    """Read Raw Zone artifact manifests."""

    def __init__(self, raw_zone_path: str | Path | None = None) -> None:
        self.raw_zone_path = _default_raw_zone_path(raw_zone_path)

    def list_artifacts(
        self,
        source_id: str,
        dataset: str,
        partition_date: date,
    ) -> list[RawArtifact]:
        """Return artifacts for one Raw Zone partition ordered by write time."""

        _validate_path_segment(source_id, "source_id")
        _validate_path_segment(dataset, "dataset")
        partition = f"dt={partition_date:%Y%m%d}"
        partition_path = self.raw_zone_path / source_id / dataset / partition
        _ensure_path_stays_under(partition_path, self.raw_zone_path)

        manifest_path = partition_path / "_manifest.json"
        if not manifest_path.exists():
            return []

        artifacts = [
            _artifact_from_dict(item)
            for item in _manifest_artifacts(_read_manifest(manifest_path))
        ]
        artifacts.sort(key=lambda artifact: artifact.written_at)
        return artifacts


def _default_raw_zone_path(raw_zone_path: str | Path | None) -> Path:
    if raw_zone_path is not None:
        return Path(raw_zone_path).expanduser()

    env_path = os.environ.get("DP_RAW_ZONE_PATH")
    if env_path:
        return Path(env_path).expanduser()

    from data_platform.config import get_settings

    return get_settings().raw_zone_path.expanduser()


def _default_iceberg_warehouse_path(
    iceberg_warehouse_path: str | Path | None,
) -> Path | None:
    if iceberg_warehouse_path is not None:
        return Path(iceberg_warehouse_path).expanduser()

    env_path = os.environ.get("DP_ICEBERG_WAREHOUSE_PATH")
    if env_path:
        return Path(env_path).expanduser()

    from data_platform.config import get_settings

    return get_settings().iceberg_warehouse_path.expanduser()


@contextmanager
def _partition_lock(lock_path: Path) -> Iterator[None]:
    with lock_path.open("a+", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def _materialize_artifact(tmp_path: Path, artifact_path: Path) -> None:
    try:
        os.link(tmp_path, artifact_path)
    except FileExistsError as exc:
        raise RawArtifactExists(f"raw artifact already exists: {artifact_path}") from exc
    finally:
        tmp_path.unlink(missing_ok=True)


def _validate_path_segment(value: str, field_name: str) -> None:
    path = Path(value)
    if not value or path.is_absolute() or value in {".", ".."} or len(path.parts) != 1:
        raise ValueError(f"{field_name} must be a single relative path segment")


def _validate_run_id(run_id: str) -> None:
    _validate_path_segment(run_id, "run_id")
    if _is_uuid4(run_id) or _is_ulid(run_id):
        return
    raise ValueError("run_id must be a ULID or UUID4")


def _is_uuid4(value: str) -> bool:
    try:
        parsed = uuid.UUID(value)
    except ValueError:
        return False
    return parsed.version == 4 and value.lower() in {str(parsed), parsed.hex}


def _is_ulid(value: str) -> bool:
    return bool(_ULID_RE.fullmatch(value.upper()))


def _ensure_path_stays_under(path: Path, root: Path) -> None:
    try:
        path.expanduser().resolve(strict=False).relative_to(root.expanduser().resolve(strict=False))
    except ValueError as exc:
        raise RawZonePathError(f"path escapes raw zone root: {path}") from exc


def _ensure_not_in_iceberg_warehouse(path: Path, iceberg_warehouse_path: Path | None) -> None:
    if iceberg_warehouse_path is None:
        return

    resolved_path = path.expanduser().resolve(strict=False)
    resolved_warehouse = iceberg_warehouse_path.expanduser().resolve(strict=False)
    try:
        resolved_path.relative_to(resolved_warehouse)
    except ValueError:
        return

    raise RawZonePathError("raw zone artifacts must not be written inside Iceberg warehouse")


def _artifact_to_dict(artifact: RawArtifact) -> dict[str, Any]:
    return {
        "source_id": artifact.source_id,
        "dataset": artifact.dataset,
        "partition_date": artifact.partition_date.isoformat(),
        "run_id": artifact.run_id,
        "path": str(artifact.path),
        "row_count": artifact.row_count,
        "written_at": artifact.written_at.isoformat(),
    }


def _artifact_from_dict(value: dict[str, Any]) -> RawArtifact:
    return RawArtifact(
        source_id=str(value["source_id"]),
        dataset=str(value["dataset"]),
        partition_date=date.fromisoformat(str(value["partition_date"])),
        run_id=str(value["run_id"]),
        path=Path(str(value["path"])),
        row_count=int(value["row_count"]),
        written_at=datetime.fromisoformat(str(value["written_at"])),
    )


def _read_manifest(manifest_path: Path) -> dict[str, Any]:
    with manifest_path.open(encoding="utf-8") as file:
        manifest = json.load(file)
    if not isinstance(manifest, dict):
        raise ValueError(f"raw manifest must be a JSON object: {manifest_path}")
    return manifest


def _manifest_artifacts(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    artifacts = manifest.get("artifacts")
    if isinstance(artifacts, list):
        return [artifact for artifact in artifacts if isinstance(artifact, dict)]
    if "run_id" in manifest:
        return [manifest]
    return []


__all__ = [
    "RawArtifact",
    "RawArtifactExists",
    "RawReader",
    "RawWriter",
    "RawZonePathError",
]
