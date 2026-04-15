from __future__ import annotations

import gzip
import json
import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

from data_platform.raw import RawArtifactExists, RawReader, RawWriter, RawZonePathError  # noqa: E402


PARTITION_DATE = date(2026, 4, 15)


@pytest.fixture
def source_id() -> str:
    return f"source-{uuid.uuid4().hex}"


@pytest.fixture
def raw_zone_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    env_path = os.environ.get("DP_RAW_ZONE_PATH")
    path = Path(env_path) if env_path else tmp_path / "raw"
    if not env_path:
        monkeypatch.setenv("DP_RAW_ZONE_PATH", str(path))
    monkeypatch.setenv(
        "DP_ICEBERG_WAREHOUSE_PATH",
        str(tmp_path / "iceberg" / "warehouse"),
    )
    return path


def test_write_arrow_round_trips_as_parquet(
    raw_zone_path: Path,
    source_id: str,
) -> None:
    run_id = str(uuid.uuid4())
    table = pa.table({"symbol": ["000001.SZ"]})

    artifact = RawWriter().write_arrow(
        source_id,
        "stock_basic",
        PARTITION_DATE,
        run_id,
        table,
    )

    assert artifact.path == (
        raw_zone_path / source_id / "stock_basic" / "dt=20260415" / f"{run_id}.parquet"
    )
    assert artifact.row_count == 1

    read_back = pq.read_table(artifact.path)
    assert read_back.num_rows == 1


def test_write_json_records_row_count_in_manifest(
    raw_zone_path: Path,
    source_id: str,
) -> None:
    run_id = uuid.uuid4().hex

    artifact = RawWriter().write_json(
        source_id,
        "stock_basic",
        PARTITION_DATE,
        run_id,
        [{"symbol": "000001.SZ"}, {"symbol": "000002.SZ"}],
    )

    with gzip.open(artifact.path, "rt", encoding="utf-8") as file:
        assert json.load(file) == [{"symbol": "000001.SZ"}, {"symbol": "000002.SZ"}]

    manifest_path = raw_zone_path / source_id / "stock_basic" / "dt=20260415" / "_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert manifest["partition_date"] == "2026-04-15"
    assert "row_count" not in manifest
    assert manifest["artifacts"][0]["row_count"] == 2


def test_repeated_run_id_raises_raw_artifact_exists(
    raw_zone_path: Path,
    source_id: str,
) -> None:
    writer = RawWriter()
    run_id = str(uuid.uuid4())

    writer.write_json(source_id, "stock_basic", PARTITION_DATE, run_id, [{"symbol": "000001.SZ"}])

    with pytest.raises(RawArtifactExists):
        writer.write_json(
            source_id,
            "stock_basic",
            PARTITION_DATE,
            run_id,
            [{"symbol": "000001.SZ"}],
        )


def test_raw_zone_rejects_iceberg_warehouse_boundary(tmp_path: Path) -> None:
    warehouse_path = tmp_path / "iceberg" / "warehouse"

    with pytest.raises(RawZonePathError):
        RawWriter(
            raw_zone_path=warehouse_path / "raw",
            iceberg_warehouse_path=warehouse_path,
        )


def test_raw_zone_rejects_settings_iceberg_warehouse_boundary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    warehouse_path = tmp_path / "iceberg" / "warehouse"
    monkeypatch.delenv("DP_ICEBERG_WAREHOUSE_PATH", raising=False)

    import data_platform.config as config

    monkeypatch.setattr(
        config,
        "get_settings",
        lambda: SimpleNamespace(iceberg_warehouse_path=warehouse_path),
    )

    with pytest.raises(RawZonePathError):
        RawWriter(raw_zone_path=warehouse_path / "raw")


def test_artifact_path_does_not_enter_iceberg_warehouse(
    raw_zone_path: Path,
    tmp_path: Path,
    source_id: str,
) -> None:
    warehouse_path = tmp_path / "iceberg" / "warehouse"
    artifact = RawWriter(
        raw_zone_path=raw_zone_path,
        iceberg_warehouse_path=warehouse_path,
    ).write_json(
        source_id,
        "stock_basic",
        PARTITION_DATE,
        str(uuid.uuid4()),
        [{"symbol": "000001.SZ"}],
    )

    with pytest.raises(ValueError):
        artifact.path.resolve(strict=False).relative_to(warehouse_path.resolve(strict=False))


def test_list_artifacts_returns_written_at_ascending(
    raw_zone_path: Path,
    source_id: str,
) -> None:
    writer = RawWriter()
    first = writer.write_json(
        source_id,
        "stock_basic",
        PARTITION_DATE,
        str(uuid.uuid4()),
        [{"symbol": "000001.SZ"}],
    )
    second = writer.write_json(
        source_id,
        "stock_basic",
        PARTITION_DATE,
        str(uuid.uuid4()),
        [{"symbol": "000002.SZ"}],
    )

    artifacts = RawReader().list_artifacts(source_id, "stock_basic", PARTITION_DATE)

    assert [artifact.run_id for artifact in artifacts] == [first.run_id, second.run_id]
    assert artifacts[0].written_at <= artifacts[1].written_at


def test_concurrent_same_run_id_allows_one_artifact(
    raw_zone_path: Path,
    source_id: str,
) -> None:
    writer = RawWriter()
    run_id = str(uuid.uuid4())
    barrier = threading.Barrier(2)

    def write() -> str:
        barrier.wait(timeout=5)
        writer.write_json(
            source_id,
            "stock_basic",
            PARTITION_DATE,
            run_id,
            [{"symbol": "000001.SZ"}],
        )
        return "written"

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = []
        for future in [executor.submit(write), executor.submit(write)]:
            try:
                results.append(future.result(timeout=10))
            except RawArtifactExists:
                results.append("exists")

    assert sorted(results) == ["exists", "written"]
    artifacts = RawReader().list_artifacts(source_id, "stock_basic", PARTITION_DATE)
    assert [artifact.run_id for artifact in artifacts] == [run_id]


def test_concurrent_different_run_ids_all_appear_in_manifest(
    raw_zone_path: Path,
    source_id: str,
) -> None:
    writer = RawWriter()
    run_ids = [str(uuid.uuid4()) for _ in range(12)]
    barrier = threading.Barrier(len(run_ids))

    def write(run_id: str) -> str:
        barrier.wait(timeout=5)
        writer.write_json(
            source_id,
            "stock_basic",
            PARTITION_DATE,
            run_id,
            [{"symbol": run_id}],
        )
        return run_id

    with ThreadPoolExecutor(max_workers=len(run_ids)) as executor:
        futures = [executor.submit(write, run_id) for run_id in run_ids]
        written_run_ids = {future.result(timeout=10) for future in futures}

    artifacts = RawReader().list_artifacts(source_id, "stock_basic", PARTITION_DATE)

    assert {artifact.run_id for artifact in artifacts} == written_run_ids
    assert len(artifacts) == len(run_ids)
