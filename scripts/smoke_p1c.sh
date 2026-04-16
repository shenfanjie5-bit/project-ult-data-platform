#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -d "${ROOT_DIR}/.venv/bin" ]]; then
  export PATH="${ROOT_DIR}/.venv/bin:${PATH}"
fi

if [[ -z "${PYTHON:-}" ]]; then
  if [[ -x "${ROOT_DIR}/.venv/bin/python" ]]; then
    PYTHON="${ROOT_DIR}/.venv/bin/python"
  else
    PYTHON="python3"
  fi
fi

if [[ -z "${DP_PG_DSN:-}" ]]; then
  echo "P1c smoke failed: DP_PG_DSN is required" >&2
  exit 2
fi

SMOKE_WORK_DIR="${DP_SMOKE_WORK_DIR:-${TMPDIR:-/tmp}/data-platform-p1c-smoke}"
LOG_DIR="${DP_SMOKE_LOG_DIR:-${SMOKE_WORK_DIR}/logs}"

export PYTHONPATH="${ROOT_DIR}/src:${PYTHONPATH:-}"
export DP_SMOKE_WORK_DIR="${SMOKE_WORK_DIR}"
export DP_RAW_ZONE_PATH="${DP_RAW_ZONE_PATH:-${SMOKE_WORK_DIR}/raw}"
export DP_ICEBERG_WAREHOUSE_PATH="${DP_ICEBERG_WAREHOUSE_PATH:-${SMOKE_WORK_DIR}/warehouse}"
export DP_DUCKDB_PATH="${DP_DUCKDB_PATH:-${SMOKE_WORK_DIR}/data_platform.duckdb}"
export DP_ICEBERG_CATALOG_NAME="${DP_ICEBERG_CATALOG_NAME:-data_platform_p1c_smoke}"
export DP_ENV="${DP_ENV:-}"

validate_smoke_target() {
  "${PYTHON}" - <<'PY'
from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from urllib.parse import unquote, urlsplit


def fail(message: str) -> None:
    print(f"P1c smoke failed: {message}", file=sys.stderr)
    raise SystemExit(2)


def is_relative_to(child: Path, parent: Path) -> bool:
    try:
        child.relative_to(parent)
    except ValueError:
        return False
    return True


def database_name(dsn: str) -> str:
    if dsn.startswith("jdbc:postgresql://"):
        dsn = "postgresql://" + dsn.removeprefix("jdbc:postgresql://")

    parsed = urlsplit(dsn)
    if not parsed.scheme or not parsed.netloc:
        fail("DP_PG_DSN must be a PostgreSQL URL with an explicit smoke database")

    name = unquote(parsed.path.lstrip("/"))
    if not name:
        fail("DP_PG_DSN must include an explicit smoke database name")
    return name


truthy = {"1", "true", "yes"}
if os.environ.get("DP_ENV") != "test":
    fail("DP_ENV must be test for smoke-p1c")

if os.environ.get("DP_SMOKE_P1C_CONFIRM_DESTRUCTIVE", "").lower() not in truthy:
    fail("set DP_SMOKE_P1C_CONFIRM_DESTRUCTIVE=1 to confirm the isolated overwrite")

db_name = database_name(os.environ["DP_PG_DSN"])
if re.fullmatch(r"dp_p1c_smoke(?:_[A-Za-z0-9]+)?", db_name) is None:
    fail(
        "DP_PG_DSN database must match dp_p1c_smoke or dp_p1c_smoke_<suffix>; "
        f"got {db_name!r}"
    )

catalog_name = os.environ["DP_ICEBERG_CATALOG_NAME"]
if re.fullmatch(r"data_platform_p1c_smoke(?:_[A-Za-z0-9]+)?", catalog_name) is None:
    fail(
        "DP_ICEBERG_CATALOG_NAME must match data_platform_p1c_smoke or "
        f"data_platform_p1c_smoke_<suffix>; got {catalog_name!r}"
    )

smoke_work_dir = Path(os.environ["DP_SMOKE_WORK_DIR"]).expanduser().resolve(strict=False)
smoke_token = re.compile(r"(?:^|[^A-Za-z0-9])(?:p1c[-_]?smoke|smoke[-_]?p1c)(?:$|[^A-Za-z0-9])")
if smoke_token.search(str(smoke_work_dir)) is None:
    fail("DP_SMOKE_WORK_DIR path must include p1c-smoke, p1c_smoke, smoke-p1c, or smoke_p1c")

for key in ("DP_RAW_ZONE_PATH", "DP_ICEBERG_WAREHOUSE_PATH", "DP_DUCKDB_PATH"):
    path = Path(os.environ[key]).expanduser().resolve(strict=False)
    if not is_relative_to(path, smoke_work_dir):
        fail(f"{key} must be inside DP_SMOKE_WORK_DIR")
PY
}

validate_smoke_target

mkdir -p \
  "${DP_RAW_ZONE_PATH}" \
  "${DP_ICEBERG_WAREHOUSE_PATH}" \
  "$(dirname "${DP_DUCKDB_PATH}")" \
  "${LOG_DIR}"

LOG_FILE="${LOG_DIR}/smoke_p1c.log"

if "${PYTHON}" - <<'PY' >"${LOG_FILE}" 2>&1; then
from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
from sqlalchemy import create_engine, text

from data_platform.config import get_settings, reset_settings_cache
from data_platform.cycle import (
    create_cycle,
    freeze_cycle_candidates,
    publish_manifest,
    transition_cycle_status,
)
from data_platform.ddl.runner import MigrationRunner, _sqlalchemy_postgres_uri
from data_platform.queue import submit_candidate
from data_platform.queue.worker import validate_pending_candidates
from data_platform.serving.catalog import DEFAULT_NAMESPACES, ensure_namespaces, load_catalog
from data_platform.serving.formal import (
    get_formal_by_id,
    get_formal_by_snapshot,
    get_formal_latest,
)


def reset_layer_b_tables(dsn: str) -> None:
    engine = create_engine(_sqlalchemy_postgres_uri(dsn))
    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    TRUNCATE TABLE
                        data_platform.cycle_publish_manifest,
                        data_platform.cycle_candidate_selection,
                        data_platform.cycle_metadata,
                        data_platform.candidate_queue
                    RESTART IDENTITY CASCADE
                    """
                )
            )
    finally:
        engine.dispose()


reset_settings_cache()
settings = get_settings()
Path(settings.raw_zone_path).expanduser().mkdir(parents=True, exist_ok=True)
Path(settings.iceberg_warehouse_path).expanduser().mkdir(parents=True, exist_ok=True)
Path(settings.duckdb_path).expanduser().parent.mkdir(parents=True, exist_ok=True)

MigrationRunner().apply_pending(str(settings.pg_dsn))
reset_layer_b_tables(str(settings.pg_dsn))

catalog = load_catalog(settings=settings)
ensure_namespaces(catalog, DEFAULT_NAMESPACES)
formal_identifier = "formal.recommendation_set"
formal_schema = pa.schema(
    [
        pa.field("symbol", pa.string()),
        pa.field("score", pa.float64()),
    ]
)
catalog.create_table_if_not_exists(formal_identifier, schema=formal_schema)
table = catalog.load_table(formal_identifier)
table.overwrite(
    pa.table(
        {
            "symbol": ["000001.SZ", "000002.SZ"],
            "score": [0.91, 0.77],
        },
        schema=formal_schema,
    )
)
snapshot = table.refresh().current_snapshot()
if snapshot is None:
    raise AssertionError("formal recommendation_set overwrite did not create a snapshot")
snapshot_id = int(snapshot.snapshot_id)

for symbol in ("000001.SZ", "000002.SZ"):
    submit_candidate(
        {
            "payload_type": "Ex-1",
            "submitted_by": "smoke-p1c",
            "subsystem_id": "smoke-p1c",
            "fact_id": f"fact-{symbol.lower()}",
            "entity_id": f"security:{symbol}",
            "fact_type": "security_candidate",
            "fact_content": {"symbol": symbol},
            "confidence": 0.9,
            "source_reference": {"source": "smoke-p1c", "symbol": symbol},
            "extracted_at": datetime(2026, 4, 16, 12, 0, tzinfo=UTC).isoformat(),
        }
    )

summary = validate_pending_candidates(limit=10)
if summary.accepted != 2 or summary.rejected != 0:
    raise AssertionError(f"unexpected validation summary: {summary}")

cycle = create_cycle(date(2026, 4, 16))
metadata = freeze_cycle_candidates(cycle.cycle_id)
if metadata.candidate_count != 2:
    raise AssertionError(f"unexpected frozen candidate_count: {metadata.candidate_count}")

for status in ("phase1", "phase2", "phase3"):
    transition_cycle_status(cycle.cycle_id, status)

manifest = publish_manifest(cycle.cycle_id, {formal_identifier: snapshot_id})
latest = get_formal_latest("recommendation_set")
by_id = get_formal_by_id(cycle.cycle_id, "recommendation_set")
by_snapshot = get_formal_by_snapshot(snapshot_id, "recommendation_set")

expected_symbols = ["000001.SZ", "000002.SZ"]
for formal_object in (latest, by_id, by_snapshot):
    if formal_object.snapshot_id != snapshot_id:
        raise AssertionError(f"unexpected snapshot_id: {formal_object.snapshot_id}")
    if formal_object.payload.column("symbol").to_pylist() != expected_symbols:
        raise AssertionError(f"unexpected formal payload: {formal_object.payload.to_pylist()}")

print(
    json.dumps(
        {
            "cycle_id": cycle.cycle_id,
            "accepted": summary.accepted,
            "candidate_count": metadata.candidate_count,
            "snapshot_id": snapshot_id,
            "published_cycle_id": manifest.published_cycle_id,
            "formal_row_count": latest.payload.num_rows,
        },
        sort_keys=True,
    )
)
PY
  cat "${LOG_FILE}"
  echo "P1c smoke OK"
  exit 0
fi

exit_code=$?
{
  printf '\n[smoke-p1c] failed (exit %d)\n' "${exit_code}"
  printf '[smoke-p1c] last 80 log lines from %s:\n' "${LOG_FILE}"
  tail -n 80 "${LOG_FILE}" || true
} >&2
exit "${exit_code}"
