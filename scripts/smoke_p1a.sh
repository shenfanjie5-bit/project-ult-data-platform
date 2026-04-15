#!/usr/bin/env bash
set -uo pipefail

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
  if [[ -n "${DATABASE_URL:-}" ]]; then
    export DP_PG_DSN="${DATABASE_URL}"
  else
    echo "P1a smoke skipped: DATABASE_URL or DP_PG_DSN is required for PostgreSQL"
    exit 0
  fi
fi

SMOKE_WORK_DIR="${DP_SMOKE_WORK_DIR:-${TMPDIR:-/tmp}/data-platform-p1a-smoke}"
LOG_DIR="${DP_SMOKE_LOG_DIR:-${SMOKE_WORK_DIR}/logs}"
PROFILES_DIR="${SMOKE_WORK_DIR}/dbt_profiles"
DBT_TARGET_DIR="${SMOKE_WORK_DIR}/dbt_target"

export PYTHONPATH="${ROOT_DIR}/src:${PYTHONPATH:-}"
export DP_RAW_ZONE_PATH="${DP_RAW_ZONE_PATH:-${SMOKE_WORK_DIR}/raw}"
export DP_ICEBERG_WAREHOUSE_PATH="${DP_ICEBERG_WAREHOUSE_PATH:-${SMOKE_WORK_DIR}/warehouse}"
export DP_DUCKDB_PATH="${DP_DUCKDB_PATH:-${SMOKE_WORK_DIR}/data_platform.duckdb}"
export DP_ICEBERG_CATALOG_NAME="${DP_ICEBERG_CATALOG_NAME:-data_platform}"
export DP_ENV="${DP_ENV:-test}"
export DBT_PROFILES_DIR="${PROFILES_DIR}"

mkdir -p \
  "${DP_RAW_ZONE_PATH}" \
  "${DP_ICEBERG_WAREHOUSE_PATH}" \
  "$(dirname "${DP_DUCKDB_PATH}")" \
  "${LOG_DIR}" \
  "${PROFILES_DIR}" \
  "${DBT_TARGET_DIR}"

cat > "${PROFILES_DIR}/profiles.yml" <<YAML
data_platform:
  target: smoke
  outputs:
    smoke:
      type: duckdb
      path: "{{ env_var('DP_DUCKDB_PATH') }}"
      threads: 1
YAML

step_index=0
started_at="$(date +%s)"

run_step() {
  local name="$1"
  shift
  step_index=$((step_index + 1))
  local log_file="${LOG_DIR}/step_${step_index}.log"

  printf '[smoke-p1a] step %d: %s\n' "${step_index}" "${name}"
  if "$@" >"${log_file}" 2>&1; then
    return 0
  fi

  local exit_code=$?
  {
    printf '\n[smoke-p1a] step failed: %s (exit %d)\n' "${name}" "${exit_code}"
    printf '[smoke-p1a] last 50 log lines from %s:\n' "${log_file}"
    tail -n 50 "${log_file}" || true
  } >&2
  exit "${exit_code}"
}

seed_tushare_fixture() {
  "${PYTHON}" - <<'PY'
from __future__ import annotations

from datetime import date
import uuid

import pandas as pd

from data_platform.adapters.tushare import TushareAdapter
from data_platform.raw import RawWriter


class MockTushareClient:
    def stock_basic(self, **_kwargs: object) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "symbol": "000001",
                    "name": "Ping An Bank",
                    "area": "Shenzhen",
                    "industry": "Banking",
                    "market": "Main Board",
                    "list_status": "L",
                    "list_date": "19910403",
                    "delist_date": None,
                    "is_hs": "S",
                    "act_name": "fixture",
                    "act_ent_type": "fixture",
                    "cnspell": "payh",
                    "curr_type": "CNY",
                    "enname": "Ping An Bank Co Ltd",
                    "exchange": "SZSE",
                    "fullname": "Ping An Bank Co Ltd",
                },
                {
                    "ts_code": "000002.SZ",
                    "symbol": "000002",
                    "name": "Vanke A",
                    "area": "Shenzhen",
                    "industry": "Real Estate",
                    "market": "Main Board",
                    "list_status": "L",
                    "list_date": "19910129",
                    "delist_date": None,
                    "is_hs": "S",
                    "act_name": "fixture",
                    "act_ent_type": "fixture",
                    "cnspell": "wka",
                    "curr_type": "CNY",
                    "enname": "China Vanke Co Ltd",
                    "exchange": "SZSE",
                    "fullname": "China Vanke Co Ltd",
                },
                {
                    "ts_code": "300001.SZ",
                    "symbol": "300001",
                    "name": "Teruide",
                    "area": "Qingdao",
                    "industry": "Electrical Equipment",
                    "market": "ChiNext",
                    "list_status": "L",
                    "list_date": "20091030",
                    "delist_date": None,
                    "is_hs": "N",
                    "act_name": "fixture",
                    "act_ent_type": "fixture",
                    "cnspell": "tld",
                    "curr_type": "CNY",
                    "enname": "Qingdao TGOOD Electric Co Ltd",
                    "exchange": "SZSE",
                    "fullname": "Qingdao TGOOD Electric Co Ltd",
                },
            ]
        )


adapter = TushareAdapter(token="mock-token", client=MockTushareClient())
table = adapter.fetch("tushare_stock_basic", {})
artifact = RawWriter().write_arrow(
    adapter.source_id(),
    "stock_basic",
    date(2026, 4, 15),
    f"p1a-smoke-{uuid.uuid4().hex}",
    table,
)
print(artifact.path)
PY
}

query_canonical_with_duckdb() {
  "${PYTHON}" - <<'PY'
from __future__ import annotations

import json

from data_platform.serving.reader import read_canonical


expected_columns = [
    "ts_code",
    "symbol",
    "name",
    "area",
    "industry",
    "market",
    "list_date",
    "is_active",
    "source_run_id",
    "canonical_loaded_at",
]

table = read_canonical("stock_basic")
if table.num_rows != 3:
    raise SystemExit(f"expected 3 canonical.stock_basic rows, got {table.num_rows}")
if table.schema.names != expected_columns:
    raise SystemExit(
        "unexpected canonical.stock_basic columns: "
        + json.dumps(table.schema.names, sort_keys=True)
    )

print(json.dumps({"columns": table.schema.names, "row_count": table.num_rows}))
PY
}

run_step "migrate" "${PYTHON}" -m data_platform.ddl.runner --upgrade
run_step "init catalog" "${PYTHON}" "${ROOT_DIR}/scripts/init_iceberg_catalog.py"
run_step "ensure tables" "${PYTHON}" -m data_platform.ddl.iceberg_tables --ensure
run_step "fetch tushare fixture" seed_tushare_fixture
run_step "dbt run" "${ROOT_DIR}/scripts/dbt.sh" run --profiles-dir "${PROFILES_DIR}" --target-path "${DBT_TARGET_DIR}" --select stg_stock_basic
run_step "canonical write" "${PYTHON}" -m data_platform.serving.canonical_writer --table stock_basic
run_step "reader query" query_canonical_with_duckdb

duration_s=$(($(date +%s) - started_at))
printf 'P1a smoke OK duration_s=%d log_dir=%s\n' "${duration_s}" "${LOG_DIR}"
