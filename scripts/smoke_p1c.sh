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

mkdir -p \
  "${DP_RAW_ZONE_PATH}" \
  "${DP_ICEBERG_WAREHOUSE_PATH}" \
  "$(dirname "${DP_DUCKDB_PATH}")" \
  "${LOG_DIR}"

step_index=0

run_step() {
  local name="$1"
  shift
  step_index=$((step_index + 1))
  local log_file="${LOG_DIR}/step_${step_index}.log"

  printf '[smoke-p1c] step %d: %s\n' "${step_index}" "${name}"
  if "$@" >"${log_file}" 2>&1; then
    if [[ "${name}" == "runner" ]]; then
      cat "${log_file}"
    fi
    return 0
  fi

  local exit_code=$?
  {
    printf '\n[smoke-p1c] step failed: %s (exit %d)\n' "${name}" "${exit_code}"
    printf '[smoke-p1c] last 80 log lines from %s:\n' "${log_file}"
    tail -n 80 "${log_file}" || true
  } >&2
  exit "${exit_code}"
}

run_step "migrate" "${PYTHON}" -m data_platform.ddl.runner --upgrade
run_step "init catalog" "${PYTHON}" "${ROOT_DIR}/scripts/init_iceberg_catalog.py"
run_step "runner" "${PYTHON}" -m data_platform.smoke.p1c
