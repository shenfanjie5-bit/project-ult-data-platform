#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DBT_PROJECT_DIR="${ROOT_DIR}/src/data_platform/dbt"

export DBT_PROFILES_DIR="${DBT_PROFILES_DIR:-${DBT_PROJECT_DIR}}"

if command -v dbt >/dev/null 2>&1; then
  DBT_BIN="dbt"
elif [ -x "${ROOT_DIR}/.venv/bin/dbt" ]; then
  DBT_BIN="${ROOT_DIR}/.venv/bin/dbt"
else
  echo "dbt executable is not installed; expected dbt on PATH or ${ROOT_DIR}/.venv/bin/dbt" >&2
  exit 127
fi

# dbt 1.8+ removed --project-dir flag; use DBT_PROJECT_DIR env var instead
# (already set above, compatible with dbt 1.5+)
export DBT_PROJECT_DIR

exec "${DBT_BIN}" "$@"
