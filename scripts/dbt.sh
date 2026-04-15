#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DBT_PROJECT_DIR="${ROOT_DIR}/src/data_platform/dbt"

export DBT_PROFILES_DIR="${DBT_PROFILES_DIR:-${DBT_PROJECT_DIR}}"

exec dbt --project-dir "${DBT_PROJECT_DIR}" "$@"
