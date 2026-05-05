# data-platform

Layer A structured-data and canonical-serving module. It owns Tushare/raw
adapter intake, Raw Zone persistence, dbt staging/intermediate/marts, Iceberg
canonical truth, Lite Layer B queue tables, and formal serving surfaces.

Source of truth:

- `docs/data-platform.project-doc.md`

## Current state

Implementation exists under `src/`, `tests/`, `fixtures/`, `scripts/`, and the
embedded dbt project. The README is a status pointer; detailed milestone state
stays in `docs/PROGRESS.md`.

Already present and relevant to the next roadmap:

- Tushare adapter infrastructure, Raw Zone writer, canonical writer/reader,
  dbt staging/mart scaffolding, and event timeline marts.
- Promoted Tushare event assets for `pledge_stat`, `pledge_detail`, and
  `stk_holdertrade`; their rows flow into event marts as `pledge_summary`,
  `pledge_event`, and `shareholder_trade`.
- Provider catalog entry for `fina_mainbz` exists as a candidate mapping to
  `business_segment_exposure`, but it is not yet promoted as a full adapter /
  dbt asset path.
- M4.3 bridge evidence now includes a non-skipped `make smoke-p1c` run
  against isolated PostgreSQL database `dp_p1c_smoke_m4bridge`, proving
  `candidate_queue` submit/validate/freeze into `cycle_candidate_selection`.

Known planning constraints:

- Holdings #96 promotes the Tushare interfaces `top10_holders`,
  `top10_floatholders`, `fund_portfolio`, `hsgt_top10`, and
  `hsgt_hold_top10` through data-platform Raw/staging/mart paths.
  `hsgt_hold_top10` is the data-platform dataset name over Tushare's
  official `hk_hold` API.
- `top10_holders` and `top10_floatholders` require explicit `ts_code`
  scope for live/raw fetches. Use repeated raw CLI `--ts-code` flags for
  bounded fetches, or set `DP_TUSHARE_TOP10_TS_CODES` for daily refresh.
- `hk_hold` / `hsgt_hold_top10` handles Tushare's row cap by splitting
  unscoped calls across `SH` and `SZ`, then paginating each exchange with
  `limit` / `offset`.
- Tushare `hk_hold` daily northbound publication stopped after 2024-08-20.
  Live daily refresh gates `hsgt_hold_top10` after that cutoff and reports an
  explicit skip instead of writing empty current-date Raw artifacts or claiming
  daily live freshness; quarterly disclosure/backfill handling is separate work.
- The provider-neutral `holding_position` mart identity includes
  `announced_date`: `(holding_source, holder_id, security_id, report_date,
  announced_date)`.
- Graph boundary: data-platform provides Phase 1 read adapters, the queue,
  and canonical intake surfaces only. Graph promotion write-back and graph
  snapshot computation are graph-engine-owned.
- Live holdings smoke stays blocked unless both `DP_TUSHARE_TOKEN` and
  `DP_TUSHARE_LIVE_HOLDINGS_SMOKE=1` are present.
- Post-merge P1a real-PG evidence was produced on 2026-05-04 from `main`
  merge commit `91038f69127677153f7bc4d1bab19859841915f8`; raw logs remain
  under `/tmp` only and are not committed.

## Post-merge P1a evidence (2026-05-04)

- P1a smoke ran with `DP_ENV=test`,
  `DP_SMOKE_P1A_CONFIRM_DESTRUCTIVE=1`,
  `DP_PG_DSN=postgresql://dp:<redacted>@localhost:5432/dp_p1a_smoke_20260504`,
  `DP_ICEBERG_CATALOG_NAME=data_platform_p1a_smoke_20260504`,
  `DP_SMOKE_WORK_DIR=/tmp/data-platform-p1a-smoke-20260504`, and
  `DP_RAW_ZONE_PATH`, `DP_ICEBERG_WAREHOUSE_PATH`, and `DP_DUCKDB_PATH`
  under that work dir, then `make smoke-p1a`.
  Result: `P1a smoke OK duration_s=8 log_dir=/tmp/data-platform-p1a-smoke-20260504/logs`;
  wrapper duration was 9s.
- Iceberg write-chain spike ran as
  `DATABASE_URL=<redacted> DP_PG_DSN=<redacted> .venv/bin/pytest -m spike tests/spike/test_iceberg_write_chain.py -v`.
  Result: 3 passed, 0 failed, 0 skipped, 0 errors in 1.25s. Covered
  `test_add_column_backward_compat`, `test_time_travel_by_snapshot`, and
  `test_concurrent_overwrite`. It used the local `.env` PG DSN with temporary
  schema behavior because `CREATE DATABASE` privilege was unavailable; no
  primary worktree artifacts were created.

## Next planning focus

Order the next data-platform work as:

1. **Keep P1a evidence reproducible**: the 2026-05-04 post-merge P1a smoke
   and Iceberg spike are non-skipped real-PG proof for the current merge.
   Do not commit raw smoke logs; keep them in `/tmp`.
2. **Holdings evidence**: run real Tushare smoke for the promoted holdings
   interfaces when `DP_TUSHARE_TOKEN` is available, then add historical
   backfill orchestration.
3. **Holdings derivations**: extend the minimal holding/northbound marts into
   top-holder QoQ change, fund co-holding, and northbound holding z-score.
   Reuse existing pledge marts instead of rebuilding pledge extraction.
4. **`fina_mainbz` adapter**: promote the structured business-segment path
   before any financial-doc NLP work tries to extract the same signal.

Execution rule:

1. read the project doc first
2. keep work inside this module unless the issue explicitly targets shared contracts
3. keep raw provider intake here; subsystem producers consume canonical/mart
   outputs instead of calling Tushare directly

Spike checks:

```bash
DATABASE_URL=<redacted> DP_PG_DSN=<redacted> .venv/bin/pytest -m spike tests/spike/test_iceberg_write_chain.py -v
cat docs/spike/iceberg-write-chain.md
```

The Iceberg write-chain spike requires `DATABASE_URL` or `DP_PG_DSN` to point at
a PostgreSQL database where the test user can create and drop temporary schemas.
The 2026-05-04 evidence used temporary schema behavior after `CREATE DATABASE`
was unavailable.

## Data storage layout

`data-platform` uses one configurable data storage root for local downloaded
and generated data:

```env
DP_DATA_STORAGE_ROOT_PATH=./data_platform/data
```

When `DP_RAW_ZONE_PATH` and `DP_PROCESSED_DATA_PATH` are not set explicitly,
they are derived from that root:

```text
<DP_DATA_STORAGE_ROOT_PATH>/raw
<DP_DATA_STORAGE_ROOT_PATH>/processed
```

Downloaded provider payloads are written through the Raw Zone writer under the
raw directory. Processed local outputs that are not Iceberg warehouse files
should use the processed directory. Existing deployments may still set
`DP_RAW_ZONE_PATH` directly; that override remains supported for backward
compatibility.

In the local `project-ult` workspace, `data-platform/.env` is a symlink to
`../.env`. Keep real credentials and local DP overrides in the workspace-level
`project-ult/.env`; the subproject `.env` path is retained for settings code
and tests that default to reading `.env` from this repository root.

The default `.env.example` layout is:

```env
DP_DATA_STORAGE_ROOT_PATH=./data_platform/data
DP_RAW_ZONE_PATH=./data_platform/data/raw
DP_PROCESSED_DATA_PATH=./data_platform/data/processed
```
