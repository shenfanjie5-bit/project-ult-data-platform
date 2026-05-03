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

- Holdings intake now promotes `top10_holders`, `top10_floatholders`,
  `fund_portfolio`, `hsgt_top10`, and `hsgt_hold_top10` through
  data-platform Raw/staging/mart paths. `hsgt_hold_top10` is the
  data-platform dataset name over Tushare's official `hk_hold` API.
  `top10_holders` and `top10_floatholders` require explicit `ts_code`
  scope for live/raw fetches; set `DP_TUSHARE_TOP10_TS_CODES` for
  daily refresh or pass `--ts-code` to the raw asset CLI.
- The P1a positive PostgreSQL evidence is still required: `make smoke-p1a`
  and the Iceberg write-chain spike must run against a real PG DSN without
  skipping before broader canonical/Iceberg production claims.

## Next planning focus

Order the next data-platform work as:

1. **Remaining real PG evidence**: produce non-skipped P1a smoke and
   Iceberg spike proof. P1c queue/freeze smoke evidence already exists for
   the M4 bridge.
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
pytest -m spike tests/spike/test_iceberg_write_chain.py -v
cat docs/spike/iceberg-write-chain.md
```

The Iceberg write-chain spike requires `DATABASE_URL` or `DP_PG_DSN` to point at
a PostgreSQL database where the test user can create and drop temporary schemas.

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

The default `.env.example` layout is:

```env
DP_DATA_STORAGE_ROOT_PATH=./data_platform/data
DP_RAW_ZONE_PATH=./data_platform/data/raw
DP_PROCESSED_DATA_PATH=./data_platform/data/processed
```
