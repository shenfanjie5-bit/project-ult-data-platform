# data-platform

This module is scaffold-only.

Source of truth:

- `docs/data-platform.project-doc.md`

Current workspace layout:

- `docs/`
- `src/`
- `tests/`
- `fixtures/`
- `scripts/`

Execution rule:

1. read the project doc first
2. keep work inside this module unless the issue explicitly targets shared contracts
3. do not treat this scaffold as finished implementation

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
