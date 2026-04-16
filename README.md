# data-platform

Lite data platform for Raw archival, dbt transforms, Iceberg storage, queue ingest,
cycle control, and formal serving.

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
3. treat contracts alignment as a compatibility requirement, not as permission to
   bypass this package's storage and serving boundaries

Primary verification commands:

```bash
make smoke-p1a
make smoke-p1c
pytest
```

Environment notes:

- `make smoke-p1a` and `make smoke-p1c` require an isolated PostgreSQL database via
  `DP_PG_DSN`.
- `make smoke-p1c` also requires `DP_ENV=test` and
  `DP_SMOKE_P1C_CONFIRM_DESTRUCTIVE=1` because it truncates Layer B control tables in
  the target smoke database.
- Formal Serving must read via `cycle_publish_manifest`; do not point readers at the
  latest Formal table head directly.

Spike checks:

```bash
pytest -m spike tests/spike/test_iceberg_write_chain.py -v
cat docs/spike/iceberg-write-chain.md
```

The Iceberg write-chain spike requires `DATABASE_URL` or `DP_PG_DSN` to point at
a PostgreSQL database where the test user can create and drop temporary schemas.
