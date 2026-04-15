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
