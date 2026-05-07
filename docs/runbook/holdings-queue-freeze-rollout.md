# Holdings Queue/Freeze Rollout Evidence Template

This runbook records only sanitized queue/freeze evidence for a holdings
production rollout. Do not paste proof-only database names, DSNs, tokens, raw
provider payloads, dbt logs, runtime paths, `.env` values, or full producer
payloads.

## Command Shape

Use redacted placeholders and bounded counts:

```bash
DP_PG_DSN=<redacted-dsn> \
PYTHONPATH=src:../contracts/src \
python -m data_platform.queue.worker --once --limit <bounded-limit>
```

```python
freeze_cycle_candidates(
    "CYCLE_<YYYYMMDD>",
    submitted_by="subsystem-holdings",
    payload_type="Ex-3",
)
```

## Receipt Shape

Record idempotent submit receipts in this shape only:

```json
{
  "candidate_id": 123,
  "payload_type": "Ex-3",
  "submitted_by": "subsystem-holdings",
  "submitted_at": "<iso8601>",
  "ingest_seq": 456,
  "validation_status": "pending",
  "rejection_reason": null,
  "replayed": false
}
```

Receipt evidence must not include `payload`, provider response fields, local
paths, DSNs, tokens, raw dbt output, or runtime artifact paths.

## Evidence Fields

Record:

- cycle id and rollout date
- command shape with redacted placeholders
- worker `scanned`, `accepted`, `rejected`, `rejection_counts`, and
  `rejections_by_reason_type`
- targeted freeze filters: `submitted_by=subsystem-holdings`,
  `payload_type=Ex-3`
- selected count and cutoff metadata
- code commit hash
- sanitized receipt hash or count, never raw payload

## Blocker Recording

If a safety gate blocks live execution, record only the blocker code, command
shape, status, and relevant counts. Do not run a destructive smoke path without
its explicit safety gate.
