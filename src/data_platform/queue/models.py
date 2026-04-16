"""Python models for PostgreSQL-backed candidate queue records."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Literal


CandidatePayloadType = Literal["Ex-0", "Ex-1", "Ex-2", "Ex-3"]
ValidationStatus = Literal["pending", "accepted", "rejected"]

CANDIDATE_QUEUE_TABLE = "data_platform.candidate_queue"
INGEST_METADATA_VIEW = "data_platform.ingest_metadata"

_PG_GENERATED_PAYLOAD_FIELDS = frozenset({"submitted_at", "ingest_seq"})


@dataclass(frozen=True, slots=True)
class CandidateQueueItem:
    """A persisted candidate queue row.

    ``submitted_at`` and ``ingest_seq`` are PostgreSQL-generated columns; they
    must never be smuggled inside producer payloads.
    """

    id: int
    payload_type: CandidatePayloadType
    payload: Mapping[str, object]
    submitted_by: str
    submitted_at: datetime
    ingest_seq: int
    validation_status: ValidationStatus
    rejection_reason: str | None

    def __post_init__(self) -> None:
        _reject_pg_generated_payload_fields(self.payload)


@dataclass(frozen=True, slots=True)
class IngestMetadataRecord:
    """Public ingest metadata view row for downstream control-plane reads."""

    candidate_id: int
    payload_type: CandidatePayloadType
    submitted_by: str
    submitted_at: datetime
    ingest_seq: int
    validation_status: ValidationStatus
    rejection_reason: str | None


def _reject_pg_generated_payload_fields(payload: Mapping[str, object]) -> None:
    forbidden_fields = sorted(_PG_GENERATED_PAYLOAD_FIELDS.intersection(payload))
    if forbidden_fields:
        joined_fields = ", ".join(forbidden_fields)
        msg = (
            "producer payload must not include PostgreSQL-generated fields: "
            f"{joined_fields}"
        )
        raise ValueError(msg)


__all__ = [
    "CANDIDATE_QUEUE_TABLE",
    "INGEST_METADATA_VIEW",
    "CandidatePayloadType",
    "CandidateQueueItem",
    "IngestMetadataRecord",
    "ValidationStatus",
]
