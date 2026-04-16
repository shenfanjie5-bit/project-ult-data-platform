"""PostgreSQL-backed Lite candidate queue APIs and types."""

from data_platform.queue.api import ExPayload, submit_candidate
from data_platform.queue.models import (
    CANDIDATE_QUEUE_TABLE,
    INGEST_METADATA_VIEW,
    CandidatePayloadType,
    CandidateQueueItem,
    IngestMetadataRecord,
    ValidationStatus,
)
from data_platform.queue.repository import CandidateQueueWriteError, CandidateRepository
from data_platform.queue.validation import (
    FORBIDDEN_PRODUCER_FIELDS,
    CandidateEnvelope,
    CandidateValidationError,
    ForbiddenIngestMetadataError,
    validate_candidate_envelope,
)

__all__ = [
    "CANDIDATE_QUEUE_TABLE",
    "FORBIDDEN_PRODUCER_FIELDS",
    "INGEST_METADATA_VIEW",
    "CandidateEnvelope",
    "CandidatePayloadType",
    "CandidateQueueItem",
    "CandidateQueueWriteError",
    "CandidateRepository",
    "CandidateValidationError",
    "ExPayload",
    "ForbiddenIngestMetadataError",
    "IngestMetadataRecord",
    "ValidationStatus",
    "submit_candidate",
    "validate_candidate_envelope",
]
