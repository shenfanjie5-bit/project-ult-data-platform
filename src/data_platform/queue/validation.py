"""Producer-envelope validation for Lite candidate queue writes."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
import json
from types import MappingProxyType
from typing import Any, Final, Protocol, TypeAlias, cast, get_args

from pydantic import ValidationError

from data_platform.contracts_compat import load_contracts_module
from data_platform.queue.models import CandidatePayloadType, CandidateQueueItem

ExPayload: TypeAlias = Mapping[str, Any]
_DEFAULT_FORBIDDEN_PRODUCER_FIELDS: Final[frozenset[str]] = frozenset(
    {"submitted_at", "ingest_seq"}
)
_CONTRACT_MODEL_NAMES: Final[dict[str, str]] = {
    "Ex-0": "Ex0Metadata",
    "Ex-1": "Ex1CandidateFact",
    "Ex-2": "Ex2CandidateSignal",
    "Ex-3": "Ex3CandidateGraphDelta",
}


def _contract_forbidden_producer_fields() -> frozenset[str]:
    contracts_module = load_contracts_module("contracts.schemas.ex_payloads")
    if contracts_module is None:
        return _DEFAULT_FORBIDDEN_PRODUCER_FIELDS
    forbidden_fields = getattr(contracts_module, "FORBIDDEN_INGEST_METADATA_FIELDS", None)
    if isinstance(forbidden_fields, frozenset | set):
        return frozenset(str(field_name) for field_name in forbidden_fields)
    return _DEFAULT_FORBIDDEN_PRODUCER_FIELDS


def _contract_candidate_payload_types() -> frozenset[str]:
    contracts_module = load_contracts_module("contracts.core.types")
    if contracts_module is None:
        return frozenset(cast(tuple[str, ...], get_args(CandidatePayloadType)))
    ex_type = getattr(contracts_module, "ExType", None)
    if ex_type is None:
        return frozenset(cast(tuple[str, ...], get_args(CandidatePayloadType)))
    return frozenset(str(member.value) for member in ex_type)


def _contract_payload_model(payload_type: str) -> type[Any] | None:
    contracts_module = load_contracts_module("contracts.schemas.ex_payloads")
    if contracts_module is None:
        return None
    model_name = _CONTRACT_MODEL_NAMES.get(payload_type)
    if model_name is None:
        return None
    model = getattr(contracts_module, model_name, None)
    return model if isinstance(model, type) else None


FORBIDDEN_PRODUCER_FIELDS: Final[frozenset[str]] = _contract_forbidden_producer_fields()
_CANDIDATE_PAYLOAD_TYPES: Final[frozenset[str]] = _contract_candidate_payload_types()


class CandidateValidationError(ValueError):
    """Raised when a producer candidate envelope is invalid."""


class ForbiddenIngestMetadataError(CandidateValidationError):
    """Raised when a producer attempts to supply Layer B ingest metadata."""

    def __init__(self, forbidden_fields: list[str]) -> None:
        self.forbidden_fields = forbidden_fields
        super().__init__(
            "producer payload must not include PostgreSQL ingest metadata fields: "
            f"{forbidden_fields}"
        )


class CandidateValidator(Protocol):
    """Validation hook for queue worker candidate checks."""

    def validate(self, item: CandidateQueueItem) -> None:
        """Raise CandidateValidationError when a candidate must be rejected."""


class EnvelopeCandidateValidator:
    """Default worker validator for producer-owned Ex payload envelope fields."""

    def validate(self, item: CandidateQueueItem) -> None:
        envelope = validate_candidate_envelope(item.payload)
        if envelope.payload_type != item.payload_type:
            msg = (
                "candidate payload_type does not match candidate_queue payload_type: "
                f"{envelope.payload_type} != {item.payload_type}"
            )
            raise CandidateValidationError(msg)
        if envelope.submitted_by != item.submitted_by:
            msg = (
                "candidate submitted_by does not match candidate_queue submitted_by: "
                f"{envelope.submitted_by} != {item.submitted_by}"
            )
            raise CandidateValidationError(msg)


@dataclass(frozen=True, slots=True)
class CandidateEnvelope:
    """Validated producer envelope ready for candidate_queue insertion."""

    payload_type: CandidatePayloadType
    submitted_by: str
    payload: Mapping[str, Any]

    def __post_init__(self) -> None:
        _validate_payload_type(self.payload_type)
        _validate_submitted_by(self.submitted_by)
        payload = _copy_valid_payload(self.payload)
        object.__setattr__(self, "payload", MappingProxyType(payload))


def validate_candidate_envelope(payload: Mapping[str, Any]) -> CandidateEnvelope:
    """Validate and normalize one producer Ex payload envelope."""

    if not isinstance(payload, Mapping):
        msg = "candidate payload must be a JSON object mapping"
        raise CandidateValidationError(msg)

    _reject_forbidden_ingest_metadata(payload)

    payload_type = payload.get("payload_type")
    submitted_by = payload.get("submitted_by")

    _validate_payload_type(payload_type)
    _validate_submitted_by(submitted_by)

    return CandidateEnvelope(
        payload_type=cast(CandidatePayloadType, payload_type),
        submitted_by=cast(str, submitted_by),
        payload=payload,
    )


def _validate_payload_type(value: object) -> None:
    if not isinstance(value, str) or value not in _CANDIDATE_PAYLOAD_TYPES:
        msg = f"payload_type must be one of {sorted(_CANDIDATE_PAYLOAD_TYPES)}"
        raise CandidateValidationError(msg)


def _validate_submitted_by(value: object) -> None:
    if not isinstance(value, str) or not value.strip():
        msg = "submitted_by is required and must be a non-empty string"
        raise CandidateValidationError(msg)


def _copy_valid_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        msg = "candidate payload must be a JSON object mapping"
        raise CandidateValidationError(msg)

    payload_copy = dict(payload)
    _reject_forbidden_ingest_metadata(payload_copy)
    _validate_string_keys(payload_copy)
    _validate_json_serializable(payload_copy)
    _validate_contract_payload(payload_copy)
    return payload_copy


def _reject_forbidden_ingest_metadata(payload: Mapping[str, Any]) -> None:
    forbidden_fields = sorted(FORBIDDEN_PRODUCER_FIELDS.intersection(payload))
    if forbidden_fields:
        raise ForbiddenIngestMetadataError(forbidden_fields)


def _validate_string_keys(value: object) -> None:
    if isinstance(value, Mapping):
        non_string_keys = [key for key in value if not isinstance(key, str)]
        if non_string_keys:
            msg = "candidate payload JSON object keys must be strings"
            raise CandidateValidationError(msg)
        for nested_value in value.values():
            _validate_string_keys(nested_value)
        return

    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray, str)):
        for nested_value in value:
            _validate_string_keys(nested_value)


def _validate_json_serializable(payload: Mapping[str, Any]) -> None:
    try:
        json.dumps(payload, allow_nan=False)
    except (TypeError, ValueError) as exc:
        msg = f"candidate payload must be JSON serializable: {exc}"
        raise CandidateValidationError(msg) from exc


def _validate_contract_payload(payload: Mapping[str, Any]) -> None:
    payload_type = payload.get("payload_type")
    submitted_by = payload.get("submitted_by")
    if not isinstance(payload_type, str) or not isinstance(submitted_by, str):
        return

    model = _contract_payload_model(payload_type)
    if model is None:
        return

    contract_payload = dict(payload)
    contract_payload.pop("payload_type", None)
    contract_payload.pop("submitted_by", None)

    subsystem_id = contract_payload.get("subsystem_id")
    if not isinstance(subsystem_id, str) or not subsystem_id.strip():
        msg = "candidate payload subsystem_id is required and must be a non-empty string"
        raise CandidateValidationError(msg)
    if subsystem_id.strip() != submitted_by.strip():
        msg = "candidate payload subsystem_id must match submitted_by"
        raise CandidateValidationError(msg)

    try:
        model.model_validate(contract_payload)
    except ValidationError as exc:
        msg = (
            f"{payload_type} payload does not match contracts schema: "
            f"{_format_contract_validation_error(exc)}"
        )
        raise CandidateValidationError(msg) from exc


def _format_contract_validation_error(exc: ValidationError) -> str:
    first_error = exc.errors()[0]
    location = ".".join(str(part) for part in first_error.get("loc", ()))
    message = str(first_error.get("msg", "invalid payload"))
    if location:
        return f"{location}: {message}"
    return message


__all__ = [
    "FORBIDDEN_PRODUCER_FIELDS",
    "CandidateEnvelope",
    "CandidateValidationError",
    "CandidateValidator",
    "EnvelopeCandidateValidator",
    "ExPayload",
    "ForbiddenIngestMetadataError",
    "validate_candidate_envelope",
]
