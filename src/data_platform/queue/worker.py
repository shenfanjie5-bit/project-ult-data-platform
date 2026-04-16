"""Synchronous worker for validating pending Lite queue candidates."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from dataclasses import asdict, dataclass
import json
import sys
from time import perf_counter_ns
from typing import Never

from data_platform.queue.repository import CandidateRepository
from data_platform.queue.validation import CandidateValidator, EnvelopeCandidateValidator

_REJECTION_REASON_MAX_CHARS = 1000


@dataclass(frozen=True, slots=True)
class QueueValidationSummary:
    """Counts for one synchronous pending-candidate validation pass."""

    scanned: int
    accepted: int
    rejected: int
    duration_ms: int


def validate_pending_candidates(
    limit: int = 100,
    *,
    validator: CandidateValidator | None = None,
) -> QueueValidationSummary:
    """Validate up to ``limit`` pending candidates in one PostgreSQL transaction."""

    if limit < 1:
        msg = "limit must be a positive integer"
        raise ValueError(msg)

    start_ns = perf_counter_ns()
    candidate_validator = validator if validator is not None else EnvelopeCandidateValidator()
    repository = CandidateRepository()
    accepted = 0
    rejected = 0
    scanned = 0

    try:
        with repository._engine.begin() as connection:
            candidates = repository.fetch_pending_for_update(limit, connection)
            scanned = len(candidates)

            for candidate in candidates:
                try:
                    candidate_validator.validate(candidate)
                except Exception as exc:
                    rejected += 1
                    repository.mark_validation_result(
                        candidate.id,
                        "rejected",
                        _format_rejection_reason(exc),
                        connection,
                    )
                else:
                    accepted += 1
                    repository.mark_validation_result(
                        candidate.id,
                        "accepted",
                        None,
                        connection,
                    )
    finally:
        repository.close()

    duration_ms = int((perf_counter_ns() - start_ns) / 1_000_000)
    return QueueValidationSummary(
        scanned=scanned,
        accepted=accepted,
        rejected=rejected,
        duration_ms=duration_ms,
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Run one synchronous validation pass and print a JSON summary."""

    parser = _JsonErrorArgumentParser(
        prog="python3 -m data_platform.queue.worker",
        description="Validate pending data_platform.candidate_queue rows once.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="run one validation pass",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="maximum pending candidates to lock and validate",
    )

    try:
        args = parser.parse_args(argv)
        if not args.once:
            msg = "--once is required; daemon mode is not implemented"
            raise _CliUsageError(msg)
        if args.limit < 1:
            msg = "--limit must be a positive integer"
            raise _CliUsageError(msg)

        summary = validate_pending_candidates(limit=args.limit)
    except _CliUsageError as exc:
        _print_json_error(exc)
        return 2
    except Exception as exc:
        _print_json_error(exc)
        return 1

    print(json.dumps(asdict(summary), sort_keys=True))
    return 0


def _format_rejection_reason(exc: Exception) -> str:
    reason = str(exc).strip() or exc.__class__.__name__
    if len(reason) <= _REJECTION_REASON_MAX_CHARS:
        return reason
    return reason[:_REJECTION_REASON_MAX_CHARS]


class _CliUsageError(ValueError):
    """Raised for CLI argument errors that must be reported as JSON."""


class _JsonErrorArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> Never:
        raise _CliUsageError(message)


def _print_json_error(exc: Exception) -> None:
    print(
        json.dumps(
            {
                "error": str(exc),
                "error_type": exc.__class__.__name__,
            },
            sort_keys=True,
        ),
        file=sys.stderr,
    )


__all__ = [
    "QueueValidationSummary",
    "main",
    "validate_pending_candidates",
]


if __name__ == "__main__":
    raise SystemExit(main())
