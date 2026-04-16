from __future__ import annotations

from typing import Any, get_args

import pytest

from data_platform.contracts_compat import load_contracts_module
from data_platform.cycle import manifest as manifest_module
from data_platform.queue import CandidatePayloadType, FORBIDDEN_PRODUCER_FIELDS


@pytest.fixture()
def contracts_modules() -> tuple[Any, Any]:
    types_module = load_contracts_module("contracts.core.types")
    ex_payloads_module = load_contracts_module("contracts.schemas.ex_payloads")
    if types_module is None or ex_payloads_module is None:
        pytest.skip("contracts workspace package is not available")
    return types_module, ex_payloads_module


def test_candidate_payload_types_match_contracts(contracts_modules: tuple[Any, Any]) -> None:
    types_module, _ = contracts_modules

    assert set(get_args(CandidatePayloadType)) == {
        str(member.value) for member in types_module.ExType
    }


def test_forbidden_ingest_metadata_fields_match_contracts(
    contracts_modules: tuple[Any, Any],
) -> None:
    _, ex_payloads_module = contracts_modules

    assert FORBIDDEN_PRODUCER_FIELDS == frozenset(
        str(field_name) for field_name in ex_payloads_module.FORBIDDEN_INGEST_METADATA_FIELDS
    )


def test_manifest_formal_namespace_prefix_matches_contract_zone(
    contracts_modules: tuple[Any, Any],
) -> None:
    types_module, _ = contracts_modules

    assert manifest_module._FORMAL_NAMESPACE_PREFIX == f"{types_module.Zone.FORMAL.value}."
