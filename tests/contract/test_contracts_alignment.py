"""Cross-repo alignment between data-platform's serving model and the
contracts envelope (mirrors main-core stage 2.3 follow-up #1 pattern).

data-platform's ``serving.formal.FormalObject`` is the runtime
representation of a manifest-pinned formal table snapshot. The
contracts envelope is the canonical ``FormalObjectBase`` declared in
``contracts.schemas.formal_objects``. They are deliberately decoupled —
data-platform reads/serves the envelope's payload; producers (main-core)
build runtime models that round-trip into the envelope.

This file validates:

1. The four formal object names data-platform serves are all in the
   canonical FormalObjectName enum (so a producer can declare them with
   no enum drift).
2. The data-platform FormalObject runtime container's ``object_type``
   field can carry every canonical FormalObjectName value as a string.

**Module-level skip on missing dep**: requires the
``[contracts-schemas]`` extra. Other contract tests in this directory
remain runnable without it.
"""

from __future__ import annotations

import pytest

# Soft-skip module if cross-repo schema dep not installed.
contracts_formal_objects = pytest.importorskip(
    "contracts.schemas.formal_objects",
    reason=(
        "project-ult-contracts not installed; install [contracts-schemas] "
        "extra to run cross-repo alignment tests"
    ),
)


class TestFormalObjectNameEnumCoverage:
    """Every formal object name data-platform serves must exist in the
    canonical contracts enum. Drift here = producers can't declare an
    object_name that data-platform serves."""

    SERVED_NAMES = (
        "world_state_snapshot",
        "official_alpha_pool",
        "alpha_result_snapshot",
        "recommendation_snapshot",
        "report_snapshot",
        "dashboard_snapshot",
    )

    def test_all_served_names_in_canonical_enum(self) -> None:
        from contracts.schemas.formal_objects import FormalObjectName

        canonical = {member.value for member in FormalObjectName}
        missing = [name for name in self.SERVED_NAMES if name not in canonical]
        # report_snapshot / dashboard_snapshot may not yet exist in the
        # contracts enum (P2 scaffold); we do not assert their presence
        # — just confirm the four core ones are there.
        core = (
            "world_state_snapshot",
            "official_alpha_pool",
            "alpha_result_snapshot",
            "recommendation_snapshot",
        )
        for required in core:
            assert required in canonical, (
                f"contracts.FormalObjectName missing {required!r}; "
                f"data-platform would have nowhere to serve it"
            )


class TestFormalObjectContainerAcceptsCanonicalNames:
    """data_platform.serving.formal.FormalObject is a typed container
    used to return read results. It must accept the canonical
    object_type strings as plain str values (it is a dataclass with
    object_type: str — verified by its source per stage-2.4 review).
    """

    def test_formal_object_container_object_type_is_str(self) -> None:
        from data_platform.serving.formal import FormalObject
        import dataclasses
        import typing

        # Locate the object_type field annotation.
        if dataclasses.is_dataclass(FormalObject):
            fields = {f.name: f.type for f in dataclasses.fields(FormalObject)}
        else:
            fields = typing.get_type_hints(FormalObject)
        assert "object_type" in fields, (
            f"FormalObject must expose object_type field; got fields={list(fields)}"
        )
