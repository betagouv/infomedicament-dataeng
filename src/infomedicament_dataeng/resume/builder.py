"""Orchestrate independently transactional resume-table builders."""

from __future__ import annotations

from collections.abc import Callable

from ..config import PostgresConfig
from ..db import get_postgres_engine
from . import generics, indications, medicines, specialties, substances

Builder = Callable[[object], int]

BUILDERS: dict[str, Builder] = {
    "indications": indications.build,
    "substances": substances.build,
    "generiques": generics.build,
    "medicaments": medicines.build,
    "specialites": specialties.build,
}
RESUME_TARGETS = tuple(BUILDERS)


def build_resume(target: str = "all", config: PostgresConfig | None = None) -> dict[str, int]:
    """Build one resume target or every target in dependency-safe order."""
    if target != "all" and target not in BUILDERS:
        raise ValueError(f"Unknown resume target: {target}")
    selected = BUILDERS.items() if target == "all" else ((target, BUILDERS[target]),)
    engine = get_postgres_engine(config)
    results = {}
    for name, builder in selected:
        with engine.begin() as conn:
            results[name] = builder(conn)
    return results
