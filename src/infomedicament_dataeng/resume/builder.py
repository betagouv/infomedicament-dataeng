"""Orchestrate independently transactional resume-table builders."""

from __future__ import annotations

import logging
from collections.abc import Callable
from time import perf_counter

from ..config import PostgresConfig
from ..db import get_postgres_engine
from . import generics, indications, medicines, specialties, substances

logger = logging.getLogger(__name__)

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
        logger.info("Building resume table '%s'", name)
        started = perf_counter()
        with engine.begin() as conn:
            results[name] = builder(conn)
        logger.info("Built resume table '%s': %d rows in %.1fs", name, results[name], perf_counter() - started)
    return results
