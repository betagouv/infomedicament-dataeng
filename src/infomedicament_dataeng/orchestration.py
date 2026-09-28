"""Simple sequential orchestration for the daily data refresh."""

import logging
import traceback
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID, uuid4

from sqlalchemy import text

from .config import AppConfig
from .datagouv import import_dataset, load_datasets
from .db import get_postgres_engine
from .grist import sync_grist
from .indications import build_indications
from .resume import build_resume

logger = logging.getLogger(__name__)

# Stable PostgreSQL advisory-lock key used to prevent overlapping daily-sync runs.
ADVISORY_LOCK_ID = 4_806_644_302_025_092_801
MAX_ERROR_LENGTH = 8_000
PROJECT_ROOT = Path(__file__).resolve().parents[2]


class PipelineAlreadyRunning(RuntimeError):
    """Raised when another daily sync owns the PostgreSQL advisory lock."""


class RunLedger:
    """Persist pipeline state using a dedicated database connection."""

    def __init__(self, connection):
        self.connection = connection

    def previous_success_started_at(self) -> datetime | None:
        return self.connection.execute(
            text("SELECT started_at FROM pipeline_run WHERE status = 'success' ORDER BY started_at DESC LIMIT 1")
        ).scalar_one_or_none()

    def start_run(self, trigger: str, started_at: datetime) -> UUID:
        run_id = uuid4()
        self.connection.execute(
            text(
                "INSERT INTO pipeline_run (id, trigger, status, started_at) "
                "VALUES (:id, :trigger, 'running', :started_at)"
            ),
            {"id": run_id, "trigger": trigger, "started_at": started_at},
        )
        self.connection.commit()
        return run_id

    def finish_run(
        self,
        run_id: UUID,
        status: str,
        *,
        failed_step: str | None = None,
        error: str | None = None,
    ) -> None:
        self.connection.execute(
            text(
                "UPDATE pipeline_run SET status = :status, finished_at = :finished_at, "
                "failed_step = :failed_step, error = :error WHERE id = :id"
            ),
            {
                "id": run_id,
                "status": status,
                "finished_at": datetime.now(timezone.utc),
                "failed_step": failed_step,
                "error": error,
            },
        )
        self.connection.commit()


def sync_datagouv_config(config_path: Path) -> dict[str, int]:
    """Fully import every resource in one data.gouv configuration."""
    imported: dict[str, int] = {}
    for name, dataset in load_datasets(config_path).items():
        imported[name] = import_dataset(dataset)
    return imported


def _error_details(error: BaseException) -> str:
    return "".join(traceback.format_exception(error))[-MAX_ERROR_LENGTH:]


def run_daily_sync(
    config: AppConfig,
    semantic_importer: Callable[..., None],
    *,
    trigger: str = "manual",
    ansm_config: Path = PROJECT_ROOT / "data_sources" / "ansm.yml",
    has_config: Path = PROJECT_ROOT / "data_sources" / "has.yml",
) -> UUID:
    """Run the daily pipeline sequentially and return its ledger run ID."""
    engine = get_postgres_engine(config.postgres)
    with engine.connect() as connection:
        acquired = connection.execute(
            text("SELECT pg_try_advisory_lock(:lock_id)"), {"lock_id": ADVISORY_LOCK_ID}
        ).scalar_one()
        if not acquired:
            raise PipelineAlreadyRunning("another daily-sync run is already in progress")

        try:
            ledger = RunLedger(connection)
            started_at = datetime.now(timezone.utc)
            previous_success = ledger.previous_success_started_at()
            run_id = ledger.start_run(trigger, started_at)
            steps = [
                ("import-ansm", lambda: sync_datagouv_config(ansm_config)),
                ("import-has", lambda: sync_datagouv_config(has_config)),
                (
                    "sync-grist",
                    lambda: sync_grist(config.grist.doc_id, config.grist.api_key, config.postgres),
                ),
                (
                    "semantic-db-import",
                    lambda: semantic_importer(since=previous_success, full=previous_success is None),
                ),
                ("build-indications", lambda: build_indications(config.postgres)),
                ("build-resume", lambda: build_resume("all", config.postgres)),
            ]

            try:
                for step_name, operation in steps:
                    logger.info("Starting pipeline step '%s'", step_name)
                    operation()
                    logger.info("Finished pipeline step '%s'", step_name)
            except Exception as error:
                ledger.finish_run(run_id, "failure", failed_step=step_name, error=_error_details(error))
                raise

            ledger.finish_run(run_id, "success")
            logger.info("Daily sync completed successfully (run %s)", run_id)
            return run_id
        finally:
            connection.execute(text("SELECT pg_advisory_unlock(:lock_id)"), {"lock_id": ADVISORY_LOCK_ID})
