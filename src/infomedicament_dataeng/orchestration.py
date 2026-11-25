"""Simple sequential orchestration for the daily data refresh."""

import json
import logging
import traceback
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID, uuid4

from sqlalchemy import text

from .config import AppConfig
from .datagouv import import_dataset, load_datasets
from .db import get_postgres_engine
from .grist import sync_grist
from .indications import build_indications
from .resume import build_resume
from .semantic_report import DocumentFailure, SemanticImportResult

logger = logging.getLogger(__name__)

# Stable PostgreSQL advisory-lock key used to prevent overlapping daily-sync runs.
ADVISORY_LOCK_ID = 4_806_644_302_025_092_801
MAX_ERROR_LENGTH = 8_000
SEMANTIC_WATERMARK_OVERLAP = timedelta(hours=24)
PROJECT_ROOT = Path(__file__).resolve().parents[2]


class PipelineAlreadyRunning(RuntimeError):
    """Raised when another daily sync owns the PostgreSQL advisory lock."""


class RunLedger:
    """Persist pipeline state using a dedicated database connection."""

    def __init__(self, connection):
        self.connection = connection

    def previous_semantic_watermark(self) -> datetime | None:
        return self.connection.execute(
            text(
                "SELECT semantic_watermark FROM pipeline_run "
                "WHERE semantic_watermark IS NOT NULL ORDER BY started_at DESC LIMIT 1"
            )
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

    def pending_documents(self) -> list[DocumentFailure]:
        report = self.connection.execute(
            text(
                "SELECT semantic_report FROM pipeline_run "
                "WHERE semantic_report IS NOT NULL ORDER BY started_at DESC LIMIT 1"
            )
        ).scalar_one_or_none()
        return [DocumentFailure(**document) for document in report["failed_documents"]] if report else []

    def record_semantic_result(self, run_id: UUID, result: SemanticImportResult) -> None:
        # Save retries and the watermark together: advancing one without the other
        # would let failed documents fall out of the incremental selection window.
        self.connection.execute(
            text(
                "UPDATE pipeline_run SET semantic_watermark = :watermark, "
                "semantic_report = CAST(:report AS jsonb) WHERE id = :id"
            ),
            {"id": run_id, "watermark": result.watermark, "report": json.dumps(result.report())},
        )
        self.connection.commit()

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
    semantic_importer: Callable[..., SemanticImportResult],
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
            previous_watermark = ledger.previous_semantic_watermark()
            pending_documents = ledger.pending_documents()
            semantic_cutoff = (
                previous_watermark - SEMANTIC_WATERMARK_OVERLAP if previous_watermark is not None else None
            )
            run_id = ledger.start_run(trigger, started_at)
            logger.info(
                "Starting daily sync (run %s, trigger=%s, pending_documents=%d)",
                run_id,
                trigger,
                len(pending_documents),
            )

            def import_semantic() -> None:
                result = semantic_importer(
                    since=semantic_cutoff, full=previous_watermark is None, pending_documents=pending_documents
                )
                ledger.record_semantic_result(run_id, result)
                if result.failed_documents:
                    logger.warning(
                        "Semantic import completed with %d pending documents; continuing pipeline (run %s)",
                        len(result.failed_documents),
                        run_id,
                    )

            steps = [
                ("import-ansm", lambda: sync_datagouv_config(ansm_config)),
                ("import-has", lambda: sync_datagouv_config(has_config)),
                (
                    "sync-grist",
                    lambda: sync_grist(config.grist.doc_id, config.grist.api_key, config.postgres),
                ),
                (
                    "semantic-db-import",
                    import_semantic,
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
