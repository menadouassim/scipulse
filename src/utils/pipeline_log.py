"""SciPulse — Journal d'exécution du pipeline (index `pipeline-logs`)."""

import logging
from datetime import datetime, timezone

from elasticsearch import Elasticsearch

from src.utils.config import ES_HOST, IDX_LOGS

logger = logging.getLogger(__name__)


def log_event(
    dag_id: str,
    task_id: str,
    status: str,
    run_id: str | None = None,
    duration_s: float | None = None,
    docs_processed: int | None = None,
    message: str | None = None,
    es: Elasticsearch | None = None,
) -> None:
    """Écrit un événement dans pipeline-logs ; n'échoue jamais (observabilité best effort)."""
    doc = {
        "@timestamp": datetime.now(timezone.utc).isoformat(),
        "dag_id": dag_id, "task_id": task_id, "status": status, "run_id": run_id,
        "duration_s": duration_s, "docs_processed": docs_processed, "message": message,
    }
    try:
        (es or Elasticsearch(ES_HOST)).index(index=IDX_LOGS, document={k: v for k, v in doc.items() if v is not None})
    except Exception as exc:  # noqa: BLE001
        logger.warning("pipeline-logs indisponible : %s", exc)


def airflow_callback(status: str):
    """Fabrique un callback Airflow (on_success_callback / on_failure_callback)."""

    def _cb(context: dict) -> None:
        ti = context["task_instance"]
        log_event(
            dag_id=ti.dag_id, task_id=ti.task_id, status=status, run_id=context.get("run_id"),
            duration_s=ti.duration, message=str(context.get("exception") or ""),
        )

    return _cb
