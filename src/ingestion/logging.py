"""Structured (JSON) logging helpers and the task failure callback."""

import json
import logging
from typing import Any

from ingestion.business_date import resolve_business_date

LOGGER_NAME = "ingestion"


def get_logger(name: str = LOGGER_NAME) -> logging.Logger:
    return logging.getLogger(name)


def log_event(
    logger: logging.Logger, event: str, *, level: int = logging.INFO, **fields: Any
) -> None:
    """Emit one log line whose message is a JSON object: {"event": ..., **fields}."""
    logger.log(level, json.dumps({"event": event, **fields}, default=str, sort_keys=True))


def on_task_failure(context: Any) -> None:
    """Airflow `on_failure_callback`: log the failure with enough context to act on it."""
    try:
        business_date = resolve_business_date(context)
    except Exception:
        # The callback must never fail itself; the date is informative only.
        business_date = None

    dag_run = context.get("dag_run")
    task_instance = context.get("ti") or context.get("task_instance")
    log_event(
        get_logger(),
        "task_failed",
        level=logging.ERROR,
        dag_id=getattr(dag_run, "dag_id", None),
        task_id=getattr(task_instance, "task_id", None),
        run_id=getattr(dag_run, "run_id", None),
        try_number=getattr(task_instance, "try_number", None),
        business_date=business_date,
        error=repr(context.get("exception")),
    )
