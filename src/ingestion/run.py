"""Extract-and-load entry point called from the Airflow tasks."""

import time
from datetime import date
from typing import Any

from ingestion.client import fetch_all
from ingestion.config import get_entity
from ingestion.loader import connect, load_snapshot
from ingestion.logging import get_logger, log_event


def extract_and_load(entity_name: str, *, business_date: date, process_name: str) -> dict[str, Any]:
    """Fetch the full snapshot of an entity and replace its `business_date` load in bronze."""
    entity = get_entity(entity_name)
    logger = get_logger()
    started = time.monotonic()
    log_event(logger, "extraction_started", entity=entity.name, business_date=business_date)

    result = fetch_all(entity)
    with connect() as conn:
        loaded = load_snapshot(
            conn,
            entity,
            result.records,
            logical_date=business_date,
            process_name=process_name,
        )

    summary = {
        "entity": entity.name,
        "business_date": business_date.isoformat(),
        "pages": result.pages,
        "records": loaded,
        "expected_total": result.total,
        "duration_s": round(time.monotonic() - started, 3),
    }
    log_event(logger, "extraction_finished", **summary)
    return summary
