import json
import logging
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from ingestion.logging import LOGGER_NAME, on_task_failure


def logged_events(caplog):
    return [json.loads(r.getMessage()) for r in caplog.records if r.name == LOGGER_NAME]


def make_context(exception=None, run_after=datetime(2026, 9, 25, 16, 10, tzinfo=UTC)):
    dag_run = SimpleNamespace(
        dag_id="dummyjson_pipeline",
        run_id="manual__1",
        run_type="manual",
        run_after=run_after,
        conf=None,
    )
    ti = SimpleNamespace(task_id="extract_carts", try_number=3)
    return {"dag_run": dag_run, "ti": ti, "exception": exception}


def test_logs_failure_with_run_context(caplog):
    caplog.set_level(logging.ERROR, logger=LOGGER_NAME)

    on_task_failure(make_context(exception=RuntimeError("API down")))

    [event] = logged_events(caplog)
    assert event == {
        "event": "task_failed",
        "dag_id": "dummyjson_pipeline",
        "task_id": "extract_carts",
        "run_id": "manual__1",
        "try_number": 3,
        "business_date": "2026-09-24",
        "error": "RuntimeError('API down')",
    }
    assert caplog.records[-1].levelno == logging.ERROR


def test_still_logs_when_business_date_cannot_be_resolved(caplog):
    caplog.set_level(logging.ERROR, logger=LOGGER_NAME)

    # A run without run_after: resolve_business_date itself fails inside the callback.
    on_task_failure(make_context(exception=ValueError("boom"), run_after=None))

    [event] = logged_events(caplog)
    assert event["business_date"] is None
    assert event["error"] == "ValueError('boom')"


@pytest.mark.parametrize("context", [{}, {"dag_run": None, "ti": None}])
def test_never_raises_on_incomplete_context(caplog, context):
    caplog.set_level(logging.ERROR, logger=LOGGER_NAME)

    on_task_failure(context)

    [event] = logged_events(caplog)
    assert event["event"] == "task_failed"
    assert event["dag_id"] is None
