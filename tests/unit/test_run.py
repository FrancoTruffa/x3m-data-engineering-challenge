import json
import logging
from contextlib import nullcontext
from datetime import date

import pytest

from ingestion import run
from ingestion.client import ExtractionError, ExtractionResult
from ingestion.logging import LOGGER_NAME

DAY = date(2026, 9, 24)


@pytest.fixture
def calls(monkeypatch):
    """Replace the HTTP client and the database with recording doubles."""
    recorded = {"fetch": [], "connect": 0, "load": []}

    def fake_fetch_all(entity):
        recorded["fetch"].append(entity)
        return ExtractionResult(records=[{"id": 1}, {"id": 2}], total=2, pages=1)

    def fake_connect():
        recorded["connect"] += 1
        return nullcontext("conn")

    def fake_load_snapshot(conn, entity, records, **kwargs):
        recorded["load"].append({"conn": conn, "entity": entity, "records": records, **kwargs})
        return len(records)

    monkeypatch.setattr(run, "fetch_all", fake_fetch_all)
    monkeypatch.setattr(run, "connect", fake_connect)
    monkeypatch.setattr(run, "load_snapshot", fake_load_snapshot)
    return recorded


def test_extracts_and_loads_with_business_date(calls):
    summary = run.extract_and_load("carts", business_date=DAY, process_name="dag.extract_carts")

    [entity] = calls["fetch"]
    assert entity.name == "carts"
    [load] = calls["load"]
    assert load["entity"] is entity
    assert load["records"] == [{"id": 1}, {"id": 2}]
    assert load["logical_date"] == DAY
    assert load["process_name"] == "dag.extract_carts"
    assert summary["entity"] == "carts"
    assert summary["business_date"] == "2026-09-24"
    assert (summary["pages"], summary["records"], summary["expected_total"]) == (1, 2, 2)
    assert summary["duration_s"] >= 0


def test_logs_start_and_finish(calls, caplog):
    caplog.set_level(logging.INFO, logger=LOGGER_NAME)

    run.extract_and_load("products", business_date=DAY, process_name="dag.task")

    events = [json.loads(r.getMessage()) for r in caplog.records if r.name == LOGGER_NAME]
    assert [e["event"] for e in events] == ["extraction_started", "extraction_finished"]
    assert events[1]["records"] == 2
    assert events[1]["expected_total"] == 2


def test_unknown_entity_fails_before_any_request(calls):
    with pytest.raises(ValueError, match="Unknown entity"):
        run.extract_and_load("users", business_date=DAY, process_name="dag.task")

    assert calls["fetch"] == []
    assert calls["connect"] == 0


def test_failed_extraction_leaves_bronze_untouched(calls, monkeypatch):
    # The whole snapshot is fetched before touching the database: if the API fails halfway,
    # the day's delete + insert never runs and the previous load stays intact.
    def failing_fetch_all(entity):
        raise ExtractionError("fetched 60 records, API reported total=208")

    monkeypatch.setattr(run, "fetch_all", failing_fetch_all)

    with pytest.raises(ExtractionError):
        run.extract_and_load("carts", business_date=DAY, process_name="dag.task")

    assert calls["connect"] == 0
    assert calls["load"] == []
