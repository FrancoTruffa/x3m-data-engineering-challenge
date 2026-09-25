from datetime import UTC, date, datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from ingestion.business_date import resolve_business_date


def make_context(run_type, *, run_after, conf=None, data_interval_start=None):
    dag_run = SimpleNamespace(run_type=run_type, run_after=run_after, conf=conf)
    return {"dag_run": dag_run, "data_interval_start": data_interval_start}


def test_scheduled_run_uses_data_interval_start():
    # Run of 2026-09-25 00:30 covers the interval starting 2026-09-24 00:30.
    context = make_context(
        "scheduled",
        run_after=datetime(2026, 9, 25, 0, 30, tzinfo=UTC),
        data_interval_start=datetime(2026, 9, 24, 0, 30, tzinfo=UTC),
    )
    assert resolve_business_date(context) == date(2026, 9, 24)


def test_scheduled_run_ignores_conf():
    context = make_context(
        "scheduled",
        run_after=datetime(2026, 9, 25, 0, 30, tzinfo=UTC),
        data_interval_start=datetime(2026, 9, 24, 0, 30, tzinfo=UTC),
        conf={"business_date": "2026-09-20"},
    )
    assert resolve_business_date(context) == date(2026, 9, 24)


def test_scheduled_run_date_is_taken_in_utc():
    # 2026-09-23 22:30 at UTC-3 is already 2026-09-24 in UTC.
    minus_3 = timezone(timedelta(hours=-3))
    context = make_context(
        "scheduled",
        run_after=datetime(2026, 9, 25, 0, 30, tzinfo=UTC),
        data_interval_start=datetime(2026, 9, 23, 21, 30, tzinfo=minus_3),
    )
    assert resolve_business_date(context) == date(2026, 9, 24)


@pytest.mark.parametrize("conf", [None, {}, {"business_date": None}, {"business_date": ""}])
def test_manual_run_without_date_uses_day_before_run_after(conf):
    # Manual runs in Airflow 3 have no logical date nor data interval.
    context = make_context("manual", run_after=datetime(2026, 9, 25, 16, 10, tzinfo=UTC), conf=conf)
    assert resolve_business_date(context) == date(2026, 9, 24)


def test_manual_run_right_after_midnight_utc():
    context = make_context("manual", run_after=datetime(2026, 9, 25, 0, 5, tzinfo=UTC))
    assert resolve_business_date(context) == date(2026, 9, 24)


def test_manual_run_uses_conf_business_date():
    context = make_context(
        "manual",
        run_after=datetime(2026, 9, 25, 16, 10, tzinfo=UTC),
        conf={"business_date": "2026-09-23"},
    )
    assert resolve_business_date(context) == date(2026, 9, 23)


def test_non_scheduled_run_types_follow_manual_rule():
    context = make_context(
        "operator_triggered", run_after=datetime(2026, 9, 25, 16, 10, tzinfo=UTC)
    )
    assert resolve_business_date(context) == date(2026, 9, 24)


@pytest.mark.parametrize("value", ["2026/09/23", "23-09-2026", "yesterday"])
def test_manual_run_rejects_malformed_date(value):
    context = make_context(
        "manual", run_after=datetime(2026, 9, 25, 16, 10, tzinfo=UTC), conf={"business_date": value}
    )
    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        resolve_business_date(context)


@pytest.mark.parametrize("value", ["2026-09-25", "2026-09-26"])
def test_manual_run_rejects_days_not_closed_yet(value):
    context = make_context(
        "manual", run_after=datetime(2026, 9, 25, 16, 10, tzinfo=UTC), conf={"business_date": value}
    )
    with pytest.raises(ValueError, match="not closed yet"):
        resolve_business_date(context)
