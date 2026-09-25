from datetime import UTC, date, datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from ingestion.business_date import ensure_business_date_is_last_closed_day, resolve_business_date


def make_context(run_type, *, run_after, data_interval_start=None, conf=None):
    dag_run = SimpleNamespace(run_type=run_type, run_after=run_after, conf=conf)
    return {"dag_run": dag_run, "data_interval_start": data_interval_start}


def scheduled(interval_start_day: int):
    """Scheduled run covering [day 00:30, day+1 00:30) of September 2026."""
    return make_context(
        "scheduled",
        run_after=datetime(2026, 9, interval_start_day + 1, 0, 30, tzinfo=UTC),
        data_interval_start=datetime(2026, 9, interval_start_day, 0, 30, tzinfo=UTC),
    )


def business_date_at(context, now):
    """What the extract task does: resolve, then check against the real execution time."""
    business_date = resolve_business_date(context)
    ensure_business_date_is_last_closed_day(business_date, now)
    return business_date


# --- resolve_business_date -------------------------------------------------------------------


def test_scheduled_run_uses_data_interval_start():
    assert resolve_business_date(scheduled(24)) == date(2026, 9, 24)


def test_scheduled_run_date_is_taken_in_utc():
    # 2026-09-23 21:30 at UTC-3 is already 2026-09-24 in UTC.
    minus_3 = timezone(timedelta(hours=-3))
    context = make_context(
        "scheduled",
        run_after=datetime(2026, 9, 25, 0, 30, tzinfo=UTC),
        data_interval_start=datetime(2026, 9, 23, 21, 30, tzinfo=minus_3),
    )
    assert resolve_business_date(context) == date(2026, 9, 24)


@pytest.mark.parametrize("run_type", ["manual", "operator_triggered"])
def test_non_scheduled_run_uses_day_before_run_after(run_type):
    # Manual runs in Airflow 3 have no logical date nor data interval.
    context = make_context(run_type, run_after=datetime(2026, 9, 25, 16, 10, tzinfo=UTC))
    assert resolve_business_date(context) == date(2026, 9, 24)


def test_manual_run_ignores_conf():
    # There is no business_date parameter: the API has no history to load another day from.
    context = make_context(
        "manual",
        run_after=datetime(2026, 9, 25, 16, 10, tzinfo=UTC),
        conf={"business_date": "2026-09-20"},
    )
    assert resolve_business_date(context) == date(2026, 9, 24)


# --- guard: the business date must be the last closed day at execution time ------------------


def test_scheduled_run_on_time_passes():
    now = datetime(2026, 9, 25, 0, 31, tzinfo=UTC)
    assert business_date_at(scheduled(24), now) == date(2026, 9, 24)


def test_scheduled_run_late_but_same_day_passes():
    # Delayed until the evening: the API still exposes day 24 until next midnight.
    now = datetime(2026, 9, 25, 23, 59, tzinfo=UTC)
    assert business_date_at(scheduled(24), now) == date(2026, 9, 24)


def test_scheduled_run_executing_after_next_midnight_fails():
    # The API already moved on to day 25: loading now would label day 25's data as day 24.
    now = datetime(2026, 9, 26, 0, 5, tzinfo=UTC)
    with pytest.raises(ValueError, match="not the last closed day"):
        business_date_at(scheduled(24), now)


def test_clear_of_an_old_run_fails():
    now = datetime(2026, 9, 25, 10, 0, tzinfo=UTC)
    with pytest.raises(ValueError, match="business date 2026-09-20 is not the last closed day"):
        business_date_at(scheduled(20), now)


def test_start_between_midnight_and_0030_fails():
    # Stack started at 00:15 on day 25: the latest complete interval starts on day 23.
    now = datetime(2026, 9, 25, 0, 15, tzinfo=UTC)
    with pytest.raises(ValueError, match="not the last closed day"):
        business_date_at(scheduled(23), now)


def test_manual_run_passes():
    run_after = datetime(2026, 9, 25, 16, 10, tzinfo=UTC)
    context = make_context("manual", run_after=run_after)
    assert business_date_at(context, run_after + timedelta(seconds=5)) == date(2026, 9, 24)


def test_manual_run_triggered_before_midnight_and_executed_after_fails():
    context = make_context("manual", run_after=datetime(2026, 9, 25, 23, 59, 50, tzinfo=UTC))
    now = datetime(2026, 9, 26, 0, 0, 10, tzinfo=UTC)
    with pytest.raises(ValueError, match="not the last closed day"):
        business_date_at(context, now)


def test_guard_compares_in_utc():
    # 21:00 at UTC-3 on day 25 is 00:00 UTC on day 26: the last closed day is 25, not 24.
    now = datetime(2026, 9, 25, 21, 0, tzinfo=timezone(timedelta(hours=-3)))
    with pytest.raises(ValueError, match="not the last closed day"):
        ensure_business_date_is_last_closed_day(date(2026, 9, 24), now)
    ensure_business_date_is_last_closed_day(date(2026, 9, 25), now)
