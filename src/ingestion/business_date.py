"""Resolution and validation of the business date a run loads.

The source's midnight (UTC) refresh closes the previous day, so the run at D+1 00:30 loads day D.
The API has no history: whatever it returns belongs to the day that closed last. A run can only
load that day.
"""

from datetime import UTC, date, datetime, timedelta
from typing import Any

# Run types whose data interval is defined by the schedule. Compared as strings so this module
# doesn't depend on Airflow (DagRunType is a str enum).
INTERVAL_RUN_TYPES = frozenset({"scheduled", "backfill"})


def resolve_business_date(context: Any) -> date:
    """Business date an Airflow run is meant to load.

    - Scheduled run: date of `data_interval_start`.
    - Any other run (manual, triggered): the day before `run_after`, in UTC. Manual runs in
      Airflow 3 have no logical date nor data interval.
    """
    dag_run = context["dag_run"]
    if dag_run.run_type in INTERVAL_RUN_TYPES:
        return _utc_date(context["data_interval_start"])
    return _utc_date(dag_run.run_after) - timedelta(days=1)


def ensure_business_date_is_last_closed_day(business_date: date, now: datetime) -> None:
    """Fail unless `business_date` is the day before `now` (UTC), the only day the API exposes.

    Blocks, for any run type: clears of old runs, scheduled runs executing after the next
    midnight, and runs started between 00:00 and 00:30 UTC whose interval is two days old. Loading
    them would label the data of the last closed day with another date.
    """
    last_closed_day = _utc_date(now) - timedelta(days=1)
    if business_date != last_closed_day:
        raise ValueError(
            f"business date {business_date} is not the last closed day ({last_closed_day}, "
            f"the day before {now.astimezone(UTC):%Y-%m-%d %H:%M} UTC): the API only exposes "
            "that day, so loading now would mislabel its data"
        )


def _utc_date(value: datetime) -> date:
    return value.astimezone(UTC).date()
