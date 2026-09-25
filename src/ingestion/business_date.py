"""Resolution of the business date a run loads.

The source's midnight (UTC) refresh closes the previous day, so the run at D+1 00:30 loads day D.
"""

from datetime import UTC, date, datetime, timedelta
from typing import Any

# Run types whose data interval is defined by the schedule. Compared as strings so this module
# doesn't depend on Airflow (DagRunType is a str enum).
INTERVAL_RUN_TYPES = frozenset({"scheduled", "backfill"})


def resolve_business_date(context: Any) -> date:
    """Business date for an Airflow run.

    - Scheduled run: date of `data_interval_start`.
    - Any other run (manual, triggered): `conf["business_date"]` (YYYY-MM-DD) if given, otherwise
      the day before `run_after` in UTC. Manual runs in Airflow 3 may have no logical date or data
      interval, so they can't be relied on.
    """
    dag_run = context["dag_run"]
    if dag_run.run_type in INTERVAL_RUN_TYPES:
        return _utc_date(context["data_interval_start"])

    run_after_date = _utc_date(dag_run.run_after)
    requested = (dag_run.conf or {}).get("business_date")
    if not requested:
        return run_after_date - timedelta(days=1)

    try:
        business_date = date.fromisoformat(str(requested))
    except ValueError:
        raise ValueError(f"conf business_date must be YYYY-MM-DD, got {requested!r}") from None
    if business_date >= run_after_date:
        raise ValueError(
            f"conf business_date {business_date} is not closed yet: the source only exposes "
            f"days before {run_after_date} (UTC)"
        )
    return business_date


def _utc_date(value: datetime) -> date:
    return value.astimezone(UTC).date()
