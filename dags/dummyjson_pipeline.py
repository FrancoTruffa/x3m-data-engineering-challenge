"""Daily DummyJSON pipeline: products and carts to bronze.

Orchestration only; all logic lives in the `ingestion` package (src/ingestion).
"""

from datetime import timedelta

import pendulum
from airflow.sdk import CronDataIntervalTimetable, Param, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException

from ingestion.business_date import resolve_business_date
from ingestion.logging import on_task_failure
from ingestion.run import extract_and_load


@dag(
    dag_id="dummyjson_pipeline",
    # The source refreshes at midnight UTC and closes the previous day; 30 minutes of grace.
    # Explicit data-interval timetable: in Airflow 3 a plain cron string maps to
    # CronTriggerTimetable (no interval, logical_date = trigger time). With intervals, the run at
    # D+1 00:30 covers the interval starting on D, so data_interval_start is the business date.
    schedule=CronDataIntervalTimetable("30 0 * * *", timezone="UTC"),
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    # The API has no history: a backfill would label today's data with past dates.
    catchup=False,
    # There's no separate initial load (every run is a full snapshot), so the first scheduled run
    # is a regular, correct one. The platform default stays "paused at creation".
    is_paused_upon_creation=False,
    max_active_runs=1,
    params={
        "business_date": Param(
            None,
            type=["null", "string"],
            description="Manual runs only: day to load (YYYY-MM-DD). Defaults to yesterday (UTC).",
        ),
    },
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
        "on_failure_callback": on_task_failure,
    },
    tags=["dummyjson", "bronze"],
)
def dummyjson_pipeline():
    @task
    def extract(entity: str) -> dict:
        context = get_current_context()
        try:
            business_date = resolve_business_date(context)
        except ValueError as exc:
            # Invalid run configuration: retrying can't fix it, fail without retries.
            raise AirflowFailException(str(exc)) from exc
        return extract_and_load(
            entity,
            business_date=business_date,
            process_name=f"{context['dag'].dag_id}.{context['task'].task_id}",
        )

    extract.override(task_id="extract_products")("products")
    extract.override(task_id="extract_carts")("carts")


dummyjson_pipeline()
