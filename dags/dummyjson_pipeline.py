"""Daily DummyJSON pipeline: products and carts to bronze, then silver and gold with dbt.

Orchestration only; extraction logic lives in the `ingestion` package (src/ingestion) and
transformations in the dbt project (dbt/).
"""

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import CronDataIntervalTimetable, Param, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException

from ingestion.business_date import resolve_business_date
from ingestion.logging import on_task_failure
from ingestion.run import extract_and_load

DBT_BUILD = "/opt/dbt-venv/bin/dbt build --project-dir /opt/airflow/dbt --selector {layer}"


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
    # dbt processes pending dates by ingestion watermark (shared state in the warehouse): two
    # concurrent runs could rebuild the same date in parallel.
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
        # A normal run takes seconds. A hung task would block the next day's run
        # (max_active_runs=1), and the source has no history to recover a missed day.
        "execution_timeout": timedelta(minutes=10),
        "on_failure_callback": on_task_failure,
    },
    tags=["dummyjson", "bronze", "dbt"],
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

    # dbt reads what's pending from the tables themselves (ingestion watermark): no dates passed.
    # `build` runs each layer's tests, so a failing silver test stops the run before gold.
    dbt_build_silver = BashOperator(
        task_id="dbt_build_silver", bash_command=DBT_BUILD.format(layer="silver")
    )
    dbt_build_gold = BashOperator(
        task_id="dbt_build_gold", bash_command=DBT_BUILD.format(layer="gold")
    )

    (
        [
            extract.override(task_id="extract_products")("products"),
            extract.override(task_id="extract_carts")("carts"),
        ]
        >> dbt_build_silver
        >> dbt_build_gold
    )


dummyjson_pipeline()
