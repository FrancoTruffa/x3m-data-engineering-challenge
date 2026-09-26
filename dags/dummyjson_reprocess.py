"""Targeted reprocessing: rebuild one day of silver.carts, silver.cart_items and
gold.product_daily_revenue from what is already in bronze. Never calls the API.

Manual only. Useful after fixing transformation logic, or if silver/gold were altered outside
the pipeline. With no logic changes and bronze untouched, it produces the same result.
silver.products is excluded: reprocessing a past day would take it back to an older state; it's
repaired with a full refresh of that model.
"""

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param, dag

# Same 1-slot pool as the daily DAG's dbt tasks (created by airflow-init). Not imported from
# dummyjson_pipeline: importing a DAG file re-registers its DAG here.
DBT_POOL = "dbt"

# force_date is validated twice: by the Param pattern at trigger time (so it's safe inside the
# shell command) and by dbt's on-run-start hook (format and presence in bronze) before any model.
REPROCESS_COMMAND = (
    "/opt/dbt-venv/bin/dbt build --project-dir /opt/airflow/dbt"
    # Selector (dbt/selectors.yml): carts, cart_items, product_daily_revenue + singular tests.
    " --selector reprocess"
    ' --vars \'{"force_date": "{{ params.force_date }}"}\''
)


@dag(
    dag_id="dummyjson_reprocess",
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    # Manual trigger only: active so a trigger runs without unpausing first.
    is_paused_upon_creation=False,
    max_active_runs=1,
    params={
        "force_date": Param(
            type="string",
            pattern=r"^\d{4}-\d{2}-\d{2}$",
            description="Day to rebuild in silver (carts, cart_items) and gold (YYYY-MM-DD).",
        ),
    },
    # One retry for transient connection errors; a failing data test is deterministic.
    default_args={"retries": 1},
    tags=["dummyjson", "dbt", "reprocess"],
)
def dummyjson_reprocess():
    # Always build (models + their tests), never run.
    BashOperator(task_id="dbt_build_force_date", bash_command=REPROCESS_COMMAND, pool=DBT_POOL)


dummyjson_reprocess()
