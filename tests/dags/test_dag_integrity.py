import os
from datetime import timedelta

import pytest
from airflow.dag_processing.dagbag import DagBag
from airflow.sdk import CronDataIntervalTimetable

from ingestion.logging import on_task_failure

DAG_ID = "dummyjson_pipeline"


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=os.environ["AIRFLOW__CORE__DAGS_FOLDER"])


@pytest.fixture(scope="module")
def dag(dagbag):
    return dagbag.get_dag(DAG_ID)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_dag_is_loaded(dag):
    assert dag is not None


def test_timetable_has_data_intervals(dag):
    # A plain cron string maps to CronTriggerTimetable in Airflow 3 (no data interval,
    # logical_date = trigger time), which would label day D's data as D+1.
    assert isinstance(dag.timetable, CronDataIntervalTimetable)
    assert dag.timetable.expression == "30 0 * * *"
    assert str(dag.timetable.timezone) == "UTC"


def test_scheduling_flags(dag):
    assert dag.catchup is False
    assert dag.is_paused_upon_creation is False
    assert dag.max_active_runs == 1


def test_no_business_date_parameter(dag):
    # The API has no history: a run can only load the last closed day, never a chosen one.
    assert "business_date" not in dag.params


def test_tasks_and_dependencies(dag):
    # extract_products ─┐
    #                   ├─→ dbt_build_silver ─→ dbt_build_gold
    # extract_carts ────┘
    expected_upstream = {
        "extract_products": set(),
        "extract_carts": set(),
        "dbt_build_silver": {"extract_products", "extract_carts"},
        "dbt_build_gold": {"dbt_build_silver"},
    }
    assert set(dag.task_ids) == set(expected_upstream)
    for task_id, upstream in expected_upstream.items():
        assert dag.get_task(task_id).upstream_task_ids == upstream


@pytest.mark.parametrize("layer", ["silver", "gold"])
def test_dbt_tasks_build_their_layer_without_dates(dag, layer):
    command = dag.get_task(f"dbt_build_{layer}").bash_command
    assert command.startswith("/opt/dbt-venv/bin/dbt build ")
    assert f"--selector {layer}" in command
    # Pending dates come from the ingestion watermark, never from Airflow.
    assert "--vars" not in command


def test_tasks_retry_and_report_failures(dag):
    # Extraction: API failures are often transient. dbt: one retry for connection errors; a
    # failing data test is deterministic.
    expected_retries = {
        "extract_products": 2,
        "extract_carts": 2,
        "dbt_build_silver": 1,
        "dbt_build_gold": 1,
    }
    for task in dag.tasks:
        assert task.retries == expected_retries[task.task_id]
        assert task.execution_timeout == timedelta(minutes=10)
        assert on_task_failure in task.on_failure_callback


DBT_TASKS = {
    "dummyjson_pipeline": ["dbt_build_silver", "dbt_build_gold"],
    "dummyjson_reprocess": ["dbt_build_force_date"],
}


def test_all_dbt_tasks_share_the_single_slot_pool(dagbag):
    # One pool with 1 slot (created by airflow-init): two dbt builds never run at once.
    for dag_id, task_ids in DBT_TASKS.items():
        for task_id in task_ids:
            assert dagbag.get_dag(dag_id).get_task(task_id).pool == "dbt", (dag_id, task_id)


@pytest.fixture(scope="module")
def reprocess_dag(dagbag):
    return dagbag.get_dag("dummyjson_reprocess")


def test_reprocess_dag_is_manual_only(reprocess_dag):
    assert reprocess_dag.timetable.can_be_scheduled is False
    assert reprocess_dag.catchup is False
    assert reprocess_dag.max_active_runs == 1
    assert reprocess_dag.task_ids == ["dbt_build_force_date"]


def test_reprocess_dag_builds_the_day_models_with_force_date(reprocess_dag):
    command = reprocess_dag.get_task("dbt_build_force_date").bash_command
    assert command.startswith("/opt/dbt-venv/bin/dbt build ")  # build (with tests), never run
    # silver.products is excluded: reprocessing a past day would take it back to an older state.
    selected = command.split("--select ")[1].split(" --")[0].split()
    assert selected == ["carts", "cart_items", "product_daily_revenue"]
    assert '"force_date": "{{ params.force_date }}"' in command


def test_reprocess_force_date_param_only_accepts_a_date_shape(reprocess_dag):
    # The value ends up in a shell command: the pattern is checked when the run is triggered.
    schema = reprocess_dag.params.get_param("force_date").schema
    assert schema["type"] == "string"
    assert schema["pattern"] == r"^\d{4}-\d{2}-\d{2}$"
