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
    for task in dag.tasks:
        assert task.retries == 2
        assert task.execution_timeout == timedelta(minutes=10)
        assert on_task_failure in task.on_failure_callback
