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
    assert set(dag.task_ids) == {"extract_products", "extract_carts"}
    for task_id in ("extract_products", "extract_carts"):
        task = dag.get_task(task_id)
        assert task.upstream_task_ids == set()
        assert task.downstream_task_ids == set()


def test_tasks_retry_and_report_failures(dag):
    for task in dag.tasks:
        assert task.retries == 2
        assert task.execution_timeout == timedelta(minutes=10)
        assert on_task_failure in task.on_failure_callback
