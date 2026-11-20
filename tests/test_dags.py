"""The Airflow DAGs parse and keep their safety rules.

Airflow isn't a project dependency (it runs in its own image), so these skip under plain
pytest; make test-dags installs it for one run.
"""

import os
import tempfile
from pathlib import Path

import pytest

# Airflow reads its home when imported; keep it out of ~/airflow.
os.environ.setdefault("AIRFLOW_HOME", tempfile.mkdtemp(prefix="airflow-"))
pytest.importorskip("airflow")

import jinja2  # noqa: E402
from airflow.dag_processing.dagbag import DagBag  # noqa: E402

DAGS = Path(__file__).parents[1] / "dags"


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAGS))


def test_dags_import_without_errors(dagbag):
    assert dagbag.import_errors == {}
    assert set(dagbag.dag_ids) == {"gold_dbt", "gold_backfill", "iceberg_maintenance"}


def test_every_task_takes_the_iceberg_write_slot(dagbag):
    tasks = [task for dag in dagbag.dags.values() for task in dag.tasks]
    assert tasks
    assert {task.pool for task in tasks} == {"iceberg_writes"}


def test_scheduled_dags_never_catch_up_or_overlap(dagbag):
    for dag_id in ("gold_dbt", "iceberg_maintenance"):
        dag = dagbag.dags[dag_id]
        assert not dag.catchup
        assert dag.max_active_runs == 1


def test_dbt_runs_leave_the_seeds_alone(dagbag):
    for dag_id in ("gold_dbt", "gold_backfill"):
        command = dagbag.dags[dag_id].get_task("dbt_build").bash_command
        assert "--exclude resource_type:seed" in command


def test_backfill_passes_the_lookback_to_dbt(dagbag):
    command = dagbag.dags["gold_backfill"].get_task("dbt_build").bash_command
    rendered = jinja2.Template(command).render(params={"lookback_hours": 48})
    assert rendered.endswith("--vars '{lookback_hours: 48}'")


def test_backfill_cannot_reach_past_silver_retention(dagbag):
    schema = dagbag.dags["gold_backfill"].params.get_param("lookback_hours").schema
    assert schema["maximum"] == 72


def test_maintenance_submits_the_maintenance_job(dagbag):
    command = dagbag.dags["iceberg_maintenance"].get_task("maintain").bash_command
    assert command.startswith("$SPARK_HOME/bin/spark-submit ")
    assert command.endswith("/flight_lakehouse/maintenance/run.py")
