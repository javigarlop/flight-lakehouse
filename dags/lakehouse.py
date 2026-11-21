"""Airflow DAGs: gold models, Iceberg maintenance and backfills.

Runs in the Airflow container (infra/airflow/Dockerfile), with dbt in its own venv and
Spark copied from the project's Spark image. Every task writes Iceberg tables, so all of
them share the one-slot `iceberg_writes` pool (created on start, see docker-compose.yml):
a dbt merge and a compaction of the same gold table would fail each other's commits.

The seeds aren't built here: make dbt-seed loads reference.airports when deploying, and
reloading it every run would replace a table the silver job reads.
"""

from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param

POOL = "iceberg_writes"
START = datetime(2026, 11, 1)
DEFAULT_ARGS = {"pool": POOL, "retries": 1, "retry_delay": timedelta(minutes=5)}

# The project, profiles, target and log paths come from the container's DBT_* variables.
DBT_BUILD = "dbt --no-version-check build --exclude resource_type:seed"
# Silver keeps 7 to 8 days, so 168 hours is the most a backfill can recompute: an older
# bucket would be rebuilt from partly deleted rows and lose part of its counts.
MAX_LOOKBACK_HOURS = 168

SPARK_SUBMIT = (
    "$SPARK_HOME/bin/spark-submit --master=local[2] --driver-memory=1g"
    " --conf=spark.ui.enabled=false"
)

with DAG(
    dag_id="gold_dbt",
    description="Build the gold models from silver's last 24 hours and run the dbt tests",
    # Flights reach silver about 35 minutes after their last position; every 15 minutes
    # keeps the dashboard close behind that.
    schedule="*/15 * * * *",
    start_date=START,
    catchup=False,  # each run recomputes 24 hours, so skipped runs need no catching up
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    default_args=DEFAULT_ARGS,
    tags=["dbt", "gold"],
):
    BashOperator(task_id="dbt_build", bash_command=DBT_BUILD)

with DAG(
    dag_id="gold_backfill",
    description="Rebuild every gold bucket silver still holds",
    schedule=None,  # trigger it by hand, such as after fixing a gold model
    start_date=START,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={
        "lookback_hours": Param(
            MAX_LOOKBACK_HOURS,
            type="integer",
            minimum=1,
            maximum=MAX_LOOKBACK_HOURS,
            description="Hours of silver to recompute gold from",
        )
    },
    tags=["dbt", "gold"],
):
    BashOperator(
        task_id="dbt_build",
        bash_command=DBT_BUILD + " --vars '{lookback_hours: {{ params.lookback_hours }}}'",
    )

with DAG(
    dag_id="iceberg_maintenance",
    description="7-day retention, compaction, snapshot expiry and orphan cleanup",
    schedule="30 3 * * *",  # daily, at a quiet hour for Barcelona traffic
    start_date=START,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    default_args=DEFAULT_ARGS,
    tags=["iceberg", "maintenance"],
):
    BashOperator(
        task_id="maintain",
        bash_command=f"{SPARK_SUBMIT} /opt/airflow/src/flight_lakehouse/maintenance/run.py",
    )
