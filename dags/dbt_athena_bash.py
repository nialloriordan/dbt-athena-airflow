from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import datetime
from airflow.utils.dates import timedelta
import os

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
DBT_DIR = os.path.join(AIRFLOW_HOME, "plugins", "dbt-athena")

default_args = {
    "owner": "data_team",
    "depends_on_past": False,
    "start_date": datetime(2021, 9, 16),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
dag = DAG(
    "dbt_dag_bash",
    default_args=default_args,
    description="An Airflow DAG to invoke simple dbt commands",
    schedule_interval=timedelta(days=1),
)

with dag:

    start = DummyOperator(task_id="start")

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"dbt deps --project-dir {DBT_DIR}",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"dbt run --project-dir {DBT_DIR}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt test --project-dir {DBT_DIR}",
    )

    end = DummyOperator(task_id="end")

    start >> dbt_deps >> dbt_run >> dbt_test >> end
