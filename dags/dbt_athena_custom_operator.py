from datetime import timedelta

from airflow import DAG
from airflow_dbt.operators.dbt_operator import DbtRunOperator, DbtTestOperator
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
    "dbt_dag_custom_operator",
    default_args=default_args,
    description="An Airflow DAG to invoke simple dbt commands",
    schedule_interval=timedelta(days=1),
)

with dag:

    dbt_run = DbtRunOperator(
        task_id="dbt_run",
        dir=DBT_DIR,
    )

    dbt_test = DbtTestOperator(
        task_id="dbt_test",
        dir=DBT_DIR,
        retries=0,
    )

    dbt_run >> dbt_test