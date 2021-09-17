import datetime
import json
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import datetime
from airflow.utils.dates import timedelta
from airflow.utils.task_group import TaskGroup
import os
from textwrap import dedent

# We're hardcoding this value here for the purpose of the demo, but in a production environment this
# would probably come from a config file and/or environment variables!
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
    "dbt_dag_task_wrapper",
    start_date=datetime(2021, 9, 16),
    default_args=default_args,
    description="A dbt wrapper for airflow",
    schedule_interval=timedelta(days=1),
)

with dag:

    run_jobs = TaskGroup("dbt_run")
    test_jobs = TaskGroup("dbt_test")

start = DummyOperator(task_id="start", dag=dag)

end = DummyOperator(task_id="end", dag=dag)

dbt_deps = BashOperator(
    task_id="dbt_deps", bash_command=f"dbt deps --project-dir {DBT_DIR}", dag=dag
)

start >> dbt_deps


def load_manifest():
    local_filepath = f"{DBT_DIR}/target/manifest.json"
    with open(local_filepath) as f:
        data = json.load(f)
    return data


def make_dbt_task(node, dbt_verb):
    """Returns an Airflow operator either run and test an individual model"""
    GLOBAL_CLI_FLAGS = "--no-write-json"
    model = node.split(".")[-1]

    with dag:
        if dbt_verb == "run":
            dbt_task = BashOperator(
                task_id=node,
                task_group=run_jobs,
                bash_command=f"""
                dbt {GLOBAL_CLI_FLAGS} {dbt_verb} --target dev --models {model} \
                --project-dir {DBT_DIR}
                """,
            )
        elif dbt_verb == "test":
            node_test = node.replace("model", "test")
            dbt_task = BashOperator(
                task_id=node_test,
                task_group=test_jobs,
                bash_command=f"""
                dbt {GLOBAL_CLI_FLAGS} {dbt_verb} --target dev --models {model} \
                --project-dir {DBT_DIR}
                """,
            )
    return dbt_task


data = load_manifest()
dbt_tasks = {}

for node in data["nodes"].keys():
    if node.split(".")[0] == "model":
        node_test = node.replace("model", "test")
        dbt_tasks[node] = make_dbt_task(node, "run")
        dbt_tasks[node_test] = make_dbt_task(node, "test")

for node in data["nodes"].keys():
    if node.split(".")[0] == "model":
        # Set dependency to run tests on a model after model runs finishes
        node_test = node.replace("model", "test")
        dbt_tasks[node] >> dbt_tasks[node_test] >> end
        # Set all model -> model dependencies
        for upstream_node in data["nodes"][node]["depends_on"]["nodes"]:
            upstream_node_type = upstream_node.split(".")[0]
            if upstream_node_type == "model":
                dbt_deps >> dbt_tasks[upstream_node] >> dbt_tasks[node]
