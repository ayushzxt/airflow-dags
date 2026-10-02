from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor

# Fails on purpose: the upstream file never arrived. Used to test triage.
with DAG(
    dag_id="demo_fail",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 1, "retry_delay": 5},
    tags=["demo"],
) as dag:
    check_file = FileSensor(
        task_id="check_input_file",
        filepath="/data/incoming/orders_{{ ds }}.csv",
        mode="reschedule",
        poke_interval=300,
        timeout=3600,
    )
    load = BashOperator(task_id="load_orders", bash_command="echo 'loading'")
    check_file >> load
