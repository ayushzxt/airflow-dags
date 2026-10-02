from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

# Fails on purpose: the upstream file never arrived. Used to test triage.
with DAG(
    dag_id="demo_fail",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 1, "retry_delay": 5},
    tags=["demo"],
) as dag:
    check_file = BashOperator(
        task_id="check_input_file",
        bash_command=(
            "f=/data/incoming/orders_{{ ds }}.csv; "
            "echo \"looking for $f\"; "
            "if [ ! -f \"$f\" ]; then "
            "echo \"ERROR: FileNotFoundError: input file $f not found (upstream export from billing-system did not arrive)\"; "
            "exit 1; fi"
        ),
    )
    load = BashOperator(task_id="load_orders", bash_command="echo 'loading'")
    check_file >> load
