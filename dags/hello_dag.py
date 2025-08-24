from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator

# Define the DAG chạy 1p một lần
with DAG(
    dag_id="hello_dag",
    start_date=datetime(2024, 1, 1),
    schedule="* * * * *",
    catchup=False,
    tags=["check"],
):
    start = EmptyOperator(task_id="start")
