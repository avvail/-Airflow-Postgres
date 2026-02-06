from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Простейший DAG, который просто печатает дату
with DAG(
    dag_id='test_dag_v1',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['my_first_dag']
) as dag:

    task1 = BashOperator(
        task_id='print_hello',
        bash_command='echo "Hello Airflow! I work!"'
    )

    task1