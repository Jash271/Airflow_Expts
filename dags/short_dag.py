import airflow
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(1),
}

def check_date(execution_date):
    print(execution_date.weekday())
    print(execution_date)
    if execution_date.weekday() == 2:
        return True

with DAG ('short_dag',default_args = default_args, schedule_interval = '@daily', catchup = False) as dag:
    task_a = BashOperator(
        task_id = 'task_a',
        bash_command = 'echo "task_a"'
    )
    task_b = BashOperator(
        task_id = 'task_b',
        bash_command = 'echo "task_b"'
    )
    task_c = BashOperator(
        task_id = 'task_c',
        bash_command = 'echo "{{ ds }}"'
    )
    is_weekly_task = ShortCircuitOperator(
        task_id = 'is_weekly_task',
        python_callable = check_date,
    )
    task_d = BashOperator(
        task_id = 'task_d',
        bash_command = 'echo "task_d"'
    )

    task_a >> task_b >> task_c >> is_weekly_task >> task_d