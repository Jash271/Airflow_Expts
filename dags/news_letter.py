import airflow 
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.latest_only import LatestOnlyOperator
default_args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(3),
}



with DAG("news_letter",schedule_interval='@daily',default_args = default_args,catchup=True) as dag:
    task_a = BashOperator(
        task_id = 'task_a',
        bash_command = 'echo "task_a"'
    )
    task_date = BashOperator(
        task_id = 'task_date',
        bash_command = 'echo "{{ ds }}"'
    )
    isLatest = LatestOnlyOperator(
        task_id = 'isLatest'
    )
    send_newsletter = BashOperator(
        task_id = 'send_newsletter',
        bash_command = 'echo "send_newsletter"'
    )

    task_a >> task_date >> isLatest >> send_newsletter

