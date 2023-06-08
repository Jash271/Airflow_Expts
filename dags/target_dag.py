import airflow
import requests
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.helpers import cross_downstream
from datetime import datetime, timedelta
from airflow.models import Variable
import boto3
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.operators.datetime import BranchDateTimeOperator
# imoort s3 file sens

default_args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(1),
    'file_name':'demo_data.csv'
}

def task_a ():
    print("task_a")

def _process(path,file_name):
    print(path,file_name)

@task(task_id = "task_b")
def process():
    context = get_current_context()
    print(context['ds'])


with DAG ('target_dag',default_args = default_args, schedule_interval = '@daily', catchup = False) as dag:
    is_in_time = BranchDateTimeOperator(
        task_id = 'is_in_time',
        follow_task_ids_if_true = ['task_a'],
        follow_task_ids_if_false = ['task_c'],
        target_lower = datetime(2023,6,2),
        target_upper = datetime(2023,6,3),
        use_task_execution_date = True
    )
    task_a = DummyOperator (
        task_id = 'task_a'
    )
    task_c = DummyOperator (
        task_id = 'task_c'
    )
    is_in_time >> [task_a,task_c]
    


    



