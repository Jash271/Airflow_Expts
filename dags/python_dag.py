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


with DAG ('python_dag',default_args = default_args, schedule_interval = '@daily', catchup = False) as dag:
    task_a = PythonOperator(
        task_id = 'task_a',
        python_callable = _process,
        op_kwargs = {'path':'/opt/airflow/includes/','file_name':'demo_data.csv'}
    )
    task_c = BashOperator(
        task_id = 'task_c',
        bash_command = 'scripts/command.sh',
        env = {'file_name':'demo_data.csv'}
    )
    process() >> task_a



