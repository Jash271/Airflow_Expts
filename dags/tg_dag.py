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
from airflow.operators.weekday import BranchDayOfWeekOperator
from airflow.utils.weekday import WeekDay
from airflow.operators.subdag import SubDagOperator
from subdag import subdag_factory
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from task_group import training_groups
# imoort s3 file sens

default_args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(1),
    'file_name':'demo_data.csv',
    'group':"hemlo"
}

def task_a ():
    print("task_a")

def _process(path,file_name):
    print(path,file_name)

@task(task_id = "task_b")
def process():
    context = get_current_context()
    print(context['ds'])


with DAG ('tg_parent_dag',default_args = default_args, schedule_interval = '@daily', catchup = False) as dag:
    start = BashOperator(
        task_id = 'start',
        bash_command = 'echo "start"'
    )
    group_training_task = TriggerDagRunOperator(
        task_id = 'group_training_task',
        trigger_dag_id = 'child_dag_tg',
        conf = {'group':'hemlo'},
        execution_date = "{{ds}}",
        wait_for_completion = True,
        #poke_interval = 10,
        reset_dag_run = True
    )

    # training_a = BashOperator(
    #     task_id = 'training_a',
    #     bash_command = 'echo "training_a"'
    # )
    # training_b = BashOperator(
    #     task_id = 'training_b',
    #     bash_command = 'echo "training_b"'
    # )
    # training_c = BashOperator(
    #     task_id = 'training_c',
    #     bash_command = 'echo "training_c"'
    # )

    end = BashOperator(
        task_id = 'end',
        bash_command = 'echo "end"'
    )
    
    start >> group_training_task >> end
    