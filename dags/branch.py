import airflow
import requests
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.helpers import cross_downstream
from datetime import datetime, timedelta
from airflow.models import Variable
import boto3
import yaml

# imoort s3 file sensor s3 provider
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor

default_args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(1),
    'file_name':'demo_data.csv'
}

class CustomPostgresOperator(PostgresOperator):
    template_fields = ('sql', 'parameters')

def check_accurate(ds):
    days_off = ['2021-08-01', '2021-08-02', '2021-08-03']
    if ds in days_off:
        return 'stop_task'
    return 'process_task'

    #return 'accurate' if 0 == 1 else 'inaccurate'

with DAG ('branch_dag',default_args = default_args, schedule_interval = '@daily', catchup = False) as dag:
    """
    train_task = DummyOperator(task_id = 'train_task')
    check_accurate = BranchPythonOperator(
        task_id = 'check_accurate',
        python_callable = check_accurate
    )

    accurate = DummyOperator(task_id = 'accurate')
    inaccurate = DummyOperator(task_id = 'inaccurate')
    publish_ml = DummyOperator(task_id = 'publish_ml')
    train_task >> check_accurate
    check_accurate >> [accurate, inaccurate]

    """

    check_op = BranchPythonOperator(
        task_id = 'check_accurate',
        python_callable = check_accurate
    )

    process_task = DummyOperator(task_id = 'process_task')
    after_task = DummyOperator(task_id = 'after_task')
    stop_task = DummyOperator(task_id = 'stop_task')

    check_op >> [process_task, stop_task]
    process_task >> after_task



