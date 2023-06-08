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
# imoort s3 file sensor s3 provider
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor

default_args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(1),
    'file_name':'demo_data.csv'
}

class CustomPostgresOperator(PostgresOperator):
    template_fields = ('sql', 'parameters')


with DAG ('PQL_dag',default_args = default_args, schedule_interval = '@daily', catchup = False) as dag:
    create_table = PostgresOperator(
        task_id = 'create_table',
        postgres_conn_id = 'postgres',
        sql = "sql/create_table.sql"
    )
    
    store = CustomPostgresOperator(
    task_id='store',
    postgres_conn_id='postgres',
    sql='sql/insert.sql',
    parameters={'column': '{{var.value.col}}', 'value': '{{var.value.my_var}}'}
)
    query = PostgresOperator(
        task_id = 'query',
        postgres_conn_id = 'postgres',
        sql = "select * from my_table"
    )



