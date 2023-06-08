import airflow
import requests
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator
from airflow.utils.helpers import cross_downstream
from datetime import datetime, timedelta
from airflow.models import Variable
import boto3
# imoort s3 file sensor s3 provider
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor

import time
default_args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(1),
    'file_name':'demo_data.csv'
}

def download_s3_website(**context):
    file_name = context['templates_dict']['file_name']
    print(file_name)
    # get access key and secret key from airflow vairbales 
    acess_key = Variable.get("aws_access_key")
    secret_key = Variable.get("aws_secret_key")
    s3 = boto3.client('s3', aws_access_key_id=acess_key, aws_secret_access_key= secret_key, region_name='us-east-1')
    s3.download_file('airflowdata1', 'processed_data/'+file_name, '/opt/airflow/includes/'+file_name)
    print("download_s3_website")
    #raise ValueError("error")



def download_website_a():
    print("download_website_a")
    #raise ValueError("error")

def download_website_b():
    print("download_website_b")
    #raise ValueError("error")

def download_failed():
    print("download_failed")
    #raise ValueError("error")

def download_succeed():
    print("download_succeed")
    #raise ValueError("error")

def process():
    print("process")
    #raise ValueError("error")

def notif_a():
    print("notif_a")
    #raise ValueError("error")

def notif_b():
    print("notif_b")
    #raise ValueError("error")

def print_me(**context):
    print("Trial_no", context['templates_dict']['trial_no'])
    #raise ValueError("error")

def sleeper():
    time.sleep(5)
    print("sleeper")
    #raise ValueError("error")

def push_xcom(**context):
    context['ti'].xcom_push(key='mykey', value='myval')
    print("push_xcom")
    #raise ValueError("error")

with DAG(dag_id='trigger_rule_dag_1', 
    default_args=default_args, 
    schedule_interval="@daily") as dag:

    # Change the trigger rules according to the
    # dag from the video
    # all_success
    # all_failed
    # all_done
    # one_failed
    # one_success
    # none_failed
    # none_skipped

    # wait for s3 file to arrive in bucket 

    # what is soft fail in file sensor -> if file is not present then it will not fail the task
    # what is poke interval -> how frequently it will check for file
    # what is timeout -> how long it will wait for file to arrive
    # what is mode -> poke or reschedule
    # what is wildcard match -> if we want to match multiple files



    
    # name of the file is found from x_com : file_name
    # check if multiple .csv file exist in s3 bucket or .txt files
    # if multiple files are present then we can use wildcard match


    # botocore.exceptions.NoCredentialsError: Unable to locate credentials -> 


    s3_file_sensor = S3KeySensor(
        task_id='s3_file_sensor',
        bucket_key=f's3://airflowdata1/processed_data/{dag.default_args["file_name"]}',
        wildcard_match=True,
        bucket_name=None,
        aws_conn_id='s3_conn',
        timeout=18*60*60,
        poke_interval=120,
        soft_fail=True,
        mode='poke',
    )

    download_s3_website_task = PythonOperator(
        task_id='download_s3_website',
        python_callable=download_s3_website,
        trigger_rule="all_success",
        provide_context=True,
        # pick file name from args and pass it
        templates_dict={
            'file_name': "{{ dag.default_args['file_name'] }}"
        }
    )



    
    download_website_a_task = PythonOperator(
        task_id='download_website_a',
        python_callable=download_website_a,
        #trigger_rule="all_success"
    )

    download_website_b_task = PythonOperator(
        task_id='download_website_b',
        python_callable=download_website_b,
        #trigger_rule="all_success"    
    )

    python_print_task = PythonOperator(
        task_id='python_print',
        python_callable=print_me,
        trigger_rule="all_success",
        retries=2,
        retry_delay=timedelta(seconds=5),
        provide_context=True,
        templates_dict={
            'trial_no': "{{ task_instance.try_number }}"
        },
        email=["jainarchana998@gmail.com"],
        email_on_failure=True,
        email_on_retry=True,
        depends_on_past=True

    )
    dummy_taska = PythonOperator(
        task_id='dummy_task_a',
        trigger_rule="all_done",
        pool = 'tmp',
        python_callable = sleeper,
        priority_weight = 3
    )

    dummy_taskb = PythonOperator(
        task_id='dummy_task_b',
        trigger_rule="all_done",
        pool = 'tmp',
        python_callable = sleeper,
        priority_weight = 2
    )

    dummy_taskc = PythonOperator(
        task_id='dummy_task_c',
        trigger_rule="all_done",
        pool = 'tmp',
        python_callable = sleeper,
        priority_weight = 4
    )

    dummy_taskd = PythonOperator(
        task_id='dummy_task_d',
        trigger_rule="all_done",
        pool = 'tmp',
        python_callable = sleeper,
        priority_weight = 1,
        execution_timeout=timedelta(seconds=2)
    )

    dummy_taske = PythonOperator(
        task_id='dummy_task_e',
        trigger_rule="all_done",
        pool = 'tmp',
        python_callable = sleeper,
        priority_weight = 5

    )
    dummy_task = DummyOperator(
        task_id='dummy_task',
        trigger_rule="all_done"
    )

    end_task = DummyOperator(
        task_id='end_task',
        trigger_rule="all_done"
    )

    push_xcom_task = PythonOperator(
        task_id='push_xcom_task',
        python_callable=push_xcom,
        trigger_rule="all_done"
    )

    # Implement dependencies below
    # ie:
    # a >> b        : b depends on a
    # [a b] >> c    : c depends on a and b
    # ...
    """
    download_website_a_task >> download_failed_task
    download_website_a_task >> download_succeed_task

    download_website_b_task >> download_failed_task
    download_website_b_task >> download_succeed_task

    download_failed_task >> process_task
    download_succeed_task >> process_task

    process_task >> notif_a_task
    process_task >> notif_b_task
"""

    download_website_a_task >> python_print_task >> dummy_task
    download_website_b_task >> python_print_task >> dummy_task

    cross_downstream([dummy_task],[dummy_taska,dummy_taskb,dummy_taskc,dummy_taskd,dummy_taske])
    [dummy_taska,dummy_taskb,dummy_taskc,dummy_taskd,dummy_taske] >> end_task 

    cross_downstream([s3_file_sensor],[download_website_a_task,download_s3_website_task])
    end_task >> push_xcom_task
