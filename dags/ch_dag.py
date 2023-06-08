
import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator

default_args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(1),
    'file_name':'demo_data.csv'
}


def print_context(ds, **context):
    
    print(ds)
    print(context['templates_dict']['group'])
    print(context['params']['hey'])
    #print(kwargs['group'])
    return 'Whatever you return gets printed in the logs'


with DAG(f'child_dag_tg', default_args=default_args) as dag:
    task_1 = BashOperator(
        task_id='task_1',
        bash_command='echo "task_1"'
    )
    task_2 = BashOperator(
        task_id='task_2',
        bash_command='echo "task_2"'
    )
    task_3 = BashOperator(
        task_id='task_3',
        bash_command='echo "task_3"'
    )
    task_4 = BashOperator(
        task_id='task_4',
        bash_command='echo "task_4"'
    )
    task_5 = PythonOperator(
        task_id='task_5',
        python_callable=print_context,
        provide_context=True,
        templates_dict = {'group': '{{ dag_run.conf["group"] }}'},
        params = {'hey':'hi'}
    )
    task_1 >> [task_2, task_3] >> task_4
