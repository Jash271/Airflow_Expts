

from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator
from airflow.utils.task_group import TaskGroup
def print_context(ds, **context):
    
    print(ds)
    print(context['templates_dict']['group'])
    print(context['params']['hey'])
    #print(kwargs['group'])
    return 'Whatever you return gets printed in the logs'

def training_groups():
    with TaskGroup("training_tasks") as training_tasks:
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
        
        task_1 >> [task_2, task_3] >> task_4
    return training_tasks

    