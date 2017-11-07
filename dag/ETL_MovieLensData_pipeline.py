"""
This script just defines the Airflow DAG. It's not the actual processing. 
Based on the dag defined here the execution takes place in a different context
For distributed computing its advised to use XCom

more info: https://airflow.apache.org/tutorial.html#tasks
"""


# We need this to instantiate the DAG
from airflow import DAG

# We need an operator to execute our scripts
from airflow.operators.bash_operator import BashOperator






# args/config can be varied for production/dev environments
# Its good to have default one around
from datetime import datetime, timedelta

default_args = {
    'owner': 'ChandanU',
    'depends_on_past': False,
    'start_date': datetime(2017, 9, 1),
    'email': ['chandan.uppuluri@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}


# Instantiate DAG object
dag = DAG('simpleETLPipeLineForMovieLensData', default_args=default_args) 



# Operators sequence
task_download = BashOperator(
    task_id='extract_data',
    bash_command='python3 ../scripts/download.py',
    dag=dag)

task_unzip = BashOperator(

    task_id='unzip',
    bash_command='sleep 5',
    retries=3,
    dag=dag)


