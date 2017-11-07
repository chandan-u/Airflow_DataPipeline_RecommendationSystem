"""
This script just defines the Airflow DAG. It's not the actual processing. 
Based on the dag defined here the execution takes place in a different context
For distributed computing its advised to use XCom

more info: https://airflow.apache.org/tutorial.html#tasks


TODO: Replace print statements with logging.info in future
"""

import os

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
    'start_date': datetime(2017, 11, 6),
    'email': ['chandan.uppuluri@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    #'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}


# Instantiate DAG object
dag = DAG('ETLPipeLineForMovieLensData', default_args=default_args) 


#  directory paths
scripts_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),'..','scripts')
datasets_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),'..','datasets')


# Instantiate functions for python operators
def movieLensTransformPipe():

    sources = { "movies": "./dataset/movies.csv", "ratings" : "./dataset/ratings.csv", "tags" : "./dataset/tags.csv"}

    pipeline = SimpleMovileLensPipeLine(sources)
     
    pipeline.data_ingestion(sqlContext)
    pipeline.data_transformation()



print("dowloading movielens dataset @ dataset/")


# Operators sequence
task_download = BashOperator(
    task_id='download_movielens_data',
    bash_command='python3 ' + os.path.join(scripts_path, 'download.py'),
    dag=dag)

print("Unziping movielens dataset @ dataset/")


task_unzip = BashOperator(

    task_id='unzip_movielens_data',
    bash_command='python3 ' + os.path.join(scripts_path, 'unzip.py'),
    dag=dag)


print("moving files @ dataset/")


task_mv = BashOperator(

    task_id='mv_movielens_data',
    bash_command='mv ' + os.path.join(datasets_path, 'ml-latest-small', '* ') + datasets_path,
    dag=dag)


print("removing tmp directories @ dataset/")

task_rmdir = BashOperator(

    task_id='rmdir_movielens_data',
    bash_command='rm -r  ' + os.path.join(datasets_path, 'ml-latest-small',''),
    dag=dag)


print("transforming @dataset")


task_transform = BashOperator(

    task_id='transform_movielens_data',
    bash_command='python3 ' + os.path.join(scripts_path,'transform.py'),
    dag=dag)

print("created a master movielens fact dataset in:  dataset/fact_movileLens.csv ")



# uncomment this if you have to remove files in the dataset dir 
# except for the final transformed data

# task_rmdir_base = BashOperator(

#     task_id='rmdir_movielens_data',
#     bash_command='rm -r ' + os.path.join(datasets_path, 'ml-latest-small',''),
#     dag=dag)

# print("removing unnecessary files @dataset")



# setting up dependencies.

task_unzip.set_upstream(task_download)
task_mv.set_upstream(task_unzip)
task_rmdir.set_upstream(task_mv)
task_transform.set_upstream(task_rmdir)

