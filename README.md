
# About:

This project is End to End ETL pipleline of MovielensDataset for training a recommendation system.The end to end workflow is automated using Apache Airflow. 

Each pipeline is executed as Directed Acyclic graph using the Airflow

It uses spark for transformations. 

## Transformations

Each transformation peipleline can be defined by exetending etl.pipline.BasePipeLine

## Scripts

Scripts that process data/play with it are placed in the scripts folder

## Datasets

This is created by the piplelines where result dataset is generated


## Sheduler:

For the now the dag should be triggered externally. Dag's can be scheduled by enabling start_time in default_args or throug CLI

# Setup


## Steps to configure Airflow

export AIRFLOW_HOME = ~/airflow/


## install Requirements: 

pip install -r requirements.txt



## Configure ~/airflow/airflow.cfg 

dags_folder = /<project-location>/dags/


## Steps To run Airflow server:


airflow webserver -p 8080 
airflow scheduler
airflow worker


## steps to run the pipeline


### Using Airflow
airflow trigger_dag ETLPipeLineForMovieLensData

  (or)    

### Using python
python dags/ETL_MovieLensData_pipeline.py    



NOTE: This code requires python version 3.5. Not tested for other versions




# Pending tasks/future points

1. Setup a pipeline to train a simple predicitve model    
2. Working on subbranch instead of master    
3. Create Config file: To handle spark & files configuration (use ConfigParser for this)    
4. Modify scripts/transform.py to function 





