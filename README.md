









# Steps to configure Airflow

export AIRFLOW_HOME = ~/airflow/



# Configure ~/airflow/airflow.cfg 

dags_folder = /<project-location>/dags/





# Steps to configure airflow


# Steps To run Airflow server:


airflow webserver -p 8080 
airflow scheduler
airflow worker


# steps to run the pipeline

airflow trigger_dag ETLPipeLineForMovieLensData

  (or)    
  
python dats/ETL_MovieLensData_pipeline.py



