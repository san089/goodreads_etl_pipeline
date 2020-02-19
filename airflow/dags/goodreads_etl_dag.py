from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.goodreads_plugin import DataQualityOperator

#config = configparser.ConfigParser()
#config.read_file(open(f"{Path(__file__).parents[0]}/emr_config.cfg"))


default_args = {
    'owner': 'goodreads',
    'depends_on_past': True,
    'start_date': datetime(2020, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    #'retries': 1,
    #'retry_delay': timedelta(minutes=15),
    'catchup': True
}


dag_name = 'goodreads_pipeline'
dag = DAG(dag_name,
          default_args=default_args,
          description='Load and Transform data from landing zone to processed zone. Populate data from Processed zone to goodreads Warehouse.',
          schedule_interval=None,
          #schedule_interval='0 * * * *',
          max_active_runs = 1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

emrsshHook= SSHHook(ssh_conn_id='emr_ssh_connection')

jobOperator = SSHOperator(
    task_id="GoodReadsETLJob",
    command='cd /home/hadoop/goodreads_etl_pipeline/src;export PYSPARK_DRIVER_PYTHON=python3;export PYSPARK_PYTHON=python3;spark-submit --master yarn goodreads_driver.py;',
    ssh_hook=emrsshHook,
    dag=dag)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = "redshift",
    tables = ["goodreads_warehouse.authors", "goodreads_warehouse.reviews", "goodreads_warehouse.books", "goodreads_warehouse.users"]

)


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> jobOperator >> run_quality_checks >> end_operator
