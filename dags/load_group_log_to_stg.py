from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from vertica_utils import load_dataset_file_to_vertica

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 1, 1)
}

dag = DAG(
    'load_group_log_to_stg',
    default_args=default_args,
    schedule_interval='@once'
)

def load_group_log_table():
    load_dataset_file_to_vertica('/data/group_log.csv', 'ST23052702__STAGING', 'group_log', ['group_id', 'user_id', 'user_id_from', 'event', 'datetime'], ['datetime'], {'group_id': 'Int64', 'user_id': 'Int64', 'user_id_from': 'Int64', 'event': str, 'datetime': str})


fill_groups_table = PythonOperator(
    task_id='fill_group_log_table',
    python_callable=load_group_log_table,
    dag=dag
)

fill_groups_table 