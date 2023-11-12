from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from vertica_utils import load_dataset_file_to_vertica

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 1, 1)
}

dag = DAG(
    'fill_tables',
    default_args=default_args,
    schedule_interval='@once'
)

def load_groups_table():
    load_dataset_file_to_vertica('/data/groups.csv', 'ST23052702__STAGING', 'groups', ['id', 'admin_id', 'group_name', 'registration_dt', 'is_private'], ['registration_dt'], {'id': int, 'admin_id': int, 'group_name': str, 'registration_dt': str, 'is_private': bool})

def load_dialogs_table():
    load_dataset_file_to_vertica('/data/dialogs.csv', 'ST23052702__STAGING', 'dialogs', ['message_id','message_ts','message_from','message_to','message','message_type'], ['message_ts'], {'message_id': int, 'message_ts': str, 'message_from': int, 'message_to': int, 'message': str, 'message_type': str})

def load_users_table():
    load_dataset_file_to_vertica('/data/users.csv', 'ST23052702__STAGING', 'users', ['id', 'chat_name', 'registration_dt', 'country', 'age'], ['registration_dt'], {'id': int, 'chat_name': str, 'registration_dt': str, 'country': str, 'age': int})


fill_groups_table = PythonOperator(
    task_id='fill_groups_table',
    python_callable=load_groups_table,
    dag=dag
)

fill_dialogs_table = PythonOperator(
    task_id='fill_dialogs_table',
    python_callable=load_dialogs_table,
    dag=dag
)

fill_users_table = PythonOperator(
    task_id='fill_users_table',
    python_callable=load_users_table,
    dag=dag
)

fill_groups_table 
fill_dialogs_table 
fill_users_table