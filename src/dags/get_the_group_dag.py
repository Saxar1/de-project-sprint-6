from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import boto3

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def create_s3_client():
    AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    return s3_client

def download_users_file():
    try:
        s3_client = create_s3_client()
        s3_client.download_file(
            Bucket='sprint6',
            Key='users.csv',
            Filename='/data/users.csv'
        )
    except Exception as e:
        print(f"Error while downloading users file: {e}")

def download_groups_file():
    try:
        s3_client = create_s3_client()
        s3_client.download_file(
            Bucket='sprint6',
            Key='groups.csv',
            Filename='/data/groups.csv'
        )
    except Exception as e:
        print(f"Error while downloading groups file: {e}")    

def download_dialogs_file():
    try:
        s3_client = create_s3_client()
        s3_client.download_file(
            Bucket='sprint6',
            Key='dialogs.csv',
            Filename='/data/dialogs.csv'
        )
    except Exception as e:
        print(f"Error while downloading dialogs file: {e}") 

def check_files():
    with open('/data/users.csv', 'r') as f:
        print(f.read(10))
    with open('/data/groups.csv', 'r') as f:
        print(f.read(10))
    with open('/data/dialogs.csv', 'r') as f:
        print(f.read(10))

with DAG('download_other_files', 
         default_args=default_args, 
         schedule_interval='@once') as dag:

    t1 = PythonOperator(task_id='download_users_file', python_callable=download_users_file)
    t2 = PythonOperator(task_id='download_groups_file', python_callable=download_groups_file)
    t3 = PythonOperator(task_id='download_dialogs_file', python_callable=download_dialogs_file)
    t4 = PythonOperator(task_id='check_files', python_callable=check_files)

    t1 >> t4
    t2 >> t4
    t3 >> t4