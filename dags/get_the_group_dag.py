from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def download_users_file():
    import boto3
    
    AWS_ACCESS_KEY_ID = "YCAJEWXOyY8Bmyk2eJL-hlt2K"
    AWS_SECRET_ACCESS_KEY = "YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA"

    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    s3_client.download_file(
        Bucket='sprint6',
        Key='users.csv',
        Filename='/data/users.csv'
    )

def download_groups_file():
    import boto3
    
    AWS_ACCESS_KEY_ID = "YCAJEWXOyY8Bmyk2eJL-hlt2K"
    AWS_SECRET_ACCESS_KEY = "YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA"

    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    s3_client.download_file(
        Bucket='sprint6',
        Key='groups.csv',
        Filename='/data/groups.csv'
    )

def download_dialogs_file():
    import boto3
    
    AWS_ACCESS_KEY_ID = "YCAJEWXOyY8Bmyk2eJL-hlt2K"
    AWS_SECRET_ACCESS_KEY = "YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA"

    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    s3_client.download_file(
        Bucket='sprint6',
        Key='dialogs.csv',
        Filename='/data/dialogs.csv'
    )

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