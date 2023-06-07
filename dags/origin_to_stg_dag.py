from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import vertica_python
from typing import List, Dict, Optional
import contextlib
import pandas as pd

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 1, 1)
}

dag = DAG(
    'fill_tables',
    default_args=default_args,
    schedule_interval='@once'
)

def load_dataset_file_to_vertica(
    dataset_path: str,
    schema: str,
    table: str,
    columns: List[str],
    parse_dates: List[str],
    type_override: Optional[Dict[str, str]] = None,
):
    df = pd.read_csv(dataset_path, parse_dates=parse_dates, dtype=type_override)
    num_rows = len(df)
    vertica_conn = vertica_python.connect(
        host='vertica.tgcloudenv.ru',
        port=5433,
        user='st23052702',
        password='oEf0V0yqMejSTPS'
    )
    columns = ', '.join(columns)
    copy_expr = f"""
    COPY {schema}.{table} ({columns}) FROM STDIN DELIMITER ',' ENCLOSED BY ''''
    """
    chunk_size = num_rows // 100
    with contextlib.closing(vertica_conn.cursor()) as cur:
        start = 0
        while start <= num_rows:
            end = min(start + chunk_size, num_rows)
            print(f"loading rows {start}-{end}")
            df.loc[start: end].to_csv('/tmp/chunk.csv', index=False)
            with open('/tmp/chunk.csv', 'rb') as chunk:
                cur.copy(copy_expr, chunk, buffer_size=65536)
            vertica_conn.commit()
            print("loaded")
            start += chunk_size + 1
 
    vertica_conn.close()


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