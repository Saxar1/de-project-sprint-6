from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import vertica_python
from typing import List, Dict, Optional
import contextlib
import pandas as pd
import logging

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 1, 1)
}

dag = DAG(
    'load_group_log_to_stg',
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
    df['user_id_from'] = pd.array(df['user_id_from'], dtype="Int64")
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


def load_group_log_table():
    load_dataset_file_to_vertica('/data/group_log.csv', 'ST23052702__STAGING', 'group_log', ['group_id', 'user_id', 'user_id_from', 'event', 'datetime'], ['datetime'], {'group_id': 'Int64', 'user_id': 'Int64', 'user_id_from': 'Int64', 'event': str, 'datetime': str})


fill_groups_table = PythonOperator(
    task_id='fill_group_log_table',
    python_callable=load_group_log_table,
    dag=dag
)

fill_groups_table 